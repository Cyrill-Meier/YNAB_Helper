"""The service wrappers around reconcile, dedupe, cleanup, crypto, IBKR, listings and DB stats
(docs/DESIGN.md §7), end to end against the fake YNAB through ``services``."""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Callable, Iterable, Mapping
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FAKE_TOKEN, FakeYnab
from tests.importer.helpers import csv_bytes
from tests.legacy import v1_revolut_to_ynab as v1
from ynab_helper import services
from ynab_helper.balance.adjust import AdjustmentRefused
from ynab_helper.balance.ibkr import IbkrClient
from ynab_helper.balance.reconcile import (
    ReconcileTokens,
    ReconcileUnavailable,
    StatementUnavailable,
)
from ynab_helper.errors import (
    Busy,
    ConfigError,
    StaleStateError,
    YnabError,
    YnabHelperError,
    YnabRateLimited,
)
from ynab_helper.locks import LOCKS
from ynab_helper.maintenance.dedupe import DedupeScans
from ynab_helper.services import UserContext, ValuationTarget
from ynab_helper.storage import connect
from ynab_helper.storage.mirror import MirrorStore
from ynab_helper.sync.ynab_sync import rate_limited_until

from .conftest import ACCT, BUDGET, fixture_bytes, make_ctx

PENDING_A = "export_pending_then_completed_a.csv"
TX_PATH = rf"/budgets/{BUDGET}/transactions"
CRYPTO = "acct-crypto"
IBKR = "acct-ibkr"
#: Last completed row of PENDING_A: SBB, completed 2026-10-12, Balance 1,282.00.
AS_OF = "2026-10-12"
TODAY = date(2026, 10, 13)
BIP84_RECEIVE_0 = "bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu"
ETH_ADDRESS = "0x" + "ab" * 20


# ── helpers ─────────────────────────────────────────────────────────────────


def _import(ctx: UserContext, name: str = PENDING_A, **kwargs: Any):
    return services.import_csv(
        ctx, fixture_bytes(name), original_name=name, source_kind="telegram", **kwargs
    )


def _mutations(fake: FakeYnab) -> list[tuple[str, str]]:
    return [(c.method, c.path) for c in fake.calls if c.method != "GET"]


def _seed_opening_balance(fake: FakeYnab, milli: int) -> None:
    """A reconciled opening balance from before the statement."""
    fake.add_transaction(
        BUDGET, ACCT, amount=milli, date="2026-09-30", payee_name="Opening", cleared="reconciled"
    )


def _meta(ctx: UserContext, key: str) -> str | None:
    conn = connect(ctx.db_path)
    try:
        return MirrorStore(conn).get_meta(key)
    finally:
        conn.close()


@pytest.fixture
def tokens() -> ReconcileTokens:
    return ReconcileTokens()


@pytest.fixture
def scans() -> DedupeScans:
    return DedupeScans()


@pytest.fixture(autouse=True)
def _token_clients(monkeypatch: pytest.MonkeyPatch, fake: FakeYnab) -> None:
    monkeypatch.setattr(
        services, "token_client_factory", lambda token: fake.client(token=token, max_retries=0)
    )


class FakeChain:
    """Stands in for BlockstreamClient: ``address -> (sats, tx_count)``."""

    def __init__(self, stats: Mapping[str, tuple[int, int]]) -> None:
        self.stats = dict(stats)
        self.queried: list[str] = []

    def address_stats(self, address: str) -> tuple[int, int]:
        self.queried.append(address)
        return self.stats.get(address, (0, 0))


class FakeEth:
    def __init__(self, wei: int = 0) -> None:
        self.wei = wei

    def get_balance(self, address: str) -> int:
        return self.wei

    def erc20_balance(self, address: str, contract: str) -> int:
        return 0


def _prices(table: Mapping[str, Decimal]) -> Callable[[Iterable[str]], dict[str, Decimal]]:
    return lambda symbols: {s: table[s] for s in symbols if s in table}


def _crypto_deps(sats: int = 50_000_000, wei: int = 2 * 10**18) -> dict[str, Any]:
    return {
        "blockstream": FakeChain({BIP84_RECEIVE_0: (sats, 3)}),
        "eth_rpc": FakeEth(wei),
        "price_fetcher": _prices({"BTC": Decimal("80000"), "ETH": Decimal("3000")}),
        "today": date(2026, 10, 16),
    }


def _gateway(nav: str = "123456.78") -> IbkrClient:
    def request(method: str, url: str, **kwargs: Any) -> Any:
        path = url.split("/v1/api", 1)[1]
        if path == "/portfolio/accounts":
            return [{"id": "U1234567", "accountId": "U1234567", "currency": "CHF"}]
        return {"BASE": {"netliquidationvalue": Decimal(nav)}}

    return IbkrClient("https://localhost:5050", request=request)


# ── reconcile (two steps) ───────────────────────────────────────────────────


def test_reconcile_two_steps_end_to_end(ctx, fake, tokens):
    _seed_opening_balance(fake, 995_000)  # YNAB ends 5.00 below the statement's 1,282.00
    assert _import(ctx).ok
    fake.reset_log()

    preview = services.reconcile_preview(ctx, now=TODAY, tokens=tokens)
    assert (preview.as_of, preview.csv_balance) == (AS_OF, 1_282_000)
    assert preview.ynab_cleared_as_of == 1_277_000 and preview.delta == 5_000
    assert preview.token is not None and preview.problems == []
    assert _mutations(fake) == []  # a preview never writes

    result = services.reconcile_apply(ctx, preview.token, now=TODAY, tokens=tokens)
    assert result.outcome == "created"
    adjustment = fake.find_by_import_id(ACCT, f"RECON:5000:{AS_OF}:1", include_deleted=False)
    assert adjustment is not None
    assert (adjustment["payee_name"], adjustment["cleared"], adjustment["date"]) == (
        "CSV Reconciliation",
        "reconciled",
        AS_OF,
    )
    assert [m for m, _ in _mutations(fake)] == ["POST"]
    # The mirror is stale after the write: the next status syncs and sees the adjustment.
    assert _meta(ctx, f"last_sync_at:{BUDGET}") is None

    again = services.reconcile_preview(ctx, now=TODAY, tokens=tokens)
    assert again.in_sync and again.token is None and again.delta == 0
    with pytest.raises(StaleStateError):  # one-time token
        services.reconcile_apply(ctx, preview.token, now=TODAY, tokens=tokens)
    assert [m for m, _ in _mutations(fake)] == ["POST"]


def test_reconcile_apply_refuses_when_ynab_changed_after_the_preview(ctx, fake, tokens):
    _seed_opening_balance(fake, 995_000)
    _import(ctx)
    preview = services.reconcile_preview(ctx, now=TODAY, tokens=tokens)
    fake.add_transaction(BUDGET, ACCT, amount=-1_000, date="2026-10-05", cleared="cleared")
    fake.reset_log()
    with pytest.raises(StaleStateError):
        services.reconcile_apply(ctx, preview.token, now=TODAY, tokens=tokens)
    assert _mutations(fake) == []  # the apply synced first, recomputed and refused


def test_reconcile_needs_an_imported_statement(ctx, fake, tokens):
    with pytest.raises(StatementUnavailable) as err:
        services.reconcile_preview(ctx, now=TODAY, tokens=tokens)
    assert err.value.reason == "no_statement"


def test_F04_reconcile_after_a_newer_reconciliation_is_refused(ctx, fake, tokens):
    _seed_opening_balance(fake, 995_000)
    _import(ctx)
    fake.add_transaction(BUDGET, ACCT, amount=-500, date="2026-10-14", cleared="reconciled")
    with pytest.raises(ReconcileUnavailable) as err:
        services.reconcile_preview(ctx, now=TODAY, tokens=tokens)
    assert err.value.reason == "reconciled_after_as_of"


@pytest.mark.parametrize("change", ["closed", "deleted"])
def test_reconcile_refuses_an_account_ynab_no_longer_offers(ctx, fake, tokens, change):
    _seed_opening_balance(fake, 995_000)
    _import(ctx)
    preview = services.reconcile_preview(ctx, now=TODAY, tokens=tokens)
    fake.edit_account(BUDGET, ACCT, **{change: True})
    fake.reset_log()
    with pytest.raises(ConfigError) as err:
        services.reconcile_apply(ctx, preview.token, now=TODAY, tokens=tokens)
    assert "Nothing was posted" in err.value.user_message and "/setup" in err.value.user_message
    assert _mutations(fake) == []


def test_reconcile_rate_limited_sync_keeps_the_token_usable(ctx, fake, tokens):
    _seed_opening_balance(fake, 995_000)
    _import(ctx)
    preview = services.reconcile_preview(ctx, now=TODAY, tokens=tokens)
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 429, retry_after=30)
    with pytest.raises(YnabRateLimited):
        services.reconcile_apply(ctx, preview.token, now=TODAY, tokens=tokens)
    assert services.reconcile_apply(ctx, preview.token, now=TODAY, tokens=tokens).outcome == (
        "created"
    )


def test_F17_a_429_on_the_reconcile_post_starts_the_backoff(ctx, fake, tokens):
    _seed_opening_balance(fake, 995_000)
    _import(ctx)
    preview = services.reconcile_preview(ctx, now=TODAY, tokens=tokens)
    fake.fail_next("POST", TX_PATH, 429, retry_after=120)
    with pytest.raises(YnabRateLimited):
        services.reconcile_apply(ctx, preview.token, now=TODAY, tokens=tokens)
    conn = connect(ctx.db_path)
    try:
        assert rate_limited_until(MirrorStore(conn), BUDGET) is not None
    finally:
        conn.close()
    fake.reset_log()
    status = services.account_status(ctx, min_interval=0)
    assert status.sync is not None and status.sync.rate_limited
    assert fake.calls == []  # the backoff keeps /status quiet


# ── dedupe ──────────────────────────────────────────────────────────────────


def test_dedupe_scan_and_delete_end_to_end(ctx, fake, scans):
    _import(ctx)
    stray = fake.add_transaction(
        BUDGET,
        ACCT,
        amount=-11_200,
        date="2026-10-10",
        payee_name="Coop Pronto",
        import_id="YNAB:-11200:2026-10-10:2",
    )
    scan = services.dedupe_scan(ctx, registry=scans)
    assert [c.id for c in scan.candidates] == [stray]
    fake.reset_log()

    result = services.dedupe_delete(ctx, scan.scan_id, [stray], registry=scans)
    assert result.ok and [c.id for c in result.deleted] == [stray]
    assert fake.tx(stray)["deleted"] is True
    assert ("DELETE", f"/budgets/{BUDGET}/transactions/{stray}") in _mutations(fake)
    assert _meta(ctx, f"last_sync_at:{BUDGET}") is None  # stale after the deletes


def test_dedupe_delete_with_an_expired_scan_sends_nothing(ctx, fake, scans):
    _import(ctx)
    fake.reset_log()
    with pytest.raises(StaleStateError):
        services.dedupe_delete(ctx, "no-such-scan", ["x"], registry=scans)
    assert fake.calls == []  # not even the sync


def test_dedupe_scan_is_replaced_by_an_import(ctx, fake):
    _import(ctx)
    scan = services.dedupe_scan(ctx)
    _import(ctx)  # the ledger may now link what the scan offered
    with pytest.raises(StaleStateError):
        services.dedupe_delete(ctx, scan.scan_id, ["x"])


def test_F17_a_429_while_deleting_duplicates_starts_the_backoff(ctx, fake, scans):
    _import(ctx)
    ids = [
        fake.add_transaction(
            BUDGET, ACCT, amount=-1_000 - n, date="2026-10-10", import_id=f"YNAB:-{1000 + n}:x:1"
        )
        for n in range(2)
    ]
    scan = services.dedupe_scan(ctx, registry=scans)
    fake.fail_next("DELETE", None, 429)
    result = services.dedupe_delete(ctx, scan.scan_id, ids, registry=scans)
    assert result.rate_limited and len(result.not_attempted) == 2
    assert _meta(ctx, f"rate_limited_until:{BUDGET}") is not None


# ── cleanup ─────────────────────────────────────────────────────────────────


def test_cleanup_pending_dry_run_then_real(ctx, fake):
    tx = fake.add_transaction(
        BUDGET,
        ACCT,
        amount=-4_500,
        date="2026-10-01",
        memo="Card Payment | (pending)",
        cleared="cleared",
    )
    fake.add_transaction(
        BUDGET, ACCT, amount=-1_000, date="2026-10-02", memo="(pending)", cleared="uncleared"
    )
    dry = services.cleanup_pending(ctx, dry_run=True)
    assert [f.tx_id for f in dry.candidates] == [tx] and dry.updated == []
    assert dry.skipped_uncleared == 1 and _mutations(fake) == []

    real = services.cleanup_pending(ctx)
    assert real.ok and [f.tx_id for f in real.updated] == [tx]
    assert fake.tx(tx)["memo"] == "Card Payment"
    assert fake.tx(tx)["cleared"] == "cleared"  # only the memo was sent (F01)


def test_F17_a_429_during_cleanup_starts_the_backoff(ctx, fake):
    fake.add_transaction(
        BUDGET, ACCT, amount=-4_500, date="2026-10-01", memo="(pending)", cleared="cleared"
    )
    fake.fail_next("PATCH", TX_PATH, 429)
    result = services.cleanup_pending(ctx)
    assert result.rate_limited and not result.ok
    assert _meta(ctx, f"rate_limited_until:{BUDGET}") is not None


# ── crypto / IBKR ───────────────────────────────────────────────────────────


@pytest.fixture
def crypto_ctx(data_dir: Path, fake: FakeYnab) -> UserContext:
    fake.add_account(BUDGET, CRYPTO, "Crypto", type="otherAsset", on_budget=False)
    return make_ctx(
        data_dir,
        crypto_account_id=CRYPTO,
        btc_xpub=BIP84_RECEIVE_0,
        eth_address=ETH_ADDRESS,
    )


def test_crypto_sync_for_a_bot_user_posts_the_difference(crypto_ctx, fake):
    fake.add_transaction(BUDGET, CRYPTO, amount=20_000_000, date="2026-10-01")
    result = services.crypto_sync(crypto_ctx, **_crypto_deps())
    assert result.portfolio_milli == 46_000_000  # 0.5 BTC × 80,000 + 2 ETH × 3,000
    assert result.adjustment.outcome == "created"
    assert result.adjustment.import_id == "CRYPTO:26000000:2026-10-16:1"
    tx = fake.tx(result.adjustment.ynab_tx_id)
    assert (tx["account_id"], tx["payee_name"]) == (CRYPTO, "Crypto Portfolio Adjustment")
    # The budget currency is cached in the user's DB; the mirror is stale after the write.
    assert _meta(crypto_ctx, f"budget_currency:{BUDGET}") == "CHF"
    assert _meta(crypto_ctx, f"last_sync_at:{BUDGET}") is None


def test_crypto_sync_dry_run_posts_nothing(crypto_ctx, fake):
    result = services.crypto_sync(crypto_ctx, dry_run=True, **_crypto_deps())
    assert result.adjustment.outcome == "dry_run"
    assert _mutations(fake) == []


def test_crypto_sync_needs_a_tracking_account(data_dir, fake):
    with pytest.raises(ConfigError) as err:
        services.crypto_sync(make_ctx(data_dir), **_crypto_deps())
    assert "/crypto_setup" in err.value.user_message
    assert fake.calls == []


def test_crypto_sync_refuses_a_non_chf_budget(data_dir, fake):
    fake.add_budget("b-eur", currency="EUR")
    fake.add_account("b-eur", "acct-eur", "Revolut")
    fake.add_account("b-eur", CRYPTO, "Crypto", type="otherAsset", on_budget=False)
    ctx = make_ctx(
        data_dir, "acct-eur", budget_id="b-eur", crypto_account_id=CRYPTO, btc_xpub=BIP84_RECEIVE_0
    )
    with pytest.raises(ConfigError):
        services.crypto_sync(ctx, **_crypto_deps())
    assert _mutations(fake) == []


def test_crypto_zero_address_guard_and_override(crypto_ctx, fake):
    fake.add_transaction(BUDGET, CRYPTO, amount=5_000_000, date="2026-10-01")
    deps = _crypto_deps(sats=0, wei=0)
    deps["blockstream"] = FakeChain({})  # the address was never used
    with pytest.raises(AdjustmentRefused):
        services.crypto_sync(crypto_ctx, eth_address="", **deps)
    assert _mutations(fake) == []
    allowed = services.crypto_sync(crypto_ctx, eth_address="", allow_zero=True, **deps)
    assert allowed.adjustment.delta_milli == -5_000_000


def test_cli_valuation_target_touches_no_local_file(tmp_path, fake):
    fake.add_account(BUDGET, CRYPTO, "Crypto", type="otherAsset", on_budget=False)
    target = ValuationTarget(
        key=str(tmp_path / "cli.db"), ynab_token=FAKE_TOKEN, budget_id=BUDGET, account_id=CRYPTO
    )
    result = services.crypto_sync(target, btc_xpub=BIP84_RECEIVE_0, **_crypto_deps())
    assert result.adjustment.outcome == "created"
    assert list(tmp_path.iterdir()) == []


def test_brokerage_sync_posts_the_nav_difference(tmp_path, fake, monkeypatch):
    fake.add_account(BUDGET, IBKR, "IBKR", type="otherAsset", on_budget=False)
    monkeypatch.setattr(services, "ibkr_client_factory", lambda url: _gateway())
    target = ValuationTarget(key="k", ynab_token=FAKE_TOKEN, budget_id=BUDGET, account_id=IBKR)
    result = services.brokerage_sync(target, today=date(2026, 10, 16))
    assert result.adjustment.import_id == "IBKR:123456780:2026-10-16:1"
    assert fake.tx(result.adjustment.ynab_tx_id)["payee_name"] == "IBKR Portfolio Adjustment"


def test_brokerage_never_posts_a_non_positive_nav(fake):
    fake.add_account(BUDGET, IBKR, "IBKR", type="otherAsset", on_budget=False)
    target = ValuationTarget(key="k", ynab_token=FAKE_TOKEN, budget_id=BUDGET, account_id=IBKR)
    with pytest.raises(AdjustmentRefused):
        services.brokerage_sync(target, ibkr=_gateway("0"))
    assert _mutations(fake) == []


def test_valuation_target_needs_every_field():
    with pytest.raises(ConfigError):
        ValuationTarget(key="k", ynab_token="t", budget_id="b", account_id=" ")


# ── locking ─────────────────────────────────────────────────────────────────


def _operations(ctx: UserContext, scans: DedupeScans) -> dict[str, Callable[[], Any]]:
    target = ValuationTarget(key=ctx.key, ynab_token=FAKE_TOKEN, budget_id=BUDGET, account_id=IBKR)
    return {
        "reconcile_preview": lambda: services.reconcile_preview(ctx),
        "reconcile_apply": lambda: services.reconcile_apply(ctx, "token"),
        "dedupe_scan": lambda: services.dedupe_scan(ctx, registry=scans),
        "dedupe_delete": lambda: services.dedupe_delete(ctx, "scan", ["x"], registry=scans),
        "cleanup_pending": lambda: services.cleanup_pending(ctx),
        "crypto_sync": lambda: services.crypto_sync(
            ctx, btc_xpub=BIP84_RECEIVE_0, **_crypto_deps()
        ),
        "brokerage_sync": lambda: services.brokerage_sync(target, ibkr=_gateway()),
    }


@pytest.mark.parametrize(
    "operation",
    [
        "reconcile_preview",
        "reconcile_apply",
        "dedupe_scan",
        "dedupe_delete",
        "cleanup_pending",
        "crypto_sync",
        "brokerage_sync",
    ],
)
def test_F16_every_operation_is_refused_while_another_one_runs(data_dir, fake, scans, operation):
    ctx = make_ctx(data_dir, crypto_account_id=CRYPTO)
    with LOCKS.operation(ctx.key, "import"):
        with pytest.raises(Busy) as err:
            _operations(ctx, scans)[operation]()
        assert err.value.operation == "import"
    assert fake.calls == []
    assert LOCKS.current(ctx.key) is None


def test_F82_no_database_transaction_is_open_during_any_ops_request(
    data_dir, fake, monkeypatch, tokens, scans
):
    fake.add_account(BUDGET, CRYPTO, "Crypto", type="otherAsset", on_budget=False)
    ctx = make_ctx(data_dir, crypto_account_id=CRYPTO, btc_xpub=BIP84_RECEIVE_0)
    connections: list[sqlite3.Connection] = []
    real_connect = sqlite3.connect

    def tracking_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        conn = real_connect(*args, **kwargs)
        connections.append(conn)
        return conn

    def open_transaction() -> bool:
        busy = False
        for conn in connections:
            try:
                busy = busy or conn.in_transaction
            except sqlite3.ProgrammingError:  # closed after its operation
                continue
        return busy

    seen: list[tuple[str, bool]] = []

    def guarded(method: str, url: str, headers: Any, body: Any, timeout: float) -> Any:
        seen.append((method, open_transaction()))
        return fake.transport(method, url, headers, body, timeout)

    monkeypatch.setattr(sqlite3, "connect", tracking_connect)
    monkeypatch.setattr(
        services, "client_factory", lambda c: fake.client(max_retries=0, transport=guarded)
    )
    _seed_opening_balance(fake, 995_000)
    _import(ctx)
    preview = services.reconcile_preview(ctx, now=TODAY, tokens=tokens)
    services.reconcile_apply(ctx, preview.token, now=TODAY, tokens=tokens)
    stray = fake.add_transaction(
        BUDGET, ACCT, amount=-11_200, date="2026-10-10", import_id="YNAB:-11200:2026-10-10:2"
    )
    scan = services.dedupe_scan(ctx, registry=scans)
    services.dedupe_delete(ctx, scan.scan_id, [stray], registry=scans)
    fake.add_transaction(
        BUDGET, ACCT, amount=-4_500, date="2026-10-01", memo="(pending)", cleared="cleared"
    )
    services.cleanup_pending(ctx)
    services.crypto_sync(ctx, **_crypto_deps())

    assert {"GET", "POST", "PATCH", "DELETE"} <= {method for method, _ in seen}
    assert [s for s in seen if s[1]] == []


# ── listings ────────────────────────────────────────────────────────────────


def test_list_budgets_and_open_accounts(fake):
    fake.add_account(BUDGET, "acct-closed", "Old card", closed=True)
    fake.add_account(BUDGET, "acct-deleted", "Gone", deleted=True)
    budgets = services.list_budgets(FAKE_TOKEN)
    assert [(b.id, b.currency) for b in budgets] == [(BUDGET, "CHF")]
    accounts = services.list_accounts(FAKE_TOKEN, BUDGET)
    assert {a.id for a in accounts} == {ACCT, "acct-other"}
    with_closed = services.list_accounts(FAKE_TOKEN, BUDGET, include_closed=True)
    assert "acct-closed" in {a.id for a in with_closed}
    assert "acct-deleted" not in {a.id for a in with_closed}


def test_listings_need_a_token_and_budget():
    with pytest.raises(ConfigError):
        services.list_budgets(" ")
    with pytest.raises(ConfigError):
        services.list_accounts(FAKE_TOKEN, "")


# ── database statistics ─────────────────────────────────────────────────────


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_database_stats_of_a_v2_db(ctx, fake):
    _import(ctx)
    services.sync(ctx, force=True)
    stats = services.database_stats(ctx.db_path)
    assert stats.exists and stats.migrated and stats.schema_version >= 1
    assert stats.path == ctx.db_path
    assert sum(stats.ledger[ACCT].values()) == 6 and stats.ledger_total == 6
    (budget,) = stats.budgets
    assert (budget.budget_id, budget.currency) == (BUDGET, "CHF")
    assert budget.transactions == len(fake.transactions(BUDGET))
    assert stats.statements == 1 and stats.latest_statement is not None
    assert stats.to_dict()["ledger"][ACCT] == stats.ledger[ACCT]


def test_C07_database_stats_of_a_v1_db_change_nothing(tmp_path):
    path = tmp_path / "legacy.db"
    conn = v1.init_db(str(path))
    conn.execute(
        "INSERT INTO transactions (import_id, date, amount, imported_at, source) "
        "VALUES ('YNAB:-100:2026-10-01:1', '2026-10-01', -100, 'x', 'revolut')"
    )
    conn.commit()
    conn.close()
    before = _digest(path)
    stats = services.database_stats(path)
    assert stats.exists and not stats.migrated and stats.legacy_rows == 1
    assert _digest(path) == before
    # No migration and no backup (SQLite's WAL companion files may appear).
    assert {p.name for p in tmp_path.iterdir()} <= {"legacy.db", "legacy.db-wal", "legacy.db-shm"}
    conn = connect(path)
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
        assert (
            conn.execute("SELECT name FROM sqlite_master WHERE name = 'import_ledger'").fetchone()
            is None
        )
    finally:
        conn.close()


def test_database_stats_of_a_missing_or_foreign_file(tmp_path):
    assert services.database_stats(tmp_path / "none.db").exists is False
    junk = tmp_path / "junk.db"
    junk.write_bytes(b"not a database at all" * 100)
    with pytest.raises(YnabHelperError):
        services.database_stats(junk)


@pytest.mark.parametrize(("given", "expected"), [(None, "env-key-123"), ("", None), ("k", "k")])
def test_the_ankr_key_defaults_to_the_environment(crypto_ctx, fake, monkeypatch, given, expected):
    from ynab_helper.balance.crypto import eth

    keys: list[str | None] = []

    class RecordingRpc(FakeEth):
        def __init__(self, *args: Any, ankr_api_key: str | None = None, **kwargs: Any) -> None:
            super().__init__(0)
            keys.append(ankr_api_key)

    monkeypatch.setenv("ANKR_API_KEY", "env-key-123")
    monkeypatch.setattr(eth, "EthRpc", RecordingRpc)
    deps = _crypto_deps()
    del deps["eth_rpc"]
    services.crypto_sync(crypto_ctx, dry_run=True, ankr_api_key=given, **deps)
    assert keys == [expected]


# ── legacy rows: only writes into the account claim them (F74) ─────────────


def _v1_db_with_an_unclaimed_row(ctx: UserContext, fake: FakeYnab) -> str:
    """A v1 database never opened by v2, holding one imported row without an account."""
    topup = fake.add_transaction(
        BUDGET,
        ACCT,
        amount=300_000,
        date="2026-10-01",
        payee_name="Top-Up",
        import_id="YNAB:300000:2026-10-01:1",
        cleared="cleared",
    )
    ctx.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = v1.init_db(str(ctx.db_path))
    conn.execute(
        "INSERT INTO transactions (import_id, date, amount, payee_name, cleared, state, "
        "ynab_tx_id, account_id, source, imported_at) VALUES ('YNAB:300000:2026-10-01:1', "
        "'2026-10-01', 300000, 'Top-Up', 'cleared', 'COMPLETED', ?, NULL, 'revolut', 'x')",
        (topup,),
    )
    conn.commit()
    conn.close()
    return topup


def _ledger_owners(ctx: UserContext) -> list[tuple[str, str]]:
    conn = connect(ctx.db_path)
    try:
        return [tuple(r) for r in conn.execute("SELECT account_id, key FROM import_ledger")]
    finally:
        conn.close()


_READ_ONLY: dict[str, Callable[[UserContext], Any]] = {
    "sync": lambda c: services.sync(c, force=True),
    "account_status": lambda c: services.account_status(c),
    "reconcile_preview": lambda c: services.reconcile_preview(c, tokens=ReconcileTokens()),
    "dedupe_scan": lambda c: services.dedupe_scan(c, registry=DedupeScans()),
    "cleanup_dry_run": lambda c: services.cleanup_pending(c, dry_run=True),
}


@pytest.mark.parametrize("operation", list(_READ_ONLY))
def test_F74_read_only_operations_opening_a_v1_db_leave_its_rows_unclaimed(ctx, fake, operation):
    topup = _v1_db_with_an_unclaimed_row(ctx, fake)
    try:
        _READ_ONLY[operation](ctx)
    except StatementUnavailable:  # reconcile and dedupe need an imported statement first
        pass
    assert _ledger_owners(ctx) == [("", "YNAB:300000:2026-10-01:1")]  # migrated, unclaimed
    assert _mutations(fake) == []

    # The next real import claims the row for the account and does not create it again.
    result = services.import_csv(
        ctx,
        csv_bytes([("2026-10-01 09:00:00", "Top-Up", "300.00", "COMPLETED", "Topup")]),
        original_name="s.csv",
        source_kind="telegram",
    )
    assert result.ok and result.created == [] and result.unchanged == 1
    assert _ledger_owners(ctx) == [(ACCT, "YNAB:300000:2026-10-01:1")]
    assert [t["id"] for t in fake.transactions(BUDGET, account_id=ACCT)] == [topup]


# ── an account YNAB no longer offers (dedupe, cleanup) ─────────────────────


@pytest.mark.parametrize("change", ["closed", "deleted"])
def test_dedupe_and_cleanup_refuse_an_account_ynab_no_longer_offers(ctx, fake, scans, change):
    _import(ctx)
    stray = fake.add_transaction(
        BUDGET, ACCT, amount=-11_200, date="2026-10-10", import_id="YNAB:-11200:2026-10-10:2"
    )
    fake.add_transaction(
        BUDGET, ACCT, amount=-4_500, date="2026-10-01", memo="(pending)", cleared="cleared"
    )
    scan = services.dedupe_scan(ctx, registry=scans)
    assert [c.id for c in scan.candidates] == [stray]
    fake.edit_account(BUDGET, ACCT, **{change: True})
    fake.reset_log()

    for operation in (
        lambda: services.dedupe_delete(ctx, scan.scan_id, [stray], registry=scans),
        lambda: services.dedupe_scan(ctx, registry=scans),
        lambda: services.cleanup_pending(ctx),
        lambda: services.cleanup_pending(ctx, dry_run=True),
    ):
        with pytest.raises(ConfigError) as err:
            operation()
        message = err.value.user_message
        assert f"{change} in YNAB" in message and "Nothing was changed." in message
        assert "/setup" in message
    assert _mutations(fake) == []
    assert fake.tx(stray)["deleted"] is False


def test_cleanup_refuses_an_account_of_another_budget(data_dir, fake):
    fake.add_budget("b2", currency="CHF")
    fake.add_account("b2", "acct-b2", "Revolut")
    ctx = make_ctx(data_dir, "acct-b2")  # budget b1, but the account lives in b2
    with pytest.raises(ConfigError) as err:
        services.cleanup_pending(ctx)
    assert "is not in your YNAB budget" in err.value.user_message
    assert _mutations(fake) == []


# ── reconcile_apply: the mirror is marked stale only when YNAB may have changed ──


def test_a_refused_reconcile_apply_keeps_the_mirror_fresh(ctx, fake, tokens):
    _seed_opening_balance(fake, 995_000)
    _import(ctx)
    with pytest.raises(StaleStateError):
        services.reconcile_apply(ctx, "unknown-token", now=TODAY, tokens=tokens)
    assert _meta(ctx, f"last_sync_at:{BUDGET}") is not None  # nothing was posted
    fake.reset_log()
    services.account_status(ctx)  # within the throttle: answered from the mirror
    assert fake.calls == []


def test_a_failed_reconcile_post_marks_the_mirror_stale(ctx, fake, tokens):
    _seed_opening_balance(fake, 995_000)
    _import(ctx)
    preview = services.reconcile_preview(ctx, now=TODAY, tokens=tokens)
    fake.fail_next("POST", TX_PATH, 500)
    with pytest.raises(YnabError):
        services.reconcile_apply(ctx, preview.token, now=TODAY, tokens=tokens)
    assert _meta(ctx, f"last_sync_at:{BUDGET}") is None  # the POST may have reached YNAB
