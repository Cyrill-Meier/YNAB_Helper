"""``services.import_csv`` and friends (docs/DESIGN.md §7) end to end against the fake YNAB."""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FAKE_TOKEN, FakeYnab
from tests.legacy import v1_revolut_to_ynab as v1
from ynab_helper import services
from ynab_helper.errors import (
    Busy,
    ConfigError,
    CsvError,
    YnabAuthError,
    YnabHelperError,
    YnabRateLimited,
)
from ynab_helper.importer import ImportResult
from ynab_helper.locks import LOCKS
from ynab_helper.revolut.files import list_stored
from ynab_helper.services import UserContext
from ynab_helper.storage import CsvFileStore, LedgerStore, connect, open_tx_db
from ynab_helper.storage.users import User

from .conftest import ACCT, BUDGET, OTHER, db_rows, fixture_bytes, make_ctx

TX_PATH = rf"/budgets/{BUDGET}/transactions"
PENDING_A = "export_pending_then_completed_a.csv"
PENDING_B = "export_pending_then_completed_b.csv"


def _import(ctx: UserContext, name: str, **kwargs: Any) -> ImportResult:
    kwargs.setdefault("source_kind", "telegram")
    kwargs.setdefault("original_name", name)
    return services.import_csv(ctx, fixture_bytes(name), **kwargs)


def _statements(ctx: UserContext) -> list[Any]:
    conn = connect(ctx.db_path)
    try:
        return CsvFileStore(conn).recent(limit=100)[::-1]
    finally:
        conn.close()


def _mutations(fake: FakeYnab) -> list[tuple[str, str]]:
    return [(c.method, c.path) for c in fake.calls if c.method != "GET"]


def _live(fake: FakeYnab, account_id: str) -> list[tuple[str, int, str]]:
    return sorted(
        (t["date"], t["amount"], t["cleared"])
        for t in fake.transactions(BUDGET, account_id=account_id)
    )


# ── UserContext ─────────────────────────────────────────────────────────────


def test_user_context_from_a_user_row_follows_the_v1_layout(data_dir):
    user = User(
        telegram_id=42,
        chat_id=42,
        username="u",
        first_name="U",
        ynab_token=" secret-token ",
        budget_id=BUDGET,
        budget_name="B",
        account_id=ACCT,
        account_name="Revolut",
        crypto_account_id="crypto",
        crypto_account_name="Crypto",
        btc_xpub="xpub123",
        eth_address=None,
        state="ready",
        temp_data=None,
        created_at="x",
        updated_at="x",
        auto_approve=False,
    )
    ctx = UserContext.from_user_row(user, data_dir)
    assert ctx.key == "42" and ctx.telegram_id == 42
    assert ctx.ynab_token == "secret-token"
    assert ctx.db_path == data_dir / "transactions_42.db"
    assert ctx.csv_dir == data_dir / "csv_cache" / "42"
    assert ctx.backups == data_dir / "backups"
    assert ctx.auto_approve is False and ctx.crypto_account_id == "crypto"
    assert "secret-token" not in repr(ctx) and "xpub123" not in repr(ctx)


def test_user_context_accepts_mappings_and_sqlite_rows(data_dir):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT 7 AS telegram_id, 'tok' AS ynab_token, 'b' AS budget_id, 'a' AS account_id, "
        "1 AS auto_approve"
    ).fetchone()
    ctx = UserContext.from_user_row(row, data_dir)
    assert (ctx.key, ctx.budget_id, ctx.account_id, ctx.auto_approve) == ("7", "b", "a", True)
    assert ctx.btc_xpub is None
    mapping = UserContext.from_user_row(
        {"telegram_id": "8", "ynab_token": "t", "budget_id": "b", "account_id": "a"}, data_dir
    )
    assert mapping.key == "8" and mapping.auto_approve is True


@pytest.mark.parametrize("missing", ["ynab_token", "budget_id", "account_id"])
def test_user_context_needs_a_complete_setup(data_dir, missing):
    row = {"telegram_id": 1, "ynab_token": "t", "budget_id": "b", "account_id": "a"}
    row[missing] = "  "
    with pytest.raises(ConfigError) as err:
        UserContext.from_user_row(row, data_dir)
    assert "/setup" in err.value.user_message


def test_user_context_for_the_cli_locks_on_the_db_path(tmp_path):
    ctx = UserContext.from_cli(token="t", budget_id="b", account_id="a", db_path=tmp_path / "x.db")
    assert ctx.key == str(tmp_path / "x.db") and ctx.csv_dir is None
    assert ctx.backups == tmp_path / "x_backups"  # specific to this DB, never a generic dir
    default = UserContext.from_cli(
        token="t", budget_id="b", account_id="a", db_path="~/.revolut_to_ynab.db"
    )
    assert default.backups == Path.home() / ".revolut_to_ynab_backups"  # not ~/backups
    explicit = UserContext.from_cli(
        token="t", budget_id="b", account_id="a", db_path=tmp_path / "x.db", backup_dir=tmp_path
    )
    assert explicit.backups == tmp_path


def test_an_incomplete_cli_context_does_not_point_to_the_bot(tmp_path):
    with pytest.raises(ConfigError) as err:
        UserContext.from_cli(token=" ", budget_id="b", account_id="a", db_path=tmp_path / "x.db")
    assert "/setup" not in err.value.user_message
    assert "YNAB_TOKEN" in err.value.user_message


# ── import_csv ──────────────────────────────────────────────────────────────


def test_import_stores_the_upload_and_records_the_statement(ctx, fake):
    result = _import(ctx, PENDING_A, original_name="../../etc/account-statement.csv")

    assert isinstance(result, ImportResult) and result.ok and len(result.created) == 6
    stored = list_stored(ctx.csv_dir)
    assert len(stored) == 1 and stored[0].parent == ctx.csv_dir
    (record,) = _statements(ctx)
    assert record.stored_path == str(stored[0])
    assert record.original_name == "account-statement.csv"  # display metadata only (F19)
    assert (record.status, record.source, record.account_id) == ("imported", "telegram", ACCT)
    assert (record.currency, record.row_count) == ("CHF", 6)
    assert record.window == ("2026-10-01", "2026-10-12")
    assert record.imported_at is not None
    assert record.result == json.loads(json.dumps(result.to_dict()))


def test_import_is_idempotent_and_applies_a_later_statement(ctx, fake):
    _import(ctx, PENDING_A)
    fake.reset_log()
    result = _import(ctx, PENDING_B)
    assert result.ok and len(result.updated) == 2 and len(result.removed) == 1
    fake.reset_log()
    again = _import(ctx, PENDING_B)
    assert again.ok and not again.has_changes and _mutations(fake) == []
    assert [s.status for s in _statements(ctx)] == ["imported"] * 3


def test_F47_a_failed_write_is_in_the_result_and_the_statement_is_partial(ctx, fake):
    _import(ctx, PENDING_A)
    fake.fail_next("PATCH", TX_PATH, 500)
    result = _import(ctx, PENDING_B)
    assert not result.ok and len(result.failures) == 2
    assert _statements(ctx)[-1].status == "partial"
    assert _statements(ctx)[-1].result["failures"]


def test_an_exception_records_the_statement_as_failed_and_is_raised(ctx, fake):
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 401)
    with pytest.raises(YnabAuthError):
        _import(ctx, PENDING_A)
    (record,) = _statements(ctx)
    assert record.status == "failed"
    assert record.result == {"error": record.result["error"], "error_type": "YnabAuthError"}
    assert "token" in record.result["error"]


def test_a_rate_limited_sync_before_planning_raises_and_writes_nothing(ctx, fake):
    fake.fail_next("GET", TX_PATH, 429, retry_after=60)
    with pytest.raises(YnabRateLimited) as err:
        _import(ctx, PENDING_A)
    assert err.value.retry_after == 60
    assert _mutations(fake) == []
    assert _statements(ctx)[-1].status == "failed"


def test_unknown_source_kind_is_refused(ctx):
    with pytest.raises(ValueError, match="source_kind"):
        services.import_csv(ctx, b"", original_name=None, source_kind="email")  # type: ignore[arg-type]


def test_a_statement_without_importable_rows_is_refused(ctx, fake):
    header = (
        b"Type,Product,Started Date,Completed Date,Description,Amount,Fee,Currency,State,Balance\n"
    )
    body = (
        b"Transfer,Savings,2026-10-01 10:00:00,2026-10-01 10:00:00,To pocket,-5,0,CHF,COMPLETED,5\n"
    )
    with pytest.raises(CsvError) as err:
        services.import_csv(ctx, header + body, original_name="x.csv", source_kind="web")
    assert "no transactions" in err.value.user_message
    assert _mutations(fake) == []


def test_the_cli_imports_a_file_in_place(tmp_path, fake):
    statement = tmp_path / "account-statement_2026-10-01_2026-10-12_en_abc.csv"
    statement.write_bytes(fixture_bytes(PENDING_A))
    ctx = UserContext.from_cli(
        token=FAKE_TOKEN, budget_id=BUDGET, account_id=ACCT, db_path=tmp_path / "cli.db"
    )
    result = services.import_csv(ctx, statement, original_name=None, source_kind="cli")
    assert result.ok and len(result.created) == 6
    (record,) = _statements(ctx)
    assert record.stored_path == str(statement.resolve())
    assert record.original_name == statement.name and record.source == "cli"
    assert sorted(p.name for p in tmp_path.iterdir() if p.suffix == ".csv") == [statement.name]


def test_the_newest_imported_statement_survives_retention(ctx, fake):
    _import(ctx, PENDING_A)
    good = Path(_statements(ctx)[0].stored_path)
    for _ in range(6):
        with pytest.raises(CsvError):
            services.import_csv(
                ctx, fixture_bytes("export_eur.csv"), original_name="eur.csv", source_kind="web"
            )
    assert good.exists()  # reconcile is based on it
    assert len(list_stored(ctx.csv_dir)) == 6  # newest five + the protected one


# ── F14: currency ───────────────────────────────────────────────────────────


def test_F14_statement_in_another_currency_is_refused_before_any_write(ctx, fake):
    _import(ctx, PENDING_A)
    ledger_before = db_rows(ctx, "import_ledger")
    fake.reset_log()

    with pytest.raises(CsvError) as err:
        services.import_csv(
            ctx, fixture_bytes("export_eur.csv"), original_name="eur.csv", source_kind="web"
        )

    assert "EUR" in err.value.user_message and "CHF" in err.value.user_message
    assert _mutations(fake) == [] and fake.calls == []  # not even a sync
    assert db_rows(ctx, "import_ledger") == ledger_before
    record = _statements(ctx)[-1]
    assert (record.status, record.currency) == ("failed", "EUR")
    assert record.result["error_type"] == "CsvError"


def test_F14_budget_currency_is_fetched_when_unknown(ctx, fake):
    with pytest.raises(CsvError):
        services.import_csv(
            ctx, fixture_bytes("export_eur.csv"), original_name="eur.csv", source_kind="web"
        )
    settings = fake.calls_matching("GET", rf"/budgets/{BUDGET}/settings")
    assert len(settings) == 1 and _mutations(fake) == []


def test_F14_unknown_budget_currency_refuses_the_import(data_dir):
    fake = FakeYnab()
    fake.add_budget(BUDGET, currency=None)
    fake.add_account(BUDGET, ACCT, "Revolut")
    services.client_factory = lambda ctx: fake.client(max_retries=0)  # restored by monkeypatch
    with pytest.raises(YnabHelperError) as err:
        _import(make_ctx(data_dir), PENDING_A)
    assert "currency" in err.value.user_message
    assert _mutations(fake) == []


def test_F14_mixed_currency_statement_is_not_even_stored(ctx, fake):
    with pytest.raises(CsvError):
        services.import_csv(
            ctx,
            fixture_bytes("export_mixed_currency.csv"),
            original_name="mixed.csv",
            source_kind="web",
        )
    assert list_stored(ctx.csv_dir) == [] and _statements(ctx) == []
    assert fake.calls == []


# ── F15 / F16 ───────────────────────────────────────────────────────────────


def test_F15_results_are_objects_and_nothing_is_printed(ctx, fake, capsys):
    result = _import(ctx, PENDING_A)
    status = services.account_status(ctx)
    synced = services.sync(ctx, force=True)
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""
    assert isinstance(result, ImportResult)
    assert status.balance == fake.balances(BUDGET, ACCT)[0]
    assert synced is not None and synced.ok


def test_F16_a_second_operation_for_the_same_user_is_refused_while_one_runs(
    ctx, fake, data_dir, monkeypatch
):
    entered, release = threading.Event(), threading.Event()

    def gated(method: str, url: str, headers: Any, body: Any, timeout: float) -> Any:
        if method == "POST":
            entered.set()
            assert release.wait(10)
        return fake.transport(method, url, headers, body, timeout)

    monkeypatch.setattr(services, "client_factory", lambda c: fake.client(transport=gated))
    outcome: dict[str, Any] = {}

    def first() -> None:
        try:
            outcome["result"] = _import(ctx, PENDING_A, source_kind="telegram")
        except BaseException as exc:  # pragma: no cover - surfaced by the assertion below
            outcome["error"] = exc

    worker = threading.Thread(target=first)
    worker.start()
    try:
        assert entered.wait(10)
        assert LOCKS.current(ctx.key) == "import"
        with pytest.raises(Busy) as err:
            _import(ctx, PENDING_A, source_kind="web")
        assert err.value.operation == "import"
        with pytest.raises(Busy):
            services.sync(ctx, force=True)
        status = services.account_status(ctx)  # reads the cache instead of waiting
        assert status.sync is None and "Another operation (import)" in status.sync_note
        # Another user is not blocked.
        other = make_ctx(data_dir, telegram_id=43)
        assert services.sync(other, force=True) is not None
    finally:
        release.set()
        worker.join(10)
    assert "error" not in outcome and outcome["result"].ok
    assert LOCKS.current(ctx.key) is None


# ── F12: switching accounts ─────────────────────────────────────────────────


def test_F12_importing_into_a_new_account_never_touches_the_old_one(data_dir, fake):
    old = make_ctx(data_dir, ACCT)
    _import(old, PENDING_A)
    old_live = _live(fake, ACCT)
    old_ledger = db_rows(old, "import_ledger")

    new = make_ctx(data_dir, OTHER)  # /setup switched the account; same DB and cache
    fake.reset_log()
    first = _import(new, PENDING_A)
    second = _import(new, PENDING_B)

    assert first.ok and len(first.created) == 6  # not "already imported"
    assert second.ok and len(second.updated) == 2 and len(second.removed) == 1
    assert _live(fake, ACCT) == old_live
    other_ids = {t["id"] for t in fake.transactions(BUDGET, account_id=OTHER, include_deleted=True)}
    for method, path in _mutations(fake):
        if method in ("PATCH", "PUT", "DELETE") and path != TX_PATH:
            assert path.rsplit("/", 1)[1] in other_ids, (method, path)
    for body in [c.body for c in fake.calls if c.method in ("POST", "PATCH")]:
        for tx in body["transactions"]:
            assert tx.get("account_id", OTHER) == OTHER
            if "id" in tx:
                assert tx["id"] in other_ids
    conn = connect(old.db_path)
    try:
        assert [r for r in db_rows(old, "import_ledger") if r[1] == ACCT] == old_ledger
        assert LedgerStore(conn).counts_by_status(OTHER)["ACTIVE"] == 6
    finally:
        conn.close()


# ── F74: dry run ────────────────────────────────────────────────────────────


def _legacy_db_with_unclaimed_row(ctx: UserContext, fake: FakeYnab) -> str:
    """A v1 DB whose revolut row has no account (claimed by the next real import)."""
    topup = fake.add_transaction(
        BUDGET,
        ACCT,
        amount=300_000,
        date="2026-10-01",
        payee_name="Top-Up by *4242",
        memo="Topup",
        import_id="YNAB:300000:2026-10-01:1",
    )
    ctx.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = v1.init_db(str(ctx.db_path))
    conn.execute(
        "INSERT INTO transactions (import_id, date, amount, payee_name, memo, cleared, state, "
        "ynab_tx_id, account_id, source, imported_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, "
        "'revolut', '2026-10-01T08:00:00')",
        (
            "YNAB:300000:2026-10-01:1",
            "2026-10-01",
            300_000,
            "Top-Up by *4242",
            "Topup",
            "cleared",
            "COMPLETED",
            topup,
        ),
    )
    conn.commit()
    conn.close()
    open_tx_db(ctx.db_path, default_account_id="", backup_dir=ctx.backups).close()
    return topup


def test_F74_dry_run_writes_nothing_locally_and_plans_like_the_real_run(ctx, fake):
    _legacy_db_with_unclaimed_row(ctx, fake)
    before = db_rows(ctx, "import_ledger", "csv_files")
    assert any(row[1] == "" for row in before)  # the unclaimed legacy entry
    fake.reset_log()

    dry = _import(ctx, PENDING_A, dry_run=True)

    assert dry.dry_run and _mutations(fake) == []
    assert db_rows(ctx, "import_ledger", "csv_files") == before  # nothing claimed (F74)
    assert list_stored(ctx.csv_dir) == []  # nothing stored, nothing pruned

    real = _import(ctx, PENDING_A)
    assert real.ok and not real.dry_run

    def view(result: ImportResult) -> dict[str, Any]:
        data = result.to_dict()
        for name in ("created", "linked"):
            data[name] = [{**t, "ynab_tx_id": None} for t in data[name]]
        for name in ("dry_run", "ok"):
            data.pop(name)
        return data

    assert view(dry) == view(real)
    assert len(real.created) == 5 and real.unchanged == 1  # the top-up was already there
    rows = db_rows(ctx, "import_ledger")
    assert all(row[1] == ACCT for row in rows)  # claimed by the real run only


def _v1_db_with_unassigned_rows(db_path: Path, fake: FakeYnab) -> str:
    """An unmigrated v1 DB: a top-up without account that the user deleted in YNAB (v1's web
    sync flagged it deleted=1). Returns the YNAB id of the deleted top-up."""
    topup = fake.add_transaction(
        BUDGET,
        ACCT,
        amount=300_000,
        date="2026-10-01",
        payee_name="Top-Up by *4242",
        memo="Topup",
        import_id="YNAB:300000:2026-10-01:1",
    )
    fake.user_delete_transaction(topup)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = v1.init_db(str(db_path))
    conn.execute(
        "INSERT INTO transactions (import_id, date, amount, payee_name, memo, cleared, state, "
        "ynab_tx_id, account_id, source, imported_at, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, "
        "NULL, 'revolut', '2026-10-01T08:00:00', 1)",
        (
            "YNAB:300000:2026-10-01:1",
            "2026-10-01",
            300_000,
            "Top-Up by *4242",
            "Topup",
            "cleared",
            "COMPLETED",
            topup,
        ),
    )
    conn.commit()
    conn.close()
    return topup


@pytest.mark.parametrize("kind", ["cli", "bot"])
def test_F74_a_dry_run_opening_a_v1_db_first_leaves_the_legacy_rows_unclaimed(
    tmp_path, data_dir, fake, kind
):
    def context(account_id: str) -> UserContext:
        if kind == "bot":
            return make_ctx(data_dir, account_id)
        return UserContext.from_cli(
            token=FAKE_TOKEN, budget_id=BUDGET, account_id=account_id, db_path=tmp_path / "v1.db"
        )

    trial = context(OTHER)  # "--dry-run" against another account, the first v2 run
    _v1_db_with_unassigned_rows(trial.db_path, fake)
    statement = tmp_path / "s.csv"
    statement.write_bytes(fixture_bytes(PENDING_A))
    fake.reset_log()

    dry = services.import_csv(
        trial,
        statement,
        original_name=None,
        source_kind="cli" if kind == "cli" else "web",
        dry_run=True,
    )

    assert dry.dry_run and _mutations(fake) == []
    owners = {row[1] for row in db_rows(trial, "import_ledger")}
    assert owners == {""}  # the dry run did not decide who owns the legacy history (F74)

    real = services.import_csv(
        context(ACCT), statement, original_name=None, source_kind="cli" if kind == "cli" else "web"
    )

    assert real.ok and real.ignored_deleted == 1
    live = [t for t in fake.transactions(BUDGET) if t["amount"] == 300_000]
    assert live == []  # the user-deleted top-up was not re-created
    rows = db_rows(trial, "import_ledger")
    assert {(row[1], row[5]) for row in rows if row[2] == "YNAB:300000:2026-10-01:1"} == {
        (ACCT, "DELETED_IN_YNAB")
    }


@pytest.mark.parametrize("change", ["deleted", "closed", "missing", "other_budget", "v1_row"])
def test_an_account_ynab_no_longer_offers_is_refused_before_any_write(data_dir, fake, change):
    problem = "is not in your YNAB budget"
    ctx = make_ctx(data_dir)
    if change == "deleted":
        fake.edit_account(BUDGET, ACCT, deleted=True)
        problem = "was deleted in YNAB"
    elif change == "closed":
        fake.edit_account(BUDGET, ACCT, closed=True)
        problem = "is closed in YNAB"
    elif change == "missing":
        ctx = make_ctx(data_dir, "acct-gone")
    elif change == "v1_row":  # only v1's web dashboard ever saw it (no budget_id)
        ctx = make_ctx(data_dir, "acct-v1")
        services.sync(ctx, force=True)
        conn = connect(ctx.db_path)
        try:
            conn.execute(
                "INSERT INTO accounts (account_id, name, first_seen_at, created_at, updated_at) "
                "VALUES ('acct-v1', 'Old Revolut', 'x', 'x', 'x')"
            )
        finally:
            conn.close()
    else:  # /setup chose another budget; the mirror still knows the old budget's account
        fake.add_budget("b2", currency="CHF")
        fake.add_account("b2", "acct-b2", "Revolut (old budget)")
        services.sync(make_ctx(data_dir, "acct-b2", budget_id="b2"), force=True)
        ctx = make_ctx(data_dir, "acct-b2")
    fake.reset_log()

    for dry_run in (True, False):
        with pytest.raises(ConfigError) as err:
            _import(ctx, PENDING_A, dry_run=dry_run)
        assert problem in err.value.user_message
        assert "Nothing was imported" in err.value.user_message
        assert "/setup" in err.value.user_message

    assert _mutations(fake) == []  # not one POST (it used to cost 2n - 1 of them, F17)
    record = _statements(ctx)[-1]
    assert (record.status, record.result["error_type"]) == ("failed", "ConfigError")


def test_the_cli_is_told_to_check_its_configuration_for_a_missing_account(tmp_path, fake):
    ctx = UserContext.from_cli(
        token=FAKE_TOKEN, budget_id=BUDGET, account_id="acct-gone", db_path=tmp_path / "cli.db"
    )
    statement = tmp_path / "s.csv"
    statement.write_bytes(fixture_bytes(PENDING_A))
    with pytest.raises(ConfigError) as err:
        services.import_csv(ctx, statement, original_name=None, source_kind="cli")
    assert "/setup" not in err.value.user_message
    assert "YNAB_ACCOUNT_ID" in err.value.user_message
    assert _mutations(fake) == []


def test_an_older_statement_sent_again_is_reported_and_changes_nothing(ctx, fake):
    _import(ctx, PENDING_A)
    _import(ctx, PENDING_B)
    fake.reset_log()
    older = _import(ctx, PENDING_A)
    assert older.ok and _mutations(fake) == []  # no downgrade, no sweep (F36)
    assert any("older than what was already imported" in w for w in older.warnings)
    assert _statements(ctx)[-1].result["warnings"] == older.warnings


# ── sync / status ───────────────────────────────────────────────────────────


def test_sync_is_throttled_unless_forced(ctx, fake):
    first = services.sync(ctx)
    assert first is not None and first.ok and first.full
    assert services.sync(ctx) is None  # within 30 s
    forced = services.sync(ctx, force=True)
    assert forced is not None and not forced.full
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 429)
    limited = services.sync(ctx, force=True)
    assert limited is not None and limited.rate_limited


def test_account_status_reports_balances_statement_and_ledger(ctx, fake):
    _import(ctx, PENDING_A)
    _import(ctx, PENDING_B)
    status = services.account_status(ctx)
    balance, cleared, uncleared = fake.balances(BUDGET, ACCT)
    assert (status.balance, status.cleared_balance, status.uncleared_balance) == (
        balance,
        cleared,
        uncleared,
    )
    assert status.account_name == "Revolut" and status.account_found
    assert status.currency == "CHF" and status.sync_note is None
    assert status.last_statement.status == "imported"
    assert status.last_statement.original_name == PENDING_B
    assert status.ledger_counts["ACTIVE"] == 6 and status.ledger_counts["REVERTED"] == 1
    data = json.loads(json.dumps(status.to_dict()))
    assert data["last_statement"]["window"] == ["2026-10-01", "2026-10-13"]


def test_account_status_falls_back_to_the_cache_when_ynab_fails(ctx, fake):
    _import(ctx, PENDING_A)
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 503)
    status = services.account_status(ctx, min_interval=0)
    assert status.balance is not None
    assert "could not be reached" in status.sync_note
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 401)
    with pytest.raises(YnabAuthError):
        services.account_status(ctx, min_interval=0)
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 429)
    limited = services.account_status(ctx, min_interval=0)
    assert "rate limit" in limited.sync_note and limited.balance is not None
    fake.reset_log()
    again = services.account_status(ctx, min_interval=0)  # backing off: nothing is sent
    assert "rate limit" in again.sync_note and fake.request_count == 0


def test_F17_status_while_rate_limited_sends_no_more_requests(ctx, fake, raw_db):
    services.account_status(ctx)
    # The last successful sync was a while ago, then YNAB starts answering 429.
    raw_db.execute(
        "UPDATE sync_meta SET value = '2026-01-01T00:00:00Z' WHERE key = ?",
        (f"last_sync_at:{BUDGET}",),
    )
    fake.reset_log()
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 429, retry_after=3600, times=100)

    for _ in range(6):  # the dashboard and the bot ask again and again
        status = services.account_status(ctx)
        assert status.sync is not None and status.sync.rate_limited
        assert "rate limit" in status.sync_note and status.balance is not None
    synced = services.sync(ctx)
    assert synced is not None and synced.rate_limited

    assert fake.request_count == 1


def test_F17_status_right_after_a_rate_limited_import_sends_nothing(ctx, fake):
    _import(ctx, PENDING_A)
    fake.fail_next("PATCH", TX_PATH, 429, retry_after=900)
    result = _import(ctx, PENDING_B)
    assert result.rate_limited and _statements(ctx)[-1].status == "partial"
    fake.reset_log()

    status = services.account_status(ctx)  # the bot's balance line after the import

    assert fake.request_count == 0
    assert status.sync is not None and status.sync.rate_limited
    again = _import(ctx, PENDING_B)  # re-sent by the user: the forced sync goes through
    assert again.ok and len(again.updated) == 2 and len(again.removed) == 1


def test_F97_status_before_any_import_syncs_and_creates_the_db(ctx, fake):
    # F97: a user who never imported still gets YNAB's numbers.
    assert not ctx.db_path.exists()
    status = services.account_status(ctx)
    assert status.account_found and status.balance == 0
    assert status.last_statement is None and status.sync is not None and status.sync.ok


def test_import_claims_unassigned_legacy_rows_for_the_account(ctx, fake):
    topup = _legacy_db_with_unclaimed_row(ctx, fake)
    result = _import(ctx, PENDING_A)
    assert result.ok and result.unchanged == 1
    conn = connect(ctx.db_path)
    try:
        entry = LedgerStore(conn).get(ACCT, "YNAB:300000:2026-10-01:1")
        assert entry is not None and entry.ynab_tx_id == topup
        assert LedgerStore(conn).list_unassigned() == []
    finally:
        conn.close()


def test_F82_no_database_transaction_is_open_during_any_service_request(ctx, fake, monkeypatch):
    connections: list[sqlite3.Connection] = []
    real_open = services._open_db

    def tracking_open(c: UserContext, **kwargs: Any) -> sqlite3.Connection:
        conn = real_open(c, **kwargs)
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

    seen: list[tuple[str, str, bool]] = []

    def guarded(method: str, url: str, headers: Any, body: Any, timeout: float) -> Any:
        seen.append((method, url, open_transaction()))
        return fake.transport(method, url, headers, body, timeout)

    monkeypatch.setattr(services, "_open_db", tracking_open)
    monkeypatch.setattr(
        services, "client_factory", lambda c: fake.client(max_retries=0, transport=guarded)
    )
    _import(ctx, PENDING_A, dry_run=True)  # currency fetch, sync, simulated claim + refresh
    _import(ctx, PENDING_A)
    # A duplicate import_id forces the executor's post-create sync.
    fake.add_transaction(
        BUDGET,
        ACCT,
        amount=-11_200,
        date="2026-10-13",
        import_id="YNAB:-11200:2026-10-13:1",
        cleared="uncleared",
    )
    assert _import(ctx, PENDING_B).ok
    services.account_status(ctx, min_interval=0)
    services.sync(ctx, force=True)

    methods = {method for method, _url, _open in seen}
    assert {"GET", "POST", "PATCH", "DELETE"} <= methods
    assert [s for s in seen if s[2]] == []
