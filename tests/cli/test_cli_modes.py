"""The CLI's other modes: --db-stats/--sync (C07), listings, cleanup, reconcile, crypto, IBKR."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FakeYnab
from tests.legacy import v1_revolut_to_ynab as v1
from ynab_helper import cli, services
from ynab_helper.balance.crypto import btc, eth
from ynab_helper.balance.crypto import sync as crypto_sync_module
from ynab_helper.balance.ibkr import IbkrClient

from .conftest import ACCT, BUDGET, CRYPTO, IBKR, PENDING_A, Cli, live, mutations

BIP84_RECEIVE_0 = "bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu"


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ── --db-stats / --sync honour --db-path (C07) ──────────────────────────────


def test_C07_db_stats_reads_and_names_the_given_database(run_cli: Cli, tmp_path: Path):
    custom = tmp_path / "custom.db"
    assert run_cli.run(str(run_cli.statement()), "--db-path", str(custom))[0] == cli.EXIT_OK
    del run_cli.env["YNAB_TOKEN"]  # --db-stats needs no token (v1 precedence)
    code, out, _err = run_cli.run("--db-stats", "--db-path", str(custom))
    assert code == cli.EXIT_OK
    assert out.splitlines()[0] == f"Database: {custom}"
    assert "Import ledger: 6 statement row(s)" in out and f"account {ACCT}" in out
    assert "Statements: 1; newest: export_pending_then_completed_a.csv (imported" in out
    assert not run_cli.db.exists()  # the default database was never touched


def test_C07_db_stats_of_a_v1_database_does_not_migrate_it(run_cli: Cli, tmp_path: Path):
    legacy = tmp_path / "v1.db"
    conn = v1.init_db(str(legacy))
    conn.execute(
        "INSERT INTO transactions (import_id, date, amount, imported_at) "
        "VALUES ('YNAB:-1:2026-10-01:1', '2026-10-01', -1, 'x')"
    )
    conn.commit()
    conn.close()
    before = _digest(legacy)
    code, out, _err = run_cli.run("--db-stats", "--db-path", str(legacy))
    assert code == cli.EXIT_OK
    assert f"Database: {legacy}" in out and "Schema: v1" in out and "1 row(s)" in out
    assert _digest(legacy) == before
    assert not any(p.name.endswith("_backups") for p in tmp_path.iterdir())


def test_db_stats_of_a_missing_database_is_a_usage_error(run_cli: Cli, tmp_path: Path):
    code, _out, err = run_cli.run("--db-stats", "--db-path", str(tmp_path / "typo.db"))
    assert code == cli.EXIT_USAGE and "no database at" in err
    assert not (tmp_path / "typo.db").exists()


def test_C07_sync_writes_into_and_reports_the_given_database(
    run_cli: Cli, tmp_path: Path, fake: FakeYnab
):
    fake.add_transaction(BUDGET, ACCT, amount=-1_000, date="2026-10-01", payee_name="Coffee")
    custom = tmp_path / "custom.db"
    code, out, _err = run_cli.run("--sync", "--db-path", str(custom))
    assert code == cli.EXIT_OK
    assert f"Syncing with YNAB into {custom}" in out and f"Database: {custom}" in out
    assert "Synced budget b1" in out and "1 transaction(s)" in out
    assert services.database_stats(custom).budgets[0].transactions == 1
    assert not run_cli.db.exists()


def test_a_rate_limited_sync_exits_two(run_cli: Cli, fake: FakeYnab):
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 429, retry_after=600)
    code, out, _err = run_cli.run("--sync")
    assert code == cli.EXIT_FAILED and "rate limit" in out and "10 minutes" in out


# ── listings ────────────────────────────────────────────────────────────────


def test_list_budgets_and_accounts(run_cli: Cli, fake: FakeYnab):
    fake.add_account(BUDGET, "acct-closed", "Old card", closed=True)
    code, out, _err = run_cli.run("--list-budgets")
    assert code == cli.EXIT_OK
    assert "Found 1 budget(s)" in out and "ID:       b1" in out and "Currency: CHF" in out
    code, out, _err = run_cli.run("--list-accounts")
    assert code == cli.EXIT_OK
    assert f"ID:      {ACCT}" in out and "Old card" not in out
    assert not run_cli.db.exists()  # listings touch no local state


# ── cleanup ─────────────────────────────────────────────────────────────────


def test_cleanup_pending_memos(run_cli: Cli, fake: FakeYnab):
    tx = fake.add_transaction(
        BUDGET,
        ACCT,
        amount=-4_500,
        date="2026-10-01",
        payee_name="Cafe",
        memo="Card | (pending)",
        cleared="reconciled",
    )
    code, out, err = run_cli.run("--cleanup-pending-memos", "--dry-run", "old.csv")
    assert code == cli.EXIT_OK and "Cafe: 'Card | (pending)' → 'Card'" in out
    assert "Dry run" in out and "the CSV file argument is ignored" in err
    assert mutations(fake) == []

    code, out, _err = run_cli.run("--cleanup-pending-memos")
    assert code == cli.EXIT_OK and "✓ Updated 1 memo(s)." in out
    assert fake.tx(tx)["memo"] == "Card" and fake.tx(tx)["cleared"] == "reconciled"


# ── reconcile ───────────────────────────────────────────────────────────────


@pytest.fixture
def imported(run_cli: Cli, fake: FakeYnab) -> Path:
    """PENDING_A imported on top of a reconciled opening balance 5.00 below Revolut's."""
    fake.add_transaction(
        BUDGET, ACCT, amount=995_000, date="2026-09-30", payee_name="Opening", cleared="reconciled"
    )
    statement = run_cli.statement()
    assert run_cli.run(str(statement))[0] == cli.EXIT_OK
    fake.reset_log()
    return statement


def _recon(fake: FakeYnab) -> list[dict[str, Any]]:
    return [t for t in live(fake) if (t.get("import_id") or "").startswith("RECON:")]


def test_reconcile_dry_run_shows_the_preview_and_posts_nothing(run_cli: Cli, imported, fake):
    code, out, _err = run_cli.run("--reconcile", "--dry-run")
    assert code == cli.EXIT_OK
    assert "Statement: export_pending_then_completed_a.csv (balance as of 2026-10-12)" in out
    assert "1,282.00 CHF" in out and "+5.00 CHF" in out and "Dry run: nothing was posted." in out
    assert mutations(fake) == []


def test_reconcile_asks_and_posts_nothing_when_declined(run_cli: Cli, imported, fake):
    code, out, err = run_cli.run("--reconcile", answers=["n"])
    assert code == cli.EXIT_USAGE and "Nothing was posted." in err
    assert "Post a 'CSV Reconciliation' adjustment of +5.00 CHF dated 2026-10-12? [y/N]" in out
    assert mutations(fake) == []
    code, _out, _err = run_cli.run("--reconcile", answers=[""])  # the default is "no"
    assert code == cli.EXIT_USAGE and mutations(fake) == []


def test_reconcile_posts_after_yes_and_is_in_sync_afterwards(run_cli: Cli, imported, fake):
    code, out, _err = run_cli.run("--reconcile", answers=["y"])
    assert code == cli.EXIT_OK and "✓ Posted 'CSV Reconciliation' +5.00 CHF" in out
    (adjustment,) = _recon(fake)
    assert (adjustment["import_id"], adjustment["cleared"]) == (
        "RECON:5000:2026-10-12:1",
        "reconciled",
    )
    code, out, _err = run_cli.run("--reconcile", "-y")
    assert code == cli.EXIT_OK and "Nothing to post" in out
    assert len(_recon(fake)) == 1


def test_reconcile_with_the_imported_file_is_accepted(run_cli: Cli, imported, fake):
    code, _out, _err = run_cli.run("--reconcile", str(imported), "-y")
    assert code == cli.EXIT_OK and len(_recon(fake)) == 1


def test_F04_reconcile_refuses_a_statement_that_was_not_imported(run_cli: Cli, imported, fake):
    other = run_cli.statement("export_pending_then_completed_b.csv")
    code, _out, err = run_cli.run("--reconcile", str(other), "-y")
    assert code == cli.EXIT_USAGE and "is not the newest imported statement" in err
    assert f"Import it first: revolut_to_ynab.py {other}" in err
    assert mutations(fake) == []


def test_reconcile_without_any_imported_statement(run_cli: Cli, fake: FakeYnab):
    code, _out, err = run_cli.run("--reconcile", "-y")
    assert code == cli.EXIT_FAILED and "Import the statement first" in err
    assert mutations(fake) == []


def test_reconcile_ignores_csv_dir(run_cli: Cli, imported):
    _code, _out, err = run_cli.run("--reconcile", "--dry-run", "--csv-dir", "/tmp")
    assert "--csv-dir is ignored with --reconcile: reconcile uses the newest imported" in err


# ── crypto / IBKR ───────────────────────────────────────────────────────────


class _Chain:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def address_stats(self, address: str) -> tuple[int, int]:
        return (50_000_000, 2) if address == BIP84_RECEIVE_0 else (0, 0)


class _Eth:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def get_balance(self, address: str) -> int:
        return 0

    def erc20_balance(self, address: str, contract: str) -> int:
        return 0


@pytest.fixture
def wallets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(btc, "BlockstreamClient", _Chain)
    monkeypatch.setattr(eth, "EthRpc", _Eth)

    def prices(symbols: Iterable[str]) -> dict[str, Decimal]:
        return {s: Decimal("80000") for s in symbols if s == "BTC"}

    monkeypatch.setattr(crypto_sync_module, "fetch_prices_chf", prices)


def test_crypto_sync_needs_no_revolut_account_and_no_database(
    run_cli: Cli, fake: FakeYnab, wallets
):
    del run_cli.env["YNAB_ACCOUNT_ID"]  # v1: only budget, tracking account and a wallet
    fake.add_transaction(BUDGET, CRYPTO, amount=30_000_000, date="2026-10-01")
    code, out, _err = run_cli.run(
        "--crypto-sync", "--crypto-account-id", CRYPTO, "--btc-xpub", BIP84_RECEIVE_0
    )
    assert code == cli.EXIT_OK
    assert "BTC" in out and "40,000.00 CHF" in out and "+10,000.00 CHF" in out
    assert "✓ Posted 'Crypto Portfolio Adjustment' +10,000.00 CHF" in out
    assert [m for m, _ in mutations(fake)] == ["POST"]
    assert not run_cli.db.exists()


def test_crypto_sync_dry_run(run_cli: Cli, fake: FakeYnab, wallets):
    run_cli.env["YNAB_CRYPTO_ACCOUNT_ID"] = CRYPTO
    run_cli.env["CRYPTO_BTC_XPUB"] = BIP84_RECEIVE_0
    code, out, _err = run_cli.run("--crypto-sync", "--dry-run")
    assert code == cli.EXIT_OK and "Dry run: would post" in out
    assert mutations(fake) == []


def test_crypto_sync_refusal_exits_two(run_cli: Cli, fake: FakeYnab, monkeypatch):
    class Unused(_Chain):
        def address_stats(self, address: str) -> tuple[int, int]:
            return (0, 0)

    monkeypatch.setattr(btc, "BlockstreamClient", Unused)
    fake.add_transaction(BUDGET, CRYPTO, amount=30_000_000, date="2026-10-01")
    code, _out, err = run_cli.run(
        "--crypto-sync", "--crypto-account-id", CRYPTO, "--btc-xpub", BIP84_RECEIVE_0
    )
    assert code == cli.EXIT_FAILED and "Nothing was posted" in err
    assert mutations(fake) == []
    code, out, _err = run_cli.run(
        "--crypto-sync",
        "--crypto-account-id",
        CRYPTO,
        "--btc-xpub",
        BIP84_RECEIVE_0,
        "--allow-zero",
    )
    assert code == cli.EXIT_OK and "-30,000.00 CHF" in out


def test_an_invalid_bitcoin_key_is_a_configuration_error(run_cli: Cli, fake: FakeYnab, wallets):
    code, _out, err = run_cli.run(
        "--crypto-sync", "--crypto-account-id", CRYPTO, "--btc-xpub", "xpubTYPO"
    )
    assert code == cli.EXIT_USAGE and "Bitcoin" in err and "xpubTYPO" not in err


@pytest.fixture
def gateway(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """A fake IBKR gateway (account U7, NAV 2,500.50 CHF); returns the URLs requested."""
    seen: list[str] = []

    def request(method: str, url: str, **kwargs: Any) -> Any:
        seen.append(url)
        if url.endswith("/portfolio/accounts"):
            return [{"id": "U7", "accountId": "U7", "currency": "CHF"}]
        return {"BASE": {"netliquidationvalue": Decimal("2500.50")}}

    monkeypatch.setattr(
        services, "ibkr_client_factory", lambda url: IbkrClient(url, request=request)
    )
    return seen


def test_brokerage_sync(run_cli: Cli, fake: FakeYnab, gateway: list[str]):
    seen = gateway
    code, out, _err = run_cli.run(
        "--brokerage-sync",
        "--brokerage-account-id",
        IBKR,
        "--ibkr-base-url",
        "https://localhost:5055/",
    )
    assert code == cli.EXIT_OK
    assert "IBKR account U7:" in out and "2,500.50 CHF" in out
    assert seen[0] == "https://localhost:5055/v1/api/portfolio/accounts"
    tx = next(t for t in live(fake, IBKR))
    assert tx["payee_name"] == "IBKR Portfolio Adjustment" and tx["amount"] == 2_500_500


def test_brokerage_sync_dry_run_posts_nothing(run_cli: Cli, fake: FakeYnab, gateway: list[str]):
    code, out, _err = run_cli.run("--brokerage-sync", "--brokerage-account-id", IBKR, "--dry-run")
    assert code == cli.EXIT_OK
    assert "IBKR account U7:" in out and "+2,500.50 CHF" in out
    assert "Dry run: would post 'IBKR Portfolio Adjustment' +2,500.50 CHF" in out
    assert mutations(fake) == [] and live(fake, IBKR) == []
    assert not run_cli.db.exists()  # the valuation syncs touch no local database


def test_brokerage_sync_with_a_bad_gateway_url_is_a_configuration_error(run_cli: Cli, fake):
    code, _out, err = run_cli.run(
        "--brokerage-sync", "--brokerage-account-id", IBKR, "--ibkr-base-url", "ftp://gateway"
    )
    assert code == cli.EXIT_USAGE and "IBKR_BASE_URL" in err
    assert fake.calls == []


def test_watch_folder_must_exist(run_cli: Cli, tmp_path: Path, fake: FakeYnab):
    code, _out, err = run_cli.run("--watch", str(tmp_path / "missing"))
    assert code == cli.EXIT_USAGE and "is not a directory" in err
    assert fake.calls == []


def test_import_of_the_fixture_used_above_is_complete(run_cli: Cli, fake: FakeYnab):
    # Sanity for the reconcile fixture: PENDING_A's last completed balance is 1,282.00.
    assert run_cli.run(str(run_cli.statement(PENDING_A)))[0] == cli.EXIT_OK
    assert sum(t["amount"] for t in live(fake) if t["cleared"] != "uncleared") == 282_000
