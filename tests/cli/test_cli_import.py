"""The CLI's import mode: statement selection, --dry-run (F74), --skip-pending (F09), results."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from tests.fakes.fake_ynab import FakeYnab
from tests.importer.helpers import csv_bytes
from tests.legacy import v1_revolut_to_ynab as v1
from ynab_helper import cli, services
from ynab_helper.storage import connect

from .conftest import ACCT, BUDGET, FIXTURES, PENDING_A, PENDING_B, Cli, live, mutations

TX_PATH = rf"/budgets/{BUDGET}/transactions"


def _rows(db: Path, sql: str) -> list[tuple[object, ...]]:
    conn = connect(db)
    try:
        return [tuple(row) for row in conn.execute(sql)]
    finally:
        conn.close()


# ── importing a file ────────────────────────────────────────────────────────


def test_import_creates_the_statement_and_a_rerun_changes_nothing(run_cli: Cli, fake: FakeYnab):
    statement = run_cli.statement()
    code, out, _err = run_cli.run(str(statement))
    assert code == cli.EXIT_OK
    assert "Reading: export_pending_then_completed_a.csv" in out
    assert "Created: 6" in out and "✓ Import complete." in out
    assert len(live(fake)) == 6
    # The CLI imports the file in place and records it (reconcile is based on it).
    assert _rows(run_cli.db, "SELECT stored_path, source, status FROM csv_files") == [
        (str(statement.resolve()), "cli", "imported")
    ]

    fake.reset_log()
    code, out, _err = run_cli.run(str(statement))
    assert code == cli.EXIT_OK and "Nothing to do" in out
    assert mutations(fake) == []


def test_a_later_statement_reports_updates_and_removals(run_cli: Cli, fake: FakeYnab):
    run_cli.run(str(run_cli.statement()))
    code, out, _err = run_cli.run(str(run_cli.statement(PENDING_B)))
    assert code == cli.EXIT_OK
    assert "Updated: 2" in out and "amount -89.99 → -90.41" in out
    assert "Removed from YNAB: 1" in out and "Hotel Bellevue (hold) (reverted)" in out


def test_F74_dry_run_writes_nothing_to_ynab_or_the_ledger(run_cli: Cli, fake: FakeYnab):
    statement = run_cli.statement()
    code, out, _err = run_cli.run(str(statement), "--dry-run")
    assert code == cli.EXIT_OK
    assert "Would create: 6" in out and "Dry run: nothing was written" in out
    assert mutations(fake) == []
    assert _rows(run_cli.db, "SELECT COUNT(*) FROM import_ledger") == [(0,)]
    assert _rows(run_cli.db, "SELECT COUNT(*) FROM csv_files") == [(0,)]


def test_F74_dry_run_on_a_v1_database_leaves_the_legacy_rows_unclaimed(
    run_cli: Cli, fake: FakeYnab
):
    conn = v1.init_db(str(run_cli.db))
    conn.execute(
        "INSERT INTO transactions (import_id, date, amount, payee_name, cleared, state, "
        "imported_at, source) VALUES ('YNAB:-5000:2026-09-01:1', '2026-09-01', -5000, 'Shop', "
        "'cleared', 'COMPLETED', 'x', 'revolut')"
    )
    conn.commit()
    conn.close()
    code, _out, _err = run_cli.run(str(run_cli.statement()), "--dry-run")
    assert code == cli.EXIT_OK and mutations(fake) == []
    # v1 claimed NULL-account rows for the dry run's account (F74); v2 leaves them unclaimed.
    assert _rows(run_cli.db, "SELECT account_id, key FROM import_ledger") == [
        ("", "YNAB:-5000:2026-09-01:1")
    ]


def test_F09_skip_pending_only_stops_creating_pendings(run_cli: Cli, fake: FakeYnab):
    first = run_cli.downloads / "first.csv"
    first.write_bytes(
        csv_bytes(
            [
                ("2026-10-01 10:00:00", "Shop", "-20.00"),
                ("2026-10-02 14:00:00", "Hotel Hold", "-110.00", "PENDING"),
                ("2026-10-02 18:00:00", "Parking", "-5.00", "PENDING"),
                ("2026-10-03 09:00:00", "Kiosk", "-3.50"),
            ]
        )
    )
    assert run_cli.run(str(first))[0] == cli.EXIT_OK
    hotel = next(t for t in live(fake) if t["payee_name"] == "Hotel Hold")
    assert hotel["cleared"] == "uncleared"

    later = run_cli.downloads / "later.csv"
    later.write_bytes(
        csv_bytes(
            [
                ("2026-10-01 10:00:00", "Shop", "-20.00"),
                ("2026-10-02 14:00:00", "Hotel Hold", "-110.00", "PENDING"),  # still pending
                ("2026-10-02 18:00:00", "Parking", "-5.00", "REVERTED"),
                ("2026-10-03 09:00:00", "Kiosk", "-3.50"),
                ("2026-10-04 08:00:00", "Cafe", "-4.50", "PENDING"),  # new pending
                ("2026-10-04 12:00:00", "Tram", "-2.80"),
            ]
        )
    )
    fake.reset_log()
    code, out, _err = run_cli.run(str(later), "--skip-pending")
    assert code == cli.EXIT_OK
    payees = {t["payee_name"] for t in live(fake)}
    # v1 deleted every earlier pending missing from the filtered list (the hotel) and never
    # removed the reverted parking; v2 keeps the hotel, removes the parking, creates the
    # completed tram and only skips creating the new Cafe pending.
    assert "Hotel Hold" in payees and fake.tx(hotel["id"])["deleted"] is False
    assert "Parking" not in payees and "Tram" in payees and "Cafe" not in payees
    deletes = [path for method, path in mutations(fake) if method == "DELETE"]
    assert len(deletes) == 1 and hotel["id"] not in deletes[0]
    assert "Skipped rows: 1" in out


def test_a_partly_applied_import_exits_two(run_cli: Cli, fake: FakeYnab):
    fake.fail_next("POST", TX_PATH, 500)
    code, out, _err = run_cli.run(str(run_cli.statement()))
    assert code == cli.EXIT_FAILED
    assert "Failed: 6" in out and "only partly applied" in out


def test_a_statement_in_another_currency_is_refused(run_cli: Cli, fake: FakeYnab):
    statement = run_cli.downloads / "eur.csv"
    statement.write_bytes((FIXTURES / "export_eur.csv").read_bytes())
    code, _out, err = run_cli.run(str(statement))
    assert code == cli.EXIT_FAILED and "budget uses CHF" in err
    assert mutations(fake) == []


def test_an_account_deleted_in_ynab_is_a_configuration_error(run_cli: Cli, fake: FakeYnab):
    fake.edit_account(BUDGET, ACCT, deleted=True)
    code, _out, err = run_cli.run(str(run_cli.statement()))
    assert code == cli.EXIT_USAGE and "YNAB_ACCOUNT_ID" in err and "/setup" not in err
    assert mutations(fake) == []


def test_a_missing_file_is_a_usage_error(run_cli: Cli, fake: FakeYnab):
    code, _out, err = run_cli.run(str(run_cli.downloads / "nope.csv"))
    assert code == cli.EXIT_USAGE and "file not found" in err
    assert fake.calls == []


# ── picking the newest statement (v1 confirm_csv_selection) ─────────────────


def _two_statements(run: Cli) -> Path:
    old = run.statement(to="account-statement_2026-09-01_2026-09-30_en_aaaa.csv")
    new = run.statement(to="account-statement_2026-10-01_2026-10-13_en_bbbb.csv")
    os.utime(old, (2_000_000_000, 2_000_000_000))  # newer mtime must not win over the name
    return new


def test_the_newest_statement_is_picked_and_imported_after_yes(run_cli: Cli, fake: FakeYnab):
    newest = _two_statements(run_cli)
    code, out, _err = run_cli.run(answers=["y"])
    assert code == cli.EXIT_OK
    assert f"File:       {newest}" in out and "Export to:  2026-10-13" in out
    assert "Transactions: 6 (3 pending)" in out and "Use this file? [Y/n]" in out
    assert "✓ Import complete." in out


def test_enter_accepts_the_proposed_statement(run_cli: Cli, fake: FakeYnab):
    _two_statements(run_cli)
    assert run_cli.run(answers=[""])[0] == cli.EXIT_OK
    assert len(live(fake)) == 6


@pytest.mark.parametrize("answers", [["n"], []], ids=["declined", "no-terminal"])
def test_a_declined_or_unanswerable_prompt_imports_nothing(run_cli: Cli, fake: FakeYnab, answers):
    _two_statements(run_cli)
    code, out, err = run_cli.run(answers=answers)
    assert code == cli.EXIT_USAGE and "Aborted" in err
    if not answers:
        assert "--yes" in out
    assert fake.calls == []


def test_yes_skips_the_prompt(run_cli: Cli, fake: FakeYnab):
    _two_statements(run_cli)
    code, out, _err = run_cli.run("-y")
    assert code == cli.EXIT_OK and "Auto-confirmed (--yes)" in out and "[Y/n]" not in out


def test_csv_dir_chooses_where_to_look(run_cli: Cli, fake: FakeYnab, tmp_path: Path):
    other = tmp_path / "elsewhere"
    other.mkdir()
    (other / "statement.csv").write_bytes((FIXTURES / PENDING_A).read_bytes())
    code, out, _err = run_cli.run("--csv-dir", str(other), "-y")
    assert code == cli.EXIT_OK and str(other / "statement.csv") in out


def test_no_statement_found_is_a_usage_error(run_cli: Cli, fake: FakeYnab):
    (run_cli.downloads / "notes.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    code, _out, err = run_cli.run("-y")
    assert code == cli.EXIT_USAGE and "no Revolut CSV found" in err
    assert fake.calls == []


def test_an_unreadable_proposed_statement_is_described(run_cli: Cli, fake: FakeYnab):
    bad = run_cli.downloads / "account-statement_2026-10-01_2026-10-13_en_cccc.csv"
    bad.write_bytes((FIXTURES / "export_mixed_currency.csv").read_bytes())
    code, out, _err = run_cli.run("-y")
    assert "It cannot be read" in out
    assert code == cli.EXIT_FAILED and mutations(fake) == []


def test_the_cli_database_lives_in_the_given_home(run_cli: Cli, fake: FakeYnab):
    run_cli.run(str(run_cli.statement()))
    assert run_cli.db.is_file()
    assert sqlite3.connect(run_cli.db).execute("PRAGMA user_version").fetchone()[0] >= 1
    assert services.database_stats(run_cli.db).ledger_total == 6
