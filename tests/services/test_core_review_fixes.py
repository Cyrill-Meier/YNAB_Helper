"""Regression tests for the final review's core findings (R01 R02 R03 R06 R07 R11 R17), driven
end to end through ``services.import_csv`` against the fake YNAB, as the bot runs an import."""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FAKE_TOKEN, FakeYnab
from tests.importer.helpers import csv_bytes
from tests.legacy import v1_revolut_to_ynab as v1
from ynab_helper import services
from ynab_helper.importer import ImportResult
from ynab_helper.services import UserContext
from ynab_helper.storage import LedgerStore, connect

from .conftest import ACCT, BUDGET


def _import(ctx: UserContext, rows: Sequence[Sequence[str]]) -> ImportResult:
    return services.import_csv(
        ctx, csv_bytes(rows), original_name="statement.csv", source_kind="telegram"
    )


def _live(fake: FakeYnab, payee: str) -> list[dict[str, Any]]:
    return [t for t in fake.transactions(BUDGET, account_id=ACCT) if t["payee_name"] == payee]


def _created(result: ImportResult) -> list[tuple[str, int, str]]:
    return [(c.date, c.amount, c.payee) for c in result.created]


def _ledger(ctx: UserContext, key: str) -> Any:
    conn = connect(ctx.db_path)
    try:
        return LedgerStore(conn).get(ACCT, key)
    finally:
        conn.close()


def _copy_db(source: Path, target: Path) -> None:
    src = sqlite3.connect(source)
    dst = sqlite3.connect(target)
    try:
        src.backup(dst)
    finally:
        src.close()
        dst.close()


# ── R01: a deleted pending settling at another amount ────────────────────────


AMAZON_PENDING = ("2026-10-10 12:00:00", "Amazon EU", "-50.00", "PENDING")
STARBUCKS = ("2026-10-11 08:00:00", "Starbucks", "-4.50")


@pytest.mark.parametrize("settled", ["-50.65", "-55.00", "-50.00"])
def test_core_R01_a_pending_deleted_in_ynab_stays_deleted_when_it_settles(ctx, fake, settled):
    _import(ctx, [AMAZON_PENDING])
    fake.user_delete_transaction(_live(fake, "Amazon EU")[0]["id"])
    between = _import(ctx, [AMAZON_PENDING, STARBUCKS])  # the entry becomes DELETED_IN_YNAB
    assert between.ignored_deleted == 1
    assert _ledger(ctx, "YNAB:-50000:2026-10-10:1").status == "DELETED_IN_YNAB"

    final = _import(ctx, [STARBUCKS, ("2026-10-10 12:00:00", "Amazon EU", settled)])

    assert final.created == [] and final.ignored_deleted == 1, final.to_dict()
    assert _live(fake, "Amazon EU") == []
    again = _import(ctx, [STARBUCKS, ("2026-10-10 12:00:00", "Amazon EU", settled)])
    assert again.created == [] and not again.has_changes


# ── R02: a fresh, lost or restored ledger ────────────────────────────────────


ROWS = [
    ("2026-09-01 10:00:00", "Coop", "-12.50"),
    ("2026-09-02 11:00:00", "Migros", "-45.30"),
    ("2026-09-03 12:00:00", "SBB", "-3.60"),
    ("2026-09-04 13:00:00", "Kiosk", "-2.00"),
]


def test_core_R02_reimporting_through_a_fresh_ledger_creates_nothing(ctx, fake, tmp_path):
    assert _import(ctx, ROWS).ok
    txs = {t["payee_name"]: t for t in fake.transactions(BUDGET, account_id=ACCT)}
    fake.user_edit_transaction(txs["Migros"]["id"], amount=-40_000)  # corrected amount
    fake.user_edit_transaction(txs["SBB"]["id"], date="2026-09-05")  # re-dated
    fake.user_delete_transaction(txs["Kiosk"]["id"])
    before = fake.snapshot(include_deleted=True)
    statement = tmp_path / "account-statement_2026-09-01_2026-09-04_en.csv"
    statement.write_bytes(csv_bytes(ROWS))
    cli = UserContext.from_cli(
        token=FAKE_TOKEN, budget_id=BUDGET, account_id=ACCT, db_path=tmp_path / "cli.db"
    )
    fake.reset_log()

    result = services.import_csv(cli, statement, original_name=statement.name, source_kind="cli")

    assert result.ok and result.created == [], result.to_dict()
    assert result.ignored_deleted == 1  # the Kiosk row the user deleted
    assert [c.method for c in fake.calls if c.method != "GET"] == []  # edits kept as they are
    assert fake.snapshot(include_deleted=True) == before


def test_core_R02_restoring_an_older_backup_creates_no_duplicates(ctx, fake, tmp_path):
    first = [("2026-09-01 10:00:00", "Coop", "-12.50")]
    assert _import(ctx, first).ok
    backup = tmp_path / "backup.db"
    _copy_db(ctx.db_path, backup)
    hold = ("2026-09-02 15:00:00", "Hotel", "-150.00", "PENDING")
    kiosk = ("2026-09-03 09:00:00", "Kiosk", "-2.00")
    assert _import(ctx, [*first, hold, kiosk]).ok
    fake.user_delete_transaction(_live(fake, "Kiosk")[0]["id"])
    latest = [*first, ("2026-09-02 15:00:00", "Hotel", "-162.40"), kiosk]
    settled = _import(ctx, latest)
    assert [c.reason for c in settled.updated] == ["settlement"]  # same YNAB import_id
    _copy_db(backup, ctx.db_path)  # OPERATIONS: restore a backup, send the statement again

    result = _import(ctx, latest)

    assert result.ok and result.created == [], result.to_dict()
    assert [t["amount"] for t in _live(fake, "Hotel")] == [-162_400]
    assert _live(fake, "Kiosk") == []
    assert result.ignored_deleted == 1


# ── R03: the legacy table stays a frozen archive ─────────────────────────────


def test_core_R03_v2_imports_never_write_the_legacy_table(ctx, fake):
    ctx.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = v1.init_db(str(ctx.db_path))
    conn.execute(
        "INSERT INTO transactions (import_id, date, amount, payee_name, cleared, state, "
        "account_id, source, imported_at) VALUES ('YNAB:-1000:2026-09-01:1', '2026-09-01', "
        "-1000, 'Pad', 'cleared', 'COMPLETED', ?, 'revolut', '2026-09-01T08:00:00')",
        (ACCT,),
    )
    conn.commit()
    conn.close()

    def legacy() -> list[tuple[Any, ...]]:
        raw = sqlite3.connect(ctx.db_path)
        try:
            return raw.execute("SELECT * FROM transactions ORDER BY import_id").fetchall()
        finally:
            raw.close()

    archived = legacy()
    _import(ctx, [AMAZON_PENDING])
    _import(ctx, [("2026-10-10 12:00:00", "Amazon EU", "-50.65"), STARBUCKS])  # settlement

    assert legacy() == archived  # DESIGN §5.2; why a rollback cannot import: tx_schema doc


# ── R06: statements rendered in another timezone ─────────────────────────────


def test_core_R06_a_shifted_statement_does_not_recreate_a_row_deleted_in_ynab(ctx, fake):
    landlord = ("2026-09-01 13:51:23", "Landlord", "-1500.00", "COMPLETED", "Transfer")
    _import(ctx, [landlord])
    fake.user_delete_transaction(_live(fake, "Landlord")[0]["id"])
    assert _import(ctx, [landlord]).ignored_deleted == 1
    behind = [  # the phone 6 h behind; the only overlap is the deleted row
        ("2026-09-01 07:51:23", "Landlord", "-1500.00", "COMPLETED", "Transfer"),
        ("2026-09-03 07:34:12", "Cafe", "-4.50"),
    ]

    result = _import(ctx, behind)

    assert _created(result) == [("2026-09-03", -4500, "Cafe")]
    assert result.ignored_deleted == 1 and _live(fake, "Landlord") == []


def test_core_R06_a_pending_imported_abroad_settles_in_the_next_statement_abroad(ctx, fake):
    _import(
        ctx,
        [
            ("2026-09-01 12:00:12", "Bakery", "-5.20"),
            ("2026-09-01 20:24:30", "Night Market", "-3.60", "PENDING"),
        ],
    )
    _import(  # +8 h: the pending is now rendered on 09-02
        ctx,
        [
            ("2026-09-01 20:00:12", "Bakery", "-5.20"),
            ("2026-09-02 04:24:30", "Night Market", "-3.60", "PENDING"),
        ],
    )
    # The next +8 h statement starts on the pending's (rendered) date: no other overlap.
    result = _import(
        ctx,
        [
            ("2026-09-02 04:24:30", "Night Market", "-3.74"),
            ("2026-09-03 09:10:11", "Taxi", "-12.00"),
        ],
    )

    assert _created(result) == [("2026-09-03", -12_000, "Taxi")]
    assert [(t["amount"], t["cleared"]) for t in _live(fake, "Night Market")] == [
        (-3_740, "cleared")
    ]


# ── R07: a create whose outcome is unknown ───────────────────────────────────


@pytest.mark.parametrize("retries", [0, 2])
def test_core_R07_a_lost_create_then_a_settlement_leaves_one_transaction(
    ctx, fake, monkeypatch, retries
):
    monkeypatch.setattr(services, "client_factory", lambda c: fake.client(max_retries=retries))
    for n in range(1, retries + 2):  # the POST is applied, then every answer is lost
        fake.raise_network_error_at_call(n, after_apply=n == 1)
    first = _import(ctx, [("2026-10-10 12:00:00", "Hotel Zurich", "-50.00", "PENDING")])
    assert not first.ok
    assert _ledger(ctx, "YNAB:-50000:2026-10-10:1").status == "UNLINKED"
    statement = [
        ("2026-10-10 12:00:00", "Hotel Zurich", "-52.30"),
        ("2026-10-11 09:00:00", "Coop", "-10.00"),
    ]

    second = _import(ctx, statement)

    assert second.ok and _created(second) == [("2026-10-11", -10_000, "Coop")]
    assert [(t["amount"], t["cleared"]) for t in _live(fake, "Hotel Zurich")] == [
        (-52_300, "cleared")
    ]
    assert not _import(ctx, statement).has_changes


def test_core_R07_a_create_that_never_reached_ynab_is_posted_again_at_its_settled_amount(ctx, fake):
    fake.raise_network_error_at_call(1)  # not applied
    assert not _import(ctx, [("2026-10-10 12:00:00", "Hotel Zurich", "-50.00", "PENDING")]).ok
    statement = [("2026-10-10 12:00:00", "Hotel Zurich", "-52.30")]

    result = _import(ctx, statement)

    assert result.ok and _created(result) == [("2026-10-10", -52_300, "Hotel Zurich")]
    assert len(_live(fake, "Hotel Zurich")) == 1
    assert not _import(ctx, statement).has_changes


# ── R11: a user's date edit and a re-rendered statement ──────────────────────


def test_core_R11_a_user_date_edit_survives_statements_from_two_timezones(ctx, fake):
    home = [
        ("2026-09-01 12:00:00", "Bakery", "-5.20"),
        ("2026-09-02 23:30:12", "Bar", "-30.40"),
        ("2026-09-03 10:00:00", "Kiosk", "-2.10"),
    ]
    _import(ctx, home)
    bar = _live(fake, "Bar")[0]
    fake.user_edit_transaction(bar["id"], date="2026-09-03")  # booked on the next day
    abroad = [  # 1 h ahead: the bar bill is rendered on 09-03 as well
        ("2026-09-01 13:00:00", "Bakery", "-5.20"),
        ("2026-09-03 00:30:12", "Bar", "-30.40"),
        ("2026-09-03 11:00:00", "Kiosk", "-2.10"),
    ]
    assert not _import(ctx, abroad).has_changes
    fake.reset_log()

    result = _import(ctx, home)

    assert result.updated == [] and [c for c in fake.calls if c.method != "GET"] == []
    assert _live(fake, "Bar")[0]["date"] == "2026-09-03"


def test_core_R11_a_lost_patch_response_still_converges(ctx, fake):
    pending = ("2026-10-10 12:00:00", "Amazon EU", "-89.99", "PENDING")
    pad = ("2026-10-09 09:00:00", "Pad", "-1.00")
    _import(ctx, [pad, pending])
    settled = [pad, ("2026-10-10 12:00:00", "Amazon EU", "-90.41")]
    fake.raise_network_error_at_call(1, after_apply=True)  # the settlement PATCH lands

    assert not _import(ctx, settled).ok
    key = "YNAB:-89990:2026-10-10:1"
    assert _ledger(ctx, key).last_error.startswith("unconfirmed write:")
    fake.reset_log()

    again = _import(ctx, settled)

    assert again.ok and [c for c in fake.calls if c.method != "GET"] == []
    entry = _ledger(ctx, key)
    assert (entry.csv_amount, entry.pushed_amount, entry.last_error) == (-90_410, -90_410, None)


# ── R17: v1's own deletions of reverted pendings ─────────────────────────────


def test_core_R17_a_reverted_pending_v1_removed_is_not_reported_as_deleted_by_the_user(ctx, fake):
    hold = fake.add_transaction(
        BUDGET,
        ACCT,
        amount=-80_000,
        date="2026-10-02",
        payee_name="Shell",
        memo="(pending)",
        cleared="uncleared",
        import_id="YNAB:-80000:2026-10-02:1",
    )
    fake.user_delete_transaction(hold)  # v1's importer deleted it when Revolut reverted it
    ctx.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = v1.init_db(str(ctx.db_path))
    # v1's YNAB sync then overwrote state REVERTED with PENDING (cleared stayed uncleared).
    conn.execute(
        "INSERT INTO transactions (import_id, date, amount, payee_name, memo, cleared, state, "
        "ynab_tx_id, account_id, source, imported_at, deleted) VALUES (?, '2026-10-02', -80000, "
        "'Shell', '(pending)', 'uncleared', 'PENDING', ?, ?, 'revolut', '2026-10-02T08:00:00', 1)",
        ("YNAB:-80000:2026-10-02:1", hold, ACCT),
    )
    conn.commit()
    conn.close()

    result = _import(
        ctx,
        [
            ("2026-10-02 10:15:00", "Shell", "-80.00", "REVERTED"),
            ("2026-10-02 10:20:00", "Shell", "-63.20"),
        ],
    )

    assert result.ok and result.ignored_deleted == 0, result.to_dict()
    assert not any("deleted in YNAB" in w for w in result.warnings)
    assert _ledger(ctx, "YNAB:-80000:2026-10-02:1").status == "REVERTED"
    assert _created(result) == [("2026-10-02", -63_200, "Shell")]
