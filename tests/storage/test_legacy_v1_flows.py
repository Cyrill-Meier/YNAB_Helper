"""Migration of a legacy DB produced by actually running the frozen v1 importer.

The v1 code (tests/legacy/v1_revolut_to_ynab.py) parses real Revolut-shaped CSVs and imports
them against a stateful fake YNAB, optionally followed by v1's two web-UI syncs. The resulting
DB is then migrated to v2 and the ledger is checked row by row, before and after the first
mirror sync + ``refresh_from_mirror``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from tests.legacy import v1_revolut_to_ynab as v1
from ynab_helper.storage import LedgerStore, MirrorStore, open_tx_db

from .helpers import FakeV1Ynab, dump_table, write_revolut_csv

ACCT = "acct-main"
HOTEL_OLD = "YNAB:-880000:2026-08-04:1"
HOTEL_NEW = "YNAB:-880000:2026-08-03:1"
COOP = "YNAB:-12500:2026-08-01:1"
MIGROS_1 = "YNAB:-30000:2026-08-02:1"
MIGROS_2 = "YNAB:-30000:2026-08-02:2"
TANTA_PENDING = "YNAB:-57480:2026-08-04:1"
KIOSK = "YNAB:-3000:2026-08-04:1"
DECLINED = "YNAB:-5000:2026-08-04:1"
ATM = "YNAB:-102000:2026-08-02:1"

# Export 1, made in Zürich. Ordered like a real export: completed rows by Completed Date,
# pending/reverted rows interleaved; one Savings-pocket row that must be ignored.
EXPORT_1 = [
    {
        "Type": "Topup",
        "Started Date": "2026-08-01 10:00:00",
        "Completed Date": "2026-08-01 10:00:05",
        "Description": "Top-up by *1234",
        "Amount": "500.00",
        "State": "COMPLETED",
        "Balance": "500.00",
    },
    {
        "Started Date": "2026-08-01 12:00:00",
        "Completed Date": "2026-08-02 09:00:00",
        "Description": "Coop",
        "Amount": "-12.50",
        "State": "COMPLETED",
        "Balance": "487.50",
    },
    {
        "Type": "ATM",
        "Started Date": "2026-08-02 15:00:00",
        "Completed Date": "2026-08-02 15:00:00",
        "Description": "ATM Bahnhof",
        "Amount": "-100.00",
        "Fee": "2.00",
        "State": "COMPLETED",
        "Balance": "385.50",
    },
    {
        "Product": "Savings",
        "Type": "Transfer",
        "Started Date": "2026-08-02 16:00:00",
        "Completed Date": "2026-08-02 16:00:00",
        "Description": "To pocket",
        "Amount": "-50.00",
        "State": "COMPLETED",
        "Balance": "0.00",
    },
    {
        "Started Date": "2026-08-02 18:00:00",
        "Completed Date": "2026-08-03 09:00:00",
        "Description": "Migros",
        "Amount": "-30.00",
        "State": "COMPLETED",
        "Balance": "355.50",
    },
    {
        "Started Date": "2026-08-02 19:00:00",
        "Completed Date": "2026-08-03 09:00:00",
        "Description": "Migros",
        "Amount": "-30.00",
        "State": "COMPLETED",
        "Balance": "325.50",
    },
    {
        "Started Date": "2026-08-04 05:30:00",
        "Description": "Hotel Bellagio",
        "Amount": "-880.00",
        "State": "PENDING",
    },
    {
        "Started Date": "2026-08-04 10:00:00",
        "Description": "Kiosk",
        "Amount": "-3.00",
        "State": "PENDING",
    },
    {
        "Started Date": "2026-08-04 11:00:00",
        "Description": "Declined shop",
        "Amount": "-5.00",
        "State": "REVERTED",
    },
    {
        "Started Date": "2026-08-04 20:00:00",
        "Description": "Tanta",
        "Amount": "-57.48",
        "State": "PENDING",
    },
]

# Export 2, made from a phone 9 hours behind: every Started Date is rendered -9h. The hotel
# crosses midnight (new key) and completes; Kiosk is reverted; the Tanta pending settled as two
# different rows.
EXPORT_2 = [
    {
        "Type": "Topup",
        "Started Date": "2026-08-01 01:00:00",
        "Completed Date": "2026-08-01 01:00:05",
        "Description": "Top-up by *1234",
        "Amount": "500.00",
        "State": "COMPLETED",
        "Balance": "500.00",
    },
    {
        "Started Date": "2026-08-01 03:00:00",
        "Completed Date": "2026-08-02 00:00:00",
        "Description": "Coop",
        "Amount": "-12.50",
        "State": "COMPLETED",
        "Balance": "487.50",
    },
    {
        "Type": "ATM",
        "Started Date": "2026-08-02 06:00:00",
        "Completed Date": "2026-08-02 06:00:00",
        "Description": "ATM Bahnhof",
        "Amount": "-100.00",
        "Fee": "2.00",
        "State": "COMPLETED",
        "Balance": "385.50",
    },
    {
        "Started Date": "2026-08-02 09:00:00",
        "Completed Date": "2026-08-03 00:00:00",
        "Description": "Migros",
        "Amount": "-30.00",
        "State": "COMPLETED",
        "Balance": "355.50",
    },
    {
        "Started Date": "2026-08-02 10:00:00",
        "Completed Date": "2026-08-03 00:00:00",
        "Description": "Migros",
        "Amount": "-30.00",
        "State": "COMPLETED",
        "Balance": "325.50",
    },
    {
        "Started Date": "2026-08-03 20:30:00",
        "Completed Date": "2026-08-05 00:00:00",
        "Description": "Hotel Bellagio",
        "Amount": "-880.00",
        "State": "COMPLETED",
        "Balance": "-554.50",
    },
    {
        "Started Date": "2026-08-04 01:00:00",
        "Description": "Kiosk",
        "Amount": "-3.00",
        "State": "REVERTED",
    },
    {
        "Started Date": "2026-08-04 02:00:00",
        "Description": "Declined shop",
        "Amount": "-5.00",
        "State": "REVERTED",
    },
    {
        "Started Date": "2026-08-04 11:00:00",
        "Completed Date": "2026-08-05 00:00:00",
        "Description": "Tanta",
        "Amount": "-51.06",
        "State": "COMPLETED",
        "Balance": "-605.56",
    },
    {
        "Started Date": "2026-08-04 11:05:00",
        "Completed Date": "2026-08-05 00:00:00",
        "Description": "Propina Tanta",
        "Amount": "-5.23",
        "State": "COMPLETED",
        "Balance": "-610.79",
    },
]


def _import(conn: sqlite3.Connection, csv_path: Path) -> None:
    v1.import_and_track(conn, "token", "b", ACCT, v1.parse_revolut_csv(str(csv_path)))


def _build_legacy_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, web_syncs: bool
) -> tuple[Path, FakeV1Ynab]:
    fake = FakeV1Ynab()
    monkeypatch.setattr(v1, "ynab_request", fake.request)
    # Already in YNAB before v1's first import: Coop live, Migros #1 deleted by the user.
    coop = fake.add(account_id=ACCT, import_id=COOP, amount=-12500, payee_name="Coop")
    fake.add(
        account_id=ACCT,
        import_id=MIGROS_1,
        amount=-30000,
        date="2026-08-02",
        payee_name="Migros",
        deleted=True,
    )
    fake.add(account_id=ACCT, import_id=None, amount=-2000, date="2026-08-03", payee_name="Cash")
    fake.add(account_id="acct-savings", import_id=None, amount=50000, payee_name="Transfer")
    assert coop["id"]

    db_path = tmp_path / "transactions_59213669.db"
    conn = v1.init_db(str(db_path))
    # Import 1: the per-account GET fails, so v1 records YNAB-side duplicates without an id.
    fake.fail_account_get = True
    _import(conn, write_revolut_csv(tmp_path / "export1.csv", EXPORT_1))
    fake.fail_account_get = False
    # Import 2: -9h export → hotel re-keyed (PATCH date), Kiosk reverted, Tanta superseded.
    _import(conn, write_revolut_csv(tmp_path / "export2.csv", EXPORT_2))
    if web_syncs:
        v1.sync_budget_transactions(conn, "token", "b", primary_account_id=ACCT)
        # The user reconciles Migros #2 in YNAB; v1's per-account sync maps that to PENDING.
        migros2 = fake.by_import_id(MIGROS_2)
        assert migros2 is not None
        fake.edit(migros2["id"], cleared="reconciled")
        v1.sync_from_ynab(conn, "token", "b", ACCT)
    conn.close()
    return db_path, fake


def _legacy_sources(db_path: Path) -> dict[str, str]:
    conn = sqlite3.connect(db_path)
    try:
        return dict(conn.execute("SELECT import_id, source FROM transactions"))
    finally:
        conn.close()


def _mirror_sync(conn: sqlite3.Connection, fake: FakeV1Ynab, *, from_knowledge_0: bool) -> Any:
    """The first v2 mirror sync.

    ``from_knowledge_0=False`` is a non-delta listing (no ``last_knowledge_of_server``), which
    YNAB returns without deleted transactions. ``True`` is a delta from knowledge 0, which
    includes them; only that lets refresh_from_mirror mark user-deleted legacy rows.
    """
    mirror = MirrorStore(conn)
    if from_knowledge_0:
        mirror.upsert_transactions("b", fake.all_transactions(), server_knowledge=fake.knowledge)
    else:
        mirror.upsert_transactions(
            "b", fake.full_listing(), server_knowledge=fake.knowledge, full=True
        )
    return mirror.view_for_account("b", ACCT)


def test_v1_import_flows_migrate_to_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backup_dir: Path
) -> None:
    db_path, fake = _build_legacy_db(tmp_path, monkeypatch, web_syncs=False)
    legacy_before = dump_table(db_path, "transactions")
    hotel_tx = fake.by_import_id(HOTEL_OLD)
    assert hotel_tx is not None and hotel_tx["date"] == "2026-08-03"  # v1 PATCHed the date

    conn = open_tx_db(db_path, default_account_id=ACCT, backup_dir=backup_dir)
    store = LedgerStore(conn)
    entries = {e.key: e for e in store.list_for_account(ACCT)}
    assert set(entries) == set(_legacy_sources(db_path))  # every v1 row is an importer row
    assert DECLINED not in entries  # never imported by v1
    assert not any("-50000" in key for key in entries)  # Savings product ignored

    hotel = entries[HOTEL_NEW]
    assert hotel.status == "ACTIVE" and hotel.ynab_tx_id == hotel_tx["id"]
    assert hotel.csv_state == "COMPLETED" and hotel.started_at == "2026-08-03 20:30:00"
    assert hotel.ynab_import_id == HOTEL_NEW  # wrong until the first sync…
    assert HOTEL_OLD not in entries  # v1 deleted the old local key

    assert entries[COOP].status == "UNLINKED" and entries[COOP].ynab_tx_id is None
    assert entries[MIGROS_1].status == "UNLINKED"
    assert entries[MIGROS_2].status == "ACTIVE"
    assert entries[KIOSK].status == "REVERTED" and entries[KIOSK].csv_state == "REVERTED"
    tanta = entries[TANTA_PENDING]
    assert tanta.status == "SUPERSEDED" and tanta.csv_state == "PENDING"
    assert tanta.pushed_cleared == "uncleared"
    assert entries[ATM].csv_amount == -102000 and entries[ATM].csv_payee == "ATM Bahnhof"
    assert all(e.identity == "legacy" for e in entries.values())
    assert entries["YNAB:-51060:2026-08-04:1"].status == "ACTIVE"
    assert entries["YNAB:-5230:2026-08-04:1"].status == "ACTIVE"

    # First mirror sync as a plain (non-delta) listing, then refresh (DESIGN §5.3).
    view = _mirror_sync(conn, fake, from_knowledge_0=False)
    result = store.refresh_from_mirror(ACCT, view)
    assert (result.linked, result.marked_deleted, result.import_ids_corrected) == (1, 0, 1)
    coop = store.get(ACCT, COOP)
    assert (
        coop is not None
        and coop.status == "ACTIVE"
        and coop.ynab_tx_id == fake.by_import_id(COOP)["id"]
    )
    hotel = store.get(ACCT, HOTEL_NEW)
    assert hotel is not None and hotel.ynab_import_id == HOTEL_OLD  # …corrected from YNAB
    assert HOTEL_OLD in store.keys_in_use(ACCT)  # never handed out again for a create
    assert view.by_import_id(HOTEL_OLD).id == hotel.ynab_tx_id
    # The listing omits deleted transactions, so the user-deleted legacy duplicate cannot be
    # resolved yet: it stays UNLINKED (the planner would POST it and get a duplicate back).
    assert store.get(ACCT, MIGROS_1).status == "UNLINKED"  # type: ignore[union-attr]
    assert MIGROS_1 not in view.used_import_ids

    # A delta from knowledge 0 includes deleted transactions and resolves it.
    view = _mirror_sync(conn, fake, from_knowledge_0=True)
    again = store.refresh_from_mirror(ACCT, view)
    assert (again.linked, again.marked_deleted, again.import_ids_corrected) == (0, 1, 0)
    migros1 = store.get(ACCT, MIGROS_1)
    assert migros1 is not None and migros1.status == "DELETED_IN_YNAB"
    assert {MIGROS_1, TANTA_PENDING, KIOSK} <= view.used_import_ids  # deleted ones included

    # Idempotent: a further refresh changes nothing.
    final = store.refresh_from_mirror(ACCT, _mirror_sync(conn, fake, from_knowledge_0=True))
    assert final == type(final)()
    conn.close()
    assert dump_table(db_path, "transactions") == legacy_before


def test_v1_flows_with_web_syncs_migrate_without_ghosts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backup_dir: Path
) -> None:
    db_path, fake = _build_legacy_db(tmp_path, monkeypatch, web_syncs=True)
    sources = _legacy_sources(db_path)
    # v1's syncs produced the production shapes: ghost row, ynab rows, PENDING+reconciled.
    assert sources[HOTEL_OLD] == "revolut"
    assert any(k.startswith("ynab_manual:") for k in sources)
    assert any(k.startswith("acct:acct-savings:") for k in sources)
    raw = sqlite3.connect(db_path)
    assert raw.execute(
        "SELECT state, cleared FROM transactions WHERE import_id = ?", (MIGROS_2,)
    ).fetchone() == ("PENDING", "reconciled")
    raw.close()

    conn = open_tx_db(db_path, default_account_id=ACCT, backup_dir=backup_dir)
    store = LedgerStore(conn)
    entries = {e.key: e for e in store.list_for_account(ACCT)}
    revolut_keys = {k for k, src in sources.items() if src == "revolut"}
    assert set(entries) == revolut_keys
    assert not any(k.startswith(("ynab_manual:", "acct:")) for k in entries)

    hotel_id = fake.by_import_id(HOTEL_OLD)["id"]
    assert entries[HOTEL_NEW].status == "ACTIVE" and entries[HOTEL_NEW].ynab_tx_id == hotel_id
    ghost = entries[HOTEL_OLD]
    assert ghost.status == "REKEYED" and ghost.rekeyed_to == HOTEL_NEW
    assert [
        e.key for e in entries.values() if e.ynab_tx_id == hotel_id and e.status == "ACTIVE"
    ] == [HOTEL_NEW]
    migros2 = entries[MIGROS_2]
    assert migros2.csv_state == "COMPLETED" and migros2.pushed_cleared == "cleared"
    assert entries[COOP].status == "ACTIVE"  # v1's sync had linked it
    assert entries[MIGROS_1].status == "UNLINKED"  # deleted in YNAB: full syncs never saw it

    view = _mirror_sync(conn, fake, from_knowledge_0=False)
    result = store.refresh_from_mirror(ACCT, view)
    assert result.marked_deleted == 0 and result.conflicts == ()
    assert store.get(ACCT, MIGROS_1).status == "UNLINKED"  # deleted: absent from the listing
    assert store.get(ACCT, HOTEL_NEW).ynab_import_id == HOTEL_OLD
    # The mirror holds YNAB's view (reconciled, renamed payees) — the ledger keeps the CSV's.
    assert view.by_id(migros2.ynab_tx_id).cleared == "reconciled"
    assert store.get(ACCT, MIGROS_2).pushed_cleared == "cleared"

    result = store.refresh_from_mirror(ACCT, _mirror_sync(conn, fake, from_knowledge_0=True))
    assert result.marked_deleted == 1 and result.conflicts == ()
    assert store.get(ACCT, MIGROS_1).status == "DELETED_IN_YNAB"
    conn.close()


def test_F33_ynab_edits_copied_by_v1_sync_do_not_become_csv_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backup_dir: Path
) -> None:
    """F33 class: v1's web sync copied a user's YNAB edits into the legacy amount/date columns.

    Migrated as csv_amount/csv_date, those edits would look like the CSV: the planner would
    PATCH the user's amount back (entries without started_at) or fail the timestamp match and
    CREATE a duplicate (entries with started_at). The v1 key holds the real CSV identity.
    """
    fake = FakeV1Ynab()
    monkeypatch.setattr(v1, "ynab_request", fake.request)
    dinner_key = "YNAB:-100000:2026-08-01:1"
    # Known to v1 only through its sync (the local row was lost); the user re-dated it in YNAB.
    synced_key = "YNAB:-2000:2026-08-02:1"
    fake.add(account_id=ACCT, import_id=synced_key, amount=-2000, date="2026-08-05")
    db_path = tmp_path / "transactions_1.db"
    conn = v1.init_db(str(db_path))
    export = [
        {
            "Started Date": "2026-08-01 12:00:00",
            "Completed Date": "2026-08-01 13:00:00",
            "Description": "Dinner",
            "Amount": "-100.00",
            "State": "COMPLETED",
            "Balance": "1.00",
        }
    ]
    _import(conn, write_revolut_csv(tmp_path / "e.csv", export))
    dinner_tx = fake.by_import_id(dinner_key)
    assert dinner_tx is not None
    # The user splits the bill and re-dates it in YNAB; any web page load runs v1's sync.
    fake.edit(dinner_tx["id"], amount=-50000, date="2026-08-03")
    v1.sync_budget_transactions(conn, "token", "b", primary_account_id=ACCT)
    legacy = {
        r[0]: (r[1], r[2]) for r in conn.execute("SELECT import_id, amount, date FROM transactions")
    }
    conn.close()
    assert legacy[dinner_key] == (-50000, "2026-08-01")  # YNAB's amount leaked into v1's row
    assert legacy[synced_key] == (-2000, "2026-08-05")  # YNAB's date leaked into v1's row

    conn = open_tx_db(db_path, default_account_id=ACCT, backup_dir=backup_dir)
    store = LedgerStore(conn)
    dinner = store.get(ACCT, dinner_key)
    assert dinner is not None and dinner.started_at == "2026-08-01 12:00:00"
    assert (dinner.csv_amount, dinner.pushed_amount) == (-100000, -100000)
    assert (dinner.csv_date, dinner.pushed_date) == ("2026-08-01", "2026-08-01")
    synced = store.get(ACCT, synced_key)
    assert synced is not None and synced.started_at is None
    assert (synced.csv_amount, synced.csv_date, synced.pushed_date) == (
        -2000,
        "2026-08-02",
        "2026-08-02",
    )

    # After the first sync the mirror shows YNAB's edit; the planner sees y != pushed_* (a
    # user edit to keep) while the re-imported CSV row equals csv_* (no CSV change).
    view = _mirror_sync(conn, fake, from_knowledge_0=False)
    store.refresh_from_mirror(ACCT, view)
    y = view.by_id(dinner.ynab_tx_id)
    assert y is not None and (y.amount, y.date) == (-50000, "2026-08-03")
    dinner = store.get(ACCT, dinner_key)
    assert dinner is not None and (dinner.csv_amount, dinner.pushed_amount) == (-100000, -100000)
    conn.close()
