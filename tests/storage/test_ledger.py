"""ledger.py: LedgerEntry invariants and LedgerStore operations."""

from __future__ import annotations

import dataclasses
import sqlite3

import pytest

from ynab_helper.storage import (
    LedgerEntry,
    LedgerStatus,
    LedgerStore,
    MirrorStore,
    MirrorView,
    RecordExists,
    RecordNotFound,
    YnabTx,
    transaction,
)
from ynab_helper.storage.ledger import LEDGER_COLUMNS
from ynab_helper.storage.mirror import MirrorStore as _MirrorStore

from .helpers import ynab_tx

A = "acct-a"
B = "acct-b"


def entry(key: str, **overrides: object) -> LedgerEntry:
    values: dict[str, object] = {
        "account_id": A,
        "key": key,
        "status": "ACTIVE",
        "ynab_tx_id": f"y-{key}",
        "csv_date": "2026-08-01",
        "csv_amount": -1000,
        "csv_state": "COMPLETED",
        "csv_payee": "Shop",
        "started_at": "2026-08-01 12:00:00",
        "pushed_cleared": "cleared",
        "pushed_amount": -1000,
        "pushed_date": "2026-08-01",
    }
    values.update(overrides)
    return LedgerEntry(**values)  # type: ignore[arg-type]


def ytx(tx_id: str, **overrides: object) -> YnabTx:
    values: dict[str, object] = {
        "id": tx_id,
        "account_id": A,
        "date": "2026-08-01",
        "amount": -1000,
        "payee_name": "Shop",
        "memo": None,
        "cleared": "cleared",
        "approved": True,
        "deleted": False,
        "import_id": None,
        "category_name": None,
        "transfer_account_id": None,
        "has_subtransactions": False,
    }
    values.update(overrides)
    return YnabTx(**values)  # type: ignore[arg-type]


@pytest.fixture
def store(tx_conn: sqlite3.Connection) -> LedgerStore:
    return LedgerStore(tx_conn)


# ── LedgerEntry ───────────────────────────────────────────────────────────────


def test_entry_fields_are_exactly_the_table_columns(tx_conn: sqlite3.Connection) -> None:
    columns = [r[1] for r in tx_conn.execute("PRAGMA table_info(import_ledger)")]
    assert list(LEDGER_COLUMNS) == columns


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"status": "PENDING"}, "status"),
        ({"csv_state": "DECLINED"}, "csv_state"),
        ({"identity": "ynab"}, "identity"),
        ({"pushed_cleared": "reconciled"}, "pushed_cleared"),
        ({"status": "ACTIVE", "ynab_tx_id": None}, "ynab_tx_id"),
        ({"csv_amount": -10.5}, "csv_amount"),
        ({"csv_amount": True}, "csv_amount"),
        ({"pushed_amount": "1"}, "pushed_amount"),
        ({"key": ""}, "key"),
    ],
)
def test_entry_rejects_invalid_values(overrides: dict, message: str) -> None:
    values = dict(overrides)
    with pytest.raises(ValueError, match=message):
        entry(values.pop("key", "k"), **values)


def test_entry_normalises_enum_members_and_is_immutable() -> None:
    e = entry("k", status=LedgerStatus.UNLINKED, ynab_tx_id=None, csv_state="PENDING")
    assert type(e.status) is str and e.status == "UNLINKED"
    with pytest.raises(dataclasses.FrozenInstanceError):
        e.status = "ACTIVE"  # type: ignore[misc]
    assert e.import_id_in_ynab == "k"
    assert dataclasses.replace(e, ynab_import_id="other").import_id_in_ynab == "other"


# ── insert / get / list ───────────────────────────────────────────────────────


def test_insert_fills_timestamps_and_round_trips(store: LedgerStore) -> None:
    stored = store.insert(entry("YNAB:-1000:2026-08-01:1", csv_type="Card Payment"))
    assert stored.first_seen_at and stored.updated_at
    assert store.get(A, "YNAB:-1000:2026-08-01:1") == stored
    assert store.get(B, "YNAB:-1000:2026-08-01:1") is None
    with pytest.raises(RecordExists):
        store.insert(entry("YNAB:-1000:2026-08-01:1"))


def test_F12_same_key_is_independent_per_account(store: LedgerStore) -> None:
    """F12: the ledger is scoped by account; switching accounts must not see old rows."""
    key = "YNAB:-1000:2026-08-01:1"
    store.insert(entry(key, account_id=A, ynab_tx_id="y-old-account"))
    store.insert(entry(key, account_id=B, ynab_tx_id="y-new-account"))
    assert [e.ynab_tx_id for e in store.list_for_account(A)] == ["y-old-account"]
    assert [e.ynab_tx_id for e in store.list_for_account(B)] == ["y-new-account"]
    store.set_status(A, key, "DELETED_IN_YNAB")
    assert store.get(B, key).status == "ACTIVE"  # type: ignore[union-attr]
    assert store.keys_in_use(B) == {key}


def test_list_for_account_orders_by_date_then_key(store: LedgerStore) -> None:
    store.insert(entry("b", csv_date="2026-08-02"))
    store.insert(entry("c", csv_date="2026-08-01"))
    store.insert(entry("a", csv_date="2026-08-02"))
    assert [e.key for e in store.list_for_account(A)] == ["c", "a", "b"]


def test_insert_many_is_atomic(store: LedgerStore) -> None:
    store.insert(entry("dup"))
    with pytest.raises(RecordExists):
        store.insert_many([entry("new-1"), entry("dup")])
    assert store.get(A, "new-1") is None


# ── update / set_status ───────────────────────────────────────────────────────


def test_update_changes_whitelisted_fields_only(store: LedgerStore) -> None:
    original = store.insert(entry("k", first_seen_at="2026-01-01T00:00:00Z", updated_at="x"))
    updated = store.update(A, "k", csv_state="PENDING", pushed_cleared="uncleared", last_error="e")
    assert updated.csv_state == "PENDING" and updated.last_error == "e"
    assert updated.first_seen_at == original.first_seen_at
    assert updated.updated_at != "x"
    assert store.get(A, "k") == updated
    for forbidden in ("account_id", "key", "first_seen_at", "updated_at", "bogus"):
        with pytest.raises(ValueError, match="not updatable"):
            store.update(A, "k", **{forbidden: "x"})


def test_update_validates_merged_entry_before_writing(store: LedgerStore) -> None:
    store.insert(entry("k"))
    with pytest.raises(ValueError, match="ynab_tx_id"):
        store.update(A, "k", ynab_tx_id=None)  # ACTIVE without a link
    with pytest.raises(ValueError, match="status"):
        store.update(A, "k", status="BOGUS")
    assert store.get(A, "k").ynab_tx_id == "y-k"  # type: ignore[union-attr]


def test_update_missing_entry_raises(store: LedgerStore) -> None:
    with pytest.raises(RecordNotFound):
        store.update(A, "missing", csv_state="PENDING")
    with pytest.raises(RecordNotFound):
        store.set_status(A, "missing", "REVERTED")


def test_set_status_records_and_clears_error(store: LedgerStore) -> None:
    store.insert(entry("k"))
    failed = store.set_status(A, "k", "ACTIVE", last_error="YNAB 500")
    assert failed.last_error == "YNAB 500"
    removed = store.set_status(A, "k", LedgerStatus.SUPERSEDED)
    assert removed.status == "SUPERSEDED" and removed.last_error is None
    assert removed.ynab_tx_id == "y-k"  # the link is kept for dedupe/reporting


def test_update_inside_outer_transaction_rolls_back_with_it(
    store: LedgerStore, tx_conn: sqlite3.Connection
) -> None:
    store.insert(entry("k"))
    with pytest.raises(RuntimeError), transaction(tx_conn):
        store.update(A, "k", csv_state="PENDING")
        raise RuntimeError("executor failed before commit")
    assert store.get(A, "k").csv_state == "COMPLETED"  # type: ignore[union-attr]


# ── rekey ─────────────────────────────────────────────────────────────────────


def test_F02_rekey_keeps_ynab_import_id_and_leaves_tombstone(store: LedgerStore) -> None:
    """F02: a re-key changes only our key; YNAB's import_id is kept separately."""
    store.insert(entry("YNAB:-1000:2026-08-01:2", first_seen_at="2026-01-01T00:00:00Z"))
    moved = store.rekey(A, "YNAB:-1000:2026-08-01:2", "YNAB:-1000:2026-08-01:1")
    assert moved.key == "YNAB:-1000:2026-08-01:1"
    assert moved.status == "ACTIVE" and moved.ynab_tx_id == "y-YNAB:-1000:2026-08-01:2"
    # YNAB never changes a transaction's import_id: keep the one it holds.
    assert moved.ynab_import_id == "YNAB:-1000:2026-08-01:2"
    assert moved.first_seen_at == "2026-01-01T00:00:00Z"
    tomb = store.get(A, "YNAB:-1000:2026-08-01:2")
    assert tomb is not None and tomb.status == "REKEYED"
    assert tomb.rekeyed_to == "YNAB:-1000:2026-08-01:1"
    assert store.get(A, moved.key) == moved
    assert {"YNAB:-1000:2026-08-01:2", "YNAB:-1000:2026-08-01:1"} <= store.keys_in_use(A)


def test_rekey_conflicts(store: LedgerStore) -> None:
    store.insert(entry("a"))
    store.insert(entry("b"))
    with pytest.raises(RecordExists):
        store.rekey(A, "a", "b")
    with pytest.raises(RecordNotFound):
        store.rekey(A, "missing", "c")
    with pytest.raises(ValueError):
        store.rekey(A, "a", "a")
    store.rekey(A, "a", "c")
    with pytest.raises(ValueError, match="tombstone"):
        store.rekey(A, "a", "d")
    # A renumbering cycle may move an identity back onto a tombstone.
    back = store.rekey(A, "c", "a")
    assert back.key == "a" and back.status == "ACTIVE"
    assert store.get(A, "c").status == "REKEYED"  # type: ignore[union-attr]
    assert store.get(A, "c").rekeyed_to == "a"  # type: ignore[union-attr]
    assert store.get(A, "b").status == "ACTIVE"  # type: ignore[union-attr]


def test_rekey_keeps_existing_ynab_import_id(store: LedgerStore) -> None:
    store.insert(entry("new", ynab_import_id="original"))
    assert store.rekey(A, "new", "newer").ynab_import_id == "original"


# ── claim_unassigned ──────────────────────────────────────────────────────────


def test_claim_unassigned_moves_legacy_rows_but_never_overwrites(store: LedgerStore) -> None:
    store.insert(entry("legacy-1", account_id=""))
    store.insert(entry("legacy-2", account_id=""))
    store.insert(entry("legacy-2", account_id=A, ynab_tx_id="y-current"))
    assert [e.key for e in store.list_unassigned()] == ["legacy-1", "legacy-2"]
    assert store.claim_unassigned(A) == 1
    assert store.get(A, "legacy-1") is not None
    assert store.get(A, "legacy-2").ynab_tx_id == "y-current"  # type: ignore[union-attr]
    assert [e.key for e in store.list_unassigned()] == ["legacy-2"]  # collision stays
    with pytest.raises(ValueError):
        store.claim_unassigned("")


def test_F74_listing_unassigned_rows_does_not_claim_them(
    store: LedgerStore, tx_conn: sqlite3.Connection
) -> None:
    """F74: a dry run must be able to plan with unclaimed rows without writing."""
    store.insert(entry("legacy", account_id=""))
    before = tx_conn.total_changes
    assert [e.key for e in store.list_unassigned()] == ["legacy"]
    assert tx_conn.total_changes == before
    assert store.list_for_account(A) == []


# ── aggregate reads ───────────────────────────────────────────────────────────


def test_keys_newest_date_counts_and_tx_lookups(store: LedgerStore) -> None:
    store.insert(entry("k1", csv_date="2026-08-03", ynab_import_id="YNAB:x"))
    store.insert(entry("k2", csv_date="2026-08-05", status="SUPERSEDED", csv_state="PENDING"))
    store.insert(entry("k3", csv_date="2026-08-04", status="UNLINKED", ynab_tx_id=None))
    store.insert(entry("k4", csv_date="2026-08-01", account_id=B))
    assert store.keys_in_use(A) == {"k1", "k2", "k3", "YNAB:x"}
    assert store.newest_active_date(A) == "2026-08-03"  # SUPERSEDED/UNLINKED ignored
    assert store.newest_active_date("nobody") is None
    counts = store.counts_by_status(A)
    assert counts == {
        "ACTIVE": 1,
        "UNLINKED": 1,
        "REVERTED": 0,
        "SUPERSEDED": 1,
        "DELETED_IN_YNAB": 0,
        "REKEYED": 0,
    }
    assert store.active_tx_ids(A) == {"y-k1"}
    assert [e.key for e in store.by_ynab_tx_id("y-k2")] == ["k2"]
    assert [e.account_id for e in store.by_ynab_tx_id("y-k4")] == [B]
    assert store.by_ynab_tx_id("y-k4", account_id=A) == []


# ── refresh_from_mirror ───────────────────────────────────────────────────────


def test_refresh_resolves_unlinked_by_key_and_by_ynab_import_id(store: LedgerStore) -> None:
    store.insert(entry("by-key", status="UNLINKED", ynab_tx_id=None, pushed_amount=None))
    store.insert(
        entry(
            "by-import-id",
            status="UNLINKED",
            ynab_tx_id=None,
            ynab_import_id="YNAB:-1:2026-08-01:9",
            pushed_cleared=None,
            pushed_date=None,
        )
    )
    store.insert(entry("deleted", status="UNLINKED", ynab_tx_id=None))
    store.insert(entry("absent", status="UNLINKED", ynab_tx_id=None))
    store.insert(entry("other-account", status="UNLINKED", ynab_tx_id=None))
    view = MirrorView(
        A,
        [
            ytx("t-key", import_id="by-key", amount=-1100),
            ytx("t-iid", import_id="YNAB:-1:2026-08-01:9", cleared="reconciled", date="2026-08-02"),
            ytx("t-del", import_id="deleted", deleted=True),
            ytx("t-other", import_id="other-account", account_id=B),
        ],
    )
    result = store.refresh_from_mirror(A, view)
    assert (result.linked, result.marked_deleted, result.conflicts) == (2, 1, ())

    by_key = store.get(A, "by-key")
    assert by_key is not None and by_key.status == "ACTIVE" and by_key.ynab_tx_id == "t-key"
    assert by_key.ynab_import_id == "by-key"
    assert by_key.pushed_amount == -1100  # unknown before: observed at link time
    by_iid = store.get(A, "by-import-id")
    assert by_iid is not None and by_iid.ynab_tx_id == "t-iid"
    assert by_iid.pushed_cleared == "cleared"  # reconciled observed as cleared, never stored
    assert by_iid.pushed_date == "2026-08-02"
    assert by_iid.pushed_amount == -1000  # known pushed value is kept (conflict detection)
    deleted = store.get(A, "deleted")
    assert deleted is not None and deleted.status == "DELETED_IN_YNAB"
    assert deleted.ynab_tx_id == "t-del"
    assert store.get(A, "absent").status == "UNLINKED"  # type: ignore[union-attr]
    assert store.get(A, "other-account").status == "UNLINKED"  # type: ignore[union-attr]


def test_refresh_corrects_import_ids_of_linked_rows(store: LedgerStore) -> None:
    store.insert(entry("new-key", ynab_tx_id="t1", ynab_import_id="new-key"))
    store.insert(entry("gone", ynab_tx_id="t2", status="REVERTED", csv_state="REVERTED"))
    store.insert(entry("manual-match", ynab_tx_id="t3", ynab_import_id="manual-match"))
    store.insert(entry("tomb", ynab_tx_id="t1", status="REKEYED", rekeyed_to="new-key"))
    view = MirrorView(
        A,
        [
            ytx("t1", import_id="old-key", date="2026-08-02"),
            ytx("t2", import_id="gone-in-ynab", deleted=True),
            ytx("t3", import_id=None),  # YNAB holds no import_id: keep what we know
        ],
    )
    result = store.refresh_from_mirror(A, view)
    assert result.import_ids_corrected == 2
    assert store.get(A, "new-key").ynab_import_id == "old-key"  # type: ignore[union-attr]
    assert store.get(A, "gone").ynab_import_id == "gone-in-ynab"  # type: ignore[union-attr]
    assert store.get(A, "manual-match").ynab_import_id == "manual-match"  # type: ignore[union-attr]
    assert store.get(A, "tomb").ynab_import_id is None  # type: ignore[union-attr]  # untouched
    # ACTIVE rows whose transaction vanished are left for the importer to report.
    assert store.refresh_from_mirror(A, MirrorView(A, [])) == type(result)()
    assert store.get(A, "new-key").status == "ACTIVE"  # type: ignore[union-attr]


def test_F02_refresh_never_links_two_entries_to_one_transaction(store: LedgerStore) -> None:
    """F02: a second claimant of a live YNAB transaction becomes a tombstone, not UNLINKED.

    Left UNLINKED, the planner's step-3 resolve would LINK it to the same transaction, and the
    unmatched twin could then be swept as superseded — deleting the real transaction.
    """
    store.insert(entry("linked", ynab_tx_id="t1", ynab_import_id="shared"))
    store.insert(entry("shared", status="UNLINKED", ynab_tx_id=None))
    store.insert(entry("x1", status="UNLINKED", ynab_tx_id=None, ynab_import_id="dup"))
    store.insert(entry("x2", status="UNLINKED", ynab_tx_id=None, ynab_import_id="dup"))
    view = MirrorView(A, [ytx("t1", import_id="shared"), ytx("t9", import_id="dup")])
    result = store.refresh_from_mirror(A, view)
    assert result.linked == 1
    assert set(result.conflicts) == {"shared", "x2"}
    shared = store.get(A, "shared")
    assert shared is not None and shared.status == "REKEYED" and shared.rekeyed_to == "linked"
    assert shared.ynab_tx_id == "t1" and shared.ynab_import_id == "shared"
    assert store.get(A, "x1").ynab_tx_id == "t9"  # type: ignore[union-attr]
    x2 = store.get(A, "x2")
    assert x2 is not None and x2.status == "REKEYED" and x2.rekeyed_to == "x1"
    # Exactly one live entry per YNAB transaction; tombstone keys stay reserved for creates.
    assert sorted(e.key for e in store.list_for_account(A) if e.status == "ACTIVE") == [
        "linked",
        "x1",
    ]
    assert {"shared", "x2"} <= store.keys_in_use(A)
    # Stable: a second refresh finds nothing left to resolve.
    assert store.refresh_from_mirror(A, view) == type(result)()


def test_refresh_tombstone_points_at_the_active_holder(store: LedgerStore) -> None:
    # A stale non-ACTIVE row shares the id; the tombstone must point at the live entry.
    store.insert(entry("a-reverted", ynab_tx_id="t1", status="REVERTED", csv_state="REVERTED"))
    store.insert(entry("b-active", ynab_tx_id="t1"))
    store.insert(entry("c-unlinked", status="UNLINKED", ynab_tx_id=None))
    view = MirrorView(A, [ytx("t1", import_id="c-unlinked")])
    result = store.refresh_from_mirror(A, view)
    assert result.conflicts == ("c-unlinked",)
    assert store.get(A, "c-unlinked").rekeyed_to == "b-active"  # type: ignore[union-attr]


def test_refresh_marks_deleted_match_even_when_another_entry_links_it(
    store: LedgerStore,
) -> None:
    # A deleted YNAB transaction is never written to again, so sharing its id is harmless and
    # DELETED_IN_YNAB (never recreate) is the conservative outcome.
    store.insert(entry("holder", ynab_tx_id="t1", status="SUPERSEDED", csv_state="PENDING"))
    store.insert(entry("twin", status="UNLINKED", ynab_tx_id=None, ynab_import_id="iid"))
    view = MirrorView(A, [ytx("t1", import_id="iid", deleted=True)])
    result = store.refresh_from_mirror(A, view)
    assert (result.marked_deleted, result.conflicts) == (1, ())
    twin = store.get(A, "twin")
    assert twin is not None and twin.status == "DELETED_IN_YNAB" and twin.ynab_tx_id == "t1"


def test_insert_missing_adds_only_new_keys(store: LedgerStore) -> None:
    store.insert(entry("kept", csv_state="PENDING"))
    added = store.insert_missing(
        [entry("kept", csv_state="COMPLETED"), entry("new"), entry("new", account_id=B)]
    )
    assert added == 2
    assert store.get(A, "kept").csv_state == "PENDING"  # type: ignore[union-attr]
    new = store.get(A, "new")
    assert new is not None and new.first_seen_at and new.updated_at
    assert store.get(B, "new") is not None
    assert store.insert_missing([]) == 0


def test_refresh_with_mirror_store_view(store: LedgerStore, tx_conn: sqlite3.Connection) -> None:
    store.insert(entry("YNAB:-1000:2026-08-01:1", status="UNLINKED", ynab_tx_id=None))
    mirror: _MirrorStore = MirrorStore(tx_conn)
    mirror.upsert_transactions(
        "b", [ynab_tx("t1", account_id=A, import_id="YNAB:-1000:2026-08-01:1")]
    )
    result = store.refresh_from_mirror(A, mirror.view_for_account("b", A))
    assert result.linked == 1
