"""Planner step 3 (per-match actions), step 4 (creates), step 5 (sweep), §6.4 import_ids,
allow_sweep and the executor helper resolve_duplicate (DESIGN §6.2-§6.4)."""

from __future__ import annotations

import dataclasses

import pytest

from ynab_helper.errors import YnabHelperError
from ynab_helper.importer import (
    ActionKind,
    ImportIdAllocator,
    PlanContext,
    actions_for_match,
    compute_allow_sweep,
    resolve_duplicate,
)
from ynab_helper.storage.ledger import LedgerStatus

from .helpers import (
    ACCT,
    OTHER_ACCT,
    entry,
    entry_for,
    export,
    fixture,
    kinds,
    only,
    options,
    plan,
    view,
    ytx,
    ytx_for,
)

ALLOWED_UPDATE_FIELDS = {"id", "cleared", "memo", "amount", "date"}


def _one_row(state: str = "COMPLETED", amount: str = "-10.00", started="2026-10-05 12:00:00"):
    return export(("2026-10-04 09:00:00", "Pad", "-1.00"), (started, "Shop", amount, state))


def _plan(exp, entries=(), txs=(), **opts):
    """plan() with the window-widening "Pad" row already imported (so it stays unchanged)."""
    pads = [
        entry_for(r, key=r.key, ynab_tx_id=f"y-pad-{r.line_no}")
        for r in exp.rows
        if r.payee == "Pad"
    ]
    pad_txs = [ytx_for(e) for e in pads]
    return plan(exp, [*pads, *entries], [*pad_txs, *txs], **opts)


def _matched(exp, **entry_kw):
    """The Shop row of ``exp`` and an ACTIVE entry for it (same instant)."""
    row = next(r for r in exp.rows if r.payee == "Shop")
    values = {"csv_state": "PENDING", "pushed_cleared": "uncleared"}
    values.update(entry_kw)
    return row, entry_for(row, **values)


# ── step 3: statuses ─────────────────────────────────────────────────────────


def test_deleted_in_ynab_is_never_recreated_or_written() -> None:
    exp = _one_row()
    _row, e = _matched(exp, status="DELETED_IN_YNAB", ynab_tx_id="y-gone")
    p = _plan(exp, [e], [ytx("y-gone", deleted=True)])
    assert p.ynab_actions == []
    assert p.ignored_deleted == 1
    # Only a local refresh of the CSV-side state.
    assert kinds(p) == [("LEDGER", "refresh")]
    assert dict(p.actions[0].ledger_updates) == {"csv_state": "COMPLETED"}


@pytest.mark.parametrize("status", ["REVERTED", "SUPERSEDED"])
def test_removed_entry_with_reverted_row_does_nothing(status: str) -> None:
    exp = _one_row(state="REVERTED")
    _row, e = _matched(exp, status=status, csv_state="REVERTED", ynab_tx_id="y-old")
    p = _plan(exp, [e], [ytx("y-old", deleted=True)])
    assert kinds(p) == []
    assert p.unchanged == 2  # the Pad row and this one


#: A row that started after everything in the ledger: proves the statement newer.
_LATER = ("2026-10-06 18:00:00", "Later", "-2.00")


@pytest.mark.parametrize(
    ("status", "state"),
    [("SUPERSEDED", "COMPLETED"), ("SUPERSEDED", "PENDING"), ("REVERTED", "COMPLETED")],
)
def test_F08_removed_entry_reappearing_is_recreated_with_a_fresh_import_id(status, state) -> None:
    exp = export(
        ("2026-10-04 09:00:00", "Pad", "-1.00"), ("2026-10-05 12:00:00", "Shop", "-10.00", state)
    )
    if state == "PENDING":  # a reappearing pending needs a statement provably newer
        exp = export(
            ("2026-10-04 09:00:00", "Pad", "-1.00"),
            ("2026-10-05 12:00:00", "Shop", "-10.00", state),
            _LATER,
        )
    row, e = _matched(exp, status=status, ynab_tx_id="y-old")
    old = ytx("y-old", deleted=True, import_id=row.key, amount=row.amount)
    p = _plan(exp, [e], [old])
    (create,) = [a for a in p.by_kind(ActionKind.CREATE) if a.reason == "reappeared"]
    assert create.import_id == "YNAB:-10000:2026-10-05:2"  # :1 is taken (ledger + YNAB)
    assert create.entry_key == create.import_id
    assert create.replaces_key == e.key
    assert create.tombstone_updates() == {"status": "REKEYED", "rekeyed_to": create.import_id}
    assert create.payload["cleared"] == row.cleared
    new = create.success_entry("y-new")
    assert (new.status, new.ynab_tx_id, new.ynab_import_id) == ("ACTIVE", "y-new", create.import_id)


def test_removed_entry_is_not_recreated_from_an_older_export() -> None:
    exp = _one_row(state="PENDING")
    _row, e = _matched(exp, status="SUPERSEDED", ynab_tx_id="y-old")
    p = _plan(exp, [e], [ytx("y-old", deleted=True)], allow_sweep=False)
    assert kinds(p) == []
    assert any("not re-created" in w for w in p.warnings)


def test_reverted_entry_is_not_recreated_from_a_pending_row() -> None:
    # Revolut never turns a REVERTED row back into PENDING: that row is from an older export.
    exp = _one_row(state="PENDING")
    _row, e = _matched(exp, status="REVERTED", csv_state="REVERTED", ynab_tx_id="y-old")
    p = _plan(exp, [e], [ytx("y-old", deleted=True)])
    assert p.ynab_actions == []


def test_reappearing_pending_respects_skip_pending() -> None:
    exp = export(
        ("2026-10-04 09:00:00", "Pad", "-1.00"),
        ("2026-10-05 12:00:00", "Shop", "-10.00", "PENDING"),
        _LATER,
    )
    row, e = _matched(exp, status="SUPERSEDED", ynab_tx_id="y-old")
    p = _plan(exp, [e], [ytx("y-old", deleted=True)], skip_pending=True)
    assert kinds(p) == [("CREATE", "new")]  # only the later row
    assert [(s.line_no, s.code) for s in p.skipped_rows] == [(row.line_no, "pending_skipped")]


def test_reappearing_pending_needs_a_statement_provably_newer() -> None:
    # Review finding (F36): re-sending yesterday's statement (inside allow_sweep's one-day
    # slack) showed a since-superseded hotel hold as pending again and re-created it. Only a
    # statement with a row that started after everything imported may bring a pending back.
    exp = _one_row(state="PENDING")
    _row, e = _matched(exp, status="SUPERSEDED", ynab_tx_id="y-old")
    p = _plan(exp, [e], [ytx("y-old", deleted=True)])
    assert kinds(p) == []
    assert any("not re-created until a newer statement" in w for w in p.warnings)


# UNLINKED


def test_unlinked_entry_found_live_is_linked_then_treated_as_active() -> None:
    exp = _one_row()
    row, e = _matched(exp, status="UNLINKED", ynab_tx_id=None, pushed_amount=None)
    y = ytx("y-1", import_id=e.key, amount=row.amount, date=row.date, memo="(pending)")
    p = _plan(exp, [e], [y])
    assert kinds(p) == [("LINK", "unlinked_resolved"), ("UPDATE", "cleared")]
    link, update = p.actions
    assert dict(link.ledger_updates) == {
        "status": "ACTIVE",
        "ynab_tx_id": "y-1",
        "pushed_amount": row.amount,  # unknown before: what YNAB holds ("observed at link")
    }
    assert dict(update.fields) == {"id": "y-1", "cleared": "cleared", "memo": ""}
    assert dict(update.ledger_updates) == {"csv_state": "COMPLETED", "pushed_cleared": "cleared"}


def test_unlinked_entry_resolves_by_ynab_import_id_first() -> None:
    exp = _one_row()
    row, e = _matched(
        exp, status="UNLINKED", ynab_tx_id=None, csv_state="COMPLETED", ynab_import_id="YNAB:x:1"
    )
    by_key = ytx("y-key", import_id=e.key, amount=row.amount, cleared="cleared")
    by_iid = ytx("y-iid", import_id="YNAB:x:1", amount=row.amount, cleared="cleared")
    p = _plan(exp, [e], [by_key, by_iid])
    assert only(p, ActionKind.LINK).ynab_tx_id == "y-iid"


def test_unlinked_entry_found_deleted_is_marked_deleted() -> None:
    exp = _one_row()
    _row, e = _matched(exp, status="UNLINKED", ynab_tx_id=None)
    p = _plan(exp, [e], [ytx("y-1", import_id=e.key, deleted=True)])
    action = only(p, ActionKind.LEDGER)
    assert action.ledger_updates["status"] == "DELETED_IN_YNAB"
    assert action.ledger_updates["ynab_tx_id"] == "y-1"
    assert p.ynab_actions == []
    assert p.ignored_deleted == 1


def test_unlinked_entry_not_in_ynab_is_posted_again_with_its_import_id() -> None:
    exp = _one_row()
    _row, e = _matched(exp, status="UNLINKED", ynab_tx_id=None)
    p = _plan(exp, [e], [])
    create = only(p, ActionKind.CREATE)
    assert (create.reason, create.import_id, create.entry_exists) == (
        "unlinked_missing",
        e.key,
        True,
    )
    assert create.entry_key == e.key
    updates = create.success_updates("y-new", e.key)
    assert updates["status"] == "ACTIVE" and updates["ynab_tx_id"] == "y-new"
    assert updates["csv_state"] == "COMPLETED" and updates["pushed_cleared"] == "cleared"


def test_unlinked_entry_whose_import_id_another_entry_holds_gets_a_fresh_id() -> None:
    exp = _one_row()
    _row, e = _matched(exp, status="UNLINKED", ynab_tx_id=None)
    other = entry("YNAB:other", ynab_import_id=e.key, status="SUPERSEDED", ynab_tx_id="y-z")
    p = _plan(exp, [e, other], [])
    create = only(p, ActionKind.CREATE)
    assert create.import_id == "YNAB:-10000:2026-10-05:2"
    assert create.success_updates("y-new")["ynab_import_id"] == create.import_id


def test_unlinked_entry_with_reverted_row_is_marked_reverted() -> None:
    exp = _one_row(state="REVERTED")
    _row, e = _matched(exp, status="UNLINKED", ynab_tx_id=None)
    p = _plan(exp, [e], [])
    action = only(p, ActionKind.LEDGER)
    assert action.ledger_updates["status"] == "REVERTED"
    assert p.ynab_actions == []


def test_unlinked_pending_respects_skip_pending() -> None:
    exp = _one_row(state="PENDING")
    _row, e = _matched(exp, status="UNLINKED", ynab_tx_id=None)
    p = _plan(exp, [e], [], skip_pending=True)
    assert kinds(p) == []
    assert [s.code for s in p.skipped_rows] == ["pending_skipped"]


def test_F02_unlinked_entry_never_links_a_transaction_another_entry_holds() -> None:
    # A v1 ghost: U (UNLINKED) resolves to y-1, which the ACTIVE entry H already links.
    exp = _one_row(state="PENDING")
    row, u = _matched(exp, status="UNLINKED", ynab_tx_id=None)
    h = entry(
        "YNAB:-10000:2026-10-05:9",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        ynab_tx_id="y-1",
        started_at=None,
        identity="legacy",
    )
    y = ytx("y-1", import_id=u.key, amount=row.amount, date=row.date)
    p = _plan(exp, [u, h], [y])
    assert p.by_kind(ActionKind.LINK) == []
    # H is unmatched, pending, uncleared and in the window — but its transaction is the one
    # the CSV row still references: the sweep must not delete it.
    assert p.by_kind(ActionKind.DELETE) == []
    assert any("already linked" in w for w in p.warnings)


# ACTIVE


def test_active_entry_whose_transaction_vanished_is_marked_deleted() -> None:
    exp = _one_row()
    _row, e = _matched(exp)
    for txs in ([], [ytx_for(e, deleted=True)]):
        p = _plan(exp, [e], txs)
        action = only(p, ActionKind.LEDGER)
        assert action.ledger_updates["status"] == "DELETED_IN_YNAB"
        assert p.ynab_actions == [] and p.ignored_deleted == 1
        assert any("will not be re-created" in w for w in p.warnings)


def test_F39_transaction_moved_to_another_account_is_left_alone() -> None:
    exp = _one_row()
    _row, e = _matched(exp)
    p = _plan(exp, [e], [ytx_for(e, account_id=OTHER_ACCT)])
    assert kinds(p) == []
    assert any("moved to another account" in w for w in p.warnings)


@pytest.mark.parametrize(
    ("cleared", "expected"),
    [("uncleared", [("DELETE", "reverted")]), ("cleared", []), ("reconciled", [])],
)
def test_reverted_row_deletes_only_uncleared_transactions(cleared, expected) -> None:
    exp = _one_row(state="REVERTED")
    _row, e = _matched(exp)
    p = _plan(exp, [e], [ytx_for(e, cleared=cleared)])
    assert [(a.kind.value, a.reason) for a in p.ynab_actions] == expected
    if expected:
        assert dict(p.actions[0].ledger_updates) == {"status": "REVERTED", "csv_state": "REVERTED"}
        assert p.actions[0].status_on_success == "REVERTED"
    else:
        assert any("reverted in Revolut" in w for w in p.warnings)
        assert dict(only(p, ActionKind.LEDGER).ledger_updates) == {"csv_state": "REVERTED"}


@pytest.mark.parametrize("status", ["ACTIVE", "UNLINKED"])
def test_reverted_row_whose_transaction_is_already_gone_becomes_reverted(status) -> None:
    # Our DELETE went through but the ledger write was lost: no "deleted in YNAB" warning.
    exp = _one_row(state="REVERTED")
    _row, e = _matched(exp, status=status, ynab_tx_id="y-1" if status == "ACTIVE" else None)
    gone = ytx("y-1", deleted=True, import_id=e.key)
    for txs in ([gone], [] if status == "ACTIVE" else [gone]):
        p = _plan(exp, [e], txs)
        action = only(p, ActionKind.LEDGER)
        assert (action.reason, action.ledger_updates["status"]) == ("reverted", "REVERTED")
        assert p.ynab_actions == [] and p.warnings == [] and p.ignored_deleted == 0


def test_reverted_but_cleared_warning_is_given_once() -> None:
    exp = _one_row(state="REVERTED")
    _row, e = _matched(exp, csv_state="REVERTED")
    p = _plan(exp, [e], [ytx_for(e, cleared="cleared")])
    assert kinds(p) == [] and p.warnings == []


@pytest.mark.parametrize("ynab_cleared", ["uncleared", "cleared", "reconciled"])
def test_F36_pending_row_never_downgrades_a_completed_entry(ynab_cleared) -> None:
    # The older export renders the row a day earlier and still pending.
    exp = _one_row(state="PENDING")
    _row, e = _matched(exp, csv_state="COMPLETED", pushed_cleared="cleared")
    e = dataclasses.replace(e, csv_date="2026-10-06", pushed_date="2026-10-06")
    p = _plan(exp, [e], [ytx_for(e, cleared=ynab_cleared, memo="x | (pending)")])
    assert kinds(p) == []  # no cleared, memo or date change; not even the ledger
    assert p.unchanged == 2


@pytest.mark.parametrize(
    ("row_state", "ynab_cleared", "memo", "expected_fields"),
    [
        # cleared only moves uncleared → cleared (F01)
        ("COMPLETED", "uncleared", None, {"cleared": "cleared"}),
        ("COMPLETED", "cleared", None, {}),
        ("COMPLETED", "reconciled", None, {}),
        # memo: only the "(pending)" marker is stripped, the rest is YNAB's memo (F01/F33)
        ("COMPLETED", "uncleared", "(pending)", {"cleared": "cleared", "memo": ""}),
        ("COMPLETED", "cleared", "Transfer | (pending)", {"memo": "Transfer"}),
        ("COMPLETED", "reconciled", "(pending) | Fee: 0.5", {"memo": "Fee: 0.5"}),
        (
            "COMPLETED",
            "cleared",
            "dinner w/ Anna | (pending) | Fee: 0.5",
            {"memo": "dinner w/ Anna | Fee: 0.5"},
        ),
        ("COMPLETED", "reconciled", "dinner w/ Anna", {}),
        ("COMPLETED", "cleared", "", {}),
        # a pending row never changes cleared or memo
        ("PENDING", "uncleared", "(pending)", {}),
        ("PENDING", "cleared", "(pending)", {}),
    ],
)
def test_cleared_and_memo_rules(row_state, ynab_cleared, memo, expected_fields) -> None:
    exp = _one_row(state=row_state)
    _row, e = _matched(exp)
    p = _plan(exp, [e], [ytx_for(e, cleared=ynab_cleared, memo=memo)])
    updates = p.by_kind(ActionKind.UPDATE)
    got = {k: v for k, v in updates[0].fields.items() if k != "id"} if updates else {}
    assert got == expected_fields
    for update in updates:
        assert set(update.fields) <= ALLOWED_UPDATE_FIELDS  # never approved/payee/category
        assert update.fields.get("memo", "") is not None  # memo is never set to None
        assert update.fields.get("cleared") in (None, "cleared")  # never uncleared/reconciled


@pytest.mark.parametrize(
    ("csv", "pushed", "ynab", "ynab_cleared", "split", "expect_amount", "conflict"),
    [
        (-10000, -10000, -10000, "uncleared", False, -12000, False),  # ours → update
        (-10000, None, -10000, "uncleared", False, -12000, False),  # pushed unknown → csv
        (-10000, -10000, -12000, "cleared", False, None, False),  # YNAB already agrees
        (-10000, -10000, -11000, "cleared", False, None, True),  # edited in YNAB (F33)
        (-10000, -10000, -10000, "reconciled", False, None, True),  # reconciled: never
        (-10000, -10000, -10000, "cleared", True, None, True),  # split: never
        (-11000, -10000, -10000, "cleared", False, -12000, False),  # pushed beats csv
    ],
)
def test_amount_rules(csv, pushed, ynab, ynab_cleared, split, expect_amount, conflict) -> None:
    # Amounts only differ for a settlement (pass 1 needs equal amounts): a pending entry of
    # the same payee settled at -12.00.
    exp = _one_row(amount="-12.00", state="PENDING")
    _row, e = _matched(exp, key="YNAB:-10000:2026-10-05:1")
    e = dataclasses.replace(e, csv_amount=csv, pushed_amount=pushed)
    y = ytx_for(e, amount=ynab, cleared=ynab_cleared, has_subtransactions=split)
    p = _plan(exp, [e], [y])
    assert [m.method.value for m in p.matches if m.entry_key == e.key] == ["settlement"]
    updates = p.by_kind(ActionKind.UPDATE)
    assert (updates[0].fields.get("amount") if updates else None) == expect_amount
    assert any("amount changed in Revolut" in w for w in p.warnings) is conflict
    # Whatever happens to YNAB, the ledger now knows the CSV's amount: no repeated warning.
    local = updates or p.by_kind(ActionKind.LEDGER)
    assert local[0].ledger_updates["csv_amount"] == -12000
    if expect_amount is not None:
        assert updates[0].ledger_updates["pushed_amount"] == -12000
        assert updates[0].changes["amount"] == (ynab, -12000)


@pytest.mark.parametrize(
    ("ynab_date", "ynab_cleared", "split", "expect_date"),
    [
        ("2026-10-04", "cleared", False, "2026-10-05"),  # ours → re-date (v1 behaviour)
        ("2026-10-05", "cleared", False, None),  # YNAB already agrees
        ("2026-10-01", "cleared", False, None),  # edited in YNAB
        ("2026-10-04", "reconciled", False, None),  # reconciled: ledger only
        ("2026-10-04", "cleared", True, None),  # split: ledger only
    ],
)
def test_date_rules(ynab_date, ynab_cleared, split, expect_date) -> None:
    exp = _one_row()
    _row, e = _matched(exp, csv_state="COMPLETED", pushed_cleared="cleared")
    e = dataclasses.replace(e, csv_date="2026-10-04", pushed_date="2026-10-04")
    y = ytx_for(e, date=ynab_date, cleared=ynab_cleared, has_subtransactions=split)
    p = _plan(exp, [e], [y])
    updates = p.by_kind(ActionKind.UPDATE)
    assert (updates[0].fields.get("date") if updates else None) == expect_date
    local = updates or p.by_kind(ActionKind.LEDGER)
    assert local[0].ledger_updates["csv_date"] == "2026-10-05"
    assert p.warnings == []


def test_unchanged_match_produces_no_action() -> None:
    exp = _one_row()
    _row, e = _matched(exp, csv_state="COMPLETED", pushed_cleared="cleared")
    p = _plan(exp, [e], [ytx_for(e, cleared="reconciled", memo="mine")])
    assert kinds(p) == [] and p.unchanged == 2


def test_update_carries_report_and_ledger_data() -> None:
    exp = _one_row()
    _row, e = _matched(exp)
    y = ytx_for(e, payee_name="Shop (renamed)", memo="(pending)")
    p = _plan(exp, [e], [y])
    update = only(p, ActionKind.UPDATE)
    assert update.reason == "cleared"
    assert update.before.payee == "Shop (renamed)" and update.before.ynab_tx_id == y.id
    assert update.changes == {"cleared": ("uncleared", "cleared"), "memo": ("(pending)", "")}
    assert dict(update.ledger_updates) == {"csv_state": "COMPLETED", "pushed_cleared": "cleared"}
    change = update.to_change()
    assert change.reason == "cleared" and change.tx == update.before


def test_actions_for_match_rejects_rekeyed_entries() -> None:
    exp = _one_row()
    row, e = _matched(exp, status="REKEYED", ynab_tx_id=None)
    ctx = PlanContext.create([e], view(), options())
    with pytest.raises(ValueError, match="REKEYED"):
        actions_for_match(row, e, None, options(), ctx)


# ── step 4: unmatched rows ───────────────────────────────────────────────────


def test_unmatched_rows_create_skip_and_report() -> None:
    exp = fixture("export_declined.csv")
    p = plan(exp, approved=False)
    created = p.by_kind(ActionKind.CREATE)
    assert [a.payload["amount"] for a in created] == [300000, -250000, -80000, -20000]
    first = created[0]
    assert dict(first.payload) == {
        "account_id": ACCT,
        "date": "2026-10-01",
        "amount": 300000,
        "payee_name": "Top-Up by *4242",
        "memo": "Topup",
        "cleared": "cleared",
        "approved": False,
        "import_id": "YNAB:300000:2026-10-01:1",
    }
    galaxus = created[2]
    assert galaxus.payload["cleared"] == "uncleared"
    assert galaxus.payload["memo"] == "(pending)"
    # F37: DECLINED/FAILED are skipped and reported; the reverted transfer is never imported.
    codes = [(s.line_no, s.code) for s in p.skipped_rows]
    assert codes == [
        (2, "unsupported_state"),
        (4, "unsupported_state"),
        (6, "reverted_never_imported"),
    ]
    assert all(a.entry.status == "UNLINKED" and a.entry.ynab_tx_id is None for a in created)


def test_skip_pending_only_skips_creating_pending_rows() -> None:
    exp = _one_row(state="PENDING")
    p = plan(exp, skip_pending=True)
    assert [a.payload["payee_name"] for a in p.by_kind(ActionKind.CREATE)] == ["Pad"]
    assert [s.code for s in p.skipped_rows] == ["pending_skipped"]


# ── step 5: superseded sweep ─────────────────────────────────────────────────


def _sweep_case(**entry_kw):
    exp = export(("2026-10-01 09:00:00", "Pad", "-1.00"), ("2026-10-09 09:00:00", "Pad", "-2.00"))
    values = {
        "csv_state": "PENDING",
        "pushed_cleared": "uncleared",
        "csv_payee": "Hotel",
        "started_at": "2026-10-05 14:00:00",
    }
    values.update(entry_kw)
    return exp, entry("YNAB:-150000:2026-10-05:1", **values)


def test_sweep_deletes_a_vanished_uncleared_pending() -> None:
    exp, e = _sweep_case()
    p = plan(exp, [e], [ytx_for(e)])
    delete = only(p, ActionKind.DELETE)
    assert (delete.reason, delete.ynab_tx_id, delete.row) == ("superseded", e.ynab_tx_id, None)
    assert dict(delete.ledger_updates) == {"status": "SUPERSEDED"}
    assert p.counters["swept"] == 1


@pytest.mark.parametrize(
    ("entry_kw", "tx_kw", "allow_sweep", "warning"),
    [
        ({}, {"cleared": "cleared"}, True, "pending vanished"),
        ({}, {"cleared": "reconciled"}, True, "pending vanished"),
        ({"csv_date": "2026-09-30", "started_at": "2026-09-30 14:00:00"}, {}, True, None),
        ({}, {}, False, None),  # older export
        ({}, {"account_id": OTHER_ACCT}, True, None),  # moved by the user (F39)
        ({}, {"deleted": True}, True, None),
        ({"status": "UNLINKED", "ynab_tx_id": None}, None, True, None),
        ({"csv_state": "COMPLETED"}, {"cleared": "cleared"}, True, "no longer in"),
        ({"csv_state": "REVERTED"}, {"cleared": "cleared"}, True, None),
    ],
)
def test_sweep_conditions(entry_kw, tx_kw, allow_sweep, warning) -> None:
    exp, e = _sweep_case(**entry_kw)
    txs = [ytx_for(dataclasses.replace(e, ynab_tx_id="y-h"), **tx_kw)] if tx_kw is not None else []
    if e.ynab_tx_id:
        txs = [ytx_for(e, **tx_kw)]
    p = plan(exp, [e], txs, allow_sweep=allow_sweep)
    assert p.by_kind(ActionKind.DELETE) == []
    if warning:
        assert any(warning in w for w in p.warnings)
    else:
        assert p.warnings == []


def test_sweep_never_deletes_a_transaction_a_matched_entry_links() -> None:
    exp, e = _sweep_case()
    exp = export(
        ("2026-10-01 09:00:00", "Pad", "-1.00"),
        ("2026-10-05 14:00:00", "Hotel", "-150.00", "PENDING"),
        ("2026-10-09 09:00:00", "Pad", "-2.00"),
    )
    twin = dataclasses.replace(e, key="YNAB:ghost", started_at=None, identity="legacy")
    p = plan(exp, [e, twin], [ytx_for(e)])
    assert p.by_kind(ActionKind.DELETE) == []


def test_vanished_warnings_are_capped() -> None:
    exp, _ = _sweep_case()
    entries = [
        entry(f"YNAB:-{i}000:2026-10-05:1", csv_payee=f"P{i}", started_at=None)
        for i in range(1, 14)
    ]
    p = plan(exp, entries, [ytx_for(e, cleared="cleared") for e in entries])
    vanished = [w for w in p.warnings if "no longer in" in w]
    assert len(vanished) == 11 and vanished[-1].startswith("… and 3 more")
    assert p.counters["vanished"] == 13


# ── §6.4 import_id choice ────────────────────────────────────────────────────


def _alloc(entries=(), ynab_ids=(), reserved=()):
    return ImportIdAllocator(entries, ynab_ids, reserved)


@pytest.mark.parametrize(
    ("entries", "ynab_ids", "expected"),
    [
        ((), (), "YNAB:-1000:2026-10-05:1"),  # natural key
        ((entry("YNAB:-1000:2026-10-05:1"),), (), "YNAB:-1000:2026-10-05:2"),
        ((entry("x", ynab_import_id="YNAB:-1000:2026-10-05:1"),), (), "YNAB:-1000:2026-10-05:2"),
        ((), ("YNAB:-1000:2026-10-05:1",), "YNAB:-1000:2026-10-05:2"),  # deleted in YNAB too
        (
            (entry("YNAB:-1000:2026-10-05:1", status="REKEYED", ynab_tx_id=None),),
            ("YNAB:-1000:2026-10-05:2",),
            "YNAB:-1000:2026-10-05:3",
        ),
    ],
)
def test_import_id_choice(entries, ynab_ids, expected) -> None:
    exp = export(("2026-10-05 08:00:00", "Shop", "-1.00"))
    assert _alloc(entries, ynab_ids).choose(exp.rows[0]) == expected


def test_import_id_choice_fills_the_smallest_free_slot() -> None:
    exp = export(("2026-10-05 08:00:00", "Shop", "-1.00"), ("2026-10-05 09:00:00", "Shop", "-1.00"))
    row2 = exp.rows[1]
    assert row2.key == "YNAB:-1000:2026-10-05:2"
    assert _alloc([entry(row2.key)]).choose(row2) == "YNAB:-1000:2026-10-05:1"


def test_import_ids_chosen_within_one_plan_are_distinct() -> None:
    exp = export(("2026-10-05 08:00:00", "Shop", "-1.00"), ("2026-10-05 09:00:00", "Shop", "-1.00"))
    taken = [
        entry("YNAB:-1000:2026-10-05:1", started_at="2026-10-05 01:01:00", csv_date="2026-10-04"),
        entry("YNAB:-1000:2026-10-05:2", started_at="2026-10-05 01:02:00", csv_date="2026-10-04"),
    ]
    p = plan(exp, taken, [ytx_for(e, cleared="cleared") for e in taken])
    ids = [a.import_id for a in p.by_kind(ActionKind.CREATE)]
    assert ids == ["YNAB:-1000:2026-10-05:3", "YNAB:-1000:2026-10-05:4"]
    assert p.reserved_import_ids == frozenset(ids)


def test_reserved_import_ids_block_reuse_and_length_is_asserted() -> None:
    exp = export(("2026-10-05 08:00:00", "Shop", "-1.00"))
    alloc = _alloc(reserved=["YNAB:-1000:2026-10-05:1"])
    assert alloc.choose(exp.rows[0]) == "YNAB:-1000:2026-10-05:2"
    assert alloc.chosen == {"YNAB:-1000:2026-10-05:2"}
    with pytest.raises(YnabHelperError):
        alloc.reserve("Y" * 37)
    longest = f"YNAB:-{10**12}:2026-10-05:999"
    assert len(longest) <= 36  # parser bound (|amount| ≤ 1e9 units) always fits


def test_F12_import_ids_are_unique_per_account_only() -> None:
    exp = export(("2026-10-05 08:00:00", "Shop", "-1.00"))
    p = plan(exp, [entry("YNAB:-1000:2026-10-05:1", account_id=OTHER_ACCT)], [])
    assert only(p, ActionKind.CREATE).import_id == "YNAB:-1000:2026-10-05:1"


# ── allow_sweep ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("newest", "expected"),
    [
        (None, True),  # nothing imported yet
        ("2026-10-09", True),
        ("2026-10-10", True),  # window end = newest − 1 day: still allowed
        ("2026-10-11", False),  # older export (F36)
    ],
)
def test_compute_allow_sweep(newest, expected) -> None:
    exp = export(("2026-10-01 09:00:00", "Pad", "-1.00"), ("2026-10-09 09:00:00", "Pad", "-2.00"))
    entries = [entry("YNAB:-1:2026-09-01:1", csv_date="2026-09-01")]
    if newest:
        entries.append(entry("YNAB:x", csv_date=newest))
    # Non-ACTIVE entries and other accounts never count.
    entries += [
        entry("YNAB:y", csv_date="2027-01-01", status="SUPERSEDED", ynab_tx_id="y"),
        entry("YNAB:z", csv_date="2027-01-01", account_id=OTHER_ACCT),
    ]
    assert compute_allow_sweep(exp, entries, account_id=ACCT) is expected


def test_compute_allow_sweep_without_rows_is_false() -> None:
    exp = export()
    assert compute_allow_sweep(exp, []) is False


# ── resolve_duplicate (executor support) ─────────────────────────────────────


def _dup_case(**opts_kw):
    exp = _one_row()
    p = _plan(exp, **opts_kw)
    create = next(a for a in p.by_kind(ActionKind.CREATE) if a.payload["payee_name"] == "Shop")
    return exp, p, create


def test_resolve_duplicate_links_and_updates_from_ynab_state() -> None:
    _exp, p, create = _dup_case()
    y = ytx("y-dup", import_id=create.import_id, amount=-10000, date="2026-10-05", memo="(pending)")
    ctx = PlanContext.create([], view(y), options(), reserved_import_ids=p.reserved_import_ids)
    out = resolve_duplicate(create, y, options(), ctx)
    assert [(a.kind.value, a.reason) for a in out.actions] == [
        ("LINK", "duplicate_resolved"),
        ("UPDATE", "cleared"),
    ]
    link, update = out.actions
    assert link.entry.status == "ACTIVE" and link.entry.ynab_tx_id == "y-dup"
    assert link.entry.pushed_cleared == "uncleared" and link.entry_exists is False
    assert dict(update.fields) == {"id": "y-dup", "cleared": "cleared", "memo": ""}


@pytest.mark.parametrize(
    "y_kw", [{"amount": -9999}, {"date": "2026-10-01"}], ids=["amount", "date"]
)
def test_resolve_duplicate_with_another_amount_or_date_is_not_linked(y_kw) -> None:
    # Review finding: the import_id may belong to a different transaction whose amount was
    # settled (import_ids are immutable); linking would mis-assign the row. Nothing is linked
    # or recorded, so the next import creates the row under a fresh import_id.
    _exp, _p, create = _dup_case()
    values = {"amount": -10000, "date": "2026-10-05", **y_kw}  # the row's, but for one field
    y = ytx("y-dup", import_id=create.import_id, cleared="cleared", **values)
    ctx = PlanContext.create([], view(y), options())
    out = resolve_duplicate(create, y, options(), ctx)
    assert out.actions == []
    assert out.unchanged and any("not linked" in w for w in out.warnings)


def test_resolve_duplicate_deleted_and_missing_and_held() -> None:
    _exp, _p, create = _dup_case()
    gone = ytx("y-dup", import_id=create.import_id, deleted=True)
    out = resolve_duplicate(create, gone, options(), PlanContext.create([], view(gone), options()))
    (action,) = out.actions
    assert action.kind is ActionKind.LEDGER and action.entry.status == "DELETED_IN_YNAB"
    assert out.ignored_deleted

    out = resolve_duplicate(create, None, options(), PlanContext.create([], view(), options()))
    (action,) = out.actions
    assert action.entry.status == "UNLINKED" and out.warnings

    y = ytx("y-dup", import_id=create.import_id)
    holder = entry("YNAB:holder", ynab_tx_id="y-dup")
    out = resolve_duplicate(create, y, options(), PlanContext.create([holder], view(y), options()))
    assert [a.entry.status for a in out.actions] == ["UNLINKED"]
    assert any("already has it" in w for w in out.warnings)


def test_resolve_duplicate_for_an_existing_entry_updates_it() -> None:
    exp = _one_row()
    row, e = _matched(exp, status="UNLINKED", ynab_tx_id=None)
    p = _plan(exp, [e], [])
    create = only(p, ActionKind.CREATE)
    y = ytx(
        "y-dup", import_id=create.import_id, amount=row.amount, date=row.date, cleared="cleared"
    )
    out = resolve_duplicate(create, y, options(), PlanContext.create([e], view(y), options()))
    (link,) = out.actions
    assert link.entry is None and link.entry_exists
    assert link.ledger_updates["status"] == "ACTIVE"
    assert link.ledger_updates["ynab_tx_id"] == "y-dup"
    assert link.ledger_updates["pushed_cleared"] == "cleared"


def test_resolve_duplicate_keeps_the_tombstone_of_a_recreated_row() -> None:
    exp = _one_row()
    row, e = _matched(exp, status="SUPERSEDED", ynab_tx_id="y-old")
    p = _plan(exp, [e], [ytx("y-old", deleted=True)])
    create = only(p, ActionKind.CREATE)
    y = ytx(
        "y-dup", import_id=create.import_id, amount=row.amount, date=row.date, cleared="cleared"
    )
    out = resolve_duplicate(create, y, options(), PlanContext.create([e], view(y), options()))
    assert [a.replaces_key for a in out.actions] == [e.key]


def test_resolve_duplicate_rejects_other_actions() -> None:
    exp = _one_row()
    _row, e = _matched(exp)
    p = _plan(exp, [e], [ytx_for(e)])
    with pytest.raises(ValueError):
        resolve_duplicate(
            only(p, ActionKind.UPDATE), None, options(), PlanContext.create([], view(), options())
        )


# ── ledger effects are valid for the real LedgerStore ───────────────────────


def test_every_ledger_effect_applies_to_a_real_ledger_store(tmp_path) -> None:
    from ynab_helper.storage import LedgerStore, open_tx_db

    conn = open_tx_db(tmp_path / "tx.db", default_account_id=ACCT, backup_dir=tmp_path / "b")
    try:
        store = LedgerStore(conn)
        exp = fixture("export_pending_then_completed_b.csv")
        hotel = next(r for r in exp.rows if r.payee.startswith("Hotel"))
        amazon = next(r for r in exp.rows if r.payee == "Amazon EU")
        migros = next(r for r in exp.rows if r.payee.startswith("Migros"))
        seed = [
            entry_for(hotel, csv_state="PENDING", pushed_cleared="uncleared"),
            entry_for(
                amazon,
                key="YNAB:-89990:2026-10-11:1",
                csv_amount=-89990,
                pushed_amount=-89990,
                csv_state="PENDING",
                pushed_cleared="uncleared",
            ),
            entry_for(
                migros, status="UNLINKED", ynab_tx_id=None, identity="legacy", started_at=None
            ),
            entry(
                "YNAB:-5000:2026-10-11:1",
                csv_state="PENDING",
                pushed_cleared="uncleared",
                csv_date="2026-10-11",
            ),
        ]
        store.insert_many(seed)
        txs = [ytx_for(seed[0]), ytx_for(seed[1], memo="(pending)"), ytx_for(seed[3])]
        txs.append(ytx("y-migros", import_id=seed[2].key, amount=migros.amount, date=migros.date))
        p = plan(exp, store.list_for_account(ACCT), txs)
        assert {a.kind for a in p.actions} >= {
            ActionKind.CREATE,
            ActionKind.LINK,
            ActionKind.UPDATE,
            ActionKind.DELETE,
        }
        for n, a in enumerate(p.actions):
            if a.kind is ActionKind.CREATE:
                if a.entry_exists:
                    store.update(a.account_id, a.entry_key, **a.success_updates(f"c{n}"))
                else:
                    store.insert(a.success_entry(f"c{n}"))
            elif a.entry is not None and not a.entry_exists:
                store.insert(a.entry)
            else:
                store.update(a.account_id, a.entry_key, **a.ledger_updates)
        statuses = {e.key: e.status for e in store.list_for_account(ACCT)}
        assert statuses[seed[0].key] == LedgerStatus.REVERTED
        assert statuses[seed[2].key] == LedgerStatus.ACTIVE
        assert statuses[seed[3].key] == LedgerStatus.SUPERSEDED
        assert store.get(ACCT, seed[1].key).csv_amount == amazon.amount
    finally:
        conn.close()
