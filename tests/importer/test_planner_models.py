"""Importer data model: options, planned actions, plan and import result (DESIGN §6.2/§6.3)."""

from __future__ import annotations

import json

import pytest

from ynab_helper.importer import (
    ActionKind,
    Change,
    Failure,
    ImportResult,
    LedgerEntry,
    Plan,
    PlannedAction,
    PlanOptions,
    Removal,
    TxRef,
    entry_update_fields,
)
from ynab_helper.storage.ledger import LedgerEntry as StorageLedgerEntry

from .helpers import ACCT, entry, export, fixture, plan, ytx_for


def test_models_reexport_the_storage_ledger_types() -> None:
    assert LedgerEntry is StorageLedgerEntry


def test_plan_options_validation_and_defaults() -> None:
    opts = PlanOptions(account_id=ACCT, approved=False)
    assert (opts.skip_pending, opts.allow_sweep) == (False, True)
    with pytest.raises(ValueError):
        PlanOptions(account_id="", approved=True)


def _create(**kw) -> PlannedAction:
    exp = export(("2026-10-05 08:00:00", "Shop", "-1.00"))
    (action,) = plan(exp).actions
    if not kw:
        return action
    values = {
        "kind": action.kind,
        "account_id": action.account_id,
        "entry_key": action.entry_key,
        "reason": action.reason,
        "row": action.row,
        "payload": action.payload,
        "entry": action.entry,
    }
    values.update(kw)
    return PlannedAction(**values)


def test_planned_action_is_immutable_and_validated() -> None:
    action = _create()
    with pytest.raises(TypeError):
        action.payload["amount"] = 1  # type: ignore[index]
    copy = action.ynab_payload()
    copy["amount"] = 1
    assert action.payload["amount"] == -1000
    with pytest.raises(ValueError):
        _create(payload={})
    with pytest.raises(ValueError):
        _create(entry=None)
    with pytest.raises(ValueError):
        _create(entry_key="other")
    with pytest.raises(ValueError):
        PlannedAction(
            kind=ActionKind.UPDATE, account_id=ACCT, entry_key="k", reason="x", ynab_tx_id="y"
        )
    with pytest.raises(ValueError):
        PlannedAction(
            kind=ActionKind.UPDATE,
            account_id=ACCT,
            entry_key="k",
            reason="x",
            ynab_tx_id="y",
            fields={"id": "other", "cleared": "cleared"},
        )
    with pytest.raises(ValueError):
        PlannedAction(kind=ActionKind.DELETE, account_id=ACCT, entry_key="k", reason="x")


def test_create_helpers() -> None:
    action = _create()
    assert action.import_id == "YNAB:-1000:2026-10-05:1"
    assert action.status_on_success == "ACTIVE"
    assert action.entry.status == "UNLINKED" and action.entry.ynab_tx_id is None
    done = action.success_entry("y-9", "YNAB:-1000:2026-10-05:1")
    assert (done.status, done.ynab_tx_id, done.pushed_cleared, done.pushed_amount) == (
        "ACTIVE",
        "y-9",
        "cleared",
        -1000,
    )
    updates = action.success_updates("y-9")
    assert updates == {
        "status": "ACTIVE",
        "ynab_tx_id": "y-9",
        "ynab_import_id": "YNAB:-1000:2026-10-05:1",
    }
    with pytest.raises(ValueError):
        action.success_updates()
    assert action.tombstone_updates() is None
    fields = entry_update_fields(done)
    assert "key" not in fields and "account_id" not in fields and fields["status"] == "ACTIVE"


def test_import_result_to_dict_is_json_safe() -> None:
    tx = TxRef("2026-10-05", -1000, "Shop", "y-1")
    result = ImportResult(
        dry_run=False,
        account_id=ACCT,
        currency="CHF",
        window=("2026-10-01", "2026-10-05"),
        created=[tx],
        updated=[Change(tx, {"cleared": ("uncleared", "cleared")}, "cleared")],
        removed=[Removal(tx, "reverted")],
        skipped_rows=[(3, "state DECLINED is not imported")],
        failures=[Failure("UPDATE", tx, "YNAB said no"), Failure("CREATE", None, "boom")],
    )
    data = json.loads(json.dumps(result.to_dict()))
    assert data["window"] == ["2026-10-01", "2026-10-05"]
    assert data["updated"][0]["fields"] == {"cleared": ["uncleared", "cleared"]}
    assert data["skipped_rows"] == [[3, "state DECLINED is not imported"]]
    assert data["failures"][1] == {"action": "CREATE", "tx": None, "error": "boom"}
    assert data["has_changes"] is True and data["ok"] is False


@pytest.mark.parametrize(
    ("kwargs", "has_changes", "ok"),
    [
        ({}, False, True),
        ({"linked": [TxRef("d", 1, "p")]}, True, True),
        ({"rate_limited": True, "not_applied": 2}, False, False),
        ({"not_applied": 1}, False, False),
        ({"failures": [Failure("DELETE", None, "x")]}, False, False),
    ],
)
def test_import_result_flags(kwargs, has_changes, ok) -> None:
    result = ImportResult(dry_run=False, account_id=ACCT, **kwargs)
    assert (result.has_changes, result.ok) == (has_changes, ok)


def test_from_plan_dry_run_counts_rows_and_reports_every_action() -> None:
    exp = fixture("export_basic.csv")
    p = plan(exp)
    result = ImportResult.from_plan_dry_run(p, exp)
    assert result.dry_run is True
    assert (result.rows_total, result.rows_pending, result.rows_reverted) == (23, 4, 1)
    assert len(result.created) == 22
    assert result.skipped_rows == [(16, "reverted by Revolut (never imported)")]
    assert result.currency == "CHF" and result.window == ("2026-10-01", "2026-10-13")
    json.dumps(result.to_dict())


def test_plan_summary_and_views() -> None:
    exp = export(("2026-10-01 09:00:00", "Pad", "-1.00"), ("2026-10-05 08:00:00", "Shop", "-1.00"))
    e = entry(
        "YNAB:-1000:2026-10-05:1",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-10-05 08:00:00",
    )
    p = plan(exp, [e], [ytx_for(e, memo="(pending)")])
    assert isinstance(p, Plan)
    assert [a.kind for a in p.ynab_actions] == [ActionKind.CREATE, ActionKind.UPDATE]
    assert p.local_actions == [] and p.has_ynab_writes
    summary = json.loads(json.dumps(p.summary()))
    assert summary["actions"]["CREATE"] == 1 and summary["actions"]["UPDATE"] == 1
    assert (summary["frame_offset"], summary["stale_export"], summary["allow_sweep"]) == (
        0,
        False,
        True,
    )
    assert p.counters["matched_timestamp"] == 1 and p.counters["unmatched_rows"] == 1
    # Actions come in execution order: local first, then creates, updates, deletes.
    order = [a.sort_key()[0] for a in p.actions]
    assert order == sorted(order)
