"""Executor (docs/DESIGN.md §6.3) against the stateful fake YNAB with fault injection.

Every test plans with the real planner from a real mirror (synced through the real client)
and the real SQLite ledger, then applies the plan with :func:`apply_plan`. Faults are
injected into the fake between planning and applying, as they would happen in production.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import sqlite3
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.errors import YnabAuthError
from ynab_helper.importer import (
    ActionKind,
    ImportResult,
    Plan,
    PlannedAction,
    PlanOptions,
    TxRef,
    compute_allow_sweep,
    executor,
    plan_import,
)
from ynab_helper.importer.executor import _bisect_allowance, apply_plan
from ynab_helper.revolut import ParsedExport, parse_export
from ynab_helper.storage import LedgerStore, MirrorStore, open_tx_db
from ynab_helper.storage.ledger import LedgerEntry, LedgerStatus
from ynab_helper.sync import sync_and_refresh, sync_budget
from ynab_helper.ynab.client import YnabClient

from .helpers import FIXTURES, export

BUDGET = "b1"
ACCT = "acct-main"
OTHER = "acct-other"
TX_PATH = rf"/budgets/{BUDGET}/transactions"

HOTEL = "YNAB:-150000:2026-10-10:1"
MIGROS = "YNAB:-45300:2026-10-11:1"
AMAZON = "YNAB:-89990:2026-10-11:1"
COOP_NEW = "YNAB:-11200:2026-10-13:1"


def export_a() -> ParsedExport:
    return parse_export(FIXTURES / "export_pending_then_completed_a.csv")


def export_b() -> ParsedExport:
    return parse_export(FIXTURES / "export_pending_then_completed_b.csv")


class Harness:
    """A user's transactions DB plus the fake YNAB; plans exactly like ``import_csv``."""

    def __init__(self, tmp_path: Path, fake: FakeYnab) -> None:
        self.fake = fake
        self.conn = open_tx_db(
            tmp_path / "tx.db", default_account_id=ACCT, backup_dir=tmp_path / "bk"
        )
        self.ledger = LedgerStore(self.conn)
        self.mirror = MirrorStore(self.conn)

    def client(self, **kwargs: Any) -> YnabClient:
        kwargs.setdefault("max_retries", 0)
        return self.fake.client(**kwargs)

    def plan(self, exp: ParsedExport, **overrides: Any) -> tuple[Plan, PlanOptions]:
        sync_and_refresh(self.client(), self.conn, BUDGET, ACCT, force=True)
        entries = self.ledger.list_for_account(ACCT)
        values: dict[str, Any] = {
            "account_id": ACCT,
            "approved": True,
            "allow_sweep": compute_allow_sweep(exp, entries, account_id=ACCT),
        }
        values.update(overrides)
        opts = PlanOptions(**values)
        view = self.mirror.view_for_account(BUDGET, ACCT)
        return plan_import(exp, entries, view, opts), opts

    def apply(
        self,
        plan: Plan,
        opts: PlanOptions,
        exp: ParsedExport,
        *,
        client: YnabClient | None = None,
        **kwargs: Any,
    ) -> ImportResult:
        kwargs.setdefault("dry_run", False)
        return apply_plan(
            plan,
            client or self.client(),
            self.conn,
            budget_id=BUDGET,
            account_id=ACCT,
            export=exp,
            opts=opts,
            **kwargs,
        )

    def run(self, exp: ParsedExport, **kwargs: Any) -> ImportResult:
        plan, opts = self.plan(exp)
        return self.apply(plan, opts, exp, **kwargs)

    def entry(self, key: str) -> LedgerEntry:
        found = self.ledger.get(ACCT, key)
        assert found is not None, key
        return found

    def tx_id(self, import_id: str) -> str:
        tx = self.fake.find_by_import_id(ACCT, import_id)
        assert tx is not None, import_id
        return tx["id"]

    def live(self) -> list[tuple[int, str, str | None]]:
        return sorted(
            (t["amount"], t["cleared"], t["memo"])
            for t in self.fake.transactions(BUDGET, account_id=ACCT)
        )

    def writes(self) -> list[tuple[str, str]]:
        return [(c.method, c.path) for c in self.fake.calls if c.method != "GET"]

    def dump(self) -> list[tuple[Any, ...]]:
        tables = ("import_ledger", "csv_files")
        return [
            tuple(row)
            for table in tables
            for row in self.conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2")
        ]


@pytest.fixture
def fake() -> FakeYnab:
    f = FakeYnab()
    f.add_budget(BUDGET, currency="CHF")
    f.add_account(BUDGET, ACCT, "Revolut")
    f.add_account(BUDGET, OTHER, "Savings", type="savings")
    return f


@pytest.fixture
def h(tmp_path: Path, fake: FakeYnab) -> Iterator[Harness]:
    harness = Harness(tmp_path, fake)
    try:
        yield harness
    finally:
        harness.conn.close()


#: What YNAB must hold once export B is fully imported (amount, cleared, memo).
FINAL_B = sorted(
    [
        (300_000, "cleared", "Topup"),
        (-11_200, "cleared", None),
        (-6_800, "cleared", None),
        (-45_300, "cleared", ""),
        (-90_410, "cleared", ""),
        (-11_200, "uncleared", "(pending)"),
    ]
)


def _imported_a(h: Harness) -> None:
    result = h.run(export_a())
    assert result.ok and len(result.created) == 6
    h.fake.reset_log()


# ── happy path ──────────────────────────────────────────────────────────────


def test_creates_are_recorded_with_the_ids_ynab_returned_and_audited(h, caplog):
    with caplog.at_level(logging.INFO, logger="ynab_helper.audit"):
        result = h.run(export_a())

    assert result.ok and not result.dry_run
    assert h.writes() == [("POST", TX_PATH)]
    assert {ref.ynab_tx_id for ref in result.created} == {
        t["id"] for t in h.fake.transactions(BUDGET, account_id=ACCT)
    }
    hotel = h.entry(HOTEL)
    assert hotel.status == LedgerStatus.ACTIVE and hotel.ynab_tx_id == h.tx_id(HOTEL)
    assert (hotel.pushed_cleared, hotel.pushed_amount, hotel.pushed_date) == (
        "uncleared",
        -150_000,
        "2026-10-10",
    )
    assert hotel.started_at == "2026-10-10 14:00:00" and hotel.csv_state == "PENDING"
    audit = [r.getMessage() for r in caplog.records if r.name == "ynab_helper.audit"]
    assert len(audit) == 6 and all(line.startswith("created ") for line in audit)
    hotel_line = next(line for line in audit if "Hotel" in line)
    assert "2026-10-10" in hotel_line and "-150.00 CHF" in hotel_line
    assert f"id={hotel.ynab_tx_id}" in hotel_line and "reason=new" in hotel_line


def test_a_second_statement_updates_deletes_and_creates_then_is_idempotent(h, caplog):
    _imported_a(h)
    with caplog.at_level(logging.INFO, logger="ynab_helper.audit"):
        result = h.run(export_b())

    assert result.ok
    assert h.writes() == [
        ("POST", TX_PATH),
        ("PATCH", TX_PATH),
        ("DELETE", f"{TX_PATH}/{h.tx_id(HOTEL)}"),
    ]
    assert h.live() == FINAL_B
    assert [c.reason for c in result.updated] == ["cleared", "settlement"]
    assert [r.reason for r in result.removed] == ["reverted"]
    assert h.entry(HOTEL).status == LedgerStatus.REVERTED
    amazon = h.entry(AMAZON)
    assert (amazon.csv_state, amazon.pushed_cleared, amazon.pushed_amount) == (
        "COMPLETED",
        "cleared",
        -90_410,
    )
    audit = [r.getMessage() for r in caplog.records if r.name == "ynab_helper.audit"]
    assert sorted(line.split()[0] for line in audit) == ["created", "deleted", "updated", "updated"]
    assert any("amount -89.99 CHF->-90.41 CHF" in line for line in audit)

    h.fake.reset_log()
    again = h.run(export_b())
    assert again.ok and not again.has_changes and h.writes() == []


# ── F06: one failing update never wedges the import ─────────────────────────


def test_F06_404_on_patch_marks_deleted_in_ynab_and_the_rest_continues(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    migros = h.tx_id(MIGROS)
    h.fake.user_delete_transaction(migros)  # deleted in YNAB after the plan was made

    result = h.apply(plan, opts, export_b())

    assert result.failures == [] and result.ok
    assert result.ignored_deleted == 1
    assert any("deleted in YNAB" in w for w in result.warnings)
    methods = Counter(method for method, _path in h.writes())
    assert methods == {"POST": 1, "PATCH": 1, "PUT": 2, "DELETE": 1}  # bulk 404 → one by one
    assert h.entry(MIGROS).status == LedgerStatus.DELETED_IN_YNAB
    assert h.entry(AMAZON).pushed_amount == -90_410
    assert h.entry(HOTEL).status == LedgerStatus.REVERTED  # the delete phase still ran
    assert [ref.amount for ref in result.created] == [-11_200]

    h.fake.reset_log()
    again = h.run(export_b())
    assert again.ok and h.writes() == []  # never re-created, never written again


def test_F06_server_error_on_the_bulk_update_fails_its_rows_and_the_deletes_still_run(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    h.fake.fail_next("PATCH", TX_PATH, 500)

    result = h.apply(plan, opts, export_b())

    assert not result.ok and not result.rate_limited
    assert sorted(f.tx.payee for f in result.failures) == ["Amazon EU", "Migros Zuerich HB"]
    assert {f.action for f in result.failures} == {"UPDATE"}
    assert "PUT" not in {m for m, _p in h.writes()}  # a 5xx is no reason to go row by row
    assert h.entry(HOTEL).status == LedgerStatus.REVERTED
    assert h.entry(MIGROS).csv_state == "PENDING"  # not recorded as applied (F07)
    h.fake.reset_log()
    assert h.run(export_b()).ok
    assert h.live() == FINAL_B


def test_bulk_4xx_falls_back_to_per_transaction_put_isolating_the_bad_row(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    amazon = h.tx_id(AMAZON)
    h.fake.fail_next("PATCH", TX_PATH, 400)
    h.fake.fail_next("PUT", rf"{TX_PATH}/{amazon}", 422)

    result = h.apply(plan, opts, export_b())

    puts = [p for m, p in h.writes() if m == "PUT"]
    assert sorted(puts) == sorted([f"{TX_PATH}/{h.tx_id(MIGROS)}", f"{TX_PATH}/{amazon}"])
    assert [f.tx.ynab_tx_id for f in result.failures] == [amazon]
    assert [c.tx.payee for c in result.updated] == ["Migros Zuerich HB"]
    assert h.entry(MIGROS).pushed_cleared == "cleared"
    stale = h.entry(AMAZON)
    assert (stale.csv_amount, stale.pushed_amount, stale.csv_state) == (-89_990, -89_990, "PENDING")
    assert not result.ok
    h.fake.reset_log()
    assert h.run(export_b()).ok
    assert h.live() == FINAL_B


# ── F07: rate limit stops writes; nothing unapplied is recorded ─────────────


def test_F07_429_mid_run_counts_not_applied_and_records_nothing_for_them(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    h.fake.fail_next("PATCH", TX_PATH, 429, retry_after=600)
    before = {key: h.entry(key) for key in (HOTEL, MIGROS, AMAZON)}

    result = h.apply(plan, opts, export_b())

    assert result.rate_limited and not result.ok
    assert result.not_applied == 3  # two updates and the delete
    assert result.failures == []
    assert any("rate limit" in w and "600" in w for w in result.warnings)
    assert h.writes() == [("POST", TX_PATH), ("PATCH", TX_PATH)]  # nothing after the 429
    for key, entry in before.items():
        assert dataclasses.replace(h.entry(key), updated_at="") == dataclasses.replace(
            entry, updated_at=""
        ), key
    assert h.entry(COOP_NEW).status == LedgerStatus.ACTIVE  # the create before it is kept

    h.fake.reset_log()
    rerun = h.run(export_b())
    assert rerun.ok and [m for m, _p in h.writes()] == ["PATCH", "DELETE"]
    assert h.live() == FINAL_B


def test_F07_429_on_the_first_write_applies_and_records_nothing(h):
    _imported_a(h)
    snapshot = h.dump()
    plan, opts = h.plan(export_b())
    h.fake.fail_next("POST", TX_PATH, 429)

    result = h.apply(plan, opts, export_b())

    assert result.rate_limited and result.not_applied == 4
    assert result.created == result.updated == result.removed == []
    assert h.writes() == [("POST", TX_PATH)]
    assert [r for r in h.dump() if r[0] != ACCT or r[1] != COOP_NEW] == snapshot


def test_network_error_on_create_is_a_failure_and_updates_and_deletes_still_run(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    h.fake.raise_network_error_next("POST")

    result = h.apply(plan, opts, export_b())

    assert [f.action for f in result.failures] == ["CREATE"]
    assert "network" in result.failures[0].error.lower()
    assert h.ledger.get(ACCT, COOP_NEW) is None
    assert len(result.updated) == 2 and len(result.removed) == 1
    h.fake.reset_log()
    assert h.run(export_b()).ok
    assert h.live() == FINAL_B


def test_a_lost_create_response_is_adopted_on_the_next_run_without_a_duplicate(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    h.fake.raise_network_error_next("POST", after_apply=True)

    first = h.apply(plan, opts, export_b())

    assert [f.action for f in first.failures] == ["CREATE"]
    assert h.ledger.get(ACCT, COOP_NEW) is None  # YNAB has it, the ledger does not know
    h.fake.reset_log()
    second = h.run(export_b())
    assert second.ok and h.writes() == []  # adopted (LINK), not POSTed again
    assert [ref.amount for ref in second.linked] == [-11_200]
    assert h.entry(COOP_NEW).ynab_tx_id == h.tx_id(COOP_NEW)
    assert h.live() == FINAL_B


def test_ynab_auth_error_propagates_after_recording_confirmed_writes(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    h.fake.fail_next("PATCH", TX_PATH, 401)
    with pytest.raises(YnabAuthError):
        h.apply(plan, opts, export_b())
    assert h.entry(COOP_NEW).status == LedgerStatus.ACTIVE
    assert h.entry(MIGROS).csv_state == "PENDING"
    assert ("DELETE", f"{TX_PATH}/{h.tx_id(HOTEL)}") not in h.writes()


# ── creates YNAB rejects ────────────────────────────────────────────────────


def test_a_create_chunk_ynab_rejects_is_bisected_to_isolate_the_bad_row(tmp_path):
    fake = FakeYnab(today="2026-10-12")  # the 2026-10-13 pending is "in the future"
    fake.add_budget(BUDGET, currency="CHF")
    fake.add_account(BUDGET, ACCT, "Revolut")
    h = Harness(tmp_path, fake)
    try:
        result = h.run(export_b())
        assert [f.tx.date for f in result.failures] == ["2026-10-13"]
        assert "future" in result.failures[0].error
        assert len(result.created) == 5
        posts = [p for m, p in h.writes() if m == "POST"]
        assert 2 <= len(posts) <= 7  # halves, not one request per row
        assert h.ledger.get(ACCT, COOP_NEW) is None
        assert h.entry(MIGROS).status == LedgerStatus.ACTIVE
    finally:
        h.conn.close()


def test_a_payload_the_client_refuses_fails_alone(h):
    plan, opts = h.plan(export_a())
    bad = plan.by_kind(ActionKind.CREATE)[0]
    broken = dataclasses.replace(bad, payload={**bad.payload, "amount": 1.5})
    plan.actions[plan.actions.index(bad)] = broken

    result = h.apply(plan, opts, export_a())

    assert [f.tx for f in result.failures] == [bad.after]
    assert "invalid transaction data" in result.failures[0].error
    assert len(result.created) == 5
    assert h.ledger.get(ACCT, bad.entry_key) is None


# ── duplicate import_ids (§6.3 step 2) ──────────────────────────────────────


def test_duplicate_import_id_is_linked_and_updated_in_the_same_run(h):
    plan, opts = h.plan(export_b())
    # YNAB got the Migros row from elsewhere (another DB, a lost ledger) after our sync.
    existing = h.fake.add_transaction(
        BUDGET,
        ACCT,
        amount=-45_300,
        date="2026-10-11",
        payee_name="Migros",
        memo="(pending) | dinner",
        cleared="uncleared",
        import_id=MIGROS,
    )

    result = h.apply(plan, opts, export_b())

    assert result.ok
    patch = [c for c in h.fake.calls if c.method == "PATCH"]
    assert [c.body for c in patch] == [
        {"transactions": [{"id": existing, "cleared": "cleared", "memo": "dinner"}]}
    ]
    assert h.fake.tx(existing)["cleared"] == "cleared"
    entry = h.entry(MIGROS)
    assert entry.status == LedgerStatus.ACTIVE and entry.ynab_tx_id == existing
    assert entry.pushed_cleared == "cleared"
    assert [ref.ynab_tx_id for ref in result.linked] == [existing]
    assert [c.reason for c in result.updated] == ["cleared"]
    assert len([t for t in h.fake.transactions(BUDGET) if t["import_id"] == MIGROS]) == 1


def test_duplicate_of_a_deleted_transaction_becomes_deleted_in_ynab(h):
    plan, opts = h.plan(export_b())
    gone = h.fake.add_transaction(
        BUDGET, ACCT, amount=-45_300, date="2026-10-11", import_id=MIGROS, cleared="uncleared"
    )
    h.fake.user_delete_transaction(gone)

    result = h.apply(plan, opts, export_b())

    assert result.ok and result.ignored_deleted == 1
    assert any("deleted in YNAB" in w for w in result.warnings)
    entry = h.entry(MIGROS)
    assert entry.status == LedgerStatus.DELETED_IN_YNAB and entry.ynab_tx_id == gone
    assert not [c for c in h.fake.calls if c.method in ("PATCH", "PUT")]
    h.fake.reset_log()
    assert h.run(export_b()).ok and h.writes() == []  # never re-created


def test_duplicate_with_another_amount_is_not_linked(h):
    plan, opts = h.plan(export_b())
    h.fake.add_transaction(
        BUDGET, ACCT, amount=-40_000, date="2026-10-11", import_id=MIGROS, cleared="cleared"
    )

    result = h.apply(plan, opts, export_b())

    assert result.ok and result.unchanged >= 1
    assert any("not linked" in w for w in result.warnings)
    assert h.ledger.get(ACCT, MIGROS) is None
    h.fake.reset_log()
    nxt = h.run(export_b())  # created under a fresh import_id now
    assert [ref.amount for ref in nxt.created] == [-45_300]
    assert h.fake.find_by_import_id(ACCT, "YNAB:-45300:2026-10-11:2") is not None


def test_duplicates_are_not_resolved_when_the_post_create_sync_is_rate_limited(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    assert Counter(a.kind for a in plan.ynab_actions) == {
        ActionKind.CREATE: 1,
        ActionKind.UPDATE: 2,
        ActionKind.DELETE: 1,
    }
    # The new Coop pending reached YNAB from elsewhere after our sync: a duplicate on POST.
    h.fake.add_transaction(
        BUDGET,
        ACCT,
        amount=-11_200,
        date="2026-10-13",
        import_id=COOP_NEW,
        cleared="uncleared",
        memo="(pending)",
    )
    before = {key: h.entry(key) for key in (HOTEL, MIGROS, AMAZON)}
    h.fake.reset_log()
    h.fake.fail_next("GET", TX_PATH, 429)

    result = h.apply(plan, opts, export_b())

    assert result.rate_limited and not result.ok
    # Nothing was written after the 429: the two updates, the delete and the duplicate wait.
    assert h.writes() == [("POST", TX_PATH)]
    assert result.not_applied == 4 and result.failures == []
    assert result.updated == result.removed == []
    assert h.ledger.get(ACCT, COOP_NEW) is None
    for key, entry in before.items():
        assert dataclasses.replace(h.entry(key), updated_at="") == dataclasses.replace(
            entry, updated_at=""
        ), key
    h.fake.reset_log()
    again = h.run(export_b())  # the duplicate is adopted from the mirror on the next run
    assert again.ok, again.to_dict()
    assert "POST" not in [m for m, _p in h.writes()]
    assert h.entry(COOP_NEW).status == LedgerStatus.ACTIVE
    assert h.live() == FINAL_B


def test_duplicates_are_reported_when_the_post_create_sync_fails(h):
    plan, opts = h.plan(export_b())
    h.fake.add_transaction(BUDGET, ACCT, amount=-45_300, date="2026-10-11", import_id=MIGROS)
    h.fake.fail_next("GET", TX_PATH, 503)

    result = h.apply(plan, opts, export_b())

    assert not result.ok and result.not_applied == 1
    assert [f.action for f in result.failures] == ["sync"]
    assert len(result.created) == 5


# ── deletes ─────────────────────────────────────────────────────────────────


def test_delete_404_counts_as_removed(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    h.fake.user_delete_transaction(h.tx_id(HOTEL))

    result = h.apply(plan, opts, export_b())

    assert result.ok and [r.reason for r in result.removed] == ["reverted"]
    assert h.entry(HOTEL).status == LedgerStatus.REVERTED


def test_delete_server_error_is_a_failure_and_the_entry_stays_active(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    h.fake.fail_next("DELETE", None, 500)

    result = h.apply(plan, opts, export_b())

    assert [f.action for f in result.failures] == ["DELETE"]
    assert h.entry(HOTEL).status == LedgerStatus.ACTIVE
    h.fake.reset_log()
    assert h.run(export_b()).ok
    assert h.entry(HOTEL).status == LedgerStatus.REVERTED
    assert h.live() == FINAL_B


# ── F74: dry run ────────────────────────────────────────────────────────────


def test_F74_dry_run_sends_nothing_and_writes_nothing(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    snapshot = h.dump()

    def refuse(*_args: Any) -> Any:
        raise AssertionError("a dry run must not call YNAB")

    result = h.apply(plan, opts, export_b(), client=YnabClient("t", transport=refuse), dry_run=True)

    assert result.dry_run and result.has_changes
    assert len(result.created) == 1 and len(result.updated) == 2 and len(result.removed) == 1
    assert h.dump() == snapshot


# ── guards ──────────────────────────────────────────────────────────────────


def test_F12_a_plan_for_another_account_is_refused_before_any_write(h):
    plan, opts = h.plan(export_a())
    with pytest.raises(ValueError, match="plan is for account"):
        apply_plan(
            plan,
            h.client(),
            h.conn,
            budget_id=BUDGET,
            account_id=OTHER,
            export=export_a(),
            dry_run=False,
        )
    foreign = dataclasses.replace(plan.actions[0], account_id="")
    plan.actions[0] = foreign
    with pytest.raises(ValueError, match="belongs to account"):
        h.apply(plan, opts, export_a())
    assert h.writes() == []


def test_a_transaction_moved_to_another_account_is_never_patched(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    amazon = h.tx_id(AMAZON)
    h.fake.user_edit_transaction(amazon, account_id=OTHER)
    sync_budget(h.client(), h.mirror, BUDGET)  # the mirror now places it elsewhere

    result = h.apply(plan, opts, export_b())

    assert [f.tx.ynab_tx_id for f in result.failures] == [amazon]
    assert "another account" in result.failures[0].error
    bodies = [c.body for c in h.fake.calls if c.method == "PATCH"]
    assert all(u["id"] != amazon for body in bodies for u in body["transactions"])


def test_a_field_ynab_did_not_apply_is_not_recorded_as_pushed(h):
    split = h.fake.add_transaction(
        BUDGET,
        ACCT,
        amount=-20_000,
        date="2026-10-05",
        cleared="uncleared",
        subtransactions=[{"amount": -15_000}, {"amount": -5_000}],
    )
    entry = LedgerEntry(
        account_id=ACCT,
        key="YNAB:-20000:2026-10-05:1",
        ynab_tx_id=split,
        status=LedgerStatus.ACTIVE.value,
        csv_date="2026-10-05",
        csv_amount=-20_000,
        csv_state="PENDING",
        pushed_cleared="uncleared",
        pushed_amount=-20_000,
        pushed_date="2026-10-05",
    )
    h.ledger.insert(entry)
    sync_budget(h.client(), h.mirror, BUDGET)
    update = PlannedAction(
        kind=ActionKind.UPDATE,
        account_id=ACCT,
        entry_key=entry.key,
        reason="redated",
        ynab_tx_id=split,
        fields={"id": split, "date": "2026-10-06", "cleared": "cleared"},
        changes={"date": ("2026-10-05", "2026-10-06"), "cleared": ("uncleared", "cleared")},
        ledger_updates={
            "csv_date": "2026-10-06",
            "pushed_date": "2026-10-06",
            "pushed_cleared": "cleared",
        },
        before=TxRef("2026-10-05", -20_000, "Split", split),
    )
    plan = Plan(account_id=ACCT, actions=[update])
    opts = PlanOptions(account_id=ACCT, approved=True)

    result = h.apply(plan, opts, export_a())

    after = h.entry(entry.key)
    assert after.csv_date == "2026-10-06" and after.pushed_cleared == "cleared"
    assert after.pushed_date == "2026-10-05"  # YNAB ignores date changes on splits
    assert any("did not apply the date change" in w for w in result.warnings)


def test_progress_is_reported_and_a_failing_callback_is_ignored(h):
    _imported_a(h)
    seen: list[tuple[str, int, int]] = []

    def progress(phase: str, done: int, total: int) -> None:
        seen.append((phase, done, total))
        raise RuntimeError("UI bug")

    result = h.run(export_b(), on_progress=progress)

    assert result.ok
    assert [s[0] for s in seen] == ["create", "update", "delete"]
    assert seen[-1][1:] == (4, 4)


def test_a_ledger_write_failing_after_a_confirmed_create_is_reported_and_converges(h, monkeypatch):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    real_insert = LedgerStore.insert

    def flaky_insert(self: LedgerStore, entry: LedgerEntry) -> LedgerEntry:
        if entry.key == COOP_NEW:
            raise sqlite3.OperationalError("disk I/O error")
        return real_insert(self, entry)

    monkeypatch.setattr(LedgerStore, "insert", flaky_insert)
    result = h.apply(plan, opts, export_b())
    monkeypatch.setattr(LedgerStore, "insert", real_insert)

    assert [ref.amount for ref in result.created] == [-11_200]  # it is in YNAB
    assert [f.action for f in result.failures] == ["CREATE"]
    assert "could not be saved" in result.failures[0].error
    assert h.ledger.get(ACCT, COOP_NEW) is None
    assert len(result.updated) == 2  # the rest went on
    h.fake.reset_log()
    assert h.run(export_b()).ok and h.writes() == []  # adopted, no duplicate
    assert h.live() == FINAL_B


# ── F81 / F82 / mirror freshness ────────────────────────────────────────────


def test_F82_no_database_transaction_is_open_during_a_ynab_request(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    # Exercise every request kind: a duplicate (post-create sync), a bulk 4xx (PUTs), a delete.
    h.fake.add_transaction(
        BUDGET, ACCT, amount=-11_200, date="2026-10-13", import_id=COOP_NEW, cleared="uncleared"
    )
    h.fake.fail_next("PATCH", TX_PATH, 400)
    open_during_request: list[bool] = []

    def guarded(method: str, url: str, headers: Any, body: Any, timeout: float) -> Any:
        open_during_request.append(h.conn.in_transaction)
        return h.fake.transport(method, url, headers, body, timeout)

    result = h.apply(plan, opts, export_b(), client=h.fake.client(transport=guarded))

    assert result.ok and len(result.linked) == 1 and len(result.removed) == 1
    methods = {c.method for c in h.fake.calls}
    assert {"POST", "GET", "PATCH", "PUT", "DELETE"} <= methods
    assert open_during_request and not any(open_during_request)


def test_F81_updates_use_the_bulk_patch_or_put_never_a_single_transaction_patch(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    h.fake.fail_next("PATCH", TX_PATH, 400)
    h.apply(plan, opts, export_b())
    for call in h.fake.calls:
        if call.method == "PATCH":
            assert call.path == TX_PATH  # bulk endpoint only
        if call.method == "PUT":
            assert call.path.startswith(f"{TX_PATH}/") and set(call.body) == {"transaction"}


def test_writes_mark_the_mirror_stale_so_the_next_status_sync_runs(h):
    plan, opts = h.plan(export_a())
    assert h.mirror.get_last_sync_at(BUDGET) is not None
    h.apply(plan, opts, export_a())
    assert h.mirror.get_last_sync_at(BUDGET) is None  # balances changed since that sync
    plan, opts = h.plan(export_a())
    h.apply(plan, opts, export_a())  # nothing to write
    assert h.mirror.get_last_sync_at(BUDGET) is not None


# ── review fixes: error-model branches, bounded bisection, ledger write failures ───


def _many_rows(n: int, *, first_amount: int = 1) -> list[tuple[str, str, str]]:
    """``n`` completed card payments with distinct (amount, date): one CREATE each."""
    return [
        (
            f"2026-09-{1 + i % 28:02d} {8 + i // 28:02d}:{i % 60:02d}:00",
            f"Shop {i}",
            f"-{first_amount + i}.00",
        )
        for i in range(n)
    ]


def test_F07_429_on_the_first_of_two_deletes_stops_the_second(h):
    first = export(
        ("2026-10-05 10:00:00", "Kiosk", "-10.00", "PENDING"),
        ("2026-10-05 11:00:00", "Bakery", "-20.00", "PENDING"),
    )
    assert h.run(first).ok
    reverted = export(
        ("2026-10-05 10:00:00", "Kiosk", "-10.00", "REVERTED"),
        ("2026-10-05 11:00:00", "Bakery", "-20.00", "REVERTED"),
    )
    plan, opts = h.plan(reverted)
    assert len(plan.by_kind(ActionKind.DELETE)) == 2
    h.fake.reset_log()
    h.fake.fail_next("DELETE", None, 429, retry_after=60)

    result = h.apply(plan, opts, reverted)

    assert [m for m, _p in h.writes()] == ["DELETE"]  # the second one was never sent
    assert result.rate_limited and result.not_applied == 2 and result.removed == []
    assert h.ledger.counts_by_status(ACCT)["ACTIVE"] == 2  # nothing recorded as removed
    h.fake.reset_log()
    assert h.run(reverted).ok
    assert h.fake.transactions(BUDGET, account_id=ACCT) == []


def test_F07_a_row_missing_from_the_patch_response_is_a_failure_and_not_recorded(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    amazon = h.tx_id(AMAZON)
    before = h.entry(AMAZON)

    def drop_amazon(method: str, url: str, headers: Any, body: Any, timeout: float) -> Any:
        status, hdrs, raw = h.fake.transport(method, url, headers, body, timeout)
        if method == "PATCH":  # YNAB applied both updates but only returned one of them
            data = json.loads(raw)
            data["data"]["transactions"] = [
                t for t in data["data"]["transactions"] if t["id"] != amazon
            ]
            raw = json.dumps(data).encode()
        return status, hdrs, raw

    result = h.apply(plan, opts, export_b(), client=h.fake.client(transport=drop_amazon))

    assert [(f.action, f.tx.ynab_tx_id) for f in result.failures] == [("UPDATE", amazon)]
    assert "did not confirm" in result.failures[0].error
    assert [c.tx.payee for c in result.updated] == ["Migros Zuerich HB"]
    assert dataclasses.replace(h.entry(AMAZON), updated_at="") == dataclasses.replace(
        before, updated_at=""
    )
    assert h.entry(MIGROS).pushed_cleared == "cleared"
    h.fake.reset_log()
    assert h.run(export_b()).ok  # YNAB already holds the change: nothing is sent twice
    assert "PATCH" not in [m for m, _p in h.writes()]
    assert h.entry(AMAZON).csv_amount == -90_410
    assert h.live() == FINAL_B


def test_a_server_error_on_a_multi_row_create_sends_exactly_one_post(h):
    h.fake.fail_next("POST", TX_PATH, 500)

    result = h.run(export_a())

    assert h.writes() == [("POST", TX_PATH)]  # a 5xx is not row-specific: no bisection
    assert len(result.failures) == 6 and {f.action for f in result.failures} == {"CREATE"}
    assert h.ledger.list_for_account(ACCT) == []
    h.fake.reset_log()
    assert h.run(export_a()).ok and len(h.fake.transactions(BUDGET, account_id=ACCT)) == 6


def test_a_404_on_create_stops_every_create_without_bisecting(h):
    many = export(*_many_rows(150))
    plan, opts = h.plan(many)
    assert len(plan.by_kind(ActionKind.CREATE)) == 150  # two chunks
    h.fake.fail_next("POST", TX_PATH, 404)

    result = h.apply(plan, opts, many)

    assert h.writes() == [("POST", TX_PATH)]  # neither halves nor the second chunk were sent
    assert len(result.failures) == 150 and result.created == []
    assert all("could not find" in f.error for f in result.failures)
    assert h.ledger.list_for_account(ACCT) == []


def test_a_404_on_create_sends_no_further_write_and_marks_nothing_deleted(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    before = h.dump()
    h.fake.fail_next("POST", TX_PATH, 404)  # e.g. the budget vanished right after the sync

    result = h.apply(plan, opts, export_b())

    # Without the stop every PUT would answer 404 too and the entries would be marked
    # DELETED_IN_YNAB for good (and a 404 on DELETE counts as "removed").
    assert h.writes() == [("POST", TX_PATH)]
    assert sorted(f.action for f in result.failures) == ["CREATE", "DELETE", "UPDATE", "UPDATE"]
    assert result.updated == result.removed == [] and result.ignored_deleted == 0
    assert any("no longer finds the budget or the account" in w for w in result.warnings)
    assert h.dump() == before
    h.fake.reset_log()
    assert h.run(export_b()).ok
    assert h.live() == FINAL_B


def test_F17_a_create_rejected_for_every_row_costs_a_bounded_number_of_requests(h):
    many = export(*_many_rows(150))
    plan, opts = h.plan(many)
    h.fake.edit_account(BUDGET, ACCT, deleted=True)  # deleted in YNAB after the plan was made

    result = h.apply(plan, opts, many)

    posts = [p for m, p in h.writes() if m == "POST"]
    # Not 2n - 1 = 199 + 99 requests (YNAB allows 200 per hour), just the per-chunk allowance.
    assert len(posts) == (1 + _bisect_allowance(100)) + (1 + _bisect_allowance(50)) == 36
    assert len(result.failures) == 150 and result.created == [] and not result.rate_limited
    assert all("account_id does not exist" in f.error for f in result.failures)
    assert any("not retried row by row" in f.error for f in result.failures)
    assert h.ledger.list_for_account(ACCT) == []


def test_one_rejected_row_in_a_full_chunk_is_still_isolated_within_the_allowance(tmp_path):
    fake = FakeYnab(today="2026-09-30")
    fake.add_budget(BUDGET, currency="CHF")
    fake.add_account(BUDGET, ACCT, "Revolut")
    rows = _many_rows(99)
    rows.insert(60, ("2026-10-05 09:00:00", "Future Shop", "-777.00"))  # "in the future"
    statement = export(*rows)
    h = Harness(tmp_path, fake)
    try:
        result = h.run(statement)
        posts = [p for m, p in h.writes() if m == "POST"]
        assert 2 <= len(posts) <= 1 + _bisect_allowance(100)
        assert [f.tx.payee for f in result.failures] == ["Future Shop"]
        assert "future" in result.failures[0].error
        assert len(result.created) == 99
        assert h.ledger.counts_by_status(ACCT)["ACTIVE"] == 99
    finally:
        h.conn.close()


def test_a_locked_database_after_a_confirmed_create_is_reported_and_converges(h, tmp_path):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    h.conn.execute("PRAGMA busy_timeout = 0")  # same as a lock held past the 10 s timeout
    other = sqlite3.connect(tmp_path / "tx.db", isolation_level=None)
    locked = {"done": False}

    def lock_after_post(method: str, url: str, headers: Any, body: Any, timeout: float) -> Any:
        if other.in_transaction:
            other.execute("ROLLBACK")  # the other writer is done by the next request
        out = h.fake.transport(method, url, headers, body, timeout)
        if method == "POST" and not locked["done"]:
            other.execute("BEGIN IMMEDIATE")  # another writer holds the DB right now
            locked["done"] = True
        return out

    try:
        result = h.apply(plan, opts, export_b(), client=h.fake.client(transport=lock_after_post))
    finally:
        if other.in_transaction:
            other.execute("ROLLBACK")
        other.close()

    assert locked["done"]
    assert [ref.amount for ref in result.created] == [-11_200]  # it is in YNAB
    assert not result.ok
    assert [f.action for f in result.failures] == ["CREATE"]
    assert "could not be saved" in result.failures[0].error
    assert h.ledger.get(ACCT, COOP_NEW) is None
    assert len(result.updated) == 2 and len(result.removed) == 1  # the rest went on
    h.conn.execute("PRAGMA busy_timeout = 10000")
    h.fake.reset_log()
    again = h.run(export_b())
    assert again.ok and h.writes() == []  # adopted, not POSTed twice
    assert h.entry(COOP_NEW).ynab_tx_id == h.tx_id(COOP_NEW)
    assert h.live() == FINAL_B


def test_failing_to_mark_the_mirror_stale_never_hides_the_result(h, monkeypatch, caplog):
    def broken(*_args: Any) -> None:
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(executor, "mark_mirror_stale", broken)
    with caplog.at_level(logging.WARNING, logger="ynab_helper.importer.executor"):
        result = h.run(export_a())

    assert result.ok and len(result.created) == 6
    assert h.ledger.counts_by_status(ACCT)["ACTIVE"] == 6
    assert any("could not mark the mirror stale" in r.getMessage() for r in caplog.records)


def test_the_post_create_sync_refreshes_the_ledger(h):
    """§6.5: every sync is followed by refresh_from_mirror, also the one after duplicates."""
    plan, opts = h.plan(export_b())
    legacy_key = "YNAB:-77700:2026-09-01:1"
    h.ledger.insert(
        LedgerEntry(
            account_id=ACCT,
            key=legacy_key,
            ynab_import_id=legacy_key,
            status=LedgerStatus.UNLINKED.value,
            csv_date="2026-09-01",
            csv_amount=-77_700,
            csv_state="COMPLETED",
            identity="legacy",
        )
    )
    # After our sync: v1 finally created the legacy row, and the Migros row came from elsewhere.
    legacy_tx = h.fake.add_transaction(
        BUDGET, ACCT, amount=-77_700, date="2026-09-01", import_id=legacy_key, cleared="cleared"
    )
    h.fake.add_transaction(BUDGET, ACCT, amount=-45_300, date="2026-10-11", import_id=MIGROS)

    result = h.apply(plan, opts, export_b())

    assert result.ok and len(result.linked) == 1
    linked = h.entry(legacy_key)
    assert linked.status == LedgerStatus.ACTIVE and linked.ynab_tx_id == legacy_tx


def test_F17_a_429_on_a_write_makes_throttled_syncs_back_off(h):
    _imported_a(h)
    plan, opts = h.plan(export_b())
    h.fake.fail_next("PATCH", TX_PATH, 429, retry_after=900)

    result = h.apply(plan, opts, export_b())

    assert result.rate_limited
    assert h.mirror.get_last_sync_at(BUDGET) is None  # stale: writes happened
    h.fake.reset_log()
    status_sync = sync_and_refresh(h.client(), h.conn, BUDGET, ACCT)  # e.g. /status right after
    assert status_sync.rate_limited and h.fake.request_count == 0
    assert status_sync.retry_after is not None and 0 < status_sync.retry_after <= 900
    forced = sync_and_refresh(h.client(), h.conn, BUDGET, ACCT, force=True)  # re-sent statement
    assert forced.ok and h.fake.request_count > 0
    assert sync_and_refresh(h.client(), h.conn, BUDGET, ACCT, min_interval=0).ok  # backoff over
