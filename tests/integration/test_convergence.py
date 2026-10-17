"""Convergence under failures (docs/DESIGN.md §1.2, §6.3, §12).

A realistic sequence of statements is imported through ``services.import_csv`` once without
faults; the fake YNAB's final :meth:`~tests.fakes.fake_ynab.FakeYnab.snapshot` is the
reference. Then, for every mutating request index ``k`` of that run and every kind of fault
(5xx, 404, 429, a network error before the request is applied, a lost response after it was
applied), the whole sequence is replayed against a fresh fake that fails request ``k``; each
statement is re-sent until the import reports ``ok`` (as the bot tells the user to do). The
final YNAB state must equal the reference — same transactions, same import_ids, nothing
duplicated, nothing lost — whatever failed where, and the local ledger must converge too.

The matrix runs twice: without client retries (every fault reaches the executor) and with
the production client's retries (a lost response is retried, YNAB answers
``duplicate_import_ids``, and the executor's post-create sync and LINK path take over).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FakeYnab
from ynab_helper import services

from .helpers import (
    ACCT,
    BUDGET,
    FIXTURES,
    OTHER,
    by_import_id,
    combined_csv,
    live_rows,
    mutating_methods,
    new_fake,
    user_ctx,
)

MAX_ATTEMPTS = 4

MIGROS = "YNAB:-45300:2026-10-11:1"
TOPUP = "YNAB:300000:2026-10-01:1"
AMAZON = "YNAB:-89990:2026-10-11:1"
HOTEL = "YNAB:-150000:2026-10-10:1"


SBB = "YNAB:-6800:2026-10-12:1"
PARKING = "YNAB:-4000:2026-10-14:1"


def _user_edits(fake: FakeYnab) -> None:
    """Between the statements the user annotates a pending and reconciles the top-up."""
    fake.user_edit_transaction(by_import_id(fake, ACCT, MIGROS)["id"], memo="(pending) | party")
    fake.user_edit_transaction(by_import_id(fake, ACCT, TOPUP)["id"], cleared="reconciled")


def _user_deletes(fake: FakeYnab) -> None:
    """The user deletes a transaction in YNAB: it must never come back."""
    fake.user_delete_transaction(by_import_id(fake, ACCT, SBB)["id"])


_HEADER = (
    b"Type,Product,Started Date,Completed Date,Description,Amount,Fee,Currency,State,Balance\n"
)
#: The day after s2: the 10-13 Coop pending completed, a parking hold and a transfer appear.
S3_EXTRA = (
    b"Card Payment,Current,2026-10-13 12:30:00,2026-10-14 06:00:00,Coop Pronto,-11.20,0.00,"
    b"CHF,COMPLETED,1135.09\n"
    b"Card Payment,Current,2026-10-14 09:00:00,,Parking Bern,-4.00,0.00,CHF,PENDING,\n"
    b"Transfer,Current,2026-10-14 10:00:00,2026-10-14 10:00:01,To Anna Example,-50.00,0.00,"
    b"CHF,COMPLETED,1085.09\n"
)
#: Two days later: the parking hold was reverted, a refund and an exchange came in.
S4_EXTRA = (
    b"Card Payment,Current,2026-10-13 12:30:00,2026-10-14 06:00:00,Coop Pronto,-11.20,0.00,"
    b"CHF,COMPLETED,1135.09\n"
    b"Card Payment,Current,2026-10-14 09:00:00,,Parking Bern,-4.00,0.00,CHF,REVERTED,\n"
    b"Transfer,Current,2026-10-14 10:00:00,2026-10-14 10:00:01,To Anna Example,-50.00,0.00,"
    b"CHF,COMPLETED,1085.09\n"
    b"Card Payment,Current,2026-10-15 18:00:00,2026-10-15 18:00:05,Zalando Refund,35.90,0.00,"
    b"CHF,COMPLETED,1120.99\n"
    b"Exchange,Current,2026-10-15 19:00:00,,Exchanged to EUR,-100.00,0.00,CHF,PENDING,\n"
)


def _without_coop_pending(csv: bytes) -> bytes:
    """s2 minus its 10-13 Coop pending row (it completed; S3_EXTRA has the new version)."""
    return b"".join(
        line
        for line in csv.splitlines(keepends=True)
        if not line.startswith(b"Card Payment,Current,2026-10-13 12:30:00")
    )


@dataclass(frozen=True)
class Step:
    account_id: str
    name: str
    csv: bytes | None = None
    edit: Callable[[FakeYnab], None] | None = None


S1 = combined_csv("export_pending_then_completed_a.csv", "export_same_amount_same_day_a.csv")
S2 = combined_csv("export_pending_then_completed_b.csv", "export_same_amount_same_day_b.csv")
S3 = _without_coop_pending(S2) + S3_EXTRA
S4 = _without_coop_pending(S2) + S4_EXTRA
SCENARIO = (
    Step(ACCT, "s1.csv", S1),  # pendings, same-amount same-day rows
    Step(ACCT, "user edits", edit=_user_edits),
    Step(ACCT, "s2.csv", S2),  # settle, FX re-quote, revert, renumbering (C01), new pending
    Step(OTHER, "basic.csv", (FIXTURES / "export_basic.csv").read_bytes()),
    Step(ACCT, "user deletes", edit=_user_deletes),
    Step(ACCT, "s3.csv", S3),  # pending completes, new hold and transfer
    Step(OTHER, "tz.csv", (FIXTURES / "export_tz_shift.csv").read_bytes()),  # +9 h rendering
    Step(ACCT, "s4.csv", S4),  # hold reverted, refund, new pending exchange
    Step(ACCT, "s2-again.csv", S2),  # an older statement re-sent: nothing may break
)


def run_scenario(
    fake: FakeYnab, data_dir: Path, monkeypatch: pytest.MonkeyPatch, *, retries: int = 0
) -> list[int]:
    """Import every statement until it is fully applied; returns the attempts per step.

    ``retries`` is the client's ``max_retries`` (production uses 2).
    """
    monkeypatch.setattr(services, "client_factory", lambda ctx: fake.client(max_retries=retries))
    data_dir.mkdir(parents=True, exist_ok=True)
    attempts: list[int] = []
    for step in SCENARIO:
        if step.edit is not None:
            step.edit(fake)
            continue
        assert step.csv is not None
        ctx = user_ctx(data_dir, step.account_id)
        for attempt in range(1, MAX_ATTEMPTS + 1):
            result = services.import_csv(ctx, step.csv, original_name=step.name, source_kind="web")
            if result.ok:
                attempts.append(attempt)
                break
        else:
            raise AssertionError(f"{step.name} never imported cleanly: {result.to_dict()}")
    return attempts


#: Ledger columns compared between runs: everything the planner decides with. Left out are
#: the timestamps and ``pushed_cleared``: after a PATCH YNAB applied but whose response was
#: lost, the next run finds YNAB already showing the cleared state and sends nothing, so
#: ``pushed_cleared`` keeps the older value it had when it was last written — informational
#: only, nothing reads it (the planner compares YNAB's cleared state with the statement).
LEDGER_COLUMNS = (
    "account_id",
    "key",
    "ynab_import_id",
    "ynab_tx_id",
    "status",
    "csv_date",
    "csv_amount",
    "csv_payee",
    "csv_type",
    "csv_state",
    "started_at",
    "pushed_amount",
    "pushed_date",
    "identity",
    "rekeyed_to",
    "last_error",
)


def ledger_state(fake: FakeYnab, data_dir: Path) -> list[tuple[Any, ...]]:
    """The user's ledger with YNAB ids replaced by what they point at (ids differ per run)."""
    conn = sqlite3.connect(data_dir / "transactions_42.db")
    try:
        rows = conn.execute(
            f"SELECT {', '.join(LEDGER_COLUMNS)} FROM import_ledger ORDER BY account_id, key"
        ).fetchall()
    finally:
        conn.close()
    tx_col = LEDGER_COLUMNS.index("ynab_tx_id")
    state = []
    for row in rows:
        values = list(row)
        if values[tx_col] is not None:
            tx = fake.tx(values[tx_col])
            values[tx_col] = (tx["import_id"], tx["account_id"], tx["deleted"])
        state.append(tuple(values))
    return state


@pytest.fixture(scope="module")
def reference(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    fake = new_fake()
    data_dir = tmp_path_factory.mktemp("reference")
    with pytest.MonkeyPatch.context() as mp:
        attempts = run_scenario(fake, data_dir, mp)
    assert attempts == [1] * len(attempts)
    return {
        "fake": fake,
        "snapshot": fake.snapshot(),
        "deleted": fake.snapshot(include_deleted=True),
        "ledger": ledger_state(fake, data_dir),
        "methods": mutating_methods(fake),
    }


def test_reference_run_reflects_the_statements(reference: dict[str, Any]) -> None:
    fake: FakeYnab = reference["fake"]
    assert len(reference["methods"]) >= 10  # creates, updates and deletes in both accounts
    assert set(reference["methods"]) == {"POST", "PATCH", "DELETE"}
    migros = by_import_id(fake, ACCT, MIGROS)
    assert (migros["cleared"], migros["memo"]) == ("cleared", "party")  # F33
    assert by_import_id(fake, ACCT, TOPUP)["cleared"] == "reconciled"  # F01
    amazon = by_import_id(fake, ACCT, AMAZON)
    assert (amazon["amount"], amazon["cleared"]) == (-90_410, "cleared")
    assert fake.find_by_import_id(ACCT, HOTEL, include_deleted=False) is None
    rows = live_rows(fake, ACCT)
    assert rows[("2026-10-05", -3_600, "SBB CFF FFS")] == 1
    assert rows[("2026-10-05", -3_800, "SBB CFF FFS")] == 1  # settled, not duplicated (C01)
    assert rows[("2026-10-05", -4_500, "Starbucks Bern")] == 3
    assert fake.find_by_import_id(ACCT, SBB, include_deleted=False) is None  # stays deleted
    assert fake.find_by_import_id(ACCT, PARKING, include_deleted=False) is None  # reverted
    coop = by_import_id(fake, ACCT, "YNAB:-11200:2026-10-13:1")
    assert coop["cleared"] == "cleared"
    exchange = by_import_id(fake, ACCT, "YNAB:-100000:2026-10-15:1")
    assert exchange["cleared"] == "uncleared"  # the older s2 re-sent did not sweep it (F36)
    assert rows[("2026-10-15", 35_900, "Zalando Refund")] == 1
    other = fake.transactions(BUDGET, account_id=OTHER)
    assert len(other) == len({t["import_id"] for t in other})
    fake.check_invariants()
    ledger = reference["ledger"]  # what the faulted runs' ledgers are compared with
    assert {row[0] for row in ledger} == {ACCT, OTHER}
    statuses = {row[LEDGER_COLUMNS.index("status")] for row in ledger}
    assert {"ACTIVE", "REVERTED", "DELETED_IN_YNAB"} <= statuses
    linked = [row for row in ledger if row[LEDGER_COLUMNS.index("ynab_tx_id")] is not None]
    assert len(linked) >= len(rows)


def _inject(kind: str, fake: FakeYnab, k: int, method: str) -> None:
    if kind == "http500":
        fake.fail_at_call(k, 500)
    elif kind == "http404":
        # A real 404 means "gone": for a DELETE that is the request having been applied.
        fake.fail_at_call(k, 404, after_apply=method == "DELETE")
    elif kind == "http429":
        fake.fail_at_call(k, 429, retry_after=30)
    elif kind == "network":
        fake.raise_network_error_at_call(k)
    elif kind == "lost_response":
        fake.raise_network_error_at_call(k, after_apply=True)
    elif kind == "http500_after_apply":
        fake.fail_at_call(k, 500, after_apply=True)
    else:  # pragma: no cover
        raise ValueError(kind)


@pytest.mark.parametrize("retries", [0, 2], ids=["no-retries", "client-retries"])
@pytest.mark.parametrize(
    "kind", ["http500", "http404", "http429", "network", "lost_response", "http500_after_apply"]
)
def test_any_single_failure_converges_to_the_reference_state(
    kind: str,
    retries: int,
    reference: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    methods: list[str] = reference["methods"]
    problems: list[str] = []
    for k, method in enumerate(methods, start=1):
        fake = new_fake()
        _inject(kind, fake, k, method)
        data_dir = tmp_path / f"{kind}-{k}"
        try:
            attempts = run_scenario(fake, data_dir, monkeypatch, retries=retries)
        except AssertionError as exc:
            problems.append(f"call {k} ({method}): {exc}")
            continue
        assert fake.pending_faults == 0, f"fault at call {k} never fired"
        if fake.snapshot() != reference["snapshot"]:
            problems.append(f"call {k} ({method}): final YNAB state differs from the reference")
        elif fake.snapshot(include_deleted=True) != reference["deleted"]:
            problems.append(f"call {k} ({method}): deleted transactions differ (create + delete?)")
        ledger = ledger_state(fake, data_dir)
        if ledger != reference["ledger"]:
            ours, ref = set(ledger), set(reference["ledger"])
            problems.append(
                f"call {k} ({method}): ledger differs: only here {sorted(ours - ref)}, "
                f"only in the reference {sorted(ref - ours)}"
            )
        if max(attempts) > 2:
            problems.append(f"call {k} ({method}): needed {attempts} attempts")
        fake.check_invariants()
    assert not problems, "\n".join(problems)
