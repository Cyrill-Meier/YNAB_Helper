"""Builders and an in-memory YNAB + ledger simulator for the planner tests.

* :func:`export` builds a realistic :class:`ParsedExport` by rendering a compact table as a
  Revolut CSV and parsing it with the real parser (so keys/occurrences are production ones).
* :func:`entry`, :func:`ytx`, :func:`view`, :func:`options` build ledger entries, mirror
  transactions and plan options with sensible defaults.
* :class:`Sim` plays both YNAB (import_id dedupe per account, deleted ids included) and the
  executor (ledger effects exactly as documented in ``ynab_helper.importer.models``), so a
  sequence of imports can be run end to end without SQLite or HTTP.
  :meth:`Sim.run_losing_ledger_writes` simulates a crash after the YNAB writes.
* :class:`StoreSim` is the same with the real :class:`LedgerStore` (SQLite) and
  ``refresh_from_mirror`` before every plan, as ``services.import_csv`` runs it; duplicate
  answers are resolved after all creates (§6.3 step 2). It can lose ledger writes too.
* :class:`T` / :func:`render` describe *real* Revolut transactions and render statements from
  them in any timezone and window (:func:`render_csv`: the CSV bytes, e.g. for the frozen v1
  importer), and :func:`truth` is what YNAB must show.
"""

from __future__ import annotations

import csv
import dataclasses
import io
import itertools
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ynab_helper.importer import (
    ActionKind,
    Plan,
    PlanContext,
    PlannedAction,
    PlanOptions,
    compute_allow_sweep,
    plan_import,
    resolve_duplicate,
)
from ynab_helper.revolut import ParsedExport, parse_export
from ynab_helper.storage import LedgerStore, open_tx_db
from ynab_helper.storage.ledger import LedgerEntry, LedgerStatus
from ynab_helper.storage.mirror import MirrorView, YnabTx

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
ACCT = "acct-main"
OTHER_ACCT = "acct-other"

#: YNAB ids are unique across every Sim of the process (Sims may share one budget's dicts).
_TX_IDS = itertools.count(1)

HEADER = [
    "Type",
    "Product",
    "Started Date",
    "Completed Date",
    "Description",
    "Amount",
    "Fee",
    "Currency",
    "State",
    "Balance",
]


# ── CSV / export builders ────────────────────────────────────────────────────


def csv_bytes(rows: Iterable[Sequence[str]]) -> bytes:
    """Render compact rows as a Revolut CSV.

    Each row is ``(started, description, amount[, state[, type[, fee]]])``; state defaults to
    COMPLETED, type to "Card Payment", fee to 0. COMPLETED rows get a Completed Date and a
    Balance; others leave both empty, as Revolut does.
    """
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(HEADER)
    for spec in rows:
        started, desc, amount, *rest = spec
        state = rest[0] if len(rest) > 0 else "COMPLETED"
        tx_type = rest[1] if len(rest) > 1 else "Card Payment"
        fee = rest[2] if len(rest) > 2 else "0.00"
        done = state == "COMPLETED"
        writer.writerow(
            [
                tx_type,
                "Current",
                started,
                started if done else "",
                desc,
                amount,
                fee,
                "CHF",
                state,
                "100.00" if done else "",
            ]
        )
    return out.getvalue().encode()


def export(*rows: Sequence[str]) -> ParsedExport:
    """A parsed export from compact rows (see :func:`csv_bytes`)."""
    return parse_export(csv_bytes(rows))


def fixture(name: str) -> ParsedExport:
    return parse_export(FIXTURES / name)


def row_by(exp: ParsedExport, *, started: str | None = None, key: str | None = None):
    """The single row of ``exp`` with the given started_at text or key."""
    found = [
        r
        for r in exp.rows
        if (started is None or r.started_at_text == started) and (key is None or r.key == key)
    ]
    assert len(found) == 1, found
    return found[0]


# ── ledger / mirror builders ─────────────────────────────────────────────────


def entry(key: str, **overrides: Any) -> LedgerEntry:
    """An ACTIVE csv-identity entry; csv_date/amount default from a ``YNAB:a:d:n`` key."""
    parts = key.split(":")
    amount = int(parts[1]) if len(parts) == 4 and parts[0] == "YNAB" else -1000
    day = parts[2] if len(parts) == 4 and parts[0] == "YNAB" else "2026-10-05"
    values: dict[str, Any] = {
        "account_id": ACCT,
        "key": key,
        "ynab_import_id": key,
        "ynab_tx_id": f"y-{key}",
        "status": LedgerStatus.ACTIVE.value,
        "csv_date": day,
        "csv_amount": amount,
        "csv_payee": "Shop",
        "csv_type": "Card Payment",
        "csv_state": "COMPLETED",
        "started_at": f"{day} 12:00:00",
        "pushed_cleared": "cleared",
        "pushed_amount": None,
        "pushed_date": None,
        "identity": "csv",
        "first_seen_at": "2026-10-01T00:00:00Z",
        "updated_at": "2026-10-01T00:00:00Z",
    }
    values.update(overrides)
    if values["status"] not in (LedgerStatus.ACTIVE.value,) and "ynab_tx_id" not in overrides:
        values["ynab_tx_id"] = None
    if values["pushed_amount"] is None and "pushed_amount" not in overrides:
        values["pushed_amount"] = values["csv_amount"]
    if values["pushed_date"] is None and "pushed_date" not in overrides:
        values["pushed_date"] = values["csv_date"]
    return LedgerEntry(**values)


def entry_for(row: Any, **overrides: Any) -> LedgerEntry:
    """An ACTIVE entry describing ``row`` exactly (as if imported from it)."""
    key = overrides.pop("key", row.key)
    values: dict[str, Any] = {
        "csv_date": row.date,
        "csv_amount": row.amount,
        "csv_payee": row.payee,
        "csv_type": row.type,
        "csv_state": row.state,
        "started_at": row.started_at_text if row.started_has_time else None,
        "pushed_cleared": row.cleared,
        "pushed_amount": row.amount,
        "pushed_date": row.date,
    }
    values.update(overrides)
    return entry(key, **values)


def ytx(tx_id: str, **overrides: Any) -> YnabTx:
    """A live, uncleared mirror transaction in ACCT."""
    values: dict[str, Any] = {
        "id": tx_id,
        "account_id": ACCT,
        "date": "2026-10-05",
        "amount": -1000,
        "payee_name": "Shop",
        "memo": None,
        "cleared": "uncleared",
        "approved": True,
        "deleted": False,
        "import_id": None,
        "category_name": None,
        "transfer_account_id": None,
        "has_subtransactions": False,
    }
    values.update(overrides)
    return YnabTx(**values)


def ytx_for(e: LedgerEntry, **overrides: Any) -> YnabTx:
    """The YNAB transaction ``e`` links to, as we pushed it."""
    values: dict[str, Any] = {
        "account_id": e.account_id or ACCT,
        "date": e.pushed_date or e.csv_date,
        "amount": e.pushed_amount if e.pushed_amount is not None else e.csv_amount,
        "payee_name": e.csv_payee,
        "cleared": e.pushed_cleared or "cleared",
        "import_id": e.ynab_import_id or e.key,
    }
    values.update(overrides)
    assert e.ynab_tx_id
    return ytx(e.ynab_tx_id, **values)


def view(*txs: YnabTx, account_id: str = ACCT) -> MirrorView:
    return MirrorView(account_id, txs)


def options(**overrides: Any) -> PlanOptions:
    values: dict[str, Any] = {"account_id": ACCT, "approved": True}
    values.update(overrides)
    return PlanOptions(**values)


def plan(
    exp: ParsedExport,
    entries: Iterable[LedgerEntry] = (),
    txs: Iterable[YnabTx] = (),
    **opts: Any,
) -> Plan:
    return plan_import(exp, list(entries), view(*txs), options(**opts))


def kinds(p: Plan) -> list[tuple[str, str]]:
    """``[(kind, reason), …]`` of a plan, in order."""
    return [(a.kind.value, a.reason) for a in p.actions]


def ynab_kinds(p: Plan) -> list[tuple[str, str]]:
    return [(a.kind.value, a.reason) for a in p.actions if a.kind.writes_ynab]


def only(p: Plan, kind: ActionKind) -> PlannedAction:
    found = p.by_kind(kind)
    assert len(found) == 1, [(a.kind, a.reason, a.entry_key) for a in p.actions]
    return found[0]


# ── simulator ────────────────────────────────────────────────────────────────


class Sim:
    """In-memory YNAB account + ledger + executor for end-to-end planner scenarios."""

    def __init__(self, account_id: str = ACCT) -> None:
        self.account_id = account_id
        self.ledger: dict[tuple[str, str], LedgerEntry] = {}
        self.txs: dict[str, YnabTx] = {}
        self.calls: list[tuple[Any, ...]] = []

    # -- state ------------------------------------------------------------------

    def entries(self) -> list[LedgerEntry]:
        return [self.ledger[k] for k in sorted(self.ledger)]

    def get(self, key: str, account_id: str | None = None) -> LedgerEntry:
        return self.ledger[(account_id or self.account_id, key)]

    def view(self) -> MirrorView:
        return MirrorView(self.account_id, self.txs.values())

    def live(self) -> list[YnabTx]:
        """Live transactions of the account, ordered by date then id."""
        return sorted(
            (t for t in self.txs.values() if not t.deleted and t.account_id == self.account_id),
            key=lambda t: (t.date, t.id),
        )

    def balance(self) -> int:
        return sum(t.amount for t in self.live())

    def add_entry(self, e: LedgerEntry) -> LedgerEntry:
        self.ledger[(e.account_id, e.key)] = e
        return e

    def add_tx(self, t: YnabTx) -> YnabTx:
        self.txs[t.id] = t
        return t

    def user_edit(self, tx_id: str, **fields: Any) -> None:
        self.txs[tx_id] = dataclasses.replace(self.txs[tx_id], **fields)

    def user_delete(self, tx_id: str) -> None:
        self.user_edit(tx_id, deleted=True)

    # -- running imports ----------------------------------------------------------

    def options(self, exp: ParsedExport, **overrides: Any) -> PlanOptions:
        values: dict[str, Any] = {
            "account_id": self.account_id,
            "approved": True,
            "allow_sweep": compute_allow_sweep(exp, self.entries(), account_id=self.account_id),
        }
        values.update(overrides)
        return PlanOptions(**values)

    def plan(self, exp: ParsedExport, **overrides: Any) -> Plan:
        return plan_import(exp, self.entries(), self.view(), self.options(exp, **overrides))

    def run(self, exp: ParsedExport, **overrides: Any) -> Plan:
        """Plan and apply one import; returns the plan."""
        opts = self.options(exp, **overrides)
        p = plan_import(exp, self.entries(), self.view(), opts)
        self.apply(p, opts)
        return p

    def apply(self, p: Plan, opts: PlanOptions) -> None:
        for action in p.actions:
            self._apply(action, p, opts)

    def run_losing_ledger_writes(self, exp: ParsedExport, **overrides: Any) -> Plan:
        """Plan and apply one import whose YNAB writes succeed but whose ledger writes are all
        lost (a crash before the ledger commit, or a lost response)."""
        opts = self.options(exp, **overrides)
        p = plan_import(exp, self.entries(), self.view(), opts)
        saved = dict(self.ledger)
        self.apply(p, opts)
        self.ledger = saved
        return p

    def _apply(self, a: PlannedAction, p: Plan, opts: PlanOptions) -> None:
        if a.kind in (ActionKind.LINK, ActionKind.LEDGER):
            if a.entry is not None and not a.entry_exists:
                self._insert(a.entry)
            else:
                self._update(a.account_id, a.entry_key, a.ledger_updates)
        elif a.kind is ActionKind.CREATE:
            payload = a.ynab_payload()
            import_id = payload["import_id"]
            dup = [
                t
                for t in self.txs.values()
                if t.account_id == payload["account_id"] and t.import_id == import_id
            ]
            if dup:
                self.calls.append(("POST-DUPLICATE", import_id))
                ctx = PlanContext.create(
                    self.entries(),
                    self.view(),
                    opts,
                    reserved_import_ids=p.reserved_import_ids,
                    frame_offset=p.frame_offset,
                    stale=p.stale_export,
                )
                out = resolve_duplicate(a, self.view().by_import_id(import_id), opts, ctx)
                for follow in out.actions:
                    self._apply(follow, p, opts)
                return
            tx_id = f"new-{next(_TX_IDS)}"
            self.txs[tx_id] = YnabTx(
                id=tx_id,
                account_id=payload["account_id"],
                date=payload["date"],
                amount=payload["amount"],
                payee_name=payload["payee_name"],
                memo=payload["memo"],
                cleared=payload["cleared"],
                approved=payload["approved"],
                deleted=False,
                import_id=import_id,
                category_name=None,
                transfer_account_id=None,
                has_subtransactions=False,
            )
            self.calls.append(("POST", import_id, payload["amount"], payload["cleared"]))
            if a.entry_exists:
                self._update(a.account_id, a.entry_key, a.success_updates(tx_id, import_id))
            else:
                self._insert(a.success_entry(tx_id, import_id))
            self._tombstone(a)
        elif a.kind is ActionKind.UPDATE:
            fields = a.ynab_fields()
            tx_id = fields.pop("id")
            current = self.txs[tx_id]
            assert not current.deleted, f"PATCH of deleted transaction {tx_id}"
            self.txs[tx_id] = dataclasses.replace(current, **fields)
            self.calls.append(("PATCH", tx_id, tuple(sorted(fields.items()))))
            self._update(a.account_id, a.entry_key, a.ledger_updates)
        elif a.kind is ActionKind.DELETE:
            assert a.ynab_tx_id
            current = self.txs[a.ynab_tx_id]
            assert not current.deleted, f"DELETE of deleted transaction {a.ynab_tx_id}"
            self.txs[a.ynab_tx_id] = dataclasses.replace(current, deleted=True)
            self.calls.append(("DELETE", a.ynab_tx_id, a.reason))
            self._update(a.account_id, a.entry_key, a.ledger_updates)
        if a.kind is not ActionKind.CREATE:
            self._tombstone(a)

    def _tombstone(self, a: PlannedAction) -> None:
        tomb = a.tombstone_updates()
        if tomb and a.replaces_key:
            self._update(a.account_id, a.replaces_key, tomb)

    def _insert(self, e: LedgerEntry) -> None:
        k = (e.account_id, e.key)
        assert k not in self.ledger, f"ledger entry {k} already exists"
        self.ledger[k] = e

    def _update(self, account_id: str, key: str, updates: Any) -> None:
        k = (account_id, key)
        assert k in self.ledger, f"no ledger entry {k}"
        self.ledger[k] = dataclasses.replace(self.ledger[k], **dict(updates))


# ── truth-based statements and a store-backed simulator ──────────────────────


@dataclass
class T:
    """A real Revolut transaction in the home timezone (naive timestamps), mutable so a test
    can let it settle, re-quote, revert (``state``) or vanish (``state="GONE"``)."""

    started: datetime
    desc: str
    amount: str
    state: str = "PENDING"  # PENDING | COMPLETED | REVERTED | GONE
    completed: datetime | None = None
    type: str = "Card Payment"

    def complete(self, at: datetime, amount: str | None = None) -> None:
        self.state, self.completed = "COMPLETED", at
        if amount is not None:
            self.amount = amount


def at(text: str) -> datetime:
    return datetime.fromisoformat(text)


def done(started: str, desc: str, amount: str, **kw: Any) -> T:
    """A COMPLETED transaction, completed 30 s after it started."""
    start = at(started)
    return T(start, desc, amount, "COMPLETED", start + timedelta(seconds=30), **kw)


def render(
    txs: Iterable[T],
    *,
    tz_hours: float = 0,
    start: str | None = None,
    end: str | None = None,
) -> ParsedExport:
    """The statement Revolut renders ``tz_hours`` away from home for Started Dates (rendered)
    in [start, end]: ordered by Completed Date, pending/reverted rows interleaved by start."""
    return parse_export(render_csv(txs, tz_hours=tz_hours, start=start, end=end))


def render_csv(
    txs: Iterable[T],
    *,
    tz_hours: float = 0,
    start: str | None = None,
    end: str | None = None,
) -> bytes:
    """The CSV bytes of :func:`render` (e.g. for the frozen v1 importer)."""
    shift = timedelta(hours=tz_hours)
    rows = []
    for t in txs:
        if t.state == "GONE":
            continue
        started = t.started + shift
        day = started.date().isoformat()
        if (start and day < start) or (end and day > end):
            continue
        completed = t.completed + shift if t.completed and t.state == "COMPLETED" else None
        rows.append((completed or started, started, completed, t))
    rows.sort(key=lambda r: (r[0], r[1]))
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(HEADER)
    for _sort, started, completed, t in rows:
        writer.writerow(
            [
                t.type,
                "Current",
                started.strftime("%Y-%m-%d %H:%M:%S"),
                completed.strftime("%Y-%m-%d %H:%M:%S") if completed else "",
                t.desc,
                t.amount,
                "0.00",
                "CHF",
                t.state,
                "100.00" if completed else "",
            ]
        )
    return out.getvalue().encode()


def truth(txs: Iterable[T]) -> list[tuple[int, str]]:
    """What YNAB must show: (milliunits, cleared|uncleared) of every live transaction."""
    return sorted(
        (round(float(t.amount) * 1000), "cleared" if t.state == "COMPLETED" else "uncleared")
        for t in txs
        if t.state in ("PENDING", "COMPLETED")
    )


class StoreSim:
    """In-memory YNAB + the real SQLite :class:`LedgerStore` + an executor following the
    recipe in ``ynab_helper.importer.models``: ``refresh_from_mirror`` runs before every plan
    and "duplicate import_id" answers are resolved after all creates, then updates and
    deletes run (§6.3)."""

    def __init__(self, tmp_path: Path, account_id: str = ACCT) -> None:
        self.account_id = account_id
        self.conn = open_tx_db(
            tmp_path / "tx.db", default_account_id=account_id, backup_dir=tmp_path / "bk"
        )
        self.ledger = LedgerStore(self.conn)
        self.txs: dict[str, YnabTx] = {}
        self.calls: list[tuple[Any, ...]] = []

    def close(self) -> None:
        self.conn.close()

    def view(self) -> MirrorView:
        return MirrorView(self.account_id, self.txs.values())

    def live(self) -> list[YnabTx]:
        return sorted(
            (t for t in self.txs.values() if not t.deleted and t.account_id == self.account_id),
            key=lambda t: (t.date, t.id),
        )

    def state(self) -> list[tuple[int, str]]:
        """(amount, cleared|uncleared) of the account's live transactions, as :func:`truth`."""
        return sorted(
            (t.amount, "uncleared" if t.cleared == "uncleared" else "cleared") for t in self.live()
        )

    def user_edit(self, tx_id: str, **fields: Any) -> None:
        self.txs[tx_id] = dataclasses.replace(self.txs[tx_id], **fields)

    def entries(self) -> list[LedgerEntry]:
        return self.ledger.list_for_account(self.account_id)

    def plan(self, exp: ParsedExport, **overrides: Any) -> tuple[Plan, PlanOptions]:
        self.ledger.refresh_from_mirror(self.account_id, self.view())
        entries = self.entries()
        values: dict[str, Any] = {
            "account_id": self.account_id,
            "approved": True,
            "allow_sweep": compute_allow_sweep(exp, entries, account_id=self.account_id),
        }
        values.update(overrides)
        opts = PlanOptions(**values)
        return plan_import(exp, entries, self.view(), opts), opts

    def run(self, exp: ParsedExport, **overrides: Any) -> Plan:
        p, opts = self.plan(exp, **overrides)
        self.execute(p, opts)
        return p

    def writes(self, exp: ParsedExport, **overrides: Any) -> list[tuple[Any, ...]]:
        """Run one import; return the YNAB calls it made."""
        n = len(self.calls)
        self.run(exp, **overrides)
        return self.calls[n:]

    def run_losing_ledger_writes(self, exp: ParsedExport, **overrides: Any) -> Plan:
        """Run one import whose YNAB writes all succeed but whose ledger writes are all lost
        (the process died before the ledger commit, or YNAB's response never arrived)."""
        p, opts = self.plan(exp, **overrides)
        store = self.ledger
        self.ledger = _LostWrites(store)  # type: ignore[assignment]
        try:
            self.execute(p, opts)
        finally:
            self.ledger = store
        return p

    def execute(self, p: Plan, opts: PlanOptions) -> None:
        for a in p.local_actions:
            self._apply_local(a)
        duplicates = []
        for a in p.by_kind(ActionKind.CREATE):
            payload = a.ynab_payload()
            import_id = payload["import_id"]
            if any(
                t.account_id == payload["account_id"] and t.import_id == import_id
                for t in self.txs.values()
            ):
                self.calls.append(("POST-DUPLICATE", import_id))
                duplicates.append(a)
                continue
            tx_id = f"s{next(_TX_IDS)}"
            self.txs[tx_id] = YnabTx(
                id=tx_id,
                account_id=payload["account_id"],
                date=payload["date"],
                amount=payload["amount"],
                payee_name=payload["payee_name"],
                memo=payload["memo"],
                cleared=payload["cleared"],
                approved=payload["approved"],
                deleted=False,
                import_id=import_id,
                category_name=None,
                transfer_account_id=None,
                has_subtransactions=False,
            )
            self.calls.append(("POST", import_id, payload["amount"], payload["cleared"]))
            if a.entry_exists:
                self.ledger.update(a.account_id, a.entry_key, **a.success_updates(tx_id, import_id))
            else:
                self.ledger.insert(a.success_entry(tx_id, import_id))
            self._tombstone(a)
        follow_ups: list[PlannedAction] = []
        if duplicates:
            self.ledger.refresh_from_mirror(self.account_id, self.view())
            ctx = PlanContext.create(
                self.entries(),
                self.view(),
                opts,
                reserved_import_ids=p.reserved_import_ids,
                frame_offset=p.frame_offset,
                stale=p.stale_export,
            )
            for a in duplicates:
                out = resolve_duplicate(a, self.view().by_import_id(a.import_id), opts, ctx)
                for f in out.actions:
                    if f.kind.writes_ynab:
                        follow_ups.append(f)
                    else:
                        self._apply_local(f)
        for a in [*p.by_kind(ActionKind.UPDATE), *follow_ups]:
            fields = a.ynab_fields()
            tx_id = fields.pop("id")
            current = self.txs[tx_id]
            assert not current.deleted, f"PATCH of deleted transaction {tx_id}"
            self.txs[tx_id] = dataclasses.replace(current, **fields)
            self.calls.append(("PATCH", tx_id, tuple(sorted(fields.items()))))
            self.ledger.update(a.account_id, a.entry_key, **a.ledger_updates)
            self._tombstone(a)
        for a in p.by_kind(ActionKind.DELETE):
            assert a.ynab_tx_id
            current = self.txs[a.ynab_tx_id]
            assert not current.deleted, f"DELETE of deleted transaction {a.ynab_tx_id}"
            self.txs[a.ynab_tx_id] = dataclasses.replace(current, deleted=True)
            self.calls.append(("DELETE", a.ynab_tx_id, a.reason))
            self.ledger.update(a.account_id, a.entry_key, **a.ledger_updates)
            self._tombstone(a)

    def _apply_local(self, a: PlannedAction) -> None:
        if a.entry is not None and not a.entry_exists:
            self.ledger.insert(a.entry)
        else:
            self.ledger.update(a.account_id, a.entry_key, **a.ledger_updates)
        self._tombstone(a)

    def _tombstone(self, a: PlannedAction) -> None:
        tomb = a.tombstone_updates()
        if tomb and a.replaces_key:
            self.ledger.update(a.account_id, a.replaces_key, **tomb)


class _LostWrites:
    """A :class:`LedgerStore` stand-in whose writes vanish (reads go to the real store)."""

    def __init__(self, store: LedgerStore) -> None:
        self._store = store

    def insert(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def update(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def refresh_from_mirror(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._store, name)
