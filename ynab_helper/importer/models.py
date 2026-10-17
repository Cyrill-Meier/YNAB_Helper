"""Importer data model: plan options, planned actions, the plan and the import result.

The planner (:mod:`ynab_helper.importer.planner`) turns a parsed Revolut export, the
account's ledger and a YNAB mirror snapshot into a :class:`Plan`: an ordered list of
:class:`PlannedAction` objects that carry *everything* the executor needs — the exact YNAB
payload or PATCH fields, and the ledger changes to apply once YNAB confirmed the write
(docs/DESIGN.md §6.2-§6.4). The executor applies them and reports an :class:`ImportResult`
(§6.3), which CLI, bot and web render; it is JSON-safe via :meth:`ImportResult.to_dict`.

Ledger effects of an action, applied by the executor **only after YNAB confirmed the write**
(LINK/LEDGER actions are local and applied first, in plan order). With ``store`` a
``LedgerStore`` and ``a`` the action:

* CREATE — ``a.entry`` is the entry as it must look afterwards, but with status UNLINKED and
  no ``ynab_tx_id`` (a placeholder: an ACTIVE entry needs an id). After YNAB created
  transaction ``t``: if ``a.entry_exists`` →
  ``store.update(a.account_id, a.entry_key, **a.success_updates(t.id, t.import_id))``, else
  ``store.insert(a.success_entry(t.id, t.import_id))``. If YNAB answers "duplicate import_id",
  resolve it after the post-create sync with
  :func:`~ynab_helper.importer.planner.resolve_duplicate`.
* UPDATE / DELETE — after success: ``store.update(a.account_id, a.entry_key,
  **a.ledger_updates)`` (a DELETE's updates carry the status REVERTED/SUPERSEDED).
* LINK / LEDGER — if ``a.entry`` is set and not ``a.entry_exists``: ``store.insert(a.entry)``;
  otherwise ``store.update(a.account_id, a.entry_key, **a.ledger_updates)``.
* Every kind — if ``a.replaces_key`` is set, also
  ``store.update(a.account_id, a.replaces_key, **a.tombstone_updates())`` (the old entry of a
  re-created row becomes a REKEYED tombstone pointing at ``a.entry_key``).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from ynab_helper.storage.ledger import CsvState, LedgerEntry, LedgerStatus

if TYPE_CHECKING:
    from ynab_helper.revolut.parser import ParsedExport, RevolutRow

__all__ = [
    "ActionKind",
    "Change",
    "CsvState",
    "Failure",
    "ImportResult",
    "LedgerEntry",
    "LedgerStatus",
    "MatchInfo",
    "MatchMethod",
    "MatchOutcome",
    "Plan",
    "PlanOptions",
    "PlanSkip",
    "PlannedAction",
    "Removal",
    "TxRef",
    "entry_update_fields",
]

_EMPTY: Mapping[str, Any] = MappingProxyType({})

# ── reason / skip codes ──────────────────────────────────────────────────────

#: CREATE reasons.
REASON_NEW = "new"  # unmatched CSV row
REASON_REAPPEARED = "reappeared"  # a REVERTED/SUPERSEDED entry is live again (§6.2 step 3)
REASON_UNLINKED_MISSING = "unlinked_missing"  # UNLINKED entry not in YNAB → POST again
#: LINK reasons.
REASON_UNLINKED_RESOLVED = "unlinked_resolved"
REASON_DUPLICATE_RESOLVED = "duplicate_resolved"
REASON_ADOPTED = "adopted"  # unmatched row ↔ unlinked YNAB transaction carrying its key
#: UPDATE reasons (the most significant change wins).
REASON_SETTLEMENT = "settlement"  # matched by the settlement pass (amount changed)
REASON_CLEARED = "cleared"  # pending completed → cleared
REASON_AMOUNT = "amount_changed"
REASON_REDATED = "redated"
REASON_MEMO = "pending_marker"  # only the "(pending)" marker was stripped
#: DELETE reasons.
REASON_REVERTED = "reverted"
REASON_SUPERSEDED = "superseded"
#: LEDGER reasons.
REASON_REFRESH = "refresh"  # csv_* / started_at backfill only
REASON_DELETED_IN_YNAB = "deleted_in_ynab"
REASON_NEVER_IMPORTED = "reverted_never_imported"
REASON_LINK_UNKNOWN = "duplicate_unresolved"

#: Skip codes added by the planner (the parser's ``SKIP_*`` codes are passed through).
SKIP_REVERTED_NEVER_IMPORTED = "reverted_never_imported"
SKIP_PENDING = "pending_skipped"
SKIP_PENDING_OLDER_EXPORT = "pending_older_export"  # unmatched pending in an older statement


class ActionKind(StrEnum):
    """What a :class:`PlannedAction` does. Only CREATE/UPDATE/DELETE write to YNAB."""

    CREATE = "CREATE"  # POST a new YNAB transaction
    LINK = "LINK"  # local: link a ledger entry to an existing YNAB transaction
    UPDATE = "UPDATE"  # PATCH an existing YNAB transaction
    DELETE = "DELETE"  # DELETE a YNAB transaction
    LEDGER = "LEDGER"  # local: ledger-only change (backfill, status)

    @property
    def writes_ynab(self) -> bool:
        return self in (ActionKind.CREATE, ActionKind.UPDATE, ActionKind.DELETE)


#: Execution phase of each kind; the plan lists actions in this order (§6.3).
ACTION_PHASE: Mapping[ActionKind, int] = MappingProxyType(
    {
        ActionKind.LINK: 0,
        ActionKind.LEDGER: 1,
        ActionKind.CREATE: 2,
        ActionKind.UPDATE: 3,
        ActionKind.DELETE: 4,
    }
)


class MatchMethod(StrEnum):
    """Which pass of §6.2 step 2 matched a CSV row to a ledger entry."""

    TIMESTAMP = "timestamp"  # pass 1: started_at identity (modulo a learned export offset)
    KEY = "key"  # pass 2: key identity (per amount/date group), legacy entry without started_at
    DATE_SHIFT = "date_shift"  # pass 3: legacy row re-dated across midnight
    SETTLEMENT = "settlement"  # pass 4: pending settled at a different amount
    PRE_SETTLEMENT = "pre_settlement"  # pass 4b: an older export still shows the pending
    DUPLICATE = "duplicate"  # executor: CREATE answered "duplicate import_id"
    ADOPTED = "adopted"  # step 4: a YNAB transaction with the row's key that no entry knows


# ── options ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class PlanOptions:
    """Per-import options (§6.2).

    * ``approved`` — the user's auto_approve; sent on creates only.
    * ``skip_pending`` — do not CREATE PENDING rows. Never affects matching, updates or the
      superseded sweep (F09).
    * ``allow_sweep`` — superseded sweep enabled; the service sets it from
      :func:`~ynab_helper.importer.planner.compute_allow_sweep` (False for an older export,
      F36). Also gates re-creating removed rows that reappear (an older export must not
      resurrect them).
    """

    account_id: str
    approved: bool
    skip_pending: bool = False
    allow_sweep: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.account_id, str) or not self.account_id:
            raise ValueError("PlanOptions.account_id must be a non-empty string")


# ── result building blocks (§6.3) ────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class TxRef:
    """A transaction as shown to the user: date, amount (milliunits), payee, YNAB id."""

    date: str
    amount: int
    payee: str
    ynab_tx_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "amount": self.amount,
            "payee": self.payee,
            "ynab_tx_id": self.ynab_tx_id,
        }


@dataclass(frozen=True, slots=True)
class Change:
    """An updated YNAB transaction: ``fields`` maps field → (old, new)."""

    tx: TxRef
    fields: Mapping[str, tuple[Any, Any]]
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx": self.tx.to_dict(),
            "fields": {name: [old, new] for name, (old, new) in self.fields.items()},
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class Removal:
    """A YNAB transaction removed by the import (reason ``reverted`` or ``superseded``)."""

    tx: TxRef
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {"tx": self.tx.to_dict(), "reason": self.reason}


@dataclass(frozen=True, slots=True)
class Failure:
    """A planned action that YNAB (or the ledger) refused; ``error`` is safe to show."""

    action: str
    tx: TxRef | None
    error: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "tx": self.tx.to_dict() if self.tx is not None else None,
            "error": self.error,
        }


# ── planned actions ──────────────────────────────────────────────────────────


def _frozen_mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not value:
        return _EMPTY
    return MappingProxyType(dict(value))


def entry_update_fields(entry: LedgerEntry) -> dict[str, Any]:
    """Every column of ``entry`` that ``LedgerStore.update`` accepts (all but the identity
    columns ``account_id``/``key`` and the timestamps)."""
    skip = {"account_id", "key", "first_seen_at", "updated_at"}
    return {f.name: getattr(entry, f.name) for f in dataclasses.fields(entry) if f.name not in skip}


@dataclass(frozen=True, slots=True, kw_only=True)
class PlannedAction:
    """One step of a :class:`Plan` (see the module docstring for the ledger effects).

    * ``kind``/``reason`` — what and why (reason codes ``REASON_*`` of this module).
    * ``account_id``/``entry_key`` — the ledger entry the action is about. For a CREATE of a
      new row, ``entry_key`` is the chosen import_id (= the new ledger key, §6.4).
    * ``row`` — the CSV row (None for sweep deletes).
    * ``ynab_tx_id`` — the YNAB transaction acted on (UPDATE/DELETE/LINK; None for CREATE).
    * ``payload`` — CREATE: the exact YNAB transaction object to POST
      (``account_id, date, amount, payee_name, memo, cleared, approved, import_id``).
    * ``fields`` — UPDATE: the PATCH object ``{"id", <changed fields>}``.
    * ``changes`` — UPDATE: field → (old, new) for reporting (:class:`Change`).
    * ``entry_exists`` — CREATE/LINK: whether ``entry_key`` already exists in the ledger.
    * ``before``/``after`` — the transaction before/after the action, for rendering.
    * ``note`` — optional human-readable detail (already in the plan's warnings if relevant).
    """

    kind: ActionKind
    account_id: str
    entry_key: str
    reason: str
    row: RevolutRow | None = None
    ynab_tx_id: str | None = None
    payload: Mapping[str, Any] = field(default_factory=lambda: _EMPTY)
    fields: Mapping[str, Any] = field(default_factory=lambda: _EMPTY)
    changes: Mapping[str, tuple[Any, Any]] = field(default_factory=lambda: _EMPTY)
    entry: LedgerEntry | None = None
    entry_exists: bool = False
    ledger_updates: Mapping[str, Any] = field(default_factory=lambda: _EMPTY)
    replaces_key: str | None = None
    before: TxRef | None = None
    after: TxRef | None = None
    note: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", ActionKind(self.kind))
        for name in ("payload", "fields", "changes", "ledger_updates"):
            object.__setattr__(self, name, _frozen_mapping(getattr(self, name)))
        if self.kind is ActionKind.CREATE:
            import_id = self.payload.get("import_id")
            if not self.payload or not isinstance(import_id, str) or not import_id:
                raise ValueError("a CREATE action needs a payload with an import_id")
            if self.entry is None or self.entry.key != self.entry_key:
                raise ValueError("a CREATE action needs the ledger entry to write on success")
        if self.kind is ActionKind.UPDATE and (
            not self.ynab_tx_id or self.fields.get("id") != self.ynab_tx_id or len(self.fields) < 2
        ):
            raise ValueError("an UPDATE action needs fields {'id': ynab_tx_id, …changes}")
        if self.kind is ActionKind.DELETE and not self.ynab_tx_id:
            raise ValueError("a DELETE action needs ynab_tx_id")

    # -- executor helpers --------------------------------------------------------

    @property
    def import_id(self) -> str | None:
        """CREATE: the import_id that is POSTed."""
        value = self.payload.get("import_id")
        return value if isinstance(value, str) else None

    @property
    def status_on_success(self) -> str | None:
        """The ledger status this action sets once applied (e.g. REVERTED for a DELETE)."""
        if self.kind is ActionKind.CREATE:
            return LedgerStatus.ACTIVE.value
        status = self.ledger_updates.get("status")
        if status is None and self.entry is not None:
            return self.entry.status
        return str(status) if status is not None else None

    def ynab_payload(self) -> dict[str, Any]:
        """CREATE: a fresh, mutable copy of the POST payload."""
        return dict(self.payload)

    def ynab_fields(self) -> dict[str, Any]:
        """UPDATE: a fresh, mutable copy of the PATCH object (``id`` + changed fields)."""
        return dict(self.fields)

    def success_entry(self, ynab_tx_id: str, import_id: str | None = None) -> LedgerEntry:
        """CREATE of a new row: the ledger entry to insert after YNAB created ``ynab_tx_id``.

        ``import_id`` is the import_id YNAB reports (defaults to the one POSTed).
        """
        if self.kind is not ActionKind.CREATE or self.entry is None:
            raise ValueError("success_entry() applies to CREATE actions")
        return dataclasses.replace(
            self.entry,
            status=LedgerStatus.ACTIVE.value,
            ynab_tx_id=ynab_tx_id,
            ynab_import_id=import_id or self.entry.ynab_import_id,
        )

    def success_updates(
        self, ynab_tx_id: str | None = None, import_id: str | None = None
    ) -> dict[str, Any]:
        """Ledger column changes for ``entry_key`` once the action succeeded.

        For a CREATE of an existing entry, pass the created transaction's id (and import_id):
        the result also sets status ACTIVE and the link. For other kinds it is a copy of
        ``ledger_updates``.
        """
        updates = dict(self.ledger_updates)
        if self.kind is ActionKind.CREATE:
            if not ynab_tx_id:
                raise ValueError("a CREATE needs the created transaction's id")
            updates.update(
                status=LedgerStatus.ACTIVE.value,
                ynab_tx_id=ynab_tx_id,
                ynab_import_id=import_id or self.import_id,
            )
        return updates

    def tombstone_updates(self) -> dict[str, Any] | None:
        """Changes for ``replaces_key`` (REKEYED → ``entry_key``), or None."""
        if not self.replaces_key:
            return None
        return {"status": LedgerStatus.REKEYED.value, "rekeyed_to": self.entry_key}

    def to_change(self) -> Change:
        """UPDATE: the :class:`Change` to report."""
        tx = self.before or self.after or TxRef("", 0, "", self.ynab_tx_id)
        return Change(tx=tx, fields=dict(self.changes), reason=self.reason)

    def to_removal(self) -> Removal:
        """DELETE: the :class:`Removal` to report."""
        tx = self.before or TxRef("", 0, "", self.ynab_tx_id)
        return Removal(tx=tx, reason=self.reason)

    def tx_ref(self) -> TxRef | None:
        """The transaction to name in a report or failure (after, else before)."""
        return self.after or self.before

    def sort_key(self) -> tuple[int, int, str, str]:
        """Execution order: phase, CSV file order (sweep deletes last), then entry key."""
        line = self.row.line_no if self.row is not None else 1 << 30
        return (ACTION_PHASE[self.kind], line, self.account_id, self.entry_key)


@dataclass(frozen=True, slots=True)
class PlanSkip:
    """A CSV row that is not imported: parser skips (bad rows, DECLINED/FAILED…) and planner
    skips (``reverted_never_imported``, ``pending_skipped``)."""

    line_no: int
    code: str
    reason: str
    row: RevolutRow | None = None

    def as_tuple(self) -> tuple[int, str]:
        return (self.line_no, self.reason)


@dataclass(frozen=True, slots=True)
class MatchInfo:
    """Diagnostics: which ledger entry a CSV row was matched to, and how."""

    line_no: int
    row_key: str
    entry_key: str
    method: MatchMethod
    offset_seconds: int | None = None  # pass 1 only: row.started_at − entry.started_at

    def to_dict(self) -> dict[str, Any]:
        return {
            "line_no": self.line_no,
            "row_key": self.row_key,
            "entry_key": self.entry_key,
            "method": self.method.value,
            "offset_seconds": self.offset_seconds,
        }


@dataclass(slots=True)
class MatchOutcome:
    """What :func:`~ynab_helper.importer.planner.actions_for_match` decided for one pair."""

    actions: list[PlannedAction] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    skipped: list[PlanSkip] = field(default_factory=list)
    unchanged: bool = False  # matched, nothing written to YNAB, nothing linked
    ignored_deleted: bool = False  # the entry is (now) DELETED_IN_YNAB: never re-created

    def extend(self, other: MatchOutcome) -> None:
        self.actions.extend(other.actions)
        self.warnings.extend(other.warnings)
        self.skipped.extend(other.skipped)
        self.unchanged = self.unchanged or other.unchanged
        self.ignored_deleted = self.ignored_deleted or other.ignored_deleted


@dataclass(slots=True)
class Plan:
    """The planner's output: actions in execution order plus what to report (§6.2).

    ``warnings`` starts with the parser's warnings, then the planner's. ``counters`` holds
    statistics (rows, matches per pass, actions per kind…); ``matches`` and ``offsets``
    (learned export offsets in seconds → observations) are for debugging.
    ``reserved_import_ids`` are the import_ids this plan POSTs (the executor passes them to
    :meth:`~ynab_helper.importer.planner.PlanContext.create` when it plans mid-run, together
    with ``frame_offset`` and ``stale=stale_export``).

    * ``allow_sweep`` — whether the superseded sweep ran (``opts.allow_sweep`` and the export
      is not older than the ledger).
    * ``stale_export`` — the export was treated as older than the ledger (F36): no sweep, no
      re-created rows, pending rows left as they are.
    * ``frame_offset`` — the export's dominant offset (seconds) to the ledger's canonical
      started_at frame; started_at values the plan writes are ``row − frame_offset``.
    """

    account_id: str
    actions: list[PlannedAction] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    unchanged: int = 0
    ignored_deleted: int = 0
    skipped_rows: list[PlanSkip] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=dict)
    matches: list[MatchInfo] = field(default_factory=list)
    offsets: dict[int, int] = field(default_factory=dict)
    allow_sweep: bool = True
    window: tuple[str, str] | None = None
    reserved_import_ids: frozenset[str] = frozenset()
    frame_offset: int = 0
    stale_export: bool = False

    def by_kind(self, *kinds: ActionKind) -> list[PlannedAction]:
        """Actions of the given kinds, in plan order."""
        return [a for a in self.actions if a.kind in kinds]

    @property
    def ynab_actions(self) -> list[PlannedAction]:
        """CREATE/UPDATE/DELETE actions (the ones that write to YNAB)."""
        return [a for a in self.actions if a.kind.writes_ynab]

    @property
    def local_actions(self) -> list[PlannedAction]:
        """LINK/LEDGER actions (applied to the ledger only, before any YNAB write)."""
        return [a for a in self.actions if not a.kind.writes_ynab]

    @property
    def has_ynab_writes(self) -> bool:
        return any(a.kind.writes_ynab for a in self.actions)

    def summary(self) -> dict[str, Any]:
        """JSON-safe overview for logs and debugging."""
        return {
            "account_id": self.account_id,
            "window": list(self.window) if self.window else None,
            "allow_sweep": self.allow_sweep,
            "actions": {k.value: len(self.by_kind(k)) for k in ActionKind},
            "unchanged": self.unchanged,
            "ignored_deleted": self.ignored_deleted,
            "skipped": len(self.skipped_rows),
            "warnings": len(self.warnings),
            "counters": dict(self.counters),
            "offsets": {str(k): v for k, v in sorted(self.offsets.items())},
            "frame_offset": self.frame_offset,
            "stale_export": self.stale_export,
        }


# ── import result (§6.3) ─────────────────────────────────────────────────────


@dataclass(slots=True)
class ImportResult:
    """Structured outcome of an import; rendered by CLI/bot/web, stored as result_json."""

    dry_run: bool
    account_id: str
    currency: str | None = None
    window: tuple[str, str] | None = None
    rows_total: int = 0
    rows_pending: int = 0
    rows_reverted: int = 0
    created: list[TxRef] = field(default_factory=list)
    linked: list[TxRef] = field(default_factory=list)
    updated: list[Change] = field(default_factory=list)
    removed: list[Removal] = field(default_factory=list)
    unchanged: int = 0
    ignored_deleted: int = 0
    skipped_rows: list[tuple[int, str]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    failures: list[Failure] = field(default_factory=list)
    not_applied: int = 0
    rate_limited: bool = False

    @property
    def has_changes(self) -> bool:
        """True if YNAB (or the ledger links) changed — or would change, for a dry run."""
        return bool(self.created or self.linked or self.updated or self.removed)

    @property
    def ok(self) -> bool:
        """No failures, not rate limited, nothing left unapplied."""
        return not self.failures and not self.rate_limited and self.not_applied == 0

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe representation (web API, ``csv_files.result_json``)."""
        return {
            "dry_run": self.dry_run,
            "account_id": self.account_id,
            "currency": self.currency,
            "window": list(self.window) if self.window else None,
            "rows_total": self.rows_total,
            "rows_pending": self.rows_pending,
            "rows_reverted": self.rows_reverted,
            "created": [t.to_dict() for t in self.created],
            "linked": [t.to_dict() for t in self.linked],
            "updated": [c.to_dict() for c in self.updated],
            "removed": [r.to_dict() for r in self.removed],
            "unchanged": self.unchanged,
            "ignored_deleted": self.ignored_deleted,
            "skipped_rows": [[line_no, reason] for line_no, reason in self.skipped_rows],
            "warnings": list(self.warnings),
            "failures": [f.to_dict() for f in self.failures],
            "not_applied": self.not_applied,
            "rate_limited": self.rate_limited,
            "has_changes": self.has_changes,
            "ok": self.ok,
        }

    @classmethod
    def for_export(cls, export: ParsedExport, *, account_id: str, dry_run: bool) -> ImportResult:
        """An empty result with the export's facts (currency, window, row counts) filled in."""
        counts = export.state_counts()
        return cls(
            dry_run=dry_run,
            account_id=account_id,
            currency=export.currency,
            window=export.window,
            rows_total=len(export.rows),
            rows_pending=counts.get(CsvState.PENDING.value, 0),
            rows_reverted=counts.get(CsvState.REVERTED.value, 0),
        )

    def add_plan_counts(self, plan: Plan) -> None:
        """Copy the plan's report-only parts (counters, skips, warnings)."""
        self.unchanged += plan.unchanged
        self.ignored_deleted += plan.ignored_deleted
        self.skipped_rows.extend(s.as_tuple() for s in plan.skipped_rows)
        self.warnings.extend(plan.warnings)

    def record_applied(self, action: PlannedAction) -> None:
        """Report one successfully applied action (CREATE/LINK/UPDATE/DELETE; LEDGER is silent)."""
        if action.kind is ActionKind.CREATE:
            if action.after is not None:
                self.created.append(action.after)
        elif action.kind is ActionKind.LINK:
            ref = action.after or action.before
            if ref is not None:
                self.linked.append(ref)
        elif action.kind is ActionKind.UPDATE:
            self.updated.append(action.to_change())
        elif action.kind is ActionKind.DELETE:
            self.removed.append(action.to_removal())

    @classmethod
    def from_plan_dry_run(cls, plan: Plan, export: ParsedExport) -> ImportResult:
        """What the import *would* do: every planned action reported as if applied."""
        result = cls.for_export(export, account_id=plan.account_id, dry_run=True)
        result.add_plan_counts(plan)
        for action in plan.actions:
            result.record_applied(action)
        return result
