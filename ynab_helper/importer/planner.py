"""Import planner: CSV rows + ledger + YNAB mirror → :class:`Plan` (docs/DESIGN.md §6.2, §6.4).

**Pure**: no SQLite, no network, no clock. The same inputs always give the same plan (rows
in file order, ledger entries in key order). Every YNAB write is computed from YNAB's
*current* state as given by the :class:`MirrorView` (built right after a fresh delta sync).

Outline (:func:`plan_import`):

1. *Learn export offsets* — rows whose key equals the key of an entry whose row every later
   export still shows (ACTIVE, UNLINKED, DELETED_IN_YNAB, REVERTED) reveal the timezone the
   export was rendered in (:func:`learn_offsets`); same-amount, same-payee pairs inside the
   export window add votes (:func:`vote_offsets`, :func:`export_offsets`).
2. *Match* CSV rows to ledger entries of this account (REKEYED tombstones ignored), each side
   at most once (:func:`match_rows`): timestamp identity, (amount, date) group assignment for
   legacy and date-only data, exact key, legacy date shift, settlement — plus
   "pre-settlement" (4b): an older export still showing a since-settled pending at its old
   amount.
3. *Per-match actions* (:func:`actions_for_match`): cleared only ever moves uncleared →
   cleared, the memo is derived from YNAB's current memo (only the ``(pending)`` marker is
   stripped), amount/date are written only when YNAB still holds what we wrote, never on
   reconciled or split transactions. Deleted-in-YNAB rows are never re-created.
4. *Unmatched rows* → CREATE with a per-account unique import_id (:class:`ImportIdAllocator`),
   unless YNAB already holds the row under an import_id no ledger entry knows: a transaction
   with the row's amount and date (a create whose ledger write was lost, maybe settled since)
   is adopted; then, for a row whose key no ledger entry knows, the transaction carrying that
   key — live (adopted whatever the user changed) or deleted (recorded, never re-created).
   That is v1's import_id dedupe, so a lost, wiped or restored ledger never duplicates (R02).
5. *Superseded sweep* (only with ``opts.allow_sweep`` and an export that is not older than the
   ledger): unmatched pending entries the export provably covers, whose YNAB transaction is
   still uncleared, are deleted.

**Time frames.** A Revolut export renders timestamps in the phone's timezone. The ledger keeps
``started_at`` in one *canonical frame*: a match never moves an entry to another rendering, and
rows stored from a shifted export are normalised by the export's dominant offset
(:attr:`Plan.frame_offset`). ``csv_date`` stays the latest rendering (it is what YNAB shows).
Window and recency tests shift entries by every plausible offset (:class:`ExportFrame`).

**Older exports (F36).** Besides ``opts.allow_sweep`` (date based, §6.2), an export is treated
as older when a ledger entry started after the export's newest timestamp, or when it shows a
row pending that an earlier statement showed completed or reverted at the same instant
(Revolut never moves a row back to pending). An older export never sweeps, never re-creates
removed rows, and leaves pending rows alone (no create, no amount/date change).

The executor reuses :func:`actions_for_match` / :func:`resolve_duplicate` for rows it links
mid-run after YNAB answered "duplicate import_id" (§6.3 step 2), with a
:class:`PlanContext` from :meth:`PlanContext.create`.
"""

from __future__ import annotations

import dataclasses
import logging
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from ynab_helper.errors import YnabHelperError
from ynab_helper.importer.models import (
    REASON_ADOPTED,
    REASON_AMOUNT,
    REASON_CLEARED,
    REASON_DELETED_IN_YNAB,
    REASON_DUPLICATE_RESOLVED,
    REASON_LINK_UNKNOWN,
    REASON_MEMO,
    REASON_NEVER_IMPORTED,
    REASON_NEW,
    REASON_REAPPEARED,
    REASON_REDATED,
    REASON_REFRESH,
    REASON_REVERTED,
    REASON_SETTLEMENT,
    REASON_SUPERSEDED,
    REASON_UNLINKED_MISSING,
    REASON_UNLINKED_RESOLVED,
    SKIP_PENDING,
    SKIP_PENDING_OLDER_EXPORT,
    SKIP_REVERTED_NEVER_IMPORTED,
    ActionKind,
    MatchInfo,
    MatchMethod,
    MatchOutcome,
    Plan,
    PlannedAction,
    PlanOptions,
    PlanSkip,
    TxRef,
    unconfirmed_write,
)
from ynab_helper.money import format_money
from ynab_helper.revolut.parser import (
    STATE_COMPLETED,
    STATE_PENDING,
    STATE_REVERTED,
    ParsedExport,
    RevolutRow,
    has_pending_marker,
    strip_pending_marker,
)
from ynab_helper.storage.ledger import (
    IDENTITY_CSV,
    UNASSIGNED_ACCOUNT,
    CsvState,
    LedgerEntry,
    LedgerStatus,
)
from ynab_helper.storage.mirror import CLEARED, RECONCILED, UNCLEARED, MirrorView, YnabTx
from ynab_helper.ynab.models import MAX_IMPORT_ID_LENGTH

__all__ = [
    "MAX_EXPORT_OFFSET_SECONDS",
    "MIN_OFFSET_VOTES",
    "NEAR_MIDNIGHT_HOURS",
    "OFFSET_STEP_SECONDS",
    "ExportFrame",
    "ImportIdAllocator",
    "PlanContext",
    "actions_for_match",
    "compute_allow_sweep",
    "export_offsets",
    "is_another_transaction",
    "is_timezone_offset",
    "learn_offsets",
    "match_rows",
    "plan_import",
    "resolve_duplicate",
    "statement_range_is_current",
    "vote_offsets",
]

log = logging.getLogger(__name__)

#: Export re-rendering offsets are whole multiples of 15 minutes …
OFFSET_STEP_SECONDS = 15 * 60
#: … and two renderings of one instant differ by at most 26 h (UTC−12 … UTC+14), e.g. an
#: export from Sydney followed by one from Los Angeles.
MAX_EXPORT_OFFSET_SECONDS = 26 * 3600
#: Pass 3 (legacy date shift): the row must have started within this many hours of midnight.
NEAR_MIDNIGHT_HOURS = 3
#: A vote-only offset (never seen on a key-equal pair) needs this many supporting pairs —
#: or one *distinctive* pair (see :func:`export_offsets`).
MIN_OFFSET_VOTES = 2
#: At most this many "vanished" transactions are named individually in the warnings.
MAX_VANISHED_LISTED = 10

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
_ONE_DAY = timedelta(days=1)
_DAY_END = timedelta(hours=23, minutes=59, seconds=59)

#: Live entries rank before tombstones whenever several entries could take a row, so a row is
#: never "re-created" or ignored while its live entry exists (and that entry is not swept).
_STATUS_RANK: Mapping[str, int] = {
    LedgerStatus.ACTIVE.value: 0,
    LedgerStatus.UNLINKED.value: 1,
    LedgerStatus.DELETED_IN_YNAB.value: 2,
    LedgerStatus.SUPERSEDED.value: 3,
    LedgerStatus.REVERTED.value: 3,
}

#: Entries whose row every later export (covering its date) still shows: one that started
#: after an export's newest timestamp proves the export older (SUPERSEDED rows vanished).
_RECENCY_EVIDENCE = frozenset(
    {
        LedgerStatus.ACTIVE.value,
        LedgerStatus.UNLINKED.value,
        LedgerStatus.DELETED_IN_YNAB.value,
        LedgerStatus.REVERTED.value,
    }
)
#: Entries whose timestamps tell which timezone an export is rendered in: the same set. A
#: transaction the user deleted in YNAB, or one Revolut reverted, is still in every statement
#: at its instant, so it identifies the export's frame as well as a live one does (R06) —
#: otherwise a statement whose only overlap is such rows re-creates them. SUPERSEDED rows
#: vanished from Revolut and say nothing about later statements.
_FRAME_EVIDENCE = _RECENCY_EVIDENCE
#: Pass 4 (settlement) considers pendings in these statuses, live ones first: a pending the
#: user deleted in YNAB must be recognised when it settles at another amount, or the settled
#: row would be POSTed again (R01); an UNLINKED pending (a create whose outcome is unknown)
#: settles like an ACTIVE one.
_SETTLEMENT_STATUSES = (
    LedgerStatus.ACTIVE.value,
    LedgerStatus.UNLINKED.value,
    LedgerStatus.DELETED_IN_YNAB.value,
)

_LIVE_ROW_STATES = frozenset({STATE_COMPLETED, STATE_PENDING})


# ── small helpers ────────────────────────────────────────────────────────────


def _parse_ts(text: str | None) -> datetime | None:
    """A ledger ``started_at`` (``YYYY-MM-DD HH:MM:SS``) or None if absent/unparseable."""
    if not text:
        return None
    try:
        return datetime.strptime(text, _TIMESTAMP_FORMAT)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed.replace(tzinfo=None)


def _format_ts(value: datetime) -> str:
    return value.strftime(_TIMESTAMP_FORMAT)


def _row_started(row: RevolutRow) -> datetime | None:
    """The row's start timestamp, or None for date-only (legacy) exports: a midnight
    placeholder carries no identity and must neither match nor be stored."""
    return row.started_at if row.started_has_time else None


def _norm_payee(payee: str | None) -> str:
    """Payee comparison key: trimmed, case-insensitive (CSV-side payees only, F11)."""
    return (payee or "").strip().casefold()


def _day(iso: str) -> date | None:
    try:
        return date.fromisoformat(iso)
    except (TypeError, ValueError):
        return None


def _day_start(iso: str) -> datetime | None:
    day = _day(iso)
    return datetime(day.year, day.month, day.day) if day is not None else None


def _days_between(a: str, b: str) -> int | None:
    da, db = _day(a), _day(b)
    if da is None or db is None:
        return None
    return (da - db).days


def _in_window(day: str, window: tuple[str, str] | None) -> bool:
    return window is not None and window[0] <= day <= window[1]


def _seconds(delta: timedelta) -> int:
    return int(delta.total_seconds())


def is_timezone_offset(seconds: int) -> bool:
    """True if ``seconds`` looks like a timezone re-rendering of one instant (non-zero,
    a multiple of 15 minutes, at most 26 h)."""
    return (
        seconds != 0
        and abs(seconds) <= MAX_EXPORT_OFFSET_SECONDS
        and seconds % OFFSET_STEP_SECONDS == 0
    )


def _describe(day: str, amount: int, payee: str | None, currency: str | None) -> str:
    name = (payee or "").strip() or "(no payee)"
    return f"{day} {name} {format_money(amount, currency)}"


def _row_ref(row: RevolutRow, payee: str | None = None) -> TxRef:
    return TxRef(date=row.date, amount=row.amount, payee=payee if payee is not None else row.payee)


def _tx_ref(y: YnabTx, fallback_payee: str | None) -> TxRef:
    return TxRef(
        date=y.date, amount=y.amount, payee=y.payee_name or fallback_payee or "", ynab_tx_id=y.id
    )


_KEY_RE = re.compile(r"YNAB:(-?\d+):(\d{4}-\d{2}-\d{2}):(\d+)")


def _parse_key(key: str | None) -> tuple[int, str, int] | None:
    """``(amount, date, occurrence)`` of a CSV-derived id ``YNAB:{amount}:{date}:{n}``; None
    for opaque legacy keys (``YNAB:P:…``, ``acct:…``)."""
    match = _KEY_RE.fullmatch(key or "")
    if match is None:
        return None
    return int(match.group(1)), match.group(2), int(match.group(3))


def _state_distance(row_state: str, entry_state: str) -> int:
    """0: same state; 1: normal progress of a pending (→ completed/reverted); 2: anything
    else (an older export, or a different transaction)."""
    if row_state == entry_state:
        return 0
    return 1 if entry_state == CsvState.PENDING else 2


def _unique_pairs(pairs: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Pairs whose row and entry each have exactly one candidate (sorted by row)."""
    per_row = Counter(ri for ri, _ in pairs)
    per_entry = Counter(ei for _, ei in pairs)
    return sorted((ri, ei) for ri, ei in pairs if per_row[ri] == 1 and per_entry[ei] == 1)


# ── import_id choice (§6.4) ──────────────────────────────────────────────────


class ImportIdAllocator:
    """Chooses per-account unique YNAB import_ids for creates (§6.4).

    An id is *used* when any ledger entry of the account has it as key or ynab_import_id (any
    status, tombstones included), when the account's mirror holds it (deleted transactions
    included — YNAB keeps refusing those ids), or when this plan already chose it.
    """

    __slots__ = ("_chosen", "_owners", "_planned", "_ynab")

    def __init__(
        self,
        entries: Iterable[LedgerEntry],
        ynab_import_ids: Iterable[str],
        reserved: Iterable[str] = (),
    ) -> None:
        self._owners: dict[str, set[str]] = defaultdict(set)
        for entry in entries:
            self._owners[entry.key].add(entry.key)
            if entry.ynab_import_id:
                self._owners[entry.ynab_import_id].add(entry.key)
        self._ynab = frozenset(ynab_import_ids)
        self._planned: set[str] = set(reserved)
        self._chosen: list[str] = []

    def is_used(self, import_id: str) -> bool:
        return import_id in self._owners or import_id in self._ynab or import_id in self._planned

    def known_to_ledger(self, import_id: str) -> bool:
        """True if a ledger entry of the account has ``import_id`` as key or ynab_import_id."""
        return import_id in self._owners

    def is_planned(self, import_id: str) -> bool:
        """True if this plan (or the plan an executor context was built for) POSTs it."""
        return import_id in self._planned

    def is_free_for(self, import_id: str, key: str) -> bool:
        """True if ``import_id`` may be POSTed for the ledger entry ``key`` (used only by it)."""
        if import_id in self._ynab or import_id in self._planned:
            return False
        return self._owners.get(import_id, set()) <= {key}

    def reserve(self, import_id: str) -> str:
        """Mark ``import_id`` as taken by this plan; returns it (length checked)."""
        _check_import_id(import_id)
        self._planned.add(import_id)
        self._chosen.append(import_id)
        return import_id

    def choose(self, row: RevolutRow) -> str:
        """``row.key`` if unused, else ``YNAB:{amount}:{date}:{n}`` with the smallest free n."""
        if not self.is_used(row.key):
            return self.reserve(row.key)
        n = 1
        while True:
            candidate = f"YNAB:{row.amount}:{row.date}:{n}"
            if not self.is_used(candidate):
                return self.reserve(candidate)
            n += 1

    @property
    def chosen(self) -> frozenset[str]:
        """Every import_id reserved through this allocator."""
        return frozenset(self._chosen)


def _check_import_id(import_id: str) -> None:
    if not import_id or len(import_id) > MAX_IMPORT_ID_LENGTH:
        # Unreachable for parser-generated rows (amounts ≤ 1e9 units, ISO dates); a guard
        # against a planner bug sending YNAB an id it would reject for the whole batch.
        raise YnabHelperError(
            f"import_id {import_id!r} is empty or longer than {MAX_IMPORT_ID_LENGTH} characters",
            user_message="Internal error while planning the import (invalid import_id).",
        )


# ── context ──────────────────────────────────────────────────────────────────


def _account_entries(ledger: Iterable[LedgerEntry], account_id: str) -> list[LedgerEntry]:
    """This account's entries plus unclaimed legacy ones (``account_id=''``) whose key the
    account does not have, sorted by (key, account_id). Other accounts are ignored (F12)."""
    own: dict[str, LedgerEntry] = {}
    unassigned: dict[str, LedgerEntry] = {}
    for entry in ledger:
        if entry.account_id == account_id:
            own[entry.key] = entry
        elif entry.account_id == UNASSIGNED_ACCOUNT:
            unassigned[entry.key] = entry
    merged = dict(unassigned)
    merged.update(own)  # an account entry wins over an unclaimed one with the same key
    return [merged[key] for key in sorted(merged)]


def _orphan_index(mirror: MirrorView) -> dict[tuple[int, str], list[YnabTx]]:
    """Live transactions of the account whose import_id is a CSV-derived id
    ``YNAB:{amount}:{date}:{n}`` that still describes them — with the transaction's own amount
    and date, or one a settlement or re-dating changed since (:func:`_settled_since`) —
    grouped by their *current* (amount, date). Candidates for :func:`_find_orphan`."""
    index: dict[tuple[int, str], list[YnabTx]] = defaultdict(list)
    for y in mirror.transactions():
        parsed = _parse_key(y.import_id)
        if parsed is not None and (
            (parsed[0], parsed[1]) == (y.amount, y.date) or _settled_since(parsed, y)
        ):
            index[(y.amount, y.date)].append(y)
    return dict(index)


def _settled_since(parsed: tuple[int, str, int], y: YnabTx) -> bool:
    """True if ``y``, created as ``YNAB:{amount}:{date}:{n}`` (``parsed``), plausibly got its
    current amount/date from a settlement or a timezone re-dating we wrote: same sign, and
    at most one day from the date it was created with."""
    amount, day, _n = parsed
    days = _days_between(y.date, day)
    return days is not None and abs(days) <= 1 and amount * y.amount > 0


@dataclass(slots=True)
class PlanContext:
    """Shared state for :func:`actions_for_match` within one plan (or one executor run).

    * ``holders`` — YNAB transaction id → key of the ledger entry linking it (never two live
      entries per transaction; F02). Updated as LINKs are planned.
    * ``claimed_tx_ids`` — YNAB ids that matched CSV rows still reference; the superseded sweep
      never deletes them.
    * ``frame_offset`` — the export's offset to the ledger's canonical frame: started_at values
      written to the ledger are ``row.started_at − frame_offset`` (:attr:`Plan.frame_offset`).
    * ``stale`` — the export is older than the ledger (``not opts.allow_sweep`` or a ledger
      entry started after the export's newest timestamp): pending rows change nothing.
    * ``newer`` — the export shows a row that started after every ledger entry: only such an
      export may re-create a removed pending.
    """

    account_id: str
    mirror: MirrorView
    ids: ImportIdAllocator
    holders: dict[str, str] = field(default_factory=dict)
    claimed_tx_ids: set[str] = field(default_factory=set)
    currency: str | None = None
    frame_offset: int = 0
    stale: bool = False
    newer: bool = True
    orphans: dict[tuple[int, str], list[YnabTx]] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        ledger: Iterable[LedgerEntry],
        mirror: MirrorView,
        opts: PlanOptions,
        *,
        reserved_import_ids: Iterable[str] = (),
        currency: str | None = None,
        frame_offset: int = 0,
        stale: bool | None = None,
        newer: bool = True,
    ) -> PlanContext:
        """Context for ``opts.account_id`` from the ledger entries and a mirror snapshot.

        The executor passes ``plan.reserved_import_ids`` (so ids chosen by the plan stay
        taken), ``plan.frame_offset`` and ``stale=plan.stale_export``. ``stale`` defaults to
        ``not opts.allow_sweep``.
        """
        entries = _account_entries(ledger, opts.account_id)
        holders: dict[str, str] = {}
        for entry in entries:
            if not entry.ynab_tx_id or entry.status == LedgerStatus.REKEYED:
                continue
            current = holders.get(entry.ynab_tx_id)
            if current is None or entry.status == LedgerStatus.ACTIVE:
                holders[entry.ynab_tx_id] = entry.key
        return cls(
            account_id=opts.account_id,
            mirror=mirror,
            ids=ImportIdAllocator(entries, mirror.used_import_ids, reserved_import_ids),
            holders=holders,
            currency=currency,
            frame_offset=frame_offset,
            stale=(not opts.allow_sweep) if stale is None else stale,
            newer=newer,
            orphans=_orphan_index(mirror),
        )

    def describe_row(self, row: RevolutRow) -> str:
        return _describe(row.date, row.amount, row.payee, self.currency)

    def describe_entry(self, entry: LedgerEntry) -> str:
        return _describe(entry.csv_date, entry.csv_amount, entry.csv_payee, self.currency)

    def started_text(self, row: RevolutRow) -> str | None:
        """``started_at`` to store for ``row``: normalised to the canonical frame, or None for
        date-only rows."""
        started = _row_started(row)
        if started is None:
            return None
        return _format_ts(started - timedelta(seconds=self.frame_offset))


# ── export frame: windows and recency ────────────────────────────────────────


def _entry_span(entry: LedgerEntry) -> tuple[datetime, datetime] | None:
    """When the entry's transaction started: its started_at, else its whole csv_date."""
    ts = _parse_ts(entry.started_at)
    if ts is not None:
        return ts, ts
    start = _day_start(entry.csv_date)
    if start is None:
        return None
    return start, start + _DAY_END


def _coverage(export: ParsedExport, latest: datetime | None) -> tuple[datetime, datetime] | None:
    """The span in which ``export`` shows every transaction that started then and exists.

    Revolut selects completed rows by their *Completed* Date. A transaction that started
    before the statement's range but completed before it began is therefore legitimately
    absent, although other rows that started on the same day are present because they
    completed later (seen on a real export: a 2026-10-01.. statement whose rows start on
    09-29). Using the first *Started* date as the lower bound would make such transactions
    look "vanished" — and a pending among them would be swept from YNAB.

    So the coverage starts at the latest of: the first Started date, the first Completed
    *instant* (exact, so it holds whatever timezone Revolut selects in), and the range start
    in the file name; it ends at the end of the earlier of the last Started date and the
    range end in the file name. It is empty (None) when nothing proves completeness:

    * rows dated after the statement's own range end (or a newest timestamp after it): a
      custom range exported later, so transactions that completed between the range end
      and the export are missing;
    * no completed row and no range in the file name (e.g. a renamed, pending-only file).
    """
    if export.window is None:
        return None
    lo = _day_start(export.window[0])
    hi_day = export.window[1]
    completed = [r.completed_at for r in export.rows if r.completed_at is not None]
    if completed:
        lo = max(lo, min(completed)) if lo is not None else None
    elif export.filename_window is None:
        return None
    if export.filename_window is not None:
        range_lo, range_hi = export.filename_window
        if export.window[1] > range_hi:
            return None
        if latest is not None and latest.date().isoformat() > range_hi:
            return None
        range_start = _day_start(range_lo)
        if lo is None or range_start is None:
            return None
        lo, hi_day = max(lo, range_start), min(hi_day, range_hi)
    hi = _day_start(hi_day)
    if lo is None or hi is None:
        return None
    hi += _DAY_END
    return (lo, hi) if lo <= hi else None


@dataclass(frozen=True, slots=True)
class ExportFrame:
    """Where an export sits relative to the ledger, for window and recency tests.

    * ``offsets`` — plausible offsets of the export to an entry: the offsets matches used
      (``{0}`` when none did); an entry's instant in the export's rendering is
      ``started_at + d`` for one of them.
    * ``window`` — the export's (first, last) Started Date.
    * ``coverage`` — the (start, end) instants between which the export is *complete*, i.e.
      shows every transaction that started then and still exists (see :func:`_coverage`).
      Never wider than ``window``; None when nothing proves the export complete.
    * ``latest`` — the newest Started/Completed timestamp of the export's timed rows: the
      export was generated after it. None for date-only exports.
    * ``latest_started`` — the newest Started timestamp of the export's timed rows.
    """

    offsets: tuple[int, ...]
    window: tuple[str, str] | None
    latest: datetime | None
    latest_started: datetime | None
    coverage: tuple[datetime, datetime] | None = None

    @classmethod
    def of(cls, export: ParsedExport, offsets: Iterable[int] = ()) -> ExportFrame:
        """The frame of ``export`` with the given plausible ``offsets`` ({0} when empty)."""
        latest: datetime | None = None
        latest_started: datetime | None = None
        for row in export.rows:
            started = _row_started(row)
            if started is None:
                continue
            if latest_started is None or started > latest_started:
                latest_started = started
            for ts in (started, row.completed_at):
                if ts is not None and (latest is None or ts > latest):
                    latest = ts
        return cls(
            offsets=tuple(sorted(set(offsets) or {0})),
            window=export.window,
            latest=latest,
            latest_started=latest_started,
            coverage=_coverage(export, latest),
        )

    def _shifted(self, entry: LedgerEntry) -> tuple[datetime, datetime] | None:
        """The earliest and latest instant the entry may have in the export's rendering."""
        span = _entry_span(entry)
        if span is None:
            return None
        return (
            span[0] + timedelta(seconds=self.offsets[0]),
            span[1] + timedelta(seconds=self.offsets[-1]),
        )

    def _window_bounds(self) -> tuple[datetime, datetime] | None:
        if self.window is None:
            return None
        lo, hi = _day_start(self.window[0]), _day_start(self.window[1])
        if lo is None or hi is None:
            return None
        return lo, hi + _DAY_END

    def touches_window(self, entry: LedgerEntry) -> bool:
        """True if the entry may lie inside the export window (under some plausible offset)."""
        span, bounds = self._shifted(entry), self._window_bounds()
        if span is None or bounds is None:
            return False
        return span[0] <= bounds[1] and span[1] >= bounds[0]

    def covers(self, entry: LedgerEntry) -> bool:
        """True if the export must show the entry's row if it still existed: inside the
        export's *coverage* (not merely its window) and started before the export was
        generated, under *every* plausible offset."""
        span = self._shifted(entry)
        if span is None or self.coverage is None:
            return False
        lo, hi = self.coverage
        upper = hi if self.latest is None else min(hi, self.latest)
        return span[0] >= lo and span[1] <= upper

    def predates(self, entry: LedgerEntry) -> bool:
        """True if the entry started after the export's newest timestamp (under every
        plausible offset): the export was generated before the entry's transaction existed."""
        span = self._shifted(entry)
        return self.latest is not None and span is not None and span[0] > self.latest

    def postdates(self, entry: LedgerEntry) -> bool:
        """True if the export shows a row that started after the entry (every offset)."""
        span = self._shifted(entry)
        return (
            self.latest_started is not None and span is not None and span[1] < self.latest_started
        )


def _export_is_older(entries: Iterable[LedgerEntry], frame: ExportFrame) -> LedgerEntry | None:
    """The newest entry that proves the export older than the ledger, or None."""
    newest: LedgerEntry | None = None
    newest_ts: datetime | None = None
    for entry in entries:
        if entry.status not in _RECENCY_EVIDENCE or not frame.predates(entry):
            continue
        span = _entry_span(entry)
        if span is not None and (newest_ts is None or span[0] > newest_ts):
            newest, newest_ts = entry, span[0]
    return newest


# ── step 1: offsets ──────────────────────────────────────────────────────────


def learn_offsets(rows: Sequence[RevolutRow], entries: Iterable[LedgerEntry]) -> Counter[int]:
    """Export offsets (seconds) observed on rows whose key equals the key of an entry whose
    row later exports still show (ACTIVE, UNLINKED, DELETED_IN_YNAB, REVERTED), both with
    started_at: ``row.started_at − entry.started_at`` when timezone-shaped.

    Returns observation counts (0 is never counted; it is always an allowed offset).
    """
    by_key: dict[str, datetime] = {}
    for entry in entries:
        if entry.status not in _FRAME_EVIDENCE:
            continue
        ts = _parse_ts(entry.started_at)
        if ts is not None:
            by_key[entry.key] = ts
    counts: Counter[int] = Counter()
    for row in rows:
        started = _row_started(row)
        old = by_key.get(row.key)
        if started is None or old is None:
            continue
        delta = _seconds(started - old)
        if is_timezone_offset(delta):
            counts[delta] += 1
    return counts


def _offset_votes(
    export: ParsedExport, entries: Iterable[LedgerEntry]
) -> tuple[Counter[int], set[int], set[int]]:
    """Offset votes from same-amount, same-payee pairs, the offsets of *distinctive* pairs
    whose entry is inside the window, and those of distinctive pairs whose entry is one day
    outside it (see :func:`export_offsets`).

    Voters are timestamped entries whose row later exports still show (ACTIVE, UNLINKED,
    DELETED_IN_YNAB, REVERTED) dated inside the export window, plus —
    with distinctive pairs only — entries dated one day outside it: a statement rendered in a
    timezone east (west) of the ledger's moves rows of the previous (next) day into its first
    (last) day, so with back-to-back statements every overlapping row crossed midnight.
    """
    window = export.window
    lo, hi = (_day(window[0]), _day(window[1])) if window is not None else (None, None)
    if window is None or lo is None or hi is None:
        return Counter(), set(), set()
    adjacent_days = {(lo - _ONE_DAY).isoformat(), (hi + _ONE_DAY).isoformat()}
    rows = export.rows
    row_instants = {(r.amount, s) for r in rows if (s := _row_started(r)) is not None}
    entry_instants: set[tuple[int, datetime]] = set()
    voters: list[tuple[int, str, datetime, bool]] = []  # (amount, payee, ts, adjacent)
    for entry in entries:
        if entry.status not in _FRAME_EVIDENCE:
            continue
        ts = _parse_ts(entry.started_at)
        if ts is None:
            continue
        entry_instants.add((entry.csv_amount, ts))
        payee = _norm_payee(entry.csv_payee)
        if not payee or (entry.csv_amount, ts) in row_instants:
            continue
        if _in_window(entry.csv_date, window):
            voters.append((entry.csv_amount, payee, ts, False))
        elif entry.csv_date in adjacent_days:
            voters.append((entry.csv_amount, payee, ts, True))
    groups: dict[tuple[int, str], list[int]] = defaultdict(list)
    for vi, (amount, payee, _ts, _adjacent) in enumerate(voters):
        groups[(amount, payee)].append(vi)
    votes: Counter[int] = Counter()
    pairs: list[tuple[int, int, int, datetime]] = []
    for ri, row in enumerate(rows):
        started = _row_started(row)
        if started is None or (row.amount, started) in entry_instants:
            continue  # date-only, or the row has its same-instant entry: no evidence
        for vi in groups.get((row.amount, _norm_payee(row.payee)), ()):
            delta = _seconds(started - voters[vi][2])
            if not is_timezone_offset(delta):
                continue
            if voters[vi][3] and started.second == 0:
                continue  # outside the window, only distinctive pairs are evidence
            votes[delta] += 1
            pairs.append((ri, vi, delta, started))
    per_row = Counter(ri for ri, *_ in pairs)
    per_voter = Counter(vi for _, vi, *_ in pairs)
    distinctive = [
        (vi, delta)
        for ri, vi, delta, started in pairs
        if started.second != 0 and per_row[ri] == 1 and per_voter[vi] == 1
    ]
    strong = {delta for vi, delta in distinctive if not voters[vi][3]}
    adjacent = {delta for vi, delta in distinctive if voters[vi][3]}
    return votes, strong, adjacent


def vote_offsets(export: ParsedExport, entries: Iterable[LedgerEntry]) -> Counter[int]:
    """Offset votes from same-amount, same-payee (CSV side) row/entry pairs, both timestamped,
    the entry's row still in every later export (ACTIVE, UNLINKED, DELETED_IN_YNAB, REVERTED)
    and dated inside the export window (so its row must be in it) —
    or one day outside it, for pairs with non-zero seconds. Rows and entries that have a
    same-instant, same-amount counterpart do not vote.

    Complements :func:`learn_offsets`: when a timezone shift renumbers same-amount rows, the
    only key-equal pair can be a collision with a bogus offset while the true offset shows up
    on every transaction of the group (finding F34). Returns ``{non-zero offset: votes}``.
    """
    return _offset_votes(export, entries)[0]


def export_offsets(export: ParsedExport, entries: Sequence[LedgerEntry]) -> dict[int, int]:
    """§6.2 step 1: the allowed non-zero export offsets with their evidence score.

    Allowed: every offset observed on a key-equal pair (:func:`learn_offsets`), every offset
    with at least :data:`MIN_OFFSET_VOTES` pair votes (:func:`vote_offsets`), and the offset
    of a *distinctive* vote whose entry is inside the window: a pair that is the only
    candidate of both its row and its entry and whose timestamps have non-zero seconds (two
    different transactions an exact multiple of 15 minutes apart to the second are a ~1/900
    coincidence; round-time scheduled transfers never qualify).

    A lone distinctive pair whose entry is one day *outside* the window (a statement whose
    only overlap with the ledger is one row that crossed midnight) is accepted only when the
    export holds no other frame evidence at all — no allowed offset, no row at the very
    instant of an entry — and the offset is not contradicted (:func:`_shift_contradicted`):
    the entry's own row is legitimately absent from a statement in the ledger's frame, so its
    absence is no evidence of a shift.

    Score = the larger of both counts; it ranks the offsets after 0 in pass 1.
    """
    learned = learn_offsets(export.rows, entries)
    votes, strong, adjacent = _offset_votes(export, entries)
    allowed = set(learned) | {d for d, n in votes.items() if n >= MIN_OFFSET_VOTES} | strong
    if not allowed and len(adjacent) == 1:
        (delta,) = adjacent
        if _zero_offset_evidence(export.rows, entries) == 0 and not _shift_contradicted(
            export, entries, delta
        ):
            allowed = {delta}
    return {d: max(learned.get(d, 0), votes.get(d, 0)) for d in sorted(allowed)}


def _shift_contradicted(export: ParsedExport, entries: Iterable[LedgerEntry], delta: int) -> bool:
    """True if, rendered at ``delta``, the export would have to show a COMPLETED ledger
    entry (inside its window, before its newest timestamp) that it does not show at that
    instant — so the export is not rendered at ``delta``. (A pending may legitimately have
    vanished or settled at another amount; a completed row stays, also when the user deleted
    it in YNAB.)"""
    frame = ExportFrame.of(export, (delta,))
    row_instants = {(r.amount, s) for r in export.rows if (s := _row_started(r)) is not None}
    shift = timedelta(seconds=delta)
    for entry in entries:
        if entry.status not in _FRAME_EVIDENCE:
            continue
        ts = _parse_ts(entry.started_at)
        if ts is None or entry.csv_state != CsvState.COMPLETED or not frame.covers(entry):
            continue
        if (entry.csv_amount, ts + shift) not in row_instants:
            return True
    return False


def _zero_offset_evidence(rows: Sequence[RevolutRow], entries: Iterable[LedgerEntry]) -> int:
    """How many rows have an entry of the same amount at the very same instant (an entry
    whose row later exports still show, :data:`_FRAME_EVIDENCE`): the evidence that the
    export is rendered in the ledger's frame (d = 0)."""
    instants = {
        (e.csv_amount, ts)
        for e in entries
        if e.status in _FRAME_EVIDENCE and (ts := _parse_ts(e.started_at)) is not None
    }
    return sum(1 for r in rows if (s := _row_started(r)) is not None and (r.amount, s) in instants)


def _offset_ranks(offsets: Mapping[int, int], zero_evidence: int = 0) -> dict[int, int]:
    """Allowed offsets ranked by evidence (ties: 0 first, then smaller |d|, then d).

    0 is always allowed; it usually ranks first. In a statement rendered in another timezone
    the true offset has more evidence than 0, whose only support is then a coincidence of
    round times (e.g. a scheduled transfer and a row stored in the canonical frame)."""
    scores = {d: n for d, n in offsets.items() if d != 0}
    scores[0] = zero_evidence
    ranked = sorted(scores, key=lambda d: (-scores[d], d != 0, abs(d), d))
    return {delta: i for i, delta in enumerate(ranked)}


# ── step 2: matching ─────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class _Pair:
    row_index: int
    entry_index: int
    info: MatchInfo


def match_rows(
    export: ParsedExport,
    entries: Sequence[LedgerEntry],
    offsets: Mapping[int, int],
) -> list[_Pair]:
    """§6.2 step 2: match rows to ledger entries (each at most once), in this order:

    1. *Timestamp identity*: same amount, ``row.started_at − entry.started_at`` an allowed
       offset, ranked by evidence (normally 0 first; :func:`_offset_ranks`); ties: live
       entries, consistent states, same key.
    2. *Group assignment* for legacy entries (migrated from v1: their started_at may be a
       sibling's, finding C01) and date-only data: rows and entries of the same
       (amount, date) — ``entry.csv_amount``/``csv_date`` — are assigned as a whole. All
       candidates share amount and date, so any assignment keeps YNAB's totals right; the
       ranking prefers plausible states, live entries, offset-compatible timestamps, the same
       CSV payee, then the same key. Then exact key for the rest (opaque keys); the same key
       with both timestamps present but not offset-compatible is a renumbering collision.
    3. *Legacy date shift* across midnight (F10 guards; the CSV payee must match for csv
       entries only — a legacy entry's payee may be YNAB's renamed one, F11).
    4. *Settlement*: a pending settled at a different amount, maybe re-dated; at the very
       instant also a REVERTED row (re-quoted, then reverted). ACTIVE pendings are matched
       first, then UNLINKED ones, then — at their very instant only — pendings the user
       deleted in YNAB (whose settled row must not be POSTed again, R01). The entry's date
       (``csv_date``, §6.2) or its instant under a plausible offset must lie inside the
       window.
    4b. *Pre-settlement*: an older export still shows a since-settled (re-quoted, reverted,
        removed) pending at its earlier amount.

    ``entries`` are this account's entries (REKEYED ones are ignored here). Returns the pairs
    in CSV file order.
    """
    rows = export.rows
    window = export.window
    ranks = _offset_ranks(offsets, _zero_offset_evidence(rows, entries))
    eligible = [i for i, e in enumerate(entries) if e.status != LedgerStatus.REKEYED]
    entry_ts = {i: _parse_ts(entries[i].started_at) for i in eligible}
    row_ts = [_row_started(row) for row in rows]
    matched_rows: dict[int, _Pair] = {}
    matched_entries: set[int] = set()

    def delta_of(ri: int, ei: int) -> int | None:
        started, ts = row_ts[ri], entry_ts[ei]
        if started is None or ts is None:
            return None
        return _seconds(started - ts)

    def take(ri: int, ei: int, method: MatchMethod, offset: int | None = None) -> None:
        row, entry = rows[ri], entries[ei]
        matched_rows[ri] = _Pair(ri, ei, MatchInfo(row.line_no, row.key, entry.key, method, offset))
        matched_entries.add(ei)

    # Pass 1 — timestamp identity.
    by_amount: dict[int, list[int]] = defaultdict(list)
    for ei in eligible:
        if entry_ts[ei] is not None:
            by_amount[entries[ei].csv_amount].append(ei)
    candidates: list[tuple[int, int, int, bool, int, int, int]] = []
    for ri, row in enumerate(rows):
        for ei in by_amount.get(row.amount, ()) if row_ts[ri] is not None else ():
            delta = delta_of(ri, ei)
            rank = ranks.get(delta) if delta is not None else None
            if rank is None or delta is None:
                continue
            entry = entries[ei]
            candidates.append(
                (
                    rank,
                    _STATUS_RANK.get(entry.status, 9),
                    _state_distance(row.state, entry.csv_state),
                    entry.key != row.key,
                    ri,
                    ei,
                    delta,
                )
            )
    candidates.sort()
    for *_ranking, ri, ei, delta in candidates:
        if ri not in matched_rows and ei not in matched_entries:
            take(ri, ei, MatchMethod.TIMESTAMP, delta)

    # Pass 2 — (amount, date) group assignment for legacy entries and date-only data.
    groups: dict[tuple[int, str], list[int]] = defaultdict(list)
    for ei in eligible:
        if ei not in matched_entries:
            groups[(entries[ei].csv_amount, entries[ei].csv_date)].append(ei)
    group_candidates: list[tuple[bool, int, int, int, bool, bool, int, int, int | None]] = []
    for ri, row in enumerate(rows):
        if ri in matched_rows:
            continue
        for ei in groups.get((row.amount, row.date), ()):
            entry = entries[ei]
            if entry.identity == IDENTITY_CSV and entry_ts[ei] is not None and row_ts[ri]:
                continue  # a timestamped csv entry is identified by pass 1 only
            delta = delta_of(ri, ei)
            ts_rank = 1 if delta is None else (0 if delta in ranks else 2)
            distance = _state_distance(row.state, entry.csv_state)
            group_candidates.append(
                (
                    distance == 2,  # implausible (Revolut never goes backwards) → last
                    _STATUS_RANK.get(entry.status, 9),
                    ts_rank,
                    distance,
                    _norm_payee(entry.csv_payee) != _norm_payee(row.payee),
                    entry.key != row.key,
                    ri,
                    ei,
                    delta if ts_rank == 0 else None,
                )
            )
    group_candidates.sort()
    for *_ranking, ri, ei, offset in group_candidates:
        if ri not in matched_rows and ei not in matched_entries:
            take(ri, ei, MatchMethod.KEY, offset)
    # Exact key for the rest (e.g. an entry whose amount a v1 re-quote changed).
    by_key = {entries[ei].key: ei for ei in eligible}
    for ri, row in enumerate(rows):
        if ri in matched_rows:
            continue
        ei = by_key.get(row.key)
        if ei is None or ei in matched_entries:
            continue
        if entries[ei].identity == IDENTITY_CSV and entry_ts[ei] is not None and row_ts[ri]:
            continue  # renumbering collision
        take(ri, ei, MatchMethod.KEY)

    # Pass 3 — legacy date shift, unique on both sides.
    csv_keys = {row.key for row in rows} | {s.key for s in export.skipped if s.key}
    pass3: list[tuple[int, int]] = []
    if window is not None:
        shift_by_amount: dict[int, list[int]] = defaultdict(list)
        for ei in eligible:
            entry = entries[ei]
            legacy = entry.identity != IDENTITY_CSV
            if (
                ei not in matched_entries
                and (legacy or entry_ts[ei] is None)
                and entry.key not in csv_keys
                and _in_window(entry.csv_date, window)
                and (legacy or _norm_payee(entry.csv_payee))
            ):
                shift_by_amount[entry.csv_amount].append(ei)
        for ri, row in enumerate(rows):
            started = row_ts[ri]
            if ri in matched_rows or started is None or not _in_window(row.date, window):
                continue
            payee = _norm_payee(row.payee)
            for ei in shift_by_amount.get(row.amount, ()):
                entry = entries[ei]
                if entry.identity == IDENTITY_CSV and (
                    not payee or _norm_payee(entry.csv_payee) != payee
                ):
                    continue
                days = _days_between(row.date, entry.csv_date)
                # Direction matters: a row rendered one day *later* crossed midnight forwards
                # (starts shortly after 00:00); one day *earlier* crossed it backwards.
                if (days == 1 and started.hour < NEAR_MIDNIGHT_HOURS) or (
                    days == -1 and started.hour >= 24 - NEAR_MIDNIGHT_HOURS
                ):
                    pass3.append((ri, ei))
    for ri, ei in _unique_pairs(pass3):
        take(ri, ei, MatchMethod.DATE_SHIFT)

    # Pass 4 — settlement: a pending settled at a different amount, maybe re-dated. The entry
    # is in the window by its date as last rendered (§6.2) or by its instant shifted by the
    # offsets the matches so far actually used (a statement in another timezone, R06). While
    # the statement taught no offset, an entry outside the window but within any plausible
    # rendering of it is a candidate for *distinctive* instant pairs only (see below).
    frame = ExportFrame.of(export, _used_offsets(matched_rows.values()))
    unlearned = not any(ranks)  # 0 is the only allowed offset
    any_rendering = ExportFrame.of(export, (-MAX_EXPORT_OFFSET_SECONDS, MAX_EXPORT_OFFSET_SECONDS))
    in_window: set[int] = set()
    pass4: list[tuple[int, int]] = []
    if window is not None:
        pending_by_payee: dict[str, list[int]] = defaultdict(list)
        for ei in eligible:
            entry = entries[ei]
            if (
                ei in matched_entries
                or entry.status not in _SETTLEMENT_STATUSES
                or entry.csv_state != CsvState.PENDING
                or not _norm_payee(entry.csv_payee)
            ):
                continue
            if _in_window(entry.csv_date, window) or frame.touches_window(entry):
                in_window.add(ei)
            elif not (unlearned and any_rendering.touches_window(entry)):
                continue
            pending_by_payee[_norm_payee(entry.csv_payee)].append(ei)
        for ri, row in enumerate(rows):
            if ri in matched_rows:
                continue
            payee = _norm_payee(row.payee)
            if not payee:
                continue
            for ei in pending_by_payee.get(payee, ()):
                entry = entries[ei]
                days = _days_between(row.date, entry.csv_date)
                if days is None or abs(days) > 1 or row.amount == entry.csv_amount:
                    continue
                if row.amount * entry.csv_amount <= 0:
                    continue  # a sign flip (refund vs charge) is never a settlement
                pass4.append((ri, ei))

    # Revolut keeps the Started Date when a pending settles: a candidate at the entry's very
    # instant (modulo an offset) is decisive even when other rows of the merchant are near
    # (several Amazon orders). Those pairs are matched first; the rest needs uniqueness.
    # A REVERTED row is only taken at the very instant: a pending re-quoted by FX and then
    # reverted (it is deleted from YNAB like any reverted pending). Each round takes live
    # entries first (ACTIVE, then UNLINKED, then DELETED_IN_YNAB), uniqueness within a tier.
    # A pending the user deleted is only recognised at its very instant: matching it to a
    # merely unique nearby row could swallow a new purchase (it would never be created).
    # Without a learned offset, "its very instant" may be under any timezone-shaped delta
    # whose timestamps have non-zero seconds (two transactions of one merchant an exact
    # multiple of 15 minutes apart to the second are a ~1/900 coincidence; F10's round-time
    # transfers never qualify) — the settled row is then the only overlap of a statement
    # rendered in another timezone.
    def instant(ri: int, ei: int) -> bool:
        delta = delta_of(ri, ei)
        if delta in ranks:
            return True
        started = row_ts[ri]
        return (
            unlearned
            and delta is not None
            and started is not None
            and started.second != 0
            and is_timezone_offset(delta)
        )

    tiers = [
        [(ri, ei) for ri, ei in pass4 if entries[ei].status == status]
        for status in _SETTLEMENT_STATUSES
    ]
    live_tiers = [
        tier
        for status, tier in zip(_SETTLEMENT_STATUSES, tiers, strict=True)
        if status != LedgerStatus.DELETED_IN_YNAB
    ]
    for tier in tiers:
        timed = [
            (ri, ei)
            for ri, ei in tier
            if ri not in matched_rows and ei not in matched_entries and instant(ri, ei)
        ]
        for ri, ei in _unique_pairs(timed):
            take(ri, ei, MatchMethod.SETTLEMENT, delta_of(ri, ei))
    for tier in live_tiers:
        rest = [
            (ri, ei)
            for ri, ei in tier
            if ri not in matched_rows
            and ei not in matched_entries
            and ei in in_window
            and rows[ri].state in _LIVE_ROW_STATES
        ]
        for ri, ei in _unique_pairs(rest):
            take(ri, ei, MatchMethod.SETTLEMENT)

    # Pass 4b — a pending seen from an older export: a PENDING row at the very instant
    # (modulo an offset) and payee of an entry that is no longer a live pending (settled,
    # reverted or removed since), still at its earlier amount (before the settlement or an FX
    # re-quote). Without it, re-sending the older statement would create the pending again.
    pass4b: list[tuple[int, int]] = []
    completed_by_payee: dict[str, list[int]] = defaultdict(list)
    for ei in eligible:
        entry = entries[ei]
        if (
            ei not in matched_entries
            and (entry.csv_state != CsvState.PENDING or entry.status != LedgerStatus.ACTIVE)
            and entry_ts[ei] is not None
            and _norm_payee(entry.csv_payee)
        ):
            completed_by_payee[_norm_payee(entry.csv_payee)].append(ei)
    for ri, row in enumerate(rows):
        if ri in matched_rows or row.state != STATE_PENDING or row_ts[ri] is None:
            continue
        for ei in completed_by_payee.get(_norm_payee(row.payee), ()):
            entry = entries[ei]
            if row.amount * entry.csv_amount <= 0 or row.amount == entry.csv_amount:
                continue
            if delta_of(ri, ei) in ranks:
                pass4b.append((ri, ei))
    for ri, ei in _unique_pairs(pass4b):
        take(ri, ei, MatchMethod.PRE_SETTLEMENT, delta_of(ri, ei))

    return [matched_rows[ri] for ri in sorted(matched_rows)]


def _used_offsets(pairs: Iterable[_Pair]) -> set[int]:
    """The offsets matches compared timestamps at (0 included when used)."""
    return {p.info.offset_seconds for p in pairs if p.info.offset_seconds is not None}


def _frame_offset(pairs: Iterable[_Pair]) -> int:
    """The export's dominant offset to the ledger: the most common offset among matches that
    compared timestamps (ties: 0, then the smaller |d|); 0 without such matches."""
    counts = Counter(p.info.offset_seconds for p in pairs if p.info.offset_seconds is not None)
    if not counts:
        return 0
    return min(counts, key=lambda d: (-counts[d], d != 0, abs(d), d))


#: Matches that identify a row with its entry by the instant it started.
_INSTANT_METHODS = frozenset({MatchMethod.TIMESTAMP, MatchMethod.PRE_SETTLEMENT})


def _regressed_match(
    pairs: Iterable[_Pair], rows: Sequence[RevolutRow], entries: Sequence[LedgerEntry]
) -> LedgerEntry | None:
    """An entry an earlier statement showed COMPLETED or REVERTED that this statement shows
    PENDING at the very same instant, or None.

    Revolut never moves a row back to pending, so such a pair proves the whole statement
    older than the ledger (F36) — even when no ledger entry started after its newest row (a
    re-sent statement from earlier the same day). Only csv-identity entries count: a migrated
    entry's csv_state may come from YNAB's cleared flag rather than from a statement.
    """
    for pair in pairs:
        row, entry = rows[pair.row_index], entries[pair.entry_index]
        if (
            pair.info.method in _INSTANT_METHODS
            and row.state == STATE_PENDING
            and entry.identity == IDENTITY_CSV
            and entry.csv_state in (CsvState.COMPLETED, CsvState.REVERTED)
        ):
            return entry
    return None


# ── step 3: actions for one matched pair ─────────────────────────────────────


def _backfill(
    row: RevolutRow, entry: LedgerEntry, ctx: PlanContext, method: MatchMethod
) -> dict[str, Any]:
    """Ledger changes that make ``entry`` describe ``row`` (csv_*, started_at, identity).

    ``started_at`` moves to the row's instant in the canonical frame (a no-op for an entry
    matched at the dominant offset); an entry the executor just built for a resolved
    duplicate keeps the value the plan computed.
    """
    target: dict[str, Any] = {
        "csv_date": row.date,
        "csv_amount": row.amount,
        "csv_payee": row.payee,
        "csv_type": row.type,
        "csv_state": row.state,
        "identity": IDENTITY_CSV,
    }
    started = ctx.started_text(row)
    keep = method is MatchMethod.DUPLICATE and entry.started_at is not None
    if started is not None and not keep:
        target["started_at"] = started
    return {name: value for name, value in target.items() if getattr(entry, name) != value}


def _observed_cleared(y: YnabTx) -> str:
    return UNCLEARED if y.cleared == UNCLEARED else CLEARED


def _link_updates(entry: LedgerEntry, y: YnabTx) -> dict[str, Any]:
    """Link ``entry`` to ``y``; unknown pushed_* values are what YNAB holds now (as the
    storage refresh does), known ones are kept so user edits stay detectable."""
    target: dict[str, Any] = {
        "status": LedgerStatus.ACTIVE.value,
        "ynab_tx_id": y.id,
        "ynab_import_id": y.import_id or entry.ynab_import_id,
        "pushed_cleared": entry.pushed_cleared or _observed_cleared(y),
        "pushed_amount": entry.pushed_amount if entry.pushed_amount is not None else y.amount,
        "pushed_date": entry.pushed_date or y.date,
        "last_error": None,
    }
    return {name: value for name, value in target.items() if getattr(entry, name) != value}


def _ledger_action(
    entry: LedgerEntry,
    row: RevolutRow | None,
    updates: Mapping[str, Any],
    reason: str,
    *,
    before: TxRef | None = None,
    note: str | None = None,
) -> PlannedAction:
    """A local-only change of ``entry`` (backfill or status)."""
    return PlannedAction(
        kind=ActionKind.LEDGER,
        account_id=entry.account_id,
        entry_key=entry.key,
        reason=reason,
        row=row,
        ynab_tx_id=updates.get("ynab_tx_id", entry.ynab_tx_id),
        ledger_updates=updates,
        before=before,
        after=before,
        note=note,
    )


def _create_payload(row: RevolutRow, import_id: str, opts: PlanOptions) -> dict[str, Any]:
    """The YNAB transaction to POST for ``row`` (v1 field values, §6.2 step 4)."""
    return {
        "account_id": opts.account_id,
        "date": row.date,
        "amount": row.amount,
        "payee_name": row.payee,
        "memo": row.memo,
        "cleared": row.cleared,
        "approved": bool(opts.approved),
        "import_id": import_id,
    }


def _pushed_from_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "pushed_cleared": payload["cleared"],
        "pushed_amount": payload["amount"],
        "pushed_date": payload["date"],
    }


def _new_entry(
    row: RevolutRow, import_id: str, payload: Mapping[str, Any], ctx: PlanContext
) -> LedgerEntry:
    """Placeholder entry for a created row (status UNLINKED until YNAB returns the id)."""
    return LedgerEntry(
        account_id=payload["account_id"],
        key=import_id,
        ynab_import_id=import_id,
        status=LedgerStatus.UNLINKED.value,
        csv_date=row.date,
        csv_amount=row.amount,
        csv_payee=row.payee,
        csv_type=row.type,
        csv_state=row.state,
        started_at=ctx.started_text(row),
        identity=IDENTITY_CSV,
        **_pushed_from_payload(payload),
    )


def _skip_pending(row: RevolutRow) -> PlanSkip:
    return PlanSkip(row.line_no, SKIP_PENDING, "pending (skipped: --skip-pending)", row)


def _skip_pending_older(row: RevolutRow) -> PlanSkip:
    return PlanSkip(
        row.line_no,
        SKIP_PENDING_OLDER_EXPORT,
        "pending in an older statement (not imported; a current statement will add it)",
        row,
    )


def actions_for_match(
    row: RevolutRow,
    entry: LedgerEntry,
    y: YnabTx | None,
    opts: PlanOptions,
    ctx: PlanContext,
    *,
    method: MatchMethod = MatchMethod.TIMESTAMP,
) -> MatchOutcome:
    """§6.2 step 3: what to do for the CSV ``row`` matched to the ledger ``entry``.

    ``y`` is the YNAB transaction ``entry.ynab_tx_id`` refers to (``ctx.mirror.by_id``);
    for UNLINKED entries it is ignored and resolved by import_id in ``ctx.mirror``.
    Never writes cleared backwards, never sends approved/payee/category, derives the memo
    from YNAB's current memo, and leaves amounts/dates alone that the user changed in YNAB.
    """
    status = entry.status
    if status == LedgerStatus.REKEYED:
        raise ValueError(f"REKEYED ledger entry {entry.key!r} must not be matched")
    if entry.ynab_tx_id and status != LedgerStatus.UNLINKED:
        ctx.claimed_tx_ids.add(entry.ynab_tx_id)
    if status == LedgerStatus.DELETED_IN_YNAB:
        if row.state == STATE_REVERTED:
            # Reverted by Revolut and gone from YNAB: the intended end state, whoever removed
            # it (v1's importer deleted reverted pendings and its YNAB sync then made them
            # look user-deleted, R17). Recorded as REVERTED, not reported as a user deletion.
            updates = {"status": LedgerStatus.REVERTED.value, **_backfill(row, entry, ctx, method)}
            out = MatchOutcome(unchanged=True)
            out.actions.append(_ledger_action(entry, row, updates, REASON_REVERTED))
            return out
        out = MatchOutcome(ignored_deleted=True)
        updates = {} if _row_is_older(row, entry, ctx) else _backfill(row, entry, ctx, method)
        if updates:
            out.actions.append(_ledger_action(entry, row, updates, REASON_REFRESH))
        return out
    if status in (LedgerStatus.REVERTED, LedgerStatus.SUPERSEDED):
        return _removed_entry(row, entry, opts, ctx, method)
    if status == LedgerStatus.UNLINKED:
        return _unlinked_entry(row, entry, opts, ctx, method)
    return _active_entry(row, entry, y, opts, ctx, method)


def _row_is_older(row: RevolutRow, entry: LedgerEntry, ctx: PlanContext) -> bool:
    """A PENDING row that must not change anything: the entry was already seen COMPLETED or
    REVERTED (Revolut never moves a row back to pending), or the export is older than the
    ledger (F36: an older FX quote, a pending since re-dated by its settlement …)."""
    if row.state != STATE_PENDING:
        return False
    return ctx.stale or entry.csv_state in (CsvState.COMPLETED, CsvState.REVERTED)


def _removed_entry(
    row: RevolutRow,
    entry: LedgerEntry,
    opts: PlanOptions,
    ctx: PlanContext,
    method: MatchMethod,
) -> MatchOutcome:
    """REVERTED/SUPERSEDED entry: a live row means it reappeared → CREATE with a new id."""
    out = MatchOutcome()
    if row.state not in _LIVE_ROW_STATES:
        out.unchanged = True
        updates = _backfill(row, entry, ctx, method)
        if updates:
            out.actions.append(_ledger_action(entry, row, updates, REASON_REFRESH))
        return out
    if ctx.stale or _row_is_older(row, entry, ctx):
        # Revolut never un-reverts a row and an older export must not resurrect what a newer
        # one removed (F36): report it, keep the ledger as it is.
        out.unchanged = True
        out.warnings.append(
            f"{ctx.describe_row(row)}: removed from YNAB earlier "
            f"({entry.status.lower()}); this statement still shows it as "
            f"{row.state.lower()} but it looks older — not re-created."
        )
        return out
    if row.state == STATE_PENDING and not ctx.newer:
        # A pending that vanished and shows up again: only a statement provably newer than
        # everything imported may bring it back (else it is a re-sent older statement).
        out.unchanged = True
        out.warnings.append(
            f"{ctx.describe_row(row)}: removed from YNAB earlier ({entry.status.lower()}) and "
            "pending again in this statement — not re-created until a newer statement "
            "confirms it."
        )
        return out
    if row.state == STATE_PENDING and opts.skip_pending:
        out.skipped.append(_skip_pending(row))
        return out
    orphan = _find_orphan(row, ctx)
    if orphan is not None:
        # Re-created by an earlier run whose ledger write was lost: link, do not POST again.
        return _adopt_orphan(row, orphan, opts, ctx, replaces_key=entry.key)
    import_id = ctx.ids.choose(row)
    payload = _create_payload(row, import_id, opts)
    out.actions.append(
        PlannedAction(
            kind=ActionKind.CREATE,
            account_id=opts.account_id,
            entry_key=import_id,
            reason=REASON_REAPPEARED,
            row=row,
            payload=payload,
            entry=_new_entry(row, import_id, payload, ctx),
            replaces_key=entry.key,
            after=_row_ref(row),
            note=f"was {entry.status.lower()} as {entry.key}",
        )
    )
    return out


def _unlinked_entry(
    row: RevolutRow,
    entry: LedgerEntry,
    opts: PlanOptions,
    ctx: PlanContext,
    method: MatchMethod,
) -> MatchOutcome:
    """UNLINKED entry: resolve by import_id; LINK, mark deleted, or POST it again."""
    out = MatchOutcome()
    y = None
    for import_id in dict.fromkeys(i for i in (entry.ynab_import_id, entry.key) if i):
        y = ctx.mirror.by_import_id(import_id)
        if y is not None:
            break
    if y is not None and y.deleted and row.state == STATE_REVERTED:
        ctx.claimed_tx_ids.add(y.id)
        out.unchanged = True
        updates = {
            "status": LedgerStatus.REVERTED.value,
            "ynab_tx_id": y.id,
            "ynab_import_id": y.import_id or entry.ynab_import_id,
            **_backfill(row, entry, ctx, method),
        }
        out.actions.append(_ledger_action(entry, row, updates, REASON_REVERTED))
        return out
    if y is not None and y.deleted:
        ctx.claimed_tx_ids.add(y.id)
        updates = {
            "status": LedgerStatus.DELETED_IN_YNAB.value,
            "ynab_tx_id": y.id,
            "ynab_import_id": y.import_id or entry.ynab_import_id,
        }
        if not _row_is_older(row, entry, ctx):
            updates.update(_backfill(row, entry, ctx, method))
        out.ignored_deleted = True
        out.warnings.append(
            f"{ctx.describe_row(row)}: deleted in YNAB — it will not be re-created."
        )
        out.actions.append(
            _ledger_action(
                entry, row, updates, REASON_DELETED_IN_YNAB, before=_tx_ref(y, row.payee)
            )
        )
        return out
    if y is not None:
        return _link_unlinked(row, entry, y, opts, ctx, method)
    # Not in YNAB (this account, deleted included): POST it again — unless it is reverted, or
    # the row is older than what the entry knows (a newer export will create it correctly).
    if _row_is_older(row, entry, ctx):
        out.unchanged = True
        return out
    if row.state == STATE_REVERTED:
        out.unchanged = True
        updates = {"status": LedgerStatus.REVERTED.value, **_backfill(row, entry, ctx, method)}
        out.actions.append(_ledger_action(entry, row, updates, REASON_NEVER_IMPORTED))
        return out
    if row.state == STATE_PENDING and opts.skip_pending:
        out.skipped.append(_skip_pending(row))
        return out
    import_id = entry.import_id_in_ynab
    if len(import_id) <= MAX_IMPORT_ID_LENGTH and ctx.ids.is_free_for(import_id, entry.key):
        ctx.ids.reserve(import_id)
    else:
        orphan = _find_orphan(row, ctx)
        if orphan is not None:  # posted under a fresh id by a run whose ledger write was lost
            return _link_unlinked(row, entry, orphan, opts, ctx, method)
        import_id = ctx.ids.choose(row)
    payload = _create_payload(row, import_id, opts)
    updates = {
        **_backfill(row, entry, ctx, method),
        **_pushed_from_payload(payload),
        "ynab_import_id": import_id,
        "last_error": None,
    }
    updates = {k: v for k, v in updates.items() if getattr(entry, k) != v}
    out.actions.append(
        PlannedAction(
            kind=ActionKind.CREATE,
            account_id=entry.account_id,
            entry_key=entry.key,
            reason=REASON_UNLINKED_MISSING,
            row=row,
            payload=payload,
            entry=dataclasses.replace(entry, **updates),
            entry_exists=True,
            ledger_updates=updates,
            after=_row_ref(row),
        )
    )
    return out


def _link_unlinked(
    row: RevolutRow,
    entry: LedgerEntry,
    y: YnabTx,
    opts: PlanOptions,
    ctx: PlanContext,
    method: MatchMethod,
) -> MatchOutcome:
    """LINK an UNLINKED entry to the live ``y``, then treat it as ACTIVE."""
    out = MatchOutcome()
    ctx.claimed_tx_ids.add(y.id)
    holder = ctx.holders.get(y.id)
    if holder is not None and holder != entry.key:
        # Never two live entries for one YNAB transaction (F02): no LINK, no write.
        out.unchanged = True
        out.warnings.append(
            f"{ctx.describe_row(row)}: its YNAB transaction is already linked to another "
            f"imported row ({holder}) — left alone, please check for a duplicate."
        )
        return out
    link = _link_updates(entry, y)
    ctx.holders[y.id] = entry.key
    linked = dataclasses.replace(entry, **link)
    follow = _active_entry(row, linked, y, opts, ctx, method)
    ref = _tx_ref(y, row.payee)
    ledger_follow = [a for a in follow.actions if a.kind is ActionKind.LEDGER]
    other = [a for a in follow.actions if a.kind is not ActionKind.LEDGER]
    merged = dict(link)
    for action in ledger_follow:  # fold local-only refreshes into the LINK itself
        merged.update(action.ledger_updates)
    out.actions.append(
        PlannedAction(
            kind=ActionKind.LINK,
            account_id=entry.account_id,
            entry_key=entry.key,
            reason=REASON_UNLINKED_RESOLVED,
            row=row,
            ynab_tx_id=y.id,
            entry_exists=True,
            ledger_updates=merged,
            before=ref,
            after=ref,
        )
    )
    out.actions.extend(other)
    out.warnings.extend(follow.warnings)
    out.skipped.extend(follow.skipped)
    return out


def _amount_conflict_reason(y: YnabTx) -> str:
    if y.cleared == RECONCILED:
        return "it is reconciled in YNAB"
    if y.has_subtransactions:
        return "it is split in YNAB"
    return "its amount was edited in YNAB"


def _active_entry(
    row: RevolutRow,
    entry: LedgerEntry,
    y: YnabTx | None,
    opts: PlanOptions,
    ctx: PlanContext,
    method: MatchMethod,
) -> MatchOutcome:
    """ACTIVE entry linked to ``y``: compute the PATCH from YNAB's current state."""
    out = MatchOutcome()
    payee = row.payee
    if (y is None or y.deleted) and row.state == STATE_REVERTED:
        # Reverted and already gone from YNAB (e.g. our DELETE went through but its ledger
        # write did not): exactly the intended end state.
        out.unchanged = True
        updates = {"status": LedgerStatus.REVERTED.value, **_backfill(row, entry, ctx, method)}
        out.actions.append(_ledger_action(entry, row, updates, REASON_REVERTED))
        return out
    if y is None or y.deleted:
        out.ignored_deleted = True
        out.warnings.append(
            f"{ctx.describe_row(row)}: deleted in YNAB — it will not be re-created."
        )
        updates = {"status": LedgerStatus.DELETED_IN_YNAB.value}
        if not _row_is_older(row, entry, ctx):
            updates.update(_backfill(row, entry, ctx, method))
        before = _tx_ref(y, payee) if y is not None else None
        out.actions.append(
            _ledger_action(entry, row, updates, REASON_DELETED_IN_YNAB, before=before)
        )
        return out
    ctx.claimed_tx_ids.add(y.id)
    before = _tx_ref(y, payee)
    if y.account_id != ctx.account_id:
        out.unchanged = True
        out.warnings.append(
            f"{ctx.describe_row(row)}: moved to another account in YNAB — not changed."
        )
        return out

    if row.state == STATE_REVERTED:
        updates = _backfill(row, entry, ctx, method)
        if y.cleared == UNCLEARED:
            out.actions.append(
                PlannedAction(
                    kind=ActionKind.DELETE,
                    account_id=entry.account_id,
                    entry_key=entry.key,
                    reason=REASON_REVERTED,
                    row=row,
                    ynab_tx_id=y.id,
                    ledger_updates={"status": LedgerStatus.REVERTED.value, **updates},
                    before=before,
                )
            )
            return out
        out.unchanged = True
        if entry.csv_state != CsvState.REVERTED:
            out.warnings.append(
                f"{ctx.describe_row(row)}: reverted in Revolut but {y.cleared} in YNAB — "
                "left alone."
            )
        if updates:
            out.actions.append(_ledger_action(entry, row, updates, REASON_REFRESH, before=before))
        return out

    if _row_is_older(row, entry, ctx):
        # An older export: never downgrade (F36). Nothing changes, not even the ledger.
        out.unchanged = True
        return out

    fields: dict[str, Any] = {}
    changes: dict[str, tuple[Any, Any]] = {}
    updates = _backfill(row, entry, ctx, method)

    if row.state == STATE_COMPLETED:
        # cleared only moves uncleared → cleared; reconciled/cleared are never touched (F01).
        if y.cleared == UNCLEARED:
            fields["cleared"] = CLEARED
            changes["cleared"] = (y.cleared, CLEARED)
            updates["pushed_cleared"] = CLEARED
        # The memo comes from YNAB's memo: only the "(pending)" marker goes (F01/F33).
        if has_pending_marker(y.memo):
            memo = strip_pending_marker(y.memo)
            fields["memo"] = memo
            changes["memo"] = (y.memo, memo)

    # YNAB holding what we wrote: the pushed value, or the value of a write whose outcome we
    # never learned. YNAB already showing the statement's new value is not ours otherwise
    # (the user's edit): pushed_* then stays, so a later re-rendering does not undo it (R11).
    intent = unconfirmed_write(entry)
    if intent:
        updates["last_error"] = None
    if row.amount != entry.csv_amount:
        ours = {entry.pushed_amount if entry.pushed_amount is not None else entry.csv_amount}
        if "amount" in intent:
            ours.add(intent["amount"])
        if y.amount == row.amount:
            if entry.pushed_amount is None or intent.get("amount") == row.amount:
                updates["pushed_amount"] = row.amount
        elif y.amount in ours and not y.has_subtransactions and y.cleared != RECONCILED:
            fields["amount"] = row.amount
            changes["amount"] = (y.amount, row.amount)
            updates["pushed_amount"] = row.amount
        else:
            out.warnings.append(
                f"{ctx.describe_row(row)}: amount changed in Revolut "
                f"({format_money(entry.csv_amount, ctx.currency)} → "
                f"{format_money(row.amount, ctx.currency)}) but {_amount_conflict_reason(y)} "
                f"({format_money(y.amount, ctx.currency)}) — amount left alone."
            )

    if row.date != entry.csv_date:
        ours_dates = {entry.pushed_date or entry.csv_date}
        if "date" in intent:
            ours_dates.add(intent["date"])
        if y.date == row.date:
            if entry.pushed_date is None or intent.get("date") == row.date:
                updates["pushed_date"] = row.date
        elif y.date in ours_dates and y.cleared != RECONCILED and not y.has_subtransactions:
            fields["date"] = row.date
            changes["date"] = (y.date, row.date)
            updates["pushed_date"] = row.date

    updates = {k: v for k, v in updates.items() if getattr(entry, k) != v}
    if fields:
        if method is MatchMethod.SETTLEMENT and "amount" in fields:
            reason = REASON_SETTLEMENT
        elif "cleared" in fields:
            reason = REASON_CLEARED
        elif "amount" in fields:
            reason = REASON_AMOUNT
        elif "date" in fields:
            reason = REASON_REDATED
        else:
            reason = REASON_MEMO
        after = TxRef(
            date=fields.get("date", y.date),
            amount=fields.get("amount", y.amount),
            payee=before.payee,
            ynab_tx_id=y.id,
        )
        out.actions.append(
            PlannedAction(
                kind=ActionKind.UPDATE,
                account_id=entry.account_id,
                entry_key=entry.key,
                reason=reason,
                row=row,
                ynab_tx_id=y.id,
                fields={"id": y.id, **fields},
                changes=changes,
                ledger_updates=updates,
                before=before,
                after=after,
            )
        )
        return out
    out.unchanged = True
    if updates:
        out.actions.append(_ledger_action(entry, row, updates, REASON_REFRESH, before=before))
    return out


# ── executor support: a CREATE answered "duplicate import_id" (§6.3 step 2) ──


def is_another_transaction(row: RevolutRow, y: YnabTx | None) -> bool:
    """True if ``y`` — the live transaction YNAB holds under the import_id POSTed for ``row``
    — has another amount or date than the row, so a duplicate answer does not prove it is
    this row (:func:`resolve_duplicate` does not link it)."""
    return y is not None and not y.deleted and (y.amount != row.amount or y.date != row.date)


#: Columns a duplicate resolution sets on an existing entry besides its planned updates.
_LINK_COLUMNS = (
    "status",
    "ynab_tx_id",
    "ynab_import_id",
    "pushed_cleared",
    "pushed_amount",
    "pushed_date",
)


def resolve_duplicate(
    action: PlannedAction,
    y: YnabTx | None,
    opts: PlanOptions,
    ctx: PlanContext,
) -> MatchOutcome:
    """Follow-up actions for a CREATE that YNAB refused as a duplicate import_id.

    ``y`` is the account's transaction carrying ``action.import_id`` after the post-create
    mirror sync (``ctx.mirror.by_import_id``), or None if it cannot be found. The outcome:

    * not found → a new row's entry is recorded as UNLINKED (resolved on a later import; an
      existing entry stays UNLINKED), with a warning;
    * deleted in YNAB → DELETED_IN_YNAB (never re-created);
    * already linked from another entry → no link, warning (never two live links, F02); a new
      row's entry is recorded as UNLINKED;
    * live, but with another amount or date than the row (:func:`is_another_transaction`) →
      not linked (it may be a different transaction whose amount was settled), nothing
      recorded, warning: the next import decides with the whole statement (§6.2 step 4);
    * otherwise → LINK, followed by the same treatment as any ACTIVE match, computed from
      ``y``'s current state (cleared/memo; amount and date already equal the row's).

    pushed_* become what YNAB holds (we did not write it). For a new row the actions carry the
    entry to insert; ``replaces_key`` (re-created rows) still tombstones the old entry.
    """
    if action.kind is not ActionKind.CREATE or action.row is None or action.entry is None:
        raise ValueError("resolve_duplicate() needs a CREATE action with its row and entry")
    row = action.row
    placeholder = action.entry
    out = MatchOutcome()
    ref = _tx_ref(y, row.payee) if y is not None else action.after

    def record(final: LedgerEntry, reason: str, kind: ActionKind = ActionKind.LEDGER) -> None:
        common: dict[str, Any] = {
            "kind": kind,
            "account_id": action.account_id,
            "entry_key": action.entry_key,
            "reason": reason,
            "row": row,
            "ynab_tx_id": final.ynab_tx_id,
            "replaces_key": action.replaces_key,
            "before": ref,
            "after": ref,
        }
        if action.entry_exists:
            updates = dict(action.ledger_updates)
            updates.update({name: getattr(final, name) for name in _LINK_COLUMNS})
            out.actions.append(PlannedAction(**common, entry_exists=True, ledger_updates=updates))
        else:
            out.actions.append(PlannedAction(**common, entry=final))

    if y is None:
        out.warnings.append(
            f"{ctx.describe_row(row)}: YNAB reports import_id {action.import_id} as a duplicate "
            "but the transaction was not found — it will be linked on a later import."
        )
        if not action.entry_exists:
            record(placeholder, REASON_LINK_UNKNOWN)
        return out
    ctx.claimed_tx_ids.add(y.id)
    observed: dict[str, Any] = {
        "ynab_tx_id": y.id,
        "ynab_import_id": y.import_id or placeholder.ynab_import_id,
        "pushed_cleared": _observed_cleared(y),
        "pushed_amount": y.amount,
        "pushed_date": y.date,
    }
    if y.deleted:
        out.ignored_deleted = True
        out.warnings.append(
            f"{ctx.describe_row(row)}: deleted in YNAB — it will not be re-created."
        )
        record(
            dataclasses.replace(placeholder, status=LedgerStatus.DELETED_IN_YNAB.value, **observed),
            REASON_DELETED_IN_YNAB,
        )
        return out
    holder = ctx.holders.get(y.id)
    if holder is not None and holder != placeholder.key:
        out.unchanged = True
        out.warnings.append(
            f"{ctx.describe_row(row)}: YNAB already has it (import_id {action.import_id}) "
            f"linked to another imported row ({holder}) — left alone, please check for a "
            "duplicate."
        )
        if not action.entry_exists:
            record(placeholder, REASON_LINK_UNKNOWN)
        return out
    if is_another_transaction(row, y):
        out.unchanged = True
        out.warnings.append(
            f"{ctx.describe_row(row)}: YNAB already has import_id {action.import_id} for "
            f"{y.date} {format_money(y.amount, ctx.currency)} — not linked in this import; "
            "the next one checks it against the whole statement. Please check for a duplicate."
        )
        return out
    linked = dataclasses.replace(placeholder, status=LedgerStatus.ACTIVE.value, **observed)
    ctx.holders[y.id] = linked.key
    record(linked, REASON_DUPLICATE_RESOLVED, ActionKind.LINK)
    out.extend(_active_entry(row, linked, y, opts, ctx, MatchMethod.DUPLICATE))
    out.unchanged = False
    return out


# ── step 4 support: adopting our own lost creates ────────────────────────────


def _find_orphan(row: RevolutRow, ctx: PlanContext) -> YnabTx | None:
    """A live YNAB transaction of the account that is our own earlier create of ``row`` whose
    ledger write was lost (crash after the POST, lost response, a lost or restored ledger, a
    row imported through another DB such as the CLI's): it has the row's amount and date, its
    import_id is a CSV-derived id that no ledger entry knows — ``YNAB:{row.amount}:{row.date}:
    {n}`` (the row's key or a fresh id of §6.4), or the id of the pending it was before a
    settlement or re-dating changed it (:func:`_settled_since`) — and no entry links it.
    Prefers the row's own key, then ids with the row's amount and date, then the smallest n.
    v1 converged the same way through YNAB's import_id dedupe."""
    best: tuple[bool, bool, int, str] | None = None
    found: YnabTx | None = None
    for y in ctx.orphans.get((row.amount, row.date), ()):
        import_id = y.import_id or ""
        if (
            y.id in ctx.holders
            or ctx.ids.known_to_ledger(import_id)
            or ctx.ids.is_planned(import_id)
        ):
            continue
        parsed = _parse_key(import_id)
        exact = parsed is not None and (parsed[0], parsed[1]) == (row.amount, row.date)
        rank = (import_id != row.key, not exact, parsed[2] if parsed else 0, y.id)
        if best is None or rank < best:
            best, found = rank, y
    return found


def _key_holder(row: RevolutRow, ctx: PlanContext) -> YnabTx | None:
    """The transaction of the account carrying ``row.key`` as its import_id when no ledger
    entry knows that key, none links the transaction and this plan does not POST the key:
    YNAB would refuse the row as a duplicate of it (v1's idempotency, R02). Live or deleted,
    whatever amount or date the user (or another database's settlement) gave it since."""
    if ctx.ids.known_to_ledger(row.key) or ctx.ids.is_planned(row.key):
        return None
    y = ctx.mirror.by_import_id(row.key)
    if y is None or y.id in ctx.holders:
        return None
    return y


def _deleted_key_holder(
    row: RevolutRow, y: YnabTx, opts: PlanOptions, ctx: PlanContext
) -> MatchOutcome:
    """Record ``row`` as the deleted transaction ``y`` that carries its key (no ledger entry
    knew it): DELETED_IN_YNAB — never re-created — or REVERTED for a reverted row."""
    reverted = row.state == STATE_REVERTED
    status = LedgerStatus.REVERTED if reverted else LedgerStatus.DELETED_IN_YNAB
    recorded = LedgerEntry(
        account_id=opts.account_id,
        key=row.key,
        ynab_import_id=row.key,
        ynab_tx_id=y.id,
        status=status.value,
        csv_date=row.date,
        csv_amount=row.amount,
        csv_payee=row.payee,
        csv_type=row.type,
        csv_state=row.state,
        started_at=ctx.started_text(row),
        identity=IDENTITY_CSV,
        pushed_cleared=_observed_cleared(y),
        pushed_amount=row.amount,  # what was POSTed under the row's key (see _adopt_orphan)
        pushed_date=row.date,
    )
    ctx.holders[y.id] = row.key
    ctx.claimed_tx_ids.add(y.id)
    ctx.ids.reserve(row.key)
    ref = _tx_ref(y, row.payee)
    out = MatchOutcome(unchanged=reverted, ignored_deleted=not reverted)
    if not reverted:
        out.warnings.append(
            f"{ctx.describe_row(row)}: deleted in YNAB — it will not be re-created."
        )
    out.actions.append(
        PlannedAction(
            kind=ActionKind.LEDGER,
            account_id=opts.account_id,
            entry_key=row.key,
            reason=REASON_REVERTED if reverted else REASON_DELETED_IN_YNAB,
            row=row,
            ynab_tx_id=y.id,
            entry=recorded,
            before=ref,
            after=ref,
            note="YNAB already had this row (deleted); its ledger entry was missing",
        )
    )
    return out


def _adopt_orphan(
    row: RevolutRow,
    y: YnabTx,
    opts: PlanOptions,
    ctx: PlanContext,
    *,
    replaces_key: str | None = None,
) -> MatchOutcome:
    """LINK ``row`` to the orphan ``y`` (:func:`_find_orphan`, or the transaction carrying the
    row's key, :func:`_key_holder`) as a new ledger entry keyed by ``y``'s import_id, then
    apply the usual per-match rules to it. ``replaces_key`` is the removed entry a re-created
    row replaces (it becomes a REKEYED tombstone).

    The entry describes the row (``csv_state`` included: a pending row stays a pending that
    can settle later, even when YNAB shows it cleared). ``pushed_amount``/``pushed_date`` are
    the row's: what an importer wrote — the orphan has them (§6.4 id with the row's amount
    and date, or what a settlement wrote), the key holder was created with them. Another
    amount or date on a key holder is therefore an edit in YNAB, and is never overwritten.
    The usual rules (clear, strip the marker, delete if reverted) apply to what YNAB holds."""
    import_id = y.import_id or row.key
    linked = LedgerEntry(
        account_id=opts.account_id,
        key=import_id,
        ynab_import_id=import_id,
        ynab_tx_id=y.id,
        status=LedgerStatus.ACTIVE.value,
        csv_date=row.date,
        csv_amount=row.amount,
        csv_payee=row.payee,
        csv_type=row.type,
        csv_state=row.state,
        started_at=ctx.started_text(row),
        identity=IDENTITY_CSV,
        pushed_cleared=_observed_cleared(y),
        pushed_amount=row.amount,
        pushed_date=row.date,
    )
    ctx.holders[y.id] = import_id
    ctx.claimed_tx_ids.add(y.id)
    ctx.ids.reserve(import_id)
    follow = _active_entry(row, linked, y, opts, ctx, MatchMethod.ADOPTED)
    # A local-only refresh becomes part of the inserted entry.
    for action in follow.actions:
        if action.kind is ActionKind.LEDGER:
            linked = dataclasses.replace(linked, **action.ledger_updates)
    ref = _tx_ref(y, row.payee)
    out = MatchOutcome(warnings=list(follow.warnings), skipped=list(follow.skipped))
    out.actions.append(
        PlannedAction(
            kind=ActionKind.LINK,
            account_id=opts.account_id,
            entry_key=import_id,
            reason=REASON_ADOPTED,
            row=row,
            ynab_tx_id=y.id,
            entry=linked,
            replaces_key=replaces_key,
            before=ref,
            after=ref,
            note="YNAB already had this row; its ledger entry was missing",
        )
    )
    out.actions.extend(a for a in follow.actions if a.kind is not ActionKind.LEDGER)
    return out


# ── allow_sweep (§6.2) ───────────────────────────────────────────────────────


def compute_allow_sweep(
    export: ParsedExport,
    ledger_entries: Iterable[LedgerEntry],
    *,
    account_id: str | None = None,
) -> bool:
    """False when the export is older than what was imported (F36), else True.

    Older = the export's window end is earlier than the newest ``csv_date`` among the
    account's ACTIVE entries minus one day. An export without rows never sweeps. (The planner
    additionally treats an export as older when a ledger entry started after the export's
    newest timestamp, which also catches older exports inside the one-day slack.)
    """
    if export.window is None:
        return False
    newest: str | None = None
    for entry in ledger_entries:
        if entry.status != LedgerStatus.ACTIVE:
            continue
        if account_id is not None and entry.account_id != account_id:
            continue
        if newest is None or entry.csv_date > newest:
            newest = entry.csv_date
    if newest is None:
        return True
    end, newest_day = _day(export.window[1]), _day(newest)
    if end is None or newest_day is None:
        return False
    return end >= newest_day - _ONE_DAY


def statement_range_is_current(export: ParsedExport, today: date) -> bool:
    """False when the range in the statement's file name ended more than a day before
    ``today`` (the import date); True otherwise, including when the name has no range.

    A statement whose range ends in the past omits transactions that completed after the
    range end, so a pending missing from it proves nothing (e.g. a September statement
    downloaded on 10-02 omits a 09-29 pending that completed on 10-01). The service passes
    the result as :attr:`PlanOptions.range_is_current`.
    """
    if export.filename_window is None:
        return True
    range_end = _day(export.filename_window[1])
    return range_end is not None and range_end >= today - _ONE_DAY


# ── the planner ──────────────────────────────────────────────────────────────


def plan_import(
    export: ParsedExport,
    ledger: Iterable[LedgerEntry],
    ynab: MirrorView,
    opts: PlanOptions,
) -> Plan:
    """§6.2 steps 1-5: the actions that bring YNAB and the ledger in line with ``export``.

    ``ledger`` may hold entries of other accounts; only ``opts.account_id``'s (and unclaimed
    legacy entries') are considered. ``ynab`` must be the account's mirror snapshot.
    """
    if ynab.account_id != opts.account_id:
        raise ValueError(
            f"mirror view is for account {ynab.account_id!r}, plan is for {opts.account_id!r}"
        )
    entries = _account_entries(ledger, opts.account_id)
    rows = export.rows
    window = export.window

    # Step 1 + 2.
    offsets = export_offsets(export, entries)
    if offsets:
        log.info(
            "import plan %s: export offsets %s",
            opts.account_id,
            ", ".join(f"{d / 3600:+g}h×{n}" for d, n in sorted(offsets.items())),
        )
    pairs = match_rows(export, entries, offsets)
    matched_entry_indexes = {p.entry_index for p in pairs}
    # Window/recency tests consider the offsets matches actually used (a learned offset no
    # match used, e.g. from a round-time renumbering collision, is no evidence of a frame).
    frame = ExportFrame.of(export, _used_offsets(pairs))
    if not opts.range_is_current:
        # A range that ended before the import proves nothing missing (sweep/vanished only;
        # this is not an *older* export, so matching and creates are unaffected).
        frame = dataclasses.replace(frame, coverage=None)
    newest = _export_is_older(entries, frame)
    regressed = _regressed_match(pairs, rows, entries)
    stale = not opts.allow_sweep or newest is not None or regressed is not None
    newer = all(frame.postdates(e) for e in entries if e.status != LedgerStatus.REKEYED)
    ctx = PlanContext.create(
        entries,
        ynab,
        opts,
        currency=export.currency,
        frame_offset=_frame_offset(pairs),
        stale=stale,
        newer=newer,
    )

    plan = Plan(
        account_id=opts.account_id,
        allow_sweep=opts.allow_sweep and not stale,
        window=window,
        frame_offset=ctx.frame_offset,
        stale_export=stale,
    )
    plan.offsets = offsets
    plan.warnings.extend(export.warnings)
    skipped: list[PlanSkip] = [PlanSkip(s.line_no, s.code, s.reason) for s in export.skipped]
    actions: list[PlannedAction] = []

    # Step 3.
    method_counts: Counter[str] = Counter()
    for pair in pairs:
        row, entry = rows[pair.row_index], entries[pair.entry_index]
        plan.matches.append(pair.info)
        method_counts[pair.info.method.value] += 1
        y = ynab.by_id(entry.ynab_tx_id)
        outcome = actions_for_match(row, entry, y, opts, ctx, method=pair.info.method)
        actions.extend(outcome.actions)
        plan.warnings.extend(outcome.warnings)
        skipped.extend(outcome.skipped)
        if outcome.ignored_deleted:
            plan.ignored_deleted += 1
        elif outcome.unchanged:
            plan.unchanged += 1

    # Step 4.
    matched_rows = {p.row_index for p in pairs}
    unmatched = [ri for ri in range(len(rows)) if ri not in matched_rows]
    adopted_rows: set[int] = set()

    def adopt(ri: int, outcome: MatchOutcome) -> None:
        row = rows[ri]
        adopted_rows.add(ri)
        plan.matches.append(
            MatchInfo(row.line_no, row.key, outcome.actions[0].entry_key, MatchMethod.ADOPTED)
        )
        method_counts[MatchMethod.ADOPTED.value] += 1
        actions.extend(outcome.actions)
        plan.warnings.extend(outcome.warnings)
        skipped.extend(outcome.skipped)
        if outcome.ignored_deleted:
            plan.ignored_deleted += 1

    # Transactions YNAB holds for unmatched rows that no ledger entry knows: first by the
    # row's amount and date (all rows, so a settled transaction goes to its settled row and
    # not to a new row that now carries its old key), then by the row's key.
    for ri in unmatched:
        orphan = _find_orphan(rows[ri], ctx)
        if orphan is not None:
            adopt(ri, _adopt_orphan(rows[ri], orphan, opts, ctx))
    for ri in unmatched:
        holder = None if ri in adopted_rows else _key_holder(rows[ri], ctx)
        if holder is None:
            continue
        if holder.deleted:
            adopt(ri, _deleted_key_holder(rows[ri], holder, opts, ctx))
        else:
            adopt(ri, _adopt_orphan(rows[ri], holder, opts, ctx))
    held_back = 0
    for ri in unmatched:
        row = rows[ri]
        if ri in adopted_rows:
            continue
        if row.state == STATE_REVERTED:
            skipped.append(
                PlanSkip(
                    row.line_no,
                    SKIP_REVERTED_NEVER_IMPORTED,
                    "reverted by Revolut (never imported)",
                    row,
                )
            )
            actions.append(_reverted_never_imported(row, opts, ctx))
            continue
        if row.state == STATE_PENDING and opts.skip_pending:
            skipped.append(_skip_pending(row))
            continue
        if row.state == STATE_PENDING and ctx.stale:
            # An older statement: the pending may have settled, been re-dated or reverted
            # since (F36). A current statement imports it if it is still pending.
            skipped.append(_skip_pending_older(row))
            held_back += 1
            continue
        import_id = ctx.ids.choose(row)
        payload = _create_payload(row, import_id, opts)
        actions.append(
            PlannedAction(
                kind=ActionKind.CREATE,
                account_id=opts.account_id,
                entry_key=import_id,
                reason=REASON_NEW,
                row=row,
                payload=payload,
                entry=_new_entry(row, import_id, payload, ctx),
                after=_row_ref(row),
            )
        )

    if stale and (held_back or any(r.state == STATE_PENDING for r in rows)):
        plan.warnings.append(_older_export_warning(newest, regressed, ctx))

    # Step 5.
    swept = vanished = 0
    if not stale and window is not None:
        swept, vanished = _sweep(entries, matched_entry_indexes, ctx, plan, actions, frame)

    actions.sort(key=PlannedAction.sort_key)
    plan.actions = actions
    plan.skipped_rows = sorted(skipped, key=lambda s: (s.line_no, s.code))
    plan.reserved_import_ids = ctx.ids.chosen
    states = export.state_counts()
    plan.counters = {
        "rows": len(rows),
        "rows_completed": states.get(STATE_COMPLETED, 0),
        "rows_pending": states.get(STATE_PENDING, 0),
        "rows_reverted": states.get(STATE_REVERTED, 0),
        "rows_skipped_by_parser": len(export.skipped),
        "rows_other_products": sum(export.other_products.values()),
        "ledger_entries": len(entries),
        "matched": len(pairs),
        **{f"matched_{m.value}": method_counts.get(m.value, 0) for m in MatchMethod},
        "unmatched_rows": len(rows) - len(pairs) - method_counts[MatchMethod.ADOPTED.value],
        **{f"actions_{k.value.lower()}": 0 for k in ActionKind},
        "swept": swept,
        "vanished": vanished,
        "pending_held_back": held_back,
    }
    for action in actions:
        plan.counters[f"actions_{action.kind.value.lower()}"] += 1
    log.info("import plan %s: %s", opts.account_id, plan.summary()["actions"])
    return plan


def _reverted_never_imported(row: RevolutRow, opts: PlanOptions, ctx: PlanContext) -> PlannedAction:
    """Record a reverted row that was never imported as a REVERTED ledger entry (local only).

    Nothing is written to YNAB; the entry makes later imports of the same statement match the
    row (no-op), and it is evidence of how recent the imported statements were: a re-sent
    older statement is recognised even when the newer one's latest row was a reverted hold
    (F36). Keyed by the row's key, or a fresh id when another entry holds it (§6.4).
    """
    key = ctx.ids.choose(row)
    entry = LedgerEntry(
        account_id=opts.account_id,
        key=key,
        status=LedgerStatus.REVERTED.value,
        csv_date=row.date,
        csv_amount=row.amount,
        csv_payee=row.payee,
        csv_type=row.type,
        csv_state=row.state,
        started_at=ctx.started_text(row),
        identity=IDENTITY_CSV,
    )
    return PlannedAction(
        kind=ActionKind.LEDGER,
        account_id=opts.account_id,
        entry_key=key,
        reason=REASON_NEVER_IMPORTED,
        row=row,
        entry=entry,
    )


def _older_export_warning(
    newest: LedgerEntry | None, regressed: LedgerEntry | None, ctx: PlanContext
) -> str:
    """The one warning an older statement gets (F36)."""
    detail = ""
    if newest is not None:
        detail = f" ({ctx.describe_entry(newest)} was imported before and is not in it)"
    elif regressed is not None:
        detail = (
            f" ({ctx.describe_entry(regressed)} was imported as "
            f"{regressed.csv_state.lower()} and is still pending in it)"
        )
    return (
        "This statement is older than what was already imported"
        f"{detail}: pending rows were left as they are and nothing was removed. "
        "Import a current statement to update them."
    )


def _sweep(
    entries: Sequence[LedgerEntry],
    matched: set[int],
    ctx: PlanContext,
    plan: Plan,
    actions: list[PlannedAction],
    frame: ExportFrame,
) -> tuple[int, int]:
    """§6.2 step 5: delete unmatched, still-uncleared pendings the export covers.

    "Covers" (:meth:`ExportFrame.covers`): inside the export's *coverage* — the span the
    statement is provably complete for, which starts at its first completion and range start
    rather than its first Started date — and started before the export's newest timestamp,
    under every plausible offset. A pending that completed before the statement's range, that
    started after an older statement was generated, or that a shifted rendering puts outside
    the coverage, is never deleted (F36; production dry run 2026-10-17).
    """
    swept_ids: set[str] = set()
    vanished: list[LedgerEntry] = []
    for ei, entry in enumerate(entries):
        if ei in matched or entry.status != LedgerStatus.ACTIVE:
            continue
        if not frame.covers(entry):
            continue
        y = ctx.mirror.by_id(entry.ynab_tx_id)
        if y is None or y.deleted or y.id in ctx.claimed_tx_ids or y.id in swept_ids:
            continue
        if y.account_id != ctx.account_id:
            continue  # moved to another account by the user: not ours to delete (F39)
        if entry.csv_state == CsvState.PENDING:
            if y.cleared == UNCLEARED:
                swept_ids.add(y.id)
                actions.append(
                    PlannedAction(
                        kind=ActionKind.DELETE,
                        account_id=entry.account_id,
                        entry_key=entry.key,
                        reason=REASON_SUPERSEDED,
                        ynab_tx_id=y.id,
                        ledger_updates={"status": LedgerStatus.SUPERSEDED.value},
                        before=_tx_ref(y, entry.csv_payee),
                    )
                )
            else:
                plan.warnings.append(
                    f"{ctx.describe_entry(entry)}: pending vanished from Revolut but "
                    f"{y.cleared} in YNAB — left alone."
                )
        elif entry.csv_state == CsvState.COMPLETED:
            vanished.append(entry)
    for entry in vanished[:MAX_VANISHED_LISTED]:
        plan.warnings.append(
            f"{ctx.describe_entry(entry)}: no longer in the Revolut statement — left alone in YNAB."
        )
    if len(vanished) > MAX_VANISHED_LISTED:
        plan.warnings.append(
            f"… and {len(vanished) - MAX_VANISHED_LISTED} more transaction(s) no longer in "
            "the statement."
        )
    return len(swept_ids), len(vanished)
