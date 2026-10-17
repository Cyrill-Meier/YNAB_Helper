"""Import executor: apply a :class:`~ynab_helper.importer.models.Plan` (docs/DESIGN.md §6.3).

Order of work (the plan lists actions in this order):

1. **LINK / LEDGER** — local only, all in one SQLite transaction, before any YNAB write. A
   failure here is a bug: it is raised and nothing is sent to YNAB.
2. **CREATE** — bulk POST in chunks of 100. The ledger is committed per chunk, after the HTTP
   call returned, and only for the transactions YNAB returned. A chunk YNAB rejects as a
   whole (4xx) is split in halves until the offending rows are isolated (every create
   carries an import_id, so a retry can never duplicate); the extra requests one chunk may
   cost are capped (:func:`_bisect_allowance`), so a rejection that applies to every row
   cannot use up YNAB's hourly quota (F17). A 404 means the budget or account is gone (it
   changed since the sync just before): no further write of this import is sent, since a
   404 on a later PUT or DELETE would wrongly read as "this transaction was deleted". ``duplicate_import_ids`` are
   resolved after the last chunk: one mirror delta sync, ``refresh_from_mirror``, then
   :func:`~ynab_helper.importer.planner.resolve_duplicate` → LINK (plus the usual ACTIVE
   treatment, whose UPDATE/DELETE run in this same import), DELETED_IN_YNAB, or a warning.
3. **UPDATE** — bulk ``PATCH /budgets/{b}/transactions`` in chunks of 50. A 4xx for the bulk
   request falls back to one ``PUT`` per transaction to isolate the failing row; a 404 there
   marks the entry DELETED_IN_YNAB (it is never written again) and the rest continues (F06).
   The ledger only records what YNAB returned.
4. **DELETE** — one request per transaction; a 404 counts as removed.

Error model (§6.3.5): per-action failures are collected in ``ImportResult.failures`` and the
import goes on (F06). :class:`~ynab_helper.errors.YnabRateLimited` stops every further YNAB
write; what was not attempted is counted in ``not_applied`` and ``rate_limited`` is set (F07),
and the backoff is remembered so throttled syncs stay quiet for a while
(:func:`~ynab_helper.sync.ynab_sync.note_rate_limited`).
:class:`~ynab_helper.errors.YnabAuthError` propagates. Local state advances only after YNAB
confirmed a write (F07), and no SQLite transaction is ever open across an HTTP call (F82).

``dry_run`` sends nothing and writes nothing: the result reports the plan as if applied
(F74). Every confirmed YNAB write gets an INFO line on the ``ynab_helper.audit`` logger
(date, amount, payee, YNAB id, reason) — bot.log is the user's audit trail.
"""

from __future__ import annotations

import dataclasses
import logging
import math
import sqlite3
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import datetime
from typing import Any

from ynab_helper.errors import (
    YnabAuthError,
    YnabError,
    YnabNotFound,
    YnabRateLimited,
)
from ynab_helper.importer.models import (
    ActionKind,
    Failure,
    ImportResult,
    LedgerStatus,
    Plan,
    PlannedAction,
    PlanOptions,
    TxRef,
)
from ynab_helper.importer.planner import PlanContext, resolve_duplicate
from ynab_helper.logging_setup import AUDIT_LOGGER
from ynab_helper.money import format_money
from ynab_helper.revolut.parser import ParsedExport
from ynab_helper.storage.db import StorageError, transaction
from ynab_helper.storage.ledger import LedgerStore
from ynab_helper.storage.mirror import MirrorStore
from ynab_helper.sync.ynab_sync import mark_mirror_stale, note_rate_limited, sync_budget
from ynab_helper.ynab.client import CREATE_CHUNK_SIZE, UPDATE_CHUNK_SIZE, YnabClient
from ynab_helper.ynab.models import YnabTransaction

__all__ = ["PHASE_CREATE", "PHASE_DELETE", "PHASE_UPDATE", "ProgressCallback", "apply_plan"]

log = logging.getLogger(__name__)
audit = logging.getLogger(AUDIT_LOGGER)

#: ``on_progress(phase, done, total)``: ``done`` of ``total`` YNAB writes are settled.
ProgressCallback = Callable[[str, int, int], None]

PHASE_CREATE = "create"
PHASE_UPDATE = "update"
PHASE_DELETE = "delete"

#: Ledger write problems that must not abort an import whose YNAB write already happened.
_LEDGER_ERRORS: tuple[type[BaseException], ...] = (sqlite3.Error, StorageError, ValueError)
#: (PATCH field, ledger column recording what we pushed).
_PUSHED_COLUMNS = (
    ("amount", "pushed_amount"),
    ("date", "pushed_date"),
    ("cleared", "pushed_cleared"),
)


def _bisect_allowance(size: int) -> int:
    """Extra POSTs a create chunk of ``size`` rows that YNAB rejected (4xx) may cost to
    isolate the rejected rows: enough for one bad row (2·⌈log₂ n⌉) plus a margin, instead of
    the 2n − 1 requests a rejection of every row would cost when split down to single rows."""
    if size < 2:
        return 0
    return 2 * math.ceil(math.log2(size)) + 4


@dataclasses.dataclass(slots=True)
class _Allowance:
    """Remaining extra requests for isolating the rejected rows of one create chunk."""

    left: int


class _RateLimitStop(Exception):
    """Internal: YNAB answered 429; no further write may be sent in this import."""

    def __init__(self, error: YnabRateLimited) -> None:
        super().__init__(str(error))
        self.error = error


def _is_client_error(exc: YnabError) -> bool:
    """A 4xx YNAB rejected (rate limit and authentication are handled separately)."""
    if isinstance(exc, YnabRateLimited | YnabAuthError):
        return False
    return exc.status is not None and 400 <= exc.status < 500


def _chunks(items: Sequence[PlannedAction], size: int) -> Iterator[list[PlannedAction]]:
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def _unique_batches(actions: Sequence[PlannedAction]) -> list[list[PlannedAction]]:
    """Split ``actions`` into batches without a repeated transaction id (YNAB's bulk PATCH
    must not list one transaction twice), each at most :data:`UPDATE_CHUNK_SIZE` long."""
    rounds: list[list[PlannedAction]] = []
    for action in actions:
        for batch in rounds:
            if all(other.ynab_tx_id != action.ynab_tx_id for other in batch):
                batch.append(action)
                break
        else:
            rounds.append([action])
    return [chunk for batch in rounds for chunk in _chunks(batch, UPDATE_CHUNK_SIZE)]


def _error_text(exc: BaseException) -> str:
    """A message that is safe to show to the user (never a token or a stack trace)."""
    text = getattr(exc, "user_message", None) or str(exc) or type(exc).__name__
    return str(text)[:300]


def _default_options(plan: Plan, account_id: str) -> PlanOptions:
    approved = next(
        (bool(a.payload.get("approved")) for a in plan.by_kind(ActionKind.CREATE)), True
    )
    return PlanOptions(account_id=account_id, approved=approved, allow_sweep=plan.allow_sweep)


def apply_plan(
    plan: Plan,
    client: YnabClient,
    conn: sqlite3.Connection,
    *,
    budget_id: str,
    account_id: str,
    export: ParsedExport,
    dry_run: bool,
    opts: PlanOptions | None = None,
    now: datetime | None = None,
    on_progress: ProgressCallback | None = None,
) -> ImportResult:
    """Apply ``plan`` to YNAB and the ledger in ``conn``; return what happened (§6.3).

    ``opts`` are the options the plan was made with (needed to resolve duplicate import_ids
    mid-run); by default they are derived from the plan. ``now`` (timezone-aware) stamps the
    post-create mirror sync. ``on_progress(phase, done, total)`` is called after each chunk or
    row; an exception in it is logged and ignored.

    Raises :class:`~ynab_helper.errors.YnabAuthError` (YNAB refused the token; writes made
    before it are recorded), ``ValueError`` for a plan that does not belong to
    ``account_id``, and storage errors from the local (LINK/LEDGER) phase, which runs before
    any YNAB write.
    """
    if plan.account_id != account_id:
        raise ValueError(f"plan is for account {plan.account_id!r}, not {account_id!r}")
    if dry_run:
        result = ImportResult.from_plan_dry_run(plan, export)
        log.info("import %s (dry run): %s", account_id, plan.summary()["actions"])
        return result
    _check_plan_account(plan, account_id)
    run = _Execution(
        plan=plan,
        client=client,
        conn=conn,
        budget_id=budget_id,
        account_id=account_id,
        export=export,
        opts=opts if opts is not None else _default_options(plan, account_id),
        now=now,
        on_progress=on_progress,
    )
    return run.execute()


def _check_plan_account(plan: Plan, account_id: str) -> None:
    """Refuse a plan that would touch another account (F12): every action and every POSTed
    transaction must belong to ``account_id`` (unclaimed legacy entries must have been
    claimed before planning a real import)."""
    for action in plan.actions:
        if action.account_id != account_id:
            raise ValueError(
                f"{action.kind.value} for ledger entry {action.entry_key!r} belongs to "
                f"account {action.account_id!r}, not {account_id!r}"
            )
        if action.kind is ActionKind.CREATE and action.payload.get("account_id") != account_id:
            raise ValueError(f"CREATE {action.entry_key!r} posts to another account")


class _Execution:
    """State of one :func:`apply_plan` run."""

    def __init__(
        self,
        *,
        plan: Plan,
        client: YnabClient,
        conn: sqlite3.Connection,
        budget_id: str,
        account_id: str,
        export: ParsedExport,
        opts: PlanOptions,
        now: datetime | None,
        on_progress: ProgressCallback | None,
    ) -> None:
        self.plan = plan
        self.client = client
        self.conn = conn
        self.ledger = LedgerStore(conn)
        self.mirror = MirrorStore(conn)
        self.budget_id = budget_id
        self.account_id = account_id
        self.export = export
        self.currency = export.currency
        self.opts = opts
        self.now = now
        self.on_progress = on_progress
        self.result = ImportResult.for_export(export, account_id=account_id, dry_run=False)
        self.result.add_plan_counts(plan)
        self.stopped: YnabRateLimited | None = None
        #: Set after a 404 on a create: the budget or account is gone, so no write may follow
        #: (a 404 on a later PUT or DELETE would wrongly read as "this transaction is gone").
        self.blocked: str | None = None
        self.settled: set[int] = set()  # id() of write actions that got a final outcome
        self.total = len(plan.ynab_actions)
        self.done = 0

    # ── driver ──────────────────────────────────────────────────────────────

    def execute(self) -> ImportResult:
        self._apply_local(self.plan.local_actions)
        try:
            duplicates = self._create_phase(self.plan.by_kind(ActionKind.CREATE))
            follow_ups = self._resolve_duplicates(duplicates)
            updates = [*self.plan.by_kind(ActionKind.UPDATE)]
            deletes = [*self.plan.by_kind(ActionKind.DELETE)]
            for action in follow_ups:
                (updates if action.kind is ActionKind.UPDATE else deletes).append(action)
            self.total += len(follow_ups)
            self._update_phase(updates)
            self._delete_phase(deletes)
        finally:
            if self.total:
                self._mark_mirror_stale()
        result = self.result
        log.info(
            "import %s: created=%d linked=%d updated=%d removed=%d unchanged=%d "
            "failures=%d not_applied=%d rate_limited=%s",
            self.account_id,
            len(result.created),
            len(result.linked),
            len(result.updated),
            len(result.removed),
            result.unchanged,
            len(result.failures),
            result.not_applied,
            result.rate_limited,
        )
        return result

    # ── bookkeeping ─────────────────────────────────────────────────────────

    def _progress(self, phase: str) -> None:
        if self.on_progress is None:
            return
        try:
            self.on_progress(phase, self.done, self.total)
        except Exception:  # a UI callback must never break an import
            log.warning("import progress callback failed", exc_info=True)

    def _settle(self, action: PlannedAction) -> None:
        if id(action) not in self.settled:
            self.settled.add(id(action))
            self.done += 1

    def _fail(self, action: PlannedAction, error: str) -> None:
        tx = action.tx_ref() or TxRef("", 0, "", action.ynab_tx_id)
        self.result.failures.append(Failure(action=action.kind.value, tx=tx, error=error))
        log.warning(
            "import %s: %s %s failed: %s",
            self.account_id,
            action.kind.value,
            action.entry_key,
            error,
        )
        self._settle(action)

    def _fail_all(self, actions: Iterable[PlannedAction], error: str) -> None:
        for action in actions:
            if id(action) not in self.settled:
                self._fail(action, error)

    def _not_applied(self, actions: Iterable[PlannedAction]) -> None:
        for action in actions:
            if id(action) in self.settled:
                continue
            self.result.not_applied += 1
            self._settle(action)

    def _stop(self, error: YnabRateLimited) -> None:
        if self.stopped is None:
            self.stopped = error
            self.result.rate_limited = True
            wait = f" (retry after {error.retry_after:g} s)" if error.retry_after else ""
            self.result.warnings.append(
                "YNAB's rate limit (200 requests per hour) was reached: the remaining changes "
                f"were not applied{wait}. Send the statement again later to finish the import."
            )
            log.warning("import %s: rate limited by YNAB, stopping writes", self.account_id)
            try:  # throttled syncs (/status right after this import) must not insist (F17)
                note_rate_limited(self.mirror, self.budget_id, error.retry_after, now=self.now)
            except _LEDGER_ERRORS:
                log.warning("could not remember the YNAB rate limit", exc_info=True)

    def _block(self, error: YnabError) -> None:
        if self.blocked is None:
            self.blocked = _error_text(error)
            self.result.warnings.append(
                "YNAB no longer finds the budget or the account (it changed since the sync "
                "just before this import): nothing else was sent. Check your YNAB setup and "
                "send the statement again."
            )
            log.warning("import %s: create answered 404, stopping writes", self.account_id)

    def _mark_mirror_stale(self) -> None:
        """The mirror predates this import's writes: let the next (even throttled) sync run,
        so balances shown right after the import include them. A failure here must never
        hide the import's result (the writes are done); it only delays that refresh."""
        try:
            mark_mirror_stale(self.mirror, self.budget_id)
        except _LEDGER_ERRORS:
            log.warning(
                "import %s: could not mark the mirror stale", self.account_id, exc_info=True
            )

    def _describe(self, ref: TxRef | None) -> str:
        if ref is None:
            return "transaction"
        return f"{ref.date} {format_money(ref.amount, self.currency)} {ref.payee}".strip()

    def _audit(self, verb: str, action: PlannedAction, ref: TxRef | None, detail: str = "") -> None:
        ref = ref or action.tx_ref()
        audit.info(
            "%s %s %s %r id=%s reason=%s account=%s%s",
            verb,
            ref.date if ref else "?",
            format_money(ref.amount, self.currency) if ref else "?",
            ref.payee if ref else "",
            (ref.ynab_tx_id if ref and ref.ynab_tx_id else action.ynab_tx_id) or "?",
            action.reason,
            self.account_id,
            f" {detail}" if detail else "",
        )

    # ── ledger effects ──────────────────────────────────────────────────────

    def _tombstone(self, action: PlannedAction) -> None:
        tomb = action.tombstone_updates()
        if tomb and action.replaces_key:
            self.ledger.update(action.account_id, action.replaces_key, **tomb)

    def _local_effect(self, action: PlannedAction) -> None:
        """LINK/LEDGER: insert the carried entry, else apply the planned updates."""
        if action.entry is not None and not action.entry_exists:
            self.ledger.insert(action.entry)
        else:
            self.ledger.update(action.account_id, action.entry_key, **action.ledger_updates)
        self._tombstone(action)

    def _commit(self, effects: Sequence[tuple[PlannedAction, Callable[[], None]]]) -> list[bool]:
        """Run each ledger effect in its own savepoint inside one transaction.

        Only called after YNAB confirmed the writes. A failing effect is reported (the YNAB
        write stands; the next import reconciles the ledger from the mirror) without undoing
        the others. If the transaction itself fails (``BEGIN`` on a database locked past the
        busy timeout, ``COMMIT``), nothing was stored and every effect not reported yet is
        reported. Returns, per effect, whether it was stored.
        """
        stored = [False] * len(effects)
        reported = [False] * len(effects)
        if not effects:
            return stored
        try:
            with transaction(self.conn):
                for index, (action, effect) in enumerate(effects):
                    try:
                        with transaction(self.conn):
                            effect()
                    except _LEDGER_ERRORS as exc:
                        self._ledger_failure(action, exc)
                        reported[index] = True
                    else:
                        stored[index] = True
        except _LEDGER_ERRORS as exc:
            for index, (action, _effect) in enumerate(effects):
                if not reported[index]:
                    self._ledger_failure(action, exc)
            return [False] * len(effects)
        return stored

    def _ledger_failure(self, action: PlannedAction, exc: BaseException) -> None:
        log.error(
            "import %s: YNAB applied %s %s but the ledger write failed",
            self.account_id,
            action.kind.value,
            action.entry_key,
            exc_info=exc,
        )
        tx = action.tx_ref() or TxRef("", 0, "", action.ynab_tx_id)
        self.result.failures.append(
            Failure(
                action=action.kind.value,
                tx=tx,
                error="applied in YNAB, but the local record could not be saved; "
                "send the statement again to reconcile it",
            )
        )

    # ── 1. local actions ────────────────────────────────────────────────────

    def _apply_local(self, actions: Sequence[PlannedAction]) -> None:
        if not actions:
            return
        with transaction(self.conn):
            for action in actions:
                self._local_effect(action)
        for action in actions:
            self.result.record_applied(action)
            if action.kind is ActionKind.LINK:
                log.info(
                    "import %s: linked %s to YNAB %s (%s)",
                    self.account_id,
                    action.entry_key,
                    action.ynab_tx_id,
                    action.reason,
                )

    # ── 2. creates ──────────────────────────────────────────────────────────

    def _create_phase(self, actions: Sequence[PlannedAction]) -> list[PlannedAction]:
        duplicates: list[PlannedAction] = []
        for chunk in _chunks(actions, CREATE_CHUNK_SIZE):
            if self.stopped is not None:
                self._not_applied(chunk)
                continue
            try:
                self._post(chunk, duplicates, _Allowance(_bisect_allowance(len(chunk))))
            except _RateLimitStop as stop:
                self._stop(stop.error)
                self._not_applied(chunk)
            self._progress(PHASE_CREATE)
        return duplicates

    def _post(
        self,
        chunk: list[PlannedAction],
        duplicates: list[PlannedAction],
        allowance: _Allowance,
        *,
        rejected: str | None = None,
    ) -> None:
        """POST one chunk (≤ 100) or a part of it.

        A payload the client refuses before sending is isolated by bisection at no request
        cost. A 4xx from YNAB is bisected too, but each part sent costs one request of
        ``allowance``; ``rejected`` (YNAB's error for the enclosing part) marks such a part,
        and once the allowance is used up the remaining parts fail with it. A 404 means the
        budget or the account is gone: this and every later write of the import fails
        without a request (:meth:`_block`). A 5xx or network error fails the part as a whole
        (nothing row-specific to isolate).
        """
        pending = [action for action in chunk if id(action) not in self.settled]
        if not pending:
            return
        if self.blocked is not None:
            self._fail_all(pending, self.blocked)
            return
        try:
            # Validates every payload; no request is sent before the iterator is consumed.
            batches = self.client.iter_create_transactions(
                self.budget_id, [action.ynab_payload() for action in pending]
            )
        except (TypeError, ValueError) as exc:  # refused by the client before sending
            if len(pending) > 1:
                self._bisect(pending, duplicates, allowance, rejected)
            else:
                self._fail(pending[0], f"invalid transaction data: {exc}")
            return
        if rejected is not None:
            if allowance.left <= 0:
                self._fail_all(
                    pending,
                    f"{rejected} (not retried row by row, to spare YNAB's hourly request "
                    "limit; send the statement again)",
                )
                return
            allowance.left -= 1
        try:
            for created in batches:
                self._record_creates(
                    pending, created.created, created.duplicate_import_ids, duplicates
                )
        except YnabRateLimited as exc:
            raise _RateLimitStop(exc) from None
        except YnabAuthError:
            raise
        except YnabNotFound as exc:
            # The payloads reference no resource but the account (the same for every row):
            # the budget or the account no longer exists. No other write is sent.
            self._block(exc)
            self._fail_all(pending, self.blocked or _error_text(exc))
        except YnabError as exc:
            if _is_client_error(exc) and len(pending) > 1:
                self._bisect(pending, duplicates, allowance, _error_text(exc))
            else:
                self._fail_all(pending, _error_text(exc))

    def _bisect(
        self,
        chunk: list[PlannedAction],
        duplicates: list[PlannedAction],
        allowance: _Allowance,
        rejected: str | None,
    ) -> None:
        middle = len(chunk) // 2
        for half in (chunk[:middle], chunk[middle:]):
            self._post(half, duplicates, allowance, rejected=rejected)

    def _record_creates(
        self,
        chunk: Sequence[PlannedAction],
        created: Sequence[YnabTransaction],
        duplicate_ids: Sequence[str],
        duplicates: list[PlannedAction],
    ) -> None:
        by_import_id = {action.import_id: action for action in chunk}
        confirmed: list[tuple[PlannedAction, YnabTransaction]] = []
        for tx in created:
            action = by_import_id.pop(tx.import_id, None)
            if action is None:
                log.error(
                    "import %s: YNAB created transaction %s with unexpected import_id %r",
                    self.account_id,
                    tx.id,
                    tx.import_id,
                )
                continue
            confirmed.append((action, tx))
        for import_id in duplicate_ids:
            action = by_import_id.pop(import_id, None)
            if action is not None:
                duplicates.append(action)
                self._settle(action)
        for action in by_import_id.values():
            self._fail(action, "YNAB did not confirm this transaction; it will be retried")

        def effect(action: PlannedAction, tx: YnabTransaction) -> Callable[[], None]:
            def apply() -> None:
                if action.entry_exists:
                    self.ledger.update(
                        action.account_id,
                        action.entry_key,
                        **action.success_updates(tx.id, tx.import_id),
                    )
                else:
                    self.ledger.insert(action.success_entry(tx.id, tx.import_id))
                self._tombstone(action)

            return apply

        self._commit([(action, effect(action, tx)) for action, tx in confirmed])
        for action, tx in confirmed:
            ref = dataclasses.replace(
                action.after or TxRef(tx.date, tx.amount, tx.payee_name or ""), ynab_tx_id=tx.id
            )
            self.result.created.append(ref)
            self._audit("created", action, ref, f"import_id={tx.import_id}")
            self._settle(action)

    # ── 2b. duplicate import_ids ────────────────────────────────────────────

    def _resolve_duplicates(self, duplicates: list[PlannedAction]) -> list[PlannedAction]:
        """Link the transactions YNAB already had (§6.3 step 2) and return the UPDATE/DELETE
        follow-ups to run in this import."""
        if not duplicates:
            return []
        if self.stopped is not None:
            self.result.not_applied += len(duplicates)
            return []
        try:
            sync_budget(self.client, self.mirror, self.budget_id, now=self.now)
        except YnabRateLimited as exc:
            self._stop(exc)
            self.result.not_applied += len(duplicates)
            return []
        except YnabAuthError:
            raise
        except YnabError as exc:
            self.result.not_applied += len(duplicates)
            self.result.failures.append(Failure(action="sync", tx=None, error=_error_text(exc)))
            self.result.warnings.append(
                f"{len(duplicates)} transaction(s) already existed in YNAB but could not be "
                "checked (the YNAB sync failed); send the statement again to finish them."
            )
            return []
        view = self.mirror.view_for_account(self.budget_id, self.account_id)
        self.ledger.refresh_from_mirror(self.account_id, view)
        ctx = PlanContext.create(
            self.ledger.list_for_account(self.account_id),
            view,
            self.opts,
            reserved_import_ids=self.plan.reserved_import_ids,
            currency=self.currency,
            frame_offset=self.plan.frame_offset,
            stale=self.plan.stale_export,
        )
        follow_ups: list[PlannedAction] = []
        for action in duplicates:
            outcome = resolve_duplicate(action, view.by_import_id(action.import_id), self.opts, ctx)
            self.result.warnings.extend(outcome.warnings)
            local = [a for a in outcome.actions if not a.kind.writes_ynab]
            stored = self._commit([(a, lambda a=a: self._local_effect(a)) for a in local])
            for follow, ok in zip(local, stored, strict=True):
                if ok:
                    self.result.record_applied(follow)
            writes = [a for a in outcome.actions if a.kind.writes_ynab]
            if all(stored):
                follow_ups.extend(writes)
            elif writes:  # the link was not saved: do not write to YNAB for an unknown entry
                self.result.not_applied += len(writes)
            if outcome.ignored_deleted:
                self.result.ignored_deleted += 1
            elif outcome.unchanged:
                self.result.unchanged += 1
            log.info(
                "import %s: duplicate import_id %s resolved: %s",
                self.account_id,
                action.import_id,
                ", ".join(f"{a.kind.value}/{a.reason}" for a in outcome.actions) or "nothing",
            )
        return follow_ups

    # ── 3. updates ──────────────────────────────────────────────────────────

    def _update_phase(self, actions: Sequence[PlannedAction]) -> None:
        for chunk in _unique_batches(self._guard_account(actions)):
            if self.stopped is not None:
                self._not_applied(chunk)
                continue
            if self.blocked is not None:
                self._fail_all(chunk, self.blocked)
                continue
            try:
                self._patch(chunk)
            except _RateLimitStop as stop:
                self._stop(stop.error)
                self._not_applied(chunk)
            self._progress(PHASE_UPDATE)

    def _patch(self, chunk: list[PlannedAction]) -> None:
        try:
            for returned in self.client.iter_update_transactions(
                self.budget_id, [action.ynab_fields() for action in chunk]
            ):
                self._record_updates(chunk, returned)
        except YnabRateLimited as exc:
            raise _RateLimitStop(exc) from None
        except YnabAuthError:
            raise
        except YnabError as exc:
            if _is_client_error(exc):
                log.info(
                    "import %s: bulk update refused (%s); retrying one by one",
                    self.account_id,
                    exc.status,
                )
                self._put_each(chunk)
            else:
                for action in chunk:
                    if id(action) not in self.settled:
                        self._fail(action, _error_text(exc))
        except (TypeError, ValueError):  # refused by the client before sending
            self._put_each(chunk)

    def _put_each(self, chunk: list[PlannedAction]) -> None:
        for action in chunk:
            if id(action) in self.settled:
                continue
            fields = action.ynab_fields()
            tx_id = fields.pop("id")
            try:
                tx = self.client.update_transaction(self.budget_id, tx_id, fields)
            except YnabRateLimited as exc:
                raise _RateLimitStop(exc) from None
            except YnabAuthError:
                raise
            except YnabNotFound:
                self._mark_deleted_in_ynab(action)
            except YnabError as exc:
                self._fail(action, _error_text(exc))
            except (TypeError, ValueError) as exc:
                self._fail(action, f"invalid update: {exc}")
            else:
                self._record_updates([action], [tx])

    def _record_updates(
        self, chunk: Sequence[PlannedAction], returned: Sequence[YnabTransaction]
    ) -> None:
        by_id = {action.ynab_tx_id: action for action in chunk if id(action) not in self.settled}
        confirmed: list[tuple[PlannedAction, YnabTransaction]] = []
        for tx in returned:
            action = by_id.pop(tx.id, None)
            if action is not None:
                confirmed.append((action, tx))
        for action in by_id.values():
            self._fail(action, "YNAB did not confirm this change; it will be retried")

        updates = {id(action): self._confirmed_updates(action, tx) for action, tx in confirmed}

        def effect(action: PlannedAction) -> Callable[[], None]:
            def apply() -> None:
                self.ledger.update(action.account_id, action.entry_key, **updates[id(action)])
                self._tombstone(action)

            return apply

        self._commit([(action, effect(action)) for action, _tx in confirmed])
        for action, _tx in confirmed:
            change = action.to_change()
            self.result.updated.append(change)
            detail = ", ".join(
                self._describe_change(name, change.fields[name]) for name in change.fields
            )
            self._audit("updated", action, action.before, detail)
            self._settle(action)

    def _describe_change(self, name: str, values: tuple[Any, Any]) -> str:
        old, new = values
        if name == "amount" and isinstance(old, int) and isinstance(new, int):
            return f"amount {format_money(old, self.currency)}->{format_money(new, self.currency)}"
        if name == "memo":
            return "memo (pending marker removed)"
        return f"{name} {old}->{new}"

    def _confirmed_updates(self, action: PlannedAction, tx: YnabTransaction) -> dict[str, Any]:
        """The action's ledger updates, minus pushed_* values YNAB did not take (e.g. a date
        change YNAB ignores on a split): the ledger records only what YNAB confirmed."""
        updates = dict(action.ledger_updates)
        for name, column in _PUSHED_COLUMNS:
            if name not in action.fields:
                continue
            if getattr(tx, name) != action.fields[name]:
                updates.pop(column, None)
                self.result.warnings.append(
                    f"{self._describe(action.before)}: YNAB did not apply the {name} change "
                    f"(it shows {getattr(tx, name)!s})."
                )
        return updates

    def _mark_deleted_in_ynab(self, action: PlannedAction) -> None:
        """A PUT answered 404: the transaction is gone; never write to it again (F06)."""
        stored = self._commit(
            [
                (
                    action,
                    lambda: self.ledger.update(
                        action.account_id,
                        action.entry_key,
                        status=LedgerStatus.DELETED_IN_YNAB.value,
                    ),
                )
            ]
        )
        self.result.ignored_deleted += 1
        self.result.warnings.append(
            f"{self._describe(action.before)}: deleted in YNAB — it will not be re-created."
        )
        log.info(
            "import %s: %s was deleted in YNAB (404)%s",
            self.account_id,
            action.entry_key,
            "" if stored[0] else "; ledger not updated",
        )
        self._settle(action)

    # ── 4. deletes ──────────────────────────────────────────────────────────

    def _delete_phase(self, actions: Sequence[PlannedAction]) -> None:
        for action in self._guard_account(actions):
            if self.stopped is not None:
                self._not_applied([action])
                continue
            if self.blocked is not None:
                self._fail_all([action], self.blocked)
                continue
            assert action.ynab_tx_id  # guaranteed by PlannedAction
            detail = ""
            try:
                self.client.delete_transaction(self.budget_id, action.ynab_tx_id)
            except YnabRateLimited as exc:
                self._stop(exc)
                self._not_applied([action])
                continue
            except YnabAuthError:
                raise
            except YnabNotFound:
                detail = "(already gone: 404)"
            except YnabError as exc:
                self._fail(action, _error_text(exc))
                self._progress(PHASE_DELETE)
                continue

            def apply(action: PlannedAction = action) -> None:
                self.ledger.update(action.account_id, action.entry_key, **action.ledger_updates)
                self._tombstone(action)

            self._commit([(action, apply)])
            self.result.removed.append(action.to_removal())
            self._audit("deleted", action, action.before, detail)
            self._settle(action)
            self._progress(PHASE_DELETE)

    # ── guards ──────────────────────────────────────────────────────────────

    def _guard_account(self, actions: Sequence[PlannedAction]) -> list[PlannedAction]:
        """Defence in depth for F12/F39: never PATCH or DELETE a transaction the mirror
        places in another account."""
        allowed: list[PlannedAction] = []
        for action in actions:
            tx = self.mirror.get_transaction(action.ynab_tx_id) if action.ynab_tx_id else None
            if tx is not None and tx.account_id != self.account_id:
                self._fail(action, "the transaction belongs to another account in YNAB; left alone")
                continue
            allowed.append(action)
        return allowed
