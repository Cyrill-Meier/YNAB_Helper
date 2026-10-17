"""Reconcile YNAB's cleared balance to a Revolut statement's running balance (DESIGN §6.6).

Two steps, so nothing is posted without the user seeing the numbers (finding F04):

1. :func:`reconcile_preview` computes, from the newest **successfully imported** statement
   of the account and the (freshly synced) YNAB mirror::

       csv_balance        Balance of the COMPLETED tracked-product row with the latest
                          Completed Date (ties → last in file; F27), as of its date
       ynab_cleared_as_of cleared balance now − Σ cleared/reconciled transactions dated
                          after as_of (so newer activity does not distort the comparison)
       delta              csv_balance − ynab_cleared_as_of

   It returns the numbers, a list of ``problems`` worth showing to the user, and — if there
   is a difference to post — a one-time ``token`` valid for 10 minutes.
2. :func:`reconcile_apply` takes that token, recomputes everything and refuses with
   :class:`~ynab_helper.errors.StaleStateError` if anything changed (statement, balances,
   delta). Otherwise it posts a ``CSV Reconciliation`` adjustment, cleared ``reconciled``,
   dated as_of, import_id ``RECON:{delta}:{as_of}:1``.

The adjustment is a single transaction; it does not change any other transaction's state,
and user-facing text must not claim otherwise.

A statement that is older than what YNAB already reflects is refused, because subtracting
the newer cleared transactions also subtracts the *corrections* made after as_of and the
same difference would be corrected twice (F04): YNAB holds a reconciled transaction (or a
reconciliation adjustment) dated after as_of — a reconciliation in the YNAB app or with a
newer statement — or the ledger holds imported rows newer than the statement (a backfill,
same rule as the importer's older-export check).

Refusals (no statement, statement not imported, file missing or changed, no balance, wrong
currency, unknown account, superseded statement) raise :class:`StatementUnavailable` /
:class:`ReconcileUnavailable` with a ``user_message`` and a machine-readable ``reason``.
"""

from __future__ import annotations

import logging
import secrets
import sqlite3
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, replace
from datetime import date as Date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Final

from ynab_helper.balance.adjust import (
    IN_SYNC_THRESHOLD_MILLI,
    PAYEE_RECON,
    PREFIX_RECON,
    AdjustmentResult,
    local_day,
    post_balance_adjustment,
)
from ynab_helper.errors import CsvError, StaleStateError, YnabHelperError
from ynab_helper.money import format_money
from ynab_helper.revolut.parser import ParsedExport, RevolutRow, parse_export
from ynab_helper.storage.csv_files import CsvFile, CsvFileStore
from ynab_helper.storage.ledger import CsvState, LedgerStatus, LedgerStore
from ynab_helper.storage.mirror import CLEARED_LIKE, RECONCILED, UNCLEARED, MirrorStore, YnabTx
from ynab_helper.ynab.client import YnabClient

__all__ = [
    "RECONCILE_TOKENS",
    "RECONCILE_TOKEN_TTL",
    "STALE_STATEMENT_DAYS",
    "ImportedStatement",
    "ReconcilePreview",
    "ReconcileResult",
    "ReconcileTokens",
    "ReconcileUnavailable",
    "StatementBalance",
    "StatementUnavailable",
    "latest_completed_balance",
    "load_imported_statement",
    "reconcile_apply",
    "reconcile_preview",
]

log = logging.getLogger(__name__)

#: Lifetime of a preview's confirmation token (seconds).
RECONCILE_TOKEN_TTL: Final = 600.0
#: A statement whose newest completed row is older than this is flagged in ``problems``.
STALE_STATEMENT_DAYS: Final = 7
_MAX_TOKENS: Final = 256
#: Revolut re-renders dates across timezones; the slack of the importer's older-export rule
#: (DESIGN §6.2, ``allow_sweep``).
_OLDER_STATEMENT_SLACK: Final = timedelta(days=1)
#: Payees of balance corrections: the bot's own and the one YNAB's in-app reconcile creates.
_CORRECTION_PAYEES: Final = (PAYEE_RECON.casefold(), "reconciliation balance adjustment")


# ── statement loading (shared with maintenance.dedupe) ─────────────────────


class StatementUnavailable(YnabHelperError):
    """The statement needed for an operation is missing, not imported or unusable.

    ``reason`` is one of ``no_statement``, ``not_imported``, ``missing_file``,
    ``unreadable``, ``file_changed``, ``no_rows``, ``no_completed_rows``,
    ``undated_completed_row``, ``no_balance``.
    """

    def __init__(self, message: str, *, reason: str, user_message: str | None = None) -> None:
        super().__init__(message, user_message=user_message)
        self.reason = reason


class ReconcileUnavailable(YnabHelperError):
    """Reconciliation cannot be computed for a reason outside the statement.

    ``reason`` is one of ``currency_unknown``, ``currency_mismatch``, ``account_unknown``,
    ``balance_unknown``, ``older_statement`` (the ledger holds imported rows newer than the
    statement) or ``reconciled_after_as_of`` (YNAB was already reconciled after the
    statement's date).
    """

    def __init__(self, message: str, *, reason: str, user_message: str | None = None) -> None:
        super().__init__(message, user_message=user_message)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class ImportedStatement:
    """The newest statement of an account, verified as imported and re-parsed from disk."""

    record: CsvFile
    export: ParsedExport

    @property
    def name(self) -> str:
        """Display name: the original upload name, else the stored file name."""
        return self.record.original_name or Path(self.record.stored_path).name


_STATUS_HINTS: Final = {
    "failed": "could not be imported. Import a statement successfully",
    "partial": "was only partially imported. Send it again to finish the import",
    "stored": "has not been imported yet. Import it",
    "legacy": "comes from the previous version of the bot and its import status is unknown. "
    "Upload the latest statement again",
}


def load_imported_statement(
    conn: sqlite3.Connection, account_id: str, *, purpose: str = "reconciling"
) -> ImportedStatement:
    """Return the newest ``csv_files`` statement of ``account_id``, re-parsed from disk.

    The newest statement must have status ``imported``: a failed, partial, merely stored or
    legacy (v1) statement is refused rather than silently skipped, because the ledger and
    YNAB only reflect a statement once its import completed (F04). The file is re-read from
    ``stored_path`` and must still hash to the recorded sha256.

    ``purpose`` completes the user message ("… before reconciling").

    Raises:
        StatementUnavailable: see its ``reason`` codes.
    """
    record = CsvFileStore(conn).latest(account_id)
    if record is None:
        raise StatementUnavailable(
            f"no statement recorded for account {account_id}",
            reason="no_statement",
            user_message="No Revolut statement has been imported for this account yet. "
            "Upload a statement first.",
        )
    name = record.original_name or Path(record.stored_path).name
    if record.status != "imported":
        hint = _STATUS_HINTS.get(record.status, "is not usable. Import a statement successfully")
        raise StatementUnavailable(
            f"newest statement #{record.id} has status {record.status!r}",
            reason="not_imported",
            user_message=f"The newest statement ({name}) {hint} before {purpose}.",
        )
    path = Path(record.stored_path)
    if not path.is_file():
        raise StatementUnavailable(
            f"statement #{record.id} file is missing",
            reason="missing_file",
            user_message=f"The statement file ({name}) is no longer stored on the server. "
            f"Upload the statement again before {purpose}.",
        )
    try:
        export = parse_export(path, filename=record.original_name)
    except CsvError as exc:
        raise StatementUnavailable(
            f"statement #{record.id} cannot be parsed: {exc}",
            reason="unreadable",
            user_message=f"The stored statement ({name}) can no longer be read: "
            f"{exc.user_message} Upload it again before {purpose}.",
        ) from exc
    if export.sha256 != record.sha256:
        raise StatementUnavailable(
            f"statement #{record.id} changed on disk since it was imported",
            reason="file_changed",
            user_message=f"The stored statement ({name}) changed since it was imported. "
            f"Upload it again before {purpose}.",
        )
    return ImportedStatement(record=record, export=export)


@dataclass(frozen=True, slots=True)
class StatementBalance:
    """The running balance of a statement and the row it comes from."""

    balance_milli: int
    as_of: str  # YYYY-MM-DD of the row's Completed Date
    completed_at: datetime
    line_no: int


def latest_completed_balance(export: ParsedExport) -> StatementBalance:
    """Balance of the COMPLETED row with the latest Completed Date (ties → last in file).

    Rows are already restricted to the tracked product, so a closed Savings pocket's 0.00
    row can never be chosen. Revolut orders rows by Completed Date, but a card payment that
    started earlier may complete later, so the file position is only a tie-breaker (F27).

    Raises:
        StatementUnavailable: no completed row, a completed row without a readable
            Completed Date (the latest balance would be ambiguous), or the latest row has
            no Balance.
    """
    completed = [(index, row) for index, row in enumerate(export.rows) if row.state == "COMPLETED"]
    if not completed:
        raise StatementUnavailable(
            "statement has no completed rows",
            reason="no_completed_rows",
            user_message="The statement has no completed transactions, so it has no balance "
            "to reconcile to.",
        )
    undated = [row for _, row in completed if row.completed_at is None]
    if undated:
        raise StatementUnavailable(
            f"data row {undated[0].line_no} is COMPLETED without a readable Completed Date",
            reason="undated_completed_row",
            user_message=f"Data row {undated[0].line_no} of the statement is completed but "
            "has no readable Completed Date, so the latest balance cannot be determined.",
        )

    def order(item: tuple[int, RevolutRow]) -> tuple[datetime, int]:
        index, row = item
        assert row.completed_at is not None
        return (row.completed_at, index)

    _, latest = max(completed, key=order)
    assert latest.completed_at is not None
    as_of = latest.completed_at.date().isoformat()
    if latest.balance is None:
        raise StatementUnavailable(
            f"latest completed row (data row {latest.line_no}) has no Balance",
            reason="no_balance",
            user_message=f"The newest completed transaction of the statement (data row "
            f"{latest.line_no}, {as_of}) has no Balance value, so there is nothing to "
            "reconcile to.",
        )
    return StatementBalance(
        balance_milli=latest.balance,
        as_of=as_of,
        completed_at=latest.completed_at,
        line_no=latest.line_no,
    )


# ── preview / result types ─────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class ReconcilePreview:
    """Numbers for the user to confirm (amounts in milliunits of ``currency``).

    ``token`` is None when YNAB already matches (``in_sync``); otherwise pass it to
    :func:`reconcile_apply` within :data:`RECONCILE_TOKEN_TTL` seconds.
    """

    budget_id: str
    account_id: str
    csv_file_id: int
    csv_name: str
    csv_sha256: str
    as_of: str
    csv_balance: int
    ynab_cleared_as_of: int
    ynab_cleared_now: int
    cleared_after_as_of: int
    delta: int
    currency: str
    problems: list[str] = field(default_factory=list)
    token: str | None = None
    token_ttl_seconds: float | None = None

    @property
    def in_sync(self) -> bool:
        return abs(self.delta) < IN_SYNC_THRESHOLD_MILLI

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe representation (adds ``in_sync``)."""
        data = asdict(self)
        data["in_sync"] = self.in_sync
        return data


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    """Outcome of :func:`reconcile_apply`: the recomputed preview and the adjustment."""

    preview: ReconcilePreview
    adjustment: AdjustmentResult

    @property
    def outcome(self) -> str:
        return self.adjustment.outcome

    def to_dict(self) -> dict[str, Any]:
        return {"preview": self.preview.to_dict(), "adjustment": self.adjustment.to_dict()}


@dataclass(frozen=True, slots=True)
class _Binding:
    """Everything a token confirms; apply refuses if any of it differs on recomputation."""

    budget_id: str
    account_id: str
    csv_file_id: int
    csv_sha256: str
    as_of: str
    csv_balance: int
    ynab_cleared_as_of: int
    delta: int
    currency: str

    @classmethod
    def of(cls, preview: ReconcilePreview) -> _Binding:
        return cls(
            budget_id=preview.budget_id,
            account_id=preview.account_id,
            csv_file_id=preview.csv_file_id,
            csv_sha256=preview.csv_sha256,
            as_of=preview.as_of,
            csv_balance=preview.csv_balance,
            ynab_cleared_as_of=preview.ynab_cleared_as_of,
            delta=preview.delta,
            currency=preview.currency,
        )


class ReconcileTokens:
    """In-memory, thread-safe registry of one-time reconcile confirmation tokens.

    A token is valid for ``ttl`` seconds and can be consumed once. Issuing a token for an
    account revokes that account's earlier tokens (an old confirmation button then reports
    "expired" instead of confirming numbers the user no longer sees).
    """

    def __init__(
        self,
        ttl: float = RECONCILE_TOKEN_TTL,
        *,
        clock: Callable[[], float] = time.monotonic,
        max_entries: int = _MAX_TOKENS,
    ) -> None:
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        self.ttl = float(ttl)
        self._clock = clock
        self._max = max(1, max_entries)
        self._lock = threading.Lock()
        self._tokens: OrderedDict[str, tuple[_Binding, float]] = OrderedDict()

    def __len__(self) -> int:
        with self._lock:
            self._purge(self._clock())
            return len(self._tokens)

    def issue(self, binding: _Binding) -> str:
        token = secrets.token_urlsafe(16)
        with self._lock:
            now = self._clock()
            self._purge(now)
            for other, (bound, _) in list(self._tokens.items()):
                if (bound.budget_id, bound.account_id) == (binding.budget_id, binding.account_id):
                    del self._tokens[other]
            while len(self._tokens) >= self._max:
                self._tokens.popitem(last=False)
            self._tokens[token] = (binding, now + self.ttl)
        return token

    def consume(self, token: str) -> _Binding:
        """Remove and return the binding of ``token``.

        Raises:
            StaleStateError: unknown, already used or expired token.
        """
        with self._lock:
            now = self._clock()
            entry = self._tokens.pop(token, None) if isinstance(token, str) else None
            self._purge(now)
        if entry is None or entry[1] <= now:
            raise StaleStateError(
                "reconcile token unknown, used or expired",
                user_message="This reconciliation preview has expired. Run the preview again.",
            )
        return entry[0]

    def clear(self) -> None:
        with self._lock:
            self._tokens.clear()

    def _purge(self, now: float) -> None:
        for token in [t for t, (_, expires) in self._tokens.items() if expires <= now]:
            del self._tokens[token]


#: Process-wide registry shared by the bot and the web UI.
RECONCILE_TOKENS = ReconcileTokens()


# ── computation ────────────────────────────────────────────────────────────


def _compute(
    conn: sqlite3.Connection,
    *,
    budget_id: str,
    account_id: str,
    budget_currency: str | None,
    today: Date,
) -> ReconcilePreview:
    """The preview numbers and problems (no token, no network)."""
    statement = load_imported_statement(conn, account_id, purpose="reconciling")
    export = statement.export
    currency = export.currency
    if not budget_currency:
        raise ReconcileUnavailable(
            "budget currency unknown",
            reason="currency_unknown",
            user_message="The YNAB budget's currency is unknown. Sync with YNAB and try again.",
        )
    if not currency:
        raise ReconcileUnavailable(
            "statement has no currency",
            reason="currency_unknown",
            user_message="The statement does not say which currency it is in, so it cannot "
            "be compared with the budget.",
        )
    if currency.upper() != budget_currency.upper():
        raise ReconcileUnavailable(
            f"statement currency {currency} != budget currency {budget_currency}",
            reason="currency_mismatch",
            user_message=f"The statement is in {currency} but the YNAB budget is in "
            f"{budget_currency}; they cannot be reconciled.",
        )
    balance = latest_completed_balance(export)

    mirror = MirrorStore(conn)
    account = mirror.account(account_id)
    if account is None or account.deleted or account.budget_id not in (None, budget_id):
        raise ReconcileUnavailable(
            f"account {account_id} not in the mirror of budget {budget_id}",
            reason="account_unknown",
            user_message="The YNAB account is unknown or was deleted. Sync with YNAB or "
            "check your account settings.",
        )
    if account.cleared_balance is None:
        raise ReconcileUnavailable(
            f"account {account_id} has no cleared balance in the mirror",
            reason="balance_unknown",
            user_message="YNAB's cleared balance for the account is unknown. Sync with YNAB "
            "and try again.",
        )
    ledger = LedgerStore(conn)
    txs = mirror.transactions_for_account(budget_id, account_id)
    window_end = export.window[1] if export.window else balance.as_of
    _refuse_superseded(
        txs,
        ledger,
        account_id=account_id,
        as_of=balance.as_of,
        statement_end=max(window_end, balance.as_of),
        name=statement.name,
    )
    cleared_now = account.cleared_balance
    after = mirror.cleared_sum_after(account_id, balance.as_of)
    cleared_as_of = cleared_now - after
    delta = balance.balance_milli - cleared_as_of
    problems = _problems(
        txs,
        ledger,
        account_id=account_id,
        as_of=balance.as_of,
        currency=currency,
        closed=account.closed,
        today=today,
    )
    return ReconcilePreview(
        budget_id=budget_id,
        account_id=account_id,
        csv_file_id=statement.record.id,
        csv_name=statement.name,
        csv_sha256=statement.record.sha256,
        as_of=balance.as_of,
        csv_balance=balance.balance_milli,
        ynab_cleared_as_of=cleared_as_of,
        ynab_cleared_now=cleared_now,
        cleared_after_as_of=after,
        delta=delta,
        currency=currency,
        problems=problems,
    )


def _is_correction(tx: YnabTx) -> bool:
    """A reconciled transaction or a balance correction (bot RECON or YNAB in-app adjustment)."""
    if tx.cleared == RECONCILED:
        return True
    if (tx.import_id or "").startswith(f"{PREFIX_RECON}:"):
        return True
    payee = (tx.payee_name or "").strip().casefold()
    return any(payee.startswith(name) for name in _CORRECTION_PAYEES)


def _refuse_superseded(
    txs: list[YnabTx],
    ledger: LedgerStore,
    *,
    account_id: str,
    as_of: str,
    statement_end: str,
    name: str,
) -> None:
    """Refuse a statement older than what YNAB already reflects (F04 backfill).

    ``ynab_cleared_as_of`` leaves out everything dated after as_of — including corrections
    made after as_of. Reconciling to an older statement would then post the same correction
    a second time, now "reconciled" and dated in the past. Refused when:

    * the ledger holds ACTIVE imported rows dated more than a day after the statement's
      last row (``statement_end``): a newer statement was imported, so this one is a
      backfill (the importer's older-export rule);
    * YNAB holds a reconciled transaction or a balance correction dated after as_of: the
      balance was already confirmed after the statement's date (in the YNAB app, or by
      reconciling a newer statement).

    Raises:
        ReconcileUnavailable: ``older_statement`` / ``reconciled_after_as_of``.
    """
    newest_imported = ledger.newest_active_date(account_id)
    if newest_imported:
        try:
            limit = (Date.fromisoformat(newest_imported) - _OLDER_STATEMENT_SLACK).isoformat()
        except ValueError:
            log.warning("ledger of %s has an unreadable csv_date %r", account_id, newest_imported)
        else:
            if statement_end < limit:
                raise ReconcileUnavailable(
                    f"statement ends {statement_end} but the ledger has rows up to "
                    f"{newest_imported}",
                    reason="older_statement",
                    user_message=f"This statement ({name}) ends on {statement_end}, but "
                    f"transactions up to {newest_imported} were already imported from a newer "
                    "statement. Reconciling to the older balance would undo or repeat "
                    "corrections made since. Upload the newest statement again and "
                    "reconcile with that one.",
                )
    later = [t for t in txs if t.date > as_of and _is_correction(t)]
    if later:
        newest = max(t.date for t in later)
        raise ReconcileUnavailable(
            f"{len(later)} reconciled/correction transaction(s) dated after as_of {as_of} "
            f"(newest {newest})",
            reason="reconciled_after_as_of",
            user_message=f"YNAB already has {len(later)} reconciled transaction(s) or balance "
            f"adjustment(s) dated after this statement's last completed transaction "
            f"({as_of}), the newest on {newest}. The balance was already reconciled after "
            f"{as_of} (in YNAB or with a newer statement), so reconciling to this statement "
            "would apply the same correction a second time. Upload a statement that covers "
            f"{newest} and reconcile with that one.",
        )


def _problems(
    txs: list[YnabTx],
    ledger: LedgerStore,
    *,
    account_id: str,
    as_of: str,
    currency: str,
    closed: bool,
    today: Date,
) -> list[str]:
    """Warnings that may make the comparison misleading (shown with the preview)."""
    problems: list[str] = []
    entries = ledger.list_for_account(account_id)
    active = {e.ynab_tx_id: e for e in entries if e.status == LedgerStatus.ACTIVE and e.ynab_tx_id}

    newer = [t for t in txs if t.date > as_of and t.cleared in CLEARED_LIKE]
    if newer:
        problems.append(
            f"{len(newer)} cleared transaction(s) dated after {as_of} "
            f"({format_money(sum(t.amount for t in newer), currency, sign=True)}) are newer "
            "than the statement and were left out of the comparison. If one of them already "
            "corrects the difference shown here, the adjustment would correct it a second "
            "time; check them before confirming."
        )

    # Uncleared on or before as_of that are not Revolut pendings: if Revolut already booked
    # them, the statement balance includes them while YNAB's cleared balance does not.
    unknown_uncleared = [
        t
        for t in txs
        if t.date <= as_of
        and t.cleared == UNCLEARED
        and not (t.id in active and active[t.id].csv_state == CsvState.PENDING)
    ]
    if unknown_uncleared:
        total = format_money(sum(t.amount for t in unknown_uncleared), currency, sign=True)
        problems.append(
            f"{len(unknown_uncleared)} uncleared transaction(s) dated on or before {as_of} "
            f"({total}) are not in YNAB's cleared balance. If they already went through at "
            "Revolut, clear them in YNAB first, otherwise the adjustment counts them twice."
        )

    # Still pending at Revolut (not in the statement balance) but cleared in YNAB.
    by_id = {t.id: t for t in txs}
    pending_cleared = [
        by_id[tx_id]
        for tx_id, entry in active.items()
        if entry.csv_state == CsvState.PENDING
        and tx_id in by_id
        and by_id[tx_id].cleared in CLEARED_LIKE
        and by_id[tx_id].date <= as_of
    ]
    if pending_cleared:
        total = format_money(sum(t.amount for t in pending_cleared), currency, sign=True)
        problems.append(
            f"{len(pending_cleared)} transaction(s) still pending at Revolut are cleared in "
            f"YNAB ({total}); the statement balance does not include them yet."
        )

    age = (today - Date.fromisoformat(as_of)).days
    if age > STALE_STATEMENT_DAYS:
        problems.append(
            f"The statement's newest completed transaction is from {as_of} ({age} days ago). "
            "Upload a newer statement for an up-to-date reconciliation."
        )
    if closed:
        problems.append("The YNAB account is closed.")
    return problems


def _verify_mirror_current(
    client: YnabClient, budget_id: str, account_id: str, mirror_cleared: int
) -> None:
    """One GET: the mirror's cleared balance must equal what YNAB reports right now."""
    live = client.get_account(budget_id, account_id)
    if live.deleted or live.cleared_balance != mirror_cleared:
        raise StaleStateError(
            f"mirror cleared balance {mirror_cleared} != YNAB {live.cleared_balance}",
            user_message="YNAB changed while the reconciliation was being prepared. "
            "Run the preview again.",
        )


def _today(now: datetime | Date | None) -> Date:
    return local_day(now)


def reconcile_preview(
    conn: sqlite3.Connection,
    client: YnabClient | None,
    *,
    budget_id: str,
    account_id: str,
    budget_currency: str | None,
    now: datetime | Date | None = None,
    tokens: ReconcileTokens | None = None,
) -> ReconcilePreview:
    """Compute the reconciliation for the account and issue a confirmation token.

    ``conn``'s mirror must be fresh (synced by the caller). With a ``client`` the mirror's
    cleared balance is cross-checked against YNAB (one GET). ``now`` (local time) only
    affects the statement-age warning. Nothing is written to YNAB.

    Raises:
        StatementUnavailable, ReconcileUnavailable: reconciliation is not possible
            (including a statement older than what YNAB already reflects).
        StaleStateError: the mirror does not match YNAB (sync and retry).
    """
    preview = _compute(
        conn,
        budget_id=budget_id,
        account_id=account_id,
        budget_currency=budget_currency,
        today=_today(now),
    )
    if client is not None:
        _verify_mirror_current(client, budget_id, account_id, preview.ynab_cleared_now)
    log.info(
        "reconcile preview account=%s as_of=%s csv=%d ynab_as_of=%d delta=%d problems=%d",
        account_id,
        preview.as_of,
        preview.csv_balance,
        preview.ynab_cleared_as_of,
        preview.delta,
        len(preview.problems),
    )
    if preview.in_sync:
        return preview
    registry = tokens if tokens is not None else RECONCILE_TOKENS
    token = registry.issue(_Binding.of(preview))
    return replace(preview, token=token, token_ttl_seconds=registry.ttl)


def reconcile_apply(
    conn: sqlite3.Connection,
    client: YnabClient,
    *,
    budget_id: str,
    account_id: str,
    token: str,
    budget_currency: str | None,
    now: datetime | Date | None = None,
    tokens: ReconcileTokens | None = None,
) -> ReconcileResult:
    """Post the reconciliation confirmed by ``token`` (one-time use).

    Recomputes the preview from ``conn`` (whose mirror the caller just synced), checks the
    mirror against YNAB, and refuses unless statement, balances and delta are exactly what
    the token confirmed.

    Raises:
        StaleStateError: unknown/used/expired token, another account, or anything changed.
        StatementUnavailable, ReconcileUnavailable: reconciliation is no longer possible.
        YnabError: the POST failed (nothing was recorded locally; YNAB dedupes a retry).
    """
    registry = tokens if tokens is not None else RECONCILE_TOKENS
    binding = registry.consume(token)
    if (binding.budget_id, binding.account_id) != (budget_id, account_id):
        raise StaleStateError(
            "reconcile token belongs to another account",
            user_message="This reconciliation preview belongs to another account. "
            "Run the preview again.",
        )
    preview = _compute(
        conn,
        budget_id=budget_id,
        account_id=account_id,
        budget_currency=budget_currency,
        today=_today(now),
    )
    if _Binding.of(preview) != binding:
        log.info("reconcile apply refused: state changed since the preview (%s)", account_id)
        raise StaleStateError(
            "reconcile state changed since the preview",
            user_message="The statement or YNAB changed since the preview, so nothing was "
            "posted. Review the new numbers and confirm again.",
        )
    _verify_mirror_current(client, budget_id, account_id, preview.ynab_cleared_now)
    memo = (
        f"Reconciled to CSV running balance {format_money(preview.csv_balance)} "
        f"{preview.currency} as of {preview.as_of}"
    )
    adjustment = post_balance_adjustment(
        client,
        budget_id,
        account_id,
        target_milli=preview.csv_balance,
        current_milli=preview.ynab_cleared_as_of,
        payee=PAYEE_RECON,
        memo=memo,
        prefix=PREFIX_RECON,
        date=preview.as_of,
        cleared="reconciled",
        dry_run=False,
    )
    log.info(
        "reconcile apply account=%s as_of=%s delta=%d outcome=%s",
        account_id,
        preview.as_of,
        preview.delta,
        adjustment.outcome,
    )
    return ReconcileResult(preview=preview, adjustment=adjustment)
