"""Find and delete stray importer transactions (DESIGN §6.7; findings F13, F20, F44, F46).

**Scan** (:func:`dedupe_scan`): candidates are mirror transactions of the account that

* are not deleted and carry an import_id starting with ``YNAB:`` (the importer's format),
* are dated inside the window of the **parsed rows** of the newest statement — never the
  window in the file name, which a renamed file could stretch over years (F46); the newest
  statement must have been imported successfully, otherwise the ledger may not yet know
  transactions that belong to it,
* are not linked from any ACTIVE ledger entry (in any account) — a re-keyed or re-dated
  transaction keeps its old import_id in YNAB but is still linked (F13) — and do not carry
  the import_id of an UNLINKED entry of the account (still being resolved).

The scan is kept server-side per user (:class:`DedupeScans`, 10-minute TTL) under a random
``scan_id``; a new scan replaces the previous one.

**Delete** (:func:`dedupe_delete`): only ids of the user's current scan are accepted (at
most 200 per call, validated before anything is sent — F20/F44). Each id is re-checked
against the mirror and the ledger, then deleted one by one. A 404 counts as deleted. Ledger
entries that still point at a deleted transaction become ``DELETED_IN_YNAB`` so the
importer never recreates it. Per-id failures are reported; a 429 stops the run; an
authentication error is raised.
"""

from __future__ import annotations

import logging
import secrets
import sqlite3
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, Final

from ynab_helper.balance.reconcile import StatementUnavailable, load_imported_statement
from ynab_helper.errors import (
    StaleStateError,
    YnabAuthError,
    YnabError,
    YnabHelperError,
    YnabNotFound,
    YnabRateLimited,
)
from ynab_helper.money import format_money
from ynab_helper.storage.db import transaction
from ynab_helper.storage.ledger import LedgerEntry, LedgerStatus, LedgerStore
from ynab_helper.storage.mirror import MirrorStore, YnabTx
from ynab_helper.ynab.client import YnabClient

__all__ = [
    "DEDUPE_SCANS",
    "DEDUPE_SCAN_TTL",
    "IMPORTER_PREFIX",
    "MAX_DELETE_IDS",
    "DedupeCandidate",
    "DedupeDeleteResult",
    "DedupeScan",
    "DedupeScans",
    "DedupeSelectionError",
    "dedupe_delete",
    "dedupe_scan",
]

log = logging.getLogger(__name__)
audit = logging.getLogger("ynab_helper.audit")

#: Lifetime of a scan (seconds).
DEDUPE_SCAN_TTL: Final = 600.0
#: Most ids one delete request may carry.
MAX_DELETE_IDS: Final = 200
#: import_id prefix of transactions created by the importer (v1 and v2).
IMPORTER_PREFIX: Final = "YNAB:"
_MAX_USERS: Final = 1024
#: Ledger statuses whose link to a deleted transaction is turned into DELETED_IN_YNAB.
#: REVERTED/SUPERSEDED already mean "removed from YNAB"; REKEYED tombstones stay as they are.
_MARKABLE: Final = frozenset({LedgerStatus.ACTIVE.value, LedgerStatus.UNLINKED.value})


class DedupeSelectionError(YnabHelperError):
    """The selection passed to :func:`dedupe_delete` is malformed (empty, too long, …)."""


@dataclass(frozen=True, slots=True)
class DedupeCandidate:
    """A transaction offered for deletion (amount in milliunits)."""

    id: str
    date: str
    amount: int
    payee_name: str | None
    memo: str | None
    import_id: str
    cleared: str
    category_name: str | None

    @classmethod
    def of(cls, tx: YnabTx) -> DedupeCandidate:
        assert tx.import_id is not None
        return cls(
            id=tx.id,
            date=tx.date,
            amount=tx.amount,
            payee_name=tx.payee_name,
            memo=tx.memo,
            import_id=tx.import_id,
            cleared=tx.cleared,
            category_name=tx.category_name,
        )


@dataclass(frozen=True, slots=True)
class DedupeScan:
    """Result of :func:`dedupe_scan`; ``scan_id`` must accompany the delete request."""

    scan_id: str
    budget_id: str
    account_id: str
    csv_file_id: int
    csv_name: str
    window: tuple[str, str]
    candidates: list[DedupeCandidate]
    statement_rows: int  # parsed rows of the statement
    ynab_count_in_window: int  # live transactions of the account inside the window
    created_at: str  # UTC ISO
    ttl_seconds: float

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["window"] = list(self.window)
        return data


@dataclass(slots=True)
class DedupeDeleteResult:
    """Outcome of :func:`dedupe_delete`.

    ``deleted`` includes transactions that were already gone (404, or deleted per the
    mirror); their ids are also listed in ``already_deleted``. ``skipped`` were not sent
    because they no longer qualify (now linked to a statement row, moved, …).
    ``not_attempted`` were left because the rate limit stopped the run.
    """

    scan_id: str
    deleted: list[DedupeCandidate] = field(default_factory=list)
    already_deleted: list[str] = field(default_factory=list)
    skipped: list[tuple[DedupeCandidate, str]] = field(default_factory=list)
    failures: list[tuple[DedupeCandidate, str]] = field(default_factory=list)
    not_attempted: list[DedupeCandidate] = field(default_factory=list)
    rate_limited: bool = False
    ledger_marked: int = 0

    @property
    def ok(self) -> bool:
        return not self.failures and not self.not_attempted and not self.rate_limited

    def to_dict(self) -> dict[str, Any]:
        return {
            "scan_id": self.scan_id,
            "deleted": [asdict(c) for c in self.deleted],
            "already_deleted": list(self.already_deleted),
            "skipped": [{"candidate": asdict(c), "reason": r} for c, r in self.skipped],
            "failures": [{"candidate": asdict(c), "error": e} for c, e in self.failures],
            "not_attempted": [asdict(c) for c in self.not_attempted],
            "rate_limited": self.rate_limited,
            "ledger_marked": self.ledger_marked,
            "ok": self.ok,
        }


@dataclass(slots=True)
class _ScanState:
    scan_id: str
    budget_id: str
    account_id: str
    candidates: dict[str, DedupeCandidate]
    expires_at: float


class DedupeScans:
    """Thread-safe, in-memory store of each user's current scan (one per ``user_key``)."""

    def __init__(
        self,
        ttl: float = DEDUPE_SCAN_TTL,
        *,
        clock: Callable[[], float] = time.monotonic,
        max_users: int = _MAX_USERS,
    ) -> None:
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        self.ttl = float(ttl)
        self._clock = clock
        self._max = max(1, max_users)
        self._lock = threading.Lock()
        self._scans: dict[str, _ScanState] = {}

    def discard(self, user_key: str) -> None:
        """Forget the user's scan (e.g. on a new import or account change)."""
        with self._lock:
            self._scans.pop(user_key, None)

    def clear(self) -> None:
        with self._lock:
            self._scans.clear()

    def register(
        self,
        user_key: str,
        *,
        budget_id: str,
        account_id: str,
        candidates: list[DedupeCandidate],
    ) -> str:
        """Make ``candidates`` the user's current scan (replacing any other); returns its id."""
        scan_id = secrets.token_urlsafe(8)
        with self._lock:
            now = self._clock()
            self._purge(now)
            if user_key not in self._scans and len(self._scans) >= self._max:
                oldest = min(self._scans, key=lambda k: self._scans[k].expires_at)
                del self._scans[oldest]
            self._scans[user_key] = _ScanState(
                scan_id=scan_id,
                budget_id=budget_id,
                account_id=account_id,
                candidates={c.id: c for c in candidates},
                expires_at=now + self.ttl,
            )
        return scan_id

    def lookup(self, user_key: str, scan_id: str) -> _ScanState:
        """The user's current scan if ``scan_id`` matches and it has not expired.

        Raises:
            StaleStateError: no scan, another (older or newer) scan id, or expired.
        """
        with self._lock:
            now = self._clock()
            self._purge(now)
            state = self._scans.get(user_key)
            if state is None or not isinstance(scan_id, str) or state.scan_id != scan_id:
                raise StaleStateError(
                    "dedupe scan unknown, replaced or expired",
                    user_message="This duplicate list has expired. Run the scan again.",
                )
            return state

    def remove_ids(self, user_key: str, scan_id: str, ids: Sequence[str]) -> None:
        """Remove processed ids from the scan so they cannot be submitted again."""
        with self._lock:
            state = self._scans.get(user_key)
            if state is not None and state.scan_id == scan_id:
                for tx_id in ids:
                    state.candidates.pop(tx_id, None)

    def _purge(self, now: float) -> None:
        for key in [k for k, s in self._scans.items() if s.expires_at <= now]:
            del self._scans[key]


#: Process-wide registry shared by the bot and the web UI.
DEDUPE_SCANS = DedupeScans()


# ── scan ───────────────────────────────────────────────────────────────────


def _linked_active(ledger: LedgerStore, tx_id: str) -> bool:
    """True if any ACTIVE ledger entry (any account) links ``tx_id``."""
    return any(e.status == LedgerStatus.ACTIVE for e in ledger.by_ynab_tx_id(tx_id))


def _unlinked_import_ids(entries: list[LedgerEntry]) -> set[str]:
    ids: set[str] = set()
    for entry in entries:
        if entry.status == LedgerStatus.UNLINKED:
            ids.add(entry.key)
            if entry.ynab_import_id:
                ids.add(entry.ynab_import_id)
    return ids


def _disqualified(
    tx: YnabTx | None,
    *,
    account_id: str,
    ledger: LedgerStore,
    unlinked_ids: set[str],
) -> str | None:
    """Why ``tx`` is not (or no longer) a deletion candidate, or None if it is one."""
    if tx is None or tx.account_id != account_id:
        return "no longer in this account"
    if not tx.import_id or not tx.import_id.startswith(IMPORTER_PREFIX):
        return "not created by the importer"
    if tx.import_id in unlinked_ids:
        return "belongs to a statement row that is still being matched"
    if _linked_active(ledger, tx.id):
        return "belongs to a statement row"
    return None


def dedupe_scan(
    conn: sqlite3.Connection,
    *,
    budget_id: str,
    account_id: str,
    user_key: str,
    now: datetime | None = None,
    registry: DedupeScans | None = None,
) -> DedupeScan:
    """List deletion candidates for the account and remember them as the user's scan.

    ``conn``'s mirror must be fresh. Any earlier scan of ``user_key`` is discarded first,
    even if this one fails. ``now`` (UTC) only stamps ``created_at``.

    Raises:
        StatementUnavailable: no successfully imported statement (or it cannot be read).
    """
    scans = registry if registry is not None else DEDUPE_SCANS
    scans.discard(user_key)
    statement = load_imported_statement(conn, account_id, purpose="looking for duplicates")
    window = statement.export.window
    if window is None:
        raise StatementUnavailable(
            f"statement #{statement.record.id} has no importable rows",
            reason="no_rows",
            user_message=f"The newest statement ({statement.name}) has no transactions, so "
            "there is no period to check.",
        )
    start, end = window
    mirror = MirrorStore(conn)
    ledger = LedgerStore(conn)
    in_window = mirror.transactions_for_account(
        budget_id, account_id, since_date=start, until_date=end
    )
    unlinked_ids = _unlinked_import_ids(ledger.list_for_account(account_id))
    active_here = ledger.active_tx_ids(account_id)
    candidates = [
        DedupeCandidate.of(tx)
        for tx in in_window
        if tx.id not in active_here
        and _disqualified(tx, account_id=account_id, ledger=ledger, unlinked_ids=unlinked_ids)
        is None
    ]
    candidates.sort(key=lambda c: (c.date, (c.payee_name or "").lower(), c.id))
    scan_id = scans.register(
        user_key, budget_id=budget_id, account_id=account_id, candidates=candidates
    )
    created = (now or datetime.now(UTC)).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info(
        "dedupe scan %s: window %s..%s, %d of %d transactions are candidates",
        account_id,
        start,
        end,
        len(candidates),
        len(in_window),
    )
    return DedupeScan(
        scan_id=scan_id,
        budget_id=budget_id,
        account_id=account_id,
        csv_file_id=statement.record.id,
        csv_name=statement.name,
        window=(start, end),
        candidates=candidates,
        statement_rows=len(statement.export.rows),
        ynab_count_in_window=len(in_window),
        created_at=created,
        ttl_seconds=scans.ttl,
    )


# ── delete ─────────────────────────────────────────────────────────────────


def _selection(ids: Sequence[str]) -> list[str]:
    if isinstance(ids, str | bytes) or not isinstance(ids, Sequence):
        raise DedupeSelectionError(
            "ids must be a list of transaction ids", user_message="Invalid selection."
        )
    if len(ids) > MAX_DELETE_IDS:
        raise DedupeSelectionError(
            f"{len(ids)} ids exceed the limit of {MAX_DELETE_IDS}",
            user_message=f"Select at most {MAX_DELETE_IDS} transactions at a time.",
        )
    if not all(isinstance(i, str) and i for i in ids):
        raise DedupeSelectionError(
            "ids must be non-empty strings", user_message="Invalid selection."
        )
    unique = list(dict.fromkeys(ids))
    if not unique:
        raise DedupeSelectionError("empty selection", user_message="Nothing was selected.")
    return unique


def dedupe_delete(
    conn: sqlite3.Connection,
    client: YnabClient,
    *,
    budget_id: str,
    account_id: str,
    user_key: str,
    scan_id: str,
    ids: Sequence[str],
    registry: DedupeScans | None = None,
) -> DedupeDeleteResult:
    """Delete the selected candidates of the user's current scan, one by one.

    The whole request is refused (nothing deleted) if the scan is unknown, replaced or
    expired, belongs to another account, or any id is not part of it. Processed ids are
    removed from the scan, so the same scan can be used for further chunks.

    Raises:
        DedupeSelectionError: empty, malformed or more than 200 ids.
        StaleStateError: scan/account mismatch or ids outside the scan.
        YnabAuthError: YNAB rejected the token (deletions done so far stay recorded).
    """
    selection = _selection(ids)
    scans = registry if registry is not None else DEDUPE_SCANS
    state = scans.lookup(user_key, scan_id)
    if (state.budget_id, state.account_id) != (budget_id, account_id):
        raise StaleStateError(
            "dedupe scan belongs to another account",
            user_message="This duplicate list belongs to another account. Run the scan again.",
        )
    foreign = [tx_id for tx_id in selection if tx_id not in state.candidates]
    if foreign:
        raise StaleStateError(
            f"{len(foreign)} selected id(s) are not part of scan {scan_id}",
            user_message="The selection does not match the current duplicate list. "
            "Run the scan again.",
        )
    candidates = [state.candidates[tx_id] for tx_id in selection]

    mirror = MirrorStore(conn)
    ledger = LedgerStore(conn)
    unlinked_ids = _unlinked_import_ids(ledger.list_for_account(account_id))
    result = DedupeDeleteResult(scan_id=scan_id)
    processed: list[str] = []
    try:
        for position, candidate in enumerate(candidates):
            tx = mirror.get_transaction(candidate.id)
            if tx is not None and tx.deleted and tx.account_id == account_id:
                _record_deleted(conn, ledger, account_id, candidate, result, already=True)
                processed.append(candidate.id)
                continue
            reason = _disqualified(
                tx, account_id=account_id, ledger=ledger, unlinked_ids=unlinked_ids
            )
            if reason is not None:
                result.skipped.append((candidate, reason))
                processed.append(candidate.id)
                continue
            try:
                client.delete_transaction(budget_id, candidate.id)
            except YnabRateLimited:
                result.rate_limited = True
                result.not_attempted.extend(candidates[position:])
                break
            except YnabAuthError:
                raise
            except YnabNotFound:
                _record_deleted(conn, ledger, account_id, candidate, result, already=True)
                processed.append(candidate.id)
            except YnabError as exc:
                log.warning("dedupe: deleting %s failed: %s", candidate.id, exc)
                result.failures.append((candidate, exc.user_message))
            else:
                _record_deleted(conn, ledger, account_id, candidate, result, already=False)
                processed.append(candidate.id)
    finally:
        scans.remove_ids(user_key, scan_id, processed)
    log.info(
        "dedupe delete %s: %d deleted, %d skipped, %d failed, %d not attempted",
        account_id,
        len(result.deleted),
        len(result.skipped),
        len(result.failures),
        len(result.not_attempted),
    )
    return result


def _record_deleted(
    conn: sqlite3.Connection,
    ledger: LedgerStore,
    account_id: str,
    candidate: DedupeCandidate,
    result: DedupeDeleteResult,
    *,
    already: bool,
) -> None:
    """Book a confirmed deletion: result lists, audit line, ledger entries (one transaction)."""
    result.deleted.append(candidate)
    if already:
        result.already_deleted.append(candidate.id)
    else:
        audit.info(
            "deleted duplicate tx=%s date=%s amount=%s payee=%r import_id=%s",
            candidate.id,
            candidate.date,
            format_money(candidate.amount),
            candidate.payee_name,
            candidate.import_id,
        )
    # UNLINKED entries that only share the import_id are resolved by the next sync
    # (LedgerStore.refresh_from_mirror marks them DELETED_IN_YNAB).
    with transaction(conn):
        for entry in ledger.by_ynab_tx_id(candidate.id, account_id=account_id):
            if entry.status not in _MARKABLE:
                continue
            ledger.update(
                account_id,
                entry.key,
                status=LedgerStatus.DELETED_IN_YNAB.value,
                ynab_tx_id=candidate.id,
                last_error=None,
            )
            result.ledger_marked += 1
