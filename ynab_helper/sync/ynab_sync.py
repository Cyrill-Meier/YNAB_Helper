"""YNAB → mirror sync (docs/DESIGN.md §6.5).

:func:`sync_budget` pulls the budget's accounts and its budget-wide transactions from YNAB
into the local mirror (:class:`~ynab_helper.storage.mirror.MirrorStore`):

* ``GET /budgets/{b}/accounts?last_knowledge_of_server=K`` → ``accounts`` (classified, with
  ``budget_id``);
* ``GET /budgets/{b}/transactions?last_knowledge_of_server=K`` → ``ynab_transactions`` with
  the ``deleted`` flag, subtransactions replaced per parent. One knowledge slot per budget.

Each delta cursor is stored **in the same SQLite transaction** as the data it covers, so the
cursor never advances past rows that were not saved. The very first sync of a budget (no
stored cursor), and a ``full=True`` sync, ask for ``last_knowledge_of_server=0``: YNAB omits
deleted transactions from a listing without the parameter, and the importer must see them —
a user-deleted transaction must become DELETED_IN_YNAB, never be re-created, and its
import_id stays taken (§6.2, §6.4). Because that listing is complete, rows of the budget the
mirror holds but YNAB no longer lists are marked deleted.

The budget currency (``GET /budgets/{b}/settings``) is refreshed at most once a day; failing
to refresh it never fails a sync. HTTP calls happen outside every database transaction.

:func:`sync_and_refresh` is what the services call: it throttles non-forced syncs through
``last_sync_at:<budget>`` (default 30 s), turns a 429 into a result flagged ``rate_limited``
(so read paths degrade to cached data instead of failing), and after a successful sync runs
``LedgerStore.refresh_from_mirror`` for the account (§5.3).

Rate-limit backoff (F17): a 429 — on a sync here or on a write in the executor — is
remembered in ``rate_limited_until:<budget>`` (YNAB's ``Retry-After``, else
:data:`DEFAULT_RATE_LIMIT_BACKOFF`, at most :data:`MAX_RATE_LIMIT_BACKOFF`). Until then a
non-forced sync answers ``rate_limited`` from the cache without sending a request, so /status
and dashboard refreshes cannot keep a token's quota exhausted. Forced syncs (an import, an
explicit /sync) still try, and any successful sync clears the mark.
"""

from __future__ import annotations

import dataclasses
import logging
import math
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from ynab_helper.errors import YnabAuthError, YnabError, YnabRateLimited
from ynab_helper.storage.ledger import LedgerStore, RefreshResult
from ynab_helper.storage.mirror import META_LAST_SYNC_AT, MirrorStore
from ynab_helper.timeutil import utc_now
from ynab_helper.ynab.client import YnabClient

__all__ = [
    "CURRENCY_MAX_AGE",
    "DEFAULT_MIN_INTERVAL",
    "DEFAULT_RATE_LIMIT_BACKOFF",
    "MAX_RATE_LIMIT_BACKOFF",
    "SyncResult",
    "format_sync_time",
    "mark_mirror_stale",
    "note_rate_limited",
    "parse_sync_time",
    "rate_limited_until",
    "sync_and_refresh",
    "sync_budget",
]

log = logging.getLogger(__name__)

#: A non-forced sync is skipped when the previous one finished less than this many seconds ago.
DEFAULT_MIN_INTERVAL = 30.0
#: The budget currency is fetched again when the stored value is older than this.
CURRENCY_MAX_AGE = timedelta(days=1)
#: Seconds non-forced syncs stay quiet after a 429 that carried no ``Retry-After``.
DEFAULT_RATE_LIMIT_BACKOFF = 60.0
#: Upper bound for a remembered backoff (a bogus ``Retry-After`` cannot silence syncs longer).
MAX_RATE_LIMIT_BACKOFF = 3600.0
#: ``sync_meta`` key of the rate-limit backoff (UTC time, same format as ``last_sync_at``).
META_RATE_LIMITED_UNTIL = "rate_limited_until:{budget_id}"
#: Format of ``last_sync_at`` / ``budget_currency_checked_at`` (same as ``utc_now_iso``).
_META_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass(frozen=True, slots=True)
class SyncResult:
    """Outcome of :func:`sync_budget` / :func:`sync_and_refresh`.

    * ``accounts_changed`` / ``tx_changed`` — objects YNAB returned (and the mirror stored).
    * ``server_knowledge`` / ``accounts_knowledge`` — the cursors after the sync.
    * ``full`` — the transactions were fetched from knowledge 0 (first sync or forced).
    * ``currency`` — the budget currency known after the sync; ``currency_refreshed`` if it
      was fetched in this sync.
    * ``skipped`` — throttled: nothing was requested (``synced_at`` is the previous sync).
    * ``rate_limited`` — YNAB answered 429; the mirror may be partly updated (every stored
      part is consistent with its cursor) and the ledger was not refreshed.
    * ``refresh`` — what ``refresh_from_mirror`` changed (:func:`sync_and_refresh` only).
    """

    budget_id: str
    accounts_changed: int = 0
    tx_changed: int = 0
    server_knowledge: int | None = None
    accounts_knowledge: int | None = None
    full: bool = False
    currency: str | None = None
    currency_refreshed: bool = False
    synced_at: str | None = None
    skipped: bool = False
    rate_limited: bool = False
    retry_after: float | None = None
    refresh: RefreshResult | None = None

    @property
    def ok(self) -> bool:
        """True if YNAB data was fetched and stored (not throttled, not rate limited)."""
        return not self.skipped and not self.rate_limited

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe representation (web API)."""
        refresh = None
        if self.refresh is not None:
            refresh = {
                "import_ids_corrected": self.refresh.import_ids_corrected,
                "linked": self.refresh.linked,
                "marked_deleted": self.refresh.marked_deleted,
                "conflicts": list(self.refresh.conflicts),
            }
        return {
            "budget_id": self.budget_id,
            "accounts_changed": self.accounts_changed,
            "tx_changed": self.tx_changed,
            "server_knowledge": self.server_knowledge,
            "accounts_knowledge": self.accounts_knowledge,
            "full": self.full,
            "currency": self.currency,
            "currency_refreshed": self.currency_refreshed,
            "synced_at": self.synced_at,
            "skipped": self.skipped,
            "rate_limited": self.rate_limited,
            "retry_after": self.retry_after,
            "refresh": refresh,
            "ok": self.ok,
        }


# ── time helpers ─────────────────────────────────────────────────────────────


def _utc(now: datetime | None) -> datetime:
    if now is None:
        return utc_now()
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware (UTC)")
    return now.astimezone(UTC)


def format_sync_time(when: datetime) -> str:
    """``2026-10-16T18:30:00Z`` — the format stored in ``sync_meta``."""
    return _utc(when).strftime(_META_TIME_FORMAT)


def parse_sync_time(text: str | None) -> datetime | None:
    """Parse a ``sync_meta`` timestamp; None for a missing or unreadable value."""
    if not text:
        return None
    try:
        return datetime.strptime(text, _META_TIME_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        log.warning("ignoring unreadable sync timestamp %r", text)
        return None


def _older_than(stamp: datetime | None, now: datetime, age: timedelta) -> bool:
    """True if ``stamp`` is missing, at least ``age`` old, or in the future (clock moved)."""
    if stamp is None:
        return True
    elapsed = now - stamp
    return elapsed < timedelta(0) or elapsed >= age


# ── sync ─────────────────────────────────────────────────────────────────────


def sync_budget(
    client: YnabClient,
    mirror_store: MirrorStore,
    budget_id: str,
    *,
    full: bool = False,
    now: datetime | None = None,
) -> SyncResult:
    """Pull the accounts and transactions deltas of ``budget_id`` into the mirror.

    ``full=True`` ignores the stored cursors and fetches everything from knowledge 0 (also
    what the first sync of a budget does). ``now`` (timezone-aware) stamps ``last_sync_at``
    and drives the daily currency refresh.

    Raises the client's typed errors (:class:`~ynab_helper.errors.YnabRateLimited`,
    :class:`~ynab_helper.errors.YnabAuthError`, …) and :class:`~ynab_helper.errors.YnabError`
    for malformed data; whatever was stored before the failure stays consistent with its
    cursor, and ``last_sync_at`` only advances after both deltas were stored.
    """
    if not isinstance(budget_id, str) or not budget_id:
        raise ValueError("budget_id must be a non-empty string")
    moment = _utc(now)

    known_accounts = None if full else mirror_store.get_accounts_knowledge(budget_id)
    accounts_full = known_accounts is None
    accounts, accounts_knowledge = client.get_accounts(
        budget_id, last_knowledge=0 if known_accounts is None else known_accounts
    )
    accounts_changed = mirror_store.upsert_accounts(
        budget_id,
        accounts,
        server_knowledge=accounts_knowledge,
        # A complete listing marks what YNAB no longer lists; an empty mirror has nothing to mark.
        full=accounts_full and bool(mirror_store.accounts(budget_id, include_closed=True)),
    )

    known_tx = None if full else mirror_store.get_server_knowledge(budget_id)
    tx_full = known_tx is None
    transactions, tx_knowledge = client.get_transactions(
        budget_id, last_knowledge=0 if known_tx is None else known_tx
    )
    tx_changed = mirror_store.upsert_transactions(
        budget_id,
        transactions,
        server_knowledge=tx_knowledge,
        full=tx_full and bool(mirror_store.account_activity(budget_id)),
    )

    currency, refreshed = _refresh_currency(client, mirror_store, budget_id, moment, force=full)
    synced_at = format_sync_time(moment)
    # YNAB answered: any remembered rate-limit backoff is over.
    mirror_store.delete_meta(META_RATE_LIMITED_UNTIL.format(budget_id=budget_id))
    mirror_store.set_last_sync_at(budget_id, synced_at)
    log.info(
        "sync %s: accounts=%d transactions=%d knowledge=%d%s",
        budget_id,
        accounts_changed,
        tx_changed,
        tx_knowledge,
        " (full)" if tx_full else "",
    )
    return SyncResult(
        budget_id=budget_id,
        accounts_changed=accounts_changed,
        tx_changed=tx_changed,
        server_knowledge=tx_knowledge,
        accounts_knowledge=accounts_knowledge,
        full=tx_full,
        currency=currency,
        currency_refreshed=refreshed,
        synced_at=synced_at,
    )


def _refresh_currency(
    client: YnabClient,
    mirror_store: MirrorStore,
    budget_id: str,
    now: datetime,
    *,
    force: bool,
) -> tuple[str | None, bool]:
    """Fetch the budget currency if unknown or older than a day (best effort).

    Returns ``(currency, refreshed)``. A failure is logged and the stored value kept; only an
    authentication error propagates (the token is unusable for everything else too).
    """
    current = mirror_store.get_budget_currency(budget_id)
    checked = parse_sync_time(mirror_store.budget_currency_checked_at(budget_id))
    if not force and current is not None and not _older_than(checked, now, CURRENCY_MAX_AGE):
        return current, False
    try:
        fetched = client.get_budget_currency(budget_id)
    except YnabAuthError:
        raise
    except YnabError as exc:
        log.warning("could not refresh the currency of budget %s: %s", budget_id, exc)
        return current, False
    if not fetched:
        log.warning("YNAB reports no currency for budget %s", budget_id)
        return current, False
    mirror_store.set_budget_currency(budget_id, fetched, checked_at=format_sync_time(now))
    if current is not None and current != fetched:
        log.warning("budget %s currency changed from %s to %s", budget_id, current, fetched)
    return fetched, True


def mark_mirror_stale(mirror_store: MirrorStore, budget_id: str) -> None:
    """Forget ``last_sync_at`` so the next non-forced sync runs (call after writing to YNAB:
    the mirror's balances and rows no longer match until then)."""
    mirror_store.delete_meta(META_LAST_SYNC_AT.format(budget_id=budget_id))


def note_rate_limited(
    mirror_store: MirrorStore,
    budget_id: str,
    retry_after: float | None,
    *,
    now: datetime | None = None,
) -> datetime:
    """Remember that YNAB answered 429: non-forced syncs send nothing until the returned time.

    ``retry_after`` is YNAB's ``Retry-After`` in seconds (None or not positive →
    :data:`DEFAULT_RATE_LIMIT_BACKOFF`), capped at :data:`MAX_RATE_LIMIT_BACKOFF`.
    """
    wait = retry_after if retry_after is not None and retry_after > 0 else None
    seconds = min(wait if wait is not None else DEFAULT_RATE_LIMIT_BACKOFF, MAX_RATE_LIMIT_BACKOFF)
    # Whole seconds, rounded up: the stored format has no fractions.
    until = _utc(now).replace(microsecond=0) + timedelta(seconds=math.ceil(seconds))
    mirror_store.set_meta(
        META_RATE_LIMITED_UNTIL.format(budget_id=budget_id), format_sync_time(until)
    )
    return until


def rate_limited_until(
    mirror_store: MirrorStore, budget_id: str, *, now: datetime | None = None
) -> datetime | None:
    """The end of a remembered rate-limit backoff that is still running at ``now``, or None.

    A stored time further away than :data:`MAX_RATE_LIMIT_BACKOFF` (the clock moved
    backwards) is ignored.
    """
    until = parse_sync_time(
        mirror_store.get_meta(META_RATE_LIMITED_UNTIL.format(budget_id=budget_id))
    )
    if until is None:
        return None
    left = until - _utc(now)
    if left <= timedelta(0) or left > timedelta(seconds=MAX_RATE_LIMIT_BACKOFF):
        return None
    return until


def sync_and_refresh(
    client: YnabClient,
    conn: sqlite3.Connection,
    budget_id: str,
    account_id: str | None,
    *,
    force: bool = False,
    min_interval: float = DEFAULT_MIN_INTERVAL,
    full: bool = False,
    now: datetime | None = None,
) -> SyncResult:
    """Throttled :func:`sync_budget` followed by ``refresh_from_mirror`` for ``account_id``.

    * Not ``force``d and the previous sync finished less than ``min_interval`` seconds ago →
      nothing is requested; the result has ``skipped=True`` (the ledger is not refreshed:
      it was refreshed after that previous sync).
    * Not ``force``d while a rate-limit backoff runs (:func:`note_rate_limited`) → nothing
      is requested; the result has ``rate_limited=True`` and ``retry_after`` = seconds left.
    * YNAB answers 429 → the backoff is remembered and the result has ``rate_limited=True``
      instead of raising, so callers can fall back to the cached mirror. Every other error
      propagates.
    * After a successful sync the account's ledger is reconciled with the mirror (§5.3);
      pass ``account_id=None`` to skip that.

    The caller holds the user's operation lock. ``conn`` is a migrated transactions DB.
    """
    mirror = MirrorStore(conn)
    moment = _utc(now)
    last_text = mirror.get_last_sync_at(budget_id)
    if not force and not full:
        last = parse_sync_time(last_text)
        if not _older_than(last, moment, timedelta(seconds=max(0.0, min_interval))):
            log.debug("sync %s skipped: last sync at %s", budget_id, last_text)
            return SyncResult(
                budget_id=budget_id,
                currency=mirror.get_budget_currency(budget_id),
                synced_at=last_text,
                skipped=True,
            )
        until = rate_limited_until(mirror, budget_id, now=moment)
        if until is not None:
            log.debug("sync %s not sent: rate limited until %s", budget_id, until)
            return SyncResult(
                budget_id=budget_id,
                currency=mirror.get_budget_currency(budget_id),
                synced_at=last_text,
                rate_limited=True,
                retry_after=(until - moment).total_seconds(),
            )
    try:
        result = sync_budget(client, mirror, budget_id, full=full, now=moment)
    except YnabRateLimited as exc:
        log.warning("sync %s rate limited by YNAB (retry after %s s)", budget_id, exc.retry_after)
        note_rate_limited(mirror, budget_id, exc.retry_after, now=moment)
        return SyncResult(
            budget_id=budget_id,
            currency=mirror.get_budget_currency(budget_id),
            synced_at=last_text,
            rate_limited=True,
            retry_after=exc.retry_after,
        )
    if not account_id:
        return result
    view = mirror.view_for_account(budget_id, account_id)
    refresh = LedgerStore(conn).refresh_from_mirror(account_id, view)
    return dataclasses.replace(result, refresh=refresh)
