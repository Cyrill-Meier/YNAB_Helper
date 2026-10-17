"""Mirror sync and account status (docs/DESIGN.md §6.5, §7 ``sync`` / ``account_status``)."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from typing import Any, Final

from ynab_helper.errors import Busy, YnabAuthError, YnabError
from ynab_helper.locks import LOCKS
from ynab_helper.services import _common
from ynab_helper.services.context import UserContext
from ynab_helper.storage.csv_files import CsvFile, CsvFileStore
from ynab_helper.storage.ledger import LedgerStore
from ynab_helper.storage.mirror import MirrorStore
from ynab_helper.sync.ynab_sync import DEFAULT_MIN_INTERVAL, SyncResult, sync_and_refresh
from ynab_helper.ynab.client import YnabClient

__all__ = ["StatusInfo", "account_status", "sync"]

OP_SYNC: Final = "sync"
OP_STATUS: Final = "status"


# ── sync ─────────────────────────────────────────────────────────────────────


def sync(
    ctx: UserContext,
    *,
    force: bool = False,
    client: YnabClient | None = None,
    min_interval: float = DEFAULT_MIN_INTERVAL,
) -> SyncResult | None:
    """Sync the mirror with YNAB and refresh the account's ledger (§6.5).

    Returns None when the sync was throttled (the previous one finished less than
    ``min_interval`` seconds ago and ``force`` is False). A 429 gives a result with
    ``rate_limited=True``, and so does a non-forced call while the backoff after a recent 429
    runs (no request is sent then; ``retry_after`` says how long it still lasts). Other YNAB
    errors are raised. Raises :class:`~ynab_helper.errors.Busy` while another operation of
    the user runs.

    A sync writes only the mirror (a cache): on the first v2 open of a v1 database the
    legacy ledger rows are left unclaimed — the next real import decides which account owns
    them (F74).
    """
    with (
        LOCKS.operation(ctx.key, OP_SYNC),
        closing(_common.open_db(ctx, assign_legacy=False)) as conn,
    ):
        result = sync_and_refresh(
            client or _common.client_for(ctx),
            conn,
            ctx.budget_id,
            ctx.account_id,
            force=force,
            min_interval=min_interval,
        )
    return None if result.skipped else result


# ── status ───────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class StatusInfo:
    """What /status and the dashboard show for the user's account.

    Balances are milliunits from the mirror (None when the account is not in the mirror).
    ``sync_note`` explains why the numbers may be older than ``last_sync_at`` suggests
    (another operation was running, YNAB rate limit, network problem).
    """

    account_id: str
    account_name: str | None
    account_found: bool
    account_closed: bool
    account_deleted: bool
    currency: str | None
    balance: int | None
    cleared_balance: int | None
    uncleared_balance: int | None
    last_sync_at: str | None
    last_statement: CsvFile | None
    ledger_counts: Mapping[str, int]
    sync: SyncResult | None = None
    sync_note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe representation (web API)."""
        statement = None
        if self.last_statement is not None:
            s = self.last_statement
            statement = {
                "id": s.id,
                "original_name": s.original_name,
                "uploaded_at": s.uploaded_at,
                "source": s.source,
                "status": s.status,
                "currency": s.currency,
                "row_count": s.row_count,
                "window": list(s.window) if s.window else None,
                "imported_at": s.imported_at,
            }
        return {
            "account_id": self.account_id,
            "account_name": self.account_name,
            "account_found": self.account_found,
            "account_closed": self.account_closed,
            "account_deleted": self.account_deleted,
            "currency": self.currency,
            "balance": self.balance,
            "cleared_balance": self.cleared_balance,
            "uncleared_balance": self.uncleared_balance,
            "last_sync_at": self.last_sync_at,
            "last_statement": statement,
            "ledger_counts": dict(self.ledger_counts),
            "sync": self.sync.to_dict() if self.sync is not None else None,
            "sync_note": self.sync_note,
        }


def account_status(
    ctx: UserContext,
    *,
    client: YnabClient | None = None,
    min_interval: float = DEFAULT_MIN_INTERVAL,
) -> StatusInfo:
    """Balances from the mirror after a throttled sync, the last statement and ledger counts.

    The sync is skipped when another operation of the user is running (the cached mirror is
    shown) and while the backoff after a recent 429 runs (no request is sent, F17); a YNAB
    failure other than a rejected token is reported in ``sync_note`` instead of raised.
    :class:`~ynab_helper.errors.YnabAuthError` propagates. Like :func:`sync`, it never
    assigns legacy ledger rows to an account.
    """
    try:
        with (
            LOCKS.operation(ctx.key, OP_STATUS),
            closing(_common.open_db(ctx, assign_legacy=False)) as conn,
        ):
            note: str | None = None
            result: SyncResult | None = None
            try:
                result = sync_and_refresh(
                    client or _common.client_for(ctx),
                    conn,
                    ctx.budget_id,
                    ctx.account_id,
                    min_interval=min_interval,
                )
            except YnabAuthError:
                raise
            except YnabError as exc:
                note = f"YNAB could not be reached: {exc.user_message}"
            if result is not None and result.rate_limited:
                note = "YNAB's rate limit was reached; the numbers may be out of date."
            return _status_from_db(ctx, conn, result, note)
    except Busy as busy:
        with closing(_common.open_db(ctx, assign_legacy=False)) as conn:
            return _status_from_db(
                ctx,
                conn,
                None,
                f"Another operation ({busy.operation}) is running; "
                "the numbers are from the last sync.",
            )


def _status_from_db(
    ctx: UserContext,
    conn: sqlite3.Connection,
    result: SyncResult | None,
    note: str | None,
) -> StatusInfo:
    mirror = MirrorStore(conn)
    account = mirror.account(ctx.account_id)
    return StatusInfo(
        account_id=ctx.account_id,
        account_name=account.name if account else None,
        account_found=account is not None,
        account_closed=bool(account and account.closed),
        account_deleted=bool(account and account.deleted),
        currency=mirror.get_budget_currency(ctx.budget_id),
        balance=account.balance if account else None,
        cleared_balance=account.cleared_balance if account else None,
        uncleared_balance=account.uncleared_balance if account else None,
        last_sync_at=mirror.get_last_sync_at(ctx.budget_id),
        last_statement=CsvFileStore(conn).latest(ctx.account_id),
        ledger_counts=LedgerStore(conn).counts_by_status(ctx.account_id),
        sync=result,
        sync_note=note,
    )
