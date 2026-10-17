"""JSON reads (docs/DESIGN.md §9.2). Money is integer milliunits plus an ISO currency code.

Every read comes from the user's **mirror** of YNAB (and the importer ledger / statement
table for the dashboard) — never from a live YNAB call. Before reading, the handler asks
:class:`~ynab_helper.web.background.BackgroundSyncer` for a throttled background sync,
which returns immediately; responses carry ``last_sync_at`` and ``syncing`` so the page
can refresh once fresh data arrived. Handlers are plain ``def`` (threadpool, F41).

Shared response fields: ``currency`` (budget currency, may be null before the first
sync), ``last_sync_at`` (UTC ISO or null), ``syncing`` (a background sync is running).
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Final

from fastapi import APIRouter, Depends
from starlette.requests import Request

from ynab_helper.services.context import UserContext
from ynab_helper.storage.csv_files import CsvFile, CsvFileStore
from ynab_helper.storage.ledger import LedgerStore
from ynab_helper.storage.mirror import MirrorStore
from ynab_helper.web import analytics, queries
from ynab_helper.web.deps import ApiError, AuthedUser, WebState, api_user, get_state

__all__ = ["parse_accounts", "parse_date", "router"]

router = APIRouter(prefix="/api")

MAX_ACCOUNT_IDS: Final = 100
MAX_ID_LENGTH: Final = 64
MAX_QUERY_LENGTH: Final = 200
MAX_PAGE: Final = 100_000
MAX_PAGE_SIZE: Final = 200
_DATE_RE: Final = re.compile(r"\d{4}-\d{2}-\d{2}")


# ── parameter parsing ────────────────────────────────────────────────────────


def parse_accounts(raw: str | None) -> tuple[str, ...]:
    """``accounts=a,b`` → unique, non-empty ids (empty = every account)."""
    if not raw:
        return ()
    ids: list[str] = []
    for piece in raw.split(","):
        item = piece.strip()
        if not item or item in ids:
            continue
        if len(item) > MAX_ID_LENGTH:
            raise ApiError(400, "bad_request", "An account id is too long.")
        ids.append(item)
    if len(ids) > MAX_ACCOUNT_IDS:
        raise ApiError(400, "bad_request", f"At most {MAX_ACCOUNT_IDS} accounts per request.")
    return tuple(ids)


def parse_date(name: str, raw: str | None) -> str | None:
    """An optional ``YYYY-MM-DD`` parameter."""
    if not raw:
        return None
    try:
        if not _DATE_RE.fullmatch(raw):
            raise ValueError(raw)
        date.fromisoformat(raw)
    except ValueError:
        raise ApiError(400, "bad_request", f"{name} must be a date (YYYY-MM-DD).") from None
    return raw


# ── helpers ──────────────────────────────────────────────────────────────────


def today_of(state: WebState) -> date:
    """Today in the server's local time zone (the container runs with TZ=Europe/Zurich)."""
    return datetime.fromtimestamp(state.clock()).date()


@contextmanager
def _reading(request: Request, authed: AuthedUser) -> Iterator[tuple[Any, UserContext, dict]]:
    """Trigger the background sync, open the user's DB; yields ``(conn, ctx, meta)``."""
    state = get_state(request)
    ctx = state.user_context(authed.user)
    syncing = state.syncer.trigger(ctx)
    conn = state.open_user_db(ctx)
    try:
        mirror = MirrorStore(conn)
        meta = {
            "currency": mirror.get_budget_currency(ctx.budget_id),
            "last_sync_at": mirror.get_last_sync_at(ctx.budget_id),
            "syncing": syncing,
        }
        yield conn, ctx, meta
    finally:
        conn.close()


def _statement(record: CsvFile | None) -> dict[str, Any] | None:
    if record is None:
        return None
    summary = None
    result = record.result
    if result is not None:

        def count(key: str) -> int:
            value = result.get(key)
            return len(value) if isinstance(value, list) else 0

        summary = {
            "created": count("created"),
            "linked": count("linked"),
            "updated": count("updated"),
            "removed": count("removed"),
            "failures": count("failures"),
            "unchanged": result.get("unchanged") if isinstance(result.get("unchanged"), int) else 0,
            "not_applied": result.get("not_applied")
            if isinstance(result.get("not_applied"), int)
            else 0,
            "ok": result.get("ok") is True,
        }
    return {
        "id": record.id,
        "name": record.original_name,
        "uploaded_at": record.uploaded_at,
        "source": record.source,
        "status": record.status,
        "currency": record.currency,
        "row_count": record.row_count,
        "window": list(record.window) if record.window else None,
        "imported_at": record.imported_at,
        "summary": summary,
    }


# ── routes ───────────────────────────────────────────────────────────────────


@router.get("/me")
def me(request: Request, authed: AuthedUser = Depends(api_user)) -> dict[str, Any]:
    """The signed-in user (no secrets)."""
    state = get_state(request)
    return {**authed.summary().to_dict(), "version_line": state.version_line}


@router.get("/dashboard")
def dashboard(request: Request, authed: AuthedUser = Depends(api_user)) -> dict[str, Any]:
    """The Revolut account from the mirror (working/cleared/uncleared balance, F64),
    the budget currency (C06), the account's transaction and ledger counts (scoped to the
    import account, F100) and the newest statement with its import status."""
    with _reading(request, authed) as (conn, ctx, meta):
        mirror = MirrorStore(conn)
        account = mirror.account(ctx.account_id)
        counts = LedgerStore(conn).counts_by_status(ctx.account_id)
        statement = CsvFileStore(conn).latest(ctx.account_id)
        tx_counts = queries.account_tx_counts(conn, ctx.budget_id, ctx.account_id)
    user = authed.user
    return {
        "account": {
            "id": ctx.account_id,
            "name": (account.name if account else None) or user.account_name,
            "type": account.type if account else None,
            "found": account is not None,
            "closed": bool(account and account.closed),
            "deleted": bool(account and account.deleted),
            "on_budget": bool(account and account.on_budget),
        },
        "budget_name": user.budget_name,
        "balances": {
            "working": account.balance if account else None,
            "cleared": account.cleared_balance if account else None,
            "uncleared": account.uncleared_balance if account else None,
        },
        "transactions": tx_counts,
        "ledger": {"counts": dict(counts), "total": sum(counts.values())},
        "last_statement": _statement(statement),
        "auto_approve": user.auto_approve,
        "version_line": get_state(request).version_line,
        **meta,
    }


@router.get("/transactions")
def transactions(
    request: Request,
    q: str = "",
    state: str = "all",
    category: str = "",
    accounts: str = "",
    date_from: str = "",
    date_to: str = "",
    page: int = 1,
    page_size: int = 50,
    sort: str = queries.DEFAULT_SORT,
    authed: AuthedUser = Depends(api_user),
) -> dict[str, Any]:
    """Paginated, filtered list of the budget's live transactions from the mirror.

    ``q`` payee/memo substring (LIKE wildcards are literal); ``state`` all | cleared |
    reconciled | uncleared; ``category`` a name or ``__none__``; ``accounts`` comma list;
    ``date_from``/``date_to`` YYYY-MM-DD; ``page`` ≥ 1; ``page_size`` 1–200; ``sort``
    [-]date | [-]amount | [-]payee_name.
    """
    text = q.strip()
    if len(text) > MAX_QUERY_LENGTH:
        raise ApiError(400, "bad_request", "The search text is too long.")
    bucket = state.strip().lower()
    if bucket in ("", "all"):
        bucket_filter = None
    elif bucket in queries.CLEARED_STATES:
        bucket_filter = bucket
    else:
        raise ApiError(400, "bad_request", "state must be all, cleared, reconciled or uncleared.")
    if len(category) > MAX_QUERY_LENGTH:
        raise ApiError(400, "bad_request", "The category name is too long.")
    page = max(1, min(MAX_PAGE, page))
    page_size = max(1, min(MAX_PAGE_SIZE, page_size))
    filters = queries.TransactionFilters(
        text=text or None,
        state=bucket_filter,
        category=category or None,
        account_ids=parse_accounts(accounts),
        date_from=parse_date("date_from", date_from),
        date_to=parse_date("date_to", date_to),
        sort=sort if len(sort) <= 32 else queries.DEFAULT_SORT,
        limit=page_size,
        offset=(page - 1) * page_size,
    )
    with _reading(request, authed) as (conn, ctx, meta):
        total, items = queries.search_transactions(conn, ctx.budget_id, filters)
    return {"items": items, "total": total, "page": page, "page_size": page_size, **meta}


@router.get("/accounts")
def accounts(request: Request, authed: AuthedUser = Depends(api_user)) -> dict[str, Any]:
    """Every open or closed (not deleted) account of the budget from the mirror."""
    with _reading(request, authed) as (conn, ctx, meta):
        mirror = MirrorStore(conn)
        rows = mirror.accounts(ctx.budget_id, include_closed=True)
        activity = mirror.account_activity(ctx.budget_id)
    items = []
    for a in rows:
        count, newest = activity.get(a.id, (0, None))
        items.append(
            {
                "id": a.id,
                "name": a.name or "(unnamed)",
                "type": a.type,
                "classification": a.classification,
                "on_budget": a.on_budget,
                "closed": a.closed,
                "deleted": a.deleted,
                "balance": a.balance,
                "cleared_balance": a.cleared_balance,
                "uncleared_balance": a.uncleared_balance,
                "note": a.note,
                "last_seen_at": a.last_seen_at,
                "tx_count": count,
                "last_activity": newest,
                "is_primary": a.id == ctx.account_id,
            }
        )
    return {"items": items, "primary_account_id": ctx.account_id, **meta}


@router.get("/categories")
def categories(
    request: Request, accounts: str = "", authed: AuthedUser = Depends(api_user)
) -> dict[str, Any]:
    """Categories present in the mirror (split lines included) with count and net spend."""
    account_ids = parse_accounts(accounts)
    state = get_state(request)
    with _reading(request, authed) as (conn, ctx, meta):
        rows = MirrorStore(conn).analytics_rows(ctx.budget_id)
    report = analytics.categories(rows, today=today_of(state), account_ids=account_ids)
    return {**report.to_dict(), **meta}


@router.get("/spending")
def spending(
    request: Request,
    months: int = 6,
    accounts: str = "",
    authed: AuthedUser = Depends(api_user),
) -> dict[str, Any]:
    """Net spending per category: N complete months plus the month-to-date (F30, F65)."""
    account_ids = parse_accounts(accounts)
    state = get_state(request)
    today = today_of(state)
    window = analytics.spending_window(today, months)
    with _reading(request, authed) as (conn, ctx, meta):
        rows = MirrorStore(conn).analytics_rows(
            ctx.budget_id, date_from=window.start.isoformat(), date_to=today.isoformat()
        )
    report = analytics.spending(rows, today=today, months=months, account_ids=account_ids)
    return {**report.to_dict(), **meta}


@router.get("/subscriptions")
def subscriptions(
    request: Request,
    lookback_months: int = analytics.YEARLY_MIN_LOOKBACK,
    min_occurrences: int = 3,
    accounts: str = "",
    authed: AuthedUser = Depends(api_user),
) -> dict[str, Any]:
    """Recurring outflows charged within ``lookback_months`` (C03, F98). Yearly cadences are
    decided over 13 more months of history, so they are found whatever the look-back."""
    account_ids = parse_accounts(accounts)
    state = get_state(request)
    today = today_of(state)
    lookback = max(1, min(analytics.MAX_LOOKBACK_MONTHS, lookback_months))
    history = analytics.subscription_history_start(today, lookback)
    with _reading(request, authed) as (conn, ctx, meta):
        rows = MirrorStore(conn).analytics_rows(
            ctx.budget_id, date_from=history.isoformat(), date_to=today.isoformat()
        )
    report = analytics.subscriptions(
        rows,
        today=today,
        lookback_months=lookback,
        min_occurrences=min_occurrences,
        account_ids=account_ids,
    )
    return {**report.to_dict(), **meta}
