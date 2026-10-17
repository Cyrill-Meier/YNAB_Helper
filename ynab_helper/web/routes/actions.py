"""Operations through the service layer (docs/DESIGN.md §9.2).

Every state-changing route depends on :func:`~ynab_helper.web.deps.api_user_csrf` (session,
then CSRF) and reads its body only afterwards, with a byte cap (F21). The service calls run
in the threadpool (F41) under the per-user operation lock shared with the bot (F16); a
second operation is answered ``409 {error: "busy"}``. Service errors are mapped by
:func:`~ynab_helper.web.deps.api_error_for`.

* ``POST /api/sync`` — forced mirror sync; at most one per user every 10 s
  (``{synced: false, reason: "throttled"}`` otherwise) to protect YNAB's 200 requests/hour.
  An attempt that did not reach YNAB (busy, error) does not use up the slot.
* ``GET /api/reconcile/preview`` → :meth:`ReconcilePreview.to_dict`;
  ``POST /api/reconcile {token}`` → :meth:`ReconcileResult.to_dict`; a stale or used token
  is ``409 {error: "stale"}`` (F04).
* ``GET /api/dedupe/scan`` → :meth:`DedupeScan.to_dict`; ``POST /api/dedupe/delete
  {scan_id, ids}`` (1–200 ids of that scan) → :meth:`DedupeDeleteResult.to_dict`; an
  expired or replaced scan is ``409 stale`` (F13, F20).

The two GETs are not read-only: each runs a *forced* YNAB sync and replaces the user's
pending reconcile token or dedupe scan. They therefore require the CSRF header like a POST
(:data:`ynab_helper.web.routes.SIDE_EFFECT_GET_ROUTES`; a cross-site navigation carrying
the SameSite=Lax cookie cannot set it) and are throttled per user and operation
(``429 {error: "throttled"}``). These GETs and ``POST /api/sync`` also honour the mirror's
YNAB rate-limit backoff (F17): while it runs they answer ``429 {error: "rate_limited"}``
without sending a request, although the service layer's forced sync would not wait.
* ``POST /api/settings {auto_approve: bool}``.
* ``POST /api/upload`` — the raw CSV is the body (``Content-Type: text/csv``, file name
  URL-encoded in ``X-Filename``; no multipart, F22) → :meth:`ImportResult.to_dict`.
* ``POST /api/logout-all`` — revokes every session and login link of the user (F61).
"""

from __future__ import annotations

import logging
import math
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from typing import Any, Final
from urllib.parse import unquote

from fastapi import APIRouter, Depends
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse

from ynab_helper import sessions
from ynab_helper.errors import Busy
from ynab_helper.maintenance.dedupe import MAX_DELETE_IDS
from ynab_helper.revolut.files import sanitize_display_name
from ynab_helper.revolut.parser import MAX_CSV_BYTES, looks_like_revolut_csv
from ynab_helper.services.context import UserContext
from ynab_helper.storage.db import connect
from ynab_helper.storage.mirror import MirrorStore
from ynab_helper.storage.users import UserStore
from ynab_helper.sync import rate_limited_until
from ynab_helper.web.deps import (
    ApiError,
    AuthedUser,
    WebState,
    api_user_csrf,
    clear_session_cookie,
    get_state,
    read_body,
    read_json,
)
from ynab_helper.web.ratelimit import SlidingWindowLimiter

__all__ = ["router"]

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api")

#: Forced syncs per user: one every this many seconds.
FORCED_SYNC_INTERVAL: Final = 10.0
#: Reconcile previews / dedupe scans (each a forced YNAB sync) per user and operation …
FRESH_READ_LIMIT: Final = 5
#: … within this many seconds.
FRESH_READ_WINDOW: Final = 60.0
#: How long an operation waits for a background sync of the same user to finish.
IDLE_WAIT: Final = 10.0
MAX_TOKEN_LENGTH: Final = 256
MAX_FILENAME_HEADER: Final = 1024
CSV_MEDIA_TYPES: Final = frozenset({"text/csv", "application/csv"})


def _context(request: Request, authed: AuthedUser) -> tuple[WebState, UserContext]:
    state = get_state(request)
    ctx = state.user_context(authed.user)
    _wait_idle(state, ctx)
    return state, ctx


def _wait_idle(state: WebState, ctx: UserContext) -> None:
    # A background sync of this user holds the operation lock for a moment; let it finish
    # instead of answering "busy" to a click that came right after a page load.
    state.syncer.wait_idle(ctx.key, IDLE_WAIT)


def _retry_header(seconds: float) -> dict[str, str]:
    return {"Retry-After": str(max(1, math.ceil(seconds)))}


def _refuse_during_backoff(state: WebState, ctx: UserContext) -> None:
    """429 without any YNAB request while the mirror's rate-limit backoff runs (F17).

    The service layer's forced sync does not wait for the backoff; the web's syncing
    reads must not keep hitting YNAB after it said 429.
    """
    if not ctx.db_path.exists():
        return  # no DB yet: nothing was ever rate limited
    conn = state.open_user_db(ctx)
    try:
        until = rate_limited_until(MirrorStore(conn), ctx.budget_id)
    finally:
        conn.close()
    if until is None:
        return
    left = max(1.0, (until - datetime.now(UTC)).total_seconds())
    raise ApiError(
        429,
        "rate_limited",
        "YNAB rate limit reached (200 requests per hour). Please try again later.",
        headers=_retry_header(left),
        extra={"retry_after": round(left, 1)},
    )


@contextmanager
def _fresh_read_slot(limiter: SlidingWindowLimiter, key: str) -> Iterator[None]:
    """Throttle a syncing GET; the slot is given back when the operation never ran (busy)."""
    if not limiter.allow(key):
        wait = limiter.retry_after(key)
        raise ApiError(
            429,
            "throttled",
            "You did this several times in a row — wait a moment and try again.",
            headers=_retry_header(wait),
            extra={"retry_after": round(wait, 1)},
        )
    try:
        yield
    except Busy:
        limiter.release(key)
        raise


def _string(data: dict[str, Any], key: str, max_length: int) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip() or len(value) > max_length:
        raise ApiError(400, "bad_request", f"'{key}' must be a non-empty string.")
    return value.strip()


# ── sync ─────────────────────────────────────────────────────────────────────


@router.post("/sync")
def sync_now(request: Request, authed: AuthedUser = Depends(api_user_csrf)) -> dict[str, Any]:
    """Forced mirror sync (the "Refresh from YNAB" button)."""
    state = get_state(request)
    ctx = state.user_context(authed.user)
    _refuse_during_backoff(state, ctx)
    if not state.sync_limiter.allow(ctx.key):
        return {
            "ok": True,
            "synced": False,
            "reason": "throttled",
            "retry_after": round(state.sync_limiter.retry_after(ctx.key), 1),
            "message": "Synced a moment ago — try again in a few seconds.",
        }
    # Only a sync that actually ran uses up the slot: a retry after "busy", a YNAB error
    # or a 429 must not be told "synced a moment ago" (F97).
    try:
        _wait_idle(state, ctx)
        result = state.services.sync(ctx, force=True)
    except BaseException:
        state.sync_limiter.release(ctx.key)
        raise
    if result is None:  # pragma: no cover - a forced sync is never throttled
        state.sync_limiter.release(ctx.key)
        return {"ok": True, "synced": False, "reason": "throttled"}
    if result.rate_limited:
        state.sync_limiter.release(ctx.key)
        retry = result.retry_after
        raise ApiError(
            429,
            "rate_limited",
            "YNAB rate limit reached (200 requests per hour). Please try again later.",
            headers=_retry_header(retry) if retry and retry > 0 else None,
            extra={"retry_after": retry},
        )
    return {"ok": True, "synced": True, "reason": None, "sync": result.to_dict()}


# ── reconcile ────────────────────────────────────────────────────────────────


@router.get("/reconcile/preview")
def reconcile_preview(
    request: Request, authed: AuthedUser = Depends(api_user_csrf)
) -> dict[str, Any]:
    """Step 1: the numbers and a confirmation token (no YNAB write; a forced sync, and the
    previous token is replaced — hence CSRF and the throttle)."""
    state = get_state(request)
    ctx = state.user_context(authed.user)
    _refuse_during_backoff(state, ctx)
    with _fresh_read_slot(state.fresh_limiter, f"{ctx.key}:reconcile"):
        _wait_idle(state, ctx)
        return state.services.reconcile_preview(ctx).to_dict()


@router.post("/reconcile")
async def reconcile_apply(
    request: Request, authed: AuthedUser = Depends(api_user_csrf)
) -> dict[str, Any]:
    """Step 2: post the adjustment the user confirmed (``{"token": …}``)."""
    data = await read_json(request)
    token = _string(data, "token", MAX_TOKEN_LENGTH)

    def work() -> dict[str, Any]:
        state, ctx = _context(request, authed)
        return state.services.reconcile_apply(ctx, token).to_dict()

    return await run_in_threadpool(work)


# ── dedupe ───────────────────────────────────────────────────────────────────


@router.get("/dedupe/scan")
def dedupe_scan(request: Request, authed: AuthedUser = Depends(api_user_csrf)) -> dict[str, Any]:
    """Importer transactions no statement row accounts for (kept server-side 10 min; a
    forced sync, and the previous scan is replaced — hence CSRF and the throttle)."""
    state = get_state(request)
    ctx = state.user_context(authed.user)
    _refuse_during_backoff(state, ctx)
    with _fresh_read_slot(state.fresh_limiter, f"{ctx.key}:dedupe"):
        _wait_idle(state, ctx)
        return state.services.dedupe_scan(ctx).to_dict()


@router.post("/dedupe/delete")
async def dedupe_delete(
    request: Request, authed: AuthedUser = Depends(api_user_csrf)
) -> dict[str, Any]:
    """Delete confirmed candidates of the current scan: ``{"scan_id", "ids": [...]}``."""
    data = await read_json(request)
    scan_id = _string(data, "scan_id", MAX_TOKEN_LENGTH)
    raw_ids = data.get("ids")
    if not isinstance(raw_ids, list) or not raw_ids:
        raise ApiError(400, "bad_selection", "Select at least one transaction.")
    ids: list[str] = []
    for item in raw_ids:
        if not isinstance(item, str) or not item.strip() or len(item) > 64:
            raise ApiError(400, "bad_selection", "Every id must be a short, non-empty string.")
        if item not in ids:
            ids.append(item)
    if len(ids) > MAX_DELETE_IDS:
        raise ApiError(
            400,
            "too_many_ids",
            f"At most {MAX_DELETE_IDS} transactions per request (got {len(ids)}).",
        )

    def work() -> dict[str, Any]:
        state, ctx = _context(request, authed)
        return state.services.dedupe_delete(ctx, scan_id, ids).to_dict()

    return await run_in_threadpool(work)


# ── settings ─────────────────────────────────────────────────────────────────


@router.post("/settings")
async def update_settings(
    request: Request, authed: AuthedUser = Depends(api_user_csrf)
) -> dict[str, Any]:
    """``{"auto_approve": true|false}`` — whether imported transactions are approved."""
    data = await read_json(request)
    if set(data) - {"auto_approve"}:
        raise ApiError(400, "bad_request", "Only 'auto_approve' can be changed here.")
    value = data.get("auto_approve")
    if not isinstance(value, bool):
        raise ApiError(400, "bad_request", "'auto_approve' must be true or false.")
    state = get_state(request)

    def work() -> bool:
        with closing(connect(state.users_db_path)) as conn:
            user = UserStore(conn, lock=state.users_lock).upsert(
                authed.telegram_id, auto_approve=value
            )
        return user.auto_approve

    stored = await run_in_threadpool(work)
    log.info("web: user %s set auto_approve=%s", authed.telegram_id, stored)
    return {"ok": True, "auto_approve": stored}


# ── upload ───────────────────────────────────────────────────────────────────


def _upload_name(request: Request) -> str | None:
    raw = request.headers.get("x-filename")
    if raw is None:
        return None
    if len(raw) > MAX_FILENAME_HEADER:
        raise ApiError(400, "bad_request", "The file name is too long.")
    name = sanitize_display_name(unquote(raw, errors="replace"))
    if name is not None and not name.lower().endswith(".csv"):
        raise ApiError(400, "bad_extension", "Choose a .csv file (the Revolut statement export).")
    return name


@router.post("/upload")
async def upload(request: Request, authed: AuthedUser = Depends(api_user_csrf)) -> dict[str, Any]:
    """Import a Revolut statement sent as the raw request body (``text/csv``)."""
    media = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if media not in CSV_MEDIA_TYPES:
        raise ApiError(
            415, "unsupported_media_type", "Send the statement as text/csv (the raw file)."
        )
    name = _upload_name(request)
    try:
        body = await read_body(request, MAX_CSV_BYTES)
    except ApiError as exc:
        if exc.status == 413:
            raise ApiError(413, "too_large", "The statement is larger than 25 MB.") from None
        raise
    if not body.strip():
        raise ApiError(400, "empty", "The file is empty.")
    if not looks_like_revolut_csv(body):
        raise ApiError(
            400,
            "not_revolut",
            "This does not look like a Revolut account statement (CSV export).",
        )

    def work() -> dict[str, Any]:
        state, ctx = _context(request, authed)
        result = state.services.import_csv(ctx, body, original_name=name, source_kind="web")
        return result.to_dict()

    result = await run_in_threadpool(work)
    log.info(
        "web: user %s imported %s (ok=%s)", authed.telegram_id, name or "a statement", result["ok"]
    )
    return result


# ── sessions ─────────────────────────────────────────────────────────────────


@router.post("/logout-all")
def logout_all(request: Request, authed: AuthedUser = Depends(api_user_csrf)) -> JSONResponse:
    """Sign out everywhere: every session and unused login link of this user (F61)."""
    state = get_state(request)
    with closing(connect(state.users_db_path)) as conn:
        removed = sessions.revoke_all(conn, authed.telegram_id)
    log.info("web: user %s signed out everywhere (%d sessions)", authed.telegram_id, removed)
    response = JSONResponse({"ok": True, "revoked": removed})
    clear_session_cookie(response, state.config)
    return response
