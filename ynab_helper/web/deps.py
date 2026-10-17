"""Request-scoped helpers of the web UI: state, authentication, CSRF, bodies, errors.

* :class:`WebState` — everything the routes need, stored on ``app.state.web``.
* Authentication dependencies (plain ``def`` → FastAPI runs them in the threadpool, the
  SQLite work never touches the event loop):
  :func:`page_user` for HTML pages (a missing/expired session raises
  :class:`LoginRequired` → 303 to ``/`` or ``/?expired=1`` with the cookie cleared, F24),
  :func:`api_user` for JSON reads (→ 401 ``{error: "session_expired", message}``) and
  :func:`api_user_csrf` for every state-changing JSON route — including the GETs that sync
  YNAB and replace a pending token or scan (``routes.SIDE_EFFECT_GET_ROUTES``) — (session
  first, then the ``X-CSRF-Token`` header checked with :meth:`TokenHasher.verify_csrf`, 403
  otherwise). Both refuse a request the browser marks as coming from another site
  (``Sec-Fetch-Site`` other than ``same-origin``/``none``): the SameSite=Lax cookie rides
  along on cross-site navigations and on every request from a sibling subdomain, and the
  page's own ``fetch`` calls are always same-origin.
  A session only counts while its user is ``ready`` (F18; enforced by
  :func:`ynab_helper.sessions.lookup_session`).
* Bodies are read **only by handlers, after** the auth/CSRF dependency ran
  (:func:`read_json`, :func:`read_body`), with a byte cap — never by FastAPI before
  authentication (F21). There is no multipart parsing at all (F22).
* :func:`api_error_for` maps service exceptions to ``{error, message}`` responses
  (``Busy`` → 409 ``busy``, ``StaleStateError`` → 409 ``stale`` …).
* :func:`template_context` builds the context every HTML template receives (contract in
  :mod:`ynab_helper.web.app`).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from collections.abc import Callable, Mapping
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from starlette.requests import Request
from starlette.responses import Response
from starlette.templating import Jinja2Templates

from ynab_helper import sessions
from ynab_helper.balance.reconcile import ReconcileUnavailable, StatementUnavailable
from ynab_helper.errors import (
    Busy,
    ConfigError,
    CsvError,
    ExternalServiceError,
    StaleStateError,
    YnabAuthError,
    YnabHelperError,
    YnabRateLimited,
)
from ynab_helper.maintenance.dedupe import DedupeSelectionError
from ynab_helper.services.context import UserContext
from ynab_helper.sessions import SESSION_COOKIE, SessionInfo, TokenHasher
from ynab_helper.storage.db import StorageError, connect, open_tx_db
from ynab_helper.storage.users import User, UserStore
from ynab_helper.web.background import BackgroundSyncer, DetachedRunner
from ynab_helper.web.config import WebConfig
from ynab_helper.web.middleware import API_BODY_LIMIT, SessionPurger
from ynab_helper.web.ratelimit import SlidingWindowLimiter

__all__ = [
    "CSRF_FORM_FIELD",
    "CSRF_HEADER",
    "NAV",
    "NOTICES",
    "PAGES",
    "ApiError",
    "AuthedUser",
    "LoginRequired",
    "UserSummary",
    "WebState",
    "api_error_for",
    "api_user",
    "api_user_csrf",
    "clear_session_cookie",
    "client_ip",
    "get_state",
    "page_user",
    "read_body",
    "read_json",
    "render",
    "resolve_session",
    "set_session_cookie",
    "template_context",
]

log = logging.getLogger(__name__)

CSRF_HEADER: Final = "X-CSRF-Token"
#: Form field carrying the CSRF token for plain HTML form posts (``POST /logout``).
CSRF_FORM_FIELD: Final = "csrf_token"

#: The app pages: key → navigation label (order = navigation order, as v1).
PAGES: Final[dict[str, str]] = {
    "dashboard": "Dashboard",
    "accounts": "Accounts",
    "transactions": "Transactions",
    "spending": "Spending",
    "subscriptions": "Subscriptions",
    "upload": "Upload",
    "reconcile": "Reconcile",
    "dedupe": "Dedupe",
    "settings": "Settings",
}
#: Navigation entries ``(key, label, href)`` handed to every template.
NAV: Final[tuple[tuple[str, str, str], ...]] = tuple(
    (key, label, f"/app/{key}") for key, label in PAGES.items()
)

SESSION_EXPIRED_MESSAGE: Final = (
    "Your session has expired. Send /login to the bot in Telegram for a new sign-in link."
)
#: Notices an app page can show, selected by ``?notice=<key>`` (never free text).
NOTICES: Final[dict[str, str]] = {
    "logout_stale": (
        "You are still signed in: that page was out of date (you signed in again in "
        "another tab). Use Sign out again to sign out."
    ),
}
#: ``Sec-Fetch-Site`` values accepted on the JSON API (the page's own requests, or the
#: user typing the URL). Absent (older browsers, non-browser clients) is accepted too.
_API_FETCH_SITES: Final = frozenset({"same-origin", "none"})


# ── state ────────────────────────────────────────────────────────────────────


@dataclass(slots=True)
class WebState:
    """Everything the routes need (``app.state.web``)."""

    config: WebConfig
    hasher: TokenHasher = field(repr=False)
    users_db_path: Path
    data_dir: Path
    version_line: str
    cache_bust: str
    templates: Jinja2Templates
    services: Any
    clock: Callable[[], float]
    auth_limiter: SlidingWindowLimiter
    sync_limiter: SlidingWindowLimiter
    fresh_limiter: SlidingWindowLimiter
    syncer: BackgroundSyncer
    purger: SessionPurger
    detached: DetachedRunner
    delete_telegram_message: Callable[[int, int], None] | None = None
    users_lock: threading.Lock | threading.RLock | None = None

    @property
    def static_base(self) -> str:
        return f"/static/{self.cache_bust}"

    def user_context(self, user: User) -> UserContext:
        """The service context of a ready user (paths under the bot's data dir)."""
        return UserContext.from_user_row(user, self.data_dir)

    def open_user_db(self, ctx: UserContext) -> sqlite3.Connection:
        """The user's migrated transactions DB, opened like a read-only service operation
        (legacy ledger rows are never assigned by the web's reads, F74)."""
        opener = getattr(self.services, "_open_db", None)
        if opener is None:
            return open_tx_db(ctx.db_path, backup_dir=ctx.backups)
        return opener(ctx, assign_legacy=False)


def get_state(request: Request) -> WebState:
    return request.app.state.web


def client_ip(request: Request) -> str | None:
    """The client address after uvicorn's proxy-header processing."""
    return request.client.host if request.client else None


# ── errors ───────────────────────────────────────────────────────────────────


class ApiError(Exception):
    """An error response ``{error, message, **extra}`` with ``status``."""

    def __init__(
        self,
        status: int,
        error: str,
        message: str,
        *,
        headers: Mapping[str, str] | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(f"{status} {error}: {message}")
        self.status = status
        self.error = error
        self.message = message
        self.headers = dict(headers or {})
        self.extra = dict(extra or {})

    def body(self) -> dict[str, Any]:
        return {"error": self.error, "message": self.message, **self.extra}


class LoginRequired(Exception):
    """No valid session. HTML routes redirect to the login page, JSON routes answer 401."""

    def __init__(self, *, api: bool, had_cookie: bool) -> None:
        super().__init__("login required")
        self.api = api
        self.had_cookie = had_cookie


def api_error_for(exc: YnabHelperError) -> ApiError:
    """The ``{error, message}`` response for a service exception (``user_message`` only)."""
    message = exc.user_message or "The operation failed."
    if isinstance(exc, Busy):
        return ApiError(409, "busy", message, extra={"operation": exc.operation})
    if isinstance(exc, StaleStateError):
        return ApiError(409, "stale", message)
    if isinstance(exc, DedupeSelectionError):
        return ApiError(400, "bad_selection", message)
    if isinstance(exc, CsvError):
        return ApiError(400, "invalid_csv", message)
    if isinstance(exc, StatementUnavailable | ReconcileUnavailable):
        return ApiError(409, "unavailable", message, extra={"reason": exc.reason})
    if isinstance(exc, ConfigError):
        return ApiError(409, "setup_required", message)
    if isinstance(exc, YnabRateLimited):
        headers: dict[str, str] = {}
        extra: dict[str, Any] = {}
        if exc.retry_after is not None and exc.retry_after > 0:
            headers["Retry-After"] = str(int(exc.retry_after + 0.999))
            extra["retry_after"] = exc.retry_after
        return ApiError(429, "rate_limited", message, headers=headers, extra=extra)
    if isinstance(exc, YnabAuthError):
        return ApiError(502, "ynab_auth", message)
    if isinstance(exc, ExternalServiceError):
        return ApiError(502, "upstream", message)
    if isinstance(exc, StorageError):
        log.error("web: storage error: %s", exc)
        return ApiError(500, "storage", message)
    return ApiError(400, "failed", message)


# ── authentication ───────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class UserSummary:
    """What templates and ``/api/me`` may show about the signed-in user (no secrets)."""

    telegram_id: int
    first_name: str | None
    username: str | None
    budget_name: str | None
    account_name: str | None
    auto_approve: bool
    crypto_configured: bool
    session_expires_at: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "telegram_id": self.telegram_id,
            "first_name": self.first_name,
            "username": self.username,
            "budget_name": self.budget_name,
            "account_name": self.account_name,
            "auto_approve": self.auto_approve,
            "crypto_configured": self.crypto_configured,
            "session_expires_at": self.session_expires_at,
        }


@dataclass(frozen=True, slots=True)
class AuthedUser:
    """A request's signed-in, ready user plus its session."""

    user: User
    session: SessionInfo
    session_token: str = field(repr=False)

    @property
    def telegram_id(self) -> int:
        return self.user.telegram_id

    def summary(self) -> UserSummary:
        u = self.user
        return UserSummary(
            telegram_id=u.telegram_id,
            first_name=u.first_name,
            username=u.username,
            budget_name=u.budget_name,
            account_name=u.account_name,
            auto_approve=u.auto_approve,
            crypto_configured=bool(u.crypto_account_id),
            session_expires_at=self.session.expires_at,
        )


def resolve_session(state: WebState, token: str | None) -> AuthedUser | None:
    """Look up (and slide) the session of ``token``; None unless its user is ready."""
    if not token:
        return None
    with closing(connect(state.users_db_path)) as conn:
        info = sessions.lookup_session(
            conn,
            state.hasher,
            token,
            sliding_ttl=state.config.session_ttl,
            absolute_ttl=state.config.session_absolute_ttl,
            now=state.clock(),
        )
        if info is None:
            return None
        user = UserStore(conn).get(info.telegram_id)
    if user is None or not user.is_ready:
        return None
    return AuthedUser(user=user, session=info, session_token=token)


def _require_same_site_fetch(request: Request) -> None:
    """403 for a JSON API request the browser marks as cross-site or same-site."""
    site = request.headers.get("sec-fetch-site")
    if site is not None and site.strip().lower() not in _API_FETCH_SITES:
        raise ApiError(403, "cross_site", "This request did not come from this site.")


def _authenticate(request: Request, *, api: bool) -> AuthedUser:
    if api:
        _require_same_site_fetch(request)
    token = request.cookies.get(SESSION_COOKIE)
    authed = resolve_session(get_state(request), token)
    if authed is None:
        raise LoginRequired(api=api, had_cookie=bool(token))
    return authed


def page_user(request: Request) -> AuthedUser:
    """Dependency for HTML pages: the signed-in user or a redirect to the login page."""
    return _authenticate(request, api=False)


def api_user(request: Request) -> AuthedUser:
    """Dependency for JSON reads: the signed-in user or 401 ``session_expired``."""
    return _authenticate(request, api=True)


def api_user_csrf(request: Request) -> AuthedUser:
    """Dependency for state-changing JSON routes (and the side-effect GETs): session, then
    the CSRF header."""
    authed = _authenticate(request, api=True)
    if not get_state(request).hasher.verify_csrf(
        authed.session_token, request.headers.get(CSRF_HEADER)
    ):
        raise ApiError(
            403,
            "csrf_failed",
            "The page is out of date (signed in again elsewhere?). Reload it and try again.",
        )
    return authed


# ── bodies (read only after authentication) ──────────────────────────────────


async def read_body(request: Request, max_bytes: int) -> bytes:
    """The request body, refused with 413 once it passes ``max_bytes`` (streamed count)."""
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > max_bytes:
        raise ApiError(413, "too_large", f"The request is larger than {max_bytes} bytes.")
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > max_bytes:
            raise ApiError(413, "too_large", f"The request is larger than {max_bytes} bytes.")
        chunks.append(chunk)
    return b"".join(chunks)


def _reject_constant(name: str) -> Any:
    raise ValueError(f"{name} is not allowed")


async def read_json(request: Request, max_bytes: int = API_BODY_LIMIT) -> dict[str, Any]:
    """A JSON object body (``Content-Type: application/json``), at most ``max_bytes``.

    Requiring the JSON media type also means a cross-site page cannot send it without a
    CORS preflight (defence in depth next to the CSRF token).
    """
    media = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if media != "application/json":
        raise ApiError(
            415, "unsupported_media_type", "Send the request as JSON (application/json)."
        )
    body = await read_body(request, max_bytes)
    if not body.strip():
        raise ApiError(400, "bad_request", "The request body is empty.")
    try:
        data = json.loads(body, parse_constant=_reject_constant)
    except (ValueError, RecursionError):
        raise ApiError(400, "bad_request", "The request body is not valid JSON.") from None
    if not isinstance(data, dict):
        raise ApiError(400, "bad_request", "The request body must be a JSON object.")
    return data


# ── templates ────────────────────────────────────────────────────────────────


def template_context(
    state: WebState,
    *,
    page: str,
    authed: AuthedUser | None = None,
    expired: bool = False,
    notice: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """The context of every HTML template (contract documented in :mod:`.app`)."""
    return {
        "page": page,
        "csrf_token": state.hasher.csrf_for(authed.session_token) if authed else "",
        "user": authed.summary() if authed else None,
        "version_line": state.version_line,
        "cache_bust": state.cache_bust,
        "static_base": state.static_base,
        "nav": NAV,
        "expired": expired,
        "notice": NOTICES.get(notice or ""),
        **extra,
    }


def render(
    state: WebState,
    request: Request,
    template: str,
    *,
    page: str,
    authed: AuthedUser | None = None,
    expired: bool = False,
    notice: str | None = None,
    status_code: int = 200,
    headers: Mapping[str, str] | None = None,
    **extra: Any,
) -> Response:
    """Render ``template`` with the standard context (plus ``extra``)."""
    context = template_context(
        state, page=page, authed=authed, expired=expired, notice=notice, **extra
    )
    return state.templates.TemplateResponse(
        request, template, context, status_code=status_code, headers=dict(headers or {})
    )


# ── cookies ──────────────────────────────────────────────────────────────────


def set_session_cookie(response: Response, config: WebConfig, token: str) -> None:
    """``rynab_session``: HttpOnly, SameSite=Lax, Secure iff the public URL is https,
    Max-Age = the absolute session TTL (DESIGN §2.8, §9.1)."""
    response.set_cookie(
        SESSION_COOKIE,
        token,
        max_age=config.session_absolute_ttl,
        path="/",
        secure=config.cookie_secure,
        httponly=True,
        samesite="lax",
    )


def clear_session_cookie(response: Response, config: WebConfig) -> None:
    response.delete_cookie(
        SESSION_COOKIE, path="/", secure=config.cookie_secure, httponly=True, samesite="lax"
    )
