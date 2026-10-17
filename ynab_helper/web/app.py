"""The web UI's FastAPI application (docs/DESIGN.md §9).

:func:`make_app` builds the app; :func:`ynab_helper.web.server.serve_in_thread` runs it in
the bot process. Interactive docs, OpenAPI and ReDoc are disabled, and so is FastAPI's
native OpenTelemetry instrumentation (request data must never leave the process).

Template context contract
=========================

Every HTML template is rendered by :func:`ynab_helper.web.deps.render` with this context
(the frontend depends on it; keys are always present):

==================  ==========================================================================
``request``         the Starlette request (added by ``TemplateResponse``)
``page``            ``str`` — the page key: one of ``dashboard accounts transactions spending
                    subscriptions upload reconcile dedupe settings`` for ``/app/<page>``
                    (template ``<page>.html``), ``"login"`` for ``login.html`` and ``"auth"``
                    for ``auth.html``. Put it on ``<body data-page="…">``.
``csrf_token``      ``str`` — the session's CSRF token (``""`` without a session). Expose it as
                    ``<meta name="csrf-token" content="…">``; JavaScript sends it in the
                    ``X-CSRF-Token`` header of every ``/api`` request (see below), and the
                    logout ``<form method=post action=/logout>`` carries it in a hidden
                    ``csrf_token`` field.
``user``            :class:`~ynab_helper.web.deps.UserSummary` or ``None`` (login/auth pages):
                    ``telegram_id, first_name, username, budget_name, account_name,
                    auto_approve, crypto_configured, session_expires_at`` — never a secret.
``version_line``    ``str`` — e.g. ``"v2.0.0 (abc1234, 2026-10-16 20:00 UTC)"`` (footer).
``cache_bust``      ``str`` — deploy token; static files live under ``/static/<cache_bust>/``.
``static_base``     ``str`` — ``"/static/<cache_bust>"``, e.g.
                    ``{{ static_base }}/js/main.js`` (immutable caching on that path).
``nav``             ``tuple[(key, label, href), ...]`` — the app pages in navigation order;
                    mark the entry whose ``key == page`` with ``aria-current="page"``.
``expired``         ``bool`` — the login page is shown because a session expired (show a
                    notice). Always ``False`` on app pages.
``notice``          ``str | None`` — a fixed server-side notice to show near the top of the
                    page (``role="status"``), e.g. after a logout from an outdated page
                    (``/app/dashboard?notice=logout_stale``). Never text from the URL.
==================  ==========================================================================

Extra keys per template:

* ``login.html`` — ``message: str`` (what to do / what went wrong) and, on errors,
  ``error: True``. Status 400 (bad or used link), 403 (cross-site sign-in), 429 (rate limit).
* ``auth.html`` — ``login_token: str``, ``confirm_switch: bool`` and ``message`` (str or
  None): render ``<form id="auth-form" method="post" action="/auth">`` with ``<input
  type="hidden" name="t" value="{{ login_token }}">`` and a visible submit button
  "Continue". Without ``confirm_switch`` the form carries ``data-autosubmit="true"`` and
  ``{{ static_base }}/js/auth.js`` submits it on load. **With** ``confirm_switch`` (this
  browser already has a valid session; ``user`` is that signed-in user) the page asks
  "Switch account?", names ``user`` (``first_name``/``budget_name``), shows ``message``,
  offers a "Stay signed in" link to ``/app/dashboard`` and must **not** auto-submit: a
  forwarded link would otherwise swap the user into someone else's budget. auth.js must
  only submit a form marked ``data-autosubmit="true"`` (a test checks it).
  Do not add a ``no-referrer`` referrer policy to this page: the POST must carry its
  ``Origin`` (the server's ``Referrer-Policy: same-origin`` already keeps the token in the
  URL from leaking to other sites).
* App pages: show ``user`` (name and budget) prominently in the header, so a swapped
  account is noticed.

Templates must not contain inline scripts, inline styles or event-handler attributes: the
CSP (:data:`ynab_helper.web.middleware.CONTENT_SECURITY_POLICY`) forbids them.

JSON conventions: money is integer milliunits with the budget ``currency`` (ISO code);
errors are ``{error, message}`` (``401 session_expired`` → go to ``/?expired=1``;
``403 csrf_failed`` → reload; ``403 cross_site``; ``409 busy`` / ``409 stale``;
``429 rate_limited`` (YNAB) / ``429 throttled`` (too many syncing requests), both with
``retry_after`` seconds). Send ``X-CSRF-Token`` on **every** ``/api`` request (simplest in
the fetch wrapper): it is required on every POST and on the two GETs that sync YNAB and
replace server-side state, ``/api/reconcile/preview`` and ``/api/dedupe/scan``
(:data:`ynab_helper.web.routes.SIDE_EFFECT_GET_ROUTES`).
"""

from __future__ import annotations

import importlib
import logging
import re
import threading
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from types import ModuleType
from typing import Any, Final

import jinja2
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from ynab_helper.errors import ConfigError, YnabHelperError
from ynab_helper.storage.db import default_backup_dir, open_users_db
from ynab_helper.web.background import BackgroundSyncer, DetachedRunner
from ynab_helper.web.config import WebConfig
from ynab_helper.web.deps import (
    SESSION_EXPIRED_MESSAGE,
    ApiError,
    LoginRequired,
    WebState,
    api_error_for,
    clear_session_cookie,
)
from ynab_helper.web.middleware import (
    BodySizeLimitMiddleware,
    BodyTooLarge,
    ErrorBoundaryMiddleware,
    IPAllowlistMiddleware,
    SecurityHeadersMiddleware,
    SessionPurger,
)
from ynab_helper.web.ratelimit import SlidingWindowLimiter
from ynab_helper.web.routes import include_routes
from ynab_helper.web.routes.actions import (
    FORCED_SYNC_INTERVAL,
    FRESH_READ_LIMIT,
    FRESH_READ_WINDOW,
)

__all__ = ["STATIC_DIR", "TEMPLATES_DIR", "make_app"]

log = logging.getLogger(__name__)

WEB_DIR: Final = Path(__file__).resolve().parent
TEMPLATES_DIR: Final = WEB_DIR / "templates"
STATIC_DIR: Final = WEB_DIR / "static"

#: POST /auth attempts per client (IPv4 address / IPv6 /64) per minute (DESIGN §9.1).
AUTH_ATTEMPTS_PER_MINUTE: Final = 10
_CACHE_BUST_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
#: FastAPI's native OpenTelemetry instrumentation, fully off.
_NO_TELEMETRY: Final[dict[str, Any]] = {
    "tracing": False,
    "metrics": False,
    "logs": False,
    "operation_spans": False,
    "auto_configure": False,
}


def _safe_cache_bust(token: str) -> str:
    """The cache-bust token if it is a safe single path segment, else ``"dev"``."""
    if isinstance(token, str) and _CACHE_BUST_RE.fullmatch(token) and token not in (".", ".."):
        return token
    log.warning("web: unusable cache-bust token %r; using 'dev'", token)
    return "dev"


def _templates() -> Jinja2Templates:
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=True,
        auto_reload=False,
    )
    return Jinja2Templates(env=env)


def make_app(
    config: WebConfig,
    *,
    users_db_path: Path,
    data_dir: Path,
    version_line: str,
    cache_bust: str,
    delete_telegram_message: Callable[[int, int], None] | None = None,
    users_lock: threading.Lock | threading.RLock | None = None,
    services_module: ModuleType | Any | None = None,
    clock: Callable[[], float] | None = None,
) -> FastAPI:
    """Build the web UI application.

    * ``config`` — a usable :class:`WebConfig` (an enabled config with problems, e.g. a
      broken IP allowlist, is refused with :class:`ConfigError`: fail closed, F59).
    * ``users_db_path`` — ``bot_users.db``; the app opens its **own** connections to it.
    * ``data_dir`` — the bot's data directory (per-user transactions DBs, csv_cache).
    * ``version_line`` / ``cache_bust`` — footer text and static-file version segment.
    * ``delete_telegram_message(chat_id, message_id)`` — called on a background thread
      after a successful sign-in to delete the message with the login link.
    * ``users_lock`` — held around the app's own writes to the ``users`` row
      (``auto_approve``) so they serialise with the bot's read-modify-write updates.
    * ``services_module`` — the service layer (default :mod:`ynab_helper.services`); tests
      may pass a stand-in exposing the same functions.
    * ``clock`` — epoch seconds (default :func:`time.time`): session expiry, "today" for
      the analytics, the purge thread.
    """
    problems = config.validate()
    if config.enabled and problems:
        raise ConfigError(
            "web UI configuration problems: " + "; ".join(problems),
            user_message="The web UI configuration has problems; see the log.",
        )
    if config.allowed_networks and str(config.trusted_proxies).strip() == "*":
        log.warning(
            "web: WEB_UI_ALLOWED_IPS is set while WEB_UI_TRUSTED_PROXIES is '*': a client "
            "that reaches port %s directly can claim an allowed address in X-Forwarded-For. "
            "Set WEB_UI_TRUSTED_PROXIES to the reverse proxy's address.",
            config.port,
        )
    hasher = config.token_hasher()
    # The bot migrates bot_users.db before it starts the web thread, so this is a no-op
    # there; standalone (devserver, tests) it guarantees the tables the app reads exist.
    open_users_db(Path(users_db_path), backup_dir=default_backup_dir(Path(data_dir))).close()
    services = (
        services_module
        if services_module is not None
        else importlib.import_module("ynab_helper.services")
    )
    now = clock or time.time
    # Rate limits measure intervals: monotonic unless a test injects its clock.
    ticks = clock or time.monotonic
    bust = _safe_cache_bust(cache_bust)
    state = WebState(
        config=config,
        hasher=hasher,
        users_db_path=Path(users_db_path),
        data_dir=Path(data_dir),
        version_line=str(version_line),
        cache_bust=bust,
        templates=_templates(),
        services=services,
        clock=now,
        auth_limiter=SlidingWindowLimiter(AUTH_ATTEMPTS_PER_MINUTE, 60.0, clock=ticks),
        sync_limiter=SlidingWindowLimiter(1, FORCED_SYNC_INTERVAL, clock=ticks),
        fresh_limiter=SlidingWindowLimiter(FRESH_READ_LIMIT, FRESH_READ_WINDOW, clock=ticks),
        syncer=BackgroundSyncer(lambda ctx: services.sync(ctx, force=False)),
        purger=SessionPurger(Path(users_db_path), clock=now),
        detached=DetachedRunner(name="telegram-delete-login"),
        delete_telegram_message=delete_telegram_message,
        users_lock=users_lock,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        state.purger.start()
        try:
            yield
        finally:
            state.syncer.close()
            state.purger.stop()

    app = FastAPI(
        title="YNAB Helper",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
        telemetry=_NO_TELEMETRY,  # type: ignore[arg-type]
    )
    app.state.web = state
    _install_error_handlers(app, config)
    include_routes(app)
    if STATIC_DIR.is_dir():
        # The versioned mount first: it is the more specific prefix.
        app.mount(f"/static/{bust}", StaticFiles(directory=STATIC_DIR), name="static-versioned")
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    else:  # pragma: no cover - the package always ships static files
        log.warning("web: static directory %s is missing", STATIC_DIR)

    # add_middleware puts each new middleware *outside* the previous ones, so the last
    # added is the outermost: body limit → allowlist → security headers → error boundary.
    app.add_middleware(ErrorBoundaryMiddleware)
    app.add_middleware(SecurityHeadersMiddleware, versioned_static_prefix=f"/static/{bust}/")
    app.add_middleware(IPAllowlistMiddleware, config=config)
    app.add_middleware(BodySizeLimitMiddleware)
    return app


def _install_error_handlers(app: FastAPI, config: WebConfig) -> None:
    """Every error leaves as ``{error, message}`` JSON (or a redirect for HTML pages)."""

    async def on_api_error(_request: Request, exc: Exception) -> Response:
        assert isinstance(exc, ApiError)
        return JSONResponse(exc.body(), status_code=exc.status, headers=exc.headers)

    async def on_login_required(_request: Request, exc: Exception) -> Response:
        assert isinstance(exc, LoginRequired)
        response: Response
        if exc.api:
            response = JSONResponse(
                {"error": "session_expired", "message": SESSION_EXPIRED_MESSAGE},
                status_code=401,
            )
        else:
            response = RedirectResponse("/?expired=1" if exc.had_cookie else "/", status_code=303)
        if exc.had_cookie:
            clear_session_cookie(response, config)
        return response

    async def on_service_error(_request: Request, exc: Exception) -> Response:
        assert isinstance(exc, YnabHelperError)
        error = api_error_for(exc)
        return JSONResponse(error.body(), status_code=error.status, headers=error.headers)

    async def on_http_error(_request: Request, exc: Exception) -> Response:
        assert isinstance(exc, StarletteHTTPException)
        names = {404: "not_found", 405: "method_not_allowed"}
        messages = {404: "Not found.", 405: "Method not allowed."}
        error = names.get(exc.status_code, "http_error")
        message = messages.get(exc.status_code) or (
            exc.detail if isinstance(exc.detail, str) else "Request failed."
        )
        return JSONResponse(
            {"error": error, "message": message},
            status_code=exc.status_code,
            headers=dict(exc.headers or {}),
        )

    async def on_validation_error(_request: Request, exc: Exception) -> Response:
        assert isinstance(exc, RequestValidationError)
        names = []
        for err in exc.errors():
            loc = err.get("loc") or ()
            if loc:
                names.append(str(loc[-1]))
        fields = ", ".join(sorted(set(names))) or "the request"
        return JSONResponse(
            {"error": "bad_request", "message": f"Invalid value for {fields}."},
            status_code=400,
        )

    async def on_body_too_large(_request: Request, exc: Exception) -> Response:
        assert isinstance(exc, BodyTooLarge)
        return JSONResponse(
            {"error": "too_large", "message": "The request is too large."},
            status_code=413,
            headers={"Connection": "close"},
        )

    app.add_exception_handler(ApiError, on_api_error)
    app.add_exception_handler(LoginRequired, on_login_required)
    app.add_exception_handler(YnabHelperError, on_service_error)
    app.add_exception_handler(StarletteHTTPException, on_http_error)
    app.add_exception_handler(RequestValidationError, on_validation_error)
    app.add_exception_handler(BodyTooLarge, on_body_too_large)
