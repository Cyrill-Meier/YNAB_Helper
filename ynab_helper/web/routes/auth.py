"""Sign-in and sign-out (docs/DESIGN.md §9.1; findings F24, F61, F86, F87, F88).

* ``GET /`` — the login page, unless the session cookie belongs to a **valid** session
  (then 303 to the dashboard). A stale cookie is cleared and the page says the session
  expired (F24: v1 redirected any cookie into a raw 401).
* ``GET /auth?t=<token>`` — never consumes the token (F88: link unfurlers and prefetchers
  issue GETs). It renders a tiny page whose form POSTs the token back; ``/static/js/auth.js``
  submits it automatically and a visible "Continue" button is the fallback. When the
  browser already has a valid session the page is a "switch account?" confirmation that is
  **not** auto-submitted (``confirm_switch``): a link someone else sent must not silently
  swap the victim into the sender's budget.
* ``POST /auth`` — same-origin check (stops a direct cross-site POST; the interstitial
  above is what guards against a forwarded link), bounded per-client rate limit of 10/min
  (F87; IPv6 clients per /64), then :func:`ynab_helper.sessions.consume_login_token`, which
  burns the token atomically (F86) and only signs in a ``ready`` user (F18). Success sets
  the ``rynab_session`` cookie and redirects (303) to ``/app/dashboard``; the Telegram
  message carrying the link is deleted on a background thread.
* ``POST /logout`` — requires the CSRF token (header or ``csrf_token`` form field, F61),
  deletes the session and clears the cookie. A plain form post with an outdated token
  (the browser signed in again in another tab) is answered with a redirect to a freshly
  rendered page and a notice, never with raw JSON; ``fetch`` callers (header) get the JSON
  403.
"""

from __future__ import annotations

import logging
import re
from contextlib import closing
from typing import Final
from urllib.parse import parse_qs, urlsplit

from fastapi import APIRouter
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from ynab_helper import sessions
from ynab_helper.sessions import SESSION_COOKIE, LoginResult
from ynab_helper.storage.db import connect
from ynab_helper.web.config import WebConfig
from ynab_helper.web.deps import (
    CSRF_FORM_FIELD,
    CSRF_HEADER,
    ApiError,
    WebState,
    clear_session_cookie,
    client_ip,
    get_state,
    read_body,
    render,
    resolve_session,
    set_session_cookie,
)
from ynab_helper.web.middleware import DEFAULT_BODY_LIMIT
from ynab_helper.web.ratelimit import client_key

__all__ = ["router"]

log = logging.getLogger(__name__)

router = APIRouter()

DASHBOARD: Final = "/app/dashboard"
#: Login tokens are ``secrets.token_urlsafe(32)``; anything else is not rendered back.
_TOKEN_RE: Final = re.compile(r"[A-Za-z0-9_-]{16,256}")
_FORM_TYPE: Final = "application/x-www-form-urlencoded"

MSG_LOGIN: Final = "Open Telegram and send /login to the bot to get a one-time sign-in link."
MSG_BAD_LINK: Final = (
    "This sign-in link has expired or was already used. Send /login to the bot for a new one."
)
MSG_RATE_LIMITED: Final = "Too many sign-in attempts. Wait a minute and try again."
MSG_CROSS_SITE: Final = "This sign-in request did not come from this site. Open the link again."
MSG_SWITCH: Final = (
    "You are already signed in. Continue only if you requested this sign-in link yourself: "
    "it may sign you in to a different account."
)


@router.get("/health", include_in_schema=False)
def health() -> dict[str, bool]:
    """Liveness probe for the Docker health check (loopback is always allowed)."""
    return {"ok": True}


@router.get("/", include_in_schema=False)
def login_page(request: Request, expired: str | None = None) -> Response:
    """The login page, or the dashboard when the cookie belongs to a valid session."""
    state = get_state(request)
    token = request.cookies.get(SESSION_COOKIE)
    if token and resolve_session(state, token) is not None:
        return RedirectResponse(DASHBOARD, status_code=303)
    response = render(
        state,
        request,
        "login.html",
        page="login",
        expired=bool(token) or expired == "1",
        message=MSG_LOGIN,
    )
    if token:
        clear_session_cookie(response, state.config)
    return response


@router.get("/auth", include_in_schema=False)
def auth_page(request: Request, t: str | None = None) -> Response:
    """Interstitial that POSTs the login token (the GET itself changes nothing, F88).

    With a valid session already in this browser, the page asks before switching
    (``confirm_switch``; not auto-submitted) and names the signed-in user.
    """
    state = get_state(request)
    if not t:
        return RedirectResponse("/", status_code=303)
    if not _TOKEN_RE.fullmatch(t):
        return _login_error(state, request, MSG_BAD_LINK, 400)
    current = resolve_session(state, request.cookies.get(SESSION_COOKIE))
    return render(
        state,
        request,
        "auth.html",
        page="auth",
        authed=current,
        login_token=t,
        confirm_switch=current is not None,
        message=MSG_SWITCH if current is not None else None,
    )


@router.post("/auth", include_in_schema=False)
async def auth_submit(request: Request) -> Response:
    """Exchange the one-time login token for a session cookie."""
    state = get_state(request)
    ip = client_ip(request) or "unknown"
    if not same_origin(request, state.config):
        log.warning("web: refused a cross-site sign-in from %s", ip)
        return _login_error(state, request, MSG_CROSS_SITE, 403)
    key = client_key(ip)
    if not state.auth_limiter.allow(key):
        log.warning("web: sign-in rate limit hit for %s", key)
        wait = max(1, int(state.auth_limiter.retry_after(key) + 0.999))
        return _login_error(
            state, request, MSG_RATE_LIMITED, 429, headers={"Retry-After": str(wait)}
        )
    form = await read_form(request)
    token = form.get("t", "")
    user_agent = request.headers.get("user-agent", "")[:200]
    result = await run_in_threadpool(_consume, state, token, ip, user_agent)
    if result is None:
        return _login_error(state, request, MSG_BAD_LINK, 400)

    old = request.cookies.get(SESSION_COOKIE)
    if old:
        await run_in_threadpool(_delete_session, state, old)
    response = RedirectResponse(DASHBOARD, status_code=303)
    set_session_cookie(response, state.config, result.session_token)
    _schedule_message_deletion(state, result)
    return response


@router.post("/logout", include_in_schema=False)
async def logout(request: Request) -> Response:
    """End this browser's session (CSRF-protected, F61) and return to the login page."""
    state = get_state(request)
    token = request.cookies.get(SESSION_COOKIE)
    response = RedirectResponse("/", status_code=303)
    if not token:
        return response
    candidate = request.headers.get(CSRF_HEADER)
    from_form = candidate is None
    if from_form:
        candidate = (await read_form(request)).get(CSRF_FORM_FIELD)
    if not state.hasher.verify_csrf(token, candidate):
        if not from_form:
            raise ApiError(403, "csrf_failed", "The page is out of date. Reload it and try again.")
        return await run_in_threadpool(_stale_form_logout, state, token)
    await run_in_threadpool(_delete_session, state, token)
    clear_session_cookie(response, state.config)
    return response


def _stale_form_logout(state: WebState, token: str) -> Response:
    """A logout form with an outdated CSRF token (F24/F62: never a raw JSON page).

    The session stays (the request may be forged), and the browser lands on a freshly
    rendered page whose logout form carries the current token, with a notice. A cookie
    that no longer belongs to a valid session is simply cleared: there is nothing left
    to protect.
    """
    if resolve_session(state, token) is None:
        response = RedirectResponse("/", status_code=303)
        clear_session_cookie(response, state.config)
        return response
    return RedirectResponse(f"{DASHBOARD}?notice=logout_stale", status_code=303)


# ── helpers ──────────────────────────────────────────────────────────────────


def _login_error(
    state: WebState,
    request: Request,
    message: str,
    status: int,
    *,
    headers: dict[str, str] | None = None,
) -> Response:
    return render(
        state,
        request,
        "login.html",
        page="login",
        message=message,
        error=True,
        status_code=status,
        headers=headers,
    )


async def read_form(request: Request) -> dict[str, str]:
    """A small urlencoded form body (first value per field). No multipart (F22).

    Anything that is not ``application/x-www-form-urlencoded`` yields an empty form.
    """
    media = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if media != _FORM_TYPE:
        return {}
    body = await read_body(request, DEFAULT_BODY_LIMIT)
    try:
        text = body.decode("ascii")
        parsed = parse_qs(text, keep_blank_values=True, max_num_fields=16)
    except (UnicodeDecodeError, ValueError):
        return {}
    return {key: values[0] for key, values in parsed.items() if values}


def _origin_of(url: str) -> str | None:
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        return None
    if not parts.scheme or not parts.hostname:
        return None
    host = parts.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    default = {"http": 80, "https": 443}.get(parts.scheme.lower())
    suffix = "" if port is None or port == default else f":{port}"
    return f"{parts.scheme.lower()}://{host}{suffix}"


def same_origin(request: Request, config: WebConfig) -> bool:
    """Whether a POST comes from this site (``POST /auth``).

    This stops a *direct* cross-site POST of a login token (a page elsewhere submitting a
    form to ``/auth``). It cannot tell who sent a link: a forwarded ``/auth?t=…`` link is
    answered by our own interstitial, whose POST is same-origin; that case is handled by
    the interstitial's "switch account?" confirmation (:func:`auth_page`).

    A browser always sends ``Origin`` on a cross-site POST; it must equal the public URL's
    origin or the origin the request was addressed to. Without ``Origin`` the request must
    not be marked cross-site by ``Sec-Fetch-Site``; non-browser clients send neither.

    ``Origin: null`` is what a browser sends for a form POST from a page whose referrer
    policy is ``no-referrer`` — but also from a sandboxed (attacker) frame. It is accepted
    only together with ``Sec-Fetch-Site: same-origin``, which the browser sets itself.
    """
    origin = request.headers.get("origin")
    site = request.headers.get("sec-fetch-site")
    if origin is None:
        return site in (None, "same-origin", "none")
    if origin.strip().lower() == "null":
        return site == "same-origin"
    actual = _origin_of(origin)
    if actual is None:
        return False
    allowed = {_origin_of(config.public_url)} if config.public_url else set()
    host = request.headers.get("host")
    if host:
        allowed.add(_origin_of(f"{request.url.scheme}://{host}"))
    return actual in allowed


def _consume(state: WebState, token: str, ip: str, user_agent: str) -> LoginResult | None:
    with closing(connect(state.users_db_path)) as conn:
        return sessions.consume_login_token(
            conn,
            state.hasher,
            token,
            session_ttl=state.config.session_ttl,
            absolute_ttl=state.config.session_absolute_ttl,
            ip=ip,
            user_agent=user_agent,
            now=state.clock(),
        )


def _delete_session(state: WebState, token: str) -> None:
    with closing(connect(state.users_db_path)) as conn:
        sessions.delete_session(conn, state.hasher, token)


def _schedule_message_deletion(state: WebState, result: LoginResult) -> None:
    """Delete the Telegram message with the (now used) login link, in the background."""
    callback = state.delete_telegram_message
    if callback is None or result.tg_chat_id is None or result.tg_message_id is None:
        return
    state.detached.submit(callback, result.tg_chat_id, result.tg_message_id)
