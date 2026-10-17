"""Pure ASGI middleware of the web UI and the session purge thread (docs/DESIGN.md §9.1).

Stack, outermost first (see :func:`ynab_helper.web.app.make_app`):

1. :class:`BodySizeLimitMiddleware` — rejects oversized requests with 413 **before any
   routing, authentication or parsing** (F21). The limit depends on the path
   (:func:`body_limit_for`): ``/api/upload`` 26 MiB, other ``/api/*`` 64 KiB, everything
   else 16 KiB. A declared ``Content-Length`` above the limit is refused without reading a
   byte. A body without ``Content-Length`` (chunked) is read up to the limit *here* for the
   small-body paths, and refused with 411 on ``/api/upload`` (browsers always send a
   length for a ``fetch`` body; an unauthenticated client must never make the server
   buffer megabytes). Streamed bytes are counted in every case, so a body can never
   exceed its limit even if a length header lies.
2. :class:`IPAllowlistMiddleware` — ``WebConfig.allows(client IP)``; the client IP is the
   one uvicorn's proxy-header handling put into the scope. Loopback is allowed only for
   requests without proxy headers (the health check), and a broken allowlist admits only
   that (F59). With ``WEB_UI_TRUSTED_PROXIES=*`` uvicorn takes the left-most
   ``X-Forwarded-For`` entry, which a client reaching the port directly controls: an
   allowlist is only meaningful with the proxy's address configured (``make_app`` warns).
3. :class:`SecurityHeadersMiddleware` — strict CSP, ``nosniff``, ``Referrer-Policy``,
   ``X-Frame-Options`` on every response, ``Cache-Control: no-store`` on everything except
   static files (F90); immutable caching for the versioned ``/static/<cache_bust>/`` path.
4. :class:`ErrorBoundaryMiddleware` — turns an unexpected exception into a logged 500
   ``{error, message}`` JSON response that still passes through the headers middleware.

Responses produced by the middleware themselves (413, 411, 400, 403, 500) carry the same
security headers and the ``{error, message}`` body shape as the API.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import re
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Final

from starlette.requests import ClientDisconnect
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ynab_helper import sessions
from ynab_helper.storage.db import connect
from ynab_helper.web.config import WebConfig

__all__ = [
    "API_BODY_LIMIT",
    "CONTENT_SECURITY_POLICY",
    "DEFAULT_BODY_LIMIT",
    "PURGE_INTERVAL",
    "STATIC_IMMUTABLE",
    "UPLOAD_BODY_LIMIT",
    "UPLOAD_PATH",
    "BodySizeLimitMiddleware",
    "BodyTooLarge",
    "ErrorBoundaryMiddleware",
    "IPAllowlistMiddleware",
    "SecurityHeadersMiddleware",
    "SessionPurger",
    "body_limit_for",
    "plain_error",
]

log = logging.getLogger(__name__)

UPLOAD_PATH: Final = "/api/upload"
#: Raw CSV upload: the statement limit is 25 MiB (parser), plus slack for the request.
UPLOAD_BODY_LIMIT: Final = 26 * 1024 * 1024
#: Every other ``/api/*`` route takes at most a small JSON object.
API_BODY_LIMIT: Final = 64 * 1024
#: Everything else (``POST /auth``, ``POST /logout`` forms; GETs have no body).
DEFAULT_BODY_LIMIT: Final = 16 * 1024
#: Paths whose (large) body is only read by the handler, after authentication.
STREAMING_PATHS: Final = frozenset({UPLOAD_PATH})

#: The strict policy of DESIGN §9.1 (the frontend is fully self-hosted, F23).
CONTENT_SECURITY_POLICY: Final = (
    "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
    "connect-src 'self'; font-src 'self'; frame-ancestors 'none'; base-uri 'self'; "
    "form-action 'self'; object-src 'none'"
)
STATIC_IMMUTABLE: Final = "public, max-age=31536000, immutable"
STATIC_REVALIDATE: Final = "no-cache"
NO_STORE: Final = "no-store"

_BASE_HEADERS: Final = (
    (b"content-security-policy", CONTENT_SECURITY_POLICY.encode("ascii")),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"same-origin"),
    (b"x-frame-options", b"DENY"),
)
_DIGITS_RE: Final = re.compile(rb"\A[0-9]{1,19}\Z")

#: How often expired sessions and login tokens are purged (seconds).
PURGE_INTERVAL: Final = 300.0


def body_limit_for(path: str) -> int:
    """The request-body limit (bytes) for ``path``."""
    if path == UPLOAD_PATH:
        return UPLOAD_BODY_LIMIT
    if path == "/api" or path.startswith("/api/"):
        return API_BODY_LIMIT
    return DEFAULT_BODY_LIMIT


class BodyTooLarge(Exception):
    """Raised from the wrapped ``receive`` when a streamed body passes its limit."""

    def __init__(self, limit: int) -> None:
        super().__init__(f"request body larger than {limit} bytes")
        self.limit = limit


def _security_headers(path: str) -> list[tuple[bytes, bytes]]:
    headers = list(_BASE_HEADERS)
    if not path.startswith("/static/"):
        headers += [(b"cache-control", NO_STORE.encode()), (b"pragma", b"no-cache")]
    return headers


async def plain_error(
    send: Send,
    status: int,
    error: str,
    message: str,
    *,
    path: str = "",
    extra_headers: Iterable[tuple[bytes, bytes]] = (),
) -> None:
    """Send a complete ``{error, message}`` JSON response with the security headers."""
    body = json.dumps({"error": error, "message": message}).encode("utf-8")
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
        *_security_headers(path),
        *extra_headers,
    ]
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body, "more_body": False})


def _header_values(scope: Scope, name: bytes) -> list[bytes]:
    return [value for key, value in scope.get("headers", ()) if key.lower() == name]


class BodySizeLimitMiddleware:
    """413 for request bodies over the per-path limit, before anything else runs (F21)."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        limit_for: Callable[[str], int] = body_limit_for,
        streaming_paths: frozenset[str] = STREAMING_PATHS,
    ) -> None:
        self.app = app
        self.limit_for = limit_for
        self.streaming_paths = streaming_paths

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        limit = self.limit_for(path)
        close = ((b"connection", b"close"),)

        lengths = {value.strip() for value in _header_values(scope, b"content-length")}
        declared: int | None = None
        if lengths:
            if len(lengths) != 1 or not _DIGITS_RE.match(next(iter(lengths))):
                await plain_error(
                    send,
                    400,
                    "bad_request",
                    "Invalid Content-Length.",
                    path=path,
                    extra_headers=close,
                )
                return
            declared = int(next(iter(lengths)))
            if declared > limit:
                await self._too_large(send, path, limit)
                return
        chunked = bool(_header_values(scope, b"transfer-encoding"))

        if chunked and path in self.streaming_paths:
            await plain_error(
                send,
                411,
                "length_required",
                "Send the file with a Content-Length header.",
                path=path,
                extra_headers=close,
            )
            return

        if declared is None and path not in self.streaming_paths:
            # No declared length: read the (small) body here, so the limit holds before any
            # authentication or parsing, then replay it to the app.
            try:
                buffered = await self._read_capped(receive, limit)
            except BodyTooLarge:
                await self._too_large(send, path, limit)
                return
            except ClientDisconnect:
                return
            receive = _replay(buffered, receive)

        counted = _CountingReceive(receive, limit)
        started = False

        async def tracking_send(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, counted, tracking_send)
        except BodyTooLarge:
            if started:
                raise
            await self._too_large(send, path, limit)

    @staticmethod
    async def _read_capped(receive: Receive, limit: int) -> list[Message]:
        messages: list[Message] = []
        total = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                raise ClientDisconnect()
            if message["type"] != "http.request":  # pragma: no cover - not sent by servers
                messages.append(message)
                continue
            total += len(message.get("body", b""))
            if total > limit:
                raise BodyTooLarge(limit)
            messages.append(message)
            if not message.get("more_body", False):
                return messages

    @staticmethod
    async def _too_large(send: Send, path: str, limit: int) -> None:
        await plain_error(
            send,
            413,
            "too_large",
            f"The request is too large (limit {_human_size(limit)}).",
            path=path,
            extra_headers=((b"connection", b"close"),),
        )


def _replay(messages: list[Message], receive: Receive) -> Receive:
    pending = list(messages)

    async def replay() -> Message:
        if pending:
            return pending.pop(0)
        return await receive()

    return replay


class _CountingReceive:
    """Wraps ``receive`` and raises :class:`BodyTooLarge` once the body passes ``limit``."""

    __slots__ = ("_limit", "_receive", "_total")

    def __init__(self, receive: Receive, limit: int) -> None:
        self._receive = receive
        self._limit = limit
        self._total = 0

    async def __call__(self) -> Message:
        message = await self._receive()
        if message["type"] == "http.request":
            self._total += len(message.get("body", b""))
            if self._total > self._limit:
                raise BodyTooLarge(self._limit)
        return message


def _human_size(size: int) -> str:
    if size >= 1024 * 1024:
        return f"{size / (1024 * 1024):g} MB"
    return f"{size // 1024} KB"


#: Headers a reverse proxy adds; their presence means the request did not originate here.
FORWARDING_HEADERS: Final = (b"x-forwarded-for", b"forwarded", b"x-real-ip")


def _is_loopback(ip: str | None) -> bool:
    if not ip:
        return False
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
        addr = addr.ipv4_mapped
    return addr.is_loopback


class IPAllowlistMiddleware:
    """403 unless ``config.allows(client IP)`` (fail closed, F59).

    The client IP is ``scope["client"]``, which uvicorn's proxy-header middleware (outside
    this app, configured with ``WEB_UI_TRUSTED_PROXIES``) has already rewritten from
    ``X-Forwarded-For``.

    :meth:`WebConfig.allows` always admits loopback so the container's health check works.
    That exemption is only granted to requests **without** forwarding headers: a loopback
    address that came out of ``X-Forwarded-For`` (spoofed by a client that reached the
    port directly while every peer is trusted, ``WEB_UI_TRUSTED_PROXIES=*``) or a local
    proxy relaying outside traffic must match the allowlist like any other address. The
    health check never sends such headers.
    """

    def __init__(self, app: ASGIApp, *, config: WebConfig) -> None:
        self.app = app
        self.config = config
        self._active = bool(config.allowed_networks) or config.allowlist_failed

    def admits(self, ip: str | None, *, forwarded: bool) -> bool:
        """Whether a request from ``ip`` (``forwarded``: it carried proxy headers) passes."""
        if not forwarded or not _is_loopback(ip):
            return self.config.allows(ip)
        if self.config.allowlist_failed:
            return False
        addr = ipaddress.ip_address(ip or "")
        if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
            addr = addr.ipv4_mapped
        return any(addr in network for network in self.config.allowed_networks)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not self._active:
            await self.app(scope, receive, send)
            return
        client = scope.get("client")
        ip = client[0] if client else None
        forwarded = any(_header_values(scope, name) for name in FORWARDING_HEADERS)
        if not self.admits(ip, forwarded=forwarded):
            log.info("web: refused a request from %s (not in WEB_UI_ALLOWED_IPS)", ip)
            await plain_error(
                send,
                403,
                "forbidden",
                "Access from this network is not allowed.",
                path=scope.get("path", ""),
            )
            return
        await self.app(scope, receive, send)


class SecurityHeadersMiddleware:
    """Adds the security headers of DESIGN §9.1 and the caching policy to every response.

    * CSP, ``X-Content-Type-Options: nosniff``, ``Referrer-Policy: same-origin`` and
      ``X-Frame-Options: DENY`` everywhere (a value set by a handler is kept).
    * ``Cache-Control: no-store`` (and ``Pragma: no-cache``) on every non-static response,
      which covers ``/app/*``, ``/api/*`` and ``/auth`` (F90: no financial page from the
      back/forward cache after logout).
    * ``/static/<cache_bust>/…`` successful responses: ``public, max-age=31536000,
      immutable``; other ``/static/…`` responses: ``no-cache`` (revalidate). Static error
      responses are ``no-store``.
    """

    def __init__(self, app: ASGIApp, *, versioned_static_prefix: str) -> None:
        self.app = app
        self.versioned_prefix = versioned_static_prefix.rstrip("/") + "/"

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                status = int(message["status"])
                headers = [
                    (k, v)
                    for k, v in message.get("headers", [])
                    if k.lower() not in (b"cache-control", b"pragma")
                ]
                present = {k.lower() for k, _ in headers}
                for name, value in _BASE_HEADERS:
                    if name not in present:
                        headers.append((name, value))
                headers += self._cache_headers(path, status)
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_with_headers)

    def _cache_headers(self, path: str, status: int) -> list[tuple[bytes, bytes]]:
        if path.startswith("/static/") and status in (200, 304):
            policy = (
                STATIC_IMMUTABLE if path.startswith(self.versioned_prefix) else STATIC_REVALIDATE
            )
            return [(b"cache-control", policy.encode())]
        return [(b"cache-control", NO_STORE.encode()), (b"pragma", b"no-cache")]


class ErrorBoundaryMiddleware:
    """Last line of defence: an unexpected exception becomes a logged 500 JSON response.

    Placed inside :class:`SecurityHeadersMiddleware`, so the 500 still carries the security
    headers (Starlette's own server-error handler sits outside every user middleware).
    :class:`BodyTooLarge` is passed on to :class:`BodySizeLimitMiddleware`; a client that
    disconnected gets nothing.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = False

        async def tracking_send(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, receive, tracking_send)
        except BodyTooLarge:
            raise
        except ClientDisconnect:
            log.debug("web: client disconnected during %s", scope.get("path"))
        except Exception:
            log.exception("web: unhandled error in %s %s", scope.get("method"), scope.get("path"))
            if started:
                raise
            await plain_error(
                send,
                500,
                "internal",
                "Something went wrong on the server. Please try again.",
                path=scope.get("path", ""),
            )


class SessionPurger:
    """Background thread deleting expired sessions and login tokens every ``interval`` s.

    Runs on its own thread with its own connection (never on the event loop). ``start`` is
    idempotent; ``stop`` wakes the thread and joins it.
    """

    def __init__(
        self,
        users_db_path: Path,
        *,
        interval: float = PURGE_INTERVAL,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.users_db_path = Path(users_db_path)
        self.interval = float(interval)
        self._clock = clock
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self.runs = 0
        self.purged = 0

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="web-session-purge", daemon=True)
            self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        with self._lock:
            thread, self._thread = self._thread, None
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)

    def purge_once(self) -> int:
        """Purge now (also used by tests); returns how many rows were removed."""
        conn: sqlite3.Connection | None = None
        try:
            conn = connect(self.users_db_path)
            removed = sessions.purge_expired(conn, now=self._clock())
        except (sqlite3.Error, OSError) as exc:
            log.warning("web: purging expired sessions failed: %s", exc)
            return 0
        finally:
            if conn is not None:
                conn.close()
        self.runs += 1
        self.purged += removed
        return removed

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.purge_once()
            except Exception:  # never let the thread die
                log.exception("web: session purge crashed")
