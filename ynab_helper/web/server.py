"""Run the web UI inside the bot process (docs/DESIGN.md §9.1; finding F94).

:func:`serve_in_thread` binds the listening socket in the caller's thread (a port that is
already taken fails right there, with the real ``OSError``), builds the app and runs
uvicorn on a daemon thread. It never raises: every startup problem — an unusable
configuration, a missing dependency, a bind failure, a crash later on — ends up in
:attr:`WebServerHandle.failed`, so the bot can tell the admin instead of handing out
``/login`` links to a dead server (v1's thread died silently on ``SystemExit``).

FastAPI and uvicorn are imported inside :func:`serve_in_thread`, so importing this module
is cheap and an import error is reported through the handle as well.

uvicorn runs with ``proxy_headers=True`` and ``forwarded_allow_ips`` from
``WEB_UI_TRUSTED_PROXIES`` (default ``*``: in production the container is only reachable
through Caddy, which sets ``X-Forwarded-For``), the lifespan on (it starts and stops the
session purge thread), ``log_config=None`` (records go to the app's logging setup) and no
``Server`` header.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

from ynab_helper.errors import ConfigError
from ynab_helper.web.config import WebConfig, web_disabled_reason

__all__ = ["WebServerHandle", "serve_in_thread"]

log = logging.getLogger(__name__)

#: Seconds uvicorn waits for open requests when stopping (the bot must exit within ~8 s).
GRACEFUL_SHUTDOWN: Final = 3.0
KEEP_ALIVE: Final = 5


class WebServerHandle:
    """Handle on the web server thread.

    * :attr:`started` — set once uvicorn accepts connections.
    * :attr:`failed` — the exception that kept the server from starting or ended it
      (``None`` while healthy or after a clean :meth:`stop`).
    * :meth:`wait_started` — wait until started (True) or failed/ended (False).
    * :meth:`stop` — graceful shutdown (``should_exit``), then join the thread.
    """

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.started = threading.Event()
        self.stopped = threading.Event()
        self.failed: BaseException | None = None
        self._server: Any = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        state = (
            "failed"
            if self.failed is not None
            else "running"
            if self.running
            else "started"
            if self.started.is_set()
            else "starting"
        )
        return f"WebServerHandle({self.host}:{self.port}, {state})"

    @property
    def running(self) -> bool:
        """Started, not failed and the thread is alive."""
        thread = self._thread
        return (
            self.started.is_set()
            and self.failed is None
            and thread is not None
            and thread.is_alive()
            and not self.stopped.is_set()
        )

    @property
    def url(self) -> str:
        host = self.host
        if host in ("0.0.0.0", "::", ""):
            host = "127.0.0.1"
        elif ":" in host:
            host = f"[{host}]"
        return f"http://{host}:{self.port}"

    def wait_started(self, timeout: float) -> bool:
        """Block until the server accepts connections (True) or failed/ended (False)."""
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            if self.started.is_set() and self.failed is None:
                return True
            if self.failed is not None or self.stopped.is_set():
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            self.started.wait(min(0.05, remaining))

    def stop(self, timeout: float = 5.0) -> None:
        """Ask uvicorn to exit, wait up to ``timeout`` s, then force it."""
        server = self._server
        if server is not None:
            server.should_exit = True
        thread = self._thread
        if thread is None or thread is threading.current_thread():
            self.stopped.set()
            return
        thread.join(max(0.0, timeout))
        if thread.is_alive() and server is not None:
            log.warning("web: server did not stop within %.1f s; forcing it", timeout)
            server.force_exit = True
            thread.join(1.0)
        if thread.is_alive():
            log.error("web: server thread is still running after stop()")
        else:
            self.stopped.set()

    # ── internal ──

    def _fail(self, exc: BaseException) -> None:
        with self._lock:
            if self.failed is None:
                self.failed = exc

    def _mark_started(self, port: int) -> None:
        self.port = port
        self.started.set()


def _bind(host: str, port: int) -> socket.socket:
    """Create, bind and return the listening socket (``OSError`` if the port is taken)."""
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM, flags=socket.AI_PASSIVE)
    if not infos:  # pragma: no cover - getaddrinfo raises instead
        raise OSError(f"cannot resolve {host!r}")
    family, kind, proto, _, address = infos[0]
    sock = socket.socket(family, kind, proto)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(address)
        sock.listen(128)
        sock.setblocking(False)
    except BaseException:
        sock.close()
        raise
    return sock


def serve_in_thread(
    config: WebConfig,
    *,
    users_db_path: Path,
    data_dir: Path,
    version_line: str,
    cache_bust: str,
    delete_telegram_message: Callable[[int, int], None] | None = None,
    users_lock: threading.Lock | threading.RLock | None = None,
) -> WebServerHandle:
    """Start the web UI on a daemon thread and return its handle (never raises).

    The bot waits with ``handle.wait_started(10)``; on False it reports
    ``handle.failed`` (or "did not start in time") to the admin and stops issuing login
    links. ``handle.stop()`` on shutdown.
    """
    handle = WebServerHandle(config.host, config.port)
    sock: socket.socket | None = None
    try:
        reason = web_disabled_reason(config, config.validate())
        if reason is not None:
            raise ConfigError(f"web UI not started: {reason}", user_message=reason)
        import uvicorn

        from ynab_helper.web.app import make_app

        app = make_app(
            config,
            users_db_path=Path(users_db_path),
            data_dir=Path(data_dir),
            version_line=version_line,
            cache_bust=cache_bust,
            delete_telegram_message=delete_telegram_message,
            users_lock=users_lock,
        )
        uv_config = uvicorn.Config(
            app,
            host=config.host,
            port=config.port,
            proxy_headers=True,
            forwarded_allow_ips=config.trusted_proxies,
            log_level="warning",
            log_config=None,
            access_log=False,
            lifespan="on",
            server_header=False,
            timeout_graceful_shutdown=int(GRACEFUL_SHUTDOWN),
            timeout_keep_alive=KEEP_ALIVE,
        )
        sock = _bind(config.host, config.port)
        handle.port = int(sock.getsockname()[1])

        class _Server(uvicorn.Server):
            async def startup(self, sockets: list[socket.socket] | None = None) -> None:
                await super().startup(sockets=sockets)
                if self.started:
                    handle._mark_started(handle.port)
                    log.info("web: UI listening on %s:%d", config.host, handle.port)

        server = _Server(uv_config)
    except BaseException as exc:  # F94: report, never die silently
        if sock is not None:
            sock.close()
        if isinstance(exc, KeyboardInterrupt):
            raise
        log.error("web: UI could not start: %s", _describe(exc))
        handle._fail(exc)
        handle.stopped.set()
        return handle

    assert sock is not None
    listening = sock

    def run() -> None:
        try:
            server.run(sockets=[listening])
        except BaseException as exc:  # SystemExit from uvicorn included (F94)
            log.error("web: UI server stopped with an error: %s", _describe(exc))
            handle._fail(exc)
        else:
            if not handle.started.is_set():
                handle._fail(RuntimeError("the web server exited before it started"))
        finally:
            try:
                listening.close()
            except OSError:  # pragma: no cover
                pass
            handle.stopped.set()

    thread = threading.Thread(target=run, name="web-ui", daemon=True)
    handle._server = server
    handle._thread = thread
    try:
        thread.start()
    except RuntimeError as exc:  # pragma: no cover - interpreter shutting down
        listening.close()
        handle._fail(exc)
        handle.stopped.set()
    return handle


def _describe(exc: BaseException) -> str:
    if isinstance(exc, SystemExit):
        return f"uvicorn exited (code {exc.code}); see the log above"
    text = getattr(exc, "user_message", None) or str(exc) or type(exc).__name__
    return f"{type(exc).__name__}: {text}"
