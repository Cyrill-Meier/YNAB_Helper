"""Work the web UI does off the request path (docs/DESIGN.md §9.1, §9.2).

* :class:`BackgroundSyncer` — read endpoints call :meth:`BackgroundSyncer.trigger`, which
  returns at once: if the user's last background sync started less than ``min_interval``
  seconds ago, or one is still running, nothing happens; otherwise a daemon thread runs
  ``services.sync(ctx, force=False)`` (which applies the mirror's own 30 s throttle and
  the 429 backoff). A GET therefore never waits for YNAB — it reads the mirror as it is
  and reports ``syncing: true`` so the page can refresh once the sync finished.
* :class:`DetachedRunner` — fire-and-forget calls such as deleting the Telegram ``/login``
  message after sign-in (F88), on daemon threads with a concurrency bound.

Daemon threads (not a ``ThreadPoolExecutor``) are deliberate: the interpreter joins
executor threads at exit, and a YNAB call stuck in its timeout must never delay the bot's
SIGTERM shutdown. SQLite rolls back an interrupted write.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from ynab_helper.errors import Busy, YnabAuthError, YnabHelperError
from ynab_helper.services.context import UserContext

__all__ = ["BackgroundSyncer", "DetachedRunner"]

log = logging.getLogger(__name__)

#: Seconds between two background syncs of the same user (the mirror's own throttle).
DEFAULT_MIN_INTERVAL = 30.0
_MAX_TRACKED = 1024


class BackgroundSyncer:
    """Throttled, single-flight background mirror sync per user."""

    def __init__(
        self,
        run: Callable[[UserContext], Any],
        *,
        min_interval: float = DEFAULT_MIN_INTERVAL,
        clock: Callable[[], float] = time.monotonic,
        max_concurrent: int = 4,
    ) -> None:
        self._run = run
        self.min_interval = float(min_interval)
        self._clock = clock
        self._lock = threading.Lock()
        self._last_start: dict[str, float] = {}
        self._running: dict[str, threading.Event] = {}
        self._slots = threading.BoundedSemaphore(max(1, max_concurrent))
        self._closed = False
        self.started_count = 0

    def trigger(self, ctx: UserContext) -> bool:
        """Start a background sync for ``ctx`` unless throttled; True if one is running now."""
        key = ctx.key
        now = self._clock()
        with self._lock:
            if key in self._running:
                return True
            if self._closed:
                return False
            last = self._last_start.get(key)
            if last is not None and now - last < self.min_interval:
                return False
            if not self._slots.acquire(blocking=False):
                return False
            if len(self._last_start) >= _MAX_TRACKED:
                oldest = min(self._last_start, key=self._last_start.__getitem__)
                del self._last_start[oldest]
            self._last_start[key] = now
            done = threading.Event()
            self._running[key] = done
            self.started_count += 1
        thread = threading.Thread(target=self._work, args=(ctx, done), name="web-sync", daemon=True)
        try:
            thread.start()
        except RuntimeError:  # pragma: no cover - interpreter shutting down
            self._finish(key, done)
            return False
        return True

    def running(self, key: str) -> bool:
        with self._lock:
            return key in self._running

    def wait_idle(self, key: str, timeout: float) -> bool:
        """Wait until no background sync of ``key`` runs (True) or ``timeout`` passes."""
        with self._lock:
            done = self._running.get(key)
        return True if done is None else done.wait(timeout)

    def close(self) -> None:
        """Start no further syncs (running ones finish on their own)."""
        with self._lock:
            self._closed = True

    def _work(self, ctx: UserContext, done: threading.Event) -> None:
        try:
            self._run(ctx)
        except Busy:
            log.debug("web: background sync of user %s skipped: busy", ctx.key)
        except YnabAuthError:
            log.warning("web: background sync of user %s: YNAB rejected the token", ctx.key)
        except YnabHelperError as exc:
            log.info("web: background sync of user %s failed: %s", ctx.key, exc.user_message)
        except Exception:
            log.exception("web: background sync of user %s crashed", ctx.key)
        finally:
            self._finish(ctx.key, done)

    def _finish(self, key: str, done: threading.Event) -> None:
        with self._lock:
            if self._running.get(key) is done:
                del self._running[key]
        self._slots.release()
        done.set()


class DetachedRunner:
    """Runs fire-and-forget callables on daemon threads, at most ``max_concurrent`` at once.

    A call over the bound is dropped with a log line rather than queued: these are
    best-effort clean-ups (the Telegram login message), never needed for correctness.
    Exceptions are logged, never raised.
    """

    def __init__(self, *, max_concurrent: int = 4, name: str = "web-detached") -> None:
        self._slots = threading.BoundedSemaphore(max(1, max_concurrent))
        self._name = name

    def submit(self, fn: Callable[..., Any], *args: Any) -> threading.Thread | None:
        if not self._slots.acquire(blocking=False):
            log.warning("web: dropped a background task (%s): too many running", self._name)
            return None

        def work() -> None:
            try:
                fn(*args)
            except Exception:
                log.warning("web: background task %s failed", self._name, exc_info=True)
            finally:
                self._slots.release()

        thread = threading.Thread(target=work, name=self._name, daemon=True)
        try:
            thread.start()
        except RuntimeError:  # pragma: no cover - interpreter shutting down
            self._slots.release()
            return None
        return thread
