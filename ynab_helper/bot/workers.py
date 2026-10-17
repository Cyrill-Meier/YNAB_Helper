"""Background workers for slow bot operations (DESIGN §8.1; F16, F43).

Imports, reconciles, dedupe deletions, cleanups, crypto syncs, status and onboarding calls
to YNAB run here, so the poll loop keeps answering every other user while one of them
waits for YNAB or a blockchain API.

:class:`WorkerPool` is a fixed pool of four threads like
``ThreadPoolExecutor(max_workers=4)``, with two differences that matter for the bot:

* **one job per user**: :meth:`WorkerPool.submit` refuses a second job for a key that
  already has one queued or running, so a user cannot fill the queue (the per-user service
  lock, :data:`ynab_helper.locks.LOCKS`, still guards against the web UI);
* **daemon threads**: ``ThreadPoolExecutor`` joins its threads at interpreter exit, which
  would hold a ``docker stop`` hostage to a minute-long xpub scan. Here
  :meth:`WorkerPool.shutdown` drops queued jobs, waits a bounded time for running ones and
  then lets the process exit; an interrupted import converges on the next run (DESIGN §1.2).
  It reports which jobs were dropped or cut off (:class:`ShutdownReport`) so the bot can
  tell those users to try again.

``workers=0`` runs every job inline in :meth:`submit` (deterministic tests).
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

__all__ = ["ShutdownReport", "WorkerPool"]

log = logging.getLogger(__name__)

DEFAULT_WORKERS: Final = 4


@dataclass(slots=True)
class _Job:
    key: str
    name: str
    fn: Callable[[], None]


@dataclass(frozen=True, slots=True)
class ShutdownReport:
    """What :meth:`WorkerPool.shutdown` left undone, as ``(key, job name)`` pairs.

    ``dropped`` jobs were queued and never started; ``abandoned`` ones were still running
    when the grace period ended. ``finished`` is True when nothing is left running.
    """

    finished: bool
    dropped: tuple[tuple[str, str], ...] = ()
    abandoned: tuple[tuple[str, str], ...] = ()


class WorkerPool:
    """Fixed-size daemon thread pool with at most one job per key."""

    def __init__(self, workers: int = DEFAULT_WORKERS, *, name: str = "bot-worker") -> None:
        if workers < 0:
            raise ValueError("workers must be >= 0")
        self._size = workers
        self._name = name
        self._queue: queue.Queue[_Job | None] = queue.Queue()
        self._lock = threading.Lock()
        self._inflight: dict[str, str] = {}
        self._outstanding = 0
        self._idle = threading.Condition(self._lock)
        self._threads: list[threading.Thread] = []
        self._accepting = True

    def start(self) -> None:
        with self._lock:
            if self._threads or self._size == 0 or not self._accepting:
                return
            for index in range(self._size):
                thread = threading.Thread(
                    target=self._work, name=f"{self._name}-{index + 1}", daemon=True
                )
                self._threads.append(thread)
                thread.start()

    def busy(self, key: str) -> str | None:
        """Name of the job queued or running for ``key``, or None."""
        with self._lock:
            return self._inflight.get(key)

    def submit(self, key: str, name: str, fn: Callable[[], None]) -> bool:
        """Queue ``fn`` for ``key``; False (not queued) if ``key`` already has a job or the
        pool is shutting down."""
        job = _Job(key, name, fn)
        if self._size:
            self.start()
        with self._lock:
            if not self._accepting or key in self._inflight:
                return False
            self._inflight[key] = name
            if self._size:
                # Queued under the lock, so a concurrent shutdown either drops it or waits
                # for it — never strands it behind the stop sentinels.
                self._outstanding += 1
                self._queue.put(job)
                return True
        self._run(job)
        return True

    def _work(self) -> None:
        while True:
            job = self._queue.get()
            if job is None:
                return
            self._run(job)
            with self._lock:
                self._outstanding -= 1
                if self._outstanding == 0:
                    self._idle.notify_all()

    def _run(self, job: _Job) -> None:
        started = time.monotonic()
        try:
            job.fn()
        except BaseException:  # a job must never kill its worker thread
            log.exception("bot job %s for %s failed", job.name, job.key)
        finally:
            with self._lock:
                self._inflight.pop(job.key, None)
            log.debug("bot job %s finished in %.1fs", job.name, time.monotonic() - started)

    def wait_idle(self, timeout: float) -> bool:
        """Wait until no job is queued or running (tests); False on timeout."""
        deadline = time.monotonic() + timeout
        with self._lock:
            while self._outstanding:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._idle.wait(remaining)
        return True

    def shutdown(self, timeout: float) -> ShutdownReport:
        """Stop accepting jobs, drop queued ones and wait up to ``timeout`` seconds for the
        running ones (after that their daemon threads are abandoned). Returns what was
        dropped or abandoned."""
        dropped: list[_Job] = []
        with self._lock:
            self._accepting = False
            while True:
                try:
                    job = self._queue.get_nowait()
                except queue.Empty:
                    break
                if job is not None:
                    dropped.append(job)
                    self._inflight.pop(job.key, None)
                    self._outstanding -= 1
            for _ in self._threads:
                self._queue.put(None)
        for job in dropped:
            log.warning("bot job %s for %s dropped at shutdown", job.name, job.key)
        finished = self.wait_idle(timeout)
        running: dict[str, str] = {}
        if not finished:
            with self._lock:
                running = dict(self._inflight)
            log.warning("shutdown: abandoning running bot jobs %s", running)
        return ShutdownReport(
            finished=finished,
            dropped=tuple((job.key, job.name) for job in dropped),
            abandoned=tuple(running.items()),
        )
