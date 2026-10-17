"""Per-user operation locks shared by the bot, the web UI and the CLI (§4.5, F16).

Only one mutating operation (import, reconcile, dedupe delete, cleanup, crypto, sync) may
run per user at a time; a second one is refused immediately with :class:`errors.Busy`
naming the operation in progress, instead of queueing behind it.

Locks are deliberately **not** re-entrant: a re-entrant lock owned by the event-loop
thread would let two concurrent async requests both "acquire" it. Service code that
composes operations must call unlocked internal helpers under a single outer lock.

Keys: ``str(telegram_id)`` for bot/web users, the database path for the CLI. Ints and
path-like objects are normalised to ``str`` so ``123`` and ``"123"`` are the same lock.
"""

from __future__ import annotations

import itertools
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from ynab_helper.errors import Busy

LockKey = str | int | os.PathLike[str]


@dataclass(frozen=True, slots=True)
class LockToken:
    """Proof of ownership returned by :meth:`UserLocks.acquire`; pass it to ``release``."""

    key: str
    name: str
    serial: int
    acquired_at: float = field(compare=False)  # time.monotonic()


def _normalise_key(key: LockKey) -> str:
    if isinstance(key, bool):
        raise TypeError("lock key must be a str, int or path, not bool")
    if isinstance(key, int):
        text = str(key)
    elif isinstance(key, str | os.PathLike):
        text = os.fspath(key)
        if not isinstance(text, str):
            raise TypeError("lock key path must be a str path")
    else:
        raise TypeError(f"lock key must be a str, int or path, not {type(key).__name__}")
    if not text:
        raise ValueError("lock key must not be empty")
    return text


class UserLocks:
    """Thread-safe registry of non-blocking, per-key operation locks."""

    def __init__(self) -> None:
        self._mutex = threading.Lock()
        self._held: dict[str, LockToken] = {}
        self._serials = itertools.count(1)

    def acquire(self, key: LockKey, name: str) -> LockToken:
        """Take the lock for ``key`` or raise ``Busy(<name of the running operation>)``.

        Use this pair (with :meth:`release`) when the lock must outlive one stack frame,
        e.g. acquired in a request handler and released by a worker thread; otherwise
        prefer :meth:`operation`.
        """
        normalised = _normalise_key(key)
        if not isinstance(name, str) or not name:
            raise ValueError("operation name must be a non-empty str")
        with self._mutex:
            holder = self._held.get(normalised)
            if holder is not None:
                raise Busy(holder.name)
            token = LockToken(normalised, name, next(self._serials), time.monotonic())
            self._held[normalised] = token
            return token

    def release(self, token: LockToken) -> None:
        """Release a lock taken with :meth:`acquire`.

        Raises ``RuntimeError`` if ``token`` does not own the lock (double release, or a
        token from an earlier acquisition) — that is always a programming error.
        """
        with self._mutex:
            if self._held.get(token.key) is not token:
                raise RuntimeError(f"lock {token.key!r} is not held by this token")
            del self._held[token.key]

    @contextmanager
    def operation(self, key: LockKey, name: str) -> Iterator[None]:
        """``with LOCKS.operation(key, "import"):`` — non-blocking; raises ``Busy``.

        The lock is released when the block exits, including by an exception.
        """
        token = self.acquire(key, name)
        try:
            yield
        finally:
            self.release(token)

    def current(self, key: LockKey) -> str | None:
        """Name of the operation currently holding ``key``, or None."""
        normalised = _normalise_key(key)
        with self._mutex:
            holder = self._held.get(normalised)
            return holder.name if holder is not None else None

    def snapshot(self) -> dict[str, str]:
        """``{key: operation name}`` of every held lock (diagnostics)."""
        with self._mutex:
            return {key: token.name for key, token in self._held.items()}


LOCKS = UserLocks()
"""Process-wide singleton shared by the bot and the web UI."""
