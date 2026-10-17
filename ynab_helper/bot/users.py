"""The bot's access to ``bot_users.db``: one shared connection guarded by one lock.

The poll loop and the worker threads share a single ``check_same_thread=False`` connection
(DESIGN §4.4). Every :class:`~ynab_helper.storage.UserStore` /
:class:`~ynab_helper.storage.SessionStore` call and every :mod:`ynab_helper.sessions` call
gets the same re-entrant lock, so their transactions never interleave, and the
check-then-write helpers here (:meth:`UsersRepo.update_if`,
:meth:`UsersRepo.register_pending`) hold it across the read and the write: a worker that
finishes an onboarding step can never overwrite a ``denied`` the admin set meanwhile.

The web server opens its own connections; SQLite's locking (``busy_timeout``) orders the
two processes' writes.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections.abc import Collection
from typing import Any

from ynab_helper import sessions
from ynab_helper.sessions import TokenHasher
from ynab_helper.storage.users import SessionStore, User, UserStore

__all__ = ["UsersRepo", "flow_of"]

log = logging.getLogger(__name__)

#: Version tag of the ``temp_data`` JSON written by this bot (v1 stored bare lists).
TEMP_VERSION = 2


def flow_of(user: User | None) -> dict[str, Any] | None:
    """The user's in-progress flow from ``temp_data`` (None if absent or a v1 leftover)."""
    if user is None:
        return None
    temp = user.temp
    if isinstance(temp, dict) and temp.get("v") == TEMP_VERSION:
        return temp
    return None


class UsersRepo:
    """Users and web sessions on the bot's shared connection."""

    def __init__(self, conn: sqlite3.Connection, lock: threading.RLock | None = None) -> None:
        self.conn = conn
        self.lock = lock if lock is not None else threading.RLock()
        self.store = UserStore(conn, lock=self.lock)
        self.sessions = SessionStore(conn, lock=self.lock)

    # ── users ──

    def get(self, telegram_id: int) -> User | None:
        return self.store.get(telegram_id)

    def list_all(self) -> list[User]:
        return self.store.list_all()

    def update(self, telegram_id: int, **fields: Any) -> User:
        return self.store.upsert(telegram_id, **fields)

    def register_pending(
        self,
        telegram_id: int,
        *,
        chat_id: int,
        username: str | None,
        first_name: str | None,
    ) -> tuple[User, bool]:
        """Insert an unknown sender as ``pending``; ``(user, created)``. Atomic."""
        with self.lock:
            existing = self.store.get(telegram_id)
            if existing is not None:
                return existing, False
            user = self.store.upsert(
                telegram_id,
                chat_id=chat_id,
                username=username,
                first_name=first_name,
                state="pending",
            )
            return user, True

    def update_if(
        self,
        telegram_id: int,
        *,
        states: Collection[str],
        flow: str | None = None,
        **fields: Any,
    ) -> User | None:
        """Write ``fields`` only if the user is still in one of ``states`` (and, with
        ``flow``, still in that flow); returns the updated user or None (nothing written)."""
        with self.lock:
            user = self.store.get(telegram_id)
            if user is None or user.state not in states:
                return None
            if flow is not None:
                current = flow_of(user)
                if current is None or current.get("flow") != flow:
                    return None
            return self.store.upsert(telegram_id, **fields)

    # ── web sessions ──

    def revoke_sessions(self, telegram_id: int) -> int:
        """Delete every web session and unused login link of the user (F18)."""
        try:
            return sessions.revoke_all(self.conn, telegram_id, lock=self.lock)
        except sqlite3.Error:
            log.exception("could not revoke the web sessions of user %s", telegram_id)
            raise

    def issue_login_token(self, hasher: TokenHasher, telegram_id: int, *, ttl_seconds: int) -> str:
        return sessions.issue_login_token(
            self.conn,
            hasher,
            telegram_id,
            ttl_seconds=ttl_seconds,
            ip="bot",
            user_agent="telegram",
            lock=self.lock,
        )

    def discard_login_token(self, hasher: TokenHasher, token: str) -> None:
        """Burn an unused login token (its message could not be sent). Nothing happens
        if it is unknown or already used."""
        self.sessions.consume_login(hasher.hash(token), time.time())

    def attach_login_message(
        self, hasher: TokenHasher, token: str, *, chat_id: int, message_id: int
    ) -> bool:
        return sessions.attach_tg_message(
            self.conn, hasher, token, chat_id=chat_id, message_id=message_id, lock=self.lock
        )
