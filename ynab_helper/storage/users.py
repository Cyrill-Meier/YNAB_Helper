"""Bot users and web sessions (``bot_users.db``).

:class:`UserStore` owns the ``users`` table, :class:`SessionStore` the ``web_sessions`` table.
Both accept an optional ``lock``: the bot shares one connection between threads
(``check_same_thread=False``), and then every store on that connection must get the *same*
lock so their transactions cannot interleave.

Token hashing is not done here — ``web/auth.py`` computes ``HMAC-SHA256(secret, token)`` and
the store only ever sees the hash.
"""

from __future__ import annotations

import contextlib
import json
import logging
import sqlite3
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Final

from ynab_helper.storage.db import RecordNotFound, transaction
from ynab_helper.timeutil import utc_now_iso

log = logging.getLogger(__name__)

#: States a user can be put in. The legacy ``approved`` state is migrated to
#: ``awaiting_token`` and can no longer be written.
USER_STATES: Final = frozenset(
    {
        "pending",
        "awaiting_token",
        "awaiting_budget",
        "awaiting_account",
        "ready",
        "denied",
        "awaiting_crypto_account",
        "awaiting_crypto_btc",
        "awaiting_crypto_eth",
    }
)
SESSION_KINDS: Final = frozenset({"login", "session"})
USER_AGENT_MAX = 200
IP_MAX = 64

_INT_FIELDS = frozenset({"chat_id"})
_STR_FIELDS = frozenset(
    {
        "username",
        "first_name",
        "ynab_token",
        "budget_id",
        "budget_name",
        "account_id",
        "account_name",
        "crypto_account_id",
        "crypto_account_name",
        "btc_xpub",
        "eth_address",
    }
)
#: Columns :meth:`UserStore.upsert` may write (telegram_id and timestamps are managed).
USER_FIELDS: Final = _INT_FIELDS | _STR_FIELDS | {"state", "temp_data", "auto_approve"}

_KEEP: Any = object()


@dataclass(frozen=True, slots=True)
class User:
    """A ``users`` row. Secrets are excluded from ``repr`` so they never reach a log line."""

    telegram_id: int
    chat_id: int | None
    username: str | None
    first_name: str | None
    ynab_token: str | None = field(repr=False)
    budget_id: str | None
    budget_name: str | None
    account_id: str | None
    account_name: str | None
    crypto_account_id: str | None
    crypto_account_name: str | None
    btc_xpub: str | None = field(repr=False)
    eth_address: str | None = field(repr=False)
    state: str
    temp_data: str | None = field(repr=False)
    created_at: str
    updated_at: str
    auto_approve: bool

    @property
    def is_ready(self) -> bool:
        """Fully onboarded: state ``ready`` with token, budget and account set."""
        return bool(
            self.state == "ready" and self.ynab_token and self.budget_id and self.account_id
        )

    @property
    def temp(self) -> Any:
        """``temp_data`` decoded from JSON (None if empty or not valid JSON)."""
        if not self.temp_data:
            return None
        try:
            return json.loads(self.temp_data)
        except ValueError:
            return None


_USER_COLUMNS = (
    "telegram_id",
    "chat_id",
    "username",
    "first_name",
    "ynab_token",
    "budget_id",
    "budget_name",
    "account_id",
    "account_name",
    "crypto_account_id",
    "crypto_account_name",
    "btc_xpub",
    "eth_address",
    "state",
    "temp_data",
    "created_at",
    "updated_at",
    "auto_approve",
)
_USER_SELECT = f"SELECT {', '.join(_USER_COLUMNS)} FROM users"


def _user(row: sqlite3.Row) -> User:
    values = {name: row[name] for name in _USER_COLUMNS}
    values["auto_approve"] = bool(values["auto_approve"])
    return User(**values)


def _check_telegram_id(telegram_id: Any) -> int:
    if isinstance(telegram_id, bool) or not isinstance(telegram_id, int):
        raise ValueError(f"telegram_id must be an int, got {telegram_id!r}")
    return telegram_id


def _normalise_user_field(name: str, value: Any) -> Any:
    if name == "state":
        if value not in USER_STATES:
            raise ValueError(f"unknown user state {value!r}")
        return value
    if name == "auto_approve":
        if not isinstance(value, bool | int) or value not in (0, 1):
            raise ValueError(f"auto_approve must be a bool, got {value!r}")
        return int(value)
    if name == "temp_data":
        if value is None or isinstance(value, str):
            return value
        return json.dumps(value, separators=(",", ":"))
    if name in _INT_FIELDS:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} must be an int, got {value!r}")
        return value
    if value is not None and not isinstance(value, str):
        raise ValueError(f"{name} must be a string, got {type(value).__name__}")
    return value


class _Locked:
    def __init__(self, lock: threading.Lock | threading.RLock | None) -> None:
        self._lock = lock

    @contextlib.contextmanager
    def held(self) -> Iterator[None]:
        if self._lock is None:
            yield
            return
        with self._lock:
            yield


class UserStore:
    """The ``users`` table."""

    def __init__(
        self, conn: sqlite3.Connection, *, lock: threading.Lock | threading.RLock | None = None
    ) -> None:
        self._conn = conn
        self._guard = _Locked(lock)

    def get(self, telegram_id: int) -> User | None:
        with self._guard.held():
            row = self._conn.execute(
                f"{_USER_SELECT} WHERE telegram_id = ?", (_check_telegram_id(telegram_id),)
            ).fetchone()
        return _user(row) if row else None

    def list_all(self) -> list[User]:
        """Every user, oldest first."""
        with self._guard.held():
            rows = self._conn.execute(f"{_USER_SELECT} ORDER BY created_at, telegram_id").fetchall()
        return [_user(r) for r in rows]

    def upsert(self, telegram_id: int, /, **fields: Any) -> User:
        """Create the user (state ``pending`` unless given) or update only the given fields.

        Allowed fields: :data:`USER_FIELDS`. ``temp_data`` accepts a JSON string or a
        JSON-serialisable value. Returns the stored user.
        """
        tid = _check_telegram_id(telegram_id)
        unknown = set(fields) - USER_FIELDS
        if unknown:
            raise ValueError(f"unknown user fields: {sorted(unknown)}")
        values = {name: _normalise_user_field(name, value) for name, value in fields.items()}
        now = utc_now_iso()
        with self._guard.held(), transaction(self._conn):
            exists = self._conn.execute(
                "SELECT 1 FROM users WHERE telegram_id = ?", (tid,)
            ).fetchone()
            if exists:
                if values:
                    assignments = ", ".join(f"{name} = ?" for name in values)
                    self._conn.execute(
                        f"UPDATE users SET {assignments}, updated_at = ? WHERE telegram_id = ?",
                        (*values.values(), now, tid),
                    )
            else:
                values.setdefault("state", "pending")
                columns = ["telegram_id", *values, "created_at", "updated_at"]
                self._conn.execute(
                    f"INSERT INTO users ({', '.join(columns)}) "
                    f"VALUES ({', '.join('?' for _ in columns)})",
                    (tid, *values.values(), now, now),
                )
            row = self._conn.execute(f"{_USER_SELECT} WHERE telegram_id = ?", (tid,)).fetchone()
        return _user(row)

    def set_state(self, telegram_id: int, state: str, *, temp_data: Any = _KEEP) -> User:
        """Change a user's state (and optionally ``temp_data``; omitted = unchanged).

        Raises :class:`RecordNotFound` if the user does not exist (states are never set on a
        user that was not created first).
        """
        tid = _check_telegram_id(telegram_id)
        values = {"state": _normalise_user_field("state", state)}
        if temp_data is not _KEEP:
            values["temp_data"] = _normalise_user_field("temp_data", temp_data)
        assignments = ", ".join(f"{name} = ?" for name in values)
        with self._guard.held(), transaction(self._conn):
            changed = self._conn.execute(
                f"UPDATE users SET {assignments}, updated_at = ? WHERE telegram_id = ?",
                (*values.values(), utc_now_iso(), tid),
            ).rowcount
            if not changed:
                raise RecordNotFound(f"no user {tid}")
            row = self._conn.execute(f"{_USER_SELECT} WHERE telegram_id = ?", (tid,)).fetchone()
        return _user(row)


@dataclass(frozen=True, slots=True)
class WebSession:
    """A ``web_sessions`` row (login token or browser session), identified by its hash."""

    token_hash: str = field(repr=False)
    telegram_id: int
    kind: str
    created_at: float
    expires_at: float
    user_agent: str | None
    ip: str | None
    tg_chat_id: int | None
    tg_message_id: int | None

    def expired(self, now: float) -> bool:
        return self.expires_at < now


_SESSION_COLUMNS = (
    "token_hash",
    "telegram_id",
    "kind",
    "created_at",
    "expires_at",
    "user_agent",
    "ip",
    "tg_chat_id",
    "tg_message_id",
)


def _session(row: sqlite3.Row) -> WebSession:
    return WebSession(**{name: row[name] for name in _SESSION_COLUMNS})


def _check_hash(token_hash: Any) -> str:
    if not isinstance(token_hash, str) or not token_hash:
        raise ValueError("token_hash must be a non-empty string")
    return token_hash


class SessionStore:
    """The ``web_sessions`` table. Callers pass token *hashes*, never tokens."""

    def __init__(
        self, conn: sqlite3.Connection, *, lock: threading.Lock | threading.RLock | None = None
    ) -> None:
        self._conn = conn
        self._guard = _Locked(lock)

    def insert(
        self,
        token_hash: str,
        telegram_id: int,
        kind: str,
        expires_at: float,
        ip: str | None,
        user_agent: str | None,
        tg_chat_id: int | None = None,
        tg_message_id: int | None = None,
        *,
        created_at: float | None = None,
    ) -> WebSession:
        """Store a login token or session (``ip``/``user_agent`` are truncated to 64/200)."""
        if kind not in SESSION_KINDS:
            raise ValueError(f"unknown session kind {kind!r}")
        created = time.time() if created_at is None else float(created_at)
        record = WebSession(
            token_hash=_check_hash(token_hash),
            telegram_id=_check_telegram_id(telegram_id),
            kind=kind,
            created_at=created,
            expires_at=float(expires_at),
            user_agent=(user_agent or "")[:USER_AGENT_MAX],
            ip=(ip or "")[:IP_MAX],
            tg_chat_id=tg_chat_id,
            tg_message_id=tg_message_id,
        )
        with self._guard.held():
            self._conn.execute(
                f"INSERT INTO web_sessions ({', '.join(_SESSION_COLUMNS)}) "
                f"VALUES ({', '.join('?' for _ in _SESSION_COLUMNS)})",
                tuple(getattr(record, name) for name in _SESSION_COLUMNS),
            )
        return record

    def attach_message(self, token_hash: str, tg_chat_id: int, tg_message_id: int) -> bool:
        """Remember which Telegram message carries a login link (deleted after sign-in)."""
        with self._guard.held():
            changed = self._conn.execute(
                "UPDATE web_sessions SET tg_chat_id = ?, tg_message_id = ? "
                "WHERE token_hash = ? AND kind = 'login'",
                (int(tg_chat_id), int(tg_message_id), _check_hash(token_hash)),
            ).rowcount
        return changed == 1

    def consume_login(self, token_hash: str, now: float) -> WebSession | None:
        """Burn a login token and return it, or None if unknown, already used or expired.

        The row is removed with a single ``DELETE … RETURNING``, so of two concurrent
        consumers exactly one gets it (finding F86). Expired tokens are deleted as well.
        """
        with self._guard.held(), transaction(self._conn):
            rows = self._conn.execute(
                f"DELETE FROM web_sessions WHERE token_hash = ? AND kind = 'login' "
                f"RETURNING {', '.join(_SESSION_COLUMNS)}",
                (_check_hash(token_hash),),
            ).fetchall()
        if not rows:
            return None
        record = _session(rows[0])
        return None if record.expired(now) else record

    def get_session(self, token_hash: str, *, now: float | None = None) -> WebSession | None:
        """The ``session`` row for ``token_hash`` (login tokens are never returned).

        Without ``now`` an expired row is still returned (the caller applies its sliding and
        absolute TTL rules); with ``now`` an expired row yields None.
        """
        with self._guard.held():
            row = self._conn.execute(
                f"SELECT {', '.join(_SESSION_COLUMNS)} FROM web_sessions "
                "WHERE token_hash = ? AND kind = 'session'",
                (_check_hash(token_hash),),
            ).fetchone()
        if row is None:
            return None
        record = _session(row)
        if now is not None and record.expired(now):
            return None
        return record

    def slide(self, token_hash: str, new_expires: float) -> bool:
        """Set a session's ``expires_at`` (the caller caps it at the absolute TTL)."""
        with self._guard.held():
            changed = self._conn.execute(
                "UPDATE web_sessions SET expires_at = ? WHERE token_hash = ? AND kind = 'session'",
                (float(new_expires), _check_hash(token_hash)),
            ).rowcount
        return changed == 1

    def delete(self, token_hash: str) -> bool:
        """Delete one row (session or login token); True if it existed."""
        with self._guard.held():
            changed = self._conn.execute(
                "DELETE FROM web_sessions WHERE token_hash = ?", (_check_hash(token_hash),)
            ).rowcount
        return changed == 1

    def revoke_all(self, telegram_id: int) -> int:
        """Delete every session and unused login token of a user; returns how many."""
        with self._guard.held():
            removed = self._conn.execute(
                "DELETE FROM web_sessions WHERE telegram_id = ?", (_check_telegram_id(telegram_id),)
            ).rowcount
        if removed:
            log.info("revoked %d web session(s)/login token(s) of user %s", removed, telegram_id)
        return removed

    def purge_expired(self, now: float) -> int:
        """Delete every row that expired before ``now``; returns how many."""
        with self._guard.held():
            return self._conn.execute(
                "DELETE FROM web_sessions WHERE expires_at < ?", (float(now),)
            ).rowcount
