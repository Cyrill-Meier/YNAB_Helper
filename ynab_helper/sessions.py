"""Web login tokens, browser sessions and CSRF tokens (docs/DESIGN.md §9.1).

Shared by the Telegram bot and the web UI (no FastAPI imports here):

* the bot's ``/login`` mints a one-shot login token (:func:`issue_login_token`) and records
  which message carries the link (:func:`attach_tg_message`); ``/deny``, ``/setup``,
  ``/revoke`` and ``/logout_all`` call :func:`revoke_all` (F18, F61);
* the web's ``POST /auth`` exchanges the login token for a session cookie
  (:func:`consume_login_token`), every authenticated request resolves the cookie with
  :func:`lookup_session`, ``/logout`` calls :func:`delete_session`, a background thread calls
  :func:`purge_expired`, and state-changing requests are checked with
  :meth:`TokenHasher.verify_csrf`.

Secrets never reach the database: a token is stored as ``HMAC-SHA256(WEB_UI_SECRET_KEY,
token)`` (F89), so a leaked ``bot_users.db`` yields no usable token and rotating the key
invalidates every session and login link. The cleartext login token travels only in the
Telegram button URL, the session token only in the ``rynab_session`` cookie.

Atomicity (F86): each function is one short ``BEGIN IMMEDIATE`` transaction on the given
connection. A login token is burnt with ``DELETE … RETURNING`` inside the same transaction
that checks the user and creates the session, so of two concurrent requests with the same
token exactly one gets a session, and a user denied in between never gets one. The
connection must be an autocommit connection from :func:`ynab_helper.storage.connect` or
:func:`ynab_helper.storage.open_users_db`. No network I/O happens here.

Shared connections: the bot shares one ``check_same_thread=False`` connection between
threads under a lock. Pass that lock as ``lock=``. It is held for the whole transaction and
the stores are used without a lock of their own, so a plain :class:`threading.Lock` works
as well as an ``RLock``. Connections opened per request (the web) need no lock.

Authorisation (F18): a login token or session only counts while its user exists and is
fully onboarded (:attr:`ynab_helper.storage.User.is_ready`: state ``ready`` with YNAB
token, budget and account set). A session found for any other user is deleted.

v1 compatibility: v1 stored plain ``sha256(token)`` hashes. Those rows never match an HMAC,
so v1 sessions and login links stop working once and users sign in again. The users-DB
migration already purges expired rows, and :func:`purge_expired` removes the rest as they
expire. The cookie name :data:`SESSION_COOKIE` is unchanged (DESIGN §2.8).
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import logging
import math
import secrets
import sqlite3
import time
from collections.abc import Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import Any, Final

from ynab_helper.storage.db import transaction
from ynab_helper.storage.users import SessionStore, UserStore

log = logging.getLogger(__name__)

#: Name of the browser session cookie (unchanged from v1).
SESSION_COOKIE: Final = "rynab_session"
#: Minimum length of ``WEB_UI_SECRET_KEY`` (characters).
MIN_SECRET_LENGTH: Final = 32
#: Length of a CSRF token (hex characters).
CSRF_TOKEN_LENGTH: Final = 32
#: Random bytes per login/session token (``secrets.token_urlsafe`` → 43 characters).
TOKEN_BYTES: Final = 32
#: Presented tokens longer than this, empty or non-ASCII are rejected without hashing.
MAX_TOKEN_LENGTH: Final = 256

_LOGIN: Final = "login"
_SESSION: Final = "session"

#: Anything usable as ``with lock:`` (``threading.Lock``, ``threading.RLock``, …).
type LockLike = AbstractContextManager[Any]


class TokenHasher:
    """Keyed hashing of tokens and derivation of CSRF tokens.

    ``hash(token)`` is the hex ``HMAC-SHA256(secret, token)`` stored in ``web_sessions``;
    ``csrf_for(session_token)`` is ``HMAC-SHA256(secret, "csrf:" + session_token)`` cut to
    32 hex characters. The secret is never shown by ``repr``.
    """

    __slots__ = ("_key",)

    def __init__(self, secret: str) -> None:
        """Raise :class:`ValueError` unless ``secret`` is a string of at least 32 characters."""
        if not isinstance(secret, str) or len(secret) < MIN_SECRET_LENGTH:
            raise ValueError(
                f"the web UI secret key must be at least {MIN_SECRET_LENGTH} characters long"
            )
        self._key = secret.encode("utf-8", "surrogatepass")

    def __repr__(self) -> str:
        return "TokenHasher(secret=<redacted>)"

    def hash(self, token: str) -> str:
        """Hex ``HMAC-SHA256(secret, token)`` (64 characters)."""
        return self._mac(token).hexdigest()

    def csrf_for(self, session_token: str) -> str:
        """The CSRF token bound to a session: ``HMAC-SHA256(secret, "csrf:" + token)[:32]``."""
        return self._mac("csrf:" + session_token).hexdigest()[:CSRF_TOKEN_LENGTH]

    def verify_csrf(self, session_token: str | None, candidate: object) -> bool:
        """True if ``candidate`` is the CSRF token of ``session_token``.

        Compared with :func:`hmac.compare_digest`. A missing session token, or a candidate
        that is not a non-empty ASCII string, is simply False.
        """
        if not isinstance(session_token, str) or not session_token:
            return False
        if not isinstance(candidate, str) or not candidate or not candidate.isascii():
            return False
        return hmac.compare_digest(self.csrf_for(session_token), candidate)

    def _mac(self, message: str) -> hmac.HMAC:
        if not isinstance(message, str):
            raise TypeError(f"token must be a str, got {type(message).__name__}")
        return hmac.new(self._key, message.encode("utf-8", "surrogatepass"), hashlib.sha256)


def new_token() -> str:
    """A fresh random token: ``secrets.token_urlsafe(32)`` (256 bits, URL-safe)."""
    return secrets.token_urlsafe(TOKEN_BYTES)


@dataclass(frozen=True, slots=True)
class LoginResult:
    """A successful login: the new session plus where the ``/login`` message lives.

    ``session_token`` is the cleartext cookie value (excluded from ``repr``).
    ``expires_at`` is the sliding expiry, ``absolute_expires_at`` the hard limit
    (``created + absolute_ttl``; use the absolute TTL as the cookie's Max-Age).
    ``tg_chat_id``/``tg_message_id`` are None when the bot did not record the message.
    """

    session_token: str = field(repr=False)
    telegram_id: int
    expires_at: float
    absolute_expires_at: float
    tg_chat_id: int | None = None
    tg_message_id: int | None = None


@dataclass(frozen=True, slots=True)
class SessionInfo:
    """A valid browser session after :func:`lookup_session` (expiry already slid)."""

    telegram_id: int
    expires_at: float
    created_at: float


# ── public API ────────────────────────────────────────────────────────────────


def issue_login_token(
    conn: sqlite3.Connection,
    hasher: TokenHasher,
    telegram_id: int,
    *,
    ttl_seconds: float,
    ip: str | None = "bot",
    user_agent: str | None = "telegram",
    now: float | None = None,
    lock: LockLike | None = None,
) -> str:
    """Mint a one-shot login token for ``telegram_id`` and return it in clear text.

    Only its hash is stored (``kind='login'``, expiring ``ttl_seconds`` from ``now``).
    Whether the user may sign in is checked when the token is consumed. Raises
    :class:`ValueError` for a non-positive TTL or a non-int ``telegram_id``.
    """
    ttl = _positive_seconds("ttl_seconds", ttl_seconds)
    issued = _now(now)
    token = new_token()
    with _held(lock), transaction(conn):
        SessionStore(conn).insert(
            hasher.hash(token),
            telegram_id,
            _LOGIN,
            issued + ttl,
            ip,
            user_agent,
            created_at=issued,
        )
    log.info("issued a web login token for user %s (valid for %.0f s)", telegram_id, ttl)
    return token


def attach_tg_message(
    conn: sqlite3.Connection,
    hasher: TokenHasher,
    token: str,
    *,
    chat_id: int | None,
    message_id: int | None,
    lock: LockLike | None = None,
) -> bool:
    """Record which Telegram message carries the login link for ``token``.

    :func:`consume_login_token` hands ``(chat_id, message_id)`` back so the web can delete
    that message after sign-in. Returns True if an unused login token was updated; False
    when there is nothing to record (``chat_id``/``message_id`` None) or the token is
    unknown, already used or a session token. Raises :class:`ValueError` for ids that are
    not ints.
    """
    if chat_id is None or message_id is None:
        return False
    for name, value in (("chat_id", chat_id), ("message_id", message_id)):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} must be an int, got {type(value).__name__}")
    if not _plausible_token(token):
        return False
    token_hash = hasher.hash(token)
    with _held(lock), transaction(conn):
        return SessionStore(conn).attach_message(token_hash, chat_id, message_id)


def consume_login_token(
    conn: sqlite3.Connection,
    hasher: TokenHasher,
    token: str,
    *,
    session_ttl: float,
    absolute_ttl: float,
    ip: str | None,
    user_agent: str | None,
    now: float | None = None,
    lock: LockLike | None = None,
) -> LoginResult | None:
    """Exchange a login token for a new browser session, atomically and at most once.

    In one transaction: the login token is deleted (``DELETE … RETURNING``, so a second or
    concurrent use finds nothing, F86); an expired token, or one whose user is missing or
    not ready (state ``ready`` with YNAB token, budget and account, F18), is rejected; then
    a ``session`` row is created with ``expires_at = min(now + session_ttl, now +
    absolute_ttl)``. A rejected token stays burnt.

    Returns None for an unknown, used, expired or rejected token (also for anything that
    is not a plausible token string). Raises :class:`ValueError` for non-positive TTLs
    before the token is touched.
    """
    sliding = _positive_seconds("session_ttl", session_ttl)
    absolute = _positive_seconds("absolute_ttl", absolute_ttl)
    if not _plausible_token(token):
        return None
    at = _now(now)
    login_hash = hasher.hash(token)
    session_token = new_token()
    session_hash = hasher.hash(session_token)
    rejected_user: int | None = None
    result: LoginResult | None = None
    with _held(lock), transaction(conn):
        sessions = SessionStore(conn)
        login = sessions.consume_login(login_hash, at)
        if login is not None:
            user = UserStore(conn).get(login.telegram_id)
            if user is None or not user.is_ready:
                rejected_user = login.telegram_id
            else:
                expires = min(at + sliding, at + absolute)
                sessions.insert(
                    session_hash,
                    login.telegram_id,
                    _SESSION,
                    expires,
                    ip,
                    user_agent,
                    created_at=at,
                )
                result = LoginResult(
                    session_token=session_token,
                    telegram_id=login.telegram_id,
                    expires_at=expires,
                    absolute_expires_at=at + absolute,
                    tg_chat_id=login.tg_chat_id,
                    tg_message_id=login.tg_message_id,
                )
    if result is not None:
        log.info("web sign-in of user %s", result.telegram_id)
    elif rejected_user is not None:
        log.warning(
            "web sign-in refused for user %s: not set up (state is not ready)", rejected_user
        )
    else:
        log.info("web sign-in refused: login link unknown, already used or expired")
    return result


def lookup_session(
    conn: sqlite3.Connection,
    hasher: TokenHasher,
    session_token: str | None,
    *,
    sliding_ttl: float,
    absolute_ttl: float,
    now: float | None = None,
    lock: LockLike | None = None,
) -> SessionInfo | None:
    """Resolve a session cookie and slide its expiry.

    Returns None (and deletes the row where there is one) when the session is unknown,
    past its sliding expiry, older than ``absolute_ttl`` (counted from its creation), or
    when its user is missing or not ready (F18). Otherwise ``expires_at`` becomes
    ``min(now + sliding_ttl, created_at + absolute_ttl)``. Login tokens are never accepted
    as sessions. Raises :class:`ValueError` for non-positive TTLs.
    """
    sliding = _positive_seconds("sliding_ttl", sliding_ttl)
    absolute = _positive_seconds("absolute_ttl", absolute_ttl)
    if not _plausible_token(session_token):
        return None
    at = _now(now)
    token_hash = hasher.hash(session_token)
    dropped: tuple[int, str] | None = None
    info: SessionInfo | None = None
    with _held(lock), transaction(conn):
        sessions = SessionStore(conn)
        record = sessions.get_session(token_hash)
        if record is None:
            return None
        deadline = record.created_at + absolute
        if record.expired(at) or deadline < at:
            sessions.delete(token_hash)
            dropped = (record.telegram_id, "expired")
        else:
            user = UserStore(conn).get(record.telegram_id)
            if user is None or not user.is_ready:
                sessions.delete(token_hash)
                dropped = (record.telegram_id, "user is not set up (state is not ready)")
            else:
                expires = min(at + sliding, deadline)
                if expires != record.expires_at:
                    sessions.slide(token_hash, expires)
                info = SessionInfo(
                    telegram_id=record.telegram_id,
                    expires_at=expires,
                    created_at=record.created_at,
                )
    if dropped is not None:
        log.info("web session of user %s ended: %s", *dropped)
    return info


def delete_session(
    conn: sqlite3.Connection,
    hasher: TokenHasher,
    session_token: str | None,
    *,
    lock: LockLike | None = None,
) -> bool:
    """Delete one browser session (``/logout``); True if it existed.

    Only ``session`` rows are deleted: presenting a login token here does nothing.
    """
    if not _plausible_token(session_token):
        return False
    token_hash = hasher.hash(session_token)
    with _held(lock), transaction(conn):
        sessions = SessionStore(conn)
        if sessions.get_session(token_hash) is None:
            return False
        return sessions.delete(token_hash)


def revoke_all(conn: sqlite3.Connection, telegram_id: int, *, lock: LockLike | None = None) -> int:
    """Delete every browser session *and* unused login token of a user (F18, F61).

    Returns how many rows were removed. Raises :class:`ValueError` for a non-int id.
    """
    with _held(lock), transaction(conn):
        return SessionStore(conn).revoke_all(telegram_id)


def purge_expired(
    conn: sqlite3.Connection, now: float | None = None, *, lock: LockLike | None = None
) -> int:
    """Delete every session and login token whose expiry has passed; returns how many."""
    at = _now(now)
    with _held(lock), transaction(conn):
        removed = SessionStore(conn).purge_expired(at)
    if removed:
        log.debug("purged %d expired web session(s)/login token(s)", removed)
    return removed


# ── helpers ───────────────────────────────────────────────────────────────────


@contextlib.contextmanager
def _held(lock: LockLike | None) -> Iterator[None]:
    if lock is None:
        yield
        return
    with lock:
        yield


def _plausible_token(token: object) -> bool:
    """Tokens we mint are 43 URL-safe ASCII characters; anything far off is rejected
    before hashing (oversized cookies, non-strings, non-ASCII garbage)."""
    return isinstance(token, str) and 0 < len(token) <= MAX_TOKEN_LENGTH and token.isascii()


def _positive_seconds(name: str, value: object) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{name} must be a positive number of seconds, got {value!r}")
    return float(value)


def _now(now: float | None) -> float:
    return time.time() if now is None else float(now)
