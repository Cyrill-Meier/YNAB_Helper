"""Web login tokens, browser sessions and CSRF tokens (docs/DESIGN.md §9.1).

Shared by the Telegram bot and the web UI (no FastAPI imports here):

* the bot's ``/login`` mints a one-shot login token (:func:`issue_login_token`) and records
  which message carries the link (:func:`attach_tg_message`); ``/deny``, ``/revoke``,
  ``/logout_all`` and a ``/setup`` that changes token, budget or account call
  :func:`revoke_all` (F18, F61);
* the web's ``POST /auth`` exchanges the login token for a session cookie
  (:func:`consume_login_token`), every authenticated request resolves the cookie with
  :func:`check_session` (or :func:`lookup_session`), ``/logout`` calls
  :func:`delete_session`, a background thread calls :func:`purge_expired`, and
  state-changing requests are checked with :meth:`TokenHasher.verify_csrf`.

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

Authorisation (F18, R05) is decided by :func:`session_access` from the user's row:

* **allowed** — YNAB token, budget and account set, and state ``ready`` or one of the
  crypto-setup states. ``/crypto_setup`` starts from ``ready``, never touches token, budget
  or account and ends in ``ready`` again, so it must not cost the user their web sign-in.
* **paused** — the same complete configuration while the main ``/setup`` runs
  (``awaiting_token``/``awaiting_budget``/``awaiting_account``). The previous configuration
  stays in the row until the new one is committed: requests are refused, but the session is
  kept (and not slid). ``/cancel`` returns to ``ready`` and the session works again; a
  commit that changes token, budget or account revokes the sessions itself (the bot's
  onboarding calls :func:`revoke_all`).
* **revoked** — everything else (unknown user, ``pending``, ``denied``, an unknown state,
  an incomplete configuration): login tokens are rejected and sessions are deleted.

v1 compatibility: v1 stored plain ``sha256(token)`` hashes. Those rows never match an HMAC,
so v1 sessions and login links stop working once and users sign in again. The users-DB
migration already purges expired rows, and :func:`purge_expired` removes the rest as they
expire. The cookie name :data:`SESSION_COOKIE` is unchanged (DESIGN §2.8).
"""

from __future__ import annotations

import contextlib
import enum
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

from ynab_helper.bot import states as S
from ynab_helper.storage.db import transaction
from ynab_helper.storage.users import SessionStore, User, UserStore

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


@dataclass(frozen=True, slots=True)
class SessionPaused:
    """A live session whose user is in the main ``/setup`` (:attr:`SessionAccess.PAUSED`).

    The session row is kept and its expiry is not slid; the request must be refused
    without signing the browser out (keep the cookie).
    """

    telegram_id: int
    state: str


class SessionAccess(enum.Enum):
    """What a user's row means for their login tokens and sessions (see the module doc)."""

    #: Sign-in works, sessions are used and slid.
    ALLOWED = "allowed"
    #: Sessions are kept but refused for now; login tokens are rejected.
    PAUSED = "paused"
    #: Login tokens are rejected and sessions are deleted.
    REVOKED = "revoked"


#: States in which a fully configured user may use the web UI: ``ready`` and the crypto
#: setup, which starts from ``ready`` and never changes token, budget or account (R05).
_ALLOWED_STATES: Final = S.READY_STATES | S.CRYPTO_STATES
#: The main setup: the previous configuration stays active until the new one is committed.
_PAUSED_STATES: Final = S.SETUP_STATES


def session_access(user: User | None) -> SessionAccess:
    """Whether ``user`` may use the web UI now (F18, R05).

    ALLOWED needs YNAB token, budget and account plus state ``ready`` or a crypto-setup
    state; PAUSED is the same complete configuration in a main-setup state; anything else
    (no such user, ``pending``, ``denied``, an unknown state, a missing token, budget or
    account) is REVOKED.
    """
    if user is None or not (user.ynab_token and user.budget_id and user.account_id):
        return SessionAccess.REVOKED
    if user.state in _ALLOWED_STATES:
        return SessionAccess.ALLOWED
    if user.state in _PAUSED_STATES:
        return SessionAccess.PAUSED
    return SessionAccess.REVOKED


#: Log wording for a refused sign-in or session.
_REFUSED_BECAUSE: Final = {
    SessionAccess.PAUSED: "the YNAB setup is in progress",
    SessionAccess.REVOKED: "user is not set up or not allowed",
}


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
    concurrent use finds nothing, F86); an expired token, or one whose user is not
    :attr:`SessionAccess.ALLOWED` (F18, :func:`session_access`; a user in the main
    ``/setup`` is refused too), is rejected; then a ``session`` row is created with
    ``expires_at = min(now + session_ttl, now + absolute_ttl)``. A rejected token stays
    burnt.

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
    rejected: tuple[int, str] | None = None
    result: LoginResult | None = None
    with _held(lock), transaction(conn):
        sessions = SessionStore(conn)
        login = sessions.consume_login(login_hash, at)
        if login is not None:
            access = session_access(UserStore(conn).get(login.telegram_id))
            if access is not SessionAccess.ALLOWED:
                rejected = (login.telegram_id, _REFUSED_BECAUSE[access])
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
    elif rejected is not None:
        log.warning("web sign-in refused for user %s: %s", *rejected)
    else:
        log.info("web sign-in refused: login link unknown, already used or expired")
    return result


def check_session(
    conn: sqlite3.Connection,
    hasher: TokenHasher,
    session_token: str | None,
    *,
    sliding_ttl: float,
    absolute_ttl: float,
    now: float | None = None,
    lock: LockLike | None = None,
) -> SessionInfo | SessionPaused | None:
    """Resolve a session cookie: a usable session (expiry slid), a paused one, or None.

    * None (and the row is deleted where there is one) when the session is unknown, past
      its sliding expiry, older than ``absolute_ttl`` (counted from its creation), or when
      its user is :attr:`SessionAccess.REVOKED` (F18).
    * :class:`SessionPaused` when its user is :attr:`SessionAccess.PAUSED` (the main
      ``/setup`` runs, R05): the row is kept as it is, neither deleted nor slid.
    * Otherwise :class:`SessionInfo`, and ``expires_at`` becomes ``min(now + sliding_ttl,
      created_at + absolute_ttl)``.

    Login tokens are never accepted as sessions. Raises :class:`ValueError` for
    non-positive TTLs.
    """
    sliding = _positive_seconds("sliding_ttl", sliding_ttl)
    absolute = _positive_seconds("absolute_ttl", absolute_ttl)
    if not _plausible_token(session_token):
        return None
    at = _now(now)
    token_hash = hasher.hash(session_token)
    dropped: tuple[int, str] | None = None
    found: SessionInfo | SessionPaused | None = None
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
            access = session_access(user)
            if user is None or access is SessionAccess.REVOKED:
                sessions.delete(token_hash)
                dropped = (record.telegram_id, _REFUSED_BECAUSE[SessionAccess.REVOKED])
            elif access is SessionAccess.PAUSED:
                found = SessionPaused(telegram_id=record.telegram_id, state=user.state)
            else:
                expires = min(at + sliding, deadline)
                if expires != record.expires_at:
                    sessions.slide(token_hash, expires)
                found = SessionInfo(
                    telegram_id=record.telegram_id,
                    expires_at=expires,
                    created_at=record.created_at,
                )
    if dropped is not None:
        log.info("web session of user %s ended: %s", *dropped)
    elif isinstance(found, SessionPaused):
        reason = _REFUSED_BECAUSE[SessionAccess.PAUSED]
        log.debug("web request of user %s refused for now: %s", found.telegram_id, reason)
    return found


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
    """:func:`check_session` reduced to "usable or not": a usable session (expiry slid),
    else None. A paused session is None here too, but its row is kept (R05)."""
    found = check_session(
        conn,
        hasher,
        session_token,
        sliding_ttl=sliding_ttl,
        absolute_ttl=absolute_ttl,
        now=now,
        lock=lock,
    )
    return found if isinstance(found, SessionInfo) else None


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
