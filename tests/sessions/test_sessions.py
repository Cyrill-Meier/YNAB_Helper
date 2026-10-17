"""ynab_helper.sessions: login tokens, sessions and CSRF (DESIGN §9.1; F18, F61, F86, F89)."""

from __future__ import annotations

import hashlib
import hmac
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from ynab_helper import sessions
from ynab_helper.sessions import (
    SESSION_COOKIE,
    LoginResult,
    SessionInfo,
    TokenHasher,
    attach_tg_message,
    consume_login_token,
    delete_session,
    issue_login_token,
    lookup_session,
    new_token,
    purge_expired,
    revoke_all,
)
from ynab_helper.storage import SessionStore, UserStore, connect, open_users_db

from .helpers import (
    ABSOLUTE_TTL,
    LOGIN_TTL,
    OTHER_ID,
    OTHER_SECRET,
    READY_ID,
    SECRET,
    SESSION_TTL,
    T0,
    make_ready,
)


def _rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM web_sessions ORDER BY created_at, kind").fetchall()


def _kinds(conn: sqlite3.Connection, telegram_id: int | None = None) -> list[str]:
    if telegram_id is None:
        rows = conn.execute("SELECT kind FROM web_sessions ORDER BY kind").fetchall()
    else:
        rows = conn.execute(
            "SELECT kind FROM web_sessions WHERE telegram_id = ? ORDER BY kind", (telegram_id,)
        ).fetchall()
    return [r[0] for r in rows]


def _login(
    conn: sqlite3.Connection,
    hasher: TokenHasher,
    telegram_id: int = READY_ID,
    *,
    now: float = T0,
) -> LoginResult:
    token = issue_login_token(conn, hasher, telegram_id, ttl_seconds=LOGIN_TTL, now=now)
    result = consume_login_token(
        conn,
        hasher,
        token,
        session_ttl=SESSION_TTL,
        absolute_ttl=ABSOLUTE_TTL,
        ip="203.0.113.9",
        user_agent="pytest-browser",
        now=now + 5,
    )
    assert result is not None
    return result


def _lookup(
    conn: sqlite3.Connection, hasher: TokenHasher, token: object, now: float
) -> SessionInfo | None:
    return lookup_session(
        conn,
        hasher,
        token,  # type: ignore[arg-type]
        sliding_ttl=SESSION_TTL,
        absolute_ttl=ABSOLUTE_TTL,
        now=now,
    )


# ── constants, tokens, TokenHasher ────────────────────────────────────────────


def test_cookie_name_is_unchanged_from_v1() -> None:
    assert SESSION_COOKIE == "rynab_session"


def test_new_token_is_256_bit_urlsafe_and_unique() -> None:
    tokens = {new_token() for _ in range(200)}
    assert len(tokens) == 200
    for token in tokens:
        assert len(token) == 43
        assert set(token) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")


@pytest.mark.parametrize("secret", ["", "x" * 31, None, b"y" * 40, 12345])
def test_hasher_rejects_short_or_non_string_secrets(secret: object) -> None:
    with pytest.raises(ValueError, match="at least 32 characters"):
        TokenHasher(secret)  # type: ignore[arg-type]


def test_hasher_accepts_exactly_32_characters_and_hides_the_secret() -> None:
    hasher = TokenHasher("s" * 32)
    assert "s" * 32 not in repr(hasher)
    assert repr(hasher) == "TokenHasher(secret=<redacted>)"


def test_F89_token_hash_is_keyed_hmac_sha256(hasher: TokenHasher) -> None:
    """F89: tokens are stored as HMAC(WEB_UI_SECRET_KEY, token), not as plain sha256."""
    token = new_token()
    expected = hmac.new(SECRET.encode(), token.encode(), hashlib.sha256).hexdigest()
    assert hasher.hash(token) == expected
    assert len(expected) == 64
    assert hasher.hash(token) == TokenHasher(SECRET).hash(token)  # deterministic
    assert hasher.hash(token) != TokenHasher(OTHER_SECRET).hash(token)  # keyed
    assert hasher.hash(token) != hashlib.sha256(token.encode()).hexdigest()  # not v1's


def test_hash_rejects_non_strings(hasher: TokenHasher) -> None:
    with pytest.raises(TypeError):
        hasher.hash(b"bytes")  # type: ignore[arg-type]


def test_F89_csrf_token_is_keyed_and_bound_to_the_session(hasher: TokenHasher) -> None:
    session_token = new_token()
    csrf = hasher.csrf_for(session_token)
    expected = hmac.new(SECRET.encode(), f"csrf:{session_token}".encode(), hashlib.sha256)
    assert csrf == expected.hexdigest()[:32]
    assert len(csrf) == 32
    assert csrf != hasher.csrf_for(new_token())
    assert csrf != TokenHasher(OTHER_SECRET).csrf_for(session_token)
    # v1 derived it without a key.
    assert csrf != hashlib.sha256(b"csrf:" + session_token.encode()).hexdigest()[:32]


def test_verify_csrf(hasher: TokenHasher) -> None:
    session_token = new_token()
    csrf = hasher.csrf_for(session_token)
    assert hasher.verify_csrf(session_token, csrf)
    assert not hasher.verify_csrf(session_token, csrf[:-1] + ("0" if csrf[-1] != "0" else "1"))
    assert not hasher.verify_csrf(session_token, csrf.upper() if csrf.upper() != csrf else "x")
    assert not hasher.verify_csrf(session_token, csrf + "0")
    assert not hasher.verify_csrf(new_token(), csrf)  # another session's token
    assert not TokenHasher(OTHER_SECRET).verify_csrf(session_token, csrf)
    for candidate in ("", None, 123, b"bytes", "é" * 32, csrf[:16] + "é" * 16):
        assert not hasher.verify_csrf(session_token, candidate)
    for missing in ("", None):
        assert not hasher.verify_csrf(missing, csrf)


def test_verify_csrf_uses_compare_digest(
    hasher: TokenHasher, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[str, str]] = []
    real = hmac.compare_digest

    def spy(a: str, b: str) -> bool:
        calls.append((a, b))
        return real(a, b)

    monkeypatch.setattr(sessions.hmac, "compare_digest", spy)
    session_token = new_token()
    assert hasher.verify_csrf(session_token, hasher.csrf_for(session_token))
    assert not hasher.verify_csrf(session_token, "0" * 32)
    assert len(calls) == 2


# ── issuing login tokens ──────────────────────────────────────────────────────


def test_issue_login_token_stores_only_the_hash(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    token = issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)
    (row,) = _rows(conn)
    assert row["token_hash"] == hasher.hash(token)
    assert row["kind"] == "login"
    assert row["telegram_id"] == ready_user
    assert row["created_at"] == T0
    assert row["expires_at"] == T0 + LOGIN_TTL
    assert (row["ip"], row["user_agent"]) == ("bot", "telegram")
    assert (row["tg_chat_id"], row["tg_message_id"]) == (None, None)
    dump = "\n".join(conn.iterdump())
    assert token not in dump


def test_issue_login_token_defaults_now_to_the_clock(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    before = time.time()
    issue_login_token(conn, hasher, ready_user, ttl_seconds=60)
    (row,) = _rows(conn)
    assert before <= row["created_at"] <= time.time()
    assert row["expires_at"] == pytest.approx(row["created_at"] + 60)


@pytest.mark.parametrize("ttl", [0, -1, float("nan"), float("inf"), True, "300"])
def test_issue_login_token_rejects_bad_ttl(
    conn: sqlite3.Connection, hasher: TokenHasher, ttl: object
) -> None:
    with pytest.raises(ValueError, match="ttl_seconds"):
        issue_login_token(conn, hasher, READY_ID, ttl_seconds=ttl)  # type: ignore[arg-type]
    assert _rows(conn) == []


def test_issue_login_token_rejects_non_int_user(conn: sqlite3.Connection, hasher: TokenHasher):
    with pytest.raises(ValueError):
        issue_login_token(conn, hasher, "4242", ttl_seconds=60)  # type: ignore[arg-type]
    assert _rows(conn) == []


def test_attach_tg_message(conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int) -> None:
    token = issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)
    assert attach_tg_message(conn, hasher, token, chat_id=ready_user, message_id=991)
    (row,) = _rows(conn)
    assert (row["tg_chat_id"], row["tg_message_id"]) == (ready_user, 991)
    assert not attach_tg_message(conn, hasher, new_token(), chat_id=1, message_id=2)
    assert not attach_tg_message(conn, hasher, token, chat_id=None, message_id=2)
    assert not attach_tg_message(conn, hasher, "", chat_id=1, message_id=2)
    with pytest.raises(ValueError, match="message_id"):
        attach_tg_message(conn, hasher, token, chat_id=1, message_id="2")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="chat_id"):
        attach_tg_message(conn, hasher, token, chat_id=True, message_id=2)
    (row,) = _rows(conn)
    assert (row["tg_chat_id"], row["tg_message_id"]) == (ready_user, 991)


def test_attach_tg_message_ignores_session_tokens(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    result = _login(conn, hasher)
    assert not attach_tg_message(conn, hasher, result.session_token, chat_id=1, message_id=2)


# ── consuming login tokens ────────────────────────────────────────────────────


def test_consume_creates_a_session_and_is_single_use(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    token = issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)
    attach_tg_message(conn, hasher, token, chat_id=ready_user, message_id=55)
    kwargs = {
        "session_ttl": SESSION_TTL,
        "absolute_ttl": ABSOLUTE_TTL,
        "ip": "203.0.113.9",
        "user_agent": "Firefox",
        "now": T0 + 10,
    }
    result = consume_login_token(conn, hasher, token, **kwargs)  # type: ignore[arg-type]
    assert result == LoginResult(
        session_token=result.session_token if result else "",
        telegram_id=ready_user,
        expires_at=T0 + 10 + SESSION_TTL,
        absolute_expires_at=T0 + 10 + ABSOLUTE_TTL,
        tg_chat_id=ready_user,
        tg_message_id=55,
    )
    assert result is not None
    assert result.session_token != token and len(result.session_token) == 43
    assert result.session_token not in repr(result)
    (row,) = _rows(conn)
    assert row["kind"] == "session"
    assert row["token_hash"] == hasher.hash(result.session_token)
    assert (row["created_at"], row["expires_at"]) == (T0 + 10, T0 + 10 + SESSION_TTL)
    assert (row["ip"], row["user_agent"]) == ("203.0.113.9", "Firefox")
    assert result.session_token not in "\n".join(conn.iterdump())
    # Second use of the same login token: nothing.
    assert consume_login_token(conn, hasher, token, **kwargs) is None  # type: ignore[arg-type]
    assert _kinds(conn) == ["session"]


def test_consume_caps_the_first_expiry_at_the_absolute_ttl(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    token = issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)
    result = consume_login_token(
        conn, hasher, token, session_ttl=7200, absolute_ttl=600, ip=None, user_agent=None, now=T0
    )
    assert result is not None
    assert result.expires_at == T0 + 600 == result.absolute_expires_at
    (row,) = _rows(conn)
    assert row["expires_at"] == T0 + 600
    assert (row["ip"], row["user_agent"]) == ("", "")


def test_consume_rejects_expired_login_tokens_and_removes_them(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    token = issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)
    result = consume_login_token(
        conn,
        hasher,
        token,
        session_ttl=SESSION_TTL,
        absolute_ttl=ABSOLUTE_TTL,
        ip="ip",
        user_agent="ua",
        now=T0 + LOGIN_TTL + 1,
    )
    assert result is None
    assert _rows(conn) == []


def test_consume_accepts_a_token_up_to_its_expiry_instant(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    token = issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)
    result = consume_login_token(
        conn,
        hasher,
        token,
        session_ttl=SESSION_TTL,
        absolute_ttl=ABSOLUTE_TTL,
        ip="ip",
        user_agent="ua",
        now=T0 + LOGIN_TTL,
    )
    assert result is not None


@pytest.mark.parametrize("garbage", ["", None, 42, "x" * 257, "tökén", new_token()])
def test_consume_rejects_garbage_and_unknown_tokens(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int, garbage: object
) -> None:
    issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)
    result = consume_login_token(
        conn,
        hasher,
        garbage,  # type: ignore[arg-type]
        session_ttl=SESSION_TTL,
        absolute_ttl=ABSOLUTE_TTL,
        ip="ip",
        user_agent="ua",
        now=T0,
    )
    assert result is None
    assert _kinds(conn) == ["login"]  # the real token is untouched


def test_consume_does_not_accept_session_tokens_as_login_tokens(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    first = _login(conn, hasher)
    result = consume_login_token(
        conn,
        hasher,
        first.session_token,
        session_ttl=SESSION_TTL,
        absolute_ttl=ABSOLUTE_TTL,
        ip="ip",
        user_agent="ua",
        now=T0 + 10,
    )
    assert result is None
    assert _kinds(conn) == ["session"]


@pytest.mark.parametrize(
    ("name", "value"),
    [("session_ttl", 0), ("absolute_ttl", -5), ("session_ttl", float("nan"))],
)
def test_consume_validates_ttls_before_burning_the_token(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int, name: str, value: float
) -> None:
    token = issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)
    kwargs: dict[str, object] = {"session_ttl": SESSION_TTL, "absolute_ttl": ABSOLUTE_TTL}
    kwargs[name] = value
    with pytest.raises(ValueError, match=name):
        consume_login_token(
            conn,
            hasher,
            token,
            ip="ip",
            user_agent="ua",
            now=T0,
            **kwargs,  # type: ignore[arg-type]
        )
    assert _kinds(conn) == ["login"]


def test_F89_rotating_the_secret_invalidates_login_tokens_and_sessions(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    session = _login(conn, hasher)
    token = issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)
    rotated = TokenHasher(OTHER_SECRET)
    assert _lookup(conn, rotated, session.session_token, T0 + 60) is None
    assert (
        consume_login_token(
            conn,
            rotated,
            token,
            session_ttl=SESSION_TTL,
            absolute_ttl=ABSOLUTE_TTL,
            ip="ip",
            user_agent="ua",
            now=T0 + 60,
        )
        is None
    )


def test_v1_sha256_rows_never_validate(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    """v1 stored sha256(token); v2 hashes with HMAC, so users simply sign in again once."""
    v1_session, v1_login = new_token(), new_token()
    store = SessionStore(conn)
    store.insert(
        hashlib.sha256(v1_session.encode()).hexdigest(),
        ready_user,
        "session",
        T0 + 3600,
        "ip",
        "ua",
        created_at=T0,
    )
    store.insert(
        hashlib.sha256(v1_login.encode()).hexdigest(),
        ready_user,
        "login",
        T0 + 300,
        "bot",
        "telegram",
        created_at=T0,
    )
    assert _lookup(conn, hasher, v1_session, T0 + 10) is None
    assert (
        consume_login_token(
            conn,
            hasher,
            v1_login,
            session_ttl=SESSION_TTL,
            absolute_ttl=ABSOLUTE_TTL,
            ip="ip",
            user_agent="ua",
            now=T0 + 10,
        )
        is None
    )
    assert purge_expired(conn, T0 + 4000) == 2


# ── F18: only ready users may sign in or keep a session ───────────────────────

_NOT_READY = [
    {"state": "pending"},
    {"state": "denied", "ynab_token": "t", "budget_id": "b", "account_id": "a"},
    {"state": "awaiting_token"},
    {"state": "awaiting_budget", "ynab_token": "t"},
    {"state": "awaiting_account", "ynab_token": "t", "budget_id": "b"},
    {"state": "awaiting_crypto_eth", "ynab_token": "t", "budget_id": "b", "account_id": "a"},
    {"state": "ready", "ynab_token": None, "budget_id": "b", "account_id": "a"},
    {"state": "ready", "ynab_token": "t", "budget_id": "", "account_id": "a"},
    {"state": "ready", "ynab_token": "t", "budget_id": "b", "account_id": None},
]


@pytest.mark.parametrize(
    "fields", _NOT_READY, ids=lambda f: "-".join(f"{k}={v}" for k, v in f.items())
)
def test_F18_login_token_of_a_user_who_is_not_ready_is_rejected(
    conn: sqlite3.Connection, hasher: TokenHasher, fields: dict[str, object]
) -> None:
    UserStore(conn).upsert(OTHER_ID, **fields)
    token = issue_login_token(conn, hasher, OTHER_ID, ttl_seconds=LOGIN_TTL, now=T0)
    result = consume_login_token(
        conn,
        hasher,
        token,
        session_ttl=SESSION_TTL,
        absolute_ttl=ABSOLUTE_TTL,
        ip="ip",
        user_agent="ua",
        now=T0 + 1,
    )
    assert result is None
    assert _rows(conn) == []  # no session, and the login token is burnt


def test_F18_login_token_of_an_unknown_user_is_rejected(
    conn: sqlite3.Connection, hasher: TokenHasher
) -> None:
    token = issue_login_token(conn, hasher, 999, ttl_seconds=LOGIN_TTL, now=T0)
    result = consume_login_token(
        conn,
        hasher,
        token,
        session_ttl=SESSION_TTL,
        absolute_ttl=ABSOLUTE_TTL,
        ip="ip",
        user_agent="ua",
        now=T0 + 1,
    )
    assert result is None
    assert _rows(conn) == []


@pytest.mark.parametrize(
    "fields", _NOT_READY, ids=lambda f: "-".join(f"{k}={v}" for k, v in f.items())
)
def test_F18_session_of_a_user_who_is_no_longer_ready_is_deleted(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int, fields: dict[str, object]
) -> None:
    """v1: /deny and /setup left web sessions working for up to 12 h."""
    session = _login(conn, hasher)
    assert _lookup(conn, hasher, session.session_token, T0 + 60) is not None
    UserStore(conn).upsert(ready_user, **fields)
    assert _lookup(conn, hasher, session.session_token, T0 + 120) is None
    assert _rows(conn) == []
    # Even if the user becomes ready again, that session is gone for good.
    make_ready(conn)
    assert _lookup(conn, hasher, session.session_token, T0 + 180) is None


def test_F18_session_of_a_deleted_user_is_deleted(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    session = _login(conn, hasher)
    conn.execute("DELETE FROM users WHERE telegram_id = ?", (ready_user,))
    assert _lookup(conn, hasher, session.session_token, T0 + 60) is None
    assert _rows(conn) == []


# ── F86: concurrent consumption ───────────────────────────────────────────────


def _race(
    hasher: TokenHasher,
    token: str,
    contenders: int,
    *,
    users_db: Path | None = None,
    shared: sqlite3.Connection | None = None,
    lock: threading.Lock | None = None,
) -> list[LoginResult]:
    """Let ``contenders`` threads consume ``token`` at once, each on its own connection to
    ``users_db`` (the web) or all on the ``shared`` connection under ``lock`` (the bot)."""
    barrier = threading.Barrier(contenders)
    wins: list[LoginResult] = []
    errors: list[BaseException] = []

    def worker() -> None:
        conn = shared if shared is not None else connect(users_db)  # type: ignore[arg-type]
        try:
            barrier.wait()
            result = consume_login_token(
                conn,
                hasher,
                token,
                session_ttl=SESSION_TTL,
                absolute_ttl=ABSOLUTE_TTL,
                ip="ip",
                user_agent="ua",
                lock=lock,
            )
            if result is not None:
                wins.append(result)
        except BaseException as exc:  # pragma: no cover - reported by the assertion
            errors.append(exc)
        finally:
            if shared is None:
                conn.close()

    threads = [threading.Thread(target=worker) for _ in range(contenders)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
        assert not thread.is_alive(), "deadlock"
    assert errors == []
    return wins


@pytest.mark.parametrize("contenders", [2, 4])
def test_F86_concurrent_consume_mints_exactly_one_session(
    users_db: Path, conn: sqlite3.Connection, hasher: TokenHasher, contenders: int
) -> None:
    """F86: v1 SELECTed then DELETEd, so two racing requests both got a session."""
    make_ready(conn)
    for round_no in range(15):
        token = issue_login_token(conn, hasher, READY_ID, ttl_seconds=LOGIN_TTL)
        wins = _race(hasher, token, contenders, users_db=users_db)
        assert len(wins) == 1, f"round {round_no}: {len(wins)} winners"
        sessions_now = conn.execute(
            "SELECT count(*) FROM web_sessions WHERE kind = 'session'"
        ).fetchone()[0]
        assert sessions_now == round_no + 1
        assert _kinds(conn).count("login") == 0


def test_F86_shared_connection_with_a_plain_lock(
    users_db: Path, tmp_path: Path, hasher: TokenHasher
) -> None:
    """The bot shares one connection under a lock; a plain Lock must not deadlock."""
    shared = open_users_db(users_db, backup_dir=tmp_path / "backups", check_same_thread=False)
    lock = threading.Lock()
    try:
        with lock:
            make_ready(shared)
        for _ in range(10):
            token = issue_login_token(shared, hasher, READY_ID, ttl_seconds=60, lock=lock)
            assert attach_tg_message(shared, hasher, token, chat_id=1, message_id=2, lock=lock)
            wins = _race(hasher, token, 3, shared=shared, lock=lock)
            assert len(wins) == 1
            session = wins[0]
            info = lookup_session(
                shared,
                hasher,
                session.session_token,
                sliding_ttl=SESSION_TTL,
                absolute_ttl=ABSOLUTE_TTL,
                lock=lock,
            )
            assert info is not None
            assert delete_session(shared, hasher, session.session_token, lock=lock)
        assert revoke_all(shared, READY_ID, lock=lock) == 0
        assert purge_expired(shared, lock=lock) == 0
    finally:
        shared.close()


# ── looking up sessions: sliding and absolute expiry ──────────────────────────


def test_lookup_returns_the_session_and_slides_its_expiry(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    session = _login(conn, hasher)  # created at T0 + 5
    info = _lookup(conn, hasher, session.session_token, T0 + 1000)
    assert info == SessionInfo(
        telegram_id=ready_user, expires_at=T0 + 1000 + SESSION_TTL, created_at=T0 + 5
    )
    (row,) = _rows(conn)
    assert row["expires_at"] == T0 + 1000 + SESSION_TTL


def test_lookup_after_the_sliding_window_deletes_the_session(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    session = _login(conn, hasher)
    expires = session.expires_at
    assert _lookup(conn, hasher, session.session_token, expires) is not None  # last instant
    slid = _rows(conn)[0]["expires_at"]
    assert _lookup(conn, hasher, session.session_token, slid + 0.001) is None
    assert _rows(conn) == []


def test_lookup_never_slides_beyond_the_absolute_ttl(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    session = _login(conn, hasher)  # created at T0 + 5
    deadline = T0 + 5 + ABSOLUTE_TTL
    now = T0 + 5
    while now + SESSION_TTL / 2 < deadline:
        now += SESSION_TTL / 2  # an active user: a request every 15 minutes
        info = _lookup(conn, hasher, session.session_token, now)
        assert info is not None
        assert info.expires_at == min(now + SESSION_TTL, deadline)
        assert info.expires_at <= deadline
    assert _lookup(conn, hasher, session.session_token, deadline) is not None
    assert _rows(conn)[0]["expires_at"] == deadline
    assert _lookup(conn, hasher, session.session_token, deadline + 1) is None
    assert _rows(conn) == []


def test_lookup_enforces_the_absolute_ttl_even_if_expires_at_is_later(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    """A row slid under a longer absolute TTL (config lowered since) is still cut off."""
    token = new_token()
    SessionStore(conn).insert(
        hasher.hash(token), ready_user, "session", T0 + 10 * 86400, "ip", "ua", created_at=T0
    )
    assert _lookup(conn, hasher, token, T0 + ABSOLUTE_TTL + 1) is None
    assert _rows(conn) == []


@pytest.mark.parametrize("garbage", ["", None, 42, b"bytes", "x" * 10_000, "été"])
def test_lookup_rejects_garbage_cookies_without_touching_the_db(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int, garbage: object
) -> None:
    _login(conn, hasher)
    assert _lookup(conn, hasher, garbage, T0 + 10) is None
    assert _kinds(conn) == ["session"]


def test_lookup_does_not_accept_login_tokens_as_sessions(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    token = issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)
    assert _lookup(conn, hasher, token, T0 + 1) is None
    assert _kinds(conn) == ["login"]  # still usable for the real sign-in


@pytest.mark.parametrize(("name", "value"), [("sliding_ttl", 0), ("absolute_ttl", -1)])
def test_lookup_validates_ttls(
    conn: sqlite3.Connection, hasher: TokenHasher, name: str, value: int
) -> None:
    kwargs = {"sliding_ttl": SESSION_TTL, "absolute_ttl": ABSOLUTE_TTL, name: value}
    with pytest.raises(ValueError, match=name):
        lookup_session(conn, hasher, new_token(), **kwargs)  # type: ignore[arg-type]


# ── logout, revoke, purge ─────────────────────────────────────────────────────


def test_delete_session(conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int) -> None:
    first = _login(conn, hasher)
    second = _login(conn, hasher, now=T0 + 100)
    assert delete_session(conn, hasher, first.session_token)
    assert not delete_session(conn, hasher, first.session_token)
    assert not delete_session(conn, hasher, None)
    assert not delete_session(conn, hasher, "")
    assert _lookup(conn, hasher, first.session_token, T0 + 200) is None
    assert _lookup(conn, hasher, second.session_token, T0 + 200) is not None


def test_delete_session_leaves_login_tokens_alone(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    token = issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)
    assert not delete_session(conn, hasher, token)
    assert _kinds(conn) == ["login"]


def test_F61_revoke_all_removes_every_session_and_pending_login_token(
    conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int
) -> None:
    """F61: 'sign out everywhere' (and F18: /deny, /setup) must kill every session."""
    make_ready(conn, OTHER_ID)
    laptop = _login(conn, hasher)
    phone = _login(conn, hasher, now=T0 + 50)
    pending = issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0 + 60)
    other = _login(conn, hasher, OTHER_ID)
    other_pending = issue_login_token(conn, hasher, OTHER_ID, ttl_seconds=LOGIN_TTL, now=T0 + 60)

    assert revoke_all(conn, ready_user) == 3
    assert revoke_all(conn, ready_user) == 0
    assert _kinds(conn, ready_user) == []
    for session in (laptop, phone):
        assert _lookup(conn, hasher, session.session_token, T0 + 100) is None
    stale = consume_login_token(
        conn,
        hasher,
        pending,
        session_ttl=SESSION_TTL,
        absolute_ttl=ABSOLUTE_TTL,
        ip="ip",
        user_agent="ua",
        now=T0 + 100,
    )
    assert stale is None
    # The other user is untouched.
    assert _lookup(conn, hasher, other.session_token, T0 + 100) is not None
    assert _kinds(conn, OTHER_ID) == ["login", "session"]
    assert other_pending


def test_revoke_all_rejects_non_int_ids(conn: sqlite3.Connection) -> None:
    with pytest.raises(ValueError):
        revoke_all(conn, "4242")  # type: ignore[arg-type]


def test_purge_expired(conn: sqlite3.Connection, hasher: TokenHasher, ready_user: int) -> None:
    session = _login(conn, hasher)  # expires T0 + 5 + 1800
    issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)  # T0 + 300
    long_login = issue_login_token(conn, hasher, ready_user, ttl_seconds=7200, now=T0)
    assert purge_expired(conn, T0 + 301) == 1
    assert purge_expired(conn, T0 + 301) == 0
    assert purge_expired(conn, T0 + 5 + SESSION_TTL + 1) == 1
    assert _lookup(conn, hasher, session.session_token, T0 + 2000) is None
    assert _kinds(conn) == ["login"]
    assert long_login
    assert purge_expired(conn, T0 + 7201) == 1
    assert _rows(conn) == []


def test_secret_material_never_appears_in_logs(
    conn: sqlite3.Connection,
    hasher: TokenHasher,
    ready_user: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("DEBUG", logger="ynab_helper")
    token = issue_login_token(conn, hasher, ready_user, ttl_seconds=LOGIN_TTL, now=T0)
    result = consume_login_token(
        conn,
        hasher,
        token,
        session_ttl=SESSION_TTL,
        absolute_ttl=ABSOLUTE_TTL,
        ip="ip",
        user_agent="ua",
        now=T0,
    )
    assert result is not None
    _lookup(conn, hasher, result.session_token, T0 + 1)
    UserStore(conn).upsert(ready_user, state="denied")
    _lookup(conn, hasher, result.session_token, T0 + 2)
    text = caplog.text
    assert caplog.records
    for secret in (token, result.session_token, hasher.hash(token), SECRET):
        assert secret not in text
