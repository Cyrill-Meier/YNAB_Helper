"""users_schema.py + users.py: users DB migration, UserStore and SessionStore."""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import pytest

from ynab_helper.storage import (
    RecordNotFound,
    SessionStore,
    UserStore,
    connect,
    open_users_db,
)
from ynab_helper.storage.db import list_backups, migrate_users_db_conn, table_columns, user_version

from .helpers import OLD_USERS_DDL, PROD_USERS_DDL

TOKEN = "ynab-pat-secret"


def _seed(path: Path, ddl: str, users: list[tuple], sessions: list[tuple]) -> None:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(ddl)
    for user in users:
        conn.execute(
            "INSERT INTO users (telegram_id, state, ynab_token, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            user,
        )
    for session in sessions:
        conn.execute(
            "INSERT INTO web_sessions (token_hash, telegram_id, kind, created_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?)",
            session,
        )
    conn.commit()
    conn.close()


# ── migration ─────────────────────────────────────────────────────────────────


def test_production_users_db_migration(tmp_path: Path, backup_dir: Path) -> None:
    path = tmp_path / "bot_users.db"
    now = time.time()
    _seed(
        path,
        PROD_USERS_DDL,
        users=[
            (59213669, "ready", TOKEN, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
            (2, "approved", None, "2026-02-01T00:00:00Z", "2026-02-01T00:00:00Z"),
            (3, "awaiting_crypto_eth", TOKEN, "2026-03-01T00:00:00Z", "2026-03-01T00:00:00Z"),
        ],
        sessions=[
            ("expired", 59213669, "session", now - 7200, now - 3600),
            ("live", 59213669, "session", now - 60, now + 3600),
            ("old-login", 2, "login", now - 900, now - 600),
        ],
    )
    conn, result = migrate_users_db_conn(path, backup_dir=backup_dir)
    try:
        assert result.applied == (1,) and user_version(conn) == 1
        users = {u.telegram_id: u for u in UserStore(conn).list_all()}
        assert users[59213669].state == "ready" and users[59213669].ynab_token == TOKEN
        assert users[59213669].auto_approve is True
        assert users[2].state == "awaiting_token"  # legacy 'approved' mapped
        assert users[2].updated_at != "2026-02-01T00:00:00Z"
        assert users[3].state == "awaiting_crypto_eth"
        remaining = [r[0] for r in conn.execute("SELECT token_hash FROM web_sessions")]
        assert remaining == ["live"]
    finally:
        conn.close()
    backups = list_backups(backup_dir, "bot_users.db")
    assert len(backups) == 1
    restored = sqlite3.connect(backups[0])
    assert restored.execute("SELECT state FROM users WHERE telegram_id = 2").fetchone() == (
        "approved",
    )
    restored.close()

    conn, again = migrate_users_db_conn(path, backup_dir=backup_dir)
    assert again.applied == ()
    conn.close()


def test_old_users_db_gets_missing_columns(tmp_path: Path, backup_dir: Path) -> None:
    path = tmp_path / "bot_users.db"
    _seed(path, OLD_USERS_DDL, users=[(1, "ready", TOKEN, "t", "t")], sessions=[])
    conn = open_users_db(path, backup_dir=backup_dir)
    assert {"auto_approve", "crypto_account_id", "btc_xpub", "eth_address"} <= table_columns(
        conn, "users"
    )
    assert {"tg_chat_id", "tg_message_id"} <= table_columns(conn, "web_sessions")
    user = UserStore(conn).get(1)
    assert user is not None and user.auto_approve is True and user.crypto_account_id is None
    indexes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert {"idx_web_sessions_expires", "idx_web_sessions_telegram"} <= indexes
    conn.close()


def test_fresh_users_db(users_conn: sqlite3.Connection) -> None:
    assert user_version(users_conn) == 1
    assert UserStore(users_conn).list_all() == []


# ── UserStore ─────────────────────────────────────────────────────────────────


def test_upsert_creates_then_updates_only_given_fields(users_conn: sqlite3.Connection) -> None:
    store = UserStore(users_conn)
    created = store.upsert(42, chat_id=42, username="cy", first_name="Cyrill")
    assert created.state == "pending" and created.auto_approve is True
    assert created.created_at == created.updated_at
    ready = store.upsert(
        42,
        ynab_token=TOKEN,
        budget_id="b",
        account_id="a",
        state="ready",
        auto_approve=False,
        temp_data=[{"id": "b", "name": "Budget"}],
    )
    assert ready.username == "cy" and ready.is_ready and ready.auto_approve is False
    assert ready.temp == [{"id": "b", "name": "Budget"}]
    assert ready.created_at == created.created_at
    assert store.get(42) == ready
    assert store.get(43) is None
    assert store.upsert(42) == ready  # no fields → no write


def test_upsert_rejects_unknown_fields_and_bad_values(users_conn: sqlite3.Connection) -> None:
    store = UserStore(users_conn)
    with pytest.raises(ValueError, match="unknown user fields"):
        store.upsert(1, telegram_id=2)
    with pytest.raises(ValueError, match="unknown user fields"):
        store.upsert(1, created_at="x")
    with pytest.raises(ValueError, match="unknown user fields"):
        store.upsert(1, **{"state = 'ready' --": "x"})
    with pytest.raises(ValueError, match="state"):
        store.upsert(1, state="approved")  # legacy state cannot be written
    with pytest.raises(ValueError):
        store.upsert(1, auto_approve=2)
    with pytest.raises(ValueError):
        store.upsert(1, chat_id="123")
    with pytest.raises(ValueError):
        store.upsert(1, ynab_token=123)
    with pytest.raises(ValueError):
        store.upsert("1")  # type: ignore[arg-type]
    assert store.get(1) is None


def test_set_state(users_conn: sqlite3.Connection) -> None:
    store = UserStore(users_conn)
    store.upsert(7, temp_data='{"step": 1}')
    user = store.set_state(7, "awaiting_token")
    assert user.state == "awaiting_token" and user.temp == {"step": 1}
    user = store.set_state(7, "awaiting_budget", temp_data=None)
    assert user.temp_data is None
    with pytest.raises(RecordNotFound):
        store.set_state(8, "ready")
    with pytest.raises(ValueError):
        store.set_state(7, "nonsense")


def test_user_repr_hides_secrets(users_conn: sqlite3.Connection) -> None:
    user = UserStore(users_conn).upsert(
        1, ynab_token=TOKEN, btc_xpub="xpub-secret", eth_address="0xabc", temp_data="tmp"
    )
    text = repr(user)
    assert TOKEN not in text and "xpub-secret" not in text and "0xabc" not in text
    assert not user.is_ready


def test_list_all_orders_by_creation(users_conn: sqlite3.Connection) -> None:
    store = UserStore(users_conn)
    for tid in (3, 1, 2):
        store.upsert(tid)
    users_conn.execute("UPDATE users SET created_at = '2026-01-0' || telegram_id || 'T00:00:00Z'")
    assert [u.telegram_id for u in store.list_all()] == [1, 2, 3]


# ── SessionStore ──────────────────────────────────────────────────────────────


def test_insert_get_slide_delete(users_conn: sqlite3.Connection) -> None:
    sessions = SessionStore(users_conn)
    now = time.time()
    record = sessions.insert(
        "h-session", 1, "session", now + 1800, "203.0.113.9" * 10, "UA" * 300, created_at=now
    )
    assert len(record.ip or "") == 64 and len(record.user_agent or "") == 200
    got = sessions.get_session("h-session")
    assert got == record
    assert sessions.slide("h-session", now + 3600)
    assert sessions.get_session("h-session").expires_at == now + 3600  # type: ignore[union-attr]
    assert sessions.get_session("h-session", now=now + 7200) is None  # expired view
    assert sessions.get_session("h-session") is not None  # …but not deleted
    assert sessions.delete("h-session") and not sessions.delete("h-session")
    assert sessions.get_session("h-session") is None
    with pytest.raises(ValueError):
        sessions.insert("h", 1, "cookie", now, None, None)
    with pytest.raises(ValueError):
        sessions.insert("", 1, "login", now, None, None)


def test_login_rows_are_never_returned_as_sessions(users_conn: sqlite3.Connection) -> None:
    sessions = SessionStore(users_conn)
    sessions.insert("h-login", 1, "login", time.time() + 300, "bot", "telegram")
    assert sessions.get_session("h-login") is None
    assert not sessions.slide("h-login", time.time() + 9999)


def test_consume_login_is_single_use(users_conn: sqlite3.Connection) -> None:
    sessions = SessionStore(users_conn)
    now = time.time()
    sessions.insert("h-login", 1, "login", now + 300, "bot", "telegram")
    assert sessions.attach_message("h-login", 1001, 77)
    first = sessions.consume_login("h-login", now)
    assert first is not None and first.telegram_id == 1
    assert (first.tg_chat_id, first.tg_message_id) == (1001, 77)
    assert sessions.consume_login("h-login", now) is None  # second use fails
    assert users_conn.execute("SELECT count(*) FROM web_sessions").fetchone()[0] == 0
    assert not sessions.attach_message("h-login", 1, 2)


def test_consume_login_rejects_and_burns_expired_tokens(users_conn: sqlite3.Connection) -> None:
    sessions = SessionStore(users_conn)
    now = time.time()
    sessions.insert("h-old", 1, "login", now - 1, "bot", "telegram")
    sessions.insert("h-session", 1, "session", now + 100, "ip", "ua")
    assert sessions.consume_login("h-old", now) is None
    assert [r[0] for r in users_conn.execute("SELECT token_hash FROM web_sessions")] == [
        "h-session"
    ]
    assert sessions.consume_login("h-session", now) is None  # sessions are not login tokens
    assert sessions.get_session("h-session") is not None


def _race_consume(path: Path, token_hash: str, contenders: int) -> list[object]:
    barrier = threading.Barrier(contenders)
    wins: list[object] = []

    def worker() -> None:
        conn = connect(path)
        try:
            barrier.wait()
            result = SessionStore(conn).consume_login(token_hash, time.time())
            if result is not None:
                wins.append(result)
        finally:
            conn.close()

    threads = [threading.Thread(target=worker) for _ in range(contenders)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return wins


def test_F86_concurrent_consume_mints_at_most_one(tmp_path: Path, backup_dir: Path) -> None:
    """F86: racing requests with the same login token: exactly one wins."""
    path = tmp_path / "bot_users.db"
    open_users_db(path, backup_dir=backup_dir).close()
    for round_no in range(20):
        setup = connect(path)
        SessionStore(setup).insert(f"h{round_no}", 1, "login", time.time() + 300, "bot", "tg")
        setup.close()
        wins = _race_consume(path, f"h{round_no}", contenders=4)
        assert len(wins) == 1, f"round {round_no}: {len(wins)} winners"


def test_shared_connection_with_lock(tmp_path: Path, backup_dir: Path) -> None:
    conn = open_users_db(tmp_path / "bot_users.db", backup_dir=backup_dir, check_same_thread=False)
    lock = threading.RLock()
    users = UserStore(conn, lock=lock)
    sessions = SessionStore(conn, lock=lock)
    errors: list[BaseException] = []

    def work(tid: int) -> None:
        try:
            for i in range(20):
                users.upsert(tid, username=f"u{i}")
                sessions.insert(f"{tid}-{i}", tid, "login", time.time() + 60, "ip", "ua")
                assert sessions.consume_login(f"{tid}-{i}", time.time()) is not None
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(tid,)) for tid in range(1, 6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert [u.username for u in users.list_all()] == ["u19"] * 5
    conn.close()


def test_F18_revoke_all_removes_sessions_and_login_tokens(users_conn: sqlite3.Connection) -> None:
    """F18/F61: /deny, /setup and 'sign out everywhere' revoke every session of the user."""
    sessions = SessionStore(users_conn)
    now = time.time()
    sessions.insert("s1", 1, "session", now + 100, "ip", "ua")
    sessions.insert("s2", 1, "session", now + 100, "ip", "ua")
    sessions.insert("l1", 1, "login", now + 100, "bot", "telegram")
    sessions.insert("other", 2, "session", now + 100, "ip", "ua")
    assert sessions.revoke_all(1) == 3
    assert sessions.revoke_all(1) == 0
    assert [r[0] for r in users_conn.execute("SELECT token_hash FROM web_sessions")] == ["other"]


def test_purge_expired(users_conn: sqlite3.Connection) -> None:
    sessions = SessionStore(users_conn)
    now = time.time()
    sessions.insert("old", 1, "session", now - 10, "ip", "ua")
    sessions.insert("old-login", 1, "login", now - 1, "bot", "tg")
    sessions.insert("fresh", 1, "session", now + 10, "ip", "ua")
    assert sessions.purge_expired(now) == 2
    assert [r[0] for r in users_conn.execute("SELECT token_hash FROM web_sessions")] == ["fresh"]
