"""db.py: connections, transactions, the migration runner and backups."""

from __future__ import annotations

import os
import sqlite3
import stat
import threading
from pathlib import Path

import pytest

from ynab_helper.errors import YnabHelperError
from ynab_helper.storage import db
from ynab_helper.storage.db import (
    Migration,
    MigrationError,
    backup_db,
    connect,
    ensure_columns,
    list_backups,
    migrate,
    table_columns,
    transaction,
    user_version,
)


def _make_db(path: Path, rows: int = 3) -> None:
    conn = connect(path)
    try:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        conn.executemany("INSERT INTO t (v) VALUES (?)", [(f"v{i}",) for i in range(rows)])
    finally:
        conn.close()


# ── connect ───────────────────────────────────────────────────────────────────


def test_connect_sets_pragmas_and_autocommit(tmp_path: Path) -> None:
    conn = connect(tmp_path / "a.db")
    try:
        assert conn.isolation_level is None
        assert conn.row_factory is sqlite3.Row
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == db.BUSY_TIMEOUT_MS
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        conn.execute("CREATE TABLE x (a)")
        conn.execute("INSERT INTO x VALUES (1)")
        assert not conn.in_transaction  # autocommit: nothing left open
    finally:
        conn.close()


def test_connect_readonly_rejects_writes(tmp_path: Path) -> None:
    path = tmp_path / "ro.db"
    _make_db(path)
    conn = connect(path, readonly=True)
    try:
        assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 3
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO t (v) VALUES ('x')")
    finally:
        conn.close()


def test_connect_enforces_same_thread_by_default(tmp_path: Path) -> None:
    conn = connect(tmp_path / "a.db")
    errors: list[Exception] = []

    def use() -> None:
        try:
            conn.execute("SELECT 1")
        except sqlite3.ProgrammingError as exc:
            errors.append(exc)

    thread = threading.Thread(target=use)
    thread.start()
    thread.join()
    conn.close()
    assert errors, "a default connection must not be usable from another thread"


# ── transaction ───────────────────────────────────────────────────────────────


def test_transaction_commits(tmp_path: Path) -> None:
    path = tmp_path / "a.db"
    _make_db(path, rows=0)
    conn = connect(path)
    with transaction(conn):
        assert conn.in_transaction
        conn.execute("INSERT INTO t (v) VALUES ('a')")
        conn.execute("INSERT INTO t (v) VALUES ('b')")
    assert not conn.in_transaction
    other = connect(path)
    assert other.execute("SELECT count(*) FROM t").fetchone()[0] == 2
    other.close()
    conn.close()


def test_transaction_rolls_back_on_error(tmp_path: Path) -> None:
    path = tmp_path / "a.db"
    _make_db(path, rows=0)
    conn = connect(path)
    with pytest.raises(RuntimeError), transaction(conn):
        conn.execute("INSERT INTO t (v) VALUES ('a')")
        raise RuntimeError("boom")
    assert not conn.in_transaction
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 0
    conn.close()


def test_transaction_takes_write_lock_immediately(tmp_path: Path) -> None:
    path = tmp_path / "a.db"
    _make_db(path, rows=0)
    first = connect(path)
    second = sqlite3.connect(path, timeout=0.05, isolation_level=None)
    try:
        # BEGIN IMMEDIATE holds the RESERVED lock even before the first write.
        with transaction(first), pytest.raises(sqlite3.OperationalError, match="locked"):
            second.execute("BEGIN IMMEDIATE")
    finally:
        second.close()
        first.close()


def test_nested_transaction_is_a_savepoint(tmp_path: Path) -> None:
    path = tmp_path / "a.db"
    _make_db(path, rows=0)
    conn = connect(path)
    with transaction(conn):
        conn.execute("INSERT INTO t (v) VALUES ('outer')")
        with pytest.raises(ValueError), transaction(conn):
            conn.execute("INSERT INTO t (v) VALUES ('inner')")
            raise ValueError("inner failure")
        with transaction(conn):
            conn.execute("INSERT INTO t (v) VALUES ('inner-ok')")
        assert conn.in_transaction
    assert [r[0] for r in conn.execute("SELECT v FROM t ORDER BY id")] == ["outer", "inner-ok"]
    conn.close()


def test_outer_rollback_discards_released_savepoints(tmp_path: Path) -> None:
    path = tmp_path / "a.db"
    _make_db(path, rows=0)
    conn = connect(path)
    with pytest.raises(KeyError), transaction(conn):
        with transaction(conn):
            conn.execute("INSERT INTO t (v) VALUES ('inner')")
        raise KeyError("outer failure")
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 0
    conn.close()


def test_transaction_requires_autocommit_connection(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "a.db")  # default isolation_level ''
    with pytest.raises(ValueError, match="autocommit"), transaction(conn):
        pass
    conn.close()


# ── schema helpers ────────────────────────────────────────────────────────────


def test_ensure_columns_is_idempotent(tmp_path: Path) -> None:
    conn = connect(tmp_path / "a.db")
    conn.execute("CREATE TABLE t (a TEXT)")
    assert ensure_columns(conn, "t", {"a": "TEXT", "b": "INTEGER NOT NULL DEFAULT 1"}) == ["b"]
    assert ensure_columns(conn, "t", {"a": "TEXT", "b": "INTEGER NOT NULL DEFAULT 1"}) == []
    assert table_columns(conn, "t") == {"a", "b"}
    with pytest.raises(ValueError):
        ensure_columns(conn, "t; DROP TABLE t", {"c": "TEXT"})
    conn.close()


# ── migrate ───────────────────────────────────────────────────────────────────


def _steps(log: list[str]) -> list[Migration]:
    def v1(conn: sqlite3.Connection) -> None:
        log.append("v1")
        conn.execute("CREATE TABLE IF NOT EXISTS a (x)")

    def v2(conn: sqlite3.Connection) -> None:
        log.append("v2")
        ensure_columns(conn, "a", {"y": "TEXT"})

    return [Migration(1, "create a", v1), Migration(2, "add a.y", v2)]


def test_migrate_applies_pending_steps_in_order(tmp_path: Path, backup_dir: Path) -> None:
    path = tmp_path / "m.db"
    conn = connect(path)
    calls: list[str] = []
    result = migrate(conn, list(reversed(_steps(calls))), db_path=path, backup_dir=backup_dir)
    assert calls == ["v1", "v2"]
    assert result.from_version == 0 and result.to_version == 2
    assert result.applied == (1, 2) and result.changed
    assert user_version(conn) == 2
    # Nothing to back up for a brand-new empty database.
    assert result.backup_path is None and not backup_dir.exists()

    again = migrate(conn, _steps(calls), db_path=path, backup_dir=backup_dir)
    assert calls == ["v1", "v2"] and again.applied == () and not again.changed
    conn.close()


def test_migrate_backs_up_before_applying(tmp_path: Path, backup_dir: Path) -> None:
    path = tmp_path / "m.db"
    _make_db(path, rows=5)
    conn = connect(path)
    result = migrate(conn, _steps([]), db_path=path, backup_dir=backup_dir)
    conn.close()
    assert result.backup_path is not None
    assert result.backup_path.parent == backup_dir
    assert result.backup_path.name.startswith("m.db.") and result.backup_path.name.endswith(
        ".pre-v2.db"
    )
    backup = sqlite3.connect(result.backup_path)
    # The backup is the pre-migration state: data present, new table absent, version 0.
    assert backup.execute("SELECT count(*) FROM t").fetchone()[0] == 5
    assert backup.execute("PRAGMA user_version").fetchone()[0] == 0
    assert not backup.execute("SELECT 1 FROM sqlite_master WHERE name = 'a'").fetchone()
    assert backup.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    backup.close()
    assert stat.S_IMODE(os.stat(result.backup_path).st_mode) == 0o600


def test_migrate_resumes_from_partial_version(tmp_path: Path, backup_dir: Path) -> None:
    path = tmp_path / "m.db"
    conn = connect(path)
    calls: list[str] = []
    migrate(conn, _steps(calls)[:1], db_path=path, backup_dir=backup_dir)
    result = migrate(conn, _steps(calls), db_path=path, backup_dir=backup_dir)
    assert calls == ["v1", "v2"]
    assert result.applied == (2,)
    assert result.backup_path is not None and result.backup_path.name.endswith(".pre-v2.db")
    conn.close()


def test_failed_step_rolls_back_and_keeps_version(tmp_path: Path, backup_dir: Path) -> None:
    path = tmp_path / "m.db"
    conn = connect(path)

    def broken(conn: sqlite3.Connection) -> None:
        conn.execute("CREATE TABLE half_done (x)")
        raise sqlite3.IntegrityError("simulated failure")

    steps = [*_steps([]), Migration(3, "broken", broken)]
    with pytest.raises(MigrationError) as info:
        migrate(conn, steps, db_path=path, backup_dir=backup_dir)
    assert isinstance(info.value, YnabHelperError)
    assert isinstance(info.value.__cause__, sqlite3.IntegrityError)
    assert user_version(conn) == 2  # steps 1 and 2 committed, 3 rolled back
    assert not conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'half_done'").fetchone()
    assert not conn.in_transaction
    conn.close()


def test_backup_failure_aborts_migration(tmp_path: Path) -> None:
    path = tmp_path / "m.db"
    _make_db(path)
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("x")
    conn = connect(path)
    calls: list[str] = []
    with pytest.raises(MigrationError, match="backup"):
        migrate(conn, _steps(calls), db_path=path, backup_dir=blocker / "backups")
    assert calls == [] and user_version(conn) == 0
    conn.close()


def test_migrate_leaves_newer_database_alone(tmp_path: Path, backup_dir: Path) -> None:
    path = tmp_path / "m.db"
    conn = connect(path)
    conn.execute("PRAGMA user_version = 7")
    calls: list[str] = []
    result = migrate(conn, _steps(calls), db_path=path, backup_dir=backup_dir)
    assert calls == [] and result.applied == () and user_version(conn) == 7
    conn.close()


def test_migrate_rejects_gaps_in_numbering(tmp_path: Path, backup_dir: Path) -> None:
    conn = connect(tmp_path / "m.db")
    with pytest.raises(ValueError, match=r"1\.\.N"):
        migrate(
            conn,
            [Migration(2, "x", lambda c: None)],
            db_path=tmp_path / "m.db",
            backup_dir=backup_dir,
        )
    conn.close()


def test_concurrent_migrators_apply_each_step_once(
    tmp_path: Path, backup_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two processes (bot and CLI) open the same DB at v0; each step must run exactly once.

    b reads user_version 0 and starts its pre-migration backup; meanwhile a migrates the file
    to v2. b must notice under the write lock (the in-transaction re-check) and skip both steps.
    """
    path = tmp_path / "m.db"
    _make_db(path)
    calls: list[str] = []
    a, b = connect(path), connect(path)
    real_backup = db.backup_db
    raced: list[db.MigrationResult] = []

    def backup_then_let_a_migrate(*args: object, **kwargs: object) -> Path:
        made = real_backup(*args, **kwargs)  # type: ignore[arg-type]
        if not raced:
            monkeypatch.setattr(db, "backup_db", real_backup)  # a backs up normally
            raced.append(migrate(a, _steps(calls), db_path=path, backup_dir=backup_dir))
        return made

    monkeypatch.setattr(db, "backup_db", backup_then_let_a_migrate)
    result_b = migrate(b, _steps(calls), db_path=path, backup_dir=backup_dir)

    assert raced and raced[0].applied == (1, 2)
    assert calls == ["v1", "v2"]  # each step ran once, in a's process
    assert result_b.from_version == 0  # b really did start from v0 …
    assert result_b.applied == () and result_b.to_version == 2  # … and applied nothing
    assert user_version(b) == 2
    a.close()
    b.close()


# ── backups ───────────────────────────────────────────────────────────────────


def test_backup_retention_keeps_newest_ten_per_db(tmp_path: Path, backup_dir: Path) -> None:
    path = tmp_path / "transactions_1.db"
    _make_db(path)
    other = tmp_path / "transactions_12.db"
    _make_db(other)
    backup_dir.mkdir()
    unrelated = backup_dir / "transactions_1.db.bak"
    unrelated.write_text("manual copy")
    other_backup = backup_db(other, backup_dir, tag="manual")

    made = [backup_db(path, backup_dir, tag=f"t{i}") for i in range(12)]
    kept = list_backups(backup_dir, path.name)
    assert len(kept) == 10
    assert kept == made[-10:]  # oldest two pruned, order preserved within one second
    assert not made[0].exists() and not made[1].exists()
    assert unrelated.exists() and other_backup.exists()
    assert list_backups(backup_dir, other.name) == [other_backup]
    assert not list(backup_dir.glob("*.tmp"))


def test_backup_is_private_while_being_written(
    tmp_path: Path, backup_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The users DB holds YNAB tokens: its copy must never be world-readable, not even while
    SQLite writes it under a permissive umask (CLI and dev runs)."""
    path = tmp_path / "bot_users.db"
    _make_db(path)
    real_copy = db._copy_database
    modes: list[int | None] = []

    def mode_of(dest: Path) -> int | None:
        return stat.S_IMODE(os.stat(dest).st_mode) if dest.exists() else None

    def spy(source: Path, dest: Path) -> None:
        modes.append(mode_of(dest))  # before SQLite touches it: must already exist, private
        real_copy(source, dest)
        modes.append(mode_of(dest))

    monkeypatch.setattr(db, "_copy_database", spy)
    old_umask = os.umask(0o022)
    try:
        made = backup_db(path, backup_dir, tag="umask")
    finally:
        os.umask(old_umask)
    assert modes == [0o600, 0o600]
    assert stat.S_IMODE(os.stat(made).st_mode) == 0o600


def test_backup_replaces_stale_temp_file(
    tmp_path: Path, backup_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "a.db"
    _make_db(path)
    backup_dir.mkdir()
    monkeypatch.setattr(db, "utc_stamp", lambda: "20261016T120000Z")
    stale = backup_dir / "a.db.20261016T120000Z.t.db.tmp"
    stale.write_text("left over by a crashed run")
    os.chmod(stale, 0o644)
    made = backup_db(path, backup_dir, tag="t")
    assert made.name == "a.db.20261016T120000Z.t.db"
    assert stat.S_IMODE(os.stat(made).st_mode) == 0o600
    restored = sqlite3.connect(made)
    assert restored.execute("SELECT count(*) FROM t").fetchone()[0] == 3
    restored.close()
    assert not stale.exists()


def test_backup_rejects_bad_tag_and_missing_file(tmp_path: Path, backup_dir: Path) -> None:
    path = tmp_path / "a.db"
    _make_db(path)
    with pytest.raises(ValueError):
        backup_db(path, backup_dir, tag="../evil")
    with pytest.raises(FileNotFoundError):
        backup_db(tmp_path / "missing.db", backup_dir, tag="x")


def test_backup_of_wal_database_contains_uncheckpointed_writes(
    tmp_path: Path, backup_dir: Path
) -> None:
    path = tmp_path / "a.db"
    conn = connect(path)
    conn.execute("PRAGMA wal_autocheckpoint = 0")
    conn.execute("CREATE TABLE t (v)")
    conn.execute("INSERT INTO t VALUES ('only-in-wal')")
    copy = backup_db(path, backup_dir, tag="wal")
    conn.close()
    restored = sqlite3.connect(copy)
    assert restored.execute("SELECT v FROM t").fetchall() == [("only-in-wal",)]
    restored.close()


def test_path_helpers(tmp_path: Path) -> None:
    assert db.tx_db_path(tmp_path, 59213669) == tmp_path / "transactions_59213669.db"
    assert db.users_db_path(tmp_path) == tmp_path / "bot_users.db"
    assert db.default_backup_dir(tmp_path) == tmp_path / "backups"
    assert db.tx_db_owner(tmp_path / "transactions_59213669.db") == 59213669
    assert db.tx_db_owner("transactions_59213669.db.bak") is None
    assert db.tx_db_owner("bot_users.db") is None
