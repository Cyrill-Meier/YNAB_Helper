"""SQLite plumbing shared by every store: connections, transactions, migrations, backups.

Conventions (docs/DESIGN.md §4.4):

* Connections run in autocommit mode (``isolation_level=None``). Every multi-statement write
  goes through :func:`transaction`, which issues ``BEGIN IMMEDIATE`` so the write lock is taken
  up front: a concurrent writer waits (``busy_timeout``) instead of failing half-way through.
* **No network I/O inside** :func:`transaction` — the write lock would be held across it and
  block the bot, the web UI and the CLI for the whole round-trip.
* Schema versions live in ``PRAGMA user_version``. :func:`migrate` copies the file with the
  SQLite backup API before it applies anything, and every step is idempotent.
"""

from __future__ import annotations

import contextlib
import itertools
import logging
import os
import re
import sqlite3
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from ynab_helper.errors import YnabHelperError
from ynab_helper.timeutil import utc_stamp

log = logging.getLogger(__name__)

BUSY_TIMEOUT_MS = 10_000
BACKUP_KEEP = 10
BACKUPS_DIRNAME = "backups"
USERS_DB_NAME = "bot_users.db"

_TX_DB_RE = re.compile(r"^transactions_(\d+)\.db$")
_BACKUP_TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_savepoint_ids = itertools.count(1)


# ── Errors ────────────────────────────────────────────────────────────────────


class StorageError(YnabHelperError):
    """Base class for persistence-layer failures."""


class RecordNotFound(StorageError):
    """The row an update/state change refers to does not exist."""


class RecordExists(StorageError):
    """An insert collided with an existing primary key."""


class MigrationError(StorageError):
    """A schema migration (or the backup that must precede it) failed.

    The database is left at the last fully applied version; nothing half-applied is committed.
    """


# ── Paths ─────────────────────────────────────────────────────────────────────


def users_db_path(data_dir: str | os.PathLike[str]) -> Path:
    """``<data_dir>/bot_users.db``."""
    return Path(data_dir) / USERS_DB_NAME


def tx_db_path(data_dir: str | os.PathLike[str], telegram_id: int) -> Path:
    """Per-user transactions DB: ``<data_dir>/transactions_<telegram_id>.db`` (v1 naming)."""
    return Path(data_dir) / f"transactions_{int(telegram_id)}.db"


def tx_db_owner(path: str | os.PathLike[str]) -> int | None:
    """Telegram id encoded in a ``transactions_<id>.db`` file name, or None for other files."""
    match = _TX_DB_RE.match(Path(path).name)
    return int(match.group(1)) if match else None


def default_backup_dir(data_dir: str | os.PathLike[str]) -> Path:
    """``<data_dir>/backups``."""
    return Path(data_dir) / BACKUPS_DIRNAME


# ── Connections and transactions ──────────────────────────────────────────────


def connect(
    path: str | os.PathLike[str],
    *,
    readonly: bool = False,
    check_same_thread: bool = True,
) -> sqlite3.Connection:
    """Open a SQLite connection configured for this app.

    WAL journal, ``busy_timeout`` 10 s, foreign keys on, ``sqlite3.Row`` rows and autocommit
    (``isolation_level=None``); use :func:`transaction` for explicit transactions.

    ``check_same_thread=False`` is only for a connection that is deliberately shared between
    threads *and* guarded by a lock by its owner (the bot's users DB).
    """
    raw = os.fspath(path)
    timeout = BUSY_TIMEOUT_MS / 1000
    if readonly:
        uri = Path(raw).resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(
            uri,
            uri=True,
            timeout=timeout,
            isolation_level=None,
            check_same_thread=check_same_thread,
        )
    else:
        conn = sqlite3.connect(
            raw,
            timeout=timeout,
            isolation_level=None,
            check_same_thread=check_same_thread,
        )
    try:
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA foreign_keys = ON")
        if not readonly:
            mode = str(conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]).lower()
            if mode != "wal" and raw != ":memory:":
                log.warning("SQLite refused WAL mode for %s (journal_mode=%s)", raw, mode)
    except BaseException:
        conn.close()
        raise
    return conn


@contextlib.contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """``BEGIN IMMEDIATE`` … ``COMMIT``; ``ROLLBACK`` and re-raise on any exception.

    Nested use becomes a ``SAVEPOINT``: an inner failure rolls back only the inner block and
    re-raises, so callers can compose store methods inside one outer transaction.
    Never perform network I/O inside the block.
    """
    if conn.isolation_level is not None:
        raise ValueError(
            "transaction() needs an autocommit connection (isolation_level=None); use connect()"
        )
    if conn.in_transaction:
        yield from _savepoint(conn)
        return
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        _rollback_quietly(conn)
        raise
    try:
        conn.execute("COMMIT")
    except BaseException:
        _rollback_quietly(conn)
        raise


def _savepoint(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    name = f"sp_{next(_savepoint_ids)}"
    conn.execute(f"SAVEPOINT {name}")
    try:
        yield conn
    except BaseException:
        # SQLite may already have rolled back the whole transaction (e.g. SQLITE_FULL);
        # then the savepoint is gone and the outer block will see the original error.
        if conn.in_transaction:
            with contextlib.suppress(sqlite3.Error):
                conn.execute(f"ROLLBACK TO SAVEPOINT {name}")
                conn.execute(f"RELEASE SAVEPOINT {name}")
        raise
    conn.execute(f"RELEASE SAVEPOINT {name}")


def _rollback_quietly(conn: sqlite3.Connection) -> None:
    if conn.in_transaction:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            log.exception("ROLLBACK failed")


# ── Schema helpers (used by the *_schema modules) ─────────────────────────────


def _check_ident(name: str) -> str:
    if not _IDENT_RE.match(name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return name


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    """True if a table named ``table`` exists in the main schema."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Column names of ``table`` (empty set if the table does not exist)."""
    return {row[1] for row in conn.execute(f"PRAGMA table_info({_check_ident(table)})")}


def ensure_columns(conn: sqlite3.Connection, table: str, columns: Mapping[str, str]) -> list[str]:
    """``ALTER TABLE … ADD COLUMN`` every column of ``columns`` that ``table`` lacks.

    ``columns`` maps name → column definition (type, default). Definitions must be valid for
    ``ADD COLUMN`` (a ``NOT NULL`` column needs a non-NULL default). Returns the added names.
    """
    existing = table_columns(conn, table)
    added: list[str] = []
    for name, definition in columns.items():
        if name in existing:
            continue
        conn.execute(
            f"ALTER TABLE {_check_ident(table)} ADD COLUMN {_check_ident(name)} {definition}"
        )
        added.append(name)
    return added


def user_version(conn: sqlite3.Connection) -> int:
    """``PRAGMA user_version`` of the main database."""
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _has_schema(conn: sqlite3.Connection) -> bool:
    return conn.execute("SELECT count(*) FROM sqlite_master").fetchone()[0] > 0


# ── Migrations ────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class Migration:
    """One schema step. ``apply`` runs inside a transaction and must be idempotent."""

    version: int
    description: str
    apply: Callable[[sqlite3.Connection], None]


@dataclass(frozen=True, slots=True)
class MigrationResult:
    """What :func:`migrate` did to one database."""

    db_path: Path | None
    from_version: int
    to_version: int
    applied: tuple[int, ...]
    backup_path: Path | None

    @property
    def changed(self) -> bool:
        return bool(self.applied)


def _validated_steps(migrations: Sequence[Migration]) -> list[Migration]:
    steps = sorted(migrations, key=lambda m: m.version)
    for expected, step in enumerate(steps, start=1):
        if step.version != expected:
            raise ValueError(
                f"migrations must be numbered 1..N without gaps; got {[m.version for m in steps]}"
            )
    return steps


def migrate(
    conn: sqlite3.Connection,
    migrations: Sequence[Migration],
    *,
    db_path: str | os.PathLike[str] | None,
    backup_dir: str | os.PathLike[str],
) -> MigrationResult:
    """Bring ``conn`` to the newest version in ``migrations``.

    If anything is pending and the database already holds a schema, it is first copied with
    :func:`backup_db` (tag ``pre-v<target>``); a failed backup aborts the migration. Each step
    runs in its own transaction together with its ``user_version`` bump, so a crash leaves the
    file at the last completed version. A database newer than the code is left untouched
    (migrations are additive, so older code keeps working on it).

    Raises :class:`MigrationError` (with the original exception as ``__cause__``).
    """
    steps = _validated_steps(migrations)
    target = steps[-1].version if steps else 0
    path = Path(db_path) if db_path not in (None, "", ":memory:") else None
    current = user_version(conn)
    if current > target:
        log.warning(
            "%s is at schema v%d, newer than this code (v%d); leaving it unchanged",
            path or "database",
            current,
            target,
        )
        return MigrationResult(path, current, current, (), None)
    pending = [m for m in steps if m.version > current]
    if not pending:
        return MigrationResult(path, current, current, (), None)

    backup_path: Path | None = None
    if path is not None and _has_schema(conn):
        try:
            backup_path = backup_db(path, backup_dir, tag=f"pre-v{target}")
        except (OSError, sqlite3.Error, ValueError) as exc:
            raise MigrationError(
                f"backup of {path.name} failed, migration to v{target} not applied: {exc}",
                user_message="Database backup failed; the upgrade was not applied.",
            ) from exc

    applied: list[int] = []
    for step in pending:
        try:
            with transaction(conn):
                # Another process may have migrated between our read and the write lock.
                if user_version(conn) >= step.version:
                    continue
                step.apply(conn)
                conn.execute(f"PRAGMA user_version = {int(step.version)}")
        except Exception as exc:
            raise MigrationError(
                f"migration v{step.version} ({step.description}) of "
                f"{path.name if path else 'database'} failed: {exc}",
                user_message="Database upgrade failed.",
            ) from exc
        applied.append(step.version)
        log.info(
            "migrated %s to v%d (%s)",
            path.name if path else "database",
            step.version,
            step.description,
        )
    return MigrationResult(path, current, user_version(conn), tuple(applied), backup_path)


# ── Backups ───────────────────────────────────────────────────────────────────


def _backup_name_re(db_name: str) -> re.Pattern[str]:
    return re.compile(rf"^{re.escape(db_name)}\.(\d{{8}}T\d{{6}}Z)(?:_(\d+))?\.(.+)\.db$")


def _unique_backup_path(backup_dir: Path, db_name: str, tag: str) -> Path:
    # Backups made within the same second get an increasing sequence suffix (whatever their
    # tag), so (stamp, seq) orders them correctly for retention.
    stamp = utc_stamp()
    pattern = _backup_name_re(db_name)
    taken = [
        int(match.group(2) or 0)
        for entry in backup_dir.iterdir()
        if (match := pattern.match(entry.name)) and match.group(1) == stamp
    ]
    if not taken:
        return backup_dir / f"{db_name}.{stamp}.{tag}.db"
    return backup_dir / f"{db_name}.{stamp}_{max(taken) + 1}.{tag}.db"


def list_backups(backup_dir: str | os.PathLike[str], db_name: str) -> list[Path]:
    """Backups of ``db_name`` in ``backup_dir``, oldest first (unrelated files are ignored)."""
    directory = Path(backup_dir)
    if not directory.is_dir():
        return []
    pattern = _backup_name_re(db_name)
    found: list[tuple[str, int, Path]] = []
    for entry in directory.iterdir():
        match = pattern.match(entry.name)
        if match and entry.is_file():
            found.append((match.group(1), int(match.group(2) or 0), entry))
    found.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in found]


def backup_db(
    db_path: str | os.PathLike[str],
    backup_dir: str | os.PathLike[str],
    *,
    tag: str,
    keep: int = BACKUP_KEEP,
) -> Path:
    """Copy ``db_path`` with the SQLite backup API and keep the newest ``keep`` copies.

    The copy is written to ``<backup_dir>/<name>.<UTC stamp>.<tag>.db`` via a temporary file
    that is integrity-checked (``PRAGMA quick_check``) and atomically renamed, so a crash never
    leaves a truncated backup with a valid-looking name. Files are created 0600 (the users DB
    holds YNAB tokens). Only files matching this DB's backup pattern are ever pruned.
    """
    source = Path(db_path)
    if not source.is_file():
        raise FileNotFoundError(f"cannot back up missing database {source}")
    if not _BACKUP_TAG_RE.match(tag):
        raise ValueError(f"invalid backup tag {tag!r}")
    if keep < 1:
        raise ValueError("keep must be >= 1")
    directory = Path(backup_dir)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    dest = _unique_backup_path(directory, source.name, tag)
    tmp = dest.with_name(dest.name + ".tmp")
    try:
        _create_private_file(tmp)
        _copy_database(source, tmp)
        os.chmod(tmp, 0o600)  # belt and braces: the copy never widens it, but be explicit
        os.replace(tmp, dest)
    finally:
        for leftover in (tmp, Path(f"{tmp}-wal"), Path(f"{tmp}-shm"), Path(f"{tmp}-journal")):
            with contextlib.suppress(FileNotFoundError):
                leftover.unlink()
    log.info("backed up %s to %s", source.name, dest)
    _prune_backups(directory, source.name, keep)
    return dest


def _create_private_file(path: Path) -> None:
    """Create ``path`` empty with mode 0600 whatever the umask (an empty file is a valid DB).

    SQLite would otherwise create the backup with the process umask, leaving a copy of the
    users DB (YNAB tokens) world-readable while it is written. SQLite gives its journal files
    the database file's mode, so they stay private too. A stale temp file from a crashed run
    is replaced; O_EXCL keeps us from writing through a planted symlink.
    """
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags, 0o600)
    except FileExistsError:
        path.unlink()
        fd = os.open(path, flags, 0o600)
    os.close(fd)
    # The umask can only narrow 0600 (an odd one could even drop the owner's write bit).
    os.chmod(path, 0o600)


def _copy_database(source: Path, dest: Path) -> None:
    src = sqlite3.connect(os.fspath(source), timeout=BUSY_TIMEOUT_MS / 1000)
    try:
        dst = sqlite3.connect(os.fspath(dest))
        try:
            src.backup(dst)
            # A WAL source yields a WAL-flagged copy; make the backup one self-contained file.
            dst.execute("PRAGMA journal_mode = DELETE")
            status = dst.execute("PRAGMA quick_check").fetchone()[0]
            if status != "ok":
                raise sqlite3.DatabaseError(f"backup failed integrity check: {status}")
        finally:
            dst.close()
    finally:
        src.close()


def _prune_backups(directory: Path, db_name: str, keep: int) -> None:
    for stale in list_backups(directory, db_name)[:-keep]:
        try:
            stale.unlink()
        except OSError as exc:
            log.warning("could not remove old backup %s: %s", stale, exc)


# ── Open + migrate helpers ────────────────────────────────────────────────────


def _open_and_migrate(
    path: str | os.PathLike[str],
    migrations: Sequence[Migration],
    *,
    backup_dir: str | os.PathLike[str],
    check_same_thread: bool = True,
) -> tuple[sqlite3.Connection, MigrationResult]:
    db_file = Path(path)
    if os.fspath(path) != ":memory:":
        db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_file, check_same_thread=check_same_thread)
    try:
        result = migrate(conn, migrations, db_path=db_file, backup_dir=backup_dir)
    except BaseException:
        conn.close()
        raise
    return conn, result


def open_tx_db(
    path: str | os.PathLike[str],
    *,
    default_account_id: str = "",
    backup_dir: str | os.PathLike[str],
    csv_cache_dir: str | os.PathLike[str] | None = None,
) -> sqlite3.Connection:
    """Connect to a per-user (or CLI) transactions DB and migrate it to the current schema.

    ``default_account_id`` is the user's configured YNAB account; legacy ledger rows without an
    account are assigned to it during the v0 → v1 migration (``''`` leaves them unclaimed).
    ``csv_cache_dir`` (``<data_dir>/csv_cache/<telegram_id>``) lets the migration register the
    newest cached statement as a ``legacy`` csv_files row.
    """
    conn, _ = migrate_tx_db_conn(
        path,
        default_account_id=default_account_id,
        backup_dir=backup_dir,
        csv_cache_dir=csv_cache_dir,
    )
    return conn


def migrate_tx_db_conn(
    path: str | os.PathLike[str],
    *,
    default_account_id: str = "",
    backup_dir: str | os.PathLike[str],
    csv_cache_dir: str | os.PathLike[str] | None = None,
) -> tuple[sqlite3.Connection, MigrationResult]:
    """Like :func:`open_tx_db` but also returns the :class:`MigrationResult`."""
    # Imported lazily: tx_schema imports this module for Migration.
    from ynab_helper.storage import tx_schema

    ctx = tx_schema.TxMigrationContext(
        default_account_id=default_account_id or "",
        csv_cache_dir=Path(csv_cache_dir) if csv_cache_dir is not None else None,
    )
    return _open_and_migrate(path, tx_schema.migrations(ctx), backup_dir=backup_dir)


def open_users_db(
    path: str | os.PathLike[str],
    *,
    backup_dir: str | os.PathLike[str],
    check_same_thread: bool = True,
) -> sqlite3.Connection:
    """Connect to ``bot_users.db`` and migrate it to the current schema.

    Pass ``check_same_thread=False`` only for a connection shared between threads under a lock
    (give the same lock to :class:`~ynab_helper.storage.users.UserStore` and
    :class:`~ynab_helper.storage.users.SessionStore`).
    """
    conn, _ = migrate_users_db_conn(
        path, backup_dir=backup_dir, check_same_thread=check_same_thread
    )
    return conn


def migrate_users_db_conn(
    path: str | os.PathLike[str],
    *,
    backup_dir: str | os.PathLike[str],
    check_same_thread: bool = True,
) -> tuple[sqlite3.Connection, MigrationResult]:
    """Like :func:`open_users_db` but also returns the :class:`MigrationResult`."""
    # Imported lazily: users_schema imports this module for Migration.
    from ynab_helper.storage import users_schema

    return _open_and_migrate(
        path,
        users_schema.migrations(),
        backup_dir=backup_dir,
        check_same_thread=check_same_thread,
    )
