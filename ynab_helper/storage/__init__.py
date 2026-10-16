"""Persistence layer (docs/DESIGN.md §4.4, §5).

Two kinds of SQLite files live in the data directory:

* ``bot_users.db`` — Telegram users and web sessions (:mod:`.users`, :mod:`.users_schema`);
* ``transactions_<telegram_id>.db`` (and the CLI's ``~/.revolut_to_ynab.db``) — the importer
  ledger (:mod:`.ledger`), the YNAB mirror (:mod:`.mirror`) and uploaded statements
  (:mod:`.csv_files`), schema in :mod:`.tx_schema`.

:func:`migrate_data_dir` upgrades every database of a data directory in place (with backups)
and is meant to run once at bot startup, before the web thread starts.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

from ynab_helper.storage.csv_files import CsvFile, CsvFileStore
from ynab_helper.storage.db import (
    Migration,
    MigrationError,
    MigrationResult,
    RecordExists,
    RecordNotFound,
    StorageError,
    backup_db,
    connect,
    default_backup_dir,
    migrate,
    migrate_tx_db_conn,
    migrate_users_db_conn,
    open_tx_db,
    open_users_db,
    transaction,
    tx_db_owner,
    tx_db_path,
    users_db_path,
)
from ynab_helper.storage.ledger import (
    CsvState,
    LedgerEntry,
    LedgerStatus,
    LedgerStore,
    RefreshResult,
)
from ynab_helper.storage.mirror import (
    MirrorAccount,
    MirrorStore,
    MirrorView,
    YnabTx,
    classify_account,
)
from ynab_helper.storage.users import SessionStore, User, UserStore, WebSession

log = logging.getLogger(__name__)

__all__ = [
    "CsvFile",
    "CsvFileStore",
    "CsvState",
    "DataDirMigration",
    "LedgerEntry",
    "LedgerStatus",
    "LedgerStore",
    "Migration",
    "MigrationError",
    "MigrationResult",
    "MirrorAccount",
    "MirrorStore",
    "MirrorView",
    "RecordExists",
    "RecordNotFound",
    "RefreshResult",
    "SessionStore",
    "StorageError",
    "User",
    "UserStore",
    "WebSession",
    "YnabTx",
    "backup_db",
    "classify_account",
    "connect",
    "default_backup_dir",
    "migrate",
    "migrate_data_dir",
    "open_tx_db",
    "open_users_db",
    "transaction",
    "tx_db_owner",
    "tx_db_path",
    "users_db_path",
]


@dataclass
class DataDirMigration:
    """Result of :func:`migrate_data_dir`."""

    users: MigrationResult
    tx: dict[Path, MigrationResult] = field(default_factory=dict)
    failures: dict[Path, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.failures


def migrate_data_dir(
    data_dir: str | os.PathLike[str], *, backup_dir: str | os.PathLike[str] | None = None
) -> DataDirMigration:
    """Migrate ``bot_users.db`` and every ``transactions_<id>.db`` in ``data_dir``.

    Each transactions DB uses its owner's configured account (from the users table) as the
    default account for unassigned legacy rows and registers ``csv_cache/<id>/``'s newest CSV.
    A failure in the users DB raises :class:`MigrationError` (nothing can run without it); a
    failing per-user DB is logged and reported in ``failures`` so the other users still start.
    """
    root = Path(data_dir)
    backups = Path(backup_dir) if backup_dir is not None else default_backup_dir(root)
    conn, users_result = migrate_users_db_conn(users_db_path(root), backup_dir=backups)
    try:
        accounts = {u.telegram_id: u.account_id or "" for u in UserStore(conn).list_all()}
    finally:
        conn.close()
    report = DataDirMigration(users=users_result)
    for path in sorted(root.glob("transactions_*.db")):
        owner = tx_db_owner(path)
        if owner is None:
            continue
        cache_dir = root / "csv_cache" / str(owner)
        try:
            tx_conn, result = migrate_tx_db_conn(
                path,
                default_account_id=accounts.get(owner, ""),
                backup_dir=backups,
                csv_cache_dir=cache_dir if cache_dir.is_dir() else None,
            )
            tx_conn.close()
        except Exception as exc:
            log.exception("migration of %s failed", path.name)
            report.failures[path] = str(exc)
            continue
        report.tx[path] = result
    return report
