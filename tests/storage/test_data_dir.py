"""storage.migrate_data_dir: startup migration of a whole bot_data directory."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from ynab_helper.storage import CsvFileStore, LedgerStore, connect, migrate_data_dir
from ynab_helper.storage.db import list_backups, user_version

from .helpers import PROD_TX_DDL, PROD_USERS_DDL, legacy_row, make_legacy_db


def _users_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(PROD_USERS_DDL)
    conn.execute(
        "INSERT INTO users (telegram_id, state, account_id, created_at, updated_at) "
        "VALUES (59213669, 'ready', 'acct-main', 't', 't')"
    )
    conn.commit()
    conn.close()


def test_migrate_data_dir_like_production(tmp_path: Path) -> None:
    data = tmp_path / "bot_data"
    data.mkdir()
    _users_db(data / "bot_users.db")
    make_legacy_db(
        data / "transactions_59213669.db",
        PROD_TX_DDL,
        [legacy_row("YNAB:-1:2026-08-01:1", ynab_tx_id="y1", account_id=None)],
    )
    make_legacy_db(
        data / "transactions_77.db",
        PROD_TX_DDL,
        [legacy_row("YNAB:-2:2026-08-01:1", account_id=None)],
    )
    (data / "transactions_88.db").write_bytes(b"this is not a sqlite database" * 100)
    manual_copy = data / "transactions_59213669.db.bak"
    manual_copy.write_bytes((data / "transactions_59213669.db").read_bytes())
    cache = data / "csv_cache" / "59213669"
    cache.mkdir(parents=True)
    (cache / "statement.csv").write_text("Type,Product\n")

    report = migrate_data_dir(data)

    assert report.users.applied == (1,)
    assert set(report.tx) == {data / "transactions_59213669.db", data / "transactions_77.db"}
    assert set(report.failures) == {data / "transactions_88.db"} and not report.ok

    conn = connect(data / "transactions_59213669.db")
    assert user_version(conn) == 1
    entry = LedgerStore(conn).get("acct-main", "YNAB:-1:2026-08-01:1")
    assert entry is not None and entry.status == "ACTIVE"  # owner's account as default
    latest = CsvFileStore(conn).latest("acct-main")
    assert latest is not None and latest.status == "legacy"
    conn.close()

    conn = connect(data / "transactions_77.db")  # unknown owner → rows stay unclaimed
    assert [e.key for e in LedgerStore(conn).list_unassigned()] == ["YNAB:-2:2026-08-01:1"]
    conn.close()

    backups = data / "backups"
    assert list_backups(backups, "bot_users.db")
    assert list_backups(backups, "transactions_59213669.db")
    assert manual_copy.exists()

    # A second start is a no-op.
    again = migrate_data_dir(data)
    assert again.users.applied == ()
    assert all(r.applied == () for r in again.tx.values())


def test_migrate_data_dir_on_empty_directory(tmp_path: Path) -> None:
    report = migrate_data_dir(tmp_path)
    assert report.ok and report.tx == {}
    assert (tmp_path / "bot_users.db").exists()
    assert not (tmp_path / "backups").exists()  # nothing existed, nothing to back up
