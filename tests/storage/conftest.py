"""Fixtures for the storage tests."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from ynab_helper.storage import open_tx_db, open_users_db


@pytest.fixture
def backup_dir(tmp_path: Path) -> Path:
    return tmp_path / "backups"


@pytest.fixture
def tx_conn(tmp_path: Path, backup_dir: Path) -> Iterator[sqlite3.Connection]:
    """A fresh, migrated transactions DB (default account ``acct-main``)."""
    conn = open_tx_db(
        tmp_path / "transactions_1.db", default_account_id="acct-main", backup_dir=backup_dir
    )
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def users_conn(tmp_path: Path, backup_dir: Path) -> Iterator[sqlite3.Connection]:
    """A fresh, migrated users DB."""
    conn = open_users_db(tmp_path / "bot_users.db", backup_dir=backup_dir)
    try:
        yield conn
    finally:
        conn.close()
