"""Fixtures for the login-token / session / CSRF tests (ynab_helper.sessions)."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from ynab_helper.sessions import TokenHasher
from ynab_helper.storage import open_users_db

from .helpers import READY_ID, SECRET, make_ready


@pytest.fixture
def users_db(tmp_path: Path) -> Path:
    """Path of a fresh, migrated ``bot_users.db``."""
    path = tmp_path / "bot_users.db"
    open_users_db(path, backup_dir=tmp_path / "backups").close()
    return path


@pytest.fixture
def conn(users_db: Path, tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """A connection to the fresh users DB (autocommit, as storage.connect makes it)."""
    connection = open_users_db(users_db, backup_dir=tmp_path / "backups")
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def hasher() -> TokenHasher:
    return TokenHasher(SECRET)


@pytest.fixture
def ready_user(conn: sqlite3.Connection) -> int:
    """A fully onboarded user; returns its telegram id."""
    make_ready(conn)
    return READY_ID
