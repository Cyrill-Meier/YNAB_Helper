"""``bot_users.db``: schema and migrations (DESIGN §5.1, §5.3 last paragraph).

Version 1 creates any missing table/index/column of the v1 bot's schema (older DBs lack
``auto_approve``, the crypto columns and ``tg_chat_id``/``tg_message_id``), maps the legacy
``approved`` state to ``awaiting_token`` and deletes expired web sessions. Nothing is dropped,
so the v1 image still starts on a migrated file.
"""

from __future__ import annotations

import logging
import sqlite3
import time

from ynab_helper.storage.db import Migration, ensure_columns
from ynab_helper.timeutil import utc_now_iso

log = logging.getLogger(__name__)

USERS_SCHEMA_VERSION = 1

_USERS_DDL = """
CREATE TABLE IF NOT EXISTS users (
  telegram_id         INTEGER PRIMARY KEY,
  chat_id             INTEGER,
  username            TEXT,
  first_name          TEXT,
  ynab_token          TEXT,
  budget_id           TEXT,
  budget_name         TEXT,
  account_id          TEXT,
  account_name        TEXT,
  crypto_account_id   TEXT,
  crypto_account_name TEXT,
  btc_xpub            TEXT,
  eth_address         TEXT,
  state               TEXT NOT NULL DEFAULT 'pending',
  temp_data           TEXT,
  created_at          TEXT NOT NULL,
  updated_at          TEXT NOT NULL,
  auto_approve        INTEGER NOT NULL DEFAULT 1
)"""

_WEB_SESSIONS_DDL = """
CREATE TABLE IF NOT EXISTS web_sessions (
  token_hash    TEXT PRIMARY KEY,
  telegram_id   INTEGER NOT NULL,
  kind          TEXT NOT NULL CHECK (kind IN ('login','session')),
  created_at    REAL NOT NULL,
  expires_at    REAL NOT NULL,
  user_agent    TEXT,
  ip            TEXT,
  tg_chat_id    INTEGER,
  tg_message_id INTEGER
)"""

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_web_sessions_expires ON web_sessions(expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_web_sessions_telegram ON web_sessions(telegram_id)",
)

# ADD COLUMN cannot add NOT NULL without a default, hence the defaults below.
_USERS_COLUMNS = {
    "chat_id": "INTEGER",
    "username": "TEXT",
    "first_name": "TEXT",
    "ynab_token": "TEXT",
    "budget_id": "TEXT",
    "budget_name": "TEXT",
    "account_id": "TEXT",
    "account_name": "TEXT",
    "crypto_account_id": "TEXT",
    "crypto_account_name": "TEXT",
    "btc_xpub": "TEXT",
    "eth_address": "TEXT",
    "state": "TEXT NOT NULL DEFAULT 'pending'",
    "temp_data": "TEXT",
    "created_at": "TEXT NOT NULL DEFAULT ''",
    "updated_at": "TEXT NOT NULL DEFAULT ''",
    "auto_approve": "INTEGER NOT NULL DEFAULT 1",
}
_WEB_SESSIONS_COLUMNS = {
    "user_agent": "TEXT",
    "ip": "TEXT",
    "tg_chat_id": "INTEGER",
    "tg_message_id": "INTEGER",
}


def migrations() -> list[Migration]:
    """The users-DB migration list for :func:`ynab_helper.storage.db.migrate`."""
    return [Migration(1, "complete v1 columns, map 'approved', purge expired sessions", _apply_v1)]


def _apply_v1(conn: sqlite3.Connection) -> None:
    conn.execute(_USERS_DDL)
    conn.execute(_WEB_SESSIONS_DDL)
    added = ensure_columns(conn, "users", _USERS_COLUMNS)
    added += ensure_columns(conn, "web_sessions", _WEB_SESSIONS_COLUMNS)
    for statement in _INDEXES:
        conn.execute(statement)
    remapped = conn.execute(
        "UPDATE users SET state = 'awaiting_token', updated_at = ? WHERE state = 'approved'",
        (utc_now_iso(),),
    ).rowcount
    purged = conn.execute("DELETE FROM web_sessions WHERE expires_at < ?", (time.time(),)).rowcount
    log.info(
        "users DB v1: added columns %s, %d 'approved' users → awaiting_token, %d expired "
        "sessions purged",
        added or "none",
        remapped,
        purged,
    )
