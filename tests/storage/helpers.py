"""Shared helpers for the storage tests: legacy DDL, YNAB payload builders, a v1 fake."""

from __future__ import annotations

import csv
import sqlite3
import urllib.parse
from pathlib import Path
from typing import Any

# Exact DDL of production's transactions_<id>.db (docs/DESIGN.md §5.1, production_facts.md).
PROD_TX_DDL = """
CREATE TABLE accounts (account_id TEXT PRIMARY KEY, name TEXT, type TEXT, classification TEXT,
  on_budget INTEGER NOT NULL DEFAULT 1, closed INTEGER NOT NULL DEFAULT 0, deleted INTEGER NOT NULL DEFAULT 0, note TEXT,
  balance INTEGER, cleared_balance INTEGER, uncleared_balance INTEGER, transfer_payee_id TEXT, last_seen_at TEXT,
  first_seen_at TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE sync_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE transactions (import_id TEXT PRIMARY KEY, date TEXT NOT NULL, amount INTEGER NOT NULL, payee_name TEXT,
  memo TEXT, cleared TEXT, state TEXT, ynab_tx_id TEXT, account_id TEXT, category_name TEXT, approved INTEGER,
  deleted INTEGER DEFAULT 0, source TEXT DEFAULT 'revolut', imported_at TEXT NOT NULL, updated_at TEXT, started_at TEXT);
CREATE INDEX idx_accounts_active ON accounts(closed, deleted);
CREATE INDEX idx_accounts_classification ON accounts(classification);
CREATE INDEX idx_transactions_account ON transactions(account_id);
CREATE INDEX idx_transactions_date ON transactions(date);
"""

# The very first v1 schema: no account/category/approved/deleted/source/updated_at/started_at
# columns, no accounts table and no sync_meta.
OLDEST_TX_DDL = """
CREATE TABLE transactions (import_id TEXT PRIMARY KEY, date TEXT NOT NULL, amount INTEGER NOT NULL,
  payee_name TEXT, memo TEXT, cleared TEXT, state TEXT, ynab_tx_id TEXT, imported_at TEXT NOT NULL);
"""

# Exact DDL of production's bot_users.db.
PROD_USERS_DDL = """
CREATE TABLE users (telegram_id INTEGER PRIMARY KEY, chat_id INTEGER, username TEXT, first_name TEXT, ynab_token TEXT,
  budget_id TEXT, budget_name TEXT, account_id TEXT, account_name TEXT, crypto_account_id TEXT, crypto_account_name TEXT,
  btc_xpub TEXT, eth_address TEXT, state TEXT NOT NULL DEFAULT 'pending', temp_data TEXT, created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL, auto_approve INTEGER NOT NULL DEFAULT 1);
CREATE TABLE web_sessions (token_hash TEXT PRIMARY KEY, telegram_id INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('login','session')), created_at REAL NOT NULL, expires_at REAL NOT NULL,
  user_agent TEXT, ip TEXT, tg_chat_id INTEGER, tg_message_id INTEGER);
CREATE INDEX idx_web_sessions_expires ON web_sessions(expires_at);
CREATE INDEX idx_web_sessions_telegram ON web_sessions(telegram_id);
"""

# An older bot_users.db: no crypto columns, no auto_approve, no tg_* columns.
OLD_USERS_DDL = """
CREATE TABLE users (telegram_id INTEGER PRIMARY KEY, chat_id INTEGER, username TEXT, first_name TEXT,
  ynab_token TEXT, budget_id TEXT, budget_name TEXT, account_id TEXT, account_name TEXT,
  state TEXT NOT NULL DEFAULT 'pending', temp_data TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE web_sessions (token_hash TEXT PRIMARY KEY, telegram_id INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('login','session')), created_at REAL NOT NULL, expires_at REAL NOT NULL,
  user_agent TEXT, ip TEXT);
"""

LEGACY_TX_COLUMNS = (
    "import_id",
    "date",
    "amount",
    "payee_name",
    "memo",
    "cleared",
    "state",
    "ynab_tx_id",
    "account_id",
    "category_name",
    "approved",
    "deleted",
    "source",
    "imported_at",
    "updated_at",
    "started_at",
)


def make_legacy_db(path: Path, ddl: str, rows: list[dict[str, Any]] | None = None) -> None:
    """Create a legacy DB with ``ddl`` (journal WAL like production) and insert ``rows``."""
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(ddl)
        for row in rows or []:
            cols = list(row)
            conn.execute(
                f"INSERT INTO transactions ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                [row[c] for c in cols],
            )
        conn.commit()
    finally:
        conn.close()


def legacy_row(import_id: str, **overrides: Any) -> dict[str, Any]:
    """A production-shaped legacy ``transactions`` row (source revolut, COMPLETED/cleared)."""
    row: dict[str, Any] = {
        "import_id": import_id,
        "date": "2026-08-01",
        "amount": -10000,
        "payee_name": "Shop",
        "memo": None,
        "cleared": "cleared",
        "state": "COMPLETED",
        "ynab_tx_id": None,
        "account_id": "acct-main",
        "category_name": None,
        "approved": 1,
        "deleted": 0,
        "source": "revolut",
        "imported_at": "2026-08-01T10:00:00.000001",
        "updated_at": "2026-08-02T10:00:00.000001",
        "started_at": None,
    }
    row.update(overrides)
    return row


def dump_table(path: Path, table: str) -> list[tuple[Any, ...]]:
    conn = sqlite3.connect(path)
    try:
        return conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
    finally:
        conn.close()


def schema_sql(path: Path, name_like: str) -> list[tuple[str, str]]:
    conn = sqlite3.connect(path)
    try:
        return conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE tbl_name = ? ORDER BY name", (name_like,)
        ).fetchall()
    finally:
        conn.close()


def ynab_tx(tx_id: str, **overrides: Any) -> dict[str, Any]:
    """A YNAB ``TransactionDetail`` JSON object as the API returns it."""
    tx: dict[str, Any] = {
        "id": tx_id,
        "date": "2026-08-01",
        "amount": -10000,
        "memo": None,
        "cleared": "cleared",
        "approved": True,
        "flag_color": None,
        "flag_name": None,
        "account_id": "acct-main",
        "payee_id": "payee-1",
        "category_id": "cat-1",
        "transfer_account_id": None,
        "transfer_transaction_id": None,
        "matched_transaction_id": None,
        "import_id": None,
        "import_payee_name": None,
        "import_payee_name_original": None,
        "debt_transaction_type": None,
        "deleted": False,
        "account_name": "Revolut",
        "payee_name": "Shop",
        "category_name": "Groceries",
        "subtransactions": [],
    }
    tx.update(overrides)
    return tx


def ynab_sub(sub_id: str, parent_id: str, **overrides: Any) -> dict[str, Any]:
    sub: dict[str, Any] = {
        "id": sub_id,
        "transaction_id": parent_id,
        "amount": -5000,
        "memo": None,
        "payee_id": None,
        "payee_name": None,
        "category_id": "cat-2",
        "category_name": "Dining",
        "transfer_account_id": None,
        "transfer_transaction_id": None,
        "deleted": False,
    }
    sub.update(overrides)
    return sub


def ynab_account(account_id: str, **overrides: Any) -> dict[str, Any]:
    """A YNAB ``Account`` JSON object as the API returns it."""
    acct: dict[str, Any] = {
        "id": account_id,
        "name": f"Account {account_id}",
        "type": "checking",
        "on_budget": True,
        "closed": False,
        "note": None,
        "balance": 100000,
        "cleared_balance": 90000,
        "uncleared_balance": 10000,
        "transfer_payee_id": f"tp-{account_id}",
        "direct_import_linked": False,
        "direct_import_in_error": False,
        "last_reconciled_at": None,
        "debt_original_balance": None,
        "debt_interest_rates": {},
        "debt_minimum_payments": {},
        "debt_escrow_amounts": {},
        "deleted": False,
    }
    acct.update(overrides)
    return acct


REVOLUT_HEADER = [
    "Type",
    "Product",
    "Started Date",
    "Completed Date",
    "Description",
    "Amount",
    "Fee",
    "Currency",
    "State",
    "Balance",
]


def write_revolut_csv(path: Path, rows: list[dict[str, str]]) -> Path:
    """Write a Revolut-shaped export (rows given as dicts keyed by REVOLUT_HEADER)."""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REVOLUT_HEADER)
        writer.writeheader()
        for row in rows:
            full = dict.fromkeys(REVOLUT_HEADER, "")
            full.update(
                {"Product": "Current", "Currency": "CHF", "Fee": "0.00", "Type": "Card Payment"}
            )
            full.update(row)
            writer.writerow(full)
    return path


class FakeV1Ynab:
    """Stateful stand-in for v1's ``ynab_request(method, path, token, body)``.

    Implements what v1's import and sync flows call: bulk POST with per-account import_id
    dedupe (deleted transactions included, like YNAB), PATCH/DELETE by id (DELETE is a soft
    delete), account and budget transaction lists with ``last_knowledge_of_server`` deltas
    (deleted rows only appear in deltas, as in YNAB). ``fail_account_get`` makes the
    per-account list raise, which v1 handles by recording duplicates without an id.
    """

    def __init__(self, budget_id: str = "b") -> None:
        self.budget_id = budget_id
        self.txs: dict[str, dict[str, Any]] = {}
        self.changed: dict[str, int] = {}
        self.knowledge = 100
        self._seq = 0
        self.fail_account_get = False
        self.calls: list[tuple[str, str]] = []

    # ── state helpers ──

    def _touch(self, tx_id: str) -> None:
        self.knowledge += 1
        self.changed[tx_id] = self.knowledge

    def add(self, **fields: Any) -> dict[str, Any]:
        self._seq += 1
        tx = ynab_tx(f"y{self._seq:03d}", **fields)
        self.txs[tx["id"]] = tx
        self._touch(tx["id"])
        return tx

    def edit(self, tx_id: str, **fields: Any) -> None:
        self.txs[tx_id].update(fields)
        self._touch(tx_id)

    def by_import_id(self, import_id: str) -> dict[str, Any] | None:
        for tx in self.txs.values():
            if tx["import_id"] == import_id:
                return tx
        return None

    def all_transactions(self) -> list[dict[str, Any]]:
        """Every transaction incl. deleted ones (what a delta from knowledge 0 returns)."""
        return [dict(tx) for tx in self.txs.values()]

    def full_listing(self) -> list[dict[str, Any]]:
        """A non-delta listing (no ``last_knowledge_of_server``): deleted ones are omitted."""
        return [dict(tx) for tx in self.txs.values() if not tx["deleted"]]

    # ── v1 transport ──

    def request(self, method: str, path: str, token: str, body: Any = None) -> dict[str, Any]:
        self.calls.append((method, path))
        parsed = urllib.parse.urlsplit(path)
        parts = parsed.path.strip("/").split("/")
        query = urllib.parse.parse_qs(parsed.query)
        assert parts[:2] == ["budgets", self.budget_id], path
        rest = parts[2:]
        if rest == ["transactions"] and method == "POST":
            return self._create(body["transactions"])
        if rest == ["transactions"] and method == "GET":
            return self._list(None, query)
        if len(rest) == 2 and rest[0] == "transactions" and method == "PATCH":
            return self._patch(rest[1], body["transaction"])
        if len(rest) == 2 and rest[0] == "transactions" and method == "DELETE":
            return self._delete(rest[1])
        if len(rest) == 3 and rest[0] == "accounts" and rest[2] == "transactions":
            if self.fail_account_get:
                raise RuntimeError("YNAB API 500: simulated outage")
            return self._list(rest[1], query)
        raise AssertionError(f"unexpected v1 call {method} {path}")

    def _create(self, payload: list[dict[str, Any]]) -> dict[str, Any]:
        created, duplicates = [], []
        for item in payload:
            clash = any(
                tx["account_id"] == item["account_id"] and tx["import_id"] == item["import_id"]
                for tx in self.txs.values()
            )
            if clash:
                duplicates.append(item["import_id"])
                continue
            tx = self.add(
                account_id=item["account_id"],
                date=item["date"],
                amount=item["amount"],
                payee_name=item.get("payee_name"),
                memo=item.get("memo"),
                cleared=item.get("cleared", "uncleared"),
                approved=bool(item.get("approved")),
                import_id=item.get("import_id"),
                category_name=None,
            )
            created.append(dict(tx))
        return {
            "data": {
                "transaction_ids": [t["id"] for t in created],
                "transactions": created,
                "duplicate_import_ids": duplicates,
                "server_knowledge": self.knowledge,
            }
        }

    def _patch(self, tx_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        if tx_id not in self.txs or self.txs[tx_id]["deleted"]:
            raise RuntimeError("YNAB API 404: resource not found")
        allowed = {k: v for k, v in fields.items() if k != "import_id"}
        self.edit(tx_id, **allowed)
        return {"data": {"transaction": dict(self.txs[tx_id])}}

    def _delete(self, tx_id: str) -> dict[str, Any]:
        if tx_id not in self.txs or self.txs[tx_id]["deleted"]:
            raise RuntimeError("YNAB API 404: resource not found")
        self.edit(tx_id, deleted=True)
        return {"data": {"transaction": dict(self.txs[tx_id])}}

    def _list(self, account_id: str | None, query: dict[str, list[str]]) -> dict[str, Any]:
        since = query.get("since_date", [None])[0]
        knowledge = query.get("last_knowledge_of_server", [None])[0]
        result = []
        for tx in self.txs.values():
            if account_id is not None and tx["account_id"] != account_id:
                continue
            if since and tx["date"] < since:
                continue
            if knowledge is not None:
                if self.changed[tx["id"]] <= int(knowledge):
                    continue
            elif tx["deleted"]:
                continue
            result.append(dict(tx))
        return {"data": {"transactions": result, "server_knowledge": self.knowledge}}
