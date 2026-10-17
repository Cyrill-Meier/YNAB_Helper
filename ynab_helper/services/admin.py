"""Read-only helpers for the CLI: budget/account listings and database statistics."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ynab_helper.errors import ConfigError, YnabHelperError
from ynab_helper.services import _common
from ynab_helper.storage.csv_files import CsvFile, CsvFileStore
from ynab_helper.storage.db import connect, table_columns, table_exists, user_version
from ynab_helper.storage.mirror import MirrorStore
from ynab_helper.ynab.client import YnabClient
from ynab_helper.ynab.models import YnabAccount

__all__ = [
    "BudgetSummary",
    "BudgetSyncInfo",
    "DatabaseStats",
    "database_stats",
    "list_accounts",
    "list_budgets",
]


# ── listings (CLI --list-budgets / --list-accounts) ──────────────────────────


@dataclass(frozen=True, slots=True)
class BudgetSummary:
    """One budget of the token's YNAB user."""

    id: str
    name: str
    currency: str | None
    last_modified_on: str | None = None


def _require_token(token: str) -> str:
    if not isinstance(token, str) or not token.strip():
        raise ConfigError("YNAB token missing", user_message="A YNAB token is required.")
    return token.strip()


def list_budgets(token: str, *, client: YnabClient | None = None) -> list[BudgetSummary]:
    """The budgets the token can see (``GET /budgets``); nothing is stored or logged."""
    ynab = client or _common.token_client(_require_token(token))
    budgets: list[BudgetSummary] = []
    for raw in ynab.get_budgets():
        if not isinstance(raw, Mapping) or not raw.get("id"):
            continue
        fmt = raw.get("currency_format")
        iso = fmt.get("iso_code") if isinstance(fmt, Mapping) else None
        modified = raw.get("last_modified_on")
        budgets.append(
            BudgetSummary(
                id=str(raw["id"]),
                name=str(raw.get("name") or ""),
                currency=iso if isinstance(iso, str) and iso else None,
                last_modified_on=modified if isinstance(modified, str) else None,
            )
        )
    return budgets


def list_accounts(
    token: str,
    budget_id: str,
    *,
    include_closed: bool = False,
    client: YnabClient | None = None,
) -> list[YnabAccount]:
    """Open, non-deleted accounts of ``budget_id`` (closed ones too with ``include_closed``),
    straight from YNAB (``GET /budgets/{b}/accounts``); nothing is stored or logged."""
    if not isinstance(budget_id, str) or not budget_id.strip():
        raise ConfigError("YNAB budget id missing", user_message="A YNAB budget id is required.")
    ynab = client or _common.token_client(_require_token(token))
    accounts, _ = ynab.get_accounts(budget_id.strip())
    return [a for a in accounts if not a.deleted and (include_closed or not a.closed)]


# ── database statistics (CLI --db-stats) ─────────────────────────────────────


@dataclass(frozen=True, slots=True)
class BudgetSyncInfo:
    """What the mirror holds for one budget."""

    budget_id: str
    currency: str | None
    last_sync_at: str | None
    server_knowledge: int | None
    accounts: int  # non-deleted accounts
    transactions: int  # live transactions
    deleted_transactions: int


@dataclass(frozen=True, slots=True)
class DatabaseStats:
    """Read-only summary of a transactions DB (:func:`database_stats`).

    ``migrated`` is False for a v1 DB that no v2 operation has opened yet (only
    ``legacy_rows`` is known then). ``ledger`` maps account id (``''`` = legacy rows not yet
    claimed by an import) → ledger status → count.
    """

    path: Path
    exists: bool
    schema_version: int = 0
    migrated: bool = False
    ledger: Mapping[str, Mapping[str, int]] = field(default_factory=dict)
    ledger_dates: tuple[str, str] | None = None
    budgets: tuple[BudgetSyncInfo, ...] = ()
    statements: int = 0
    latest_statement: CsvFile | None = None
    legacy_rows: int | None = None  # rows of v1's `transactions` table (kept as an archive)

    @property
    def ledger_total(self) -> int:
        return sum(sum(counts.values()) for counts in self.ledger.values())

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe representation."""
        latest = self.latest_statement
        return {
            "path": str(self.path),
            "exists": self.exists,
            "schema_version": self.schema_version,
            "migrated": self.migrated,
            "ledger": {account: dict(counts) for account, counts in self.ledger.items()},
            "ledger_dates": list(self.ledger_dates) if self.ledger_dates else None,
            "budgets": [
                {
                    "budget_id": b.budget_id,
                    "currency": b.currency,
                    "last_sync_at": b.last_sync_at,
                    "server_knowledge": b.server_knowledge,
                    "accounts": b.accounts,
                    "transactions": b.transactions,
                    "deleted_transactions": b.deleted_transactions,
                }
                for b in self.budgets
            ],
            "statements": self.statements,
            "latest_statement": None
            if latest is None
            else {
                "id": latest.id,
                "original_name": latest.original_name,
                "status": latest.status,
                "uploaded_at": latest.uploaded_at,
                "window": list(latest.window) if latest.window else None,
            },
            "legacy_rows": self.legacy_rows,
        }


def database_stats(db_path: str | os.PathLike[str]) -> DatabaseStats:
    """Summarise the transactions DB at ``db_path`` without changing it (C07: the stats are
    always those of the given path).

    The DB is opened read-only: no migration, no backup and no change to its content (SQLite
    may add its ``-wal``/``-shm`` companion files); a missing file gives ``exists=False``.
    Raises :class:`YnabHelperError` if the file is not a readable SQLite database.
    """
    path = Path(db_path).expanduser()
    if not path.is_file():
        return DatabaseStats(path=path, exists=False)
    try:
        with closing(connect(path, readonly=True)) as conn:
            return _collect_stats(conn, path)
    except sqlite3.Error as exc:
        raise YnabHelperError(
            f"cannot read database {path}: {exc}",
            user_message=f"{path} is not a readable transactions database ({exc}).",
        ) from exc


def _collect_stats(conn: sqlite3.Connection, path: Path) -> DatabaseStats:
    version = user_version(conn)
    legacy_rows = None
    if table_exists(conn, "transactions"):
        legacy_rows = int(conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0])
    if version < 1 or not table_exists(conn, "import_ledger"):
        return DatabaseStats(
            path=path, exists=True, schema_version=version, legacy_rows=legacy_rows
        )

    ledger: dict[str, dict[str, int]] = {}
    for account_id, status, count in conn.execute(
        "SELECT account_id, status, COUNT(*) FROM import_ledger GROUP BY 1, 2 ORDER BY 1, 2"
    ):
        ledger.setdefault(account_id, {})[status] = int(count)
    first, last = conn.execute("SELECT MIN(csv_date), MAX(csv_date) FROM import_ledger").fetchone()

    tx_counts = {
        row[0]: (int(row[1]), int(row[2] or 0))
        for row in conn.execute(
            "SELECT budget_id, SUM(deleted = 0), SUM(deleted = 1) FROM ynab_transactions "
            "GROUP BY budget_id"
        )
    }
    account_counts: dict[str, int] = {}
    if "budget_id" in table_columns(conn, "accounts"):
        account_counts = {
            row[0]: int(row[1])
            for row in conn.execute(
                "SELECT budget_id, COUNT(*) FROM accounts "
                "WHERE budget_id IS NOT NULL AND deleted = 0 GROUP BY budget_id"
            )
        }
    mirror = MirrorStore(conn)
    budgets = tuple(
        BudgetSyncInfo(
            budget_id=budget_id,
            currency=mirror.get_budget_currency(budget_id),
            last_sync_at=mirror.get_last_sync_at(budget_id),
            server_knowledge=mirror.get_server_knowledge(budget_id),
            accounts=account_counts.get(budget_id, 0),
            transactions=tx_counts.get(budget_id, (0, 0))[0],
            deleted_transactions=tx_counts.get(budget_id, (0, 0))[1],
        )
        for budget_id in sorted(set(tx_counts) | set(account_counts))
    )
    files = CsvFileStore(conn)
    return DatabaseStats(
        path=path,
        exists=True,
        schema_version=version,
        migrated=True,
        ledger=ledger,
        ledger_dates=(first, last) if first and last else None,
        budgets=budgets,
        statements=int(conn.execute("SELECT COUNT(*) FROM csv_files").fetchone()[0]),
        latest_statement=files.latest(),
        legacy_rows=legacy_rows,
    )
