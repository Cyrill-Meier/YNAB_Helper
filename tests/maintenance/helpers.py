"""Shared helpers for the balance (reconcile) and maintenance (cleanup, dedupe) tests.

They build a real, migrated transactions DB, a :class:`FakeYnab` budget, copy the fake's
state into the mirror the way the sync does (API-shaped dicts through ``MirrorStore``) and
record statements in ``csv_files``.
"""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from typing import Any

from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.revolut.parser import parse_export
from ynab_helper.storage import LedgerEntry, LedgerStore, MirrorStore, open_tx_db
from ynab_helper.storage.csv_files import CsvFileStore

BUDGET = "budget-1"
ACCOUNT = "acct-revolut"
OTHER = "acct-other"
ACCOUNTS = (ACCOUNT, OTHER)


def make_fake(currency: str = "CHF", **kwargs: Any) -> FakeYnab:
    """A fake budget with the Revolut checking account and a second account."""
    fake = FakeYnab(**kwargs)
    fake.add_budget(BUDGET, currency=currency)
    fake.add_account(BUDGET, ACCOUNT, "Revolut")
    fake.add_account(BUDGET, OTHER, "Savings", type="savings")
    return fake


def open_db(tmp_path: Path) -> sqlite3.Connection:
    """A fresh, migrated per-user transactions DB."""
    return open_tx_db(
        tmp_path / "transactions_1.db", default_account_id=ACCOUNT, backup_dir=tmp_path / "bk"
    )


def sync_mirror(
    conn: sqlite3.Connection, fake: FakeYnab, accounts: tuple[str, ...] = ACCOUNTS
) -> None:
    """Copy the fake's accounts and transactions (deleted included) into the mirror."""
    mirror = MirrorStore(conn)
    mirror.upsert_accounts(BUDGET, [fake.account(BUDGET, a) for a in accounts])
    mirror.upsert_transactions(BUDGET, fake.transactions(BUDGET, include_deleted=True))


def record_statement(
    conn: sqlite3.Connection,
    directory: Path,
    content: bytes,
    *,
    name: str = "account-statement_2026-10-01_2026-10-31_en_abc123.csv",
    status: str = "imported",
    account_id: str = ACCOUNT,
    uploaded_at: str = "2026-10-16T10:00:00Z",
    source: str = "web",
) -> tuple[int, Path]:
    """Store ``content`` under ``directory`` and record it in ``csv_files``."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{uploaded_at.replace(':', '')}_{len(list(directory.iterdir()))}.csv"
    path.write_bytes(content)
    export = parse_export(content, filename=name)
    file_id = CsvFileStore(conn).add(
        stored_path=str(path),
        sha256=hashlib.sha256(content).hexdigest(),
        source=source,
        account_id=account_id,
        original_name=name,
        uploaded_at=uploaded_at,
        currency=export.currency,
        row_count=len(export.rows),
        window=export.window,
        status=status,
    )
    return file_id, path


def add_entry(conn: sqlite3.Connection, key: str, **fields: Any) -> LedgerEntry:
    """Insert a ledger entry (defaults: ACTIVE COMPLETED row of the Revolut account)."""
    values: dict[str, Any] = {
        "account_id": ACCOUNT,
        "key": key,
        "status": "ACTIVE",
        "csv_date": "2026-10-01",
        "csv_amount": -1000,
        "csv_state": "COMPLETED",
        "ynab_import_id": key,
        "pushed_cleared": "cleared",
    }
    values.update(fields)
    return LedgerStore(conn).insert(LedgerEntry(**values))


def mutating_calls(fake: FakeYnab) -> list[Any]:
    """Every POST/PUT/PATCH/DELETE the fake received."""
    return [c for c in fake.calls if c.method in ("POST", "PUT", "PATCH", "DELETE")]
