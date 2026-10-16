"""tx_schema.py: v2 DDL and the legacy → v1 migration of the transactions DB."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from pathlib import Path

import pytest

from ynab_helper.storage import (
    CsvFileStore,
    LedgerStore,
    MigrationError,
    MirrorStore,
    connect,
    open_tx_db,
    transaction,
)
from ynab_helper.storage.db import list_backups, migrate_tx_db_conn, table_columns, user_version
from ynab_helper.storage.tx_schema import (
    TX_SCHEMA_VERSION,
    LegacyMigrationStats,
    create_v1_schema,
    migrate_legacy_rows,
)

from .helpers import (
    OLDEST_TX_DDL,
    PROD_TX_DDL,
    dump_table,
    legacy_row,
    make_legacy_db,
    schema_sql,
    ynab_account,
    ynab_tx,
)

V2_TABLES = {"import_ledger", "ynab_transactions", "ynab_subtransactions", "csv_files"}
U = "11111111-1111-1111-1111-111111111111"


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _ledger(conn: sqlite3.Connection) -> dict[str, dict]:
    return {
        r["key"]: dict(r)
        for r in conn.execute("SELECT * FROM import_ledger ORDER BY account_id, key")
    }


# ── fresh database ────────────────────────────────────────────────────────────


def test_fresh_db_gets_full_v2_schema(tx_conn: sqlite3.Connection, backup_dir: Path) -> None:
    assert user_version(tx_conn) == TX_SCHEMA_VERSION == 1
    tables = _tables(tx_conn)
    assert V2_TABLES | {"accounts", "sync_meta"} <= tables
    assert "transactions" not in tables  # the legacy table is never created by v2
    assert table_columns(tx_conn, "import_ledger") == {
        "account_id",
        "key",
        "ynab_import_id",
        "ynab_tx_id",
        "status",
        "csv_date",
        "csv_amount",
        "csv_payee",
        "csv_type",
        "csv_state",
        "started_at",
        "pushed_cleared",
        "pushed_amount",
        "pushed_date",
        "identity",
        "rekeyed_to",
        "last_error",
        "first_seen_at",
        "updated_at",
    }
    assert "budget_id" in table_columns(tx_conn, "accounts")
    indexes = {r[0] for r in tx_conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert {"idx_ledger_tx", "idx_ledger_date", "idx_ytx_acct_date", "idx_ytx_import"} <= indexes
    assert "idx_ysub_parent" in indexes
    pk = [r[1] for r in tx_conn.execute("PRAGMA table_info(import_ledger)") if r[5]]
    assert pk == ["account_id", "key"]
    assert not backup_dir.exists()  # an empty new DB needs no backup


def test_v1_image_can_still_open_a_migrated_fresh_db(tmp_path: Path, backup_dir: Path) -> None:
    """Rollback safety: v1's init_db must start on a DB created by v2."""
    from tests.legacy import v1_revolut_to_ynab as v1

    path = tmp_path / "transactions_7.db"
    open_tx_db(path, backup_dir=backup_dir).close()
    conn = v1.init_db(str(path))
    conn.execute(
        "INSERT INTO accounts (account_id, first_seen_at, created_at, updated_at) "
        "VALUES ('a', 'x', 'x', 'x')"
    )
    conn.commit()
    conn.close()


# ── production DDL ────────────────────────────────────────────────────────────


def _production_rows() -> list[dict]:
    """One legacy row per production shape (production_facts.md)."""
    return [
        legacy_row(
            "YNAB:-12500:2026-08-01:1",
            ynab_tx_id="y-completed",
            started_at="2026-08-01 12:00:00",
            payee_name="Coop",
            amount=-12500,
        ),
        legacy_row("YNAB:-2000:2026-08-02:1", ynab_tx_id="y-reconciled", cleared="reconciled"),
        # 174 production rows: PENDING in state, reconciled in YNAB.
        legacy_row(
            "YNAB:-3000:2026-08-02:1",
            ynab_tx_id="y-pend-recon",
            state="PENDING",
            cleared="reconciled",
        ),
        legacy_row(
            "YNAB:-4000:2026-08-03:1",
            ynab_tx_id="y-pending",
            state="PENDING",
            cleared="uncleared",
            memo="(pending)",
        ),
        legacy_row(
            "YNAB:-5000:2026-08-03:1", ynab_tx_id="y-del", state="DELETED_IN_YNAB", deleted=1
        ),
        legacy_row("YNAB:-6000:2026-08-04:1", state="DUPLICATE"),  # no ynab_tx_id
        legacy_row("YNAB:-6000:2026-08-04:2", state="DUPLICATE", ynab_tx_id="y-dup"),
        # REKEYED pair sharing a ynab_tx_id: old key tombstone, new key live.
        legacy_row("YNAB:-7000:2026-08-05:1", state="REKEYED", ynab_tx_id="y-rekey"),
        legacy_row(
            "YNAB:-7000:2026-08-04:1",
            ynab_tx_id="y-rekey",
            date="2026-08-04",
            started_at="2026-08-04 23:30:00",
        ),
        legacy_row(
            "YNAB:-8000:2026-08-06:1",
            ynab_tx_id="y-rev",
            state="REVERTED",
            cleared="uncleared",
            deleted=1,
        ),
        legacy_row(
            "YNAB:-9000:2026-08-06:1",
            ynab_tx_id="y-sup",
            state="SUPERSEDED",
            cleared="uncleared",
            deleted=1,
        ),
        # deleted in YNAB, seen by v1's sync (state untouched, deleted=1)
        legacy_row("YNAB:-9100:2026-08-07:1", ynab_tx_id="y-gone", deleted=1),
        legacy_row("YNAB:-9200:2026-08-07:1", ynab_tx_id="y-null-acct", account_id=None),
        legacy_row("YNAB:-9300:2026-08-07:1", ynab_tx_id="y-empty-acct", account_id=""),
        legacy_row("YNAB:-9400:2026-08-07:1", ynab_tx_id="y-other", account_id="acct-old"),
        legacy_row("YNAB:P:-9500:2026-08-08:1", ynab_tx_id="y-p-key", amount=-9500),
        legacy_row("YNAB:9600:2026-08-08:1", ynab_tx_id="y-inflow", amount=9600),
        # NULL source: YNAB: key → importer row; anything else → not.
        legacy_row("YNAB:-9700:2026-08-08:1", ynab_tx_id="y-nullsrc", source=None),
        legacy_row(f"acct:{U}:{U}", source=None, ynab_tx_id="y-nullsrc-acct"),
        # source='ynab' rows (v1 web sync) are never migrated.
        legacy_row(f"acct:{U}:{U}x", source="ynab", ynab_tx_id="y-a1", account_id="acct-other"),
        legacy_row(f"acct:{U}:{U}_2026-08-01", source="ynab", ynab_tx_id="y-a2"),
        legacy_row(f"acct:{U}:{U}_t_2026-08-01", source="ynab", ynab_tx_id="y-a3"),
        legacy_row(f"ynab_manual:{U}", source="ynab", ynab_tx_id="y-m1"),
        legacy_row(f"ynab_manual:{U}_2026-08-01", source="ynab", ynab_tx_id="y-m2"),
        legacy_row(
            "RECON:-250:2026-08-01:1", source="ynab", ynab_tx_id="y-r1", cleared="reconciled"
        ),
        legacy_row("YNAB:-1:2026-01-01:1", source="ynab", ynab_tx_id="y-ynab-src", deleted=1),
    ]


@pytest.fixture
def prod_db(tmp_path: Path) -> Path:
    path = tmp_path / "transactions_59213669.db"
    make_legacy_db(path, PROD_TX_DDL, _production_rows())
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO accounts (account_id, name, type, classification, first_seen_at, "
        "created_at, updated_at, balance) VALUES ('acct-main', 'Revolut', 'checking', 'cash', "
        "'t', 't', 't', 123)"
    )
    conn.execute("INSERT INTO sync_meta VALUES ('server_knowledge:budget:b', '42')")
    conn.commit()
    conn.close()
    return path


def test_production_ddl_migration_maps_every_legacy_shape(prod_db: Path, backup_dir: Path) -> None:
    legacy_before = dump_table(prod_db, "transactions")
    legacy_schema_before = schema_sql(prod_db, "transactions")
    conn = open_tx_db(prod_db, default_account_id="acct-main", backup_dir=backup_dir)
    try:
        ledger = _ledger(conn)
        assert user_version(conn) == 1

        def check(key: str, **expected: object) -> None:
            row = ledger[key]
            for name, value in expected.items():
                assert row[name] == value, (key, name, row[name], value)

        check(
            "YNAB:-12500:2026-08-01:1",
            account_id="acct-main",
            status="ACTIVE",
            csv_state="COMPLETED",
            ynab_tx_id="y-completed",
            ynab_import_id="YNAB:-12500:2026-08-01:1",
            csv_date="2026-08-01",
            csv_amount=-12500,
            csv_payee="Coop",
            started_at="2026-08-01 12:00:00",
            pushed_cleared="cleared",
            pushed_amount=-12500,
            pushed_date="2026-08-01",
            identity="legacy",
            first_seen_at="2026-08-01T10:00:00.000001",
            updated_at="2026-08-02T10:00:00.000001",
        )
        check(
            "YNAB:-2000:2026-08-02:1",
            status="ACTIVE",
            csv_state="COMPLETED",
            pushed_cleared="cleared",
        )
        check(
            "YNAB:-3000:2026-08-02:1",
            status="ACTIVE",
            csv_state="COMPLETED",
            pushed_cleared="cleared",
        )
        check(
            "YNAB:-4000:2026-08-03:1",
            status="ACTIVE",
            csv_state="PENDING",
            pushed_cleared="uncleared",
        )
        check("YNAB:-5000:2026-08-03:1", status="DELETED_IN_YNAB", ynab_tx_id="y-del")
        check("YNAB:-6000:2026-08-04:1", status="UNLINKED", ynab_tx_id=None, csv_state="COMPLETED")
        check("YNAB:-6000:2026-08-04:2", status="ACTIVE", ynab_tx_id="y-dup")
        check("YNAB:-7000:2026-08-05:1", status="REKEYED", rekeyed_to="YNAB:-7000:2026-08-04:1")
        check("YNAB:-7000:2026-08-04:1", status="ACTIVE", rekeyed_to=None)
        check("YNAB:-8000:2026-08-06:1", status="REVERTED", csv_state="REVERTED")
        check(
            "YNAB:-9000:2026-08-06:1",
            status="SUPERSEDED",
            csv_state="PENDING",
            pushed_cleared="uncleared",
        )
        check("YNAB:-9100:2026-08-07:1", status="DELETED_IN_YNAB", csv_state="COMPLETED")
        check("YNAB:-9200:2026-08-07:1", account_id="acct-main")
        check("YNAB:-9300:2026-08-07:1", account_id="acct-main")
        check("YNAB:-9400:2026-08-07:1", account_id="acct-old", status="ACTIVE")
        check(
            "YNAB:P:-9500:2026-08-08:1", status="ACTIVE", ynab_import_id="YNAB:P:-9500:2026-08-08:1"
        )
        check("YNAB:9600:2026-08-08:1", csv_amount=9600)
        check("YNAB:-9700:2026-08-08:1", status="ACTIVE")

        importer_keys = {
            r["import_id"]
            for r in _production_rows()
            if r["source"] == "revolut"
            or (r["source"] is None and r["import_id"].startswith("YNAB:"))
        }
        assert set(ledger) == importer_keys
        assert not any(k.startswith(("acct:", "ynab_manual:", "RECON:")) for k in ledger)
        assert "YNAB:-1:2026-01-01:1" not in ledger  # source='ynab' even with a YNAB: key

        # Statuses and csv_states are always valid values the stores accept.
        entries = LedgerStore(conn).list_for_account("acct-main")
        assert {e.status for e in entries} <= {
            "ACTIVE",
            "UNLINKED",
            "REVERTED",
            "SUPERSEDED",
            "DELETED_IN_YNAB",
            "REKEYED",
        }

        # accounts: column added, data kept, invisible until a sync claims its budget.
        mirror = MirrorStore(conn)
        legacy_account = mirror.account("acct-main")
        assert legacy_account is not None and legacy_account.balance == 123
        assert legacy_account.budget_id is None
        assert mirror.accounts("b") == []
        assert mirror.get_meta("server_knowledge:budget:b") == "42"  # v1 key untouched
        assert mirror.get_server_knowledge("b") is None  # v2 starts with a full sync
    finally:
        conn.close()

    # The legacy table is a frozen archive: same rows, same DDL, same indexes.
    assert dump_table(prod_db, "transactions") == legacy_before
    assert schema_sql(prod_db, "transactions") == legacy_schema_before
    backups = list_backups(backup_dir, prod_db.name)
    assert len(backups) == 1 and backups[0].name.endswith(".pre-v1.db")
    archived = sqlite3.connect(backups[0])
    assert archived.execute("SELECT count(*) FROM transactions").fetchone()[0] == len(
        _production_rows()
    )
    assert archived.execute("PRAGMA user_version").fetchone()[0] == 0
    archived.close()


def test_migration_without_default_account_leaves_rows_unclaimed(
    prod_db: Path, backup_dir: Path
) -> None:
    conn = open_tx_db(prod_db, default_account_id="", backup_dir=backup_dir)
    store = LedgerStore(conn)
    unclaimed = {e.key for e in store.list_unassigned()}
    assert unclaimed == {"YNAB:-9200:2026-08-07:1", "YNAB:-9300:2026-08-07:1"}
    assert store.claim_unassigned("acct-main") == 2
    assert store.list_unassigned() == []
    conn.close()


def test_migration_is_idempotent(prod_db: Path, backup_dir: Path) -> None:
    conn = open_tx_db(prod_db, default_account_id="acct-main", backup_dir=backup_dir)
    first = _ledger(conn)
    # Importer activity after the migration must survive a re-run of the step.
    LedgerStore(conn).update("acct-main", "YNAB:-4000:2026-08-03:1", csv_state="COMPLETED")
    conn.close()

    # 1) Re-opening is a no-op: no step runs, no new backup.
    conn, result = migrate_tx_db_conn(
        prod_db, default_account_id="acct-main", backup_dir=backup_dir
    )
    assert result.applied == ()
    conn.close()
    assert len(list_backups(backup_dir, prod_db.name)) == 1

    # 2) Even if the version is lost, re-running the step adds nothing and overwrites nothing.
    raw = sqlite3.connect(prod_db)
    raw.execute("PRAGMA user_version = 0")
    raw.close()
    conn, result = migrate_tx_db_conn(
        prod_db, default_account_id="acct-main", backup_dir=backup_dir
    )
    assert result.applied == (1,)
    second = _ledger(conn)
    assert set(second) == set(first)
    assert second["YNAB:-4000:2026-08-03:1"]["csv_state"] == "COMPLETED"
    assert conn.execute("SELECT count(*) FROM import_ledger").fetchone()[0] == len(first)
    conn.close()


# ── older legacy schemas ──────────────────────────────────────────────────────


def test_F75_oldest_schema_without_columns_or_accounts_table_migrates(
    tmp_path: Path, backup_dir: Path
) -> None:
    """F75: a DB that predates account_id (and the accounts table) must open and migrate."""
    path = tmp_path / "transactions_5.db"
    make_legacy_db(
        path,
        OLDEST_TX_DDL,
        [
            {
                "import_id": "YNAB:-1000:2025-01-02:1",
                "date": "2025-01-02",
                "amount": -1000,
                "payee_name": "Bakery",
                "memo": None,
                "cleared": "cleared",
                "state": "COMPLETED",
                "ynab_tx_id": "y-old-1",
                "imported_at": "2025-01-02T08:00:00",
            },
            {
                "import_id": "YNAB:-2000:2025-01-03:1",
                "date": "2025-01-03",
                "amount": -2000,
                "payee_name": "Kiosk",
                "memo": "(pending)",
                "cleared": "uncleared",
                "state": "PENDING",
                "ynab_tx_id": None,
                "imported_at": "2025-01-03T08:00:00",
            },
            {
                "import_id": "ynab_manual:abc",
                "date": "2025-01-03",
                "amount": -1,
                "payee_name": "Manual",
                "memo": None,
                "cleared": "cleared",
                "state": "COMPLETED",
                "ynab_tx_id": "abc",
                "imported_at": "2025-01-03T08:00:00",
            },
        ],
    )
    conn = open_tx_db(path, default_account_id="acct-cli", backup_dir=backup_dir)
    ledger = _ledger(conn)
    assert set(ledger) == {"YNAB:-1000:2025-01-02:1", "YNAB:-2000:2025-01-03:1"}
    first = ledger["YNAB:-1000:2025-01-02:1"]
    assert first["account_id"] == "acct-cli" and first["status"] == "ACTIVE"
    assert first["started_at"] is None
    assert first["updated_at"] == "2025-01-02T08:00:00"  # falls back to imported_at
    second = ledger["YNAB:-2000:2025-01-03:1"]
    assert second["status"] == "UNLINKED" and second["csv_state"] == "PENDING"
    assert {"accounts", "sync_meta"} <= _tables(conn)
    assert "budget_id" in table_columns(conn, "accounts")
    # The legacy table keeps its old shape (no columns added by v2).
    assert "account_id" not in table_columns(conn, "transactions")
    conn.close()


@pytest.mark.parametrize(
    "missing",
    ["started_at", "account_id", "category_name", "approved", "source", "deleted", "updated_at"],
)
def test_legacy_db_missing_one_column(tmp_path: Path, backup_dir: Path, missing: str) -> None:
    columns = [
        "import_id TEXT PRIMARY KEY",
        "date TEXT NOT NULL",
        "amount INTEGER NOT NULL",
        "payee_name TEXT",
        "memo TEXT",
        "cleared TEXT",
        "state TEXT",
        "ynab_tx_id TEXT",
        "account_id TEXT",
        "category_name TEXT",
        "approved INTEGER",
        "deleted INTEGER DEFAULT 0",
        "source TEXT DEFAULT 'revolut'",
        "imported_at TEXT NOT NULL",
        "updated_at TEXT",
        "started_at TEXT",
    ]
    ddl = f"CREATE TABLE transactions ({', '.join(c for c in columns if not c.startswith(missing + ' '))});"
    path = tmp_path / "transactions_9.db"
    row = legacy_row("YNAB:-100:2026-01-01:1", ynab_tx_id="y1")
    row.pop(missing)
    make_legacy_db(path, ddl, [row])
    conn = open_tx_db(path, default_account_id="acct-main", backup_dir=backup_dir)
    ledger = _ledger(conn)
    assert list(ledger) == ["YNAB:-100:2026-01-01:1"]
    assert ledger["YNAB:-100:2026-01-01:1"]["status"] == "ACTIVE"
    assert ledger["YNAB:-100:2026-01-01:1"]["account_id"] == "acct-main"
    conn.close()


def test_legacy_accounts_table_missing_columns_is_completed(
    tmp_path: Path, backup_dir: Path
) -> None:
    path = tmp_path / "transactions_3.db"
    make_legacy_db(
        path,
        OLDEST_TX_DDL + "CREATE TABLE accounts (account_id TEXT PRIMARY KEY, name TEXT);",
    )
    raw = sqlite3.connect(path)
    raw.execute("INSERT INTO accounts VALUES ('acct-x', 'Old')")
    raw.commit()
    raw.close()
    conn = open_tx_db(path, backup_dir=backup_dir)
    mirror = MirrorStore(conn)
    mirror.upsert_accounts("b", [ynab_account("acct-x", name="New name")])
    account = mirror.account("acct-x")
    assert account is not None and account.name == "New name" and account.budget_id == "b"
    conn.close()


def test_cli_db_without_accounts_and_null_accounts(tmp_path: Path, backup_dir: Path) -> None:
    """The CLI's ~/.revolut_to_ynab.db has the same schema; its account comes from config."""
    path = tmp_path / ".revolut_to_ynab.db"
    make_legacy_db(
        path,
        PROD_TX_DDL,
        [legacy_row("YNAB:-100:2026-01-01:1", ynab_tx_id="y1", account_id=None)],
    )
    conn = open_tx_db(path, default_account_id="acct-cli", backup_dir=backup_dir)
    assert _ledger(conn)["YNAB:-100:2026-01-01:1"]["account_id"] == "acct-cli"
    conn.close()
    assert list_backups(backup_dir, ".revolut_to_ynab.db")


# ── F02 ghost rows ────────────────────────────────────────────────────────────


def test_F02_ghost_row_from_v1_sync_becomes_tombstone(tmp_path: Path, backup_dir: Path) -> None:
    """F02: v1 re-keyed a row locally; its sync re-inserted YNAB's old import_id as a second
    live row with the same ynab_tx_id. Only the CSV-backed row may stay ACTIVE, or the
    superseded sweep would delete the real transaction."""
    path = tmp_path / "transactions_4.db"
    rows = [
        # real row: new key (CSV date), started_at from the export
        legacy_row(
            "YNAB:-880000:2026-08-10:1",
            ynab_tx_id="Y1",
            date="2026-08-10",
            state="PENDING",
            cleared="uncleared",
            started_at="2026-08-10 08:30:00",
            imported_at="2026-08-10T09:00:00",
        ),
        # ghost: YNAB's import_id (old date) carrying YNAB's new date, inserted later by sync
        legacy_row(
            "YNAB:-880000:2026-08-09:1",
            ynab_tx_id="Y1",
            date="2026-08-10",
            state="PENDING",
            cleared="uncleared",
            payee_name="Hotel Bellagio (renamed)",
            imported_at="2026-08-10T10:00:00",
        ),
        # two legacy rows (no started_at): the key/date consistent one wins
        legacy_row("YNAB:-5000:2026-07-02:1", ynab_tx_id="Y2", date="2026-07-02"),
        legacy_row(
            "YNAB:-5000:2026-07-01:1",
            ynab_tx_id="Y2",
            date="2026-07-02",
            imported_at="2026-07-01T09:00:00",
        ),
    ]
    make_legacy_db(path, PROD_TX_DDL, rows)
    conn = open_tx_db(path, default_account_id="acct-main", backup_dir=backup_dir)
    ledger = _ledger(conn)
    assert ledger["YNAB:-880000:2026-08-10:1"]["status"] == "ACTIVE"
    assert ledger["YNAB:-880000:2026-08-09:1"]["status"] == "REKEYED"
    assert ledger["YNAB:-880000:2026-08-09:1"]["rekeyed_to"] == "YNAB:-880000:2026-08-10:1"
    # The keeper is chosen on the legacy date column; csv_date itself comes from the key.
    assert ledger["YNAB:-880000:2026-08-09:1"]["csv_date"] == "2026-08-09"
    assert ledger["YNAB:-5000:2026-07-02:1"]["status"] == "ACTIVE"
    assert ledger["YNAB:-5000:2026-07-01:1"]["status"] == "REKEYED"
    assert ledger["YNAB:-5000:2026-07-01:1"]["csv_date"] == "2026-07-01"
    active_links = [
        e.ynab_tx_id
        for e in LedgerStore(conn).list_for_account("acct-main")
        if e.status == "ACTIVE"
    ]
    assert sorted(active_links) == ["Y1", "Y2"]  # one live entry per YNAB transaction
    conn.close()


def test_legacy_rekeyed_row_without_successor_keeps_no_pointer(
    tmp_path: Path, backup_dir: Path
) -> None:
    path = tmp_path / "transactions_6.db"
    make_legacy_db(
        path,
        PROD_TX_DDL,
        [
            legacy_row("YNAB:-100:2026-08-01:1", state="REKEYED", ynab_tx_id="Y-gone"),
            legacy_row("YNAB:-200:2026-08-01:1", state="REKEYED"),  # no YNAB id at all
            # A successor in another account does not count (F12: ledgers are per account).
            legacy_row("YNAB:-100:2026-08-02:1", ynab_tx_id="Y-gone", account_id="acct-x"),
        ],
    )
    conn = open_tx_db(path, default_account_id="acct-main", backup_dir=backup_dir)
    store = LedgerStore(conn)
    orphan = store.get("acct-main", "YNAB:-100:2026-08-01:1")
    assert orphan is not None and orphan.status == "REKEYED" and orphan.rekeyed_to is None
    no_id = store.get("acct-main", "YNAB:-200:2026-08-01:1")
    assert no_id is not None and no_id.status == "REKEYED" and no_id.rekeyed_to is None
    assert "YNAB:-100:2026-08-01:1" in store.keys_in_use("acct-main")  # still reserved
    conn.close()


# ── CSV identity: key vs. YNAB-overwritten columns ────────────────────────────


def _migrate_rows_directly(
    path: Path, rows: list[dict]
) -> tuple[sqlite3.Connection, LegacyMigrationStats]:
    make_legacy_db(path, PROD_TX_DDL, rows)
    conn = connect(path)
    create_v1_schema(conn)
    with transaction(conn):
        stats = migrate_legacy_rows(conn, "acct-main")
    return conn, stats


def test_F33_csv_identity_comes_from_the_key_not_ynab_edited_columns(tmp_path: Path) -> None:
    """F33 class: v1's syncs wrote YNAB's (user-edited) amount/date into the legacy columns.

    For ``YNAB:{milli}:{date}:{n}`` keys the CSV identity and what v1 pushed come from the
    key; other key shapes fall back to the columns.
    """
    conn, stats = _migrate_rows_directly(
        tmp_path / "transactions_8.db",
        [
            # amount edited in YNAB, copied by v1's db_upsert ON CONFLICT
            legacy_row("YNAB:-100000:2026-08-01:1", ynab_tx_id="y1", amount=-50000),
            # first seen by v1's sync with a date the user changed in YNAB
            legacy_row("YNAB:-2000:2026-08-02:1", ynab_tx_id="y2", amount=-2000, date="2026-08-05"),
            # consistent row: nothing to override
            legacy_row("YNAB:4200:2026-08-03:2", ynab_tx_id="y3", amount=4200, date="2026-08-03"),
            # YNAB's own pending-id shape and an impossible date: the columns are all we have
            legacy_row("YNAB:P:-9500:2026-08-08:1", ynab_tx_id="y4", amount=-9400),
            legacy_row("YNAB:-700:2026-13-45:1", ynab_tx_id="y5", amount=-700, date="2026-08-06"),
        ],
    )
    ledger = _ledger(conn)

    def identity(key: str) -> tuple:
        row = ledger[key]
        return row["csv_amount"], row["csv_date"], row["pushed_amount"], row["pushed_date"]

    assert identity("YNAB:-100000:2026-08-01:1") == (-100000, "2026-08-01", -100000, "2026-08-01")
    assert identity("YNAB:-2000:2026-08-02:1") == (-2000, "2026-08-02", -2000, "2026-08-02")
    assert identity("YNAB:4200:2026-08-03:2") == (4200, "2026-08-03", 4200, "2026-08-03")
    assert identity("YNAB:P:-9500:2026-08-08:1") == (-9400, "2026-08-01", -9400, "2026-08-01")
    assert identity("YNAB:-700:2026-13-45:1") == (-700, "2026-08-06", -700, "2026-08-06")
    assert stats.identity_from_key == 2
    assert stats.inserted == 5 and stats.already_present == 0
    conn.close()


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"amount": "abc"}, "invalid literal"),  # INTEGER affinity keeps non-numeric text
        ({"payee_name": b"\xff\x00"}, "csv_payee"),  # a BLOB survives TEXT affinity
    ],
)
def test_invalid_legacy_row_aborts_migration_behind_its_backup(
    tmp_path: Path, backup_dir: Path, overrides: dict, reason: str
) -> None:
    path = tmp_path / "transactions_10.db"
    make_legacy_db(
        path,
        PROD_TX_DDL,
        [
            legacy_row("YNAB:-100:2026-08-01:1", ynab_tx_id="y1"),
            legacy_row("YNAB:-200:2026-08-01:1", ynab_tx_id="y2", **overrides),
        ],
    )
    legacy_before = dump_table(path, "transactions")
    with pytest.raises(MigrationError) as info:
        open_tx_db(path, default_account_id="acct-main", backup_dir=backup_dir)
    message = str(info.value)
    assert "YNAB:-200:2026-08-01:1" in message and reason in message
    raw = sqlite3.connect(path)
    try:
        assert raw.execute("PRAGMA user_version").fetchone()[0] == 0
        tables = {r[0] for r in raw.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "import_ledger" not in tables  # the whole step rolled back, DDL included
    finally:
        raw.close()
    assert dump_table(path, "transactions") == legacy_before
    assert len(list_backups(backup_dir, path.name)) == 1


# ── legacy csv_cache ──────────────────────────────────────────────────────────


def test_newest_legacy_csv_is_registered_once(
    tmp_path: Path, prod_db: Path, backup_dir: Path
) -> None:
    cache = tmp_path / "csv_cache" / "59213669"
    cache.mkdir(parents=True)
    old = cache / "account-statement_2026-01-01_2026-07-31.csv"
    old.write_text("old")
    newest = cache / "account-statement_2026-01-01_2026-08-31.csv"
    newest.write_bytes(b"Type,Product\nnewest\n")
    (cache / "notes.txt").write_text("ignored")
    os.utime(old, (1_700_000_000, 1_700_000_000))
    os.utime(newest, (1_800_000_000, 1_800_000_000))

    conn = open_tx_db(
        prod_db, default_account_id="acct-main", backup_dir=backup_dir, csv_cache_dir=cache
    )
    files = CsvFileStore(conn)
    latest = files.latest()
    assert latest is not None
    assert latest.status == "legacy" and latest.source == "legacy"
    assert latest.stored_path == str(newest)
    assert latest.original_name == newest.name
    assert latest.account_id == "acct-main"
    assert latest.sha256 == hashlib.sha256(b"Type,Product\nnewest\n").hexdigest()
    assert latest.uploaded_at == "2027-01-15T08:00:00Z"
    assert files.latest("acct-main") == latest
    conn.close()

    raw = sqlite3.connect(prod_db)
    raw.execute("PRAGMA user_version = 0")
    raw.close()
    conn = open_tx_db(
        prod_db, default_account_id="acct-main", backup_dir=backup_dir, csv_cache_dir=cache
    )
    assert conn.execute("SELECT count(*) FROM csv_files").fetchone()[0] == 1
    conn.close()


def test_missing_or_empty_csv_cache_is_fine(tmp_path: Path, backup_dir: Path) -> None:
    empty = tmp_path / "csv_cache" / "1"
    empty.mkdir(parents=True)
    conn = open_tx_db(tmp_path / "a.db", backup_dir=backup_dir, csv_cache_dir=empty)
    assert CsvFileStore(conn).latest() is None
    conn.close()
    conn = open_tx_db(tmp_path / "b.db", backup_dir=backup_dir, csv_cache_dir=tmp_path / "nope")
    assert CsvFileStore(conn).latest() is None
    conn.close()


# ── after migration: mirror + refresh ─────────────────────────────────────────


def test_refresh_after_first_sync_resolves_migrated_rows(prod_db: Path, backup_dir: Path) -> None:
    conn = open_tx_db(prod_db, default_account_id="acct-main", backup_dir=backup_dir)
    mirror = MirrorStore(conn)
    ledger = LedgerStore(conn)
    mirror.upsert_transactions(
        "b",
        [
            # DUPLICATE without id: YNAB has it under the key → linked
            ynab_tx("y-dup-found", import_id="YNAB:-6000:2026-08-04:1", amount=-6000),
            # REKEYED pair: YNAB kept the old key as import_id
            ynab_tx("y-rekey", import_id="YNAB:-7000:2026-08-05:1", date="2026-08-04"),
        ],
        server_knowledge=500,
    )
    result = ledger.refresh_from_mirror("acct-main", mirror.view_for_account("b", "acct-main"))
    assert result.linked == 1 and result.import_ids_corrected == 1
    dup = ledger.get("acct-main", "YNAB:-6000:2026-08-04:1")
    assert dup is not None and dup.status == "ACTIVE" and dup.ynab_tx_id == "y-dup-found"
    live = ledger.get("acct-main", "YNAB:-7000:2026-08-04:1")
    assert live is not None and live.ynab_import_id == "YNAB:-7000:2026-08-05:1"
    assert "YNAB:-7000:2026-08-05:1" in ledger.keys_in_use("acct-main")
    conn.close()


def test_F02_unlinked_duplicate_twin_becomes_tombstone_after_first_sync(
    tmp_path: Path, backup_dir: Path
) -> None:
    """F02: v1 re-keyed the hotel to the 08-03 key, while YNAB kept the 08-04 import_id; a later
    v1 import recorded the 08-04 key as DUPLICATE without an id. After the first sync that key
    resolves to the hotel's transaction. It must become a tombstone, not a second ACTIVE link
    that the superseded sweep could use to delete the real transaction."""
    path = tmp_path / "transactions_11.db"
    new_key, old_key = "YNAB:-880000:2026-08-03:1", "YNAB:-880000:2026-08-04:1"
    make_legacy_db(
        path,
        PROD_TX_DDL,
        [
            legacy_row(
                new_key,
                ynab_tx_id="Y1",
                date="2026-08-03",
                amount=-880000,
                state="PENDING",
                cleared="uncleared",
                started_at="2026-08-03 20:30:00",
            ),
            legacy_row(old_key, state="DUPLICATE", date="2026-08-04", amount=-880000),
        ],
    )
    conn = open_tx_db(path, default_account_id="acct-main", backup_dir=backup_dir)
    store, mirror = LedgerStore(conn), MirrorStore(conn)
    assert store.get("acct-main", old_key).status == "UNLINKED"  # type: ignore[union-attr]
    mirror.upsert_transactions(
        "b",
        [ynab_tx("Y1", import_id=old_key, date="2026-08-03", amount=-880000, cleared="uncleared")],
    )
    result = store.refresh_from_mirror("acct-main", mirror.view_for_account("b", "acct-main"))
    assert result.conflicts == (old_key,) and result.linked == 0
    twin = store.get("acct-main", old_key)
    assert twin is not None and twin.status == "REKEYED" and twin.rekeyed_to == new_key
    live = store.get("acct-main", new_key)
    assert live is not None and live.status == "ACTIVE" and live.ynab_import_id == old_key
    assert store.active_tx_ids("acct-main") == {"Y1"}
    assert [e.key for e in store.by_ynab_tx_id("Y1") if e.status == "ACTIVE"] == [new_key]
    conn.close()


def test_connect_on_migrated_db_reads_v2_tables_readonly(prod_db: Path, backup_dir: Path) -> None:
    open_tx_db(prod_db, default_account_id="acct-main", backup_dir=backup_dir).close()
    ro = connect(prod_db, readonly=True)
    assert ro.execute("SELECT count(*) FROM import_ledger").fetchone()[0] > 0
    ro.close()
