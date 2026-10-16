"""Per-user (and CLI) transactions DB: v2 schema and the legacy → v1 migration (DESIGN §5.2/§5.3).

Version 1 adds the importer ledger (``import_ledger``), the YNAB mirror (``ynab_transactions``,
``ynab_subtransactions``, ``accounts.budget_id``) and ``csv_files``, then copies the importer's
own rows out of the v1 ``transactions`` table into the ledger. The legacy table itself is never
modified (an emergency rollback to the v1 image still finds it), and rows that v1's YNAB sync
wrote (``source='ynab'``) are not migrated: the first v2 sync rebuilds the mirror from YNAB.

Every statement is idempotent (``IF NOT EXISTS``, ``INSERT OR IGNORE``, column checks), so the
step is safe on fresh DBs, on the production DDL, on older DBs that lack columns or the
``accounts`` table, and when it is re-run.

Every migrated row is validated as a :class:`~ynab_helper.storage.ledger.LedgerEntry` before
anything is written: invalid legacy data fails the step (behind its backup, with the file left
at v0) instead of surfacing later as an unreadable ledger.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ynab_helper.storage.db import Migration, ensure_columns, table_columns, table_exists
from ynab_helper.storage.ledger import (
    CSV_STATES,
    IDENTITY_LEGACY,
    UNASSIGNED_ACCOUNT,
    CsvState,
    LedgerEntry,
    LedgerStatus,
    LedgerStore,
)
from ynab_helper.storage.mirror import CLEARED_LIKE
from ynab_helper.timeutil import utc_now_iso

log = logging.getLogger(__name__)

TX_SCHEMA_VERSION = 1

_SYNC_META_DDL = """
CREATE TABLE IF NOT EXISTS sync_meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
)"""

# v1 DDL of the accounts table plus v2's budget_id (rollback-compatible: only a column added).
_ACCOUNTS_DDL = """
CREATE TABLE IF NOT EXISTS accounts (
  account_id        TEXT PRIMARY KEY,
  name              TEXT,
  type              TEXT,
  classification    TEXT,
  on_budget         INTEGER NOT NULL DEFAULT 1,
  closed            INTEGER NOT NULL DEFAULT 0,
  deleted           INTEGER NOT NULL DEFAULT 0,
  note              TEXT,
  balance           INTEGER,
  cleared_balance   INTEGER,
  uncleared_balance INTEGER,
  transfer_payee_id TEXT,
  last_seen_at      TEXT,
  first_seen_at     TEXT NOT NULL,
  created_at        TEXT NOT NULL,
  updated_at        TEXT NOT NULL,
  budget_id         TEXT
)"""

# Columns the mirror writes; added to older accounts tables that lack them. ADD COLUMN cannot
# add NOT NULL without a default, hence the '' defaults for the timestamp columns.
_ACCOUNTS_COLUMNS = {
    "name": "TEXT",
    "type": "TEXT",
    "classification": "TEXT",
    "on_budget": "INTEGER NOT NULL DEFAULT 1",
    "closed": "INTEGER NOT NULL DEFAULT 0",
    "deleted": "INTEGER NOT NULL DEFAULT 0",
    "note": "TEXT",
    "balance": "INTEGER",
    "cleared_balance": "INTEGER",
    "uncleared_balance": "INTEGER",
    "transfer_payee_id": "TEXT",
    "last_seen_at": "TEXT",
    "first_seen_at": "TEXT NOT NULL DEFAULT ''",
    "created_at": "TEXT NOT NULL DEFAULT ''",
    "updated_at": "TEXT NOT NULL DEFAULT ''",
    "budget_id": "TEXT",
}

_V1_DDL = (
    """
CREATE TABLE IF NOT EXISTS import_ledger (
  account_id      TEXT NOT NULL,
  key             TEXT NOT NULL,
  ynab_import_id  TEXT,
  ynab_tx_id      TEXT,
  status          TEXT NOT NULL,
  csv_date        TEXT NOT NULL,
  csv_amount      INTEGER NOT NULL,
  csv_payee       TEXT,
  csv_type        TEXT,
  csv_state       TEXT NOT NULL,
  started_at      TEXT,
  pushed_cleared  TEXT,
  pushed_amount   INTEGER,
  pushed_date     TEXT,
  identity        TEXT NOT NULL DEFAULT 'csv',
  rekeyed_to      TEXT,
  last_error      TEXT,
  first_seen_at   TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  PRIMARY KEY (account_id, key)
)""",
    "CREATE INDEX IF NOT EXISTS idx_ledger_tx ON import_ledger(ynab_tx_id)",
    "CREATE INDEX IF NOT EXISTS idx_ledger_date ON import_ledger(account_id, csv_date)",
    """
CREATE TABLE IF NOT EXISTS ynab_transactions (
  id TEXT PRIMARY KEY, budget_id TEXT NOT NULL, account_id TEXT NOT NULL, date TEXT NOT NULL,
  amount INTEGER NOT NULL, payee_id TEXT, payee_name TEXT, memo TEXT,
  cleared TEXT NOT NULL,
  approved INTEGER NOT NULL, flag_color TEXT, category_id TEXT, category_name TEXT,
  transfer_account_id TEXT, transfer_transaction_id TEXT, matched_transaction_id TEXT,
  import_id TEXT, deleted INTEGER NOT NULL DEFAULT 0, synced_at TEXT NOT NULL
)""",
    "CREATE INDEX IF NOT EXISTS idx_ytx_acct_date ON ynab_transactions(budget_id, account_id, date)",
    "CREATE INDEX IF NOT EXISTS idx_ytx_import ON ynab_transactions(account_id, import_id)",
    """
CREATE TABLE IF NOT EXISTS ynab_subtransactions (
  id TEXT PRIMARY KEY, transaction_id TEXT NOT NULL, amount INTEGER NOT NULL, memo TEXT,
  payee_id TEXT, payee_name TEXT, category_id TEXT, category_name TEXT,
  transfer_account_id TEXT, deleted INTEGER NOT NULL DEFAULT 0
)""",
    "CREATE INDEX IF NOT EXISTS idx_ysub_parent ON ynab_subtransactions(transaction_id)",
    """
CREATE TABLE IF NOT EXISTS csv_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT, stored_path TEXT NOT NULL,
  original_name TEXT, sha256 TEXT NOT NULL, uploaded_at TEXT NOT NULL, source TEXT NOT NULL,
  currency TEXT, row_count INTEGER, window_start TEXT, window_end TEXT,
  status TEXT NOT NULL,
  imported_at TEXT, result_json TEXT
)""",
    # Additive helper indexes (not in the §5.2 listing; they only speed up lookups).
    "CREATE INDEX IF NOT EXISTS idx_csv_files_account ON csv_files(account_id, uploaded_at)",
    "CREATE INDEX IF NOT EXISTS idx_accounts_budget ON accounts(budget_id)",
    # v1 creates these too; keeping them means a v1 rollback finds the same schema.
    "CREATE INDEX IF NOT EXISTS idx_accounts_classification ON accounts(classification)",
    "CREATE INDEX IF NOT EXISTS idx_accounts_active ON accounts(closed, deleted)",
)

# Legacy columns read by the data migration; any of them may be missing in older DBs.
_LEGACY_COLUMNS = (
    "import_id",
    "date",
    "amount",
    "payee_name",
    "cleared",
    "state",
    "ynab_tx_id",
    "account_id",
    "deleted",
    "source",
    "imported_at",
    "updated_at",
    "started_at",
)
_CARRIED_STATUSES = frozenset(
    {
        LedgerStatus.REVERTED.value,
        LedgerStatus.SUPERSEDED.value,
        LedgerStatus.DELETED_IN_YNAB.value,
        LedgerStatus.REKEYED.value,
    }
)
# v1 built every importer key from the parsed CSV row as YNAB:{milliunits}:{started date}:{n}
# (fee included), and only ever sent that amount and date to YNAB for the key: on create, on
# its drift PATCH and on a re-key PATCH (which moved the row to a new key built the same way).
_V1_KEY_RE = re.compile(r"^YNAB:(-?\d+):(\d{4}-\d{2}-\d{2}):\d+$")


@dataclass(frozen=True, slots=True)
class TxMigrationContext:
    """Inputs of the legacy → v1 data migration.

    ``default_account_id``: the owner's configured YNAB account; legacy rows without an
    account are assigned to it (``''`` leaves them unclaimed until the next import).
    ``csv_cache_dir``: the owner's ``csv_cache/<telegram_id>`` directory, if any; its newest
    CSV is registered in ``csv_files`` with status ``legacy``.
    """

    default_account_id: str = ""
    csv_cache_dir: Path | None = None


def migrations(ctx: TxMigrationContext | None = None) -> list[Migration]:
    """The transactions-DB migration list for :func:`ynab_helper.storage.db.migrate`."""
    context = ctx or TxMigrationContext()
    return [
        Migration(
            1,
            "import ledger, YNAB mirror and csv_files; legacy rows → ledger",
            lambda conn: _apply_v1(conn, context),
        )
    ]


def create_v1_schema(conn: sqlite3.Connection) -> None:
    """Create every v1 table and index that is missing (no data migration)."""
    conn.execute(_SYNC_META_DDL)
    conn.execute(_ACCOUNTS_DDL)
    ensure_columns(conn, "accounts", _ACCOUNTS_COLUMNS)
    for statement in _V1_DDL:
        conn.execute(statement)


def _apply_v1(conn: sqlite3.Connection, ctx: TxMigrationContext) -> None:
    create_v1_schema(conn)
    if table_exists(conn, "transactions"):
        migrate_legacy_rows(conn, ctx.default_account_id)
    if ctx.csv_cache_dir is not None:
        register_legacy_csv(conn, ctx.csv_cache_dir, ctx.default_account_id or None)


# ── legacy rows → ledger ──────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class LegacyMigrationStats:
    """Counts reported by :func:`migrate_legacy_rows` (also logged).

    ``identity_from_key``: rows whose legacy ``amount``/``date`` columns disagreed with their
    ``YNAB:{milli}:{date}:{n}`` key — v1's syncs had copied a user's YNAB edit into them — so
    the CSV identity was taken from the key.
    """

    inserted: int
    already_present: int
    skipped_not_importer: int
    ghosts_collapsed: int
    identity_from_key: int
    by_status: dict[str, int]


@dataclass(slots=True)
class _MigratedRow:
    """A ledger row under construction (mutable until validated as a LedgerEntry)."""

    fields: dict[str, Any]
    legacy_date: str  # the v1 ``date`` column: YNAB's date on rows v1's sync inserted
    identity_from_key: bool

    @property
    def key(self) -> str:
        return str(self.fields["key"])

    @property
    def status(self) -> str:
        return str(self.fields["status"])


def _is_importer_row(source: str | None, key: str) -> bool:
    if source is None:
        return key.startswith("YNAB:")
    return source == "revolut"


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    return int(str(value).strip())


def _ledger_status(state: str, deleted: bool, ynab_tx_id: str | None) -> str:
    if state in _CARRIED_STATUSES:
        return state
    if deleted:
        return LedgerStatus.DELETED_IN_YNAB.value
    # Covers COMPLETED/PENDING and v1's DUPLICATE rows: linked → ACTIVE, else resolve later.
    return LedgerStatus.ACTIVE.value if ynab_tx_id else LedgerStatus.UNLINKED.value


def _csv_state(state: str, cleared: str | None) -> str:
    cleared_like = cleared in CLEARED_LIKE
    # v1's two YNAB syncs disagreed on 'reconciled' (174 production rows are PENDING +
    # reconciled): YNAB having it cleared proves the pending completed.
    if state == CsvState.PENDING and cleared_like:
        return CsvState.COMPLETED.value
    if state in CSV_STATES:
        return state
    return CsvState.COMPLETED.value if cleared_like else CsvState.PENDING.value


def _key_identity(key: str) -> tuple[int, str] | None:
    """``(milliunits, date)`` encoded in a v1 ``YNAB:{milli}:{date}:{n}`` key, else None.

    Other shapes (e.g. YNAB's own ``YNAB:P:…`` pending ids) carry no reliable CSV identity.
    """
    match = _V1_KEY_RE.match(key)
    if match is None:
        return None
    try:
        date.fromisoformat(match.group(2))
    except ValueError:
        return None
    return int(match.group(1)), match.group(2)


def _legacy_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    present = table_columns(conn, "transactions")
    select = ", ".join(c if c in present else f"NULL AS {c}" for c in _LEGACY_COLUMNS)
    return [dict(r) for r in conn.execute(f"SELECT {select} FROM transactions")]


def _to_ledger_row(row: dict[str, Any], default_account_id: str, now: str) -> _MigratedRow:
    key = str(row["import_id"])
    state = str(row["state"] or "").strip().upper()
    cleared = (row["cleared"] or "").strip().lower() or None
    ynab_tx_id = row["ynab_tx_id"] or None
    column_amount = _as_int(row["amount"])
    column_date = str(row["date"])
    deleted = bool(_as_int(row["deleted"])) if row["deleted"] is not None else False
    imported_at = row["imported_at"] or now
    # v1's syncs overwrote `amount` (and set `date` on rows they inserted) with YNAB's values,
    # so a user's edit in YNAB sits in those columns. Taking them as the CSV identity would let
    # the planner revert that edit, or miss the match and create a duplicate. The key holds
    # what the CSV said and what v1 wrote to YNAB.
    from_key = _key_identity(key)
    csv_amount, csv_date = from_key if from_key else (column_amount, column_date)
    return _MigratedRow(
        fields={
            "account_id": row["account_id"] or default_account_id or UNASSIGNED_ACCOUNT,
            "key": key,
            "ynab_import_id": key,  # corrected from the mirror after the first sync
            "ynab_tx_id": ynab_tx_id,
            "status": _ledger_status(state, deleted, ynab_tx_id),
            "csv_date": csv_date,
            "csv_amount": csv_amount,
            "csv_payee": row["payee_name"],
            "csv_type": None,
            "csv_state": _csv_state(state, cleared),
            "started_at": row["started_at"] or None,
            "pushed_cleared": "cleared" if cleared in CLEARED_LIKE else "uncleared",
            "pushed_amount": csv_amount,
            "pushed_date": csv_date,
            "identity": IDENTITY_LEGACY,
            "rekeyed_to": None,
            "last_error": None,
            "first_seen_at": imported_at,
            "updated_at": row["updated_at"] or imported_at,
        },
        legacy_date=column_date,
        identity_from_key=from_key is not None and from_key != (column_amount, column_date),
    )


def _collapse_shared_links(rows: list[_MigratedRow]) -> int:
    """Turn F02 "ghost" rows into REKEYED tombstones; returns how many were collapsed.

    v1 re-keyed a date-shifted row locally while YNAB kept the old import_id, and its web sync
    then re-inserted the old key as a second live row with the same ynab_tx_id. Migrating both
    as ACTIVE would let the superseded sweep delete the real transaction (finding F02). Of the
    ACTIVE rows sharing one YNAB transaction in an account, the one carrying CSV evidence is
    kept: a ``started_at`` from an export, then a key whose date matches the legacy ``date``
    column (a ghost carries YNAB's re-dated date under the old key), then the oldest. The
    others become REKEYED tombstones pointing at it.
    """
    groups: dict[tuple[str, str], list[_MigratedRow]] = {}
    for row in rows:
        if row.status == LedgerStatus.ACTIVE:
            groups.setdefault((row.fields["account_id"], row.fields["ynab_tx_id"]), []).append(row)
    collapsed = 0
    for (account_id, ynab_tx_id), group in groups.items():
        if len(group) < 2:
            continue
        oldest_first = sorted(group, key=lambda r: (r.fields["first_seen_at"] or "", r.key))
        keeper = max(oldest_first, key=_keeper_rank)
        for row in group:
            if row is keeper:
                continue
            row.fields["status"] = LedgerStatus.REKEYED.value
            row.fields["rekeyed_to"] = keeper.key
            collapsed += 1
        log.warning(
            "legacy migration: %d rows of account %s shared YNAB transaction %s; kept %s, "
            "the others became REKEYED tombstones",
            len(group),
            account_id,
            ynab_tx_id,
            keeper.key,
        )
    return collapsed


def _keeper_rank(row: _MigratedRow) -> tuple[bool, bool]:
    key_identity = _key_identity(row.key)
    key_date_matches = key_identity is not None and key_identity[1] == row.legacy_date
    return row.fields["started_at"] is not None, key_date_matches


def _resolve_rekeyed(rows: list[_MigratedRow]) -> None:
    """Point each legacy REKEYED row at the live row sharing its ynab_tx_id (same account)."""
    successors: dict[tuple[str, str], list[_MigratedRow]] = {}
    for row in rows:
        if row.fields["ynab_tx_id"] and row.status != LedgerStatus.REKEYED:
            key = (row.fields["account_id"], row.fields["ynab_tx_id"])
            successors.setdefault(key, []).append(row)
    for row in rows:
        fields = row.fields
        if row.status != LedgerStatus.REKEYED or fields["rekeyed_to"] or not fields["ynab_tx_id"]:
            continue
        candidates = successors.get((fields["account_id"], fields["ynab_tx_id"]), [])
        if not candidates:
            # The successor is gone (or in another account); the tombstone still reserves
            # its key, it just points nowhere.
            continue
        best = max(
            candidates,
            key=lambda c: (c.status == LedgerStatus.ACTIVE, c.fields["updated_at"] or "", c.key),
        )
        fields["rekeyed_to"] = best.key


def _validated(row: _MigratedRow) -> LedgerEntry:
    try:
        return LedgerEntry(**row.fields)
    except ValueError as exc:
        raise ValueError(f"legacy row {row.key!r} cannot be migrated: {exc}") from exc


def migrate_legacy_rows(
    conn: sqlite3.Connection, default_account_id: str = ""
) -> LegacyMigrationStats:
    """Copy the importer's legacy rows into ``import_ledger`` (DESIGN §5.3).

    Only rows with ``source='revolut'`` (or no source and a ``YNAB:`` key) are copied; rows
    already in the ledger are left untouched (``INSERT OR IGNORE``), so re-running is safe.
    The legacy table is only read. For v1 keys (``YNAB:{milli}:{date}:{n}``) ``csv_amount``,
    ``csv_date`` and ``pushed_amount``/``pushed_date`` come from the key, not from the
    columns v1's syncs overwrote with YNAB's values (a deviation from §5.3's wording).

    Raises ValueError if a legacy row cannot form a valid ledger entry; nothing is written.
    """
    now = utc_now_iso()
    rows: list[_MigratedRow] = []
    skipped = 0
    for legacy in _legacy_rows(conn):
        key = legacy["import_id"]
        if key is None or str(key) == "" or not _is_importer_row(legacy["source"], str(key)):
            skipped += 1
            continue
        try:
            rows.append(_to_ledger_row(legacy, default_account_id, now))
        except ValueError as exc:
            raise ValueError(f"legacy row {str(key)!r} cannot be migrated: {exc}") from exc
    ghosts = _collapse_shared_links(rows)
    _resolve_rekeyed(rows)
    entries = [_validated(row) for row in rows]
    inserted = LedgerStore(conn).insert_missing(entries)
    stats = LegacyMigrationStats(
        inserted=inserted,
        already_present=len(entries) - inserted,
        skipped_not_importer=skipped,
        ghosts_collapsed=ghosts,
        identity_from_key=sum(row.identity_from_key for row in rows),
        by_status=dict(Counter(entry.status for entry in entries)),
    )
    log.info(
        "legacy migration: %d ledger rows inserted, %d already present, %d non-importer rows "
        "skipped, %d ghost rows collapsed, %d rows took amount/date from the key (YNAB edits "
        "in the legacy columns), statuses %s",
        stats.inserted,
        stats.already_present,
        stats.skipped_not_importer,
        stats.ghosts_collapsed,
        stats.identity_from_key,
        stats.by_status,
    )
    return stats


# ── legacy csv_cache → csv_files ──────────────────────────────────────────────


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _newest_csv(directory: Path) -> Path | None:
    candidates = [
        p
        for p in directory.iterdir()
        if p.suffix.lower() == ".csv" and p.is_file() and not p.is_symlink()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: (p.stat().st_mtime, p.name))


def register_legacy_csv(
    conn: sqlite3.Connection, csv_cache_dir: Path, account_id: str | None
) -> int | None:
    """Record the newest CSV of a v1 ``csv_cache/<id>/`` directory as a ``legacy`` csv_files row.

    Returns the new row id, or None if there is nothing (new) to register. A file that cannot
    be read is logged and skipped: a missing statement must not block the schema upgrade.
    """
    directory = Path(csv_cache_dir)
    try:
        if not directory.is_dir():
            return None
        newest = _newest_csv(directory)
        if newest is None:
            return None
        sha256 = _sha256_file(newest)
        mtime = datetime.fromtimestamp(newest.stat().st_mtime, UTC)
    except OSError as exc:
        log.warning("legacy csv_cache %s not registered: %s", directory, exc)
        return None
    stored_path = str(newest)
    if conn.execute(
        "SELECT 1 FROM csv_files WHERE stored_path = ? AND sha256 = ?", (stored_path, sha256)
    ).fetchone():
        return None
    cursor = conn.execute(
        "INSERT INTO csv_files (account_id, stored_path, original_name, sha256, uploaded_at, "
        "source, status) VALUES (?, ?, ?, ?, ?, 'legacy', 'legacy')",
        (account_id, stored_path, newest.name, sha256, mtime.strftime("%Y-%m-%dT%H:%M:%SZ")),
    )
    log.info("registered legacy statement %s as csv_files #%s", newest.name, cursor.lastrowid)
    return cursor.lastrowid
