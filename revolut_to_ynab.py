#!/usr/bin/env python3
"""
Revolut CSV → YNAB Importer

Reads a Revolut account statement CSV and pushes transactions
directly to YNAB via their API. Tracks all imported transactions
in a local SQLite database so only new or changed transactions
are sent to YNAB.

Setup:
  1. Get your YNAB Personal Access Token:
     → Sign in at app.ynab.com → Account Settings → Developer Settings → New Token
  2. Find your Budget ID and Account ID:
     → Run this script with --list-budgets to see your budgets
     → Run with --list-accounts --budget-id <ID> to see accounts
  3. Set environment variables or pass as arguments:
     export YNAB_TOKEN="your-token-here"
     export YNAB_BUDGET_ID="your-budget-id"
     export YNAB_ACCOUNT_ID="your-account-id"

Usage:
  python3 revolut_to_ynab.py                       # Auto-pick latest CSV in CSV_DIR (prompts to confirm)
  python3 revolut_to_ynab.py <revolut_csv_file>    # Import a specific CSV
  python3 revolut_to_ynab.py -y                    # Auto-pick latest CSV without prompting
  python3 revolut_to_ynab.py --sync                # Sync transactions from YNAB → local DB
  python3 revolut_to_ynab.py --list-budgets
  python3 revolut_to_ynab.py --list-accounts
  python3 revolut_to_ynab.py --watch ~/Downloads   # Watch folder for new CSVs
  python3 revolut_to_ynab.py --db-stats            # Show database stats
  python3 revolut_to_ynab.py --crypto-sync          # Sync crypto portfolio value → YNAB
  python3 revolut_to_ynab.py --brokerage-sync       # Sync Interactive Brokers NAV → YNAB
  python3 revolut_to_ynab.py --reconcile            # Reconcile YNAB cleared balance to latest CSV
  python3 revolut_to_ynab.py --cleanup-pending-memos # Strip stale '(pending)' memos from cleared YNAB txns
"""

import csv
import json
import logging
import os
import re
import sqlite3
import sys
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import ssl
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# ─── Configuration ───────────────────────────────────────────────────────────

YNAB_BASE_URL = "https://api.ynab.com/v1"
DEFAULT_DB_PATH = Path.home() / ".revolut_to_ynab.db"
DEFAULT_LOG_PATH = Path.home() / ".revolut_to_ynab.log"
DEFAULT_CSV_DIR = Path.home() / "Downloads"

# Module-level logger — configured in setup_logging() during main()
log = logging.getLogger("revolut_to_ynab")
log.addHandler(logging.NullHandler())


def setup_logging(level_str="INFO", log_file=None):
    """Configure the module logger.

    `level_str`: one of DEBUG, INFO, WARNING, ERROR (case-insensitive).
    `log_file`:  path to a log file. "" or None disables file logging.

    Logs go ONLY to the file (stdout is already used for human-readable
    progress output), so the log file ends up as a clean audit trail of
    every transaction / sync / reconciliation.
    """
    level = getattr(logging, (level_str or "INFO").upper(), logging.INFO)
    log.setLevel(level)
    log.propagate = False
    # Drop any previously-installed handlers (idempotent)
    for h in list(log.handlers):
        log.removeHandler(h)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if log_file:
        try:
            path = Path(str(log_file)).expanduser()
            path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(str(path), encoding="utf-8")
            fh.setFormatter(fmt)
            log.addHandler(fh)
            log.info("── logging started (level=%s, file=%s) ──", level_str.upper(), path)
            return path
        except OSError as e:
            # Fall through to NullHandler so the script still runs
            sys.stderr.write(f"⚠ Could not open log file {log_file}: {e}\n")

    log.addHandler(logging.NullHandler())
    return None


def _load_dotenv(path=None, override=False):
    """Minimal .env loader — no external dependency.

    Looks for a `.env` file next to the script (or at the given path) and
    loads `KEY=VALUE` pairs into os.environ. By default, existing env vars
    take precedence (so shell exports override the file).

    Supports:
      - `KEY=value` and `KEY="value with spaces"`  (single or double quotes)
      - Lines starting with `#` are comments
      - `export KEY=value` (the `export ` prefix is stripped)
      - Blank lines
    """
    if path is None:
        path = Path(__file__).resolve().parent / ".env"
    else:
        path = Path(path)

    if not path.is_file():
        return False

    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].lstrip()
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.lstrip()
            # Quoted value: "..." or '...' — keep what's inside, discard rest
            if value and value[0] in ("'", '"'):
                q = value[0]
                end = value.find(q, 1)
                if end != -1:
                    value = value[1:end]
                else:
                    value = value[1:]
            else:
                # Strip inline comment: whitespace followed by `#`
                out = []
                for i, c in enumerate(value):
                    if c == "#" and (i == 0 or value[i - 1].isspace()):
                        break
                    out.append(c)
                value = "".join(out).rstrip()
            if not key:
                continue
            if override or key not in os.environ:
                os.environ[key] = value
        return True
    except OSError:
        return False


def get_config():
    """Load configuration from environment variables."""
    return {
        "token": os.environ.get("YNAB_TOKEN", ""),
        "budget_id": os.environ.get("YNAB_BUDGET_ID", ""),
        "account_id": os.environ.get("YNAB_ACCOUNT_ID", ""),
        "crypto_account_id": os.environ.get("YNAB_CRYPTO_ACCOUNT_ID", ""),
        "btc_xpub": os.environ.get("CRYPTO_BTC_XPUB", ""),
        "eth_address": os.environ.get("CRYPTO_ETH_ADDRESS", ""),
        "ibkr_base_url": os.environ.get("IBKR_BASE_URL", DEFAULT_IBKR_BASE_URL),
        "ibkr_account_id": os.environ.get("IBKR_ACCOUNT_ID", ""),
        "brokerage_account_id": os.environ.get("YNAB_BROKERAGE_ACCOUNT_ID", ""),
        "log_level": os.environ.get("LOG_LEVEL", "INFO"),
        "log_file": os.environ.get("LOG_FILE", str(DEFAULT_LOG_PATH)),
        "csv_dir": os.environ.get("CSV_DIR", str(DEFAULT_CSV_DIR)),
    }


# ─── Local SQLite database ──────────────────────────────────────────────────

def init_db(db_path=None):
    """Initialize the SQLite database for tracking imported transactions."""
    path = db_path or DEFAULT_DB_PATH
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL lets the web UI (separate threads) read concurrently while the
    # bot's main loop writes. busy_timeout retries lock contention rather
    # than instantly failing.
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
    except sqlite3.Error:
        pass

    # Main transactions table — stores both pushed (from Revolut CSV)
    # and pulled (from YNAB sync) transactions
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            import_id     TEXT PRIMARY KEY,
            date          TEXT NOT NULL,
            amount        INTEGER NOT NULL,
            payee_name    TEXT,
            memo          TEXT,
            cleared       TEXT,
            state         TEXT,
            ynab_tx_id    TEXT,
            account_id    TEXT,
            category_name TEXT,
            approved      INTEGER,
            deleted       INTEGER DEFAULT 0,
            source        TEXT DEFAULT 'revolut',
            imported_at   TEXT NOT NULL,
            updated_at    TEXT
        )
    """)

    # Sync metadata — tracks server_knowledge for delta syncing
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # Accounts table — every YNAB account in the user's budget. Synced
    # via sync_accounts_from_ynab. The classification column buckets
    # the YNAB-side `type` + `on_budget` into one of three buckets:
    # cash / credit / tracking — see `classify_account`.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            account_id           TEXT PRIMARY KEY,
            name                 TEXT,
            type                 TEXT,
            classification       TEXT,
            on_budget            INTEGER NOT NULL DEFAULT 1,
            closed               INTEGER NOT NULL DEFAULT 0,
            deleted              INTEGER NOT NULL DEFAULT 0,
            note                 TEXT,
            balance              INTEGER,
            cleared_balance      INTEGER,
            uncleared_balance    INTEGER,
            transfer_payee_id    TEXT,
            last_seen_at         TEXT,
            first_seen_at        TEXT NOT NULL,
            created_at           TEXT NOT NULL,
            updated_at           TEXT NOT NULL
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_accounts_classification "
        "ON accounts(classification)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_accounts_active "
        "ON accounts(closed, deleted)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_transactions_account "
        "ON transactions(account_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_transactions_date "
        "ON transactions(date)"
    )

    # Migrate: add columns if they don't exist yet (for users with older DB)
    _migrate_db(conn)

    conn.commit()
    return conn


def _migrate_db(conn):
    """Add new columns to existing databases without losing data."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()}
    migrations = {
        "account_id":    "TEXT",
        "category_name": "TEXT",
        "approved":      "INTEGER",
        "deleted":       "INTEGER DEFAULT 0",
        "source":        "TEXT DEFAULT 'revolut'",
    }
    for col, col_type in migrations.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE transactions ADD COLUMN {col} {col_type}")

    # One-time backfill: strip stale "(pending)" markers from cleared rows
    # in the local DB. Pre-1.2.7 db_upsert used COALESCE on `memo`, so
    # when a transaction transitioned PENDING → COMPLETED (and the parser
    # produced memo=NULL), the original "(pending)" memo was preserved in
    # the local DB. YNAB itself was patched correctly via PUT, but the
    # web UI reads memos from the local DB so users saw the stale string.
    #
    # This pass runs every startup but does no work on a clean DB
    # (LIKE filter narrows to the affected rows; cleaned rows no longer
    # match, so the next run is a no-op).
    stale = conn.execute(
        "SELECT import_id, memo FROM transactions "
        "WHERE cleared = 'cleared' AND memo LIKE '%(pending)%'"
    ).fetchall()
    if stale:
        now = datetime.now().isoformat()
        for iid, memo in stale:
            cleaned = _strip_pending_marker(memo) or None
            conn.execute(
                "UPDATE transactions SET memo = ?, updated_at = ? "
                "WHERE import_id = ?",
                (cleaned, now, iid),
            )
        log.info(
            "init_db: backfilled %d stale '(pending)' memos on cleared rows",
            len(stale),
        )


def db_get_server_knowledge(conn, account_id):
    """Get the last server_knowledge value for delta syncing."""
    row = conn.execute(
        "SELECT value FROM sync_meta WHERE key = ?",
        (f"server_knowledge:{account_id}",),
    ).fetchone()
    return int(row["value"]) if row else None


def db_set_server_knowledge(conn, account_id, knowledge):
    """Store the server_knowledge value after a sync."""
    conn.execute(
        "INSERT INTO sync_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (f"server_knowledge:{account_id}", str(knowledge)),
    )


def db_get_existing(conn, import_ids):
    """Look up which import_ids already exist in our local DB."""
    if not import_ids:
        return {}
    placeholders = ",".join("?" for _ in import_ids)
    rows = conn.execute(
        f"SELECT import_id, amount, cleared, state, ynab_tx_id, deleted FROM transactions WHERE import_id IN ({placeholders})",
        import_ids,
    ).fetchall()
    return {row["import_id"]: dict(row) for row in rows}


def db_upsert(conn, tx, ynab_tx_id=None, source="revolut"):
    """Insert or update a transaction in the local DB."""
    now = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO transactions (
            import_id, date, amount, payee_name, memo, cleared, state,
            ynab_tx_id, account_id, category_name, approved, deleted, source, imported_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(import_id) DO UPDATE SET
            amount = excluded.amount,
            payee_name = COALESCE(excluded.payee_name, transactions.payee_name),
            -- NOT COALESCE: the parser explicitly sets memo to NULL when
            -- a row clears (PENDING → COMPLETED produces an empty memo,
            -- which the parser stores as None). COALESCE here preserved
            -- the stale "(pending)" memo from the original PENDING import,
            -- so the web UI showed Memo="(pending)" while State="cleared"
            -- on every row that had ever been pending.
            memo = excluded.memo,
            cleared = excluded.cleared,
            state = excluded.state,
            ynab_tx_id = COALESCE(excluded.ynab_tx_id, transactions.ynab_tx_id),
            account_id = COALESCE(excluded.account_id, transactions.account_id),
            category_name = COALESCE(excluded.category_name, transactions.category_name),
            approved = COALESCE(excluded.approved, transactions.approved),
            deleted = excluded.deleted,
            source = CASE WHEN transactions.source = 'revolut' THEN 'revolut' ELSE excluded.source END,
            updated_at = ?
    """, (
        tx["import_id"], tx["date"], tx["amount"],
        tx.get("payee_name"), tx.get("memo"), tx.get("cleared"),
        tx.get("_state", ""),
        ynab_tx_id,
        tx.get("account_id"),
        tx.get("category_name"),
        1 if tx.get("approved") else 0,
        1 if tx.get("deleted") else 0,
        source,
        now, now,
    ))


def db_stats(conn):
    """Print database statistics."""
    total = conn.execute("SELECT COUNT(*) FROM transactions WHERE deleted = 0").fetchone()[0]
    cleared = conn.execute("SELECT COUNT(*) FROM transactions WHERE cleared = 'cleared' AND deleted = 0").fetchone()[0]
    uncleared = conn.execute("SELECT COUNT(*) FROM transactions WHERE cleared = 'uncleared' AND deleted = 0").fetchone()[0]
    latest = conn.execute("SELECT MAX(date) FROM transactions WHERE deleted = 0").fetchone()[0]
    earliest = conn.execute("SELECT MIN(date) FROM transactions WHERE deleted = 0").fetchone()[0]
    from_revolut = conn.execute("SELECT COUNT(*) FROM transactions WHERE source = 'revolut' AND deleted = 0").fetchone()[0]
    from_ynab = conn.execute("SELECT COUNT(*) FROM transactions WHERE source = 'ynab' AND deleted = 0").fetchone()[0]
    deleted = conn.execute("SELECT COUNT(*) FROM transactions WHERE deleted = 1").fetchone()[0]

    # Last sync info
    last_sync = conn.execute("SELECT value FROM sync_meta WHERE key LIKE 'server_knowledge:%'").fetchone()

    print(f"\n📊 Database: {DEFAULT_DB_PATH}")
    print(f"   Total transactions: {total}")
    print(f"   Cleared:  {cleared}")
    print(f"   Pending:  {uncleared}")
    print(f"   Deleted:  {deleted}")
    print(f"   Source — Revolut CSV: {from_revolut}  |  YNAB sync: {from_ynab}")
    if earliest and latest:
        print(f"   Date range: {earliest} → {latest}")
    if last_sync:
        print(f"   YNAB server knowledge: {last_sync['value']}")
    else:
        print(f"   YNAB sync: never (run --sync to pull from YNAB)")
    print()


# ─── YNAB API helpers ───────────────────────────────────────────────────────

def ynab_request(method, path, token, body=None):
    """Make a request to the YNAB API."""
    url = f"{YNAB_BASE_URL}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode() if body else None
    req = Request(url, data=data, headers=headers, method=method)

    try:
        with urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        try:
            error_body = e.read().decode()
        except Exception:
            error_body = ""
        print(f"  ✗ YNAB API error ({e.code}): {error_body}")
        # Raise instead of sys.exit so bot handlers can surface the failure
        # to users via Telegram instead of silently dying.
        raise RuntimeError(f"YNAB API {e.code}: {error_body[:200]}") from e
    except URLError as e:
        print(f"  ✗ YNAB network error: {e}")
        raise RuntimeError(f"YNAB network error: {e}") from e


def list_budgets(token):
    """List all YNAB budgets."""
    result = ynab_request("GET", "/budgets", token)
    budgets = result["data"]["budgets"]
    print(f"\nFound {len(budgets)} budget(s):\n")
    for b in budgets:
        print(f"  Name:  {b['name']}")
        print(f"  ID:    {b['id']}")
        currency = b.get("currency_format", {}).get("iso_code", "???")
        print(f"  Currency: {currency}")
        print()
    return budgets


def list_accounts(token, budget_id):
    """List all accounts in a YNAB budget."""
    result = ynab_request("GET", f"/budgets/{budget_id}/accounts", token)
    accounts = result["data"]["accounts"]
    print(f"\nFound {len(accounts)} account(s):\n")
    for a in accounts:
        if a.get("deleted") or a.get("closed"):
            continue
        balance = a["balance"] / 1000
        print(f"  Name:    {a['name']}")
        print(f"  ID:      {a['id']}")
        print(f"  Type:    {a['type']}")
        print(f"  Balance: {balance:,.2f}")
        print()
    return accounts


# ─── Accounts ──────────────────────────────────────────────────────────────

#: Map YNAB's raw `type` values onto our three-bucket classification.
#: Anything not in this dict (or any account with `on_budget=False`)
#: falls into 'tracking'.
_ACCOUNT_TYPE_TO_CLASSIFICATION = {
    # Budget — cash side
    "checking":     "cash",
    "savings":      "cash",
    "cash":         "cash",
    # Budget — credit side
    "creditCard":   "credit",
    "lineOfCredit": "credit",
    # Tracking — assets / liabilities (on_budget=False also forces this)
    "otherAsset":     "tracking",
    "otherLiability": "tracking",
    "mortgage":       "tracking",
    "autoLoan":       "tracking",
    "studentLoan":    "tracking",
    "personalLoan":   "tracking",
    "medicalDebt":    "tracking",
    "otherDebt":      "tracking",
}


def classify_account(yt):
    """Bucket a YNAB account dict into 'cash', 'credit', or 'tracking'."""
    if not yt.get("on_budget"):
        return "tracking"
    return _ACCOUNT_TYPE_TO_CLASSIFICATION.get(yt.get("type") or "", "tracking")


def sync_accounts_from_ynab(conn, token, budget_id):
    """Pull every account from YNAB and reconcile with the local table.

    Returns a dict summarising what changed:
        {"added": [name…], "renamed": [(old, new)…],
         "closed": [name…], "reopened": [name…],
         "deleted": [name…], "unchanged": int}

    The first time this runs against a fresh DB the entire account list
    is "added" — caller can detect that and decide whether to suppress
    notifications.
    """
    result = ynab_request("GET", f"/budgets/{budget_id}/accounts", token)
    accounts = result.get("data", {}).get("accounts", [])
    now = datetime.now().isoformat()

    pre_existing = {
        r["account_id"]: dict(r)
        for r in conn.execute(
            "SELECT account_id, name, classification, closed, deleted "
            "FROM accounts"
        ).fetchall()
    }
    seen_ids = set()
    added, renamed, closed, reopened, deleted = [], [], [], [], []
    unchanged = 0

    for ya in accounts:
        ya_id = ya.get("id")
        if not ya_id:
            continue
        seen_ids.add(ya_id)
        cls = classify_account(ya)
        is_closed = bool(ya.get("closed"))
        is_deleted = bool(ya.get("deleted"))
        new_name = ya.get("name") or ""

        prev = pre_existing.get(ya_id)
        if prev is None:
            added.append(new_name)
        else:
            if (prev["name"] or "") != new_name:
                renamed.append((prev["name"] or "", new_name))
            if not prev["closed"] and is_closed:
                closed.append(new_name)
            elif prev["closed"] and not is_closed:
                reopened.append(new_name)
            if not prev["deleted"] and is_deleted:
                deleted.append(new_name)
            if (prev["name"] == new_name and bool(prev["closed"]) == is_closed
                    and bool(prev["deleted"]) == is_deleted
                    and (prev["classification"] or "") == cls):
                unchanged += 1

        conn.execute("""
            INSERT INTO accounts (
                account_id, name, type, classification,
                on_budget, closed, deleted, note,
                balance, cleared_balance, uncleared_balance,
                transfer_payee_id, last_seen_at, first_seen_at,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
                name = excluded.name,
                type = excluded.type,
                classification = excluded.classification,
                on_budget = excluded.on_budget,
                closed = excluded.closed,
                deleted = excluded.deleted,
                note = excluded.note,
                balance = excluded.balance,
                cleared_balance = excluded.cleared_balance,
                uncleared_balance = excluded.uncleared_balance,
                transfer_payee_id = excluded.transfer_payee_id,
                last_seen_at = excluded.last_seen_at,
                updated_at = excluded.updated_at
        """, (
            ya_id, new_name, ya.get("type") or "", cls,
            int(bool(ya.get("on_budget"))),
            int(is_closed),
            int(is_deleted),
            ya.get("note") or "",
            ya.get("balance"), ya.get("cleared_balance"),
            ya.get("uncleared_balance"),
            ya.get("transfer_payee_id"),
            now, now, now, now,
        ))

    # Accounts that were in our local DB but NOT in this response are
    # implicitly deleted on YNAB's side. YNAB usually returns them with
    # deleted=true so we already handled that above; this is a fallback.
    for prev_id, prev in pre_existing.items():
        if prev_id in seen_ids:
            continue
        if not prev["deleted"]:
            deleted.append(prev["name"] or "")
            conn.execute(
                "UPDATE accounts SET deleted = 1, updated_at = ? "
                "WHERE account_id = ?",
                (now, prev_id),
            )

    conn.commit()
    summary = {
        "added": added, "renamed": renamed,
        "closed": closed, "reopened": reopened,
        "deleted": deleted, "unchanged": unchanged,
    }
    if any(summary[k] for k in ("added", "renamed", "closed",
                                 "reopened", "deleted")):
        log.info("accounts sync: %s", summary)
    return summary


def sync_budget_transactions(conn, token, budget_id, primary_account_id=None):
    """Pull transactions from EVERY account in the budget in one call.

    Uses YNAB's budget-wide /transactions endpoint with delta syncing
    (separate ``server_knowledge`` slot so it doesn't fight the
    per-account ``sync_from_ynab``). Local rows for non-primary
    accounts get a synthesized import_id of the form
    ``acct:<account_id>:<ynab_tx_id>`` so they can never collide with
    primary-account YNAB:milli:date:occ rows.
    """
    knowledge_key = f"budget:{budget_id}"
    knowledge = db_get_server_knowledge(conn, knowledge_key)
    path = f"/budgets/{budget_id}/transactions"
    if knowledge is not None:
        path += f"?last_knowledge_of_server={knowledge}"

    result = ynab_request("GET", path, token)
    data = result.get("data", {})
    txns = data.get("transactions", [])
    new_knowledge = data.get("server_knowledge")

    created = updated = deleted = 0
    for yt in txns:
        ya_id = yt.get("account_id") or ""
        ynab_iid = yt.get("import_id")
        if primary_account_id and ya_id == primary_account_id:
            # Preserve existing import_id format for compatibility with
            # the CSV-import / dedupe flow.
            local_iid = ynab_iid or f"ynab_manual:{yt['id']}"
            source = "revolut" if (ynab_iid or "").startswith("YNAB:") else "ynab"
        else:
            local_iid = f"acct:{ya_id}:{yt['id']}"
            source = "ynab"

        tx = {
            "import_id":     local_iid,
            "date":          yt.get("date", ""),
            "amount":        yt.get("amount", 0),
            "payee_name":    yt.get("payee_name") or "",
            "memo":          yt.get("memo") or "",
            "cleared":       yt.get("cleared", "uncleared"),
            "approved":      yt.get("approved", False),
            "deleted":       yt.get("deleted", False),
            "account_id":    ya_id,
            "category_name": yt.get("category_name") or "",
            "_state": "COMPLETED" if yt.get("cleared") in ("cleared", "reconciled") else "PENDING",
        }

        if tx["deleted"]:
            deleted += 1
        elif conn.execute(
            "SELECT 1 FROM transactions WHERE import_id = ?", (local_iid,)
        ).fetchone():
            updated += 1
        else:
            created += 1
        db_upsert(conn, tx, ynab_tx_id=yt["id"], source=source)

    if new_knowledge is not None:
        db_set_server_knowledge(conn, knowledge_key, new_knowledge)
    conn.commit()
    if created or updated or deleted:
        log.info(
            "sync_budget_transactions: created=%d updated=%d deleted=%d",
            created, updated, deleted,
        )
    return {"created": created, "updated": updated, "deleted": deleted}


# ─── YNAB sync (pull transactions down) ─────────────────────────────────────

def sync_from_ynab(conn, token, budget_id, account_id, since_date=None):
    """
    Pull transactions from YNAB into the local database.
    Uses delta syncing via server_knowledge so subsequent syncs
    only fetch what changed.
    """
    # Build query path
    path = f"/budgets/{budget_id}/accounts/{account_id}/transactions"
    params = []
    if since_date:
        params.append(f"since_date={since_date}")

    # Use stored server_knowledge for delta sync
    knowledge = db_get_server_knowledge(conn, account_id)
    if knowledge is not None:
        params.append(f"last_knowledge_of_server={knowledge}")
        print(f"  ↻ Delta sync (server_knowledge: {knowledge})")
    else:
        print(f"  ↓ Full sync (first time)")

    if params:
        path += "?" + "&".join(params)

    result = ynab_request("GET", path, token)
    data = result.get("data", {})
    ynab_transactions = data.get("transactions", [])
    new_knowledge = data.get("server_knowledge")

    created = 0
    updated = 0
    deleted = 0

    for yt in ynab_transactions:
        # Build a normalized transaction dict
        import_id = yt.get("import_id")
        if not import_id:
            # Transactions entered manually in YNAB don't have an import_id.
            # Use the YNAB transaction ID as a synthetic import_id.
            import_id = f"ynab_manual:{yt['id']}"

        tx = {
            "import_id": import_id,
            "date": yt.get("date", ""),
            "amount": yt.get("amount", 0),
            "payee_name": yt.get("payee_name") or "",
            "memo": yt.get("memo") or "",
            "cleared": yt.get("cleared", "uncleared"),
            "approved": yt.get("approved", False),
            "deleted": yt.get("deleted", False),
            "account_id": yt.get("account_id", account_id),
            "category_name": yt.get("category_name") or "",
            "_state": "COMPLETED" if yt.get("cleared") == "cleared" else "PENDING",
        }

        # Check if this is new or updated
        existing = conn.execute(
            "SELECT import_id FROM transactions WHERE import_id = ?",
            (import_id,),
        ).fetchone()

        if tx["deleted"]:
            deleted += 1
        elif existing:
            updated += 1
        else:
            created += 1

        source = "revolut" if import_id.startswith("YNAB:") else "ynab"
        db_upsert(conn, tx, ynab_tx_id=yt["id"], source=source)

    # Store the new server_knowledge for next delta sync
    if new_knowledge is not None:
        db_set_server_knowledge(conn, account_id, new_knowledge)

    conn.commit()

    print(f"\n  📋 Sync results:")
    print(f"     Fetched from YNAB: {len(ynab_transactions)} transaction(s)")
    print(f"     New to local DB:   {created}")
    print(f"     Updated:           {updated}")
    print(f"     Deleted:           {deleted}")
    if new_knowledge is not None:
        print(f"     Server knowledge:  {new_knowledge}")

    return len(ynab_transactions)


# ─── Revolut CSV parsing ────────────────────────────────────────────────────

_FILENAME_DATE_RANGE_RE = re.compile(
    r"account-statement_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})", re.IGNORECASE,
)


def parse_csv_date_range(filepath):
    """Return (start_iso, end_iso) covered by the CSV.

    Tries the Revolut filename pattern
    ``account-statement_YYYY-MM-DD_YYYY-MM-DD_*.csv`` first; falls back to
    scanning the parsed transactions for the min/max date. Returns
    ``(None, None)`` only if both methods fail.
    """
    name = Path(filepath).name
    m = _FILENAME_DATE_RANGE_RE.search(name)
    if m:
        return m.group(1), m.group(2)

    try:
        txs = parse_revolut_csv(filepath)
    except Exception:
        return None, None
    if not txs:
        return None, None
    dates = sorted(t["date"] for t in txs)
    return dates[0], dates[-1]


#: Revolut "Product" column values we treat as the main checking account.
#: Anything else (Savings pockets, Vault, Crypto, Wealth) is skipped — those
#: are tracked separately in YNAB (or not at all). Without this filter,
#: e.g. a closed Savings pocket's `0.00` closing-transaction row at the
#: bottom of the CSV is the "latest" row reconcile reads, and the bot
#: pushes YNAB to 0.
TRACKED_PRODUCTS = frozenset(("Current",))


def parse_revolut_csv(filepath):
    """Parse a Revolut account statement CSV into a list of transaction dicts.

    Only rows whose ``Product`` column is in :data:`TRACKED_PRODUCTS` are
    kept. Other products (Savings pockets, Crypto, Wealth, …) are
    silently dropped — they're either tracked elsewhere in YNAB or
    intentionally not tracked at all.
    """
    transactions = []
    occurrence_counter = {}  # key: "amount:date" → count
    skipped_products = {}    # for the log line at the end

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            product = (row.get("Product") or "").strip()
            if product and product not in TRACKED_PRODUCTS:
                skipped_products[product] = skipped_products.get(product, 0) + 1
                continue

            date_str = row.get("Started Date", "").strip()
            if not date_str:
                continue

            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    print(f"  ⚠ Skipping row with unparseable date: {date_str}")
                    continue

            iso_date = dt.strftime("%Y-%m-%d")

            try:
                amount = float(row.get("Amount", "0"))
            except ValueError:
                print(f"  ⚠ Skipping row with invalid amount: {row.get('Amount')}")
                continue

            try:
                fee_val = float(row.get("Fee", "0") or "0")
            except ValueError:
                fee_val = 0.0

            if fee_val:
                amount -= fee_val

            milliunit_amount = int(round(amount * 1000))

            payee = row.get("Description", "").strip()
            if not payee:
                payee = row.get("Type", "Unknown")

            tx_type = row.get("Type", "").strip()
            state = row.get("State", "").strip()

            memo_parts = []
            if tx_type and tx_type != "Card Payment":
                memo_parts.append(tx_type)
            if state == "PENDING":
                memo_parts.append("(pending)")
            if fee_val:
                memo_parts.append(f"Fee: {fee_val}")
            memo = " | ".join(memo_parts) if memo_parts else ""

            # YNAB-native import_id format for deduplication
            occurrence_key = f"{milliunit_amount}:{iso_date}"
            occurrence_counter[occurrence_key] = occurrence_counter.get(occurrence_key, 0) + 1
            occurrence = occurrence_counter[occurrence_key]
            import_id = f"YNAB:{milliunit_amount}:{iso_date}:{occurrence}"

            cleared = "cleared" if state == "COMPLETED" else "uncleared"

            transactions.append({
                "date": iso_date,
                "amount": milliunit_amount,
                "payee_name": payee[:100],
                "memo": memo[:200] if memo else None,
                "cleared": cleared,
                "approved": True,
                "import_id": import_id,
                "_state": state,  # keep original state for DB tracking
                # Start time of day — used to detect rows whose date shifted
                # between exports (timezone re-rendering near midnight).
                "_started_time": dt.strftime("%H:%M:%S"),
            })

    if skipped_products:
        details = ", ".join(f"{k}={v}" for k, v in sorted(skipped_products.items()))
        log.info(
            "parse_revolut_csv: skipped non-Current rows by Product (%s)",
            details,
        )

    return transactions


# ─── Diff & import ───────────────────────────────────────────────────────────

def _find_date_shifted_original(conn, tx, csv_import_ids, consumed):
    """Find a local row that is this CSV row under a shifted date.

    Revolut occasionally re-renders a row's Started Date between exports
    (timezone change near midnight), which changes our amount+date-based
    import_id and would create a YNAB duplicate. A row qualifies as the
    same transaction when ALL of:
      - the CSV row started within 3h of midnight (only those can shift),
      - a local Revolut-sourced row exists exactly ±1 day away with the
        same amount AND same payee, still linked to a YNAB transaction,
      - that local row's import_id is absent from the current CSV
        (its old date no longer exists in the export).
    Returns the local row dict or None.
    """
    started = tx.get("_started_time") or ""
    try:
        hour = int(started[:2])
    except ValueError:
        return None
    if 3 <= hour < 21:
        return None

    try:
        day = datetime.strptime(tx["date"], "%Y-%m-%d")
    except ValueError:
        return None

    for delta in (-1, 1):
        neighbor = (day + timedelta(days=delta)).strftime("%Y-%m-%d")
        row = conn.execute(
            "SELECT import_id, amount, cleared, ynab_tx_id FROM transactions "
            "WHERE amount = ? AND payee_name = ? AND date = ? "
            "AND source = 'revolut' AND deleted = 0 AND ynab_tx_id IS NOT NULL",
            (tx["amount"], tx["payee_name"], neighbor),
        ).fetchone()
        if (
            row
            and row["import_id"] not in csv_import_ids
            and row["import_id"] not in consumed
        ):
            return dict(row)
    return None


def diff_transactions(conn, transactions):
    """
    Compare parsed transactions against the local DB.
    Returns (to_create, to_update, to_delete, skipped) where:
      - to_create: transactions not in the DB (genuinely new)
      - to_update: transactions whose state changed (e.g. pending → completed,
                   or amount changed after settlement)
      - to_delete: previously imported transactions Revolut now reports as
                   REVERTED — their YNAB copy should be removed
    """
    import_ids = [tx["import_id"] for tx in transactions]
    existing = db_get_existing(conn, import_ids)
    csv_import_ids = set(import_ids)
    shift_consumed = set()

    to_create = []
    to_update = []
    to_delete = []
    skipped = 0

    for tx in transactions:
        iid = tx["import_id"]
        reverted = tx.get("_state") == "REVERTED"

        if iid not in existing:
            if reverted:
                # A failed/cancelled authorization we never imported —
                # it must not reach YNAB at all.
                skipped += 1
                continue
            shifted = _find_date_shifted_original(
                conn, tx, csv_import_ids, shift_consumed,
            )
            if shifted:
                # Same transaction, re-dated by Revolut between exports:
                # update the existing YNAB row and re-key the local record
                # instead of creating a duplicate.
                shift_consumed.add(shifted["import_id"])
                tx["_ynab_tx_id"] = shifted["ynab_tx_id"]
                tx["_rekey_from"] = shifted["import_id"]
                to_update.append(tx)
            else:
                to_create.append(tx)
            continue

        old = existing[iid]
        if reverted:
            # Imported earlier (usually while PENDING), since reverted by
            # Revolut: delete the YNAB copy. Rows already unlinked or
            # already marked deleted need no further action.
            if old.get("ynab_tx_id") and not old.get("deleted"):
                tx["_ynab_tx_id"] = old["ynab_tx_id"]
                tx["_delete_reason"] = "reverted"
                to_delete.append(tx)
            else:
                skipped += 1
            continue

        # Check if anything meaningful changed
        amount_changed = old["amount"] != tx["amount"]
        state_changed = old["cleared"] != tx["cleared"]

        if amount_changed or state_changed:
            tx["_ynab_tx_id"] = old.get("ynab_tx_id")
            if amount_changed:
                # Carried along so the import can report old -> new
                tx["_old_amount"] = old["amount"]
            to_update.append(tx)
        else:
            skipped += 1

    # ── Orphaned pendings ──
    # A pending we imported earlier can vanish from the export even though
    # its date is still inside the export's window: Revolut settled it
    # under DIFFERENT rows (restaurant bill + tip split into two, FX
    # re-quote changing the amount, one auth splitting into several
    # settlements). The amount+date import_id no longer matches anything,
    # so the YNAB copy would linger uncleared forever. Queue it for
    # removal — the settled replacement rows arrive as new transactions
    # in the same import.
    if transactions:
        window_lo = min(t["date"] for t in transactions)
        window_hi = max(t["date"] for t in transactions)
        orphans = conn.execute(
            "SELECT import_id, date, amount, payee_name, ynab_tx_id "
            "FROM transactions "
            "WHERE cleared = 'uncleared' AND source = 'revolut' "
            "AND deleted = 0 AND ynab_tx_id IS NOT NULL "
            "AND date BETWEEN ? AND ?",
            (window_lo, window_hi),
        ).fetchall()
        for row in orphans:
            if row["import_id"] in csv_import_ids or row["import_id"] in shift_consumed:
                continue
            to_delete.append({
                "date": row["date"],
                "amount": row["amount"],
                "payee_name": row["payee_name"] or "",
                "import_id": row["import_id"],
                "_ynab_tx_id": row["ynab_tx_id"],
                "_delete_reason": "superseded",
            })

    return to_create, to_update, to_delete, skipped


def _fetch_ynab_txns_by_import_id(token, budget_id, account_id, import_ids, since_date=None):
    """Return {import_id: ynab_tx_dict} for any import_ids present on YNAB.

    Fetches the account's transactions (optionally since `since_date` as YYYY-MM-DD)
    and filters to the requested import_ids. Used to recover YNAB tx IDs after a
    fresh local DB so we can patch existing rows instead of losing state.
    """
    path = f"/budgets/{budget_id}/accounts/{account_id}/transactions"
    if since_date:
        path += f"?since_date={since_date}"
    result = ynab_request("GET", path, token)
    txns = result.get("data", {}).get("transactions", [])
    wanted = set(import_ids)
    return {t["import_id"]: t for t in txns if t.get("import_id") in wanted}


def _strip_pending_marker(memo):
    """Remove any '(pending)' substring (and surrounding ' | ' separators) from memo."""
    if not memo:
        return memo
    # Drop '(pending)' with any adjacent pipe separators produced by parse_revolut_csv
    cleaned = re.sub(r"\s*\|\s*\(pending\)|\(pending\)\s*\|\s*|\(pending\)", "", memo)
    return cleaned.strip(" |")


def cleanup_pending_memos(token, budget_id, account_id, csv_path=None, dry_run=False):
    """Strip stale '(pending)' memos from YNAB transactions.

    Any YNAB row whose memo contains '(pending)' is considered stale — the
    importer adds that marker on first import and relies on a subsequent CSV
    to flip it off. After a local-DB wipe (e.g. VM redeploy) that subsequent
    flip never happens for rows that YNAB's import_id dedup rejects.

    If `csv_path` is provided, the latest CSV is used to also flip each tx's
    `cleared` state when Revolut now reports it as COMPLETED. Rows the CSV
    confirms are still PENDING are left untouched. Rows not present in the
    CSV (e.g. ancient / out-of-window) have their memo stripped but cleared
    state left alone.
    """
    print(f"\n🧹 Scanning account for stale '(pending)' memos...")

    # Optional CSV cross-reference: build import_id → (cleared, pending_flag) map
    csv_state = {}
    if csv_path:
        try:
            csv_txs = parse_revolut_csv(str(csv_path))
            for tx in csv_txs:
                csv_state[tx["import_id"]] = {
                    "cleared": tx["cleared"],
                    "is_pending": tx["_state"] == "PENDING",
                    "memo": tx.get("memo") or "",
                    "amount": tx["amount"],
                }
            print(f"   Cross-referencing against {len(csv_state)} row(s) from {Path(csv_path).name}.")
        except Exception as e:
            print(f"   ⚠ Could not parse CSV for cross-reference: {e}")
            csv_state = {}

    path = f"/budgets/{budget_id}/accounts/{account_id}/transactions"
    result = ynab_request("GET", path, token)
    txns = result.get("data", {}).get("transactions", [])
    print(f"   Fetched {len(txns)} transaction(s) from YNAB.")

    stale = [
        t for t in txns
        if not t.get("deleted") and "(pending)" in (t.get("memo") or "")
    ]

    if not stale:
        print(f"   ✓ No stale '(pending)' memos — nothing to do.")
        return 0

    # Skip rows the CSV confirms are still PENDING (don't strip their marker)
    actionable = []
    still_pending = 0
    for t in stale:
        iid = t.get("import_id")
        if iid and iid in csv_state and csv_state[iid]["is_pending"]:
            still_pending += 1
            continue
        actionable.append(t)

    print(f"   Found {len(stale)} stale entr{'y' if len(stale) == 1 else 'ies'}; "
          f"{len(actionable)} actionable, {still_pending} still pending per CSV.")

    if not actionable:
        return 0

    if dry_run:
        print(f"\n   Would patch (dry-run):")
        for t in actionable[:20]:
            amt = t["amount"] / 1000
            iid = t.get("import_id")
            csv_row = csv_state.get(iid) if iid else None
            flip = " → cleared" if csv_row and csv_row["cleared"] == "cleared" and t.get("cleared") != "cleared" else ""
            print(f"    ↻ {t['date']}  {amt:>10.2f}  {t.get('payee_name', '')}{flip}")
        if len(actionable) > 20:
            print(f"    …and {len(actionable) - 20} more.")
        return len(actionable)

    patched = 0
    flipped = 0
    for t in actionable:
        new_memo = _strip_pending_marker(t.get("memo") or "")
        update = {"memo": new_memo or None}

        # If the CSV confirms this row has cleared on Revolut, flip cleared too
        iid = t.get("import_id")
        csv_row = csv_state.get(iid) if iid else None
        if csv_row and csv_row["cleared"] == "cleared" and t.get("cleared") != "cleared":
            update["cleared"] = "cleared"
            flipped += 1

        try:
            ynab_request(
                "PATCH",
                f"/budgets/{budget_id}/transactions/{t['id']}",
                token, {"transaction": update},
            )
            patched += 1
            log.info(
                "cleanup tx   id=%s memo: %r → %r cleared: %s → %s",
                t["id"], t.get("memo"), new_memo,
                t.get("cleared"), update.get("cleared", t.get("cleared")),
            )
        except Exception as e:
            print(f"    ⚠ Failed to patch {t['id']}: {e}")

    print(f"   ✓ Cleaned {patched} memo(s){f', flipped {flipped} to cleared' if flipped else ''}.")
    return patched


def find_orphaned_imports(token, budget_id, account_id, csv_path):
    """Find YNAB transactions in the CSV's date range that have no matching CSV row.

    These are typically stale imports from an earlier run where the CSV amount
    later changed (so the regenerated import_id no longer matches). We restrict
    to transactions whose ``import_id`` starts with ``YNAB:`` (i.e. created by
    this importer) so we don't flag manually-entered or other-source rows.

    Returns a list of YNAB transaction dicts (id, date, amount, payee_name,
    memo, import_id) that look like orphaned duplicates.
    """
    start_date, end_date = parse_csv_date_range(csv_path)
    if not start_date or not end_date:
        raise RuntimeError(
            "Could not determine the CSV's date range from filename or contents."
        )

    csv_txs = parse_revolut_csv(csv_path)
    csv_import_ids = {tx["import_id"] for tx in csv_txs}

    path = (
        f"/budgets/{budget_id}/accounts/{account_id}/transactions"
        f"?since_date={start_date}"
    )
    result = ynab_request("GET", path, token)
    ynab_txns = result.get("data", {}).get("transactions", [])

    orphans = []
    for t in ynab_txns:
        if t.get("deleted"):
            continue
        date = t.get("date") or ""
        if date < start_date or date > end_date:
            continue
        iid = t.get("import_id") or ""
        if not iid.startswith("YNAB:"):
            # Only touch rows we know we created
            continue
        if iid in csv_import_ids:
            continue
        orphans.append({
            "id": t.get("id"),
            "date": date,
            "amount": t.get("amount", 0),
            "payee_name": t.get("payee_name") or "",
            "memo": t.get("memo") or "",
            "import_id": iid,
            "cleared": t.get("cleared"),
        })

    orphans.sort(key=lambda x: (x["date"], x["payee_name"]))
    return {
        "start_date": start_date,
        "end_date": end_date,
        "csv_count": len(csv_txs),
        "ynab_count_in_range": sum(
            1 for t in ynab_txns
            if not t.get("deleted")
            and start_date <= (t.get("date") or "") <= end_date
        ),
        "orphans": orphans,
    }


def delete_ynab_transaction(conn, token, budget_id, ynab_tx_id):
    """Delete a YNAB transaction and remove its local DB row.

    ``conn`` may be ``None`` to skip local DB cleanup. Raises on API failure.
    """
    ynab_request("DELETE", f"/budgets/{budget_id}/transactions/{ynab_tx_id}", token)
    if conn is not None:
        try:
            conn.execute("DELETE FROM transactions WHERE ynab_tx_id = ?", (ynab_tx_id,))
            conn.commit()
        except sqlite3.Error as e:
            log.warning("dedupe: could not remove local row for %s: %s", ynab_tx_id, e)


def import_and_track(conn, token, budget_id, account_id, transactions, dry_run=False):
    """
    Diff transactions against the local DB, then create/update only what's needed.
    """
    to_create, to_update, to_delete, skipped = diff_transactions(conn, transactions)

    log.info(
        "import diff: new=%d updated=%d remove=%d skipped=%d (account=%s)",
        len(to_create), len(to_update), len(to_delete), skipped, account_id,
    )

    print(f"\n  📋 Summary:")
    print(f"     New transactions:     {len(to_create)}")
    print(f"     Updated (state/amt):  {len(to_update)}")
    if to_delete:
        print(f"     Stale (to remove):    {len(to_delete)}")
    print(f"     Already imported:     {skipped}")

    if not to_create and not to_update and not to_delete:
        print("  ✓ Nothing to do — everything is already up to date.")
        return

    if dry_run:
        if to_create:
            print(f"\n  ── Would create {len(to_create)} new transaction(s): ──\n")
            for tx in to_create:
                amt = tx["amount"] / 1000
                status = "✓" if tx["cleared"] == "cleared" else "◌"
                print(f"    {status} {tx['date']}  {amt:>10.2f}  {tx['payee_name']}")
        if to_update:
            print(f"\n  ── Would update {len(to_update)} transaction(s): ──\n")
            for tx in to_update:
                amt = tx["amount"] / 1000
                note = f"  (re-dated from {tx['_rekey_from'].split(':')[2]})" if tx.get("_rekey_from") else ""
                print(f"    ↻ {tx['date']}  {amt:>10.2f}  {tx['payee_name']}  → {tx['cleared']}{note}")
        if to_delete:
            print(f"\n  ── Would remove {len(to_delete)} stale transaction(s) from YNAB: ──\n")
            for tx in to_delete:
                amt = tx["amount"] / 1000
                reason = tx.get("_delete_reason", "reverted")
                print(f"    ✗ {tx['date']}  {amt:>10.2f}  {tx['payee_name']}  ({reason})")
        return

    # ── Create new transactions ──
    if to_create:
        # Prepare API payload (strip internal fields)
        api_txs = []
        for tx in to_create:
            api_tx = {k: v for k, v in tx.items() if not k.startswith("_")}
            api_tx["account_id"] = account_id
            api_txs.append(api_tx)

        print(f"\n  → Creating {len(api_txs)} new transaction(s) in YNAB...")
        body = {"transactions": api_txs}
        result = ynab_request("POST", f"/budgets/{budget_id}/transactions", token, body)

        data = result.get("data", {})
        created_ids = data.get("transaction_ids", [])
        duplicates = data.get("duplicate_import_ids", [])

        print(f"    ✓ Created:    {len(created_ids)}")
        if duplicates:
            print(f"    ⊘ Duplicates: {len(duplicates)} (YNAB-side dedup)")

        # Store in local DB — map YNAB tx IDs back to our transactions
        created_txs = data.get("transactions", [])
        ynab_id_map = {t.get("import_id"): t.get("id") for t in created_txs}

        for tx in to_create:
            ynab_tx_id = ynab_id_map.get(tx["import_id"])
            db_upsert(conn, tx, ynab_tx_id)
            action = "duplicate" if tx["import_id"] in duplicates else "created"
            log.info(
                "tx %-9s date=%s amount=%+.2f payee=%s state=%s import_id=%s ynab_id=%s",
                action, tx["date"], tx["amount"] / 1000,
                tx.get("payee_name", ""), tx["cleared"],
                tx["import_id"], ynab_tx_id or "-",
            )
        # Commit before duplicate reconciliation: YNAB already accepted
        # these rows, so the local id-mapping must survive even if the
        # reconciliation below fails. An uncommitted transaction here
        # would also hold the write lock across its network calls.
        conn.commit()

        # ── Duplicate reconciliation ──
        # YNAB's import_id dedup skips the POST when an import_id already exists,
        # but it does NOT update the existing row — so if our CSV now has a
        # different cleared state / memo (e.g. a previously-pending row that
        # has cleared), the old record stays stale. Fetch the existing rows,
        # PATCH anything that drifted, and record their YNAB tx IDs locally so
        # the normal diff path handles them on future imports.
        if duplicates:
            dup_txs = [tx for tx in to_create if tx["import_id"] in duplicates]
            # Fetch only the window we care about to keep the GET cheap
            earliest = min(tx["date"] for tx in dup_txs)
            try:
                existing_map = _fetch_ynab_txns_by_import_id(
                    token, budget_id, account_id, duplicates, since_date=earliest,
                )
            except Exception as e:
                print(f"    ⚠ Could not fetch existing duplicates for patch: {e}")
                existing_map = {}

            dup_patched = 0
            for tx in dup_txs:
                existing = existing_map.get(tx["import_id"])
                if not existing:
                    db_upsert(conn, tx)
                    continue

                ynab_tx_id = existing.get("id")
                incoming_memo = tx.get("memo") or ""
                existing_memo = existing.get("memo") or ""
                drift = (
                    existing.get("cleared", "uncleared") != tx["cleared"]
                    or existing.get("amount", 0) != tx["amount"]
                    or existing_memo != incoming_memo
                )
                if drift and ynab_tx_id:
                    update_body = {
                        "transaction": {
                            "amount": tx["amount"],
                            "cleared": tx["cleared"],
                            "memo": incoming_memo or None,
                        }
                    }
                    try:
                        ynab_request(
                            "PATCH",
                            f"/budgets/{budget_id}/transactions/{ynab_tx_id}",
                            token, update_body,
                        )
                        dup_patched += 1
                        if existing.get("amount", 0) != tx["amount"]:
                            print(
                                f"    Δ {tx['date']}  {tx['payee_name']} — amount "
                                f"{existing.get('amount', 0)/1000:+.2f} → {tx['amount']/1000:+.2f}"
                            )
                        log.info(
                            "tx drift-fix date=%s amount=%+.2f payee=%s "
                            "cleared=%s→%s memo=%r→%r import_id=%s ynab_id=%s",
                            tx["date"], tx["amount"] / 1000,
                            tx.get("payee_name", ""),
                            existing.get("cleared", "uncleared"), tx["cleared"],
                            existing_memo, incoming_memo,
                            tx["import_id"], ynab_tx_id,
                        )
                    except Exception as e:
                        print(f"    ⚠ Failed to patch {ynab_tx_id}: {e}")

                # Record locally either way so future imports have the ynab_tx_id
                db_upsert(conn, tx, ynab_tx_id=ynab_tx_id)
                # Commit per row — the PATCHes above mean this loop can
                # hold the write lock across many network round-trips.
                conn.commit()

            if dup_patched:
                print(f"    ↻ Patched:   {dup_patched} drifted (pending→cleared / memo / amount)")

        conn.commit()

    # ── Update changed transactions ──
    if to_update:
        print(f"\n  → Updating {len(to_update)} transaction(s) in YNAB...")
        updated_count = 0

        for tx in to_update:
            ynab_tx_id = tx.get("_ynab_tx_id")
            if not ynab_tx_id:
                # No stored YNAB ID — try to create (YNAB will dedup by import_id
                # if it exists, or create new if not)
                api_tx = {k: v for k, v in tx.items() if not k.startswith("_")}
                api_tx["account_id"] = account_id
                body = {"transactions": [api_tx]}
                result = ynab_request("POST", f"/budgets/{budget_id}/transactions", token, body)
                data = result.get("data", {})
                created_txs = data.get("transactions", [])
                if created_txs:
                    ynab_tx_id = created_txs[0].get("id")
            else:
                # We have the YNAB transaction ID — use PATCH to update
                update_body = {
                    "transaction": {
                        "amount": tx["amount"],
                        "cleared": tx["cleared"],
                        "memo": tx.get("memo"),
                    }
                }
                if tx.get("_rekey_from"):
                    # Date-shifted duplicate: move the YNAB row to the
                    # re-dated CSV date instead of creating a copy.
                    update_body["transaction"]["date"] = tx["date"]
                result = ynab_request(
                    "PATCH",
                    f"/budgets/{budget_id}/transactions/{ynab_tx_id}",
                    token,
                    update_body,
                )
                updated_count += 1

            if tx.get("_rekey_from"):
                # Replace the old-dated local record; db_upsert below
                # re-inserts the same transaction under its new import_id.
                conn.execute(
                    "DELETE FROM transactions WHERE import_id = ?",
                    (tx["_rekey_from"],),
                )
                log.info(
                    "tx re-dated  %s -> %s (ynab_id=%s kept, no duplicate created)",
                    tx["_rekey_from"], tx["import_id"], ynab_tx_id,
                )
            changes = []
            if tx.get("_old_amount") is not None and tx["_old_amount"] != tx["amount"]:
                changes.append(
                    f"amount {tx['_old_amount']/1000:+.2f} → {tx['amount']/1000:+.2f}"
                )
            if tx.get("_rekey_from"):
                old_date = tx["_rekey_from"].split(":")[2]
                if old_date != tx["date"]:
                    changes.append(f"date {old_date} → {tx['date']}")
            if changes:
                # "Δ" lines are picked up by the bot and surfaced in the
                # Telegram import summary.
                print(f"    Δ {tx['date']}  {tx['payee_name']} — {', '.join(changes)}")
            db_upsert(conn, tx, ynab_tx_id)
            # Per-row commit: the YNAB-side write above already happened,
            # so record it durably now. Committing only after the loop
            # meant a mid-loop API error (e.g. PATCH on a transaction
            # deleted in YNAB → 404) left an open write transaction that
            # locked the DB for every subsequent import.
            conn.commit()
            log.info(
                "tx updated   date=%s amount=%+.2f payee=%s state=%s import_id=%s ynab_id=%s",
                tx["date"], tx["amount"] / 1000,
                tx.get("payee_name", ""), tx["cleared"],
                tx["import_id"], ynab_tx_id or "-",
            )

        print(f"    ✓ Updated: {updated_count}")

    # ── Remove reverted / superseded transactions ──
    if to_delete:
        print(f"\n  → Removing {len(to_delete)} stale transaction(s) from YNAB...")
        removed = 0
        for tx in to_delete:
            ynab_tx_id = tx["_ynab_tx_id"]
            reason = tx.get("_delete_reason", "reverted")
            try:
                ynab_request(
                    "DELETE",
                    f"/budgets/{budget_id}/transactions/{ynab_tx_id}",
                    token,
                )
                removed += 1
            except RuntimeError as e:
                # Already gone in YNAB (e.g. deleted by hand) — just
                # record that locally so we stop retrying.
                if "YNAB API 404" not in str(e):
                    raise
            conn.execute(
                "UPDATE transactions SET deleted = 1, state = ?, "
                "updated_at = ? WHERE import_id = ?",
                (reason.upper(), datetime.now().isoformat(), tx["import_id"]),
            )
            conn.commit()
            label = ("pending superseded by settled rows"
                     if reason == "superseded" else "reverted")
            # "✗" lines are picked up by the bot and surfaced in the
            # Telegram import summary.
            print(f"    ✗ {tx['date']}  {tx['amount']/1000:+.2f}  "
                  f"{tx.get('payee_name', '')} — {label}")
            log.info(
                "tx removed   date=%s amount=%+.2f payee=%s import_id=%s ynab_id=%s (%s)",
                tx["date"], tx["amount"] / 1000,
                tx.get("payee_name", ""), tx["import_id"], ynab_tx_id, reason,
            )

        print(f"    ✓ Removed: {removed}")


# ─── Auto-detect the latest Revolut export ─────────────────────────────────
#
# Revolut exports are named:
#   account-statement_YYYY-MM-DD_YYYY-MM-DD_xx-xx_XXXXXX.csv
# where the second date is the export's "to" date (typically today).
# We pick the file with the newest "to" date; ties are broken by mtime.
# ───────────────────────────────────────────────────────────────────────────

_REVOLUT_FILENAME_RE = re.compile(
    r"^account-statement_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_.*\.csv$",
    re.IGNORECASE,
)


def find_latest_revolut_csv(directory):
    """Return the path to the newest Revolut CSV in `directory`, or None.

    Ranks candidates by the second date in the filename, falling back to
    file mtime when the filename can't be parsed or two files share a date.
    """
    directory = Path(str(directory)).expanduser()
    if not directory.is_dir():
        return None

    candidates = []
    for p in directory.iterdir():
        if not p.is_file():
            continue
        m = _REVOLUT_FILENAME_RE.match(p.name)
        if m:
            export_to = m.group(2)  # the "to" date — usually the export date
        else:
            # Fall back: any CSV with a Revolut-style header
            if not is_revolut_csv(p):
                continue
            export_to = ""  # unknown — will sort last by this key, then by mtime

        try:
            mtime = p.stat().st_mtime
        except OSError:
            mtime = 0.0
        candidates.append((export_to, mtime, p))

    if not candidates:
        return None

    # Sort: newest export_to first, then newest mtime
    candidates.sort(key=lambda t: (t[0], t[1]), reverse=True)
    return candidates[0][2]


def _parse_revolut_export_date(path):
    """Best-effort: extract the "to" date from a Revolut filename, or None."""
    m = _REVOLUT_FILENAME_RE.match(Path(path).name)
    return m.group(2) if m else None


def _preview_csv(path, n_recent=3):
    """Quickly parse a CSV and return (total_count, pending_count, recent_txs).

    `recent_txs` is a list of up to n_recent dicts sorted by date descending.
    Returns (None, None, []) if the CSV can't be parsed.
    """
    try:
        txs = parse_revolut_csv(str(path))
    except Exception:
        return None, None, []
    if not txs:
        return 0, 0, []
    pending = sum(1 for t in txs if t.get("cleared") != "cleared")
    # Parsed list order reflects CSV order; sort by date desc for a stable preview
    recent = sorted(txs, key=lambda t: t.get("date", ""), reverse=True)[:n_recent]
    return len(txs), pending, recent


def confirm_csv_selection(path, assume_yes=False):
    """Show the auto-detected CSV and ask the user to confirm.

    Displays the file path, export date, size, a transaction count, and the
    most recent transactions so the user can sanity-check before importing.
    Returns True on confirm, False on decline. `assume_yes` skips the prompt
    (useful for cron/headless runs).
    """
    path = Path(path)
    export_date = _parse_revolut_export_date(path)
    size_kb = max(1, path.stat().st_size // 1024) if path.exists() else 0

    print(f"\n📄 Latest Revolut CSV found:")
    print(f"   File:       {path}")
    if export_date:
        print(f"   Export to:  {export_date}")
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        print(f"   Modified:   {mtime}")
    except OSError:
        pass
    print(f"   Size:       {size_kb:,} KB")

    # ── Preview the CSV contents ──
    total, pending, recent = _preview_csv(path, n_recent=3)
    if total is None:
        print(f"   ⚠ Could not parse CSV — proceeding will still attempt import.")
    else:
        pending_suffix = f" ({pending} pending)" if pending else ""
        print(f"   Transactions: {total}{pending_suffix}")
        if recent:
            print(f"\n   Most recent transactions:")
            for tx in recent:
                amt = tx["amount"] / 1000
                mark = "✓" if tx.get("cleared") == "cleared" else "◌"
                payee = (tx.get("payee_name") or "").strip()
                # Truncate long payees so the preview stays tidy
                if len(payee) > 40:
                    payee = payee[:37] + "..."
                print(f"     {mark} {tx['date']}  {amt:>10.2f}  {payee}")

    if assume_yes:
        print("   → Auto-confirmed (--yes)")
        return True

    try:
        resp = input("\nUse this file? [Y/n]: ").strip().lower()
    except EOFError:
        # Non-interactive stdin — treat as decline so we don't silently use the wrong file
        print("\n(no tty — rerun with --yes to skip the prompt)")
        return False
    return resp in ("", "y", "yes")


# ─── File watcher ────────────────────────────────────────────────────────────

def is_revolut_csv(filepath):
    """Check if a file looks like a Revolut account statement CSV."""
    filepath = Path(filepath)
    name = filepath.name.lower()
    if not name.endswith(".csv"):
        return False
    if "account-statement" in name or "revolut" in name:
        return True
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            header = f.readline()
            return "Started Date" in header and "Completed Date" in header
    except Exception:
        return False


def watch_folder(folder_path, token, budget_id, account_id, poll_interval=5):
    """Watch a folder for new Revolut CSVs and auto-import them."""
    folder = Path(folder_path).expanduser().resolve()
    if not folder.is_dir():
        print(f"Error: '{folder}' is not a valid directory.")
        sys.exit(1)

    conn = init_db()

    print(f"\n👀 Watching '{folder}' for Revolut CSVs...")
    print("   (AirDrop your CSV from your phone — it will be imported automatically)")
    print("   Press Ctrl+C to stop.\n")

    seen = set()
    for f in folder.iterdir():
        if f.is_file():
            seen.add(f.name)

    try:
        while True:
            for f in folder.iterdir():
                if f.is_file() and f.name not in seen:
                    seen.add(f.name)
                    if is_revolut_csv(f):
                        print(f"\n📄 Found new Revolut CSV: {f.name}")
                        try:
                            transactions = parse_revolut_csv(str(f))
                            print(f"   Parsed {len(transactions)} transaction(s)")
                            import_and_track(conn, token, budget_id, account_id, transactions)
                            print(f"   ✓ Done with {f.name}\n")
                        except Exception as e:
                            print(f"   ✗ Error processing {f.name}: {e}\n")
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        print("\n\nStopped watching.")
    finally:
        conn.close()


# ─── Crypto portfolio sync ─────────────────────────────────────────────────
#
# BIP84 HD wallet support: derives native segwit (bc1...) addresses locally
# from an xpub, then queries Blockstream for each address balance.
# Pure Python — no external dependencies beyond stdlib.
# ───────────────────────────────────────────────────────────────────────────

import hashlib
import hmac
import struct

# ── secp256k1 elliptic curve (minimal operations for BIP32) ──

_EC_P = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F
_EC_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
_EC_Gx = 0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798
_EC_Gy = 0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8


def _ec_add(p, q):
    """Add two secp256k1 points."""
    if p is None:
        return q
    if q is None:
        return p
    x1, y1 = p
    x2, y2 = q
    if x1 == x2:
        if y1 != y2:
            return None
        s = (3 * x1 * x1) * pow(2 * y1, _EC_P - 2, _EC_P) % _EC_P
    else:
        s = (y2 - y1) * pow(x2 - x1, _EC_P - 2, _EC_P) % _EC_P
    x3 = (s * s - x1 - x2) % _EC_P
    y3 = (s * (x1 - x3) - y1) % _EC_P
    return (x3, y3)


def _ec_mul(k, point):
    """Scalar multiplication on secp256k1 (double-and-add)."""
    result = None
    addend = point
    while k:
        if k & 1:
            result = _ec_add(result, addend)
        addend = _ec_add(addend, addend)
        k >>= 1
    return result


def _decompress_pubkey(data):
    """Decompress a 33-byte compressed public key to (x, y)."""
    prefix = data[0]
    x = int.from_bytes(data[1:], "big")
    y_sq = (pow(x, 3, _EC_P) + 7) % _EC_P
    y = pow(y_sq, (_EC_P + 1) // 4, _EC_P)
    if (y % 2) != (prefix % 2):
        y = _EC_P - y
    return (x, y)


def _compress_pubkey(x, y):
    """Compress an (x, y) point to 33 bytes."""
    prefix = b"\x02" if y % 2 == 0 else b"\x03"
    return prefix + x.to_bytes(32, "big")


# ── BIP32 public child derivation ──

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58decode_xpub(s):
    """Base58-decode an extended public key (always 82 bytes)."""
    n = 0
    for c in s:
        n = n * 58 + _B58_ALPHABET.index(c)
    return n.to_bytes(82, "big")


def _parse_xpub(xpub_str):
    """Parse an xpub string → (compressed_pubkey_33, chain_code_32)."""
    raw = _b58decode_xpub(xpub_str)
    # [4 version][1 depth][4 fingerprint][4 child_index][32 chain_code][33 key][4 checksum]
    chain_code = raw[13:45]
    key_data = raw[45:78]
    return key_data, chain_code


def _bip32_derive_child_pub(parent_key, parent_chain, index):
    """Non-hardened BIP32 child key derivation (public only)."""
    assert index < 0x80000000, "Cannot do hardened derivation from xpub"
    data = parent_key + struct.pack(">I", index)
    I = hmac.new(parent_chain, data, hashlib.sha512).digest()
    IL, IR = I[:32], I[32:]
    il_int = int.from_bytes(IL, "big")
    if il_int >= _EC_N:
        raise ValueError("Invalid child key")
    parent_point = _decompress_pubkey(parent_key)
    child_point = _ec_add(_ec_mul(il_int, (_EC_Gx, _EC_Gy)), parent_point)
    if child_point is None:
        raise ValueError("Invalid child key (point at infinity)")
    return _compress_pubkey(*child_point), IR


# ── Bech32 encoding (BIP173) ──

_BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"


def _bech32_polymod(values):
    GEN = [0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3]
    chk = 1
    for v in values:
        b = chk >> 25
        chk = ((chk & 0x1FFFFFF) << 5) ^ v
        for i in range(5):
            chk ^= GEN[i] if ((b >> i) & 1) else 0
    return chk


def _bech32_encode(hrp, data):
    values = [ord(x) >> 5 for x in hrp] + [0] + [ord(x) & 31 for x in hrp]
    polymod = _bech32_polymod(values + data + [0, 0, 0, 0, 0, 0]) ^ 1
    checksum = [(polymod >> 5 * (5 - i)) & 31 for i in range(6)]
    return hrp + "1" + "".join(_BECH32_CHARSET[d] for d in data + checksum)


def _convertbits(data, frombits, tobits, pad=True):
    acc, bits, ret = 0, 0, []
    maxv = (1 << tobits) - 1
    for value in data:
        acc = (acc << frombits) | value
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if pad and bits:
        ret.append((acc << (tobits - bits)) & maxv)
    return ret


def _hash160(data):
    """HASH160: SHA256 then RIPEMD160 (portable across OpenSSL configurations)."""
    sha = hashlib.sha256(data).digest()
    # Try standard hashlib first (works on most macOS / older Linux)
    try:
        return hashlib.new("ripemd160", sha).digest()
    except ValueError:
        pass
    # Try with usedforsecurity=False (newer OpenSSL with legacy provider)
    try:
        return hashlib.new("ripemd160", sha, usedforsecurity=False).digest()
    except (ValueError, TypeError):
        pass
    # Fallback: pycryptodome (pip install pycryptodome)
    try:
        from Crypto.Hash import RIPEMD160
        return RIPEMD160.new(sha).digest()
    except ImportError:
        print("  ✗ RIPEMD160 not available. Install pycryptodome: pip install pycryptodome")
        sys.exit(1)


def _pubkey_to_bech32(compressed_pub):
    """Convert 33-byte compressed public key → bc1... native segwit address."""
    h160 = _hash160(compressed_pub)
    data5 = [0] + _convertbits(list(h160), 8, 5)  # witness version 0
    return _bech32_encode("bc", data5)


# ── xpub → address derivation + Blockstream balance queries ──

def _query_address_balance(address):
    """Query Blockstream API for an address's balance and tx count.

    Retries 429 (rate limit) and transient 5xx with exponential backoff.
    Raises RuntimeError on final failure instead of sys.exit, so callers
    (including the Telegram bot) can surface a real error message.
    """
    url = f"https://blockstream.info/api/address/{address}"
    req = Request(url, headers={"User-Agent": "revolut-to-ynab/1.0"})

    last_err = None
    data = None
    for attempt in range(5):
        try:
            with urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            break
        except HTTPError as e:
            last_err = e
            if e.code in (429, 502, 503, 504) and attempt < 4:
                wait = 2 ** attempt  # 1, 2, 4, 8s
                print(f"  ⏳ Blockstream {e.code} for {address[:12]}..., retry in {wait}s")
                time.sleep(wait)
                continue
            raise RuntimeError(
                f"Blockstream error for {address[:12]}...: HTTP {e.code}"
            ) from e
        except URLError as e:
            last_err = e
            if attempt < 4:
                wait = 2 ** attempt
                print(f"  ⏳ Network error for {address[:12]}..., retry in {wait}s: {e}")
                time.sleep(wait)
                continue
            raise RuntimeError(
                f"Blockstream network error for {address[:12]}...: {e}"
            ) from e

    if data is None:
        raise RuntimeError(
            f"Blockstream failed after retries for {address[:12]}...: {last_err}"
        )

    cs = data.get("chain_stats", {})
    ms = data.get("mempool_stats", {})
    funded = cs.get("funded_txo_sum", 0) + ms.get("funded_txo_sum", 0)
    spent = cs.get("spent_txo_sum", 0) + ms.get("spent_txo_sum", 0)
    balance = funded - spent
    tx_count = cs.get("tx_count", 0) + ms.get("tx_count", 0)
    return balance, tx_count


def fetch_btc_balance_xpub(xpub):
    """
    Fetch total BTC balance for an HD wallet from its xpub.
    Derives BIP84 native segwit addresses locally, then queries
    Blockstream for each. Stops after 20 consecutive unused
    addresses (standard gap limit).
    """
    GAP_LIMIT = 20

    parent_key, parent_chain = _parse_xpub(xpub)

    total_sats = 0
    active_addrs = 0

    for chain_idx in (0, 1):  # 0 = receive, 1 = change
        label = "receive" if chain_idx == 0 else "change"
        chain_key, chain_chain = _bip32_derive_child_pub(
            parent_key, parent_chain, chain_idx
        )

        gap = 0
        i = 0
        while gap < GAP_LIMIT:
            child_key, _ = _bip32_derive_child_pub(chain_key, chain_chain, i)
            addr = _pubkey_to_bech32(child_key)

            balance, tx_count = _query_address_balance(addr)

            if tx_count > 0:
                total_sats += balance
                active_addrs += 1
                gap = 0
                if balance > 0:
                    btc = balance / 1e8
                    print(f"    {label}/{i}: {addr[:16]}... {btc:.8f} BTC")
            else:
                gap += 1

            i += 1
            time.sleep(0.6)  # Blockstream free tier is strict; stay well under 2 req/s

    print(f"    Active addresses scanned: {active_addrs}")
    return total_sats / 1e8


def fetch_btc_balance_address(address):
    """Fetch BTC balance for a single address (fallback if no xpub)."""
    balance, _ = _query_address_balance(address)
    return balance / 1e8


# ── Ethereum balance queries (via public JSON-RPC) ──

def _get_eth_rpc_urls():
    """Build ordered list of Ethereum RPC endpoints.

    Authenticated Ankr first if configured, then a broad set of free public
    endpoints as fallbacks. Endpoints are tried in order and the first one
    that returns a usable result wins.
    """
    urls = []
    ankr_key = os.environ.get("ANKR_API_KEY", "")
    if ankr_key:
        urls.append(f"https://rpc.ankr.com/eth/{ankr_key}")
    # Public endpoints — ordered by observed reliability.
    # Note: unauthenticated Ankr now returns -32000 Unauthorized, so it's
    # kept last as a last-ditch option rather than first.
    urls.extend([
        "https://ethereum.publicnode.com",
        "https://eth.drpc.org",
        "https://rpc.payload.de",
        "https://1rpc.io/eth",
        "https://eth.meowrpc.com",
        "https://cloudflare-eth.com",
        "https://eth.llamarpc.com",
        "https://rpc.ankr.com/eth",
    ])
    return urls

# ERC-20 tokens to check: (symbol, contract_address, decimals, coingecko_id)
ERC20_TOKENS = [
    ("AAVE",  "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", 18, "aave"),
    ("USDC",  "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  6, "usd-coin"),
    ("USDT",  "0xdAC17F958D2ee523a2206206994597C13D831ec7",  6, "tether"),
    ("aUSDT", "0x23878914EFE38d27C4D67Ab83ed1b93A74D4086a",  6, "tether"),
]


def _eth_rpc_call(method, params):
    """Make a JSON-RPC call to a public Ethereum node (tries multiple endpoints).

    Tries each endpoint in order, skipping those that return RPC errors or
    raise network exceptions. Raises RuntimeError if every endpoint fails
    so callers (including the Telegram bot) can surface a proper error
    message instead of silently sys.exit'ing.
    """
    payload = json.dumps({
        "jsonrpc": "2.0", "id": 1,
        "method": method, "params": params,
    }).encode()

    errors = []
    for rpc_url in _get_eth_rpc_urls():
        try:
            req = Request(rpc_url, data=payload, headers={
                "Content-Type": "application/json",
                "User-Agent": "revolut-to-ynab/1.0",
            })
            with urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            if "result" in data:
                return data["result"]
            if "error" in data:
                err = data["error"]
                msg = err.get("message") if isinstance(err, dict) else str(err)
                short = (msg or "")[:120]
                print(f"  ⚠ RPC error from {rpc_url}: {short}")
                errors.append(f"{rpc_url}: {short}")
                continue
        except Exception as e:
            short = str(e)[:120]
            print(f"  ⚠ Network error from {rpc_url}: {short}")
            errors.append(f"{rpc_url}: {short}")
            continue

    print("  ✗ All Ethereum RPC endpoints failed")
    # Raise a readable error so bot users see why, e.g.:
    #   "Crypto sync failed: All ETH RPC endpoints failed for eth_getBalance"
    raise RuntimeError(
        f"All ETH RPC endpoints failed for {method}: "
        + " | ".join(errors[-3:])  # last 3 keep the message short
    )


def fetch_eth_balance(eth_address):
    """Fetch native ETH balance (in ETH, 18 decimals)."""
    result = _eth_rpc_call("eth_getBalance", [eth_address, "latest"])
    wei = int(result, 16)
    return wei / 1e18


def fetch_erc20_balance(eth_address, contract_address, decimals=6):
    """Fetch an ERC-20 token balance using balanceOf(address)."""
    addr_padded = "0" * 24 + eth_address[2:].lower()
    calldata = "0x70a08231" + addr_padded
    result = _eth_rpc_call("eth_call", [
        {"to": contract_address, "data": calldata}, "latest"
    ])
    raw = int(result, 16)
    return raw / (10 ** decimals)


def fetch_eth_wallet_balances(eth_address):
    """
    Fetch all relevant balances from an Ethereum address.
    Returns dict: {"ETH": amount, "AAVE": amount, "USDC": amount, ...}
    Only includes tokens with non-zero balances.
    """
    balances = {}

    # Native ETH
    balances["ETH"] = fetch_eth_balance(eth_address)

    # Scan all configured ERC-20 tokens
    for symbol, contract, decimals, _ in ERC20_TOKENS:
        amount = fetch_erc20_balance(eth_address, contract, decimals=decimals)
        if amount > 0:
            balances[symbol] = amount

    return balances


# ── Price fetching (multi-asset) ──

def fetch_crypto_prices_chf(symbols_needed):
    """
    Fetch CHF prices from CoinGecko for the given symbols.
    symbols_needed: set of symbols like {"BTC", "ETH", "AAVE", "USDC"}
    Returns dict: {"BTC": price_chf, "ETH": price_chf, ...}
    """
    # Map symbols to CoinGecko IDs
    symbol_to_cg = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "USDT": "tether",
        "USDC": "usd-coin",
        "aUSDT": "tether",  # aUSDT tracks USDT 1:1
    }
    # Add token mappings from ERC20_TOKENS list
    for symbol, _, _, cg_id in ERC20_TOKENS:
        if symbol not in symbol_to_cg:
            symbol_to_cg[symbol] = cg_id

    cg_ids = set()
    for sym in symbols_needed:
        cg_id = symbol_to_cg.get(sym)
        if cg_id:
            cg_ids.add(cg_id)

    if not cg_ids:
        return {}

    ids_param = ",".join(sorted(cg_ids))
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids_param}&vs_currencies=chf"
    req = Request(url, headers={"User-Agent": "revolut-to-ynab/1.0"})
    try:
        with urlopen(req) as resp:
            data = json.loads(resp.read().decode())
    except HTTPError as e:
        print(f"  ✗ CoinGecko API error ({e.code}): {e.read().decode()}")
        sys.exit(1)

    prices = {}
    for sym in symbols_needed:
        cg_id = symbol_to_cg.get(sym)
        if cg_id:
            p = data.get(cg_id, {}).get("chf")
            if p is not None:
                prices[sym] = float(p)
            else:
                print(f"  ⚠ Could not get {sym}/CHF price — skipping")

    return prices


def get_ynab_account_balance(token, budget_id, account_id):
    """Get the current cleared balance of a YNAB account (in milliunits)."""
    result = ynab_request("GET", f"/budgets/{budget_id}/accounts/{account_id}", token)
    account = result.get("data", {}).get("account", {})
    # cleared_balance is the settled balance in milliunits
    return account.get("cleared_balance", 0)


def crypto_sync(token, budget_id, crypto_account_id, btc_xpub=None, eth_address=None, dry_run=False):
    """
    Sync crypto portfolio value to YNAB:
    1. Fetch balances from all configured wallets (BTC xpub, ETH address)
    2. Convert everything to CHF using live prices
    3. Get current YNAB crypto account balance
    4. Post a single adjustment transaction for the delta
    """
    if not btc_xpub and not eth_address:
        print("Error: at least one of --btc-xpub or --eth-address required.")
        sys.exit(1)

    print(f"\n🪙 Crypto portfolio sync")

    # ── Step 1: Fetch all balances ──
    holdings = {}  # {"BTC": amount, "ETH": amount, "aUSDT": amount}
    memo_parts = []

    if btc_xpub:
        is_xpub = btc_xpub.startswith(("xpub", "ypub", "zpub"))
        if is_xpub:
            print(f"   BTC xpub: {btc_xpub[:12]}...{btc_xpub[-8:]}")
        else:
            print(f"   BTC addr: {btc_xpub[:8]}...{btc_xpub[-6:]}")

        print(f"\n  → Fetching BTC balance...")
        if is_xpub:
            holdings["BTC"] = fetch_btc_balance_xpub(btc_xpub)
        else:
            holdings["BTC"] = fetch_btc_balance_address(btc_xpub)
        print(f"    Balance: {holdings['BTC']:.8f} BTC")

    if eth_address:
        print(f"   ETH addr: {eth_address[:8]}...{eth_address[-6:]}")
        print(f"\n  → Fetching ETH wallet balances...")
        eth_balances = fetch_eth_wallet_balances(eth_address)

        for sym, amount in eth_balances.items():
            holdings[sym] = amount
            if amount > 0:
                if sym == "ETH":
                    print(f"    {sym}:  {amount:.6f}")
                else:
                    print(f"    {sym}: {amount:,.6f}")
            else:
                print(f"    {sym}:  0")

    # ── Step 2: Fetch prices and compute total CHF value ──
    # Only fetch prices for assets we actually hold
    symbols_needed = {sym for sym, amt in holdings.items() if amt > 0}
    print(f"\n  → Fetching CHF prices...")
    prices = fetch_crypto_prices_chf(symbols_needed)

    portfolio_chf = 0.0
    print(f"    ┌────────────────────────────────────────────")

    for sym in ["BTC", "ETH"] + sorted(s for s in holdings if s not in ("BTC", "ETH")):
        amount = holdings.get(sym, 0)
        if amount <= 0 or sym not in prices:
            continue
        value_chf = amount * prices[sym]
        portfolio_chf += value_chf
        memo_parts.append(f"{sym} {amount:.6g}@{prices[sym]:,.0f}")
        print(f"    │ {sym:<6} {amount:>14.6f} × {prices[sym]:>10,.2f} = {value_chf:>12,.2f} CHF")

    print(f"    ├────────────────────────────────────────────")
    print(f"    │ TOTAL: {portfolio_chf:>49,.2f} CHF")
    print(f"    └────────────────────────────────────────────")

    # ── Step 3: Get current YNAB balance ──
    print(f"\n  → Fetching YNAB crypto account balance...")
    ynab_balance_milli = get_ynab_account_balance(token, budget_id, crypto_account_id)
    ynab_balance_chf = ynab_balance_milli / 1000
    print(f"    YNAB balance: {ynab_balance_chf:,.2f} CHF")

    # ── Step 4: Compute delta ──
    portfolio_milli = int(round(portfolio_chf * 1000))
    delta_milli = portfolio_milli - ynab_balance_milli
    delta_chf = delta_milli / 1000

    if abs(delta_milli) < 10:  # less than 1 cent
        print(f"\n  ✓ Already in sync (delta: {delta_chf:,.2f} CHF)")
        log.info("crypto sync: already in sync portfolio=%.2f CHF ynab=%.2f CHF",
                 portfolio_chf, ynab_balance_chf)
        return

    direction = "📈" if delta_milli > 0 else "📉"
    print(f"\n  {direction} Delta: {delta_chf:+,.2f} CHF")
    log.info("crypto sync: portfolio=%.2f CHF ynab=%.2f CHF delta=%+.2f CHF",
             portfolio_chf, ynab_balance_chf, delta_chf)

    if dry_run:
        print(f"  (dry run — no transaction created)")
        log.info("crypto sync: dry-run, no adjustment created")
        return

    # ── Step 5: Create adjustment transaction in YNAB ──
    today = datetime.now().strftime("%Y-%m-%d")
    import_id = f"CRYPTO:{delta_milli}:{today}:1"

    memo = " | ".join(memo_parts)
    if len(memo) > 200:
        memo = memo[:197] + "..."

    tx = {
        "account_id": crypto_account_id,
        "date": today,
        "amount": delta_milli,
        "payee_name": "Crypto Portfolio Adjustment",
        "memo": memo,
        "cleared": "cleared",
        "approved": True,
        "import_id": import_id,
    }

    print(f"  → Creating adjustment transaction in YNAB...")
    body = {"transaction": tx}
    result = ynab_request("POST", f"/budgets/{budget_id}/transactions", token, body)

    ynab_tx = result.get("data", {}).get("transaction", {})
    if ynab_tx.get("id"):
        print(f"    ✓ Transaction created: {delta_chf:+,.2f} CHF")
        print(f"    New YNAB balance: {portfolio_chf:,.2f} CHF")
        log.info("crypto sync: adjustment created ynab_id=%s amount=%+.2f CHF new_balance=%.2f CHF",
                 ynab_tx.get("id"), delta_chf, portfolio_chf)
    else:
        dupes = result.get("data", {}).get("duplicate_import_ids", [])
        if dupes:
            print(f"    ⊘ Already synced today (duplicate import_id)")
            log.info("crypto sync: duplicate import_id %s — no new transaction", import_id)
        else:
            print(f"    ✓ Done")
            log.info("crypto sync: done (no ynab_id returned, no duplicate)")


# ─── Interactive Brokers brokerage sync (Client Portal Gateway) ────────────
#
# Uses the IB Client Portal Gateway running locally on your Mac.
# The gateway uses self-signed certificates, so SSL verification is disabled.
# ───────────────────────────────────────────────────────────────────────────

DEFAULT_IBKR_BASE_URL = "https://localhost:5050"

# SSL context for IB Client Portal Gateway (self-signed certificates)
_IBKR_SSL_CTX = ssl.create_default_context()
_IBKR_SSL_CTX.check_hostname = False
_IBKR_SSL_CTX.verify_mode = ssl.CERT_NONE


def _ibkr_request(method, path, base_url):
    """Make a request to the IB Client Portal API (localhost gateway)."""
    url = f"{base_url}/v1/api{path}"
    req = Request(url, headers={"User-Agent": "revolut-to-ynab/1.0"}, method=method)
    try:
        with urlopen(req, context=_IBKR_SSL_CTX, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        body = e.read().decode()
        print(f"  ✗ IBKR API error ({e.code}): {body}")
        if e.code == 401:
            print("    → Session expired. Re-authenticate at your IB Gateway web UI.")
        sys.exit(1)
    except URLError as e:
        print(f"  ✗ Cannot reach IB Gateway at {base_url}")
        print(f"    → Make sure the Client Portal Gateway is running.")
        print(f"    → Authenticate in your browser at {base_url}")
        sys.exit(1)


def ibkr_get_accounts(base_url):
    """List IB accounts."""
    return _ibkr_request("GET", "/portfolio/accounts", base_url)


def ibkr_get_nav_chf(base_url, account_id):
    """
    Get the Net Asset Value (NAV) of an IB account in CHF.
    Uses the /portfolio/{accountId}/ledger endpoint which returns
    balances broken down by currency, plus a BASE summary.
    """
    data = _ibkr_request("GET", f"/portfolio/{account_id}/ledger", base_url)

    # "BASE" contains the total in the account's base currency (CHF).
    base = data.get("BASE", {})
    nav = base.get("netliquidationvalue", 0)

    if nav == 0:
        chf_entry = data.get("CHF", {})
        nav = chf_entry.get("netliquidationvalue", 0)

    return float(nav)


def brokerage_sync(token, budget_id, brokerage_account_id, ibkr_base_url,
                   ibkr_account_id=None, dry_run=False):
    """
    Sync Interactive Brokers portfolio value to YNAB:
    1. Query IB Client Portal Gateway for Net Asset Value (CHF)
    2. Get current YNAB brokerage account balance
    3. Post a transaction for the delta
    """
    print(f"\n📊 Interactive Brokers sync")
    print(f"   Gateway: {ibkr_base_url}")

    # Step 1: Resolve account ID if not provided
    if not ibkr_account_id:
        print(f"\n  → Discovering accounts...")
        accounts = ibkr_get_accounts(ibkr_base_url)
        if not accounts:
            print("  ✗ No accounts found")
            sys.exit(1)
        ibkr_account_id = accounts[0].get("accountId", accounts[0].get("id"))
        print(f"    Using account: {ibkr_account_id}")
    else:
        print(f"   Account: {ibkr_account_id}")

    # Step 2: Get NAV from IB
    print(f"\n  → Fetching portfolio NAV...")
    nav_chf = ibkr_get_nav_chf(ibkr_base_url, ibkr_account_id)
    print(f"    Net Asset Value: {nav_chf:,.2f} CHF")

    if nav_chf <= 0:
        print("  ⚠ NAV is zero or negative — check your IBKR OAuth config and account access")
        return

    # Step 4: Get current YNAB balance
    print(f"  → Fetching YNAB brokerage account balance...")
    ynab_balance_milli = get_ynab_account_balance(token, budget_id, brokerage_account_id)
    ynab_balance_chf = ynab_balance_milli / 1000
    print(f"    YNAB balance: {ynab_balance_chf:,.2f} CHF")

    # Step 5: Compute delta
    nav_milli = int(round(nav_chf * 1000))
    delta_milli = nav_milli - ynab_balance_milli
    delta_chf = delta_milli / 1000

    if abs(delta_milli) < 10:
        print(f"\n  ✓ Already in sync (delta: {delta_chf:,.2f} CHF)")
        log.info("brokerage sync: already in sync nav=%.2f CHF ynab=%.2f CHF account=%s",
                 nav_chf, ynab_balance_chf, ibkr_account_id)
        return

    direction = "📈" if delta_milli > 0 else "📉"
    print(f"\n  {direction} Delta: {delta_chf:+,.2f} CHF")
    log.info("brokerage sync: nav=%.2f CHF ynab=%.2f CHF delta=%+.2f CHF account=%s",
             nav_chf, ynab_balance_chf, delta_chf, ibkr_account_id)

    if dry_run:
        print(f"  (dry run — no transaction created)")
        log.info("brokerage sync: dry-run, no adjustment created")
        return

    # Step 6: Create adjustment transaction
    today = datetime.now().strftime("%Y-%m-%d")
    import_id = f"IBKR:{delta_milli}:{today}:1"

    tx = {
        "account_id": brokerage_account_id,
        "date": today,
        "amount": delta_milli,
        "payee_name": "IBKR Portfolio Adjustment",
        "memo": f"NAV {nav_chf:,.2f} CHF (account {ibkr_account_id})",
        "cleared": "cleared",
        "approved": True,
        "import_id": import_id,
    }

    print(f"  → Creating adjustment transaction in YNAB...")
    body = {"transaction": tx}
    result = ynab_request("POST", f"/budgets/{budget_id}/transactions", token, body)

    ynab_tx = result.get("data", {}).get("transaction", {})
    if ynab_tx.get("id"):
        print(f"    ✓ Transaction created: {delta_chf:+,.2f} CHF")
        print(f"    New YNAB balance: {nav_chf:,.2f} CHF")
        log.info("brokerage sync: adjustment created ynab_id=%s amount=%+.2f CHF new_balance=%.2f CHF",
                 ynab_tx.get("id"), delta_chf, nav_chf)
    else:
        dupes = result.get("data", {}).get("duplicate_import_ids", [])
        if dupes:
            print(f"    ⊘ Already synced today (duplicate import_id)")
            log.info("brokerage sync: duplicate import_id %s — no new transaction", import_id)
        else:
            print(f"    ✓ Done")
            log.info("brokerage sync: done (no ynab_id returned, no duplicate)")


# ─── Reconciliation (from CSV running balance) ─────────────────────────────
#
# Revolut's CSV export includes a `Balance` column showing the running balance
# after each row. The latest row-with-balance is the authoritative cleared
# position at that moment. Reconcile by comparing it to YNAB's cleared_balance
# and, if they differ, creating a YNAB "Reconciliation Balance Adjustment".
# ───────────────────────────────────────────────────────────────────────────


def extract_csv_running_balance(filepath):
    """Find the most recent row in a Revolut CSV that has a non-empty Balance.

    Restricted to rows whose ``Product`` is in :data:`TRACKED_PRODUCTS`
    so that e.g. a closed Savings pocket's 0.00 closing entry at the
    bottom of the CSV doesn't get picked up as the "latest" running
    balance and trigger reconcile to push YNAB toward zero.

    Returns a dict: ``{"balance": float, "date": "YYYY-MM-DD", "currency": "CHF"}``
    or None if no such row exists.
    """
    with open(filepath, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    for row in reversed(rows):
        product = (row.get("Product") or "").strip()
        if product and product not in TRACKED_PRODUCTS:
            continue
        bal = (row.get("Balance") or "").strip()
        if not bal:
            continue
        try:
            balance = float(bal)
        except ValueError:
            continue
        date_raw = (row.get("Completed Date") or row.get("Started Date") or "").strip()
        iso_date = date_raw.split(" ")[0] if date_raw else datetime.now().strftime("%Y-%m-%d")
        return {
            "balance": balance,
            "date": iso_date,
            "currency": (row.get("Currency") or "").strip(),
        }
    return None


def reconcile_from_csv(token, budget_id, account_id, csv_path, dry_run=False):
    """Reconcile YNAB's cleared balance to the running balance in a Revolut CSV.

    Finds the latest row with a non-empty `Balance` column, compares it to
    YNAB's cleared_balance for `account_id`, and posts a reconciliation
    adjustment transaction if they diverge by ≥ 0.01.
    """
    print(f"\n🧮 Reconcile from CSV")
    print(f"   File: {csv_path}")
    log.info("reconcile: start file=%s account=%s", csv_path, account_id)

    snapshot = extract_csv_running_balance(csv_path)
    if not snapshot:
        msg = "  ⚠ CSV has no populated Balance column — cannot reconcile."
        print(msg)
        log.warning("reconcile: no running balance found in CSV")
        return

    target_balance = snapshot["balance"]
    as_of = snapshot["date"]
    currency = snapshot["currency"] or "CHF"

    target_milli = int(round(target_balance * 1000))
    ynab_cleared_milli = get_ynab_account_balance(token, budget_id, account_id)
    ynab_cleared = ynab_cleared_milli / 1000

    print(f"   CSV balance (as of {as_of}): {target_balance:,.2f} {currency}")
    print(f"   YNAB cleared balance:        {ynab_cleared:,.2f} {currency}")

    delta_milli = target_milli - ynab_cleared_milli
    delta = delta_milli / 1000

    if abs(delta_milli) < 10:  # less than 1 cent difference
        print(f"\n  ✓ Already reconciled (delta: {delta:,.2f} {currency})")
        log.info("reconcile: already in sync target=%.2f ynab=%.2f currency=%s as_of=%s",
                 target_balance, ynab_cleared, currency, as_of)
        return

    direction = "📈" if delta_milli > 0 else "📉"
    print(f"\n  {direction} Delta to reconcile: {delta:+,.2f} {currency}")
    log.info("reconcile: target=%.2f ynab=%.2f delta=%+.2f currency=%s as_of=%s",
             target_balance, ynab_cleared, delta, currency, as_of)

    if dry_run:
        print(f"  (dry run — no reconciliation adjustment created)")
        log.info("reconcile: dry-run, no adjustment created")
        return

    # Create a reconciliation adjustment in YNAB. `cleared=reconciled` tells
    # YNAB to lock these (and all cleared transactions up to this point) in the
    # reconciled state, matching what the built-in reconcile flow does.
    #
    # NOTE: YNAB reserves a set of internal payee names ("Reconciliation
    # Balance Adjustment", "Starting Balance", "Manual Balance Adjustment",
    # "Transfer :") — the API rejects transactions whose payee_name *starts*
    # with any of them. We use "CSV Reconciliation" so the transaction still
    # shows its purpose without tripping the reserved-name check.
    import_id = f"RECON:{delta_milli}:{as_of}:1"
    tx = {
        "account_id": account_id,
        "date": as_of,
        "amount": delta_milli,
        "payee_name": "CSV Reconciliation",
        "memo": f"Reconciled to CSV running balance {target_balance:,.2f} {currency} as of {as_of}",
        "cleared": "reconciled",
        "approved": True,
        "import_id": import_id,
    }

    print(f"  → Creating reconciliation adjustment in YNAB...")
    body = {"transaction": tx}
    result = ynab_request("POST", f"/budgets/{budget_id}/transactions", token, body)

    ynab_tx = result.get("data", {}).get("transaction", {})
    if ynab_tx.get("id"):
        print(f"    ✓ Adjustment created: {delta:+,.2f} {currency}")
        print(f"    YNAB cleared balance is now: {target_balance:,.2f} {currency}")
        log.info("reconcile: adjustment created ynab_id=%s amount=%+.2f %s new_balance=%.2f %s",
                 ynab_tx.get("id"), delta, currency, target_balance, currency)
    else:
        dupes = result.get("data", {}).get("duplicate_import_ids", [])
        if dupes:
            print(f"    ⊘ Already reconciled at this date (duplicate import_id)")
            log.info("reconcile: duplicate import_id %s — no new transaction", import_id)
        else:
            print(f"    ✓ Done")
            log.info("reconcile: done (no ynab_id returned, no duplicate)")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    # Load .env file (if present) into os.environ before reading config.
    # Shell-exported vars take precedence over .env values.
    _load_dotenv()

    parser = argparse.ArgumentParser(
        description="Import Revolut transactions into YNAB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("csv_file", nargs="?", help="Path to Revolut CSV file")
    parser.add_argument("--token", help="YNAB Personal Access Token (or set YNAB_TOKEN)")
    parser.add_argument("--budget-id", help="YNAB Budget ID (or set YNAB_BUDGET_ID)")
    parser.add_argument("--account-id", help="YNAB Account ID (or set YNAB_ACCOUNT_ID)")
    parser.add_argument("--list-budgets", action="store_true", help="List your YNAB budgets")
    parser.add_argument("--list-accounts", action="store_true", help="List accounts in a budget")
    parser.add_argument("--watch", metavar="FOLDER", help="Watch a folder for new Revolut CSVs")
    parser.add_argument("--sync", action="store_true", help="Sync transactions from YNAB into the local database")
    parser.add_argument("--since-date", help="For --sync: only fetch transactions on/after this date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Parse CSV and show what would be imported/updated")
    parser.add_argument("--skip-pending", action="store_true", help="Skip pending transactions")
    parser.add_argument("--db-stats", action="store_true", help="Show local database statistics")
    parser.add_argument("--db-path", help=f"Custom database path (default: {DEFAULT_DB_PATH})")
    parser.add_argument("--crypto-sync", action="store_true", help="Sync crypto portfolio value to YNAB tracking account")
    parser.add_argument("--crypto-account-id", help="YNAB Account ID for crypto tracking (or set YNAB_CRYPTO_ACCOUNT_ID)")
    parser.add_argument("--btc-xpub", help="BTC extended public key or single address (or set CRYPTO_BTC_XPUB)")
    parser.add_argument("--eth-address", help="Ethereum address for ETH + tokens (or set CRYPTO_ETH_ADDRESS)")
    parser.add_argument("--brokerage-sync", action="store_true", help="Sync Interactive Brokers NAV to YNAB tracking account")
    parser.add_argument("--brokerage-account-id", help="YNAB Account ID for brokerage tracking (or set YNAB_BROKERAGE_ACCOUNT_ID)")
    parser.add_argument("--ibkr-account-id", help="IB account ID (or set IBKR_ACCOUNT_ID; auto-detected if omitted)")
    parser.add_argument("--ibkr-base-url", help="IB Gateway URL (or set IBKR_BASE_URL; default: https://localhost:5050)")
    parser.add_argument("--reconcile", action="store_true",
                        help="Reconcile YNAB cleared balance to the CSV's running Balance column")
    parser.add_argument("--cleanup-pending-memos", action="store_true",
                        help="Scan YNAB for transactions marked cleared with a stale '(pending)' memo and strip the marker")
    parser.add_argument("--log-level", help="Log level: DEBUG, INFO, WARNING, ERROR (or set LOG_LEVEL)")
    parser.add_argument("--log-file", help="Path to log file (or set LOG_FILE; \"\" disables file logging)")
    parser.add_argument("--csv-dir",
                        help=f"Folder to auto-detect the latest Revolut CSV from (or set CSV_DIR; default: {DEFAULT_CSV_DIR})")
    parser.add_argument("-y", "--yes", action="store_true",
                        help="Skip the \"Use this file?\" prompt when auto-detecting the latest CSV")

    args = parser.parse_args()
    config = get_config()

    # ── Logging ──
    log_level = args.log_level or config["log_level"]
    # CLI arg wins over env; empty string from CLI means "disable"
    if args.log_file is not None:
        log_file = args.log_file
    else:
        log_file = config["log_file"]
    setup_logging(log_level, log_file or None)

    token = args.token or config["token"]
    budget_id = args.budget_id or config["budget_id"]
    account_id = args.account_id or config["account_id"]
    crypto_account_id = getattr(args, "crypto_account_id", None) or config["crypto_account_id"]
    btc_xpub = getattr(args, "btc_xpub", None) or config["btc_xpub"]
    eth_address = getattr(args, "eth_address", None) or config["eth_address"]
    ibkr_base_url = getattr(args, "ibkr_base_url", None) or config["ibkr_base_url"]
    ibkr_account_id = getattr(args, "ibkr_account_id", None) or config["ibkr_account_id"]
    brokerage_account_id = getattr(args, "brokerage_account_id", None) or config["brokerage_account_id"]
    csv_dir = getattr(args, "csv_dir", None) or config["csv_dir"]

    def _resolve_csv_path():
        """Return the CSV path to operate on, auto-detecting if needed.

        If the user passed a positional CSV file, use it. Otherwise look in
        `csv_dir` for the newest Revolut export and ask them to confirm.
        Returns None if nothing was found or the user declined.
        """
        if args.csv_file:
            return Path(args.csv_file).expanduser().resolve()

        latest = find_latest_revolut_csv(csv_dir)
        if not latest:
            print(f"\nError: no Revolut CSV found in {csv_dir}.")
            print(f"  Pass a file explicitly, or set CSV_DIR / --csv-dir to the right folder.")
            return None

        if not confirm_csv_selection(latest, assume_yes=args.yes):
            print("  Aborted — pass the CSV path explicitly as an argument if you'd prefer.")
            return None

        return latest.resolve()

    # ── DB stats ──
    if args.db_stats:
        conn = init_db(args.db_path)
        db_stats(conn)
        conn.close()
        return

    if not token:
        print("Error: YNAB token required. Set YNAB_TOKEN or use --token")
        print("  Get your token at: app.ynab.com → Account Settings → Developer Settings")
        sys.exit(1)

    # ── List budgets ──
    if args.list_budgets:
        list_budgets(token)
        return

    # ── List accounts ──
    if args.list_accounts:
        if not budget_id:
            print("Error: --budget-id required (or set YNAB_BUDGET_ID). Run --list-budgets first.")
            sys.exit(1)
        list_accounts(token, budget_id)
        return

    # ── Sync from YNAB ──
    if args.sync:
        if not budget_id or not account_id:
            print("Error: --budget-id and --account-id required for sync.")
            print("  Run --list-budgets and --list-accounts to find your IDs.")
            sys.exit(1)
        conn = init_db(args.db_path)
        try:
            print(f"\n🔄 Syncing transactions from YNAB...")
            count = sync_from_ynab(conn, token, budget_id, account_id, since_date=args.since_date)
            print(f"\n✓ Sync complete!")
            db_stats(conn)
        finally:
            conn.close()
        return

    # ── Crypto sync ──
    if args.crypto_sync:
        if not budget_id:
            print("Error: --budget-id required (or set YNAB_BUDGET_ID).")
            sys.exit(1)
        if not crypto_account_id:
            print("Error: --crypto-account-id required (or set YNAB_CRYPTO_ACCOUNT_ID).")
            print("  Run --list-accounts to find your crypto tracking account ID.")
            sys.exit(1)
        if not btc_xpub and not eth_address:
            print("Error: at least one of --btc-xpub or --eth-address required.")
            print("  BTC: set CRYPTO_BTC_XPUB (Ledger Live → Account → Edit → Advanced → xpub)")
            print("  ETH: set CRYPTO_ETH_ADDRESS (your 0x... Ethereum address)")
            sys.exit(1)
        crypto_sync(
            token, budget_id, crypto_account_id,
            btc_xpub=btc_xpub or None,
            eth_address=eth_address or None,
            dry_run=args.dry_run,
        )
        return

    # ── Brokerage sync (Interactive Brokers) ──
    if args.brokerage_sync:
        if not budget_id:
            print("Error: --budget-id required (or set YNAB_BUDGET_ID).")
            sys.exit(1)
        if not brokerage_account_id:
            print("Error: --brokerage-account-id required (or set YNAB_BROKERAGE_ACCOUNT_ID).")
            print("  Run --list-accounts to find your brokerage tracking account ID.")
            sys.exit(1)
        brokerage_sync(
            token, budget_id, brokerage_account_id, ibkr_base_url,
            ibkr_account_id=ibkr_account_id or None,
            dry_run=args.dry_run,
        )
        return

    # ── Watch mode ──
    if args.watch:
        if not budget_id or not account_id:
            print("Error: --budget-id and --account-id required for watch mode.")
            sys.exit(1)
        watch_folder(args.watch, token, budget_id, account_id)
        return

    # ── Cleanup stale '(pending)' memos ──
    if args.cleanup_pending_memos:
        if not budget_id or not account_id:
            print("Error: --budget-id and --account-id required to cleanup pending memos.")
            sys.exit(1)
        # Try to auto-detect a CSV for cross-reference (non-fatal if none found)
        csv_for_cleanup = None
        if args.csv_file:
            csv_for_cleanup = Path(args.csv_file).expanduser().resolve()
        else:
            latest = find_latest_revolut_csv(csv_dir)
            if latest:
                csv_for_cleanup = latest.resolve()
        cleanup_pending_memos(
            token, budget_id, account_id,
            csv_path=str(csv_for_cleanup) if csv_for_cleanup else None,
            dry_run=args.dry_run,
        )
        return

    # ── Reconcile from CSV running balance ──
    if args.reconcile:
        if not budget_id or not account_id:
            print("Error: --budget-id and --account-id required to reconcile.")
            sys.exit(1)
        csv_path = _resolve_csv_path()
        if csv_path is None:
            sys.exit(1)
        if not csv_path.exists():
            print(f"Error: File not found: {csv_path}")
            sys.exit(1)
        reconcile_from_csv(token, budget_id, account_id, str(csv_path), dry_run=args.dry_run)
        return

    # ── Import CSV ──
    csv_path = _resolve_csv_path()
    if csv_path is None:
        # No file given and no auto-detection succeeded (or declined)
        if not args.csv_file:
            parser.print_help()
        sys.exit(1)

    if not csv_path.exists():
        print(f"Error: File not found: {csv_path}")
        sys.exit(1)

    print(f"\n📄 Reading: {csv_path.name}")
    transactions = parse_revolut_csv(str(csv_path))

    if args.skip_pending:
        before = len(transactions)
        transactions = [t for t in transactions if t["cleared"] != "uncleared"]
        skipped = before - len(transactions)
        if skipped:
            print(f"   Skipped {skipped} pending transaction(s)")

    print(f"   Found {len(transactions)} transaction(s)")

    if not transactions:
        print("   Nothing to import.")
        return

    total_out = sum(t["amount"] for t in transactions if t["amount"] < 0) / 1000
    total_in = sum(t["amount"] for t in transactions if t["amount"] > 0) / 1000
    print(f"   Total outflow: {total_out:,.2f} CHF")
    print(f"   Total inflow:  {total_in:,.2f} CHF")

    if not budget_id or not account_id:
        if not args.dry_run:
            print("\nError: --budget-id and --account-id required to import.")
            print("  Run --list-budgets and --list-accounts to find your IDs.")
            sys.exit(1)

    conn = init_db(args.db_path)

    try:
        import_and_track(conn, token, budget_id, account_id, transactions, dry_run=args.dry_run)
        if not args.dry_run:
            print("\n✓ Import complete!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
