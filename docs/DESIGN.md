# YNAB Helper v2 — design

Status: implementation spec for the v2 rework (branch `rework/v2`). Every module in
`ynab_helper/` follows this document. Where this document and the legacy code disagree,
this document wins — except for the **compatibility constraints** in §2, which are absolute.

Background: a full review of v1 (October 2026) confirmed 55 defects, 2 critical. Most of
the data-integrity ones share one root cause: the local `transactions` table is used both
as the importer's ledger (CSV identity + what we pushed) and as a mirror of YNAB (written
by the web UI's sync), so each side overwrites the other's fields. v2 separates them.

---

## 1. Goals

1. **Never corrupt the user's YNAB data.** No downgrading reconciled/cleared transactions,
   no wiping user memos, no deleting live transactions, no duplicate creates, no silent
   partial imports. Every YNAB write is computed from YNAB's *current* state.
2. **Converge.** A failed or interrupted import leaves state from which the next run
   finishes the job. Local state advances only after YNAB confirmed a write.
3. **Structured results, no stdout scraping.** Library code never prints; it returns
   dataclasses. CLI, bot and web render them.
4. **One service layer** shared by CLI, bot and web, serialised per user by a lock.
5. **Secure by default**: authorization gate for every Telegram update (messages *and*
   callbacks), private chats only, HTML-escaped output, auth-before-body in the web, body
   size limits, strict CSP with self-hosted assets, non-root container.
6. **Tested.** pytest suite with stateful fakes for YNAB and Telegram; a regression test
   for every confirmed review finding; CI gates the auto-deploy.
7. Keep the "boring dependencies" philosophy: stdlib + pycryptodome + FastAPI/uvicorn/Jinja2.

Non-goals: new end-user features beyond what the fixes require; changing the deployment
topology (single container, Telegram long-polling, web UI in a thread).

---

## 2. Compatibility constraints (absolute)

Production runs `ghcr.io/cyrill-meier/ynab_helper:latest`, auto-deployed by Watchtower
within 5 minutes of a push to `main`. The server's compose file and Caddyfile are shared
with other apps and are **not** changed by this repo. Facts (from read-only inspection):

* Container: `container_name: revolut-ynab-bot`, no `command:` override (image CMD/ENTRYPOINT
  are used), no `user:` override, `TZ=Europe/Zurich`, `PYTHONUNBUFFERED=1`.
* Mounts: `/home/ubuntu/ynab-bot/bot_data -> /app/bot_data` (rw, **all files root-owned**),
  `/home/ubuntu/ynab-bot/bot.env -> /app/.env` (ro, mode 0600 root).
* Caddy proxies `bot.netcope.ch -> revolut-ynab-bot:8080` and adds its own CSP that allows
  `'self' 'unsafe-inline' https://cdn.tailwindcss.com`. The app may send a *stricter* CSP
  (browsers enforce the intersection) but must not need any new external origin.
* `bot.env` sets: TELEGRAM_BOT_TOKEN TELEGRAM_ADMIN_ID YNAB_TOKEN YNAB_BUDGET_ID
  YNAB_ACCOUNT_ID YNAB_CRYPTO_ACCOUNT_ID CRYPTO_BTC_XPUB CRYPTO_ETH_ADDRESS LOG_LEVEL
  LOG_FILE(=/app/bot_data/bot.log) WEB_UI_ENABLED WEB_UI_PUBLIC_URL WEB_UI_SECRET_KEY.

Therefore:

1. **Paths**: data dir `/app/bot_data` (env `BOT_DATA_DIR`, default `<repo root>/bot_data`),
   env file `/app/.env` (= `<repo root>/.env`), web on `0.0.0.0:8080`.
2. **Env vars**: every variable listed in §11 keeps its name and meaning.
3. **Entry**: the image must start the bot with no arguments. `python3 revolut_ynab_bot.py`
   and `python3 revolut_to_ynab.py ...` keep working (thin shims) because the README and
   operators use them via `docker exec`.
4. **Existing SQLite files** (schemas in §5.1) must be migrated in place, additively,
   idempotently, with an automatic backup first. Old tables are left in place (not dropped)
   so an emergency rollback to the v1 image still starts.
5. **import_id formats are byte-identical** to v1, because YNAB dedupes on them and
   ~6,600 existing YNAB transactions carry them:
   * Revolut rows: `YNAB:{milli}:{YYYY-MM-DD}:{occurrence}` where milli =
     `int(round((float(Amount) - float(Fee or 0)) * 1000))` (fee subtracted only if non-zero,
     float arithmetic exactly as v1), date = Started Date's date part, occurrence = 1-based
     counter per `(milli, date)` over rows in **file order**, counting only rows that passed
     the product filter and date/amount parsing (all states count, including REVERTED).
   * Adjustments: `CRYPTO:{delta_milli}:{local_today}:1`, `IBKR:{delta_milli}:{local_today}:1`,
     `RECON:{delta_milli}:{as_of}:1`.
   * Legacy local keys seen in production that v2 must tolerate as opaque strings:
     `YNAB:P:-…:date:n`, `acct:<uuid>:<uuid>[_date|_t_date]`, `ynab_manual:<uuid>[...]`.
6. **Payees/memos of adjustments**: `CSV Reconciliation` (cleared=`reconciled`),
   `Crypto Portfolio Adjustment` (cleared), `IBKR Portfolio Adjustment` (cleared).
7. **Telegram**: command names (§8.2) stay; callback data already sitting in chats
   (`st:auto:0|1`, `st:crypto`, `st:setup`, `st:close`, `dd:*`) must be handled or expired
   gracefully (answer the callback, remove the keyboard). The admin startup DM stays
   (it is the deploy notification).
8. **Web**: cookie name `rynab_session`, URL paths `/`, `/auth`, `/logout`, `/health`,
   `/app/{dashboard,transactions,reconcile,dedupe,settings,upload,accounts,spending,subscriptions}`
   and `/static/*`. JSON API shapes may change (the bundled frontend is the only client).
9. **Revolut export facts** (verified on a real export): columns `Type, Product, Started Date,
   Completed Date, Description, Amount, Fee, Currency, State, Balance`; rows are ordered by
   **Completed Date ascending** with PENDING/REVERTED rows (empty Completed Date and Balance)
   interleaved; Started Date is *not* monotonic. Therefore the positional `occurrence`
   can change between exports when a pending row completes — identity must come from the
   started timestamp, not the key (§6).

---

## 3. Package layout

```
ynab_helper/
  __init__.py            # __version__ = "2.0.0"
  __main__.py            # `python -m ynab_helper [bot|cli ...]` → dispatch
  version.py             # version line: "v2.0.0 (abc1234, 2026-10-16 20:00 UTC)" from BUILD_SHA/BUILD_DATE
  errors.py              # exception hierarchy (§4.1)
  config.py              # .env loading + typed settings + validation (§11)
  logging_setup.py       # stderr + RotatingFileHandler, noisy-logger levels
  money.py               # milliunit helpers, formatting, Decimal conversions
  net.py                 # shared urllib JSON HTTP helper with timeout/retry/backoff/redaction
  timeutil.py            # utc_now_iso(), local_today(), parse helpers
  locks.py               # per-user operation locks (Busy)
  container.py           # container entrypoint: root → chown data dir, load .env, drop to uid 10001, run
  healthcheck.py         # `python -m ynab_helper.healthcheck` for Docker HEALTHCHECK
  storage/
    db.py                # connect(), transaction(), migration runner (PRAGMA user_version), backup_db()
    tx_schema.py         # per-user transactions DB: DDL + migrations incl. legacy → v1 data migration
    users_schema.py      # bot_users.db: DDL + migrations
    ledger.py            # LedgerStore (import_ledger table)
    mirror.py            # MirrorStore (ynab_accounts/ynab_transactions/ynab_subtransactions + sync_meta)
    csv_files.py         # CsvFileStore (csv_files table: uploaded statements + import status)
    users.py             # UserStore (users table) + SessionStore (web_sessions table)
  ynab/
    client.py            # YnabClient (§4.2)
    models.py            # YnabAccount, YnabTransaction, YnabSubtransaction (+ from_api)
  revolut/
    parser.py            # parse_export() → ParsedExport; legacy import_id derivation
    files.py             # sanitize/store uploaded CSVs atomically; discovery for CLI
  importer/
    models.py            # LedgerEntry, PlannedAction, Plan, ImportResult …
    planner.py           # PURE: plan(export, ledger, mirror_view, options) → Plan
    executor.py          # apply(plan, client, stores) → ImportResult
  sync/
    ynab_sync.py         # sync_budget(): accounts + transactions delta → mirror
  balance/
    adjust.py            # post_balance_adjustment() shared by crypto/IBKR/reconcile
    reconcile.py         # preview/apply reconcile from a CSV
    crypto/
      btc.py             # base58check, SLIP-132 versions, BIP32 CKDpub, bech32, address scan
      eth.py             # JSON-RPC client with failover, ERC-20 table
      prices.py          # CoinGecko CHF prices (complete or raise)
      sync.py            # crypto_sync() → CryptoSyncResult
    ibkr.py              # IBKR client portal NAV → brokerage_sync()
  maintenance/
    cleanup.py           # cleanup_pending_memos() → CleanupResult
    dedupe.py            # dedupe scan / delete (scan ids, server-side)
  services.py            # UserContext + per-user operations used by CLI/bot/web (§7)
  cli.py                 # argparse CLI (legacy flags preserved) rendering results
  bot/
    telegram.py          # TelegramClient (typed errors, HTML, chunking by UTF-16 units)
    render.py            # HTML formatters for results (escaped)
    app.py               # Bot: poll loop, offset, backoff, SIGTERM, heartbeat, dispatcher, workers
    auth.py              # authorize(update) gate
    commands.py          # command registry → dispatch, /help, setMyCommands
    handlers/            # admin.py onboarding.py settings.py login.py importing.py
                         # reconcile.py dedupe.py status.py crypto.py
    main.py              # bot entry: config, logging, migrations, admin auto-register, web thread, run
  web/
    app.py               # make_app(config, deps) → FastAPI; routers; middleware
    config.py            # WebConfig (strict parsing, errors collected, never raises)
    auth.py              # login tokens / sessions / CSRF (HMAC with WEB_UI_SECRET_KEY)
    middleware.py        # pure ASGI: body-size limit, security headers, IP allowlist
    analytics.py         # PURE spending/categories/subscriptions over mirror rows
    server.py            # serve_in_thread(): uvicorn in a daemon thread with stop handle
    templates/           # Jinja2 templates (no inline scripts/styles)
    static/              # app.css (built), js/*.js (ES modules), favicon.svg
revolut_to_ynab.py       # shim → ynab_helper.cli.main()
revolut_ynab_bot.py      # shim → ynab_helper.bot.main.main()
tests/                   # pytest; mirrors package; fakes/ and fixtures/
docs/                    # DESIGN.md (this), ARCHITECTURE.md, OPERATIONS.md
scripts/build_css.sh     # pinned Tailwind standalone CLI (v3.4.17) with sha256 check
```

Code style: Python 3.12, type hints on public functions, `from __future__ import annotations`,
dataclasses (`slots=True` where convenient), module-level `log = logging.getLogger(__name__)`,
no `print()` outside `cli.py`, no `sys.exit()` outside entrypoints, ruff-clean
(rules `E,F,W,I,B,UP,SIM,RUF` with sensible ignores, line length 100).

---

## 4. Cross-cutting building blocks

### 4.1 errors.py

```python
class YnabHelperError(Exception):            # base; .user_message: str (safe to show to users)
class ConfigError(YnabHelperError)
class ExternalServiceError(YnabHelperError): # .service: str, .retriable: bool, .status: int|None
class YnabError(ExternalServiceError):       # .status, .error_id, .detail
class YnabAuthError(YnabError)               # 401
class YnabNotFound(YnabError)                # 404
class YnabRateLimited(YnabError)             # 429, .retry_after: float|None
class YnabValidationError(YnabError)         # 400/409/422
class YnabServerError(YnabError)             # 5xx
class YnabNetworkError(YnabError)            # timeouts, DNS, connection reset
class TelegramError(ExternalServiceError)    # .error_code, .description, .retry_after
class CsvError(YnabHelperError)              # invalid/unsupported statement (user_message explains)
class Busy(YnabHelperError)                  # per-user lock held; .operation
class StaleStateError(YnabHelperError)       # confirm token / scan id expired or mismatch
```
Exception messages must never contain tokens/keys (net.py redacts).

### 4.2 YnabClient (ynab/client.py)

```python
class YnabClient:
    def __init__(self, token: str, *, base_url="https://api.ynab.com/v1", timeout=30.0,
                 transport: Transport | None = None, max_retries=2): ...
    def get_user(self) -> dict
    def get_budgets(self) -> list[dict]
    def get_budget_currency(self, budget_id) -> str | None          # GET /budgets/{b}/settings → currency_format.iso_code
    def get_accounts(self, budget_id, last_knowledge: int | None = None) -> tuple[list[YnabAccount], int]
    def get_account(self, budget_id, account_id) -> YnabAccount
    def get_transactions(self, budget_id, *, account_id=None, since_date=None,
                         last_knowledge: int | None = None) -> tuple[list[YnabTransaction], int]
    def create_transactions(self, budget_id, txs: list[dict]) -> CreateResult   # POST, chunked by 100
    def update_transactions(self, budget_id, updates: list[dict]) -> list[YnabTransaction]  # PATCH /budgets/{b}/transactions (bulk, each has "id")
    def update_transaction(self, budget_id, tx_id, fields: dict) -> YnabTransaction        # PUT /budgets/{b}/transactions/{id}
    def delete_transaction(self, budget_id, tx_id) -> YnabTransaction
```
* `Transport` = callable `(method, url, headers, body_bytes|None, timeout) -> (status, headers, body_bytes)`;
  default uses urllib. Tests inject `tests/fakes/fake_ynab.py`.
* Retries (with jittered exponential backoff, honour `Retry-After` up to 10 s) only for
  network errors and 5xx, max 2 retries. **429 is not retried** inside a request (YNAB's limit is
  200/hour; waiting would block the bot) — raise `YnabRateLimited`. Map statuses to the typed errors.
* Read `X-Rate-Limit` header (`"36/200"`) into `client.rate_limit_used` for logging.
* `CreateResult(created: list[YnabTransaction], duplicate_import_ids: list[str])`.
* User-Agent `ynab-helper/<version>`.

### 4.3 net.py
`request_json(method, url, *, headers=None, json_body=None, timeout=20, retries=3,
retry_statuses=(429,500,502,503,504), backoff=1.0, redact=()) -> Any` raising
`ExternalServiceError(service=...)`. Used by Blockstream, ETH RPC, CoinGecko, IBKR, Telegram.
Every call has a timeout. Socket timeouts are caught and retried like network errors.

### 4.4 storage/db.py
* `connect(path, *, readonly=False) -> sqlite3.Connection`: `check_same_thread=False`
  is **not** used except where a connection is deliberately shared (bot users DB guarded by a lock);
  `PRAGMA journal_mode=WAL`, `busy_timeout=10000`, `foreign_keys=ON`, `row_factory=sqlite3.Row`,
  `isolation_level=None` (autocommit; explicit transactions only).
* `transaction(conn)` context manager: `BEGIN IMMEDIATE` … `COMMIT` / `ROLLBACK` on exception.
  **Rule: no network I/O inside a transaction.**
* `migrate(conn, migrations: list[Migration], *, db_path, backup_dir)`: reads `PRAGMA user_version`;
  if any migration is pending, first `backup_db(db_path, backup_dir, tag=f"pre-v{target}")`
  (sqlite3 backup API → `bot_data/backups/<name>.<UTC stamp>.pre-vN.db`, keep newest 10 per DB),
  then applies each step in its own transaction and bumps user_version. Steps are idempotent
  (safe on DBs where columns already exist).
* `os.umask(0o077)` is set by entrypoints so new files are 0600.

### 4.5 locks.py
```python
class UserLocks:
    def operation(self, key: str, name: str) -> ContextManager[None]   # non-blocking; raises Busy(current_name)
LOCKS = UserLocks()   # process-wide singleton shared by bot and web
```
Key = `str(telegram_id)` for bot/web users, `db_path` for CLI.

### 4.6 logging_setup.py
`setup_logging(level="INFO", log_file: str | None)`: root logger → stderr handler (format with
time, level, logger); optional `RotatingFileHandler(log_file, maxBytes=5 MB, backupCount=5)`;
`python_multipart`, `urllib3`, `asyncio`, `uvicorn.access` at WARNING. Idempotent.
Transaction-level audit lines ("created", "updated", "deleted" with date/amount/payee) are
logged at INFO by the executor under logger `ynab_helper.audit` — the user relies on bot.log
as an audit trail.

---

## 5. Data model

### 5.1 Existing (v1) schemas — must be readable and migrated

`bot_users.db` (WAL, user_version 0):
```sql
CREATE TABLE users (telegram_id INTEGER PRIMARY KEY, chat_id INTEGER, username TEXT, first_name TEXT,
  ynab_token TEXT, budget_id TEXT, budget_name TEXT, account_id TEXT, account_name TEXT,
  crypto_account_id TEXT, crypto_account_name TEXT, btc_xpub TEXT, eth_address TEXT,
  state TEXT NOT NULL DEFAULT 'pending', temp_data TEXT, created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL, auto_approve INTEGER NOT NULL DEFAULT 1);
CREATE TABLE web_sessions (token_hash TEXT PRIMARY KEY, telegram_id INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('login','session')), created_at REAL NOT NULL,
  expires_at REAL NOT NULL, user_agent TEXT, ip TEXT, tg_chat_id INTEGER, tg_message_id INTEGER);
CREATE INDEX idx_web_sessions_expires ON web_sessions(expires_at);
CREATE INDEX idx_web_sessions_telegram ON web_sessions(telegram_id);
```
(Older DBs may lack `auto_approve`, `tg_chat_id`, `tg_message_id`.)
users.state values in the wild: pending, approved (legacy), awaiting_token, awaiting_budget,
awaiting_account, ready, denied, awaiting_crypto_account, awaiting_crypto_btc, awaiting_crypto_eth.

`transactions_<telegram_id>.db` (WAL, user_version 0):
```sql
CREATE TABLE transactions (import_id TEXT PRIMARY KEY, date TEXT NOT NULL, amount INTEGER NOT NULL,
  payee_name TEXT, memo TEXT, cleared TEXT, state TEXT, ynab_tx_id TEXT, account_id TEXT,
  category_name TEXT, approved INTEGER, deleted INTEGER DEFAULT 0, source TEXT DEFAULT 'revolut',
  imported_at TEXT NOT NULL, updated_at TEXT, started_at TEXT);
CREATE TABLE sync_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE accounts (account_id TEXT PRIMARY KEY, name TEXT, type TEXT, classification TEXT,
  on_budget INTEGER NOT NULL DEFAULT 1, closed INTEGER NOT NULL DEFAULT 0, deleted INTEGER NOT NULL DEFAULT 0,
  note TEXT, balance INTEGER, cleared_balance INTEGER, uncleared_balance INTEGER, transfer_payee_id TEXT,
  last_seen_at TEXT, first_seen_at TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
```
(Older DBs may lack `started_at`, `account_id`, `category_name`, `approved`, `source`, `deleted`,
`updated_at`, and the `accounts` table.) Production row shapes: `source='revolut'` rows keyed
`YNAB:…` with `state ∈ {COMPLETED, PENDING, REVERTED, SUPERSEDED, DELETED_IN_YNAB, DUPLICATE, REKEYED}`
and `cleared ∈ {cleared, uncleared, reconciled}` (note: 174 rows are `PENDING`+`reconciled` because
v1's two syncs disagreed); `source='ynab'` rows keyed `acct:…`, `ynab_manual:…`, `RECON:…`.
The CLI's DB (`~/.revolut_to_ynab.db`) has the same schema.

### 5.2 v2 transactions DB (user_version 1)

New tables (legacy `transactions` table is kept untouched as a frozen archive; `accounts`
and `sync_meta` are reused):

```sql
-- Importer ledger: what we know about each Revolut row we have seen, keyed per account.
CREATE TABLE import_ledger (
  account_id      TEXT NOT NULL,          -- YNAB account the row belongs to ('' = unclaimed legacy row)
  key             TEXT NOT NULL,          -- ledger key; = CSV-derived import_id for rows created by key
  ynab_import_id  TEXT,                   -- import_id YNAB stores for the linked transaction (immutable in YNAB)
  ynab_tx_id      TEXT,                   -- YNAB transaction id (NULL = not linked)
  status          TEXT NOT NULL,          -- see below
  csv_date        TEXT NOT NULL,          -- YYYY-MM-DD, Started Date as rendered by the latest export seen
  csv_amount      INTEGER NOT NULL,       -- milliunits (fee included), from the latest export seen
  csv_payee       TEXT,                   -- Revolut Description (never overwritten by YNAB data)
  csv_type        TEXT,
  csv_state       TEXT NOT NULL,          -- COMPLETED | PENDING | REVERTED (last seen)
  started_at      TEXT,                   -- 'YYYY-MM-DD HH:MM:SS' as rendered by the latest export seen
  pushed_cleared  TEXT,                   -- 'cleared'|'uncleared' we last successfully wrote (or observed at link time)
  pushed_amount   INTEGER,                -- amount we last successfully wrote
  pushed_date     TEXT,                   -- date we last successfully wrote
  identity        TEXT NOT NULL DEFAULT 'csv',  -- 'csv' (csv_* fields from a CSV) | 'legacy' (migrated, csv_payee may be YNAB's payee)
  rekeyed_to      TEXT,                   -- for status REKEYED: the key that took over
  last_error      TEXT,
  first_seen_at   TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  PRIMARY KEY (account_id, key)
);
CREATE INDEX idx_ledger_tx ON import_ledger(ynab_tx_id);
CREATE INDEX idx_ledger_date ON import_ledger(account_id, csv_date);

-- status values:
--   ACTIVE           linked to a live YNAB transaction (ynab_tx_id NOT NULL)
--   UNLINKED         we believe YNAB has it (created or duplicate) but the id is unknown; resolved via mirror
--   REVERTED         Revolut reverted it; removed from YNAB (or never created)
--   SUPERSEDED       pending that settled under different rows; removed from YNAB by us
--   DELETED_IN_YNAB  the user (or something else) deleted it in YNAB; we never recreate it
--   REKEYED          tombstone: identity moved to `rekeyed_to` (legacy v1 re-key rows, or v2 renumbering)

-- YNAB mirror, written only by sync/ynab_sync.py.
CREATE TABLE ynab_transactions (
  id TEXT PRIMARY KEY, budget_id TEXT NOT NULL, account_id TEXT NOT NULL, date TEXT NOT NULL,
  amount INTEGER NOT NULL, payee_id TEXT, payee_name TEXT, memo TEXT,
  cleared TEXT NOT NULL,                 -- cleared | uncleared | reconciled
  approved INTEGER NOT NULL, flag_color TEXT, category_id TEXT, category_name TEXT,
  transfer_account_id TEXT, transfer_transaction_id TEXT, matched_transaction_id TEXT,
  import_id TEXT, deleted INTEGER NOT NULL DEFAULT 0, synced_at TEXT NOT NULL);
CREATE INDEX idx_ytx_acct_date ON ynab_transactions(budget_id, account_id, date);
CREATE INDEX idx_ytx_import ON ynab_transactions(account_id, import_id);
CREATE TABLE ynab_subtransactions (
  id TEXT PRIMARY KEY, transaction_id TEXT NOT NULL, amount INTEGER NOT NULL, memo TEXT,
  payee_id TEXT, payee_name TEXT, category_id TEXT, category_name TEXT,
  transfer_account_id TEXT, deleted INTEGER NOT NULL DEFAULT 0);
CREATE INDEX idx_ysub_parent ON ynab_subtransactions(transaction_id);
-- accounts table: add column budget_id TEXT (NULL for legacy rows until next sync)

-- Uploaded statements.
CREATE TABLE csv_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT, stored_path TEXT NOT NULL,
  original_name TEXT, sha256 TEXT NOT NULL, uploaded_at TEXT NOT NULL, source TEXT NOT NULL, -- telegram|web|cli|legacy
  currency TEXT, row_count INTEGER, window_start TEXT, window_end TEXT,
  status TEXT NOT NULL,                  -- stored | imported | partial | failed | legacy
  imported_at TEXT, result_json TEXT);
```
`sync_meta` keys used by v2: `mirror_knowledge:<budget_id>` (budget-wide transactions delta),
`accounts_knowledge:<budget_id>`, `budget_currency:<budget_id>`, `last_sync_at:<budget_id>`.

### 5.3 Migration legacy → v1 (tx DB)

Runs once (user_version 0 → 1) inside one transaction after an automatic backup. Input: the
legacy tables (whatever columns exist) and `default_account_id` (the user's configured account,
or `''` if unknown).

* Create the new tables/indexes; `ALTER TABLE accounts ADD COLUMN budget_id` if missing.
* For every legacy row with `source='revolut'` (or NULL source and key starting `YNAB:`):
  * `account_id` = row.account_id or default_account_id or `''`; `key` = row.import_id;
    `ynab_import_id` = row.import_id (corrected after first sync, below); `ynab_tx_id` = row.ynab_tx_id.
  * `status`: REVERTED/SUPERSEDED/DELETED_IN_YNAB/REKEYED → same; `deleted=1` otherwise → DELETED_IN_YNAB;
    otherwise ACTIVE if ynab_tx_id else UNLINKED (covers DUPLICATE).
  * `csv_state`: legacy state if in (COMPLETED, PENDING, REVERTED); else COMPLETED if cleared in
    (cleared, reconciled) else PENDING. A row whose cleared is cleared/reconciled is COMPLETED even
    if state says PENDING.
  * `csv_date`=date, `csv_amount`=amount, `csv_payee`=payee_name, `started_at`=started_at,
    `pushed_cleared` = 'cleared' if cleared in (cleared, reconciled) else 'uncleared',
    `pushed_amount`=amount, `pushed_date`=date, `identity`='legacy', timestamps from the row.
  * For REKEYED rows, set `rekeyed_to` to the key of the non-REKEYED row with the same ynab_tx_id, if any.
* Rows with `source='ynab'` are not migrated (the mirror is rebuilt from YNAB by the first sync,
  which runs with no server knowledge = full fetch).
* Register the newest file in `csv_cache/<id>/` (if any) in `csv_files` with status `legacy`.
* After the **first successful mirror sync** (any time later), `ledger.refresh_from_mirror()`
  sets `ynab_import_id` from the mirror for linked rows and resolves UNLINKED rows whose key or
  ynab_import_id matches a mirror transaction in the same account (→ ACTIVE, or DELETED_IN_YNAB
  if the mirror row is deleted). Ledger rows with `account_id=''` are claimed by the account
  being imported on the next import (v1 behaviour).

`bot_users.db` migration v0 → v1: add missing columns idempotently; map legacy state
`approved` → `awaiting_token`; delete expired web_sessions. (Token hashing changes to HMAC in
v2, so existing sessions/login tokens simply stop validating — users re-login once.)

---

## 6. Importer

### 6.1 Parsing (revolut/parser.py)

```python
@dataclass(frozen=True, slots=True)
class RevolutRow:
    line_no: int                 # 1-based data row number in the file
    key: str                     # legacy import_id (§2.5)
    type: str; product: str; description: str; payee: str   # payee = (Description or Type or "Unknown")[:100]
    started_at: datetime         # naive, as rendered
    completed_at: datetime | None
    date: str                    # started_at.date().isoformat()
    amount: int                  # milliunits incl. fee (legacy float formula)
    fee: float                   # as parsed (for the memo)
    currency: str
    state: str                   # raw Revolut State, upper-cased/stripped
    balance: int | None          # milliunits of the Balance column (None if empty)
    memo: str | None             # legacy memo: parts joined by " | ": Type (unless "Card Payment"),
                                 # "(pending)" if PENDING, f"Fee: {fee}" if fee; None if empty; [:200]

@dataclass
class ParsedExport:
    rows: list[RevolutRow]              # tracked product (Product == "Current" or empty) only, file order
    currency: str | None                # the single currency of `rows`; CsvError if mixed
    window: tuple[str, str] | None      # (min date, max date) over rows
    skipped: list[SkippedRow]           # (line_no, reason) — bad date/amount, non-finite, absurd values
    other_products: dict[str, int]      # Product → count (skipped silently but reported)
    filename_window: tuple[str, str] | None
    sha256: str

def parse_export(source: Path | bytes, *, filename: str | None = None) -> ParsedExport
def looks_like_revolut_csv(head: bytes) -> bool   # header must contain Started Date, Completed Date, Description, Amount, State
```
* Read with `utf-8-sig`; tolerate `\r\n`; max 25 MB (CsvError above). Rows with non-finite or
  |amount| > 10^9 are skipped and reported. The `occurrence` counter must match v1 exactly (see §2.5).
* State allowlist: COMPLETED → cleared, PENDING → uncleared, REVERTED → special; any other
  state (DECLINED, FAILED, …) is **skipped and reported**, but still counted for `occurrence`
  (v1 counted them).
* A golden test compares keys/amounts/dates/memos against a frozen copy of the v1 parser
  (`tests/legacy/v1_revolut_to_ynab.py`) on fixtures and randomized inputs.

### 6.2 Planning (importer/planner.py) — pure, no I/O

```python
@dataclass
class PlanOptions:
    account_id: str
    approved: bool                 # user's auto_approve
    skip_pending: bool = False     # only affects CREATE of PENDING rows; never the sweep
    allow_sweep: bool = True       # superseded sweep enabled (False e.g. when export is older, see below)

def plan_import(export: ParsedExport, ledger: list[LedgerEntry], ynab: MirrorView,
                opts: PlanOptions) -> Plan
```
`MirrorView` gives, for the account: `by_id(tx_id) -> YnabTx | None` (incl. deleted rows) and
`by_import_id(import_id) -> YnabTx | None`, plus `used_import_ids` (all import_ids present in the
account, deleted included). It is built from the mirror right after a fresh delta sync.

**Step 1 – learn export offsets.** For each CSV row whose `key` equals an ACTIVE/UNLINKED ledger
entry's key where both have `started_at`, compute `d = row.started_at − entry.started_at`. If `d`
is "timezone-shaped" (multiple of 15 minutes, |d| ≤ 14 h) count it. `offsets = {0} ∪ {d observed ≥ 1×}`.

**Step 2 – match CSV rows to ledger entries** (each side matched at most once; consider ledger
entries of this account in every status except REKEYED). In order:

1. *Timestamp identity*: row ↔ entry with equal `amount` (row.amount == entry.csv_amount),
   both with `started_at`, and `row.started_at − entry.started_at ∈ offsets`. Prefer d = 0, then
   the most frequent offset; ties broken by key equality, then file order.
2. *Key identity (legacy)*: row ↔ entry with the same key where the entry has no `started_at`
   (legacy rows) — or both have started_at and step 1 failed only because amounts differ? **No**:
   a key match with both timestamps present but not offset-compatible is a *renumbering collision*
   and is NOT a match.
3. *Legacy date shift*: unmatched row ↔ unmatched entry without `started_at`, same amount, same
   payee (row.payee == entry.csv_payee, case-insensitive, trimmed), dates differ by exactly one
   day, the entry's key not present among CSV keys, **both dates inside the export window**, the
   row's started time within 3 h of midnight, and exactly one candidate on each side.
4. *Settlement* (a pending that settled at a different amount, possibly re-dated): unmatched row
   (COMPLETED or PENDING) ↔ unmatched entry with `csv_state == PENDING`, status ACTIVE, same payee
   (case-insensitive, trimmed), |date difference| ≤ 1 day, amounts differ, the entry's date inside
   the export window, exactly one candidate on each side.

Every match backfills `started_at`, `csv_*` fields from the row (identity becomes 'csv').

**Step 3 – actions for each matched (row, entry)**. Let `y = ynab.by_id(entry.ynab_tx_id)`.

* entry.status REKEYED: never matched (skipped in step 2).
* entry.status DELETED_IN_YNAB → no YNAB write; count as `ignored_deleted`.
* entry.status REVERTED or SUPERSEDED:
  * row.state REVERTED → nothing.
  * row.state COMPLETED/PENDING (it's live again) → **CREATE** a new YNAB transaction with a fresh
    import_id (§6.4) and relink the entry (status ACTIVE). Reason `reappeared`.
* entry.status UNLINKED → resolve: `y = ynab.by_import_id(entry.ynab_import_id or entry.key)` in
  this account. Found & not deleted → LINK (local only), then continue as ACTIVE with y. Found &
  deleted → mark DELETED_IN_YNAB. Not found → CREATE (POST with the entry's ynab_import_id/key;
  a duplicate response is resolved after the post-create sync, §6.3).
* entry.status ACTIVE:
  * `y` missing or `y.deleted` → mark DELETED_IN_YNAB (local only), report. Never recreate.
  * `y.account_id != account` → report "moved in YNAB", no write.
  * row.state REVERTED → if `y.cleared == "uncleared"` → **DELETE** (reason `reverted`);
    else report "reverted in Revolut but cleared in YNAB — left alone". csv_state := REVERTED.
  * row.state PENDING → if entry.csv_state == COMPLETED (an older export) → nothing (no downgrade).
    Else amount check below; no cleared change.
  * row.state COMPLETED → fields to PATCH:
    * `cleared = "cleared"` iff `y.cleared == "uncleared"`. **Never** send cleared when YNAB is
      cleared or reconciled. **Never** move to uncleared.
    * `memo`: only if `y.memo` contains the `(pending)` marker → `strip_pending_marker(y.memo)`
      (removes the marker token and its ` | ` separator, preserving everything else the user wrote).
  * Amount (any row state): if `row.amount != entry.csv_amount` (CSV changed: settlement match or
    FX re-quote): if `y.amount == (entry.pushed_amount if not None else entry.csv_amount)` and `y`
    has no subtransactions and is not reconciled → PATCH `amount = row.amount`; elif `y.amount ==
    row.amount` → nothing; else → report conflict ("amount edited in YNAB"), no amount write.
  * Date: if `row.date != entry.csv_date` (timezone re-render or settlement re-date) and
    `y.date == (entry.pushed_date or entry.csv_date)` and `y.cleared != "reconciled"` and no
    subtransactions → PATCH `date = row.date` (v1 re-dated on timezone shifts; keep that).
    Otherwise update only the ledger.
  * If fields is empty → local-only ledger refresh (csv_* / started_at), not a YNAB write.
  * `approved`, `payee`, `category` are never sent on updates.

**Step 4 – unmatched CSV rows**:
* state REVERTED → skip (`reverted_never_imported`); other non-allowlisted states → skip & report;
  state PENDING and `skip_pending` → skip (`pending_skipped`).
* else **CREATE** `{account_id, date, amount, payee_name, memo, cleared: cleared|uncleared,
  approved: opts.approved, import_id}` with import_id per §6.4; new ledger entry key = the chosen
  import_id.

**Step 5 – superseded sweep** (only if `opts.allow_sweep`): every unmatched ledger entry with
status ACTIVE, `csv_state == PENDING`, `csv_date` inside the export window, `y` present, not
deleted, `y.cleared == "uncleared"` → **DELETE** (reason `superseded`). If YNAB shows it cleared
or reconciled → report "pending vanished from Revolut but cleared in YNAB — left alone".
Unmatched COMPLETED entries are never deleted (reported only when inside the window and the
export is complete — `vanished`).

`allow_sweep` is set by the service: False when the export's window end is earlier than the
newest `csv_date` among this account's ACTIVE ledger entries minus 1 day (an older export).

**Plan** = ordered list of `PlannedAction(kind ∈ {CREATE, LINK, UPDATE, DELETE, LEDGER},
entry_key, row, ynab_tx_id, fields, reason, before, after)` + `warnings` + counters
(`unchanged`, `ignored_deleted`, `skipped_rows: list[(row, reason)]`).

### 6.3 Execution (importer/executor.py)

`apply_plan(plan, client, ledger_store, mirror_store, *, budget_id, account_id, dry_run) -> ImportResult`

1. LEDGER/LINK actions → local only (one transaction).
2. CREATE → bulk POST in chunks of 100. For each created transaction: ledger entry ACTIVE with
   ynab_tx_id, ynab_import_id, pushed_* (commit per chunk, after the HTTP call returned).
   `duplicate_import_ids` → after all chunks, run one mirror delta sync and resolve each by
   import_id in this account → LINK (or DELETED_IN_YNAB if the mirror row is deleted, or UNLINKED
   with a warning if not found). Resolved duplicates then get the same UPDATE treatment as ACTIVE
   rows (computed from the now-known YNAB state) within the same run.
3. UPDATE → bulk `PATCH /budgets/{b}/transactions` in chunks of 50 (`[{id, cleared?, memo?, amount?, date?}]`).
   On a 4xx for the bulk request, fall back to per-transaction `PUT` to isolate the failing row.
   Ledger updated only for transactions YNAB returned. 404 → status DELETED_IN_YNAB.
4. DELETE → per transaction. Success or 404 → status REVERTED/SUPERSEDED (404 still counts as
   removed). Other errors → failure recorded, ledger unchanged.
5. **Error model**: per-action failures are collected (`ImportResult.failures`) and execution
   continues. `YnabRateLimited` stops all further YNAB writes: remaining actions are reported as
   `not_applied` and `result.rate_limited = True`. `YnabAuthError` aborts (raised).
6. Nothing is written to YNAB or the ledger when `dry_run` (the mirror sync still runs; it is a cache).
7. Audit log line per successful YNAB write (`ynab_helper.audit`).

```python
@dataclass
class TxRef: date: str; amount: int; payee: str; ynab_tx_id: str | None = None
@dataclass
class Change: tx: TxRef; fields: dict[str, tuple[Any, Any]]; reason: str   # field → (old, new)
@dataclass
class Removal: tx: TxRef; reason: str            # reverted | superseded
@dataclass
class Failure: action: str; tx: TxRef | None; error: str
@dataclass
class ImportResult:
    dry_run: bool; account_id: str; currency: str | None; window: tuple[str, str] | None
    rows_total: int; rows_pending: int; rows_reverted: int
    created: list[TxRef]; linked: list[TxRef]; updated: list[Change]; removed: list[Removal]
    unchanged: int; ignored_deleted: int
    skipped_rows: list[tuple[int, str]]          # (line_no, reason)
    warnings: list[str]; failures: list[Failure]; not_applied: int; rate_limited: bool
    @property
    def has_changes(self) -> bool; @property
    def ok(self) -> bool                         # no failures, not rate limited
    def to_dict(self) -> dict                    # JSON-safe (web, csv_files.result_json)
```

### 6.4 Choosing YNAB import_ids for creates
For a row being created: candidate = `row.key`. If the candidate is already used in this account
(by any ledger entry's key/ynab_import_id, any status, or by `ynab.used_import_ids`), use
`YNAB:{amount}:{date}:{n}` with the smallest `n ≥ 1` not used. Ledger key = chosen import_id.
(Same rule for re-creating REVERTED/SUPERSEDED rows.) Max length 36 is asserted.

### 6.5 YNAB → mirror sync (sync/ynab_sync.py)
`sync_budget(client, mirror_store, budget_id, *, full=False) -> SyncResult(accounts_changed, tx_changed, server_knowledge)`
* `GET /budgets/{b}/accounts?last_knowledge_of_server` → upsert `accounts` (classification:
  cash for checking/savings/cash, credit for creditCard/lineOfCredit/*Debt*/loans, tracking for
  otherAsset/otherLiability, else by on_budget), set budget_id.
* `GET /budgets/{b}/transactions?last_knowledge_of_server` → upsert `ynab_transactions` (deleted
  flag included), replace subtransactions per parent. One knowledge slot per budget.
* Also refresh `budget_currency:<b>` once per day (GET settings).
* Throttle: the service skips a non-forced sync if the last one was < 30 s ago.
* After a sync, call `ledger_store.refresh_from_mirror(account_id, mirror_view)` (§5.3).

### 6.6 Balance adjustments, reconcile, crypto, IBKR
* `post_balance_adjustment(client, budget_id, account_id, *, target_milli, current_milli, payee,
  memo, prefix, date, cleared, dry_run) -> AdjustmentResult(outcome ∈ {in_sync, created,
  duplicate, dry_run}, delta, ynab_tx_id)`. |delta| < 10 → in_sync. import_id
  `f"{prefix}:{delta}:{date}:1"` (legacy). Duplicate → outcome duplicate (not an error).
* **Valuation syncs (crypto, IBKR) compare against the account's working `balance`**
  (cleared + uncleared), not cleared_balance. Missing balance field → raise.
* **Reconcile** is two-step:
  `reconcile_preview(ctx) -> ReconcilePreview(csv_file_id, csv_name, as_of, csv_balance, ynab_cleared_as_of,
  ynab_cleared_now, cleared_after_as_of, delta, currency, problems: list[str], token)` and
  `reconcile_apply(ctx, token) -> ReconcileResult`. Rules: the CSV must be the newest statement
  for the account with status `imported` (refuse `failed`/`partial`/`stored`/`legacy` with a
  clear message); balance = Balance column of the COMPLETED tracked-product row with the latest
  Completed Date (ties → last in file); currency must match the budget currency;
  `ynab_cleared_as_of = account.cleared_balance − Σ(cleared/reconciled mirror tx in the account dated
  after as_of)`; `delta = csv_balance − ynab_cleared_as_of`. `token` = HMAC-free opaque id stored
  in memory with (csv sha256, delta, as_of) and 10-min expiry; apply recomputes and refuses with
  `StaleStateError` if anything changed. The adjustment is `RECON:{delta}:{as_of}:1`, payee
  `CSV Reconciliation`, cleared `reconciled`, dated as_of. (It does not lock other transactions —
  messages must not claim that.)
* **Crypto**: btc.py implements base58check (checksum, 78-byte payload, known version bytes:
  xpub/ypub/zpub mainnet; reject private/testnet), on-curve check, SLIP-132 script type
  (xpub → scan P2PKH, P2SH-P2WPKH and P2WPKH and sum; ypub → P2SH-P2WPKH; zpub → P2WPKH),
  BIP32 CKDpub for m/0/i and m/1/i, gap limit 20, Blockstream with timeouts/backoff. Single
  addresses (bech32 v0/v1 incl. bech32m, base58check P2PKH/P2SH) validated. Amounts as integers
  (sats/wei/raw) and `Decimal` for CHF; milliunits via `Decimal.quantize(ROUND_HALF_EVEN)`.
  `prices.py` returns a price for **every** requested symbol or raises. `crypto_sync` refuses to
  post if any holding with amount > 0 has no positive finite price, or if an xpub scan found zero
  used addresses while YNAB's balance is non-zero (unless `allow_zero=True`).
  `CryptoSyncResult(holdings[(symbol, amount Decimal, price Decimal, value_milli)], portfolio_milli,
  ynab_before_milli, adjustment: AdjustmentResult)`.
* **IBKR**: verify TLS except when the host is localhost/127.0.0.1/::1; call `/portfolio/accounts`
  before `/portfolio/{id}/*`; NAV ≤ 0 → never post.

### 6.7 Maintenance
* `cleanup_pending_memos(ctx, dry_run)`: for mirror transactions in the account with cleared in
  (cleared, reconciled) whose memo contains the `(pending)` marker → PATCH memo only (never cleared).
  Uncleared rows are never touched. Result counts.
* Dedupe: `dedupe_scan(ctx) -> DedupeScan(scan_id, csv_name, window, candidates[...])` where
  candidates = mirror transactions in the account, not deleted, import_id starting `YNAB:`, date in
  the **parsed rows'** window (not the filename), and whose id is not linked from any ACTIVE ledger
  entry. The scan is stored server-side per user (10-min TTL). `dedupe_delete(ctx, scan_id, ids)`
  accepts only ids from that scan (max 200), deletes one by one, reports per-id failures, marks
  matching ledger entries DELETED_IN_YNAB.

---

## 7. Services (services.py)

```python
@dataclass(frozen=True)
class UserContext:
    key: str                     # lock key (telegram id or db path)
    ynab_token: str; budget_id: str; account_id: str
    db_path: Path; csv_dir: Path | None
    auto_approve: bool = True
    telegram_id: int | None = None
    crypto_account_id: str | None = None; btc_xpub: str | None = None; eth_address: str | None = None

def import_csv(ctx, csv_path_or_bytes, *, original_name, source, dry_run=False, skip_pending=False,
               client: YnabClient | None = None) -> ImportResult
def sync(ctx, *, force=False) -> SyncResult | None
def reconcile_preview(ctx) -> ReconcilePreview ; def reconcile_apply(ctx, token) -> ReconcileResult
def dedupe_scan(ctx) -> DedupeScan ; def dedupe_delete(ctx, scan_id, ids) -> DedupeDeleteResult
def cleanup_pending(ctx, *, dry_run=False) -> CleanupResult
def crypto_sync(ctx, *, dry_run=False) -> CryptoSyncResult
def account_status(ctx) -> StatusInfo     # balances from mirror (after throttled sync), last CSV, ledger counts
```
* Every mutating operation runs under `LOCKS.operation(ctx.key, name)`; contention → `Busy`.
* `import_csv`: store the CSV (revolut/files.py: server-generated name `csv_cache/<key>/<UTC stamp>_<8 hex>.csv`,
  atomic write, original name kept as metadata, keep the newest 5 files), parse, validate currency
  against the budget currency (refuse mismatch), sync mirror, load ledger, claim `''` ledger rows,
  compute allow_sweep, plan, apply, record `csv_files` status (`imported` if result.ok, `partial`
  if some actions failed/not applied, `failed` on exception) with `result_json`.
* Connections are opened per operation (`with closing(connect(db_path))`).
* Ledger/mirror stores take a connection; all multi-row writes use `transaction()`.

---

## 8. Telegram bot

### 8.1 Runtime
* `TelegramClient` (bot/telegram.py) over net.py; base URL from `TELEGRAM_API_BASE`
  (default `https://api.telegram.org`, overridable for tests/smoke). Methods: get_me, get_updates
  (long poll 50 s, `allowed_updates=["message","callback_query"]`), send_message, edit_message_text,
  edit_message_reply_markup, answer_callback_query, delete_message, send_document, get_file /
  download_file (size cap 25 MB, atomic temp file), set_my_commands. All sends use
  `parse_mode="HTML"` with content escaped by `render.h()`; link previews disabled. Long messages
  are split on line boundaries so each chunk ≤ 4096 UTF-16 code units (formatters close tags per line).
  Telegram 429 → wait `retry_after` (≤ 30 s) once.
* Poll loop (bot/app.py): getUpdates → for each update: dispatch → persist offset = update_id + 1
  (atomic file write) — at-least-once. Errors from getUpdates → exponential backoff (1 s … 60 s).
  SIGTERM/SIGINT → stop flag; finish/abandon gracefully within 8 s; stop uvicorn.
  Heartbeat file `bot_data/.heartbeat` (mtime) touched every loop iteration.
* Slow work (import, reconcile, dedupe delete, cleanup, crypto, status) runs on a
  `ThreadPoolExecutor(max_workers=4)` under the per-user lock; the handler immediately replies
  with a short "working…" message; `Busy` → "⏳ Another operation is still running." Exceptions
  are logged with traceback and reported to the user via `error.user_message` (never raw secrets).
* Startup: config validation; logging; migrate users DB and every `transactions_*.db` **before**
  the web thread starts; admin auto-register (only when the admin row is missing or not ready);
  register commands; admin DM "🚀 Bot started v… — web UI: enabled/disabled (reason)/failed".

### 8.2 Authorization gate (bot/auth.py)
Applied to messages **and** callback queries before any handler:
* Ignore updates whose chat is not private (`chat.type != "private"`); for callbacks use
  `callback_query.message.chat`. Optionally reply once "Please message me directly."
* Resolve the user by `from.id`. Admin = `from.id == TELEGRAM_ADMIN_ID`.
* Unknown sender sending a message → insert `pending` + notify admin (v1 behaviour). Unknown or
  pending/denied sender pressing a button → answer "Not authorized", remove keyboard, no DB write.
* Each command/callback declares allowed states; callbacks require `ready` unless they belong to
  the onboarding flow the user is currently in.
* Admin commands match exactly (`/ip` ≠ `/ipfoo`), case-insensitive, `@botname` stripped.

Commands (names unchanged): user — `/start` `/help` `/status` `/settings` `/setup` `/login`
`/logout_all` (new: revoke web sessions) `/auto_approve [on|off]` `/reconcile` `/cleanup_pending`
`/dedupe` `/dedupe_delete <1,3-5|all>` `/dedupe_cancel` `/crypto` `/crypto_setup` `/crypto_status`
`/cancel` (new: leave any onboarding step); admin — `/approve <id>` `/deny <id>` `/users` `/ip`
`/logs [N]` `/revoke <id>` (new: revoke a user's web sessions). setMyCommands as in v1 plus the
new ones; `/dedupe_delete` and `/dedupe_cancel` stay unregistered.

### 8.3 Flows
* **Onboarding**: approve → awaiting_token (prompt) → token message is deleted after reading
  (deleteMessage) and validated with get_user/get_budgets (401 → ask again; network → retry hint)
  → budget picker → account picker (open, non-deleted accounts only; inline buttons with index
  callback `ob:b:<i>` / `ob:a:<i>`; typed name or number still accepted) → ready. Selections are
  kept in temp_data and committed atomically at the end. `/cancel` returns to the previous
  ready configuration if one existed (setup keeps the old config until the new one is complete),
  else to awaiting_token. Changing budget/account clears crypto_account_id if the budget changed
  and revokes web sessions. /deny revokes web sessions and drops in-memory flow state.
* **Settings** keeps `st:*` callback data (gated).
* **/login**: requires ready + web UI enabled & healthy; message text without the URL plus an
  inline keyboard **URL button** "Open dashboard" (token never in text); records message id for
  deletion after sign-in.
* **CSV document**: requires ready; name must end with .csv; size ≤ 25 MB; download to temp →
  content check → `services.import_csv(...)` on a worker → reply rendered from ImportResult
  (counts, up to 8 changes, up to 8 removals, skipped rows summary, warnings, failures with
  "⚠ Partially applied — send the file again to retry"), then balance line from the mirror.
* **/reconcile**: preview message with numbers + buttons `rc:<nonce>:ok` / `rc:<nonce>:no`.
* **/dedupe**: picker bound to a scan nonce: callback data `dx:<nonce>:t:<i>` / `dx:<nonce>:p:<page>`
  / `dx:<nonce>:all|none|del|back|ok|c`. `ok` only valid in the confirm view, which lists the
  selected rows. Legacy `dd:*` callbacks → "This list has expired — run /dedupe again." and
  keyboard removed. `/dedupe_delete` validates ranges against the candidate count before
  expanding (max 200) and then shows the same confirm view (no unconfirmed deletes).
* **/crypto_setup**: inline account picker; BTC/ETH validated with btc.py / eth address check;
  `/cancel` or "cancel" exits; documents and commands are not swallowed (they get "finish or /cancel
  crypto setup first").
* **/status**: from mirror after a throttled sync: working/cleared/uncleared balance, last statement
  (name, uploaded, status), ledger counts.
* Admin `/logs [N]` sends the tail (default 500 lines, max 100k) of the rotated log as a document.

---

## 9. Web UI

### 9.1 Server
* `make_app(config: WebConfig, *, users_db_path, data_dir, version_line, cache_bust,
  services=services, telegram=None) -> FastAPI` (docs/openapi disabled).
* Pure ASGI middleware (outermost first): request size limit (Content-Length and streamed bytes;
  `/api/upload` 26 MB, other `/api/*` 64 KB, everything else 16 KB) → IP allowlist (fail closed on
  parse errors: the web UI refuses to start) → security headers (`Content-Security-Policy:
  default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self';
  font-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'; object-src 'none'`,
  `X-Content-Type-Options: nosniff`, `Referrer-Policy: same-origin`, `X-Frame-Options: DENY`,
  `Cache-Control: no-store` on `/app/*`, `/api/*`, `/auth`).
* **No multipart**: `/api/upload` takes the raw CSV as the request body (`Content-Type: text/csv`,
  filename in `X-Filename`, URL-encoded). JSON endpoints read the body via a helper that enforces
  the size cap **after** the auth/CSRF dependency has run (handlers take `Request`, no body params).
  python-multipart is removed from the dependencies.
* Blocking work runs in the threadpool (plain `def` endpoints or `run_in_threadpool`); nothing
  blocking on the event loop. Session purge runs in a background thread every 5 minutes.
* Auth (web/auth.py): tokens `secrets.token_urlsafe(32)`; stored as `HMAC-SHA256(secret, token)`
  hex; CSRF token = `HMAC-SHA256(secret, "csrf:" + session_token)[:32]`, compared with
  `hmac.compare_digest`; atomic login-token consumption (`DELETE … RETURNING`); `revoke_all(conn,
  telegram_id)`; `current_user` requires state `ready` with token/budget/account set (else 401 and
  the session is deleted). Cookie unchanged (`rynab_session`, HttpOnly, SameSite=Lax, Secure iff
  public URL is https, Max-Age = absolute TTL).
* `GET /auth?t=` renders a small page that POSTs the token (auto-submitted by `/static/js/auth.js`,
  with a visible "Continue" button as fallback); `POST /auth` consumes it (rate-limited 10/min/IP
  with a bounded, evicting limiter), sets the cookie, 303 → `/app/dashboard`, and deletes the
  Telegram login message on a background thread. `GET /` shows the login page unless the session
  is *valid*. `POST /logout` requires CSRF. `POST /api/logout-all` revokes every session of the user.
* HTML routes for missing/expired sessions redirect to `/?expired=1`; JSON routes return 401
  `{error: "session_expired", message}`. All error JSON bodies are `{error, message}`.
* uvicorn `proxy_headers=True`, `forwarded_allow_ips` from `WEB_UI_TRUSTED_PROXIES` (default `*`,
  documented: the container is only reachable through Caddy).
* `serve_in_thread(...)` returns a handle with `.started` (event), `.failed` (exception) and
  `.stop()`; startup failure is reported to the bot (admin DM) instead of dying silently.

### 9.2 API (JSON; money as integer milliunits + ISO currency code)
`GET /api/me`, `GET /api/dashboard`, `GET /api/transactions` (filters as v1; LIKE wildcards
escaped), `GET /api/accounts`, `GET /api/categories`, `GET /api/spending?months=`,
`GET /api/subscriptions`, `POST /api/sync`, `GET /api/reconcile/preview`, `POST /api/reconcile`
`{token}`, `GET /api/dedupe/scan`, `POST /api/dedupe/delete {scan_id, ids}`, `POST /api/settings`
`{auto_approve}`, `POST /api/upload` (raw CSV), `POST /api/logout-all`. Busy → 409
`{error:"busy"}`. All reads come from the mirror/ledger after a throttled background sync
(sync never blocks a GET for more than the throttle check; `POST /api/sync` forces one).
Transactions list reads the **mirror** (all accounts of the budget, filter by account), so
it shows what YNAB has.

### 9.3 Analytics (web/analytics.py, pure)
Input rows from the mirror joined with accounts. Spending = outflows on on-budget accounts,
excluding transfers (`transfer_account_id` not null), excluding the importer's adjustment
payees/import_ids (`RECON:`, `CRYPTO:`, `IBKR:`), with split parents replaced by their
subtransactions, inflows in the same category netted (refunds), category "Inflow: Ready to Assign"
excluded. "Last N months" = N complete calendar months + month-to-date shown separately;
averages over complete months; dates after today excluded. Subscriptions: group by normalized
payee (lower/trim), sort each group by date in Python, cadence buckets as v1 (weekly 6–8,
biweekly 12–16, monthly 25–35, yearly 350–380 days, yearly needs lookback ≥ 13 months), amounts
equal within 0.05 currency units, transfers excluded.

### 9.4 Frontend
* No inline scripts or style attributes. `base.html` sets `<body data-page="…">` and
  `<meta name="csrf-token">`; `/static/js/main.js` (ES module) boots the page module.
* `js/core/api.js` (fetch wrapper: CSRF header, JSON/err normalisation `{error,message}` and FastAPI
  `detail`, 401 → redirect to `/?expired=1`, 403 csrf → reload prompt, AbortController per list
  with request ids so stale responses are dropped), `js/core/html.js` (tagged template that escapes
  every interpolation; `raw()` escape hatch), `js/core/format.js` (money from milliunits +
  currency via `Intl.NumberFormat`, date strings rendered verbatim), components (toast with
  aria-live, confirm `<dialog>`, account picker disclosure, busy button), `js/pages/*.js`.
* Tailwind is compiled ahead of time: `scripts/build_css.sh` downloads the pinned standalone CLI
  v3.4.17 (sha256-verified) into `.cache/` and builds `web/static/app.css` from
  `web/styles/input.css` + `tailwind.config.js` (content: templates + js). The built CSS is
  committed; CI fails if rebuilding changes it. Visual parity with v1 is the goal (same palette,
  dark mode via `class` + localStorage `theme`).
* Static URLs are versioned: `/static/<cache_bust>/…` mounted alongside `/static/…`.
* Reconcile page: preview → confirm dialog. Dedupe page: scan → select → confirm dialog; chunks
  of 200. Upload: keyboard-accessible input, in-flight guard, result rendered from ImportResult.

---

## 10. Container, CI, docs

* **Dockerfile**: `python:3.12-slim-bookworm` pinned by digest; `ENV PYTHONDONTWRITEBYTECODE=1
  PYTHONUNBUFFERED=1`; non-root user `app` uid/gid 10001; `pip install --require-hashes -r
  requirements.txt`; copy `ynab_helper/`, shims; `VOLUME /app/bot_data`; `EXPOSE 8080`;
  `STOPSIGNAL SIGTERM`; `HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3
  CMD ["python3", "-m", "ynab_helper.healthcheck"]`; `ENTRYPOINT ["python3", "-m", "ynab_helper.container"]`;
  `CMD ["bot"]`. No `USER` line — `container.py` starts as root, `chown -R 10001:10001 /app/bot_data`
  (only entries not already owned), reads `/app/.env` into the environment (root can read the
  0600 file), sets `umask 077`, drops privileges (`os.setgroups([])`, `setgid`, `setuid`) and runs
  the requested command. If not root, it just runs. `container.py cli …` runs the CLI.
  This needs no change on the host.
* `.dockerignore` as an allowlist.
* **Dependencies**: `requirements.in` (fastapi, uvicorn, jinja2, pycryptodome — current releases
  without known advisories) compiled to `requirements.txt` with hashes (`uv pip compile
  --generate-hashes --python-version 3.12`); `requirements-dev.in/.txt` adds pytest, httpx,
  ruff, pip-audit.
* **CI** (`.github/workflows/ci.yml`, replaces docker-publish.yml): triggers pull_request, push to
  main, workflow_dispatch. Job `test`: setup-python 3.12, install dev requirements, `ruff check`,
  `ruff format --check`, `pytest -q`, CSS drift check, `pip-audit -r requirements.txt`. Job `image`
  (needs test): build with buildx (gha cache), run the image's self-check
  (`docker run --rm IMAGE selfcheck`), push `latest` + short sha **only** on push to main
  (`concurrency: deploy-main`). Actions pinned by commit SHA. `dependabot.yml` for pip,
  docker, github-actions (weekly, grouped).
* **Docs**: README (concise, accurate quick start + operations), `docs/ARCHITECTURE.md`,
  `docs/OPERATIONS.md` (deploy flow, rollback by pinning `:<sha>`, backups/restore, logs, the v2
  migration, health), `CHANGELOG.md`, PENTEST_REPORT.md with a status column, `.env.example`
  cleaned (no cloudflared), reference `docker-compose.yml` (healthcheck, `stop_grace_period: 20s`,
  `logging: {driver: local, options: {max-size: 10m, max-file: 3}}`) and reference `Caddyfile`
  (`request_body { max_size 26MB }`). The server's real files are not changed by the repo; the
  docs list the optional host-side improvements.

---

## 11. Configuration (config.py)

`.env` loading (v1 semantics): file `<repo root>/.env` (`/app/.env`); lines `KEY=VALUE`, optional
`export ` prefix, single/double quotes stripped, inline ` #` comments removed for unquoted
values; existing environment variables win. Unreadable file → warning logged (not silent).

| Variable | Used by | Default |
|---|---|---|
| YNAB_TOKEN, YNAB_BUDGET_ID, YNAB_ACCOUNT_ID | CLI; bot admin auto-register | — |
| YNAB_CRYPTO_ACCOUNT_ID, CRYPTO_BTC_XPUB, CRYPTO_ETH_ADDRESS, ANKR_API_KEY | crypto | — |
| YNAB_BROKERAGE_ACCOUNT_ID, IBKR_BASE_URL, IBKR_ACCOUNT_ID | IBKR (CLI) | https://localhost:5050 |
| LOG_LEVEL, LOG_FILE | all | INFO; CLI `~/.revolut_to_ynab.log`, bot `$BOT_DATA_DIR/bot.log`? **No** — keep v1: `~/.revolut_to_ynab.log` unless set ("" disables) |
| CSV_DIR | CLI | ~/Downloads |
| TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID | bot | required |
| TELEGRAM_API_BASE | bot (tests) | https://api.telegram.org |
| BOT_DATA_DIR | bot/web | `<repo root>/bot_data` |
| WEB_UI_ENABLED, WEB_UI_HOST, WEB_UI_PORT, WEB_UI_PUBLIC_URL, WEB_UI_SECRET_KEY (≥ 32 chars, now used for HMAC), WEB_UI_LOGIN_TTL, WEB_UI_SESSION_TTL, WEB_UI_SESSION_ABS_TTL, WEB_UI_ALLOWED_IPS, WEB_UI_TRUSTED_PROXIES | web | 0, 0.0.0.0, 8080, —, —, 300, 1800, 43200, (all), * |
| BUILD_SHA, BUILD_DATE | version line | dev, local |
| APP_UID / APP_GID | container.py | 10001 |

Validation collects all errors. Bot-required errors → exit 1 with a clear message. Web errors →
web UI disabled, bot keeps running, admin is told in the startup DM. Malformed integers never
crash the bot.

---

## 12. Testing

pytest, no network (an autouse fixture makes `urllib.request.urlopen` and sockets raise).
* `tests/fakes/fake_ynab.py` — stateful in-memory YNAB implementing the YnabClient transport:
  budgets/settings/accounts, transactions with import_id dedupe per account (deleted ones included
  — configurable), import_id immutable on PUT/PATCH, bulk PATCH, DELETE = soft delete visible in
  deltas, server_knowledge deltas, subtransactions, injectable failures (404/429/500/timeouts at
  call N), request log.
* `tests/fakes/fake_telegram.py` — records sends, serves updates, file downloads.
* `tests/fixtures/*.csv` — synthetic exports that mimic the real ordering (Completed Date order,
  pendings/reverted interleaved, same-amount same-day pairs, multi-currency, fees, other products).
* `tests/legacy/v1_revolut_to_ynab.py` — frozen copy of v1 used as an oracle (import_id golden
  tests) and to build realistic legacy DBs for migration tests.
* Required test groups: parser golden/property tests; planner table tests (every rule in §6.2);
  executor failure-injection convergence test (fail at call k for every k, rerun, final YNAB state
  equals the no-failure run); one regression test per confirmed finding (named `test_<ID>_…`,
  e.g. `test_F01_reconciled_never_downgraded`); migration from a legacy DB built with the v1 code
  and from a DB with the exact production DDL; sync; analytics fixtures (transfers, splits, refunds,
  tracking accounts, month windows); crypto vectors (BIP32 TV1, BIP84, BIP49, bad checksum);
  reconcile; services locking (bot vs web contention → Busy); bot authorization matrix (unknown /
  pending / denied / ready / admin × every command and callback prefix, group chats); onboarding;
  dedupe nonce/confirm; web auth (expiry, state, CSRF on every POST, revoke), body limits before
  auth, upload, analytics endpoints, XSS-escaping of templates; config parsing; container
  privilege drop (unit-tested with mocks).

---

## 13. Review findings → where they are fixed

| Finding | Fix location |
|---|---|
| F01 reconciled downgraded / memos wiped; F33 user edits overwritten; C02 | §6.2 step 3 (cleared monotonic, memo from YNAB), §6.7 |
| F02 ghost row after re-key; F11 payee rename breaks matching; F39 moved tx | ledger/mirror split §5.2, matching on csv_* fields |
| F06 one failing PATCH wedges imports; F07 failed writes recorded as applied | §6.3 error model |
| F08 deleted rows resurrected | §6.2 step 3 statuses |
| F09 --skip-pending deletes pendings; F36 older export downgrades/deletes | PlanOptions, allow_sweep, monotonic rules |
| F10 window-boundary date shift; F34/C01 renumbering; F35 settlement | §6.2 step 2 |
| F12 ledger not scoped by account | PK (account_id, key) |
| F14 currency not validated; F37 unknown states | §6.1, §7 import_csv |
| F04/F27 reconcile from stale CSV, wrong row | §6.6 reconcile |
| F05/F28/F48/F49/F29 crypto | §6.6 crypto, structured results |
| F15 stdout swapping; F47 summary parsing | structured results everywhere |
| F16 no per-user mutual exclusion; F43 blocking loop; F41 async upload blocks | locks.py, bot workers, threadpool |
| F17 no 429 handling / quota | YnabClient, throttled sync, bulk PATCH |
| F03 callbacks bypass auth; F56 group chats; F18 deny doesn't revoke | §8.2, web current_user |
| F19 file name traversal | revolut/files.py server-generated names |
| F20/F44/F13/F46 dedupe | §6.7, §8.3 |
| F21/F22 body parsed before auth, multipart CVE | §9.1 no multipart, size limits, auth first |
| F23 Tailwind CDN / unsafe-inline | §9.4 |
| F25 login link Markdown corruption; F91/F96 escaping | HTML + escaping, URL button |
| F30/C03/F65 analytics | §9.3 |
| F52/F53/F54/F55 onboarding | §8.3 |
| F57 plaintext PAT messages | deleteMessage, umask, non-root |
| F59/F60 config fail-open/crash | §11 |
| F61/F24/F62/F63 web session & frontend errors | §9.1, §9.4 |
| F64/C06/F100 dashboard numbers | §9.2 (mirror, currency) |
| F26/F40/F69 polling, offset, health | §8.1, §10 |
| F31/F68/F71/F73/F109/F115/F107 CI, deps | §10 |
| F70 root container | §10 container.py |
| F110 log rotation | §4.6 |
| F42/F78/F79/F80 network robustness, IBKR TLS | net.py, §6.6 |
| F51 watch mode; F74 dry-run writes; C07 db-stats path; F75/F76 | cli.py, executor dry_run, migrations |
| F86/F87/F88/F89/F90 web auth details | §9.1 |
| F94 web thread dies silently | serve_in_thread handle |
| F112 stale version | version.py from build args + `__version__` 2.0.0 |
