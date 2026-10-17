# Architecture

This document explains how v2 is put together. [DESIGN.md](DESIGN.md) is the detailed
specification, and its appendix lists where the code deliberately differs. When a document
and the code disagree, the code is right.

## One process, three front ends

```
                     ┌────────────────────────── container (uid 10001) ──────────────────────────┐
 Telegram ◄─long poll─┤ bot: poll loop ─► auth gate ─► handlers ─► WorkerPool (4 threads)        │
                      │                                                     │                      │
 Browser ─► Caddy ───►┤ web: uvicorn thread ─► middleware ─► routes ───────┤                      │
                      │                                                     ▼                      │
 docker exec ────────►┤ cli (separate process)  ─────────────────────► services ─► YNAB API       │
                      │                                                     │    ─► Blockstream,   │
                      │                                                     │       ETH RPC,       │
                      │                                                     ▼       CoinGecko, IBKR│
                      │                        bot_data/: bot_users.db, transactions_<id>.db, …    │
                      └────────────────────────────────────────────────────────────────────────────┘
```

The image runs one process: the Telegram bot, with the web UI on a thread inside it. The CLI
is the same code started as its own process. All three front ends call the same
**service layer** (`ynab_helper.services`). It returns structured results (dataclasses) and
never prints, and each front end renders those results its own way.

## Module map

| Package / module | Responsibility |
|---|---|
| `errors` | Exception hierarchy. Every error carries a `user_message` that is safe to show. |
| `config` | `.env` loading (v1 semantics) and typed, validated `Settings`. It never raises on bad values. |
| `logging_setup` | stderr plus a rotating file (5 MB × 5), the audit logger `ynab_helper.audit`, and secret redaction. |
| `net` | urllib JSON client with timeouts, a deadline per attempt, retry with backoff, `Retry-After` and redaction. |
| `money`, `timeutil`, `version`, `locks` | Milliunit and Decimal helpers; UTC and local "today"; the version line from `BUILD_SHA`/`BUILD_DATE`; per-user operation locks. |
| `storage` | SQLite access (`db`), schemas and migrations (`tx_schema`, `users_schema`), and the stores `LedgerStore`, `MirrorStore`, `CsvFileStore`, `UserStore` and `SessionStore`. |
| `ynab` | `YnabClient` (typed errors, bulk create and update, 429/5xx handling) and models. |
| `revolut` | `parser` (statement → `ParsedExport`, with v1-identical import ids) and `files` (upload storage under server-generated names, retention). |
| `sync` | `sync_budget` / `sync_and_refresh`: the YNAB delta sync into the mirror, throttling and the 429 backoff. |
| `importer` | `planner` (pure: rows + ledger + mirror → plan), `executor` (applies the plan to YNAB, then to the ledger) and `models`. |
| `balance` | `adjust` (balance adjustments), `reconcile` (preview/apply), `crypto/` (`btc`, `eth`, `prices`, `sync`), `ibkr` and `brokerage`. |
| `maintenance` | `cleanup` (stale `(pending)` markers) and `dedupe` (scan and delete). |
| `services` | Per-user operations (`import_csv`, `sync`, `account_status`, `reconcile_*`, `dedupe_*`, `cleanup_pending`, `crypto_sync`, `brokerage_sync`), each under the user's lock. |
| `sessions` | Web login tokens, sessions and CSRF (HMAC). It does not import FastAPI. |
| `bot` | `telegram` (client), `app` (poll loop, dispatch, jobs, shutdown), `auth` (gate), `commands` (registry), `handlers/`, `render` (HTML) and `main` (startup). |
| `web` | `config` (`WEB_UI_*`, no FastAPI import), `app` (factory), `middleware`, `routes/`, `deps`, `queries`, `analytics` (pure), `background`, `server` (`serve_in_thread`), templates and static files, and `devserver` (development only). |
| `cli` | argparse with every v1 flag; renders results as text. |
| `container`, `healthcheck`, `__main__` | Container entrypoint (root setup, then privilege drop, then the command), the Docker health check, and `python -m ynab_helper`. |
| `revolut_to_ynab.py`, `revolut_ynab_bot.py` | Thin shims for the documented commands. As root they first drop privileges like the entrypoint. |

**Dependency direction.** Imports only point down this list; nothing below imports from
above it:

```
cli · bot · web                       front ends (bot imports web.config, which is FastAPI-free, and web.server lazily)
services                              per-user operations, locks
importer · balance · maintenance      domain logic (maintenance uses balance.reconcile's statement loader)
sync                                  YNAB → mirror
storage · ynab · revolut · sessions   persistence, API client, CSV parsing, web auth primitives
net · config · logging_setup · money · timeutil · locks · errors · version
```

`importer.planner` and `web.analytics` are pure: they do no I/O, read no clock and touch
no SQLite, so table tests cover them.

## Data model

All state lives in `BOT_DATA_DIR` (`/app/bot_data`):

| Path | Contents |
|---|---|
| `bot_users.db` | `users` (Telegram id, YNAB token, budget, account, crypto settings, state, temporary flow data, auto-approve) and `web_sessions` (HMAC of login tokens and sessions). Same tables as v1. |
| `transactions_<telegram_id>.db` | One per user, described below. The CLI's `~/.revolut_to_ynab.db` has the same schema. |
| `csv_cache/<telegram_id>/` | Uploaded statements under server-generated names (`<UTC stamp>_<8 hex>.csv`). The newest 5 are kept, plus the newest imported one. |
| `backups/` | Automatic backups taken before each migration: `<db name>.<UTC stamp>.pre-v<N>.db`, newest 10 per database. |
| `bot.log*` | Log file (when `LOG_FILE=/app/bot_data/bot.log`) and up to 5 rotated files. |
| `telegram_offset`, `.heartbeat`, `tmp/` | The poll offset (the same file as v1), the liveness file and statement downloads in progress. |

### The transactions database

v1 used one `transactions` table both as the importer's record of what it pushed and as a copy
of YNAB written by the web UI's sync, so each side overwrote the other's fields (the root cause
of findings F01, F02, F11 and F33). v2 keeps three separate things:

| Table | Written by | Meaning |
|---|---|---|
| `import_ledger` (PK `account_id, key`) | importer only | One entry per Revolut row ever seen in that YNAB account. It holds the CSV identity (`started_at`, `csv_amount`, `csv_date`, `csv_payee`, `csv_state`), the link to YNAB (`ynab_tx_id`, `ynab_import_id`) and what we last wrote (`pushed_cleared`, `pushed_amount`, `pushed_date`). `status` is one of `ACTIVE`, `UNLINKED`, `REVERTED`, `SUPERSEDED`, `DELETED_IN_YNAB` or `REKEYED` (a tombstone that points at the key that took over). |
| `ynab_transactions`, `ynab_subtransactions`, `accounts` | `sync` only | A mirror of YNAB for the whole budget, deleted rows included. It is rebuilt from YNAB's delta API and is never edited locally. |
| `csv_files` | services | Uploaded statements: path, sha256, source (`telegram`, `web`, `cli` or `legacy`), currency, date window and import status (`stored`, `imported`, `partial`, `failed` or `legacy`) with the `ImportResult` as JSON. Reconcile and dedupe use the newest statement only when its status is `imported`. |
| `sync_meta` | `sync` | Delta cursors (`mirror_knowledge:<budget>`, `accounts_knowledge:<budget>`), `budget_currency:<budget>`, `last_sync_at:<budget>` and `rate_limited_until:<budget>`. |

**v1 tables are frozen.** The migration (user_version 0 → 1) copies the importer's own v1
rows (`source='revolut'`) into `import_ledger` with `identity='legacy'`. It never modifies or
drops the v1 `transactions` table, so the v1 image can still start on a migrated file.
`accounts` and `sync_meta` are shared; v2 adds a `budget_id` column and uses its own keys.
The migration also collapses v1 "ghost" rows (`tx_schema.migrate_legacy_rows` →
`_collapse_shared_links`): several ACTIVE entries for one YNAB transaction become one entry
plus `REKEYED` tombstones. v1's own rows of its YNAB copy (`source='ynab'`) are not migrated:
the first v2 sync fetches the whole budget (`last_knowledge_of_server=0`). After every sync,
`refresh_from_mirror` links UNLINKED entries to their YNAB transactions. An UNLINKED entry
that resolves to a transaction another entry already holds becomes a `REKEYED` tombstone
instead, so one YNAB transaction keeps exactly one live entry.

The migration is idempotent, runs inside one transaction after a verified backup, and is
**fail-closed**: an invalid legacy row aborts that database's migration, so the file stays at
v0 and the admin is told in the startup DM.

## Import pipeline

`services.import_csv(ctx, source, …)` runs under the user's lock:

1. **Store and parse.** `revolut.files.store_upload` parses the whole file before storing it.
   It checks the header, the 25 MiB limit, UTF-8 encoding, the shape of each row and a single
   currency, then writes it atomically and adds a `csv_files` row (`stored`). Each row gets
   its v1 import id, with occurrence counting identical to v1; a golden test checks this
   against the frozen v1 parser. States outside COMPLETED, PENDING and REVERTED, and rows of
   other products, are skipped and reported.
2. **Check.** The statement's currency must equal the budget currency.
3. **Sync.** A forced delta sync updates the mirror. If YNAB answers 429, the import stops
   with `YnabRateLimited` before writing anything, because planning from a stale mirror could
   undo the user's edits. Then the account check runs: an account that is missing, deleted,
   closed or in another budget is refused. Unassigned legacy ledger rows are claimed for the
   account, and `refresh_from_mirror` links entries to YNAB.
4. **Plan** (`importer.planner.plan_import`, pure):
   - *Offsets*: a statement renders timestamps in the phone's timezone. Rows whose key
     matches a ledger entry, plus same-amount, same-payee pairs, reveal the export's offset
     (a multiple of 15 minutes, at most 26 h).
   - *Matching passes*, each side matched at most once:
     1. **timestamp identity**: same amount, and `started_at` equal up to an allowed offset;
     2. **legacy group/key**: v1 entries without a timestamp, matched by (amount, date) group
        or by exact key;
     3. **legacy date shift**: amount and payee equal, dates one day apart, and the row
        starts near midnight in the right direction;
     4. **settlement**: a pending entry that settled at a different amount, with the same
        payee and sign, at most one day apart;
     5. **pre-settlement**: an older statement still showing, at its old amount, a pending
        that has since settled. It changes nothing.
   - *Per match*: cleared only ever moves uncleared → cleared. The memo comes from YNAB's
     current memo, with only the `(pending)` marker removed. Amount and date are written only
     when YNAB still holds what we last wrote, and never on reconciled or split transactions.
     Payee, category and approval are never sent on updates. A reverted row is deleted only
     while it is uncleared in YNAB. Entries deleted in YNAB are never recreated, and moved
     transactions are reported but not changed.
   - *Unmatched rows* are created with an import id that is unique in the account (the CSV
     key, or the next free `:n`). If YNAB already holds the row under an import id no ledger
     entry knows (our own create whose ledger write was lost), it is adopted instead.
   - *Superseded sweep*: pending entries the statement provably covers but no longer lists
     are deleted, as long as they are still uncleared in YNAB.
   - *Older statements* (detected from the dates, from timestamps newer than the statement,
     or from a row shown pending that was already completed) never sweep, never recreate
     anything, and their pending rows change nothing.
5. **Execute** (`importer.executor.apply_plan`): local LINK/LEDGER actions, then CREATE (bulk
   POST, 100 per chunk), UPDATE (bulk PATCH, 50 per chunk, with a per-row PUT fallback on
   4xx), then DELETE (per row). The ledger advances only for what YNAB confirmed, one savepoint
   per action. Failures are collected and execution continues. A 429 stops all further writes
   (the rest is reported as `not_applied`) and starts the sync backoff. A 401/403 aborts. A
   404 on a create stops the import, because it means the budget or account is gone.
   "Duplicate import_id" answers are resolved after a post-create sync and linked. Rejected
   create chunks are bisected (with a request cap) to isolate the bad rows.
6. **Record.** `csv_files.status` becomes `imported` when `ImportResult.ok`, `partial` when
   something failed or was not applied, and `failed` on an exception. The result is stored
   as JSON. Sending the same statement again converges; a test proves it (see
   [Testing](#testing)).

`--dry-run` runs steps 1–4 without storing the file, and simulates the claims inside a
rolled-back transaction. It writes nothing to YNAB or the ledger. The mirror sync still
runs, because the mirror is a cache.

Reconcile (`balance.reconcile`) is two-step. `reconcile_preview` computes the statement
balance (from the COMPLETED row with the latest completion time), YNAB's cleared balance as
of the statement date, the difference and any warnings, and issues a single-use token
(10 minutes) bound to all those numbers. `reconcile_apply(token)` re-checks everything
against YNAB's live account and refuses if anything changed.

## Threading model

| Thread | What runs there |
|---|---|
| main | Bot startup, then the poll loop. `getUpdates` (a 50 s long poll) runs on a helper thread while the main thread waits in 0.25 s ticks, so SIGTERM is noticed at once. Updates are dispatched one by one: auth gate, then handler. The offset is saved after each update (at-least-once delivery; imports are idempotent). |
| WorkerPool (4 daemon threads) | Slow bot work: import, reconcile, dedupe, cleanup, crypto and status. One job per user at a time; the user gets a "working…" reply first. |
| supervisor | Touches `.heartbeat` every 15 s while the loop makes progress, and ends the process 7.5 s after a stop signal if anything hangs. |
| uvicorn (daemon) | The web UI's event loop. Blocking endpoints run in the threadpool (plain `def` or `run_in_threadpool`), never on the loop. |
| web helpers (daemon) | `BackgroundSyncer` (throttled mirror sync triggered by reads), `SessionPurger` (every 5 min) and `DetachedRunner` (deletes the Telegram login message after sign-in). |

**Per-user locks** (`locks.LOCKS`, process-wide). Every mutating service operation takes
`LOCKS.operation(key, name)`, where the key is the Telegram id (the CLI uses the database
path). Locks do not block and are not re-entrant: a second operation for the same user gets
`Busy` ("⏳ Another operation is still running" in the bot, 409 `busy` in the web) instead of
queueing. Bot jobs and web requests share the locks. The CLI runs in a separate process, so
its lock is not shared with the bot.

**SQLite rules** (`storage.db`):

- Every connection uses WAL, `busy_timeout=10000`, `foreign_keys=ON` and autocommit, and
  writes in explicit `BEGIN IMMEDIATE` transactions (nested uses become savepoints).
- No network I/O inside a transaction. YNAB is called first, and the result is committed
  afterwards.
- Transactions databases are opened per operation (`with closing(connect(...))`) and never
  shared between threads.
- The bot holds one long-lived connection to `bot_users.db` (`check_same_thread=False`),
  guarded by an `RLock`. The web uses its own short-lived connections and takes that lock
  only for its own user-row writes.
- Migrations run at startup before the web thread starts. `umask 077` makes every new file
  0600.

**In-memory state** (lost on restart by design, so a stale button simply expires):
reconcile tokens and dedupe scans (10 minutes), the bot's flow memory (reconcile previews,
dedupe pickers) and the web's rate limiters.

**Shutdown.** On SIGTERM the loop stops after the current update and queued jobs are
dropped. Running jobs get 4 s, and affected users get a short "bot restarted" notice. The web
server is stopped, and the process exits within about 7.5 s, inside Docker's 10 s grace
period.

## Security model

**Telegram bot** (`bot/auth.py`). One gate runs for messages *and* button presses, before
any handler:

- Only private chats count. Group updates are ignored, apart from one "Please message me
  directly." per group.
- The sender is `from.id`. An unknown sender's message registers them as `pending` and
  notifies the admin (rate-limited). Pending and denied users get at most one reply per
  5 minutes. Buttons from unknown, pending or denied users answer "Not authorized" and write
  nothing.
- Every command and callback declares the user states it accepts. Admin commands match
  exactly and exist only for `TELEGRAM_ADMIN_ID`.
- Callback data carries a nonce bound to the flow or message (`ob:<nonce>:…`,
  `rc:<nonce>:…`, `dx:<scan>:…`), so an old button can never act on newer data.
- All output is HTML with escaped content. Messages with a YNAB token are deleted. `/login`
  uses a URL button, so the token is never in message text.

**Web UI** (`web/`):

- Middleware, outermost first: body size limit (`/api/upload` 26 MiB, other `/api/*` 64 KiB,
  everything else 16 KiB, enforced before routing or auth; a chunked upload gets 411), then
  the IP allowlist (fails closed), then security headers, then an error boundary.
- Headers: CSP `default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'
  data:; connect-src 'self'; font-src 'self'; frame-ancestors 'none'; base-uri 'self';
  form-action 'self'; object-src 'none'`, plus `nosniff`, `Referrer-Policy: same-origin`,
  `X-Frame-Options: DENY`, and `Cache-Control: no-store` except on static files. Templates
  have no inline scripts or styles, and Tailwind is precompiled and self-hosted.
- Authentication runs before any body is read. Handlers take the raw `Request`, the auth
  and CSRF dependencies run first, and only then does a helper read the body within its cap.
  There is no multipart parser; uploads are the raw CSV body.
- Tokens: `secrets.token_urlsafe(32)`, stored as HMAC-SHA256 with `WEB_UI_SECRET_KEY`.
  Login tokens are single-use, consumed atomically (`DELETE … RETURNING`) by a POST from the
  `/auth` page; the GET changes nothing. Sessions have a sliding TTL plus an absolute TTL
  and require a `ready` user. `/deny`, `/setup` changes, `/revoke`, `/logout_all` and *Sign
  out everywhere* revoke them.
- CSRF: an `X-CSRF-Token` header, which is an HMAC of the session token, on every
  state-changing `/api` POST and on the two GETs that sync YNAB and replace server-side state.
  `POST /logout` also accepts it as a `csrf_token` form field. `POST /auth` has no session
  yet: it requires a same-origin request and the one-time login token, and is rate-limited
  to 10 per minute per client (IPv4 address or IPv6 /64). JSON routes refuse any
  `Sec-Fetch-Site` other than `same-origin` or `none`.
- Errors are always `{error, message}`, with no stack traces. OpenAPI and docs are off, and
  FastAPI's telemetry is disabled.

**Container and host:**

- The entrypoint starts as root (production bind-mounts root-owned files). It reads
  `/app/.env`, sets `umask 077`, and hands `bot_data/` to 10001:10001 (directories 0700,
  files 0600). It opens entries relative to their directory with `O_NOFOLLOW` and never
  follows symlinks or touches hard-linked files. It then drops to 10001 with no
  supplementary groups. `docker exec` shims do the same.
- The secrets from `bot.env` (`settings.secret_values()`: bot token, `YNAB_TOKEN`,
  `CRYPTO_BTC_XPUB`, Ankr key, web secret) are registered with the log redactor and masked
  in every log line. Per-user YNAB tokens and xpubs from `bot_users.db` are not registered;
  the YNAB and network clients redact tokens only in their own error messages.
- Supply chain: the base image is pinned by digest, dependencies are hash-locked and
  installed as wheels only, `pip-audit` runs in CI, actions are pinned by commit SHA, and
  only the `publish` job, after a merge to `main`, can push the image.
- Still open, by choice: YNAB personal access tokens are stored in plain text in
  `bot_users.db` (0600, uid 10001). Anyone with root on the host can read them.

## Testing

`pytest`, about 4,200 tests, fully offline: an autouse fixture blocks every non-loopback
socket. CI runs ruff (lint and format), the tests, the CSS drift check, `pip-audit`, and then
builds the image and checks it (`selfcheck`, `scripts/ci_smoke.sh`).

- **Fakes.** `tests/fakes/fake_ynab.py` is a stateful in-memory YNAB behind the client's
  transport hook. It has budgets, accounts, import-id dedupe per account (deleted rows
  included), immutable import ids, bulk PATCH, soft deletes visible in deltas, server
  knowledge, subtransactions, injectable faults (404, 429, 5xx, network errors, lost
  responses) at call N, and a request log. `tests/fakes/fake_telegram.py` records sends and
  edits, serves updates and files, and mimics Telegram's errors.
- **Fixtures.** `tests/fixtures/*.csv` are synthetic exports in the real format: Completed
  Date order, interleaved pending and reverted rows, timezone shifts, same-amount same-day
  pairs, fees, other products and several currencies.
- **Oracle.** `tests/legacy/v1_revolut_to_ynab.py` is a frozen copy of v1. Golden tests
  require byte-identical import ids, amounts, dates, memos and payees, on the fixtures and on
  random input.
- **Planner and executor.** Table tests cover every planning rule. Each confirmed review
  finding has a regression test named `test_<ID>_…` (for example `test_F01_…`).
- **Convergence** (`tests/integration/test_convergence.py`). A realistic statement sequence
  is imported once without faults to get a reference YNAB state. Then, for every mutating
  request *k* and every fault kind, the sequence is replayed with request *k* failing, and
  each statement is re-sent until the import reports `ok`. The final YNAB state and ledger
  must equal the reference. The matrix runs both with and without client retries.
- **v1 → v2 transition** (`tests/integration/test_v1_to_v2_transition.py`). The frozen v1
  importer, with its web sync and user edits in YNAB, builds a realistic legacy database.
  That database is migrated, and v2 then imports the following statements. The test checks
  that nothing is duplicated or downgraded, no live transaction is deleted, and memos are
  kept. Migration tests also run on a database with the exact production DDL.
- **Bot.** An authorization matrix covers every state and every command and callback,
  including group chats. Also onboarding, dedupe nonces and confirmation, and shutdown.
- **Web.** Auth (expiry, state, CSRF, revoke), body limits before auth, uploads, analytics,
  template escaping, and the frontend modules, which run under Node with a minimal fake DOM
  (skipped when Node is missing).
- **Runtime and infrastructure.** Config parsing, the privilege drop (mocked), the health
  check, locks and logging, plus static checks of the Dockerfile, compose file, Caddyfile and
  workflow.
