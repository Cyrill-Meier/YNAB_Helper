# YNAB Helper

Imports Revolut account statements (CSV) into [YNAB](https://www.ynab.com/) and keeps them
in sync, with a Telegram bot, a small web dashboard and a command-line tool on top of one
shared service layer. It also posts balance adjustments for a crypto wallet (BTC/ETH/ERC-20)
and an Interactive Brokers account into YNAB tracking accounts.

Version 2.0.0 is a rework of v1 that fixes the review findings listed in
[CHANGELOG.md](CHANGELOG.md). It runs on the same host layout, the same `bot.env` and the same
data directory; the first start migrates the data in place (with backups). Before deploying it
over v1, read [The first v2 deploy](docs/OPERATIONS.md#the-first-v2-deploy).

| Part | What it does |
|---|---|
| Telegram bot | Send it a Revolut CSV and it lands in YNAB. Also `/reconcile`, `/dedupe`, `/status`, `/crypto`, and more. Several users, each with their own YNAB token, budget and account, approved by the admin. |
| Web UI | Dashboard, accounts, transactions, spending, subscriptions, CSV upload, reconcile, dedupe and settings. You sign in with a one-time button from the bot (`/login`). |
| CLI | `python3 revolut_to_ynab.py` with every v1 flag: import, watch a folder, reconcile, crypto and IBKR sync, and more. |

Documentation:

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): module map, data model, import pipeline,
  threading, security model and testing.
- [docs/OPERATIONS.md](docs/OPERATIONS.md): deploying, the first v2 deploy, rollback,
  backups, logs, health checks and dependency updates.
- [docs/DESIGN.md](docs/DESIGN.md): the v2 specification, with an appendix of deviations.
- [CHANGELOG.md](CHANGELOG.md) and [PENTEST_REPORT.md](PENTEST_REPORT.md) (now with a
  status table).

## Contents

- [Quick start (Docker)](#quick-start-docker)
- [Configuration](#configuration)
- [Telegram commands](#telegram-commands)
- [Web UI](#web-ui)
- [CLI](#cli)
- [How imports work](#how-imports-work)
- [Crypto and IBKR](#crypto-and-ibkr)
- [Development](#development)

---

## Quick start (Docker)

The image is `ghcr.io/cyrill-meier/ynab_helper` (`:latest`, plus one `:<short sha>` tag per
published commit; built for linux/amd64). The host layout is the one production uses:

```
~/ynab-bot/
  docker-compose.yml   the bot, Caddy and Watchtower (reference: docker-compose.yml in this repo)
  Caddyfile            HTTPS reverse proxy for the web UI (reference: Caddyfile in this repo)
  bot.env              your settings and secrets, chmod 600 (template: .env.example)
  bot_data/            created on first start: databases, statements, backups, bot.log
```

If a server already runs v1 with its own compose file and Caddyfile, keep them: v2 needs no
change on the host. [docs/OPERATIONS.md](docs/OPERATIONS.md#optional-host-side-improvements)
lists the optional improvements.

**1. Create the Telegram bot.** In Telegram, ask @BotFather for `/newbot` and copy the token.
Ask @userinfobot for your numeric user id; this is the admin id. The bot registers its command
menu itself on every start, so do not set `/setcommands` in BotFather.

**2. Write `bot.env`.**

```bash
mkdir -p ~/ynab-bot && cd ~/ynab-bot
curl -fsSLO https://raw.githubusercontent.com/Cyrill-Meier/YNAB_Helper/main/docker-compose.yml
curl -fsSLO https://raw.githubusercontent.com/Cyrill-Meier/YNAB_Helper/main/Caddyfile
curl -fsSL -o bot.env https://raw.githubusercontent.com/Cyrill-Meier/YNAB_Helper/main/.env.example
chmod 600 bot.env
```

Set at least `TELEGRAM_BOT_TOKEN` and `TELEGRAM_ADMIN_ID`. The template already sets
`LOG_FILE=/app/bot_data/bot.log`. To skip the in-chat onboarding for yourself, also set `YNAB_TOKEN`, `YNAB_BUDGET_ID` and
`YNAB_ACCOUNT_ID` (the admin is then registered from them). For the web UI, set
`WEB_UI_ENABLED=1`, `WEB_UI_PUBLIC_URL=https://bot.example.com` and a `WEB_UI_SECRET_KEY` of at
least 32 characters:

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(48))"
```

**3. Set up the web UI's hostname (optional).** Point a DNS record at the host, open TCP 80
and 443 in the cloud firewall (80 is needed for Let's Encrypt), and replace `bot.example.com`
in the `Caddyfile`. The bot's port 8080 is never published on the host; Caddy reaches it over
the compose network.

**4. Start it.**

```bash
docker compose up -d
docker logs -f revolut-ynab-bot
```

The admin gets a DM such as `🚀 Bot started v2.0.0 (abc1234, 2026-10-17 09:00 UTC) — web UI:
enabled at https://bot.example.com`. This message is also the deploy notification. Watchtower
then pulls every new `:latest` within 5 minutes. Other people message the bot, you get a
notice, and you `/approve <id>` them.

The container starts as root only to take ownership of `bot_data/` (it becomes uid/gid
10001, mode 0700/0600) and to read `bot.env`. It then drops to uid 10001 for good. Reading
`bot_data/bot.log` on the host therefore needs `sudo`.

---

## Configuration

Settings come from the environment and from `/app/.env` (that is, `bot.env`; the CLI reads
`.env` next to the code). Variables that are already set in the environment win over the
file. The file format is v1's: `KEY=VALUE`, an optional `export ` prefix, quotes, and ` #`
comments after unquoted values. A malformed optional value is replaced by its default and
reported in the log. If it is a web variable, the web UI stays off and the startup DM says why.
Missing or malformed `TELEGRAM_*` variables stop the bot with a clear message.

The bot reads `bot.env` only at startup. After changing it, run
`docker restart revolut-ynab-bot`; `docker compose up -d` does not notice a change inside a
mounted file (see [OPERATIONS.md](docs/OPERATIONS.md#changing-botenv)).

### Telegram bot

| Variable | Default | Used by | Notes |
|---|---|---|---|
| `TELEGRAM_BOT_TOKEN` | required | bot | From @BotFather. |
| `TELEGRAM_ADMIN_ID` | required | bot | Your positive numeric Telegram user id. Admin commands only work for this id. |
| `TELEGRAM_API_BASE` | `https://api.telegram.org` | bot | **Tests and smoke runs only.** Never set this in production. |

### YNAB (CLI defaults and admin auto-registration)

| Variable | Default | Used by | Notes |
|---|---|---|---|
| `YNAB_TOKEN` | — | CLI; bot (admin auto-register) | Personal access token (app.ynab.com → Account Settings → Developer Settings). Bot users enter their own token in chat. |
| `YNAB_BUDGET_ID` | — | CLI; bot (admin auto-register) | `--list-budgets` shows the ids. |
| `YNAB_ACCOUNT_ID` | — | CLI; bot (admin auto-register) | The Revolut account in YNAB (`--list-accounts`). |

When all three are set, the admin is made a ready user at startup. This happens only if the
admin's row is missing or not ready, so a setup chosen in the bot is never overwritten.

### Crypto and IBKR

| Variable | Default | Used by | Notes |
|---|---|---|---|
| `YNAB_CRYPTO_ACCOUNT_ID` | — | CLI `--crypto-sync`; bot (admin auto-register) | The YNAB tracking account that holds the portfolio value. |
| `CRYPTO_BTC_XPUB` | — | same | xpub, ypub or zpub, or a single BTC address. Validated (checksum, version, curve). |
| `CRYPTO_ETH_ADDRESS` | — | same | Native ETH plus AAVE, USDC, USDT and aUSDT. |
| `ANKR_API_KEY` | — | crypto sync (bot and CLI) | Optional. Adds an authenticated Ankr RPC before the public endpoints. |
| `YNAB_BROKERAGE_ACCOUNT_ID` | — | CLI `--brokerage-sync` | Tracking account for the IBKR NAV. |
| `IBKR_BASE_URL` | `https://localhost:5050` | CLI | IB Client Portal Gateway. TLS is verified except for localhost, 127.0.0.1 and ::1. |
| `IBKR_ACCOUNT_ID` | first account of the gateway | CLI | For example `U1234567`. |

### Web UI

| Variable | Default | Notes |
|---|---|---|
| `WEB_UI_ENABLED` | `0` | `1`, `true`, `yes` or `on` starts the web server inside the bot process. |
| `WEB_UI_HOST` | `0.0.0.0` | Bind address inside the container. |
| `WEB_UI_PORT` | `8080` | |
| `WEB_UI_PUBLIC_URL` | required when enabled | For example `https://bot.example.com`, without a query or fragment; a trailing `/` is removed. Telegram only accepts public URLs for the sign-in button. |
| `WEB_UI_SECRET_KEY` | required when enabled | At least 32 characters. HMAC key for login tokens, sessions and CSRF. Changing it signs everyone out. |
| `WEB_UI_LOGIN_TTL` | `300` | Seconds a `/login` button stays valid. |
| `WEB_UI_SESSION_TTL` | `1800` | Idle timeout in seconds (sliding). |
| `WEB_UI_SESSION_ABS_TTL` | `43200` | Hard session limit in seconds (at least `WEB_UI_SESSION_TTL`; every TTL is at most 400 days). |
| `WEB_UI_ALLOWED_IPS` | empty (all) | IPs or networks, separated by commas or spaces, for example `203.0.113.7, 192.168.1.0/24`. Fails closed: one bad entry keeps the web UI off. |
| `WEB_UI_TRUSTED_PROXIES` | `*` | Proxies whose `X-Forwarded-For` is trusted (uvicorn `forwarded_allow_ips`; IPs or networks, comma-separated). `*` trusts any client that reaches port 8080 directly. On production the compose network is shared with other apps, so any container on it can reach `revolut-ynab-bot:8080` and spoof `X-Forwarded-For`. Today that only affects the `/auth` rate-limit key and logged IPs, but it would bypass `WEB_UI_ALLOWED_IPS`: with an allowlist, set Caddy's IP here (and give Caddy a fixed address). |

The web UI only starts when all of these are valid. Otherwise the bot runs without it, and the
startup DM says `web UI: disabled (configuration problem: …)`.

### Logging, paths and runtime

| Variable | Default | Used by | Notes |
|---|---|---|---|
| `LOG_LEVEL` | `INFO` | all | `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL` (`WARN` and `FATAL` are accepted). Audit lines (one per YNAB write) are logged at INFO regardless of this setting. |
| `LOG_FILE` | `~/.revolut_to_ynab.log` | all | Rotated at 5 MB, 5 old files kept. Set it to an empty value to disable the log file (and `/logs`). In Docker use `/app/bot_data/bot.log`, so the host-side log is `bot_data/bot.log` as in v1; with the default it would be `bot_data/.revolut_to_ynab.log`, because inside the container `~` is `/app/bot_data`. |
| `CSV_DIR` | `~/Downloads` | CLI | Where the CLI looks for the newest statement. |
| `BOT_DATA_DIR` | `<repo>/bot_data` (`/app/bot_data` in the image) | bot, web, entrypoint, health check | Used as given, with no `~` expansion. |
| `APP_UID`, `APP_GID` | `10001` | container entrypoint | The unprivileged user the container switches to (0 is refused). |
| `HEALTHCHECK_MAX_AGE` | `180` | health check | Seconds the bot's heartbeat may be old. |
| `TZ` | set by compose (`Europe/Zurich`) | all | Local "today" for adjustment dates and the web UI's month boundaries. |
| `BUILD_SHA`, `BUILD_DATE` | `dev`, `local` | version line | Set by the image build. Do not set them in `bot.env`. |

---

## Telegram commands

The bot only talks in private chats. Every message and every button press goes through one
authorization gate. Strangers who write to the bot are registered as `pending`, and the admin
is notified. Buttons from strangers, pending users and denied users do nothing.

### User commands

| Command | What it does |
|---|---|
| *(send a `.csv` file)* | Imports a Revolut statement (see [How imports work](#how-imports-work)). The reply lists the counts, the changes, the removals, skipped rows, warnings and failures, followed by the account balance. If some writes failed, it ends with "⚠ Partially applied — send the file again to retry". The bot accepts up to 25 MiB, but Telegram only lets bots download files up to 20 MB; upload a larger statement in the web UI. |
| `/reconcile` | **Two steps.** It first shows a preview: the statement balance on the statement date, YNAB's cleared balance on that date, the difference and any warnings. The buttons are *Post adjustment* and *Cancel*. Only *Post adjustment* creates a `CSV Reconciliation` transaction (cleared `reconciled`, dated the statement date). The newest statement must have been imported successfully; otherwise you are asked to upload it again. The adjustment does not lock other transactions. |
| `/cleanup_pending` | Removes leftover `(pending)` markers from the memos of cleared or reconciled transactions. It never changes the cleared state and never touches uncleared transactions. |
| `/dedupe` | Finds importer transactions (`YNAB:…` import ids) inside the newest imported statement's dates that no statement row accounts for. You tick rows in a picker and press *Delete*. A **confirmation view** then lists exactly the selected rows, and only *Yes, delete* deletes them (at most 200 per run). |
| `/dedupe_delete 1,3-5` or `all` | Selects by number and then shows the same confirmation view; it never deletes directly. Not in the menu. |
| `/dedupe_cancel` | Discards the selection. Not in the menu. |
| `/status` | Shows the working, cleared and uncleared balance, the last statement and its import status, the import ledger counts and the last sync. |
| `/setup` | Re-runs the YNAB setup (token → budget → account) with buttons. Your previous setup stays saved, and `/cancel` restores it. While the setup is open, other commands (imports, `/reconcile`, `/status`, …) are refused, and the web UI does not accept your session (it needs a finished setup). The message with your token is deleted. Changing the token, budget or account signs you out of the web UI. |
| `/settings` | A menu for auto-approve, crypto setup and YNAB setup. |
| `/auto_approve [on\|off]` | Whether imported transactions are approved. When off, they land in YNAB's inbox for review. |
| `/login` | Sends a **sign-in button** for the web UI. It works once, expires after `WEB_UI_LOGIN_TTL`, and is deleted after you sign in. The link is never in the message text, so do not forward the message. |
| `/logout_all` | Signs you out of the web UI everywhere: all sessions and unused sign-in buttons. |
| `/crypto` | Values the configured wallets in CHF and posts the difference to the tracking account. |
| `/crypto_setup` | Sets the tracking account, the BTC xpub or address and the ETH address. Each step offers *Keep current*, *Skip* and *Cancel*, and nothing is saved until the end. |
| `/crypto_status` | Shows the crypto configuration (masked). |
| `/cancel` | Leaves the current setup step. |
| `/help`, `/start` | The command list and the running version. |

### Admin commands

Admin commands exist only for `TELEGRAM_ADMIN_ID`. For everyone else they are unknown commands.

| Command | What it does |
|---|---|
| `/approve <id>` | Moves a pending or denied user to the token step and sends them a welcome message. Users who have already completed setup are left alone. |
| `/deny <id>` | Blocks the user, signs them out of the web UI and drops their open flows. You cannot deny yourself. |
| `/revoke <id>` | Signs a user out of the web UI without blocking them. |
| `/users` | Lists the users and their states. |
| `/logs [N]` | Sends the last N lines of the log as a file (default 500, at most 100,000, reading rotated files as needed). Unavailable only when `LOG_FILE` is set to an empty value. |
| `/ip` | Shows the host's public IP and hostname. |

Buttons from v1 messages still in your chats either work (`/settings` menus) or expire and
lose their keyboard (old `/dedupe` pickers).

---

## Web UI

Enabled with `WEB_UI_ENABLED=1`. Sign in with `/login` in the bot. The button opens
`/auth?t=…`, and a small page there submits the token. If the browser is already signed in,
that page asks before switching accounts. Sessions expire after 30 minutes idle and 12 hours
at most.

| Page | What it shows |
|---|---|
| Dashboard | Working, cleared and uncleared balance of the import account, transaction and ledger counts, the last statement and its import status. |
| Accounts | Every account of the budget from the YNAB mirror (closed and deleted ones listed separately). |
| Transactions | Everything YNAB has, for all accounts, with filters for text (payee and memo), state (cleared, reconciled or uncleared), category, account and date range, plus sorting and pagination. |
| Spending | Spending per category for the last N complete months plus the current month. Transfers, adjustments and split parents are excluded, and refunds are netted. |
| Subscriptions | Recurring charges (weekly to yearly), price changes and the monthly run-rate. By default it looks back 13 months, so yearly charges are found. |
| Upload | Import a statement (same import as in Telegram) and see the structured result. |
| Reconcile | The preview, then a confirmation dialog (*Post adjustment of ±X*). |
| Dedupe | Scan, select, confirm. Deletes in chunks of 200. |
| Settings | Auto-approve, the active YNAB setup, *Sign out everywhere*. |

The data comes from a local mirror of YNAB. Pages refresh it in the background (at most every
30 s), and *Refresh from YNAB* / *Sync* forces a refresh (at most once per 10 s). YNAB allows 200
requests per hour per token. When YNAB answers "rate limited", syncs pause until its
`Retry-After` has passed, up to an hour.

Security in short: the app sends a strict Content-Security-Policy (only `'self'`, with no
inline scripts or CDN). Request bodies are size-limited before authentication. Every `/api`
POST needs the CSRF header. Tokens are stored as HMACs. Details are in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#security-model).

---

## CLI

`python3 revolut_to_ynab.py` keeps every v1 option. It uses its own database
(`~/.revolut_to_ynab.db`, or `--db-path`) and the `YNAB_*` settings. `--help` lists all options.

```bash
python3 revolut_to_ynab.py --list-budgets
python3 revolut_to_ynab.py --list-accounts --budget-id <id>
python3 revolut_to_ynab.py ~/Downloads/account-statement_2026-09-01_2026-10-16.csv
python3 revolut_to_ynab.py                 # newest statement in CSV_DIR, asks to confirm
python3 revolut_to_ynab.py -y              # same, without asking
python3 revolut_to_ynab.py statement.csv --dry-run      # show the plan, write nothing
python3 revolut_to_ynab.py statement.csv --skip-pending # do not create pending rows
python3 revolut_to_ynab.py --watch ~/Downloads          # import new statements as they appear
python3 revolut_to_ynab.py --sync                        # refresh the local YNAB mirror
python3 revolut_to_ynab.py --reconcile                   # preview, then asks before posting
python3 revolut_to_ynab.py --cleanup-pending-memos [--dry-run]
python3 revolut_to_ynab.py --crypto-sync [--dry-run] [--allow-zero]
python3 revolut_to_ynab.py --brokerage-sync [--dry-run]
python3 revolut_to_ynab.py --db-stats
```

- If several mode flags are given, v1's precedence decides which mode runs (`--db-stats` >
  `--list-budgets` > `--list-accounts` > `--sync` > `--crypto-sync` > `--brokerage-sync` >
  `--watch` > `--cleanup-pending-memos` > `--reconcile` > import). Options the chosen mode
  ignores are reported as warnings.
- `--since-date` is accepted but ignored, because syncs are incremental now.
  `--skip-pending` only stops creating pending rows; it never deletes pendings already in
  YNAB. `--dry-run` writes nothing to YNAB or the import ledger.
- `--watch` imports a file only once its size is stable, retries failed imports later, and
  waits out YNAB's rate limit.
- `--reconcile` reconciles against the newest **imported** statement. If you name a file, it
  must be that statement.
- Exit codes: `0` success; `1` configuration or usage error, or a declined prompt; `2` the
  operation failed or was only partly applied; `130` interrupted with Ctrl+C.

Inside the container, run the CLI through the shim. It drops root privileges the way the
entrypoint does, so it never leaves root-owned files in `bot_data/`:

```bash
docker exec revolut-ynab-bot python3 revolut_to_ynab.py --db-stats
docker exec revolut-ynab-bot python3 -m ynab_helper.container cli --list-budgets
```

Do not use `python3 -m ynab_helper …` with `docker exec`: it runs as root. Inside the container
the CLI's database is `/app/bot_data/.revolut_to_ynab.db`, which is separate from the bot
users' databases and has no imported statement, so `--reconcile` there fails. Use the bot's
`/reconcile`, or see [OPERATIONS.md](docs/OPERATIONS.md#running-commands-in-the-container)
for `--db-path` and its caveats.

---

## How imports work

**Identity.** Every Revolut row gets the same YNAB `import_id` as in v1:
`YNAB:<amount in milliunits>:<date>:<n>`, where `n` counts rows with the same amount on the
same day, in file order. YNAB refuses a second transaction with the same import id. That id
is not a stable identity, though. The date follows the timezone your phone was in when you
exported, and `n` shifts when a pending row completes, because Revolut sorts by completion
date. So v2 keeps its own **import ledger** per YNAB account. It recognises a row by its
**started timestamp and amount**, corrected for the export's timezone offset (learned from
rows both exports share). For rows that v1 imported without a timestamp, it falls back to
key, amount and date. A row seen before is updated; a row never seen is created. Statement
rows for other Revolut products (savings pockets, deposits) are skipped and reported.

**What a statement changes in YNAB:**

| Situation | What happens |
|---|---|
| New completed row | Created as cleared. Approved if auto-approve is on. |
| New pending row | Created as uncleared, with `(pending)` in the memo. |
| Pending row completed | Set to cleared, and the `(pending)` marker is removed from the memo; the rest of the memo is kept. If it settled at a different amount (FX, tip), the amount is updated, unless you changed the amount in YNAB. |
| Pending row vanished (settled under other rows) | Deleted from YNAB, but only if it is still uncleared there and the statement covers its date. |
| Reverted row | Deleted from YNAB if it is still uncleared there; otherwise left alone with a warning. A reverted row that was never imported is never created. |
| Export made in another timezone | Rows are recognised. If a row's date moved, YNAB is re-dated, but only if YNAB still has the date we wrote and the transaction is not reconciled. |
| Older statement sent again | Nothing is downgraded, deleted or re-created. Its pending rows change nothing. |
| `DECLINED`, `FAILED` and other states | Skipped and reported. |

**What is never touched:**

- A cleared or reconciled state is never changed. The importer only moves uncleared to
  cleared, and `/cleanup_pending` never changes the cleared state.
- Memos you wrote: only the `(pending)` marker the importer added is ever removed.
- Your edits: payee, category, approval and flags are never sent after creation. An amount
  or date you changed in YNAB is not overwritten (the import reports a conflict). Split and
  reconciled transactions never get amount or date changes.
- Transactions you deleted in YNAB are never re-created. Transactions you moved to another
  account are left alone.
- Transactions the importer did not create.

**Safety rails.** Before planning, every import syncs the YNAB mirror, so each write is
computed from YNAB's current state; if YNAB is rate limiting, the import stops before writing
anything. The statement's currency must match the budget's. The account must still exist and
be open. If some writes fail, the rest still run, local state only records what YNAB
confirmed, and sending the same file again finishes the job. Each YNAB write is logged as an
audit line in `bot.log`.

---

## Crypto and IBKR

- **Crypto** (`/crypto`, `--crypto-sync`): BTC from an xpub, ypub or zpub (derived locally,
  with each script type scanned as it should be; gap limit 20; balances from Blockstream) or
  from a single address; ETH and ERC-20 balances over public JSON-RPC endpoints with failover;
  CHF prices from CoinGecko. It posts one `Crypto Portfolio Adjustment`
  (`CRYPTO:<delta>:<date>:1`) so the tracking account's working balance matches the
  portfolio. It refuses to post when a price is missing, when the budget is not in CHF, or
  when the BTC wallet shows no transactions at all while YNAB has a balance. Only the CLI's
  `--allow-zero` overrides that last check.
- **IBKR** (CLI only, `--brokerage-sync`): reads the NAV in CHF from a running IB Client
  Portal Gateway and posts an `IBKR Portfolio Adjustment`. A NAV of zero or less is never
  posted.

Differences under 0.01 count as in sync, and nothing is posted. The import id is v1's
(`PREFIX:<delta>:<date>:1`), so a second adjustment with exactly the same amount on the same
day is refused by YNAB as a duplicate. The bot reports that; it is not an error.

---

## Development

Python 3.12. The dependencies are hash-locked (`requirements*.txt`, compiled with uv).

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python --require-hashes -r requirements-dev.txt

.venv/bin/pytest -q                       # ~4,200 tests, offline (network is blocked)
.venv/bin/ruff check . && .venv/bin/ruff format --check .
bash scripts/build_css.sh                 # rebuild web/static/app.css after changing classes
bash scripts/build_css.sh --check         # what CI runs; fails if app.css is stale
.venv/bin/python -m ynab_helper.web.devserver   # web UI on 127.0.0.1:8766 with fake data
```

- The devserver seeds a temporary data directory and serves the real app against an
  in-process fake YNAB. It prints a sign-in URL, and `/dev/login` mints a new one.
- `scripts/build_css.sh` downloads the pinned Tailwind standalone CLI (v3.4.17,
  sha256-verified) into `.cache/`. Use `--offline` to forbid the download. The built CSS is
  committed.
- Run the bot or CLI from a checkout with `python3 revolut_ynab_bot.py`,
  `python3 revolut_to_ynab.py` or `python3 -m ynab_helper [bot|cli|selfcheck]`, using a `.env`
  in the repository root.
- Build an image locally with
  `docker build -t ynab-helper:dev .`, check it with `docker run --rm ynab-helper:dev selfcheck`
  and `bash scripts/ci_smoke.sh ynab-helper:dev`.
- Every push to `main` that changes code deploys to production within minutes. Work on a
  branch, open a PR and let CI pass; see [docs/OPERATIONS.md](docs/OPERATIONS.md#deploy-flow).
