# YNAB Helper

A personal-finance assistant that pulls data from Revolut, your Ledger/Ethereum wallets, and Interactive Brokers into [YNAB](https://www.ynab.com/). Runs either as a **Telegram bot** (recommended — multi-user, phone-first, no laptop required) or as a **command-line tool** on your own machine.

- **Telegram bot** — Share a Revolut CSV straight from your phone to the bot and it lands in YNAB. Run `/crypto` to refresh your wallet-backed tracking account. Multi-tenant: each user has their own YNAB config and data.
- **CLI** — Everything the bot does, scripted. Useful for local imports, cron jobs, or power-user workflows with IBKR, watch folders, and reconciliation.

## Contents

- [Quick start (Telegram bot via Docker)](#quick-start-telegram-bot-via-docker)
- [Commands](#commands)
- [Crypto portfolio sync](#crypto-portfolio-sync)
- [Interactive Brokers NAV sync](#interactive-brokers-nav-sync)
- [CLI mode](#cli-mode)
- [Configuration reference](#configuration-reference)
- [Operations](#operations)
- [Architecture](#architecture)
- [Security & privacy](#security--privacy)
- [Development](#development)

---

## Quick start (Telegram bot via Docker)

This is the canonical deployment: a small Linux host running Docker, pulling the prebuilt image from GHCR. AWS Lightsail ($5/month), Hetzner, a Raspberry Pi, a Synology NAS, or any VPS will all work.

### 1. Create the Telegram bot

1. In Telegram, search for **@BotFather** → send `/newbot` → follow the prompts → copy the bot token.
2. Search for **@userinfobot** → send `/start` → copy your numeric user ID. That's your admin ID.

#### Optional — polish the bot's profile in BotFather

The bot pushes its slash-command list to Telegram automatically on every startup (via `setMyCommands`), so users get autocomplete when they type `/` — and admin-only commands are scoped to the admin chat. The remaining BotFather settings are pure UX polish; do them once:

| BotFather menu | What to set |
| --- | --- |
| `/mybots` → *Edit Bot* → *Edit Description* | Short intro shown above an empty chat. e.g. *"Forward your Revolut CSV → it lands in YNAB."* |
| *Edit About* | One-liner shown in the bot's profile. e.g. *"Personal Revolut → YNAB importer with crypto + IBKR sync."* |
| *Edit Botpic* | A 512×512 image. The Revolut/YNAB logos work fine; or anything recognizable. |
| `/setcommands` | **Skip** — the bot does this itself. Don't override it here or your edits will be overwritten on the next restart. |
| *Bot Settings* → *Menu Button* | Default ("show commands") is fine. The bot's commands feed straight into this. |

### 2. Prepare the host

On a fresh Ubuntu 24.04 VM (Lightsail, EC2, Hetzner, etc.):

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
```

### 3. Configure the bot

```bash
mkdir -p ~/ynab-bot && cd ~/ynab-bot
mkdir -p bot_data
cat > bot.env <<'EOF'
# ── Telegram (required) ────────────────────────────
TELEGRAM_BOT_TOKEN=123456:ABC-DEF1234...
TELEGRAM_ADMIN_ID=123456789

# ── Auto-register admin (optional — skip in-chat onboarding for yourself) ──
YNAB_TOKEN=your-ynab-pat
YNAB_BUDGET_ID=your-budget-id
YNAB_ACCOUNT_ID=your-revolut-account-id

# ── Auto-register admin crypto config (optional) ──
YNAB_CRYPTO_ACCOUNT_ID=your-tracking-account-id
CRYPTO_BTC_XPUB=xpub6C8BFPB...
CRYPTO_ETH_ADDRESS=0x2a1EC41E...
# ANKR_API_KEY=                         # only if ETH public RPCs rate-limit you

# ── Logging ────────────────────────────────────────
LOG_LEVEL=INFO
LOG_FILE=/app/bot_data/bot.log
EOF

curl -O https://raw.githubusercontent.com/Cyrill-Meier/YNAB_Helper/main/docker-compose.yml
```

If you don't yet know your budget/account IDs, leave the `YNAB_*` lines out — new users (including you) can onboard entirely in chat with `/setup`.

### 4. Start it

```bash
docker compose up -d
```

The bot sends you a startup DM with the running version number. Watchtower polls GHCR every 5 minutes and auto-updates the bot on new releases.

Stop everything with `docker compose down`. Persistent data lives in `./bot_data/` and survives restarts and image updates.

---

## Commands

### User commands

Everything below is sent from your personal Telegram to the bot.

| Command | What it does |
|---|---|
| *(send a CSV attachment)* | Import a Revolut statement into YNAB. The bot diffs against past imports so duplicates are skipped. |
| `/reconcile` | Reconcile the YNAB balance against the last CSV you uploaded. |
| `/cleanup_pending` | Scan YNAB and strip `(pending)` memos from transactions that have since cleared. Useful after a fresh deploy or DB wipe. |
| `/status` | Show current YNAB balance and last-import state. |
| `/setup` | Re-run onboarding — change your YNAB token, budget, or account. |
| `/crypto` | Refresh BTC + ETH + ERC-20 balances, update the tracking account. |
| `/crypto_setup` | Configure your BTC xpub, ETH address, and tracking account. |
| `/crypto_status` | Show the currently configured crypto wallets (redacted). |
| `/help` | List commands plus the running version. |

### Admin commands

Only the user whose ID matches `TELEGRAM_ADMIN_ID` can run these.

| Command | What it does |
|---|---|
| `/approve <user_id>` | Approve a pending user — they get a welcome DM and move into onboarding. |
| `/deny <user_id>` | Deny a pending user — they receive a polite rejection. |
| `/users` | List all registered users with their current state (`ready`, `awaiting_token`, etc.). |

### New-user onboarding flow

1. New user messages the bot with `/start`.
2. Bot replies with "request sent" and DMs the admin with the user's name + ID.
3. Admin runs `/approve <user_id>` (or `/deny <user_id>`).
4. Approved user is prompted for their **YNAB Personal Access Token** — get it at [app.ynab.com](https://app.ynab.com) → *Account Settings* → *Developer Settings* → *New Token*.
5. Bot validates the token and offers a list of budgets — the user picks one.
6. Bot lists accounts in that budget — the user picks their Revolut account.
7. Done — the user can now send CSVs and run all commands.

Users can re-run `/setup` at any time to switch budget/account without admin involvement.

---

## Crypto portfolio sync

Track the on-chain value of your Ledger (or any BTC/ETH/ERC-20) portfolio in a YNAB **tracking account**. No exchange API keys, no write access to wallets — xpubs and public addresses only.

### What gets synced

- **BTC** — all addresses derived locally from your BIP84 xpub (native SegWit `bc1…`), queried via Blockstream. Scans until 20 consecutive unused addresses (standard gap limit).
- **ETH** — native balance via public JSON-RPC (8 fallback endpoints; authenticated Ankr first if `ANKR_API_KEY` is set).
- **ERC-20 tokens** — AAVE, USDC, USDT, and aUSDT from the same ETH address.
- **Prices** — CoinGecko (public API, no key), in CHF.
- **Write to YNAB** — a single adjustment transaction in the tracking account so its balance matches the on-chain portfolio value. The `import_id` includes the date, so re-runs on the same day deduplicate.

### Setup (via the bot)

1. In YNAB, add a **Tracking → Asset** account (e.g. "Crypto").
2. In Telegram, run `/crypto_setup`.
3. Pick that account from the list the bot shows you.
4. Paste your **BTC xpub** (or a single bech32 address) — or type `skip` to disable BTC tracking.
5. Paste your **Ethereum address** (`0x…`) — or `skip`.
6. Run `/crypto` any time to refresh. The bot fetches, prices, adjusts, and DMs back a summary.

### Where to get an xpub

- **Ledger Live** → select your Bitcoin account → *Edit* (wrench icon) → *Advanced* → *Extended Public Key* → copy.
- The xpub lets anyone **view** all your addresses and balances — it does **not** allow spending.

### Troubleshooting

- **"Blockstream error: HTTP 429"** — rate-limited. The sync retries with exponential backoff (1/2/4/8 s) up to 5 times before giving up. If your host IP has been hammered recently, wait 10 minutes and try again, or deploy on a different host.
- **"All ETH RPC endpoints failed"** — all 8 public endpoints are down or blocking you. Sign up at [ankr.com/rpc](https://www.ankr.com/rpc/) (free tier), drop the key into `ANKR_API_KEY`, and restart.
- **`/crypto_status` shows nothing** — onboarding didn't complete. Re-run `/crypto_setup`.

---

## Interactive Brokers NAV sync

Track your IBKR portfolio Net Asset Value in a YNAB tracking account. **CLI only** for now — requires the IB Client Portal Gateway running locally on the same machine as the script.

### Prerequisites

1. Download the **[IB Client Portal Gateway](https://www.interactivebrokers.com/en/trading/ib-api.php)**.
2. Start it: `bin/run.sh root/conf.yaml` (listens on port 5050).
3. Authenticate in your browser at `https://localhost:5050`.

### Config

```dotenv
YNAB_BROKERAGE_ACCOUNT_ID=your-brokerage-tracking-account-id
# Optional — auto-detected if omitted:
IBKR_ACCOUNT_ID=U1234567
# Optional — default is https://localhost:5050:
IBKR_BASE_URL=https://localhost:5050
```

### Run

```bash
python3 revolut_to_ynab.py --brokerage-sync              # real
python3 revolut_to_ynab.py --brokerage-sync --dry-run    # preview only
```

The script pulls total NAV in CHF, compares to the YNAB tracking account, and posts an adjustment transaction for the difference.

---

## CLI mode

Everything the bot does (minus approvals) is available from the command line. Useful for cron, scripting, or a single-user setup without Telegram.

### First-time setup

```bash
cp .env.example .env        # fill in YNAB_TOKEN, IDs, etc.
python3 revolut_to_ynab.py --list-budgets
python3 revolut_to_ynab.py --list-accounts --budget-id <id>
```

### Common invocations

```bash
# One-shot import
python3 revolut_to_ynab.py ~/Downloads/account-statement*.csv

# Dry run (no writes to YNAB)
python3 revolut_to_ynab.py --dry-run ~/Downloads/account-statement*.csv

# Let the script auto-pick the newest CSV in CSV_DIR (~/Downloads by default)
python3 revolut_to_ynab.py

# Watch mode — auto-import when a new CSV appears in a folder
python3 revolut_to_ynab.py --watch ~/Downloads

# Sync remote YNAB changes into the local dedup DB
python3 revolut_to_ynab.py --sync --since-date 2026-01-01

# Reconcile local state against YNAB balance
python3 revolut_to_ynab.py --reconcile

# Strip stale "(pending)" memos from transactions that have since cleared
python3 revolut_to_ynab.py --cleanup-pending-memos
python3 revolut_to_ynab.py --cleanup-pending-memos --dry-run

# Crypto sync
python3 revolut_to_ynab.py --crypto-sync
python3 revolut_to_ynab.py --crypto-sync --dry-run

# Non-interactive (skip all prompts, e.g. in cron)
python3 revolut_to_ynab.py -y
```

### All flags

Run `python3 revolut_to_ynab.py --help` for the full list. Notable ones:

- `--skip-pending` — ignore pending transactions (they often re-export with different amounts).
- `--db-stats` — show local dedup DB statistics.
- `--db-path` / `--log-file` / `--log-level` — override paths and logging verbosity.
- `--csv-dir` — look for new CSVs somewhere other than `~/Downloads`.

---

## Configuration reference

All settings can be provided via environment variables (exported, `.env` file, or Docker `env_file`). Shell-exported values take precedence over `.env`.

### Core YNAB

| Variable | Purpose |
|---|---|
| `YNAB_TOKEN` | Personal Access Token. Required for CLI; optional for bot (each bot user has their own). |
| `YNAB_BUDGET_ID` | Budget containing your Revolut account. |
| `YNAB_ACCOUNT_ID` | YNAB account ID for your Revolut account. |

### Crypto sync (optional)

| Variable | Purpose |
|---|---|
| `YNAB_CRYPTO_ACCOUNT_ID` | Tracking account ID to hold the on-chain portfolio value. |
| `CRYPTO_BTC_XPUB` | BIP84 extended public key (`xpub…`, `ypub…`, `zpub…`) or a single bech32 address. |
| `CRYPTO_ETH_ADDRESS` | Ethereum address. Used for native ETH + AAVE/USDC/USDT/aUSDT. |
| `ANKR_API_KEY` | Optional — prepends an authenticated Ankr RPC endpoint ahead of the public fallbacks. |

### Interactive Brokers sync (CLI only)

| Variable | Purpose |
|---|---|
| `YNAB_BROKERAGE_ACCOUNT_ID` | Tracking account ID for brokerage NAV. |
| `IBKR_BASE_URL` | IB Client Portal Gateway URL. Default `https://localhost:5050`. |
| `IBKR_ACCOUNT_ID` | IB account like `U1234567`. Auto-detected from the gateway if omitted. |

### Telegram bot

| Variable | Purpose |
|---|---|
| `TELEGRAM_BOT_TOKEN` | From @BotFather. |
| `TELEGRAM_ADMIN_ID` | Your numeric Telegram user ID — the admin who approves/denies new users. |
| `BOT_DATA_DIR` | Where the bot keeps its databases. Default `./bot_data/`. Mounted as a Docker volume. |

### Logging & runtime

| Variable | Purpose |
|---|---|
| `LOG_LEVEL` | `DEBUG` / `INFO` (default) / `WARNING` / `ERROR`. |
| `LOG_FILE` | Path to a log file. Empty string disables file logging. In Docker, use a path under `/app/bot_data/` so it survives. |
| `CSV_DIR` | Folder the CLI scans for auto-picked Revolut CSVs. Default `~/Downloads`. |
| `PYTHONUNBUFFERED` | Set to `1` in Docker so logs stream to `docker logs` in real time. Already set in the provided `docker-compose.yml`. |
| `TZ` | Timezone (e.g. `Europe/Zurich`) — affects timestamps in logs and dedup IDs. |

---

## Operations

### Check the running version

In Telegram, send `/help` — the first line is the version (e.g. `v1.1.6 (cfabb75, 2026-04-16 20:06 UTC)`). From the host:

```bash
docker exec revolut-ynab-bot printenv BUILD_SHA BUILD_DATE
```

### View logs

```bash
# Live, real-time
docker logs -f revolut-ynab-bot

# Last 200 lines
docker logs --tail=200 revolut-ynab-bot

# Persistent file log (if LOG_FILE=/app/bot_data/bot.log)
tail -f ~/ynab-bot/bot_data/bot.log
```

Set `LOG_LEVEL=DEBUG` in `bot.env` and restart (`docker compose up -d`) to get noisier logs when debugging.

### Force an immediate update

Watchtower polls every 5 minutes. To pull a newly built image right now:

```bash
cd ~/ynab-bot
docker compose pull
docker compose up -d --force-recreate revolut-ynab-bot
```

Or trigger Watchtower manually:

```bash
docker exec watchtower /watchtower --run-once --cleanup
```

### Inspect the database

The user DB is `bot_data/bot_users.db`. The bot's slim image has no `sqlite3` CLI, so use Python instead:

```bash
docker exec revolut-ynab-bot python3 -c "
import sqlite3
c = sqlite3.connect('/app/bot_data/bot_users.db')
c.row_factory = sqlite3.Row
for r in c.execute('SELECT telegram_user_id, state, budget_name, account_name FROM users'):
    print(dict(r))"
```

### Run a crypto sync interactively (skip the Telegram round-trip)

Useful for seeing full output when `/crypto` reports a cryptic failure:

```bash
docker exec -it revolut-ynab-bot python3 -c "
import sys, sqlite3, traceback
sys.path.insert(0, '/app')
import revolut_to_ynab as r
c = sqlite3.connect('/app/bot_data/bot_users.db')
c.row_factory = sqlite3.Row
row = dict(c.execute(\"SELECT * FROM users WHERE state='ready' LIMIT 1\").fetchone())
try:
    r.crypto_sync(row['ynab_token'], row['budget_id'], row['crypto_account_id'],
                  btc_xpub=row.get('btc_xpub'), eth_address=row.get('eth_address'))
except Exception:
    traceback.print_exc()
"
```

### Rotate or prune disk

```bash
docker system prune -af --volumes     # remove stopped containers + dangling images
docker image prune -f                  # just unused images
```

Rotate the bot log file if it grows large:

```bash
mv ~/ynab-bot/bot_data/bot.log ~/ynab-bot/bot_data/bot.log.$(date +%F)
docker compose restart revolut-ynab-bot
```

### Private GHCR image

If the repo (and therefore its container image) is private:

```bash
docker login ghcr.io -u Cyrill-Meier -p <GITHUB_PAT_WITH_PACKAGES_READ>
```

Watchtower picks up credentials from `~/.docker/config.json` automatically. Alternatively, flip the image package to public under *Package settings → Danger Zone → Change visibility*.

---

## Architecture

### Files

- `revolut_to_ynab.py` — the importer and all sync logic (Revolut parsing, YNAB API, crypto, IBKR). Usable standalone via CLI; also imported by the bot.
- `revolut_ynab_bot.py` — Telegram bot front-end. Handles approvals, onboarding state machine, and per-user execution of the importer.
- `Dockerfile` — python:3.12-slim base + pycryptodome (required for BTC xpub → bech32 derivation on Debian bookworm, where OpenSSL 3 has disabled legacy RIPEMD160). Build args `BUILD_SHA` and `BUILD_DATE` bake the commit SHA and timestamp into the image so `/help` can display them.
- `docker-compose.yml` — the bot + Watchtower auto-updater, scoped by label.
- `.github/workflows/docker-publish.yml` — builds and publishes `ghcr.io/cyrill-meier/ynab_helper:latest` on push to `main`.

### Data storage

Bot data lives in the `bot_data/` directory (mounted as a Docker volume). Each user's data is fully isolated:

- `bot_users.db` — one row per Telegram user with their YNAB token, budget/account selection, crypto config, and onboarding state.
- `transactions_<telegram_id>.db` — per-user dedup state: which YNAB transactions have been synced locally, which Revolut rows have been imported.

### Deduplication

Each transaction gets an `import_id` derived from its date, amount, and payee. YNAB drops duplicates on re-import, so running the same CSV twice is a no-op. The crypto sync uses a date-stamped `import_id` so re-runs within a day update the same adjustment rather than stacking new ones.

**Pending → cleared drift.** When a row is first imported while still pending, its memo gets a `(pending)` tag and `cleared=uncleared`. On a later CSV where that row has cleared, the importer normally sees the state change in the local DB and issues a `PATCH` to strip the marker and flip `cleared`. If the local DB was wiped (fresh VM, deleted volume, etc.), that record is missing — the importer re-POSTs, YNAB returns it as a duplicate, and without intervention the original stale row would stay. The importer now fetches duplicates and patches any whose `cleared` / `memo` / `amount` has drifted. To clean up historic drift that predates this fix, run `/cleanup_pending` (bot) or `--cleanup-pending-memos` (CLI).

### Auto-update flow

1. You push to `main`.
2. GitHub Actions builds + pushes `ghcr.io/cyrill-meier/ynab_helper:latest` tagged with the short SHA.
3. Watchtower (polling every 5 min) sees the new image, pulls it, stops the old bot container, starts the new one.
4. `bot_data/` persists across swaps via the Docker volume.

---

## Security & privacy

- **xpubs and public addresses are read-only.** They reveal balances and transaction history; they cannot spend. Still, exposing an xpub compromises your on-chain privacy (anyone with it can see all past and future addresses you'll generate). Don't commit one to a public repo.
- **YNAB Personal Access Tokens are stored in plain text** in the bot's SQLite database. Anyone with shell access to the host (or the `bot_data/` directory) can read them. Protect the host accordingly — SSH keys only, disk at rest encryption, restrict who has shell access.
- **Multi-tenant isolation** — user A cannot see or touch user B's data through the bot UI. Shell access to the host bypasses this boundary.
- **Approve/deny gate** — only the admin (`TELEGRAM_ADMIN_ID`) can onboard new users. Random strangers who message the bot get a pending status and cannot do anything until approved.
- **No outbound telemetry.** The only external services contacted are YNAB, Blockstream, CoinGecko, and the configured ETH RPC endpoints.

---

## Development

### Local Python run

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install pycryptodome             # required for BTC xpub derivation
cp .env.example .env && vim .env
python3 revolut_to_ynab.py --list-budgets
python3 revolut_ynab_bot.py
```

### Build a local image

```bash
docker build -t ynab-helper:dev \
  --build-arg BUILD_SHA=$(git rev-parse --short HEAD) \
  --build-arg BUILD_DATE=$(date -u +%Y-%m-%dT%H:%MZ) \
  .
```

### Release a new version

1. Bump `__version__` at the top of `revolut_ynab_bot.py`.
2. Commit the change with a short rationale.
3. Push to `main`. GitHub Actions builds and publishes. Watchtower picks it up on the next poll (≤ 5 min).
4. Confirm by sending `/help` in Telegram — the version string on the first line is what's now live.

### Project philosophy

- **Boring dependencies.** Only pycryptodome is required beyond the standard library. Makes the image tiny, cold-starts fast, and supply-chain risk low.
- **Fail loud, not silent.** Earlier versions used `sys.exit(1)` for error paths, which breaks inside a bot (SystemExit bypasses `except Exception`). All network helpers now raise `RuntimeError` so the bot surfaces a real error message.
- **Retry transient errors.** Blockstream and ETH RPCs rate-limit. Every external call has bounded retry with exponential backoff before giving up.
- **Multi-tenant by default.** Even if you're the only user, the bot structure (per-user DB rows, per-user token) means adding family members later is a `/approve` away.
