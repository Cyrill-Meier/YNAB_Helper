# Revolut → YNAB Importer — Setup Guide

## Step 1: Get your YNAB Personal Access Token

1. Sign in at [app.ynab.com](https://app.ynab.com)
2. Go to **Account Settings** → **Developer Settings**
3. Click **New Token**, enter your password
4. Copy the token immediately (you won't see it again)

## Step 2: Find your Budget ID and Account ID

```bash
# Set your token
export YNAB_TOKEN="your-token-here"

# List your budgets to find the Budget ID
python3 revolut_to_ynab.py --list-budgets

# List accounts in that budget to find the Account ID for your Revolut account
python3 revolut_to_ynab.py --list-accounts --budget-id "your-budget-id"
```

## Step 3: Save your config (add to ~/.zshrc or ~/.bashrc)

```bash
export YNAB_TOKEN="your-token-here"
export YNAB_BUDGET_ID="your-budget-id"
export YNAB_ACCOUNT_ID="your-account-id"
```

Then run `source ~/.zshrc` to apply.

## Usage

### One-shot import (after AirDropping a CSV)

```bash
python3 revolut_to_ynab.py ~/Downloads/account-statement*.csv
```

### Dry run (preview without importing)

```bash
python3 revolut_to_ynab.py --dry-run ~/Downloads/account-statement*.csv
```

### Watch mode (auto-import when you AirDrop)

```bash
python3 revolut_to_ynab.py --watch ~/Downloads
```

This watches your Downloads folder. When you AirDrop a Revolut CSV from your phone, it gets picked up and imported automatically.

### Skip pending transactions

```bash
python3 revolut_to_ynab.py --skip-pending ~/Downloads/account-statement*.csv
```

## Crypto Portfolio Sync

Track your Ledger crypto portfolio value in a YNAB tracking account. The script fetches your BTC balance from the blockchain, converts it to CHF using the live CoinGecko price, and creates an adjustment transaction for the difference.

### Setup

1. Create a **tracking account** in YNAB for your crypto portfolio (e.g. "Crypto - Ledger")
2. Find its Account ID: `python3 revolut_to_ynab.py --list-accounts`
3. Get your **extended public key (xpub)** from Ledger Live:
   - Open Ledger Live → select your Bitcoin account → click the wrench icon (Edit) → Advanced → copy the xpub
4. Add to your shell config:

```bash
export YNAB_CRYPTO_ACCOUNT_ID="your-crypto-account-id"
export CRYPTO_BTC_XPUB="xpub6C8BFPB..."
export CRYPTO_ETH_ADDRESS="0x2a1EC41E..."
```

The BTC xpub covers all derived addresses from your HD wallet automatically (no need to track individual addresses as Ledger generates new ones). The ETH address is queried for native ETH plus Aave USDT (aUSDT v2/v3) token balances.

### Usage

```bash
# Sync crypto value to YNAB
python3 revolut_to_ynab.py --crypto-sync

# Preview without creating a transaction
python3 revolut_to_ynab.py --crypto-sync --dry-run
```

BTC addresses are derived locally from your xpub and queried via Blockstream. ETH + ERC-20 token balances (AAVE, USDC, USDT, aUSDT) are fetched via public Ethereum RPC. Prices come from CoinGecko. No API keys needed for any of these.

The script is safe to run multiple times per day — it uses an `import_id` that includes the date, so YNAB will deduplicate if run again on the same day with the same delta.

## Interactive Brokers Brokerage Sync

Track your IBKR portfolio Net Asset Value in a YNAB tracking account. Uses the IB Client Portal Gateway running locally on your Mac.

### Prerequisites

1. Download the **IB Client Portal Gateway** from the [IB API page](https://www.interactivebrokers.com/en/trading/ib-api.php)
2. Start the gateway (it runs on port 5050): `bin/run.sh root/conf.yaml`
3. Authenticate in your browser at `https://localhost:5050`

### YNAB Setup

1. Create a **tracking account** in YNAB for your brokerage (e.g. "Interactive Brokers")
2. Find its Account ID: `python3 revolut_to_ynab.py --list-accounts`
3. Add to your shell config:

```bash
export YNAB_BROKERAGE_ACCOUNT_ID="your-brokerage-account-id"
# Optional — auto-detected if omitted:
export IBKR_ACCOUNT_ID="U1234567"
# Optional — default is https://localhost:5050:
export IBKR_BASE_URL="https://localhost:5050"
```

### Usage

```bash
# Make sure IB Gateway is running and authenticated, then:
python3 revolut_to_ynab.py --brokerage-sync

# Preview without creating a transaction
python3 revolut_to_ynab.py --brokerage-sync --dry-run
```

The script pulls the total Net Asset Value (NAV) in CHF from the IB Client Portal API, compares it to your YNAB tracking account, and posts an adjustment transaction for the difference.

## Telegram Bot (multi-tenant)

Skip the AirDrop/download step entirely — share the CSV from Revolut on your phone directly to a Telegram bot. Supports multiple users, each with their own YNAB account.

### Admin Setup

1. Open Telegram → search for **@BotFather** → send `/newbot` → follow the prompts → copy the bot token
2. Search for **@userinfobot** → send `/start` → copy your numeric user ID (you are the admin)
3. Add to your `.env`:

```bash
TELEGRAM_BOT_TOKEN=123456:ABC-DEF1234ghIkl-zyx57W2v...
TELEGRAM_ADMIN_ID=123456789
```

If you also have `YNAB_TOKEN`, `YNAB_BUDGET_ID`, and `YNAB_ACCOUNT_ID` in your `.env`, the admin account is auto-registered and ready to use immediately.

4. Start the bot:

```bash
python3 revolut_ynab_bot.py
```

### New User Onboarding

When a new user messages the bot:

1. They get a "request sent to admin" message
2. You (admin) receive a notification with their name and Telegram ID
3. Reply `/approve <user_id>` to grant access (or `/deny <user_id>`)
4. The user is prompted to enter their YNAB Personal Access Token
5. Bot validates the token and shows their budgets — they pick one
6. Bot shows accounts in that budget — they pick one
7. Done — they can now send CSVs and use all commands

### User Commands

From your phone: Revolut → Export → Share → Telegram → send to your bot.

- **Send a CSV file** — Import transactions into YNAB
- `/reconcile` — Reconcile YNAB balance against the last uploaded CSV
- `/status` — Show current YNAB balance and reconciliation status
- `/setup` — Re-run onboarding (change token / budget / account)
- `/help` — List commands

### Admin Commands

- `/approve <user_id>` — Approve a pending user
- `/deny <user_id>` — Deny a pending user
- `/users` — List all registered users with their states

### Data Storage

The bot stores data in `bot_data/` (configurable via `BOT_DATA_DIR`):

- `bot_users.db` — User settings (Telegram ID, YNAB token, budget/account selection, onboarding state)
- `transactions_<telegram_id>.db` — Per-user transaction tracking (dedup, sync state)

Each user's data is fully isolated.

### Running in the background

To keep the bot running after you close the terminal:

```bash
nohup python3 revolut_ynab_bot.py >> ~/revolut_bot.out 2>&1 &
```

Or create a macOS Launch Agent for automatic startup (save as `~/Library/LaunchAgents/com.revolut-ynab.bot.plist`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.revolut-ynab.bot</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>/path/to/revolut_ynab_bot.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/path/to/YNAB_Helper/</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/revolut-ynab-bot.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/revolut-ynab-bot.log</string>
</dict>
</plist>
```

Then load it: `launchctl load ~/Library/LaunchAgents/com.revolut-ynab.bot.plist`

### Deploy on Synology NAS (Docker)

Run the bot 24/7 on your NAS with automatic updates when you push to GitHub.

**One-time setup on your Synology:**

1. SSH into your NAS and create a directory:

```bash
mkdir -p /volume1/docker/revolut-ynab-bot && cd /volume1/docker/revolut-ynab-bot
```

2. Create a `bot.env` file with your secrets:

```bash
cat > bot.env << 'EOF'
TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
TELEGRAM_ADMIN_ID=123456789

# Optional: auto-register admin (skip onboarding for yourself)
YNAB_TOKEN=your-ynab-token
YNAB_BUDGET_ID=your-budget-id
YNAB_ACCOUNT_ID=your-account-id

LOG_LEVEL=INFO
LOG_FILE=/app/bot_data/bot.log
EOF
```

3. Copy `docker-compose.yml` from the repo (or download it):

```bash
curl -O https://raw.githubusercontent.com/Cyrill-Meier/YNAB_Helper/main/docker-compose.yml
```

4. Start everything:

```bash
docker compose up -d
```

That's it. The bot is running, and Watchtower checks for new images every 5 minutes.

**How auto-deploy works:**

1. You push code to `main` on GitHub
2. GitHub Actions builds a new Docker image and publishes it to `ghcr.io/cyrill-meier/ynab_helper:latest`
3. Watchtower (running on your NAS) detects the new image, pulls it, and restarts the bot
4. Your `bot_data/` directory (user DB, transaction DBs) persists across restarts

**If your GitHub repo is private**, Watchtower needs a token to pull images:

```bash
# On your NAS:
docker login ghcr.io -u Cyrill-Meier -p <GITHUB_PAT_WITH_PACKAGES_READ>
```

Watchtower will pick up the credentials from `~/.docker/config.json` automatically.

**Useful commands:**

```bash
# View bot logs
docker logs -f revolut-ynab-bot

# Restart the bot
docker compose restart revolut-ynab-bot

# Pull latest manually (without waiting for Watchtower)
docker compose pull && docker compose up -d

# Stop everything
docker compose down
```

## How duplicates are handled

Each transaction gets a unique `import_id` based on its amount, date, and payee. If you import the same CSV twice, YNAB will skip already-imported transactions. This makes it safe to run repeatedly.

## Recommended workflow

1. Open Revolut on your phone → Accounts → Export statement as CSV
2. AirDrop it to your Mac (lands in ~/Downloads)
3. Run: `python3 revolut_to_ynab.py` — the script auto-picks the newest `account-statement_*.csv` in `~/Downloads` and asks you to confirm

```text
📄 Latest Revolut CSV found:
   File:       /Users/you/Downloads/account-statement_2026-04-01_2026-04-16_en-us_49ea4a.csv
   Export to:  2026-04-16
   Modified:   2026-04-16 09:12
   Size:       24 KB

Use this file? [Y/n]:
```

Files are ranked by the **second date in the filename** (the export "to" date), so the most recent export wins regardless of mtime. Change the search folder with `CSV_DIR` in `.env` or `--csv-dir /some/path`. Skip the prompt with `-y` / `--yes` when running non-interactively (cron jobs, `&& --reconcile`, etc.).

You can still pass a specific file explicitly: `python3 revolut_to_ynab.py ~/Downloads/account-statement*.csv` — or keep the watcher running in a terminal tab and just AirDrop, which handles things the moment a new file appears.
