#!/usr/bin/env python3
"""
Telegram Bot for Revolut → YNAB Importer  (multi-tenant)

Share a Revolut CSV from your phone → Telegram → bot processes it → done.

User commands:
  (send a CSV file)  — Import transactions into YNAB
  /reconcile         — Reconcile YNAB cleared balance against the last uploaded CSV
  /status            — Show YNAB account balance and last import info
  /setup             — Re-run onboarding (change token / budget / account)
  /crypto            — Sync crypto portfolio value → YNAB tracking account
  /crypto_setup      — Configure BTC xpub, ETH address, crypto tracking account
  /crypto_status     — Show current crypto configuration
  /help              — List available commands

Admin commands:
  /approve <user_id> — Approve a pending user
  /deny <user_id>    — Deny a pending user
  /users             — List all users and their states

Setup:
  1. Message @BotFather on Telegram → /newbot → copy the token
  2. Message @userinfobot → copy your numeric user ID (this is the admin)
  3. Add to your .env (next to revolut_to_ynab.py):
       TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
       TELEGRAM_ADMIN_ID=123456789
  4. Run:  python3 revolut_ynab_bot.py
"""

import io
import json
import os
import sqlite3
import sys
import time
import tempfile
import traceback
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# ─── Import from the main script ────────────────────────────────────────────

_script_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_script_dir))

import revolut_to_ynab as ynab

# ─── Version ────────────────────────────────────────────────────────────────
# Bump __version__ manually for meaningful releases.
# BUILD_SHA and BUILD_DATE are injected at Docker build time from GitHub Actions
# (see Dockerfile ARGs). When running locally from source, they stay "dev".

__version__ = "1.1.6"


def get_version_info():
    """Return a dict with version, build sha, and build date."""
    return {
        "version": __version__,
        "sha": os.environ.get("BUILD_SHA", "dev")[:7] or "dev",
        "date": os.environ.get("BUILD_DATE", "local"),
    }


def format_version_line():
    """Compact one-line version string for /help and logs."""
    v = get_version_info()
    return f"v{v['version']} ({v['sha']}, {v['date']})"


# ─── Telegram API helpers ───────────────────────────────────────────────────

TELEGRAM_API = "https://api.telegram.org/bot{token}"


def tg_request(token, method, data=None, timeout=60):
    """Call a Telegram Bot API method. Returns parsed JSON."""
    url = f"{TELEGRAM_API.format(token=token)}/{method}"
    if data is not None:
        payload = json.dumps(data).encode("utf-8")
        req = Request(url, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
    else:
        req = Request(url)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        body = e.read().decode()
        ynab.log.error("Telegram API error %d: %s", e.code, body)
        return {"ok": False, "description": body}
    except (URLError, OSError) as e:
        ynab.log.error("Telegram network error: %s", e)
        return {"ok": False, "description": str(e)}


def tg_send(token, chat_id, text, parse_mode="Markdown"):
    """Send a text message. Long messages are split at ~4000 chars."""
    chunks = []
    while len(text) > 4000:
        split = text.rfind("\n", 0, 4000)
        if split == -1:
            split = 4000
        chunks.append(text[:split])
        text = text[split:].lstrip("\n")
    chunks.append(text)

    for chunk in chunks:
        data = {"chat_id": chat_id, "text": chunk}
        if parse_mode:
            data["parse_mode"] = parse_mode
        tg_request(token, "sendMessage", data)


def tg_download_file(token, file_id, dest_path):
    """Download a file from Telegram to a local path."""
    info = tg_request(token, "getFile", {"file_id": file_id})
    if not info.get("ok"):
        return False
    file_path = info["result"]["file_path"]
    url = f"https://api.telegram.org/file/bot{token}/{file_path}"
    try:
        with urlopen(Request(url), timeout=30) as resp:
            Path(dest_path).write_bytes(resp.read())
        return True
    except (HTTPError, URLError, OSError) as e:
        ynab.log.error("File download failed: %s", e)
        return False


# ─── User database ──────────────────────────────────────────────────────────

# States: pending → approved → awaiting_token → awaiting_budget → awaiting_account → ready
#         pending → denied
# Crypto sub-flow (only entered from `ready`):
#   ready → awaiting_crypto_account → awaiting_crypto_btc → awaiting_crypto_eth → ready

USER_STATES = ("pending", "approved", "awaiting_token", "awaiting_budget",
               "awaiting_account", "ready", "denied",
               "awaiting_crypto_account", "awaiting_crypto_btc",
               "awaiting_crypto_eth")


def init_user_db(db_path):
    """Create or open the user settings database."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id         INTEGER PRIMARY KEY,
            chat_id             INTEGER,
            username            TEXT,
            first_name          TEXT,
            ynab_token          TEXT,
            budget_id           TEXT,
            budget_name         TEXT,
            account_id          TEXT,
            account_name        TEXT,
            crypto_account_id   TEXT,
            crypto_account_name TEXT,
            btc_xpub            TEXT,
            eth_address         TEXT,
            state               TEXT NOT NULL DEFAULT 'pending',
            temp_data           TEXT,
            created_at          TEXT NOT NULL,
            updated_at          TEXT NOT NULL
        )
    """)
    # Idempotent migration for DBs created before crypto columns existed
    for col in ("crypto_account_id", "crypto_account_name",
                "btc_xpub", "eth_address"):
        try:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()
    return conn


def get_user(conn, telegram_id):
    """Get a user row as a dict, or None."""
    row = conn.execute(
        "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)
    ).fetchone()
    return dict(row) if row else None


def upsert_user(conn, telegram_id, **fields):
    """Insert or update a user. Only provided fields are updated."""
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    existing = get_user(conn, telegram_id)

    if existing:
        sets = []
        vals = []
        for k, v in fields.items():
            sets.append(f"{k} = ?")
            vals.append(v)
        sets.append("updated_at = ?")
        vals.append(now)
        vals.append(telegram_id)
        conn.execute(
            f"UPDATE users SET {', '.join(sets)} WHERE telegram_id = ?", vals
        )
    else:
        fields.setdefault("state", "pending")
        fields["telegram_id"] = telegram_id
        fields["created_at"] = now
        fields["updated_at"] = now
        cols = ", ".join(fields.keys())
        placeholders = ", ".join("?" for _ in fields)
        conn.execute(
            f"INSERT INTO users ({cols}) VALUES ({placeholders})",
            list(fields.values()),
        )
    conn.commit()


def list_users(conn):
    """Return all users as a list of dicts."""
    rows = conn.execute("SELECT * FROM users ORDER BY created_at").fetchall()
    return [dict(r) for r in rows]


# ─── Per-user transaction DB ────────────────────────────────────────────────

def user_tx_db_path(data_dir, telegram_id):
    """Return the path to a user's transaction database."""
    return Path(data_dir) / f"transactions_{telegram_id}.db"


# ─── Bot logic ──────────────────────────────────────────────────────────────

class RevolutYNABBot:
    def __init__(self, token, admin_id, user_db_conn, data_dir):
        self.token = token
        self.admin_id = int(admin_id)
        self.db = user_db_conn
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # Persist the Telegram update offset so a container restart doesn't
        # cause Telegram to redeliver updates that were already processed.
        self._offset_path = self.data_dir / "telegram_offset"
        self.offset = self._load_offset()
        self._tmp_dir = Path(tempfile.mkdtemp(prefix="revolut_bot_"))
        # Per-user last CSV cache: {telegram_id: Path}
        self._last_csv = {}
        # In-memory guard against double-processing a single update_id
        # (belt-and-suspenders alongside the persisted offset).
        self._seen_updates = set()

    def _load_offset(self):
        """Read the persisted Telegram update offset, or 0 if missing."""
        try:
            if self._offset_path.exists():
                raw = self._offset_path.read_text().strip()
                if raw:
                    return int(raw)
        except (OSError, ValueError) as e:
            ynab.log.warning("bot: could not load offset (%s), starting at 0", e)
        return 0

    def _save_offset(self):
        """Atomically persist the current offset."""
        try:
            tmp = self._offset_path.with_suffix(".tmp")
            tmp.write_text(str(self.offset))
            os.replace(tmp, self._offset_path)
        except OSError as e:
            ynab.log.warning("bot: could not save offset: %s", e)

    # ── Polling ──────────────────────────────────────────────────────────

    def poll(self):
        """Long-poll for updates. Returns a list of update dicts."""
        data = {
            "offset": self.offset,
            "timeout": 30,
            "allowed_updates": ["message"],
        }
        result = tg_request(self.token, "getUpdates", data, timeout=35)
        if not result.get("ok"):
            return []
        updates = result.get("result", [])
        if updates:
            self.offset = updates[-1]["update_id"] + 1
            self._save_offset()
        return updates

    # ── Message routing ──────────────────────────────────────────────────

    def handle_update(self, update):
        """Route an incoming update to the right handler."""
        msg = update.get("message")
        if not msg:
            return

        sender_id = msg.get("from", {}).get("id")
        chat_id = msg["chat"]["id"]
        username = msg.get("from", {}).get("username", "")
        first_name = msg.get("from", {}).get("first_name", "")

        # ── Admin commands (always available) ────────────────────────────
        if sender_id == self.admin_id:
            text = (msg.get("text") or "").strip()
            if text.startswith("/approve"):
                self._admin_approve(chat_id, text)
                return
            elif text.startswith("/deny"):
                self._admin_deny(chat_id, text)
                return
            elif text.startswith("/users"):
                self._admin_list_users(chat_id)
                return

        # ── Look up or create the user ───────────────────────────────────
        user = get_user(self.db, sender_id)

        if not user:
            # New user — register as pending
            upsert_user(self.db, sender_id,
                        chat_id=chat_id, username=username,
                        first_name=first_name, state="pending")
            tg_send(self.token, chat_id,
                    "Welcome! Your access request has been sent to the admin.\n"
                    "You'll be notified once you're approved.", None)
            # Notify admin
            tg_send(self.token, self.admin_id, (
                f"🆕 New access request:\n"
                f"  User: {first_name} (@{username})\n"
                f"  ID: `{sender_id}`\n\n"
                f"Reply with /approve {sender_id} or /deny {sender_id}"
            ))
            ynab.log.info("bot: new user request id=%s name=%s @%s",
                          sender_id, first_name, username)
            return

        # Update chat_id in case it changed
        if user["chat_id"] != chat_id:
            upsert_user(self.db, sender_id, chat_id=chat_id)

        state = user["state"]

        # ── Denied users ─────────────────────────────────────────────────
        if state == "denied":
            tg_send(self.token, chat_id,
                    "Your access has been denied. Contact the admin if you "
                    "think this is a mistake.", None)
            return

        # ── Pending users ────────────────────────────────────────────────
        if state == "pending":
            tg_send(self.token, chat_id,
                    "Your request is still pending admin approval. Hang tight!", None)
            return

        # ── Onboarding states ────────────────────────────────────────────
        if state in ("approved", "awaiting_token"):
            self._onboard_token(chat_id, sender_id, msg)
            return

        if state == "awaiting_budget":
            self._onboard_budget(chat_id, sender_id, msg)
            return

        if state == "awaiting_account":
            self._onboard_account(chat_id, sender_id, msg)
            return

        # ── Crypto onboarding states ─────────────────────────────────────
        if state == "awaiting_crypto_account":
            self._onboard_crypto_account(chat_id, sender_id, msg)
            return

        if state == "awaiting_crypto_btc":
            self._onboard_crypto_btc(chat_id, sender_id, msg)
            return

        if state == "awaiting_crypto_eth":
            self._onboard_crypto_eth(chat_id, sender_id, msg)
            return

        # ── Ready users: normal commands ─────────────────────────────────
        assert state == "ready"

        # Document (CSV file)
        if msg.get("document"):
            self._handle_document(chat_id, sender_id, msg)
            return

        # Text command
        text = (msg.get("text") or "").strip()
        if text.startswith("/"):
            cmd = text.split()[0].lower().split("@")[0]
            if cmd == "/reconcile":
                self._handle_reconcile(chat_id, sender_id)
            elif cmd == "/status":
                self._handle_status(chat_id, sender_id)
            elif cmd == "/setup":
                upsert_user(self.db, sender_id, state="awaiting_token",
                            ynab_token=None, budget_id=None, budget_name=None,
                            account_id=None, account_name=None, temp_data=None)
                tg_send(self.token, chat_id,
                        "Let's set up your account again.\n\n"
                        "Please send me your YNAB Personal Access Token.\n"
                        "(Get it from app.ynab.com → Account Settings → Developer Settings)",
                        None)
            elif cmd == "/crypto":
                self._handle_crypto(chat_id, sender_id)
            elif cmd == "/crypto_setup":
                self._handle_crypto_setup(chat_id, sender_id)
            elif cmd == "/crypto_status":
                self._handle_crypto_status(chat_id, sender_id)
            elif cmd in ("/start", "/help"):
                self._handle_help(chat_id, sender_id)
            else:
                tg_send(self.token, chat_id,
                        "Unknown command. Send /help for available commands.", None)
            return

        # Fallback
        tg_send(self.token, chat_id,
                "Send me a Revolut CSV to import, or use /help for commands.", None)

    # ── Admin commands ───────────────────────────────────────────────────

    def _admin_approve(self, chat_id, text):
        parts = text.split()
        if len(parts) < 2:
            tg_send(self.token, chat_id,
                    "Usage: /approve <user_id>", None)
            return

        try:
            target_id = int(parts[1])
        except ValueError:
            tg_send(self.token, chat_id, "Invalid user ID.", None)
            return

        user = get_user(self.db, target_id)
        if not user:
            tg_send(self.token, chat_id, f"User {target_id} not found.", None)
            return

        upsert_user(self.db, target_id, state="awaiting_token")
        tg_send(self.token, chat_id,
                f"✅ User {user.get('first_name', '')} ({target_id}) approved.", None)

        # Notify the user
        if user.get("chat_id"):
            tg_send(self.token, user["chat_id"], (
                "🎉 You've been approved!\n\n"
                "Let's get you set up. Please send me your YNAB Personal Access Token.\n"
                "(Get it from app.ynab.com → Account Settings → Developer Settings)"
            ), None)

        ynab.log.info("bot: admin approved user %s", target_id)

    def _admin_deny(self, chat_id, text):
        parts = text.split()
        if len(parts) < 2:
            tg_send(self.token, chat_id, "Usage: /deny <user_id>", None)
            return

        try:
            target_id = int(parts[1])
        except ValueError:
            tg_send(self.token, chat_id, "Invalid user ID.", None)
            return

        user = get_user(self.db, target_id)
        if not user:
            tg_send(self.token, chat_id, f"User {target_id} not found.", None)
            return

        upsert_user(self.db, target_id, state="denied")
        tg_send(self.token, chat_id,
                f"❌ User {user.get('first_name', '')} ({target_id}) denied.", None)

        if user.get("chat_id"):
            tg_send(self.token, user["chat_id"],
                    "Your access request has been denied.", None)

        ynab.log.info("bot: admin denied user %s", target_id)

    def _admin_list_users(self, chat_id):
        users = list_users(self.db)
        if not users:
            tg_send(self.token, chat_id, "No users registered yet.", None)
            return

        state_icons = {
            "pending": "⏳", "approved": "✅", "awaiting_token": "🔑",
            "awaiting_budget": "📋", "awaiting_account": "🏦",
            "ready": "🟢", "denied": "🔴",
        }
        lines = ["*Registered users:*\n"]
        for u in users:
            icon = state_icons.get(u["state"], "?")
            name = u.get("first_name") or "?"
            uname = f" @{u['username']}" if u.get("username") else ""
            acct = f" → {u.get('account_name', '?')}" if u["state"] == "ready" else ""
            lines.append(
                f"{icon} `{u['telegram_id']}` {name}{uname} [{u['state']}]{acct}"
            )
        tg_send(self.token, chat_id, "\n".join(lines))

    # ── Onboarding: token ────────────────────────────────────────────────

    def _onboard_token(self, chat_id, sender_id, msg):
        text = (msg.get("text") or "").strip()

        # First contact after approval — show the prompt
        user = get_user(self.db, sender_id)
        if user["state"] == "approved":
            upsert_user(self.db, sender_id, state="awaiting_token")
            tg_send(self.token, chat_id, (
                "Please send me your YNAB Personal Access Token.\n"
                "(Get it from app.ynab.com → Account Settings → Developer Settings)"
            ), None)
            return

        if not text or text.startswith("/"):
            tg_send(self.token, chat_id,
                    "Please send your YNAB Personal Access Token to continue.", None)
            return

        # Validate the token by trying to list budgets
        tg_send(self.token, chat_id, "🔑 Validating token...", None)
        try:
            budgets = ynab.list_budgets(text)
        except SystemExit:
            tg_send(self.token, chat_id,
                    "❌ Invalid token — YNAB rejected it. "
                    "Please check and send the token again.", None)
            return
        except Exception as e:
            tg_send(self.token, chat_id,
                    f"❌ Could not reach YNAB: {e}\nPlease try again.", None)
            return

        if not budgets:
            tg_send(self.token, chat_id,
                    "Token is valid but no budgets found in your YNAB account.", None)
            return

        # Save token, store budgets for selection
        budget_list = [{"id": b["id"], "name": b["name"]} for b in budgets]
        upsert_user(self.db, sender_id,
                    ynab_token=text,
                    state="awaiting_budget",
                    temp_data=json.dumps(budget_list))

        lines = ["✅ Token valid! Select your budget:\n"]
        for i, b in enumerate(budget_list, 1):
            lines.append(f"  {i}. {b['name']}")
        lines.append("\nReply with the number.")
        tg_send(self.token, chat_id, "\n".join(lines), None)

    # ── Onboarding: budget ───────────────────────────────────────────────

    def _onboard_budget(self, chat_id, sender_id, msg):
        text = (msg.get("text") or "").strip()
        user = get_user(self.db, sender_id)
        budget_list = json.loads(user.get("temp_data") or "[]")

        if not budget_list:
            # Shouldn't happen — restart onboarding
            upsert_user(self.db, sender_id, state="awaiting_token", temp_data=None)
            tg_send(self.token, chat_id,
                    "Something went wrong. Please send your YNAB token again.", None)
            return

        # Parse selection
        try:
            idx = int(text) - 1
            if idx < 0 or idx >= len(budget_list):
                raise ValueError
        except ValueError:
            lines = ["Please reply with the number of your budget:\n"]
            for i, b in enumerate(budget_list, 1):
                lines.append(f"  {i}. {b['name']}")
            tg_send(self.token, chat_id, "\n".join(lines), None)
            return

        selected = budget_list[idx]

        # Fetch accounts for this budget
        try:
            accounts = ynab.list_accounts(user["ynab_token"], selected["id"])
        except SystemExit:
            tg_send(self.token, chat_id,
                    "❌ Could not fetch accounts. Please try /setup again.", None)
            return

        if not accounts:
            tg_send(self.token, chat_id,
                    f"No accounts found in budget '{selected['name']}'.\n"
                    "Please try a different budget.", None)
            return

        account_list = [{"id": a["id"], "name": a["name"],
                         "type": a.get("type", ""), "balance": a.get("balance", 0)}
                        for a in accounts]

        upsert_user(self.db, sender_id,
                    budget_id=selected["id"],
                    budget_name=selected["name"],
                    state="awaiting_account",
                    temp_data=json.dumps(account_list))

        lines = [f"Budget: *{selected['name']}*\n\nSelect your Revolut account:\n"]
        for i, a in enumerate(account_list, 1):
            bal = a["balance"] / 1000
            lines.append(f"  {i}. {a['name']} ({a['type']}) — {bal:,.2f}")
        lines.append("\nReply with the number.")
        tg_send(self.token, chat_id, "\n".join(lines))

    # ── Onboarding: account ──────────────────────────────────────────────

    def _onboard_account(self, chat_id, sender_id, msg):
        text = (msg.get("text") or "").strip()
        user = get_user(self.db, sender_id)
        account_list = json.loads(user.get("temp_data") or "[]")

        if not account_list:
            upsert_user(self.db, sender_id, state="awaiting_token", temp_data=None)
            tg_send(self.token, chat_id,
                    "Something went wrong. Please send your YNAB token again.", None)
            return

        try:
            idx = int(text) - 1
            if idx < 0 or idx >= len(account_list):
                raise ValueError
        except ValueError:
            lines = ["Please reply with the number of your account:\n"]
            for i, a in enumerate(account_list, 1):
                bal = a["balance"] / 1000
                lines.append(f"  {i}. {a['name']} ({a['type']}) — {bal:,.2f}")
            tg_send(self.token, chat_id, "\n".join(lines), None)
            return

        selected = account_list[idx]

        upsert_user(self.db, sender_id,
                    account_id=selected["id"],
                    account_name=selected["name"],
                    state="ready",
                    temp_data=None)

        tg_send(self.token, chat_id, (
            f"🎉 All set!\n\n"
            f"  Budget:  {user['budget_name']}\n"
            f"  Account: {selected['name']}\n\n"
            f"You can now send me Revolut CSV files to import.\n"
            f"Type /help to see all commands."
        ), None)

        ynab.log.info("bot: user %s onboarding complete — budget=%s account=%s",
                      sender_id, user["budget_name"], selected["name"])

    # ── /help ────────────────────────────────────────────────────────────

    def _handle_help(self, chat_id, sender_id):
        lines = [
            f"*Revolut → YNAB Bot* — {format_version_line()}\n",
            "📎 *Send a CSV* — Import Revolut transactions into YNAB",
            "🧮 /reconcile — Reconcile YNAB balance against last CSV",
            "📊 /status — Show YNAB account balance & last import",
            "🔧 /setup — Re-run setup (change token / budget / account)",
            "🪙 /crypto — Sync crypto portfolio value to YNAB",
            "🛠 /crypto\\_setup — Configure BTC xpub / ETH address / tracking account",
            "🔍 /crypto\\_status — Show current crypto configuration",
            "❓ /help — This message",
        ]
        if sender_id == self.admin_id:
            lines.extend([
                "\n*Admin commands:*",
                "/approve <id> — Approve a pending user",
                "/deny <id> — Deny a user",
                "/users — List all registered users",
            ])
        tg_send(self.token, chat_id, "\n".join(lines))

    # ── Helpers to get user config ───────────────────────────────────────

    def _user_config(self, sender_id):
        """Build a config dict from the user's DB row."""
        user = get_user(self.db, sender_id)
        if not user or user["state"] != "ready":
            return None
        return {
            "ynab_token": user["ynab_token"],
            "budget_id": user["budget_id"],
            "account_id": user["account_id"],
            "db_path": str(user_tx_db_path(self.data_dir, sender_id)),
        }

    def _user_tmp_dir(self, sender_id):
        """Get or create a per-user temp directory."""
        d = self._tmp_dir / str(sender_id)
        d.mkdir(parents=True, exist_ok=True)
        return d

    # ── CSV import ───────────────────────────────────────────────────────

    def _handle_document(self, chat_id, sender_id, msg):
        doc = msg["document"]
        filename = doc.get("file_name", "unknown")

        if not filename.lower().endswith(".csv"):
            tg_send(self.token, chat_id,
                    f"_{filename}_ doesn't look like a CSV. "
                    "Please send a Revolut account-statement CSV.", None)
            return

        tg_send(self.token, chat_id, f"📥 Downloading _{filename}_...", None)

        dest = self._user_tmp_dir(sender_id) / filename
        if not tg_download_file(self.token, doc["file_id"], dest):
            tg_send(self.token, chat_id, "Failed to download the file.", None)
            return

        if not ynab.is_revolut_csv(str(dest)):
            tg_send(self.token, chat_id,
                    "This doesn't look like a Revolut account statement CSV "
                    "(missing expected headers).", None)
            return

        self._last_csv[sender_id] = dest
        ynab.log.info("bot: received CSV %s from user %s", filename, sender_id)

        try:
            transactions = ynab.parse_revolut_csv(str(dest))
        except Exception as e:
            tg_send(self.token, chat_id, f"Error parsing CSV: {e}", None)
            return

        if not transactions:
            tg_send(self.token, chat_id, "CSV parsed but contains no transactions.", None)
            return

        n_total = len(transactions)
        n_pending = sum(1 for t in transactions if t.get("cleared") != "cleared")
        dates = sorted(set(t["date"] for t in transactions))
        date_range = f"{dates[0]} → {dates[-1]}" if len(dates) > 1 else dates[0]

        tg_send(self.token, chat_id, (
            f"📋 *{n_total}* transactions parsed ({n_pending} pending)\n"
            f"📅 {date_range}\n\n"
            f"Importing into YNAB..."
        ))

        cfg = self._user_config(sender_id)
        if not cfg:
            tg_send(self.token, chat_id, "Setup incomplete. Run /setup first.", None)
            return

        try:
            conn = ynab.init_db(cfg["db_path"])
            buf = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = buf
            try:
                ynab.import_and_track(
                    conn, cfg["ynab_token"], cfg["budget_id"],
                    cfg["account_id"], transactions,
                )
            finally:
                sys.stdout = old_stdout
            output = buf.getvalue()
            conn.close()
        except Exception as e:
            ynab.log.error("bot: import failed for user %s: %s",
                           sender_id, traceback.format_exc())
            tg_send(self.token, chat_id, f"Import failed: {e}", None)
            return

        summary = self._format_import_summary(output)
        tg_send(self.token, chat_id, summary, None)
        self._send_balance(chat_id, cfg)

    def _format_import_summary(self, stdout_output):
        """Turn the script's stdout into a clean Telegram message."""
        lines = stdout_output.strip().splitlines()
        parts = ["✅ Import complete\n"]
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if "New transactions:" in line:
                parts.append(f"  New:     {line.split(':')[-1].strip()}")
            elif "Updated" in line and "state/amt" in line:
                parts.append(f"  Updated: {line.split(':')[-1].strip()}")
            elif "Already imported:" in line:
                parts.append(f"  Skipped: {line.split(':')[-1].strip()}")
            elif "Nothing to do" in line:
                parts = ["✅ Everything already up to date"]
                break
            elif "Created:" in line:
                parts.append(f"  Created: {line.split(':')[-1].strip()}")
            elif "Duplicates:" in line:
                parts.append(f"  Dupes:   {line.split(':')[-1].strip()}")
        return "\n".join(parts)

    # ── /reconcile ───────────────────────────────────────────────────────

    def _handle_reconcile(self, chat_id, sender_id):
        csv_path = self._last_csv.get(sender_id)
        if not csv_path or not csv_path.exists():
            tg_send(self.token, chat_id,
                    "No CSV available. Send me a Revolut CSV first.", None)
            return

        cfg = self._user_config(sender_id)
        if not cfg:
            tg_send(self.token, chat_id, "Setup incomplete. Run /setup first.", None)
            return

        tg_send(self.token, chat_id,
                f"🧮 Reconciling against _{csv_path.name}_...", None)

        try:
            buf = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = buf
            try:
                ynab.reconcile_from_csv(
                    cfg["ynab_token"], cfg["budget_id"],
                    cfg["account_id"], str(csv_path),
                )
            finally:
                sys.stdout = old_stdout
            output = buf.getvalue()
        except Exception as e:
            ynab.log.error("bot: reconcile failed for user %s: %s",
                           sender_id, traceback.format_exc())
            tg_send(self.token, chat_id, f"Reconcile failed: {e}", None)
            return

        clean = self._format_reconcile_summary(output)
        tg_send(self.token, chat_id, clean, None)

    def _format_reconcile_summary(self, stdout_output):
        lines = stdout_output.strip().splitlines()
        parts = []
        for line in lines:
            line = line.strip()
            if not line or line.startswith("File:") or line.startswith("🧮"):
                continue
            parts.append(line)
        return "\n".join(parts) if parts else "Reconciliation complete."

    # ── /status ──────────────────────────────────────────────────────────

    def _handle_status(self, chat_id, sender_id):
        cfg = self._user_config(sender_id)
        if not cfg:
            tg_send(self.token, chat_id, "Setup incomplete. Run /setup first.", None)
            return

        try:
            cleared_milli = ynab.get_ynab_account_balance(
                cfg["ynab_token"], cfg["budget_id"], cfg["account_id"],
            )
            cleared = cleared_milli / 1000

            accounts = ynab.list_accounts(cfg["ynab_token"], cfg["budget_id"])
            acct = next((a for a in accounts if a["id"] == cfg["account_id"]), None)
            acct_name = acct["name"] if acct else cfg["account_id"][:8]
            total = acct["balance"] / 1000 if acct else cleared

            parts = [
                f"📊 *{acct_name}*\n",
                f"  Cleared balance: {cleared:,.2f}",
                f"  Total balance:   {total:,.2f}",
            ]

            csv_path = self._last_csv.get(sender_id)
            if csv_path and csv_path.exists():
                bal = ynab.extract_csv_running_balance(str(csv_path))
                if bal:
                    parts.append(
                        f"\n  Last CSV balance: {bal['balance']:,.2f} {bal['currency']} "
                        f"(as of {bal['date']})")
                    delta = bal["balance"] - cleared
                    if abs(delta) >= 0.01:
                        parts.append(f"  Unreconciled delta: {delta:+,.2f} {bal['currency']}")
                    else:
                        parts.append(f"  ✅ Reconciled")

            tg_send(self.token, chat_id, "\n".join(parts))

        except Exception as e:
            ynab.log.error("bot: status failed for user %s: %s",
                           sender_id, traceback.format_exc())
            tg_send(self.token, chat_id, f"Could not fetch status: {e}", None)

    def _send_balance(self, chat_id, cfg):
        """Send a short balance line after import."""
        try:
            cleared_milli = ynab.get_ynab_account_balance(
                cfg["ynab_token"], cfg["budget_id"], cfg["account_id"],
            )
            tg_send(self.token, chat_id,
                    f"💰 YNAB cleared balance: {cleared_milli / 1000:,.2f}", None)
        except Exception:
            pass

    # ── /crypto: sync portfolio to YNAB tracking account ────────────────

    def _handle_crypto(self, chat_id, sender_id):
        user = get_user(self.db, sender_id)
        if not user.get("crypto_account_id"):
            tg_send(self.token, chat_id,
                    "🪙 Crypto tracking isn't configured yet.\n"
                    "Run /crypto\\_setup to pick a tracking account and add "
                    "your BTC xpub or ETH address.")
            return
        if not user.get("btc_xpub") and not user.get("eth_address"):
            tg_send(self.token, chat_id,
                    "No BTC xpub or ETH address on file. "
                    "Run /crypto\\_setup to add one.")
            return

        tg_send(self.token, chat_id,
                "🪙 Syncing crypto portfolio... (this can take 30–60s for BTC xpubs)",
                None)

        try:
            buf = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = buf
            try:
                ynab.crypto_sync(
                    user["ynab_token"],
                    user["budget_id"],
                    user["crypto_account_id"],
                    btc_xpub=user.get("btc_xpub") or None,
                    eth_address=user.get("eth_address") or None,
                )
            finally:
                sys.stdout = old_stdout
            output = buf.getvalue()
        except (Exception, SystemExit) as e:
            # SystemExit bypasses `except Exception`; catch it here too so the
            # bot always reports sync failures instead of going silent when a
            # helper in revolut_to_ynab.py calls sys.exit(1).
            ynab.log.error("bot: crypto sync failed for user %s: %s",
                           sender_id, traceback.format_exc())
            msg = f"exit({e.code})" if isinstance(e, SystemExit) else str(e)
            tg_send(self.token, chat_id, f"Crypto sync failed: {msg}", None)
            return

        summary = self._format_crypto_summary(output, user)
        tg_send(self.token, chat_id, summary, None)

    def _format_crypto_summary(self, stdout_output, user):
        """Condense crypto_sync stdout into a Telegram-friendly summary."""
        lines = stdout_output.strip().splitlines()
        holdings = []
        portfolio_chf = None
        ynab_before = None
        delta = None
        new_balance = None
        in_sync = False

        for raw in lines:
            line = raw.strip()
            if not line:
                continue
            if "Portfolio value:" in line or "Total portfolio:" in line:
                portfolio_chf = line.split(":", 1)[-1].strip()
            elif "YNAB balance:" in line:
                ynab_before = line.split(":", 1)[-1].strip()
            elif "Delta:" in line or "Adjustment:" in line:
                delta = line.split(":", 1)[-1].strip()
            elif "New balance:" in line:
                new_balance = line.split(":", 1)[-1].strip()
            elif "already in sync" in line.lower():
                in_sync = True
            elif line.startswith(("BTC", "ETH", "USDC", "USDT", "AAVE")) and "CHF" in line:
                holdings.append(line)

        parts = [f"🪙 *Crypto sync — {user.get('crypto_account_name') or 'tracking account'}*\n"]
        if holdings:
            parts.append("*Holdings:*")
            for h in holdings[:15]:
                parts.append(f"  {h}")
            parts.append("")
        if portfolio_chf:
            parts.append(f"Portfolio: {portfolio_chf}")
        if ynab_before:
            parts.append(f"YNAB was:  {ynab_before}")
        if in_sync:
            parts.append("\n✅ Already in sync — no adjustment needed.")
        else:
            if delta:
                parts.append(f"Adjustment: {delta}")
            if new_balance:
                parts.append(f"New bal:   {new_balance}")
        return "\n".join(parts)

    # ── /crypto_status: show current crypto config ──────────────────────

    def _handle_crypto_status(self, chat_id, sender_id):
        user = get_user(self.db, sender_id)
        parts = ["🔍 *Crypto configuration*\n"]
        if user.get("crypto_account_name"):
            parts.append(f"Tracking account: {user['crypto_account_name']}")
        else:
            parts.append("Tracking account: _(not set)_")

        if user.get("btc_xpub"):
            x = user["btc_xpub"]
            masked = f"{x[:12]}...{x[-8:]}" if len(x) > 24 else x
            parts.append(f"BTC xpub/addr: `{masked}`")
        else:
            parts.append("BTC xpub/addr: _(not set)_")

        if user.get("eth_address"):
            a = user["eth_address"]
            masked = f"{a[:8]}...{a[-6:]}" if len(a) > 20 else a
            parts.append(f"ETH address:   `{masked}`")
        else:
            parts.append("ETH address:   _(not set)_")

        parts.append("\nUse /crypto\\_setup to change these.")
        tg_send(self.token, chat_id, "\n".join(parts))

    # ── /crypto_setup: start onboarding flow ────────────────────────────

    def _handle_crypto_setup(self, chat_id, sender_id):
        user = get_user(self.db, sender_id)
        if user["state"] != "ready":
            tg_send(self.token, chat_id,
                    "Please finish the main /setup first.", None)
            return

        try:
            accounts = ynab.list_accounts(user["ynab_token"], user["budget_id"])
        except Exception as e:
            tg_send(self.token, chat_id,
                    f"Could not fetch YNAB accounts: {e}", None)
            return

        # Only "tracking" accounts are appropriate for crypto portfolio value
        tracking = [a for a in accounts
                    if a.get("type") in ("otherAsset", "otherLiability")
                    and not a.get("closed")]
        if not tracking:
            tg_send(self.token, chat_id,
                    "No tracking accounts found in your YNAB budget.\n\n"
                    "Create one in YNAB first:\n"
                    "  app.ynab.com → Add Account → *Tracking* → "
                    "Asset → name it e.g. 'Crypto'.\n"
                    "Then run /crypto\\_setup again.")
            return

        account_list = [{"id": a["id"], "name": a["name"],
                         "balance": a.get("balance", 0),
                         "type": a.get("type", "")}
                        for a in tracking]

        upsert_user(self.db, sender_id,
                    state="awaiting_crypto_account",
                    temp_data=json.dumps(account_list))

        lines = ["🪙 *Crypto setup* — pick your tracking account:\n"]
        for i, a in enumerate(account_list, 1):
            bal = a["balance"] / 1000
            lines.append(f"  {i}. {a['name']} — {bal:,.2f}")
        lines.append("\nReply with the number (or 'cancel' to abort).")
        tg_send(self.token, chat_id, "\n".join(lines))

    # ── Crypto onboarding: tracking account ─────────────────────────────

    def _onboard_crypto_account(self, chat_id, sender_id, msg):
        text = (msg.get("text") or "").strip()
        user = get_user(self.db, sender_id)

        if text.lower() == "cancel":
            upsert_user(self.db, sender_id, state="ready", temp_data=None)
            tg_send(self.token, chat_id, "Cancelled. Back to normal.", None)
            return

        account_list = json.loads(user.get("temp_data") or "[]")
        if not account_list:
            upsert_user(self.db, sender_id, state="ready", temp_data=None)
            tg_send(self.token, chat_id,
                    "Something went wrong. Run /crypto\\_setup again.")
            return

        try:
            idx = int(text) - 1
            if idx < 0 or idx >= len(account_list):
                raise ValueError
        except ValueError:
            lines = ["Please reply with the number of your tracking account:\n"]
            for i, a in enumerate(account_list, 1):
                bal = a["balance"] / 1000
                lines.append(f"  {i}. {a['name']} — {bal:,.2f}")
            lines.append("\n(Or type 'cancel' to abort.)")
            tg_send(self.token, chat_id, "\n".join(lines), None)
            return

        selected = account_list[idx]
        upsert_user(self.db, sender_id,
                    crypto_account_id=selected["id"],
                    crypto_account_name=selected["name"],
                    state="awaiting_crypto_btc",
                    temp_data=None)

        tg_send(self.token, chat_id, (
            f"✅ Tracking account: *{selected['name']}*\n\n"
            "Now send your *BTC xpub* (from Ledger Live → Account → Edit → "
            "Advanced logs) — or a single BTC address.\n\n"
            "• xpub/ypub/zpub: tracks the whole HD wallet\n"
            "• Single address: tracks just that address\n"
            "• Type 'skip' to skip BTC\n"
            "• Type 'cancel' to abort"
        ))

    # ── Crypto onboarding: BTC xpub/address ─────────────────────────────

    def _onboard_crypto_btc(self, chat_id, sender_id, msg):
        text = (msg.get("text") or "").strip()
        low = text.lower()

        if low == "cancel":
            upsert_user(self.db, sender_id, state="ready", temp_data=None)
            tg_send(self.token, chat_id, "Cancelled.", None)
            return

        if low == "skip":
            upsert_user(self.db, sender_id,
                        btc_xpub=None,
                        state="awaiting_crypto_eth",
                        temp_data=None)
            tg_send(self.token, chat_id,
                    "⏭ Skipped BTC.\n\n"
                    "Now send your *ETH address* (0x...) — "
                    "or type 'skip' / 'cancel'.")
            return

        # Very light validation
        looks_xpub = text.startswith(("xpub", "ypub", "zpub"))
        looks_btc_addr = (text.startswith(("bc1", "1", "3"))
                          and 25 <= len(text) <= 100)
        if not looks_xpub and not looks_btc_addr:
            tg_send(self.token, chat_id,
                    "That doesn't look like an xpub or BTC address.\n"
                    "Expected xpub/ypub/zpub... or bc1.../1.../3...\n"
                    "Type 'skip' to skip or 'cancel' to abort.",
                    None)
            return

        upsert_user(self.db, sender_id,
                    btc_xpub=text,
                    state="awaiting_crypto_eth",
                    temp_data=None)

        masked = f"{text[:12]}...{text[-8:]}" if len(text) > 24 else text
        tg_send(self.token, chat_id, (
            f"✅ BTC saved: `{masked}`\n\n"
            "Now send your *ETH address* (0x...) — or type 'skip' / 'cancel'."
        ))

    # ── Crypto onboarding: ETH address ──────────────────────────────────

    def _onboard_crypto_eth(self, chat_id, sender_id, msg):
        text = (msg.get("text") or "").strip()
        low = text.lower()

        if low == "cancel":
            upsert_user(self.db, sender_id, state="ready", temp_data=None)
            tg_send(self.token, chat_id, "Cancelled.", None)
            return

        if low == "skip":
            upsert_user(self.db, sender_id, eth_address=None, state="ready",
                        temp_data=None)
            self._finish_crypto_setup(chat_id, sender_id)
            return

        if not (text.startswith("0x") and len(text) == 42):
            tg_send(self.token, chat_id,
                    "That doesn't look like an Ethereum address.\n"
                    "Expected a 42-character string starting with 0x.\n"
                    "Type 'skip' to skip or 'cancel' to abort.",
                    None)
            return

        upsert_user(self.db, sender_id, eth_address=text, state="ready",
                    temp_data=None)
        self._finish_crypto_setup(chat_id, sender_id)

    def _finish_crypto_setup(self, chat_id, sender_id):
        user = get_user(self.db, sender_id)
        parts = ["🎉 *Crypto setup complete!*\n"]
        parts.append(f"Tracking account: {user.get('crypto_account_name')}")
        if user.get("btc_xpub"):
            x = user["btc_xpub"]
            parts.append(f"BTC: `{x[:12]}...{x[-8:]}`")
        if user.get("eth_address"):
            a = user["eth_address"]
            parts.append(f"ETH: `{a[:8]}...{a[-6:]}`")
        if not user.get("btc_xpub") and not user.get("eth_address"):
            parts.append("\n⚠ No BTC or ETH configured — /crypto won't do anything.")
            parts.append("Run /crypto\\_setup again to add one.")
        else:
            parts.append("\nRun /crypto anytime to sync your portfolio value to YNAB.")
        tg_send(self.token, chat_id, "\n".join(parts))
        ynab.log.info("bot: user %s crypto setup complete (btc=%s eth=%s)",
                      sender_id, bool(user.get("btc_xpub")),
                      bool(user.get("eth_address")))

    # ── Main loop ────────────────────────────────────────────────────────

    def run(self):
        """Start the bot with long-polling."""
        me = tg_request(self.token, "getMe")
        if not me.get("ok"):
            print("Error: invalid Telegram bot token.")
            sys.exit(1)
        bot_name = me["result"].get("username", "?")
        version_line = format_version_line()
        print(f"🤖 Bot @{bot_name} started ({version_line}) admin={self.admin_id}")
        print(f"   User DB: {self.db.execute('SELECT count(*) FROM users').fetchone()[0]} users")
        print(f"   Data dir: {self.data_dir}")
        print(f"   Press Ctrl+C to stop.\n")
        ynab.log.info("bot: started @%s %s admin=%s",
                      bot_name, version_line, self.admin_id)

        # Notify admin that the bot just (re)started + which version is running
        try:
            n_users = self.db.execute(
                "SELECT count(*) FROM users WHERE state='ready'"
            ).fetchone()[0]
            tg_send(self.token, self.admin_id, (
                f"🚀 *Bot started*\n\n"
                f"Version: {version_line}\n"
                f"Ready users: {n_users}"
            ))
        except Exception as e:
            ynab.log.warning("bot: could not send startup message to admin: %s", e)

        retry_delay = 1
        while True:
            try:
                updates = self.poll()
                retry_delay = 1
                for update in updates:
                    uid = update.get("update_id")
                    if uid in self._seen_updates:
                        ynab.log.info("bot: skipping duplicate update %s", uid)
                        continue
                    self._seen_updates.add(uid)
                    # Cap memory — keep the last 500 IDs
                    if len(self._seen_updates) > 500:
                        self._seen_updates = set(
                            list(self._seen_updates)[-250:]
                        )
                    try:
                        self.handle_update(update)
                    except SystemExit as e:
                        # A called helper (e.g. ynab.list_budgets, crypto_sync)
                        # tried to sys.exit(). Swallow it so one bad command
                        # doesn't kill the whole bot.
                        ynab.log.error("bot: handler raised SystemExit(%s): %s",
                                       e.code, traceback.format_exc())
                    except Exception:
                        ynab.log.error("bot: error handling update: %s",
                                       traceback.format_exc())
            except KeyboardInterrupt:
                print("\n👋 Bot stopped.")
                ynab.log.info("bot: stopped by user")
                break
            except Exception as e:
                ynab.log.error("bot: polling error: %s", e)
                print(f"  ⚠ Connection error: {e} — retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


# ─── Entry point ────────────────────────────────────────────────────────────

def main():
    ynab._load_dotenv()

    log_level = os.environ.get("LOG_LEVEL", "INFO")
    log_file = os.environ.get("LOG_FILE", str(ynab.DEFAULT_LOG_PATH))
    ynab.setup_logging(log_level, log_file if log_file else None)

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    admin_id = os.environ.get("TELEGRAM_ADMIN_ID", "").strip()

    missing = []
    if not bot_token:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not admin_id:
        missing.append("TELEGRAM_ADMIN_ID")
    if missing:
        print(f"Error: missing required config: {', '.join(missing)}")
        print("Set them in your .env file or as environment variables.")
        sys.exit(1)

    # Database paths
    data_dir = Path(os.environ.get("BOT_DATA_DIR",
                                   str(_script_dir / "bot_data")))
    data_dir.mkdir(parents=True, exist_ok=True)

    user_db_path = data_dir / "bot_users.db"
    user_db = init_user_db(user_db_path)

    # Auto-register admin as ready if they have YNAB config in .env
    admin_ynab_token = os.environ.get("YNAB_TOKEN", "").strip()
    admin_budget_id = os.environ.get("YNAB_BUDGET_ID", "").strip()
    admin_account_id = os.environ.get("YNAB_ACCOUNT_ID", "").strip()
    admin_crypto_account_id = os.environ.get("YNAB_CRYPTO_ACCOUNT_ID", "").strip()
    admin_btc_xpub = os.environ.get("CRYPTO_BTC_XPUB", "").strip()
    admin_eth_address = os.environ.get("CRYPTO_ETH_ADDRESS", "").strip()

    if admin_ynab_token and admin_budget_id and admin_account_id:
        existing = get_user(user_db, int(admin_id))
        fields = {
            "state": "ready",
            "ynab_token": admin_ynab_token,
            "budget_id": admin_budget_id,
            "account_id": admin_account_id,
            "first_name": "Admin",
        }
        # Optional crypto config from .env (admin convenience)
        if admin_crypto_account_id:
            fields["crypto_account_id"] = admin_crypto_account_id
        if admin_btc_xpub:
            fields["btc_xpub"] = admin_btc_xpub
        if admin_eth_address:
            fields["eth_address"] = admin_eth_address
        if not existing or existing["state"] != "ready":
            upsert_user(user_db, int(admin_id), **fields)
            print(f"   Admin ({admin_id}) auto-registered from .env config.")

    bot = RevolutYNABBot(bot_token, admin_id, user_db, str(data_dir))
    bot.run()


if __name__ == "__main__":
    main()
