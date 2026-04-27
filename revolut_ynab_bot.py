#!/usr/bin/env python3
"""
Telegram Bot for Revolut → YNAB Importer  (multi-tenant)

Share a Revolut CSV from your phone → Telegram → bot processes it → done.

User commands:
  (send a CSV file)  — Import transactions into YNAB
  /reconcile         — Reconcile YNAB cleared balance against the last uploaded CSV
  /cleanup_pending   — Strip stale "(pending)" memos from cleared YNAB transactions
  /dedupe            — Scan YNAB for orphaned duplicate imports in last CSV's range
  /dedupe_delete ... — Delete selected orphans (e.g. "1,3,5" or "all")
  /dedupe_cancel     — Discard the pending dedupe selection
  /status            — Show YNAB account balance and last import info
  /setup             — Re-run onboarding (change token / budget / account)
  /auto_approve on|off — Toggle whether imported txns are auto-approved
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

__version__ = "1.1.16"


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


def tg_send(token, chat_id, text, parse_mode="Markdown", reply_markup=None):
    """Send a text message. Long messages are split at ~4000 chars.

    Returns the API result of the LAST chunk sent, so callers that supplied
    a ``reply_markup`` can read back ``message_id`` for later edits.
    """
    chunks = []
    while len(text) > 4000:
        split = text.rfind("\n", 0, 4000)
        if split == -1:
            split = 4000
        chunks.append(text[:split])
        text = text[split:].lstrip("\n")
    chunks.append(text)

    last = None
    for i, chunk in enumerate(chunks):
        data = {"chat_id": chat_id, "text": chunk}
        if parse_mode:
            data["parse_mode"] = parse_mode
        # Only attach the keyboard to the final chunk
        if reply_markup is not None and i == len(chunks) - 1:
            data["reply_markup"] = reply_markup
        result = tg_request(token, "sendMessage", data)
        # If Markdown parsing failed (e.g. an unmatched underscore inside
        # the text), Telegram drops the whole message — making the bot
        # appear silent. Retry once as plain text so the user always
        # sees *something* and we don't have to chase every stray '_'.
        if (parse_mode and not result.get("ok")
                and _looks_like_parse_error(result.get("description", ""))):
            ynab.log.warning(
                "Telegram rejected Markdown; retrying without parse_mode. "
                "Description: %s", result.get("description"),
            )
            data.pop("parse_mode", None)
            result = tg_request(token, "sendMessage", data)
        last = result
    return last


def _looks_like_parse_error(description):
    if not description:
        return False
    d = description.lower()
    return ("can't parse entities" in d
            or "can't find end" in d
            or "parse_mode" in d)


def tg_edit_message(token, chat_id, message_id, text=None,
                    parse_mode="Markdown", reply_markup=None):
    """Edit an existing bot message (text + reply_markup)."""
    data = {"chat_id": chat_id, "message_id": message_id}
    if text is not None:
        data["text"] = text
        if parse_mode:
            data["parse_mode"] = parse_mode
    if reply_markup is not None:
        data["reply_markup"] = reply_markup
    method = "editMessageText" if text is not None else "editMessageReplyMarkup"
    result = tg_request(token, method, data)
    if (text is not None and parse_mode and not result.get("ok")
            and _looks_like_parse_error(result.get("description", ""))):
        ynab.log.warning(
            "Telegram rejected Markdown on edit; retrying without parse_mode. "
            "Description: %s", result.get("description"),
        )
        data.pop("parse_mode", None)
        result = tg_request(token, method, data)
    return result


def tg_answer_callback(token, callback_query_id, text=None, show_alert=False):
    """Acknowledge a callback_query so Telegram dismisses the loading spinner."""
    data = {"callback_query_id": callback_query_id}
    if text:
        data["text"] = text[:200]
        data["show_alert"] = show_alert
    return tg_request(token, "answerCallbackQuery", data)


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
            auto_approve        INTEGER NOT NULL DEFAULT 1,
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
    try:
        conn.execute(
            "ALTER TABLE users ADD COLUMN auto_approve INTEGER NOT NULL DEFAULT 1"
        )
    except sqlite3.OperationalError:
        pass
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
        # Persist uploaded CSVs under the mounted volume so they survive
        # container restarts (older code used tempfile.mkdtemp under /tmp,
        # which Docker wipes on every redeploy — that's why /reconcile and
        # /dedupe used to lose state after a deploy).
        self._csv_cache_dir = self.data_dir / "csv_cache"
        self._csv_cache_dir.mkdir(parents=True, exist_ok=True)
        # Per-user dedupe candidates from the last /dedupe scan:
        # {telegram_id: [orphan_dict, ...]}
        self._dedupe_candidates = {}
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
            "allowed_updates": ["message", "callback_query"],
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
        # Inline-keyboard button click
        cq = update.get("callback_query")
        if cq:
            self._handle_callback_query(cq)
            return

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
            elif cmd == "/cleanup_pending":
                self._handle_cleanup_pending(chat_id, sender_id)
            elif cmd == "/dedupe":
                self._handle_dedupe(chat_id, sender_id)
            elif cmd == "/dedupe_delete":
                self._handle_dedupe_delete(chat_id, sender_id, text)
            elif cmd == "/dedupe_cancel":
                self._handle_dedupe_cancel(chat_id, sender_id)
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
            elif cmd == "/auto_approve":
                self._handle_auto_approve(chat_id, sender_id, text)
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

    # ── /auto_approve ────────────────────────────────────────────────────

    def _handle_auto_approve(self, chat_id, sender_id, text):
        """Show or toggle whether imported transactions are marked approved."""
        parts = text.split()
        arg = parts[1].lower() if len(parts) > 1 else ""

        if arg in ("on", "true", "1", "yes"):
            new_val = 1
        elif arg in ("off", "false", "0", "no"):
            new_val = 0
        elif arg == "":
            user = get_user(self.db, sender_id)
            current = bool(user.get("auto_approve", 1)) if user else True
            tg_send(self.token, chat_id,
                    f"Auto-approve is currently *{'ON' if current else 'OFF'}*.\n\n"
                    f"Usage: `/auto_approve on` or `/auto_approve off`\n"
                    f"When OFF, imported transactions land in YNAB's inbox "
                    f"as unapproved for manual review.")
            return
        else:
            tg_send(self.token, chat_id,
                    "Usage: `/auto_approve on` or `/auto_approve off`")
            return

        upsert_user(self.db, sender_id, auto_approve=new_val)
        tg_send(self.token, chat_id,
                f"✅ Auto-approve set to *{'ON' if new_val else 'OFF'}*.")

    # ── /help ────────────────────────────────────────────────────────────

    def _handle_help(self, chat_id, sender_id):
        lines = [
            f"*Revolut → YNAB Bot* — {format_version_line()}\n",
            "📎 *Send a CSV* — Import Revolut transactions into YNAB",
            "🧮 /reconcile — Reconcile YNAB balance against last CSV",
            "🧹 `/cleanup_pending` — Strip stale '(pending)' memos from cleared txns",
            "🔎 /dedupe — Find orphaned duplicate imports for the last CSV",
            "📊 /status — Show YNAB account balance & last import",
            "🔧 /setup — Re-run setup (change token / budget / account)",
            "✅ `/auto_approve on|off` — Toggle auto-approval of imported txns",
            "🪙 /crypto — Sync crypto portfolio value to YNAB",
            "🛠 `/crypto_setup` — Configure BTC xpub / ETH address / tracking account",
            "🔍 `/crypto_status` — Show current crypto configuration",
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

    def _user_csv_dir(self, sender_id):
        """Per-user directory for cached CSVs (persistent across restarts)."""
        d = self._csv_cache_dir / str(sender_id)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _last_csv_path(self, sender_id):
        """Most recently uploaded CSV for this user, or None."""
        d = self._user_csv_dir(sender_id)
        try:
            csvs = [p for p in d.iterdir() if p.is_file() and p.suffix.lower() == ".csv"]
        except OSError:
            return None
        if not csvs:
            return None
        return max(csvs, key=lambda p: p.stat().st_mtime)

    def _prune_csv_cache(self, sender_id, keep):
        """Remove all CSVs in the user's cache except `keep`."""
        d = self._user_csv_dir(sender_id)
        keep_path = Path(keep).resolve()
        for p in d.iterdir():
            try:
                if p.is_file() and p.resolve() != keep_path:
                    p.unlink()
            except OSError as e:
                ynab.log.warning("bot: could not prune %s: %s", p, e)

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

        dest = self._user_csv_dir(sender_id) / filename
        if not tg_download_file(self.token, doc["file_id"], dest):
            tg_send(self.token, chat_id, "Failed to download the file.", None)
            return

        if not ynab.is_revolut_csv(str(dest)):
            tg_send(self.token, chat_id,
                    "This doesn't look like a Revolut account statement CSV "
                    "(missing expected headers).", None)
            try:
                dest.unlink()
            except OSError:
                pass
            return

        # Keep only the latest CSV per user so the cache doesn't grow.
        self._prune_csv_cache(sender_id, dest)
        ynab.log.info("bot: received CSV %s from user %s", filename, sender_id)

        try:
            transactions = ynab.parse_revolut_csv(str(dest))
        except Exception as e:
            tg_send(self.token, chat_id, f"Error parsing CSV: {e}", None)
            return

        if not transactions:
            tg_send(self.token, chat_id, "CSV parsed but contains no transactions.", None)
            return

        user = get_user(self.db, sender_id)
        auto_approve = bool(user.get("auto_approve", 1)) if user else True
        for tx in transactions:
            tx["approved"] = auto_approve

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
        csv_path = self._last_csv_path(sender_id)
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
        except (Exception, SystemExit) as e:
            # SystemExit bypasses `except Exception`; catch it too so the bot
            # always reports failures instead of going silent when a helper
            # in revolut_to_ynab.py calls sys.exit(1).
            ynab.log.error("bot: reconcile failed for user %s: %s",
                           sender_id, traceback.format_exc())
            msg = f"exit({e.code})" if isinstance(e, SystemExit) else str(e)
            tg_send(self.token, chat_id, f"Reconcile failed: {msg}", None)
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

    # ── /cleanup_pending ─────────────────────────────────────────────────

    def _handle_cleanup_pending(self, chat_id, sender_id):
        cfg = self._user_config(sender_id)
        if not cfg:
            tg_send(self.token, chat_id, "Setup incomplete. Run /setup first.", None)
            return

        # Use the last uploaded CSV to also flip cleared state where the CSV
        # confirms the row has cleared on Revolut's side.
        csv_path = self._last_csv_path(sender_id)
        csv_arg = str(csv_path) if csv_path and csv_path.exists() else None

        hint = f" against _{csv_path.name}_" if csv_arg else " (no CSV — memo-only)"
        tg_send(self.token, chat_id,
                f"🧹 Scanning YNAB for stale '(pending)' memos{hint}...", None)

        try:
            buf = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = buf
            try:
                patched = ynab.cleanup_pending_memos(
                    cfg["ynab_token"], cfg["budget_id"], cfg["account_id"],
                    csv_path=csv_arg,
                )
            finally:
                sys.stdout = old_stdout
        except (Exception, SystemExit) as e:
            ynab.log.error("bot: cleanup_pending failed for user %s: %s",
                           sender_id, traceback.format_exc())
            msg = f"exit({e.code})" if isinstance(e, SystemExit) else str(e)
            tg_send(self.token, chat_id, f"Cleanup failed: {msg}", None)
            return

        if patched == 0:
            tg_send(self.token, chat_id,
                    "✓ No stale '(pending)' memos found.", None)
        else:
            tg_send(self.token, chat_id,
                    f"✓ Cleaned {patched} stale '(pending)' memo"
                    f"{'s' if patched != 1 else ''}.", None)

    # ── /dedupe ──────────────────────────────────────────────────────────
    #
    # UX: an inline-keyboard "checkbox list" — each orphaned YNAB transaction
    # is a toggleable button (⬜ / ✅). Action row at the bottom: select all,
    # clear, delete (with live count), and cancel. Long lists paginate.

    _DEDUPE_PAGE_SIZE = 6  # rows of toggle buttons per page

    def _handle_dedupe(self, chat_id, sender_id):
        """Scan YNAB for orphaned duplicate imports in the last CSV's date range."""
        cfg = self._user_config(sender_id)
        if not cfg:
            tg_send(self.token, chat_id, "Setup incomplete. Run /setup first.", None)
            return

        csv_path = self._last_csv_path(sender_id)
        if not csv_path or not csv_path.exists():
            tg_send(self.token, chat_id,
                    "No CSV available. Send me a Revolut CSV first, "
                    "then run /dedupe.", None)
            return

        tg_send(self.token, chat_id,
                f"🔎 Scanning YNAB for orphaned imports in {csv_path.name}…",
                None)

        try:
            report = ynab.find_orphaned_imports(
                cfg["ynab_token"], cfg["budget_id"], cfg["account_id"],
                str(csv_path),
            )
        except Exception as e:
            ynab.log.error("bot: dedupe scan failed for user %s: %s",
                           sender_id, traceback.format_exc())
            tg_send(self.token, chat_id, f"Dedupe scan failed: {e}", None)
            return

        orphans = report["orphans"]

        if not orphans:
            self._dedupe_candidates.pop(sender_id, None)
            tg_send(self.token, chat_id,
                    f"📅 Range: {report['start_date']} → {report['end_date']}\n"
                    f"📋 CSV: {report['csv_count']} txns | "
                    f"YNAB in range: {report['ynab_count_in_range']}\n\n"
                    f"✓ No orphaned imports found.",
                    None)
            return

        # Initialize selection state and store everything we need to re-render
        # the keyboard from a callback later.
        state = {
            "items": orphans,
            "selected": set(),       # set of indices into items
            "page": 0,
            "chat_id": chat_id,
            "message_id": None,      # filled in below
            "confirming": False,
            "report": {
                "start_date": report["start_date"],
                "end_date": report["end_date"],
                "csv_count": report["csv_count"],
                "ynab_count_in_range": report["ynab_count_in_range"],
            },
        }
        self._dedupe_candidates[sender_id] = state

        text, markup = self._render_dedupe_message(state)
        result = tg_send(self.token, chat_id, text, "Markdown", markup)
        if not result or not result.get("ok"):
            self._dedupe_candidates.pop(sender_id, None)
            desc = (result or {}).get("description", "unknown error")
            ynab.log.error("bot: dedupe keyboard send failed: %s", desc)
            tg_send(self.token, chat_id,
                    f"Couldn't post the dedupe picker: {desc}\n"
                    f"Check the bot logs (`docker compose logs bot --tail 50`).",
                    None)
            return
        try:
            state["message_id"] = result["result"]["message_id"]
        except (KeyError, TypeError):
            ynab.log.warning("bot: dedupe send returned no message_id; "
                             "callbacks will still work but edits may fail")

    # ── Keyboard rendering ───────────────────────────────────────────────

    @staticmethod
    def _fmt_orphan_button(orphan, selected):
        """Compact label for one orphan toggle button."""
        amt = orphan["amount"] / 1000
        payee = (orphan["payee_name"] or "?")[:18]
        # MM-DD is enough — the full range is shown in the header.
        date_short = orphan["date"][5:]  # 'YYYY-MM-DD' → 'MM-DD'
        check = "✅" if selected else "⬜"
        return f"{check} {date_short}  {amt:>8.2f}  {payee}"

    def _render_dedupe_message(self, state):
        """Build (text, reply_markup) for the current dedupe state."""
        report = state["report"]
        items = state["items"]
        selected = state["selected"]
        total = len(items)

        if state.get("confirming"):
            n = len(selected)
            text = (
                f"⚠ *Confirm deletion*\n\n"
                f"Delete *{n}* transaction{'s' if n != 1 else ''} from YNAB?\n"
                f"This cannot be undone."
            )
            keyboard = [[
                {"text": f"✅ Yes, delete {n}", "callback_data": "dd:ok"},
                {"text": "↩ Back", "callback_data": "dd:back"},
            ]]
            return text, {"inline_keyboard": keyboard}

        page_size = self._DEDUPE_PAGE_SIZE
        page_count = max(1, (total + page_size - 1) // page_size)
        page = max(0, min(state["page"], page_count - 1))
        state["page"] = page
        start = page * page_size
        end = min(start + page_size, total)

        text = (
            f"📅 *Range:* {report['start_date']} → {report['end_date']}\n"
            f"📋 CSV: {report['csv_count']} | "
            f"YNAB in range: {report['ynab_count_in_range']}\n\n"
            f"⚠ Found *{total}* orphaned import"
            f"{'s' if total != 1 else ''} "
            f"(YNAB rows whose `import_id` no longer matches the CSV).\n"
            f"Tick the rows to delete, then press 🗑."
        )
        if page_count > 1:
            text += f"\n\nPage {page + 1}/{page_count}"

        keyboard = []
        for idx in range(start, end):
            label = self._fmt_orphan_button(items[idx], idx in selected)
            keyboard.append([
                {"text": label, "callback_data": f"dd:t:{idx}"}
            ])

        if page_count > 1:
            nav = []
            if page > 0:
                nav.append({"text": "« Prev", "callback_data": f"dd:p:{page - 1}"})
            nav.append({"text": f"{page + 1}/{page_count}",
                        "callback_data": "dd:noop"})
            if page < page_count - 1:
                nav.append({"text": "Next »", "callback_data": f"dd:p:{page + 1}"})
            keyboard.append(nav)

        n_sel = len(selected)
        keyboard.append([
            {"text": "✅ All", "callback_data": "dd:all"},
            {"text": "⬜ None", "callback_data": "dd:none"},
        ])
        keyboard.append([
            {"text": f"🗑 Delete ({n_sel})", "callback_data": "dd:del"},
            {"text": "✖ Cancel", "callback_data": "dd:c"},
        ])
        return text, {"inline_keyboard": keyboard}

    # ── Callback queries ─────────────────────────────────────────────────

    def _handle_callback_query(self, cq):
        """Dispatch inline-keyboard button clicks."""
        cq_id = cq.get("id")
        sender_id = cq.get("from", {}).get("id")
        data = cq.get("data") or ""
        msg = cq.get("message") or {}
        chat_id = (msg.get("chat") or {}).get("id")
        message_id = msg.get("message_id")

        if data.startswith("dd:"):
            self._handle_dedupe_callback(
                cq_id, sender_id, chat_id, message_id, data[3:]
            )
            return

        # Unknown callback — just ack so spinner clears
        tg_answer_callback(self.token, cq_id)

    def _handle_dedupe_callback(self, cq_id, sender_id, chat_id, message_id, action):
        """Process a dedupe button click."""
        state = self._dedupe_candidates.get(sender_id)
        if not state:
            tg_answer_callback(self.token, cq_id,
                               "This list has expired. Run /dedupe again.",
                               show_alert=True)
            try:
                tg_edit_message(self.token, chat_id, message_id,
                                text="_Dedupe list expired._",
                                reply_markup={"inline_keyboard": []})
            except Exception:
                pass
            return

        # Keep message_id current — handles the case where _handle_dedupe
        # couldn't read it back from sendMessage.
        state["message_id"] = message_id
        state["chat_id"] = chat_id

        if action == "noop":
            tg_answer_callback(self.token, cq_id)
            return

        if action == "c":
            self._dedupe_candidates.pop(sender_id, None)
            tg_answer_callback(self.token, cq_id, "Cancelled.")
            try:
                tg_edit_message(self.token, chat_id, message_id,
                                text="✖ Dedupe cancelled.",
                                reply_markup={"inline_keyboard": []})
            except Exception:
                pass
            return

        if action == "all":
            state["selected"] = set(range(len(state["items"])))
            tg_answer_callback(self.token, cq_id, "All selected.")
            self._refresh_dedupe_message(state)
            return

        if action == "none":
            state["selected"].clear()
            tg_answer_callback(self.token, cq_id, "Cleared.")
            self._refresh_dedupe_message(state)
            return

        if action.startswith("t:"):
            try:
                idx = int(action[2:])
            except ValueError:
                tg_answer_callback(self.token, cq_id)
                return
            if 0 <= idx < len(state["items"]):
                if idx in state["selected"]:
                    state["selected"].remove(idx)
                else:
                    state["selected"].add(idx)
            tg_answer_callback(self.token, cq_id)
            self._refresh_dedupe_message(state)
            return

        if action.startswith("p:"):
            try:
                page = int(action[2:])
            except ValueError:
                tg_answer_callback(self.token, cq_id)
                return
            state["page"] = page
            tg_answer_callback(self.token, cq_id)
            self._refresh_dedupe_message(state)
            return

        if action == "del":
            if not state["selected"]:
                tg_answer_callback(self.token, cq_id, "Nothing selected.",
                                   show_alert=True)
                return
            state["confirming"] = True
            tg_answer_callback(self.token, cq_id)
            self._refresh_dedupe_message(state)
            return

        if action == "back":
            state["confirming"] = False
            tg_answer_callback(self.token, cq_id)
            self._refresh_dedupe_message(state)
            return

        if action == "ok":
            self._execute_dedupe_delete(cq_id, sender_id, state)
            return

        tg_answer_callback(self.token, cq_id)

    def _refresh_dedupe_message(self, state):
        """Re-render the dedupe message in place from current state."""
        text, markup = self._render_dedupe_message(state)
        try:
            tg_edit_message(self.token, state["chat_id"], state["message_id"],
                            text=text, reply_markup=markup)
        except Exception as e:
            ynab.log.warning("bot: dedupe edit failed: %s", e)

    def _execute_dedupe_delete(self, cq_id, sender_id, state):
        """Perform the YNAB deletes for currently selected items."""
        cfg = self._user_config(sender_id)
        if not cfg:
            tg_answer_callback(self.token, cq_id, "Setup incomplete.",
                               show_alert=True)
            return

        items = state["items"]
        to_delete = [items[i] for i in sorted(state["selected"])
                     if 0 <= i < len(items)]
        if not to_delete:
            tg_answer_callback(self.token, cq_id, "Nothing selected.",
                               show_alert=True)
            state["confirming"] = False
            self._refresh_dedupe_message(state)
            return

        tg_answer_callback(self.token, cq_id, f"Deleting {len(to_delete)}…")

        conn = None
        try:
            conn = ynab.init_db(cfg["db_path"])
        except Exception as e:
            ynab.log.warning("bot: could not open local DB for dedupe: %s", e)

        deleted = 0
        failures = []
        for o in to_delete:
            try:
                ynab.delete_ynab_transaction(
                    conn, cfg["ynab_token"], cfg["budget_id"], o["id"],
                )
                deleted += 1
                ynab.log.info(
                    "bot: dedupe deleted ynab_id=%s date=%s amt=%+.2f payee=%s "
                    "import_id=%s user=%s",
                    o["id"], o["date"], o["amount"] / 1000,
                    o["payee_name"], o["import_id"], sender_id,
                )
            except Exception as e:
                failures.append((o, str(e)))
                ynab.log.error("bot: dedupe delete failed id=%s: %s", o["id"], e)

        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

        # Drop the picker and report.
        self._dedupe_candidates.pop(sender_id, None)
        lines = [f"✓ Deleted {deleted}/{len(to_delete)} transaction(s)."]
        if failures:
            lines.append(f"\n⚠ {len(failures)} failed:")
            for o, err in failures[:5]:
                lines.append(f"  • {o['date']} {o['payee_name'][:25]}: {err[:80]}")
        try:
            tg_edit_message(self.token, state["chat_id"], state["message_id"],
                            text="\n".join(lines),
                            reply_markup={"inline_keyboard": []})
        except Exception:
            tg_send(self.token, state["chat_id"], "\n".join(lines))

    # ── Text-command fallbacks (used by /dedupe_cancel etc.) ─────────────

    def _handle_dedupe_cancel(self, chat_id, sender_id):
        state = self._dedupe_candidates.pop(sender_id, None)
        if state and state.get("message_id"):
            try:
                tg_edit_message(self.token, state["chat_id"],
                                state["message_id"],
                                text="✖ Dedupe cancelled.",
                                reply_markup={"inline_keyboard": []})
            except Exception:
                pass
        if state:
            tg_send(self.token, chat_id, "🗑 Dedupe selection cleared.", None)
        else:
            tg_send(self.token, chat_id,
                    "Nothing pending. Run /dedupe first to scan.", None)

    def _handle_dedupe_delete(self, chat_id, sender_id, text):
        """Legacy text-based delete (e.g. /dedupe_delete all). Buttons preferred."""
        state = self._dedupe_candidates.get(sender_id)
        if not state:
            tg_send(self.token, chat_id,
                    "No dedupe candidates pending. Run /dedupe first.", None)
            return
        items = state["items"]

        parts = text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            tg_send(self.token, chat_id,
                    "Tip: use the buttons above. "
                    "Or send /dedupe_delete 1,3,5 / /dedupe_delete all.",
                    None)
            return
        arg = parts[1].strip().lower()

        if arg in ("all", "*"):
            state["selected"] = set(range(len(items)))
        else:
            indices = set()
            try:
                for piece in arg.replace(" ", "").split(","):
                    if not piece:
                        continue
                    if "-" in piece:
                        a, b = piece.split("-", 1)
                        for i in range(int(a), int(b) + 1):
                            indices.add(i)
                    else:
                        indices.add(int(piece))
            except ValueError:
                tg_send(self.token, chat_id,
                        "Couldn't parse selection. Use 1,3,5 or 2-4 or all.",
                        None)
                return
            invalid = [i for i in indices if i < 1 or i > len(items)]
            if invalid:
                tg_send(self.token, chat_id,
                        f"Out-of-range index: {sorted(invalid)} "
                        f"(valid 1–{len(items)}).", None)
                return
            state["selected"] = {i - 1 for i in indices}

        # Skip the confirmation step for the explicit text command — the
        # user already typed exactly what they want.
        cq_state = type("X", (), {})()  # not used; reuse the helper directly
        # Build a fake context so we can reuse _execute_dedupe_delete logic
        # without a real callback id.
        cfg = self._user_config(sender_id)
        if not cfg:
            tg_send(self.token, chat_id, "Setup incomplete. Run /setup first.", None)
            return

        to_delete = [items[i] for i in sorted(state["selected"])
                     if 0 <= i < len(items)]
        if not to_delete:
            tg_send(self.token, chat_id, "Nothing selected.", None)
            return
        tg_send(self.token, chat_id,
                f"🗑 Deleting {len(to_delete)} transaction"
                f"{'s' if len(to_delete) != 1 else ''}…", None)

        conn = None
        try:
            conn = ynab.init_db(cfg["db_path"])
        except Exception as e:
            ynab.log.warning("bot: could not open local DB for dedupe: %s", e)

        deleted = 0
        failures = []
        for o in to_delete:
            try:
                ynab.delete_ynab_transaction(
                    conn, cfg["ynab_token"], cfg["budget_id"], o["id"],
                )
                deleted += 1
            except Exception as e:
                failures.append((o, str(e)))
                ynab.log.error("bot: dedupe delete failed id=%s: %s", o["id"], e)

        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

        self._dedupe_candidates.pop(sender_id, None)
        msg = [f"✓ Deleted {deleted}/{len(to_delete)} transaction(s)."]
        if failures:
            msg.append(f"\n⚠ {len(failures)} failed:")
            for o, err in failures[:5]:
                msg.append(f"  • {o['date']} {o['payee_name'][:25]}: {err[:80]}")
        tg_send(self.token, chat_id, "\n".join(msg), None)

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

            csv_path = self._last_csv_path(sender_id)
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
                    "Run `/crypto_setup` to pick a tracking account and add "
                    "your BTC xpub or ETH address.")
            return
        if not user.get("btc_xpub") and not user.get("eth_address"):
            tg_send(self.token, chat_id,
                    "No BTC xpub or ETH address on file. "
                    "Run `/crypto_setup` to add one.")
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

        parts.append("\nUse `/crypto_setup` to change these.")
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
                    "Then run `/crypto_setup` again.")
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
                    "Something went wrong. Run `/crypto_setup` again.")
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
            parts.append("Run `/crypto_setup` again to add one.")
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
