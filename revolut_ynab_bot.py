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
  /settings          — Open an inline settings menu
  /login             — Mint a one-shot URL to the web dashboard
  /auto_approve on|off — Toggle whether imported txns are auto-approved
  /crypto            — Sync crypto portfolio value → YNAB tracking account
  /crypto_setup      — Configure BTC xpub, ETH address, crypto tracking account
  /crypto_status     — Show current crypto configuration
  /help              — List available commands

Admin commands:
  /approve <user_id> — Approve a pending user
  /deny <user_id>    — Deny a pending user
  /users             — List all users and their states
  /ip                — Show the host's public IP and hostname
  /logs [N]          — Download the bot log file (optionally last N lines)

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

__version__ = "1.2.4"


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


# ─── Bot command registry ──────────────────────────────────────────────────
#
# Pushed to Telegram via setMyCommands at startup so users get an autocomplete
# list when they type "/" — without us having to keep BotFather in sync. Two
# scopes:
#   • "default" — everyone sees these
#   • BotCommandScopeChat for the admin — admin sees these PLUS the admin set
#
# Descriptions: Telegram caps each at 256 chars; keep them short and
# action-first so they read well in the autocomplete dropdown.

USER_COMMANDS = [
    ("reconcile",       "Reconcile YNAB balance against the last CSV"),
    ("cleanup_pending", "Strip stale (pending) memos from cleared txns"),
    ("dedupe",          "Find orphaned duplicate imports in YNAB"),
    ("status",          "Show YNAB account balance & last import"),
    ("setup",           "Re-run YNAB setup (token / budget / account)"),
    ("settings",        "Open the settings menu"),
    ("auto_approve",    "Toggle auto-approval of imported txns"),
    ("login",           "Get a one-shot URL to the web dashboard"),
    ("crypto",          "Sync crypto portfolio value to YNAB"),
    ("crypto_setup",    "Configure BTC xpub / ETH address / tracking acct"),
    ("crypto_status",   "Show current crypto configuration"),
    ("help",            "Show available commands"),
]

ADMIN_EXTRA_COMMANDS = [
    ("approve", "Approve a pending user (/approve <id>)"),
    ("deny",    "Deny a user (/deny <id>)"),
    ("users",   "List registered users"),
    ("ip",      "Show this host's public IP and hostname"),
    ("logs",    "Download the bot log file (or last N lines)"),
]


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


def tg_send(token, chat_id, text, parse_mode="Markdown", reply_markup=None,
            disable_link_preview=False):
    """Send a text message. Long messages are split at ~4000 chars.

    Returns the API result of the LAST chunk sent, so callers that supplied
    a ``reply_markup`` can read back ``message_id`` for later edits.

    Set ``disable_link_preview=True`` to suppress Telegram's link-preview
    crawler — required for messages that contain one-shot URL tokens
    (the preview crawler hits the URL and burns the token before the
    user can click it).
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
        if disable_link_preview:
            # New API name (Bot API 7.0+); older clients accept the legacy
            # `disable_web_page_preview` field — set both for safety.
            data["link_preview_options"] = {"is_disabled": True}
            data["disable_web_page_preview"] = True
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


def tg_send_document(token, chat_id, file_path, caption=None,
                     filename=None, parse_mode=None):
    """Send a file as a Telegram document via multipart/form-data.

    Telegram caps bot-API document uploads at 50 MB. Caller is
    responsible for staying under that.
    """
    import uuid
    file_path = Path(file_path)
    try:
        file_bytes = file_path.read_bytes()
    except OSError as e:
        ynab.log.error("tg_send_document: cannot read %s: %s", file_path, e)
        return {"ok": False, "description": str(e)}

    safe_name = (filename or file_path.name).replace('"', "_")
    boundary = f"----RevolutYNABBotBoundary{uuid.uuid4().hex}"
    crlf = b"\r\n"

    parts = []

    def add_field(name, value):
        parts.append(f"--{boundary}".encode())
        parts.append(crlf)
        parts.append(
            f'Content-Disposition: form-data; name="{name}"'.encode()
        )
        parts.append(crlf + crlf)
        parts.append(str(value).encode("utf-8"))
        parts.append(crlf)

    add_field("chat_id", chat_id)
    if caption:
        add_field("caption", caption)
        if parse_mode:
            add_field("parse_mode", parse_mode)

    parts.append(f"--{boundary}".encode())
    parts.append(crlf)
    parts.append(
        f'Content-Disposition: form-data; name="document"; '
        f'filename="{safe_name}"'.encode()
    )
    parts.append(crlf)
    parts.append(b"Content-Type: application/octet-stream")
    parts.append(crlf + crlf)
    parts.append(file_bytes)
    parts.append(crlf)
    parts.append(f"--{boundary}--".encode())
    parts.append(crlf)

    body = b"".join(parts)
    url = f"{TELEGRAM_API.format(token=token)}/sendDocument"
    req = Request(url, data=body, method="POST")
    req.add_header(
        "Content-Type", f"multipart/form-data; boundary={boundary}"
    )
    req.add_header("Content-Length", str(len(body)))

    try:
        with urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        err_body = e.read().decode()
        ynab.log.error("Telegram sendDocument error %d: %s", e.code, err_body)
        return {"ok": False, "description": err_body}
    except (URLError, OSError) as e:
        ynab.log.error("Telegram sendDocument network error: %s", e)
        return {"ok": False, "description": str(e)}


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
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL mode lets the bot's main thread write while the FastAPI worker
    # threads read concurrently without "database is locked" errors.
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
    except sqlite3.Error:
        pass
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
    # Web sessions: both one-shot login URL tokens (kind='login', short
    # TTL, deleted on first use) and longer-lived browser cookies
    # (kind='session', sliding TTL). Stored hashed (SHA-256 hex) so a DB
    # leak doesn't let an attacker hijack live sessions.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS web_sessions (
            token_hash   TEXT PRIMARY KEY,
            telegram_id  INTEGER NOT NULL,
            kind         TEXT NOT NULL CHECK (kind IN ('login','session')),
            created_at   REAL NOT NULL,
            expires_at   REAL NOT NULL,
            user_agent   TEXT,
            ip           TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_web_sessions_telegram "
        "ON web_sessions(telegram_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_web_sessions_expires "
        "ON web_sessions(expires_at)"
    )
    # Idempotent migration: tg_* columns let us auto-delete the /login DM
    # after the user successfully exchanges the URL for a session.
    for col in ("tg_chat_id INTEGER", "tg_message_id INTEGER"):
        name = col.split()[0]
        try:
            conn.execute(f"ALTER TABLE web_sessions ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass  # column already exists
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
            elif text.startswith("/ip"):
                self._admin_ip(chat_id)
                return
            elif text.startswith("/logs"):
                self._admin_logs(chat_id, text)
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
            elif cmd == "/settings":
                self._handle_settings(chat_id, sender_id)
            elif cmd == "/login":
                self._handle_login(chat_id, sender_id)
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

    # ── Admin: /ip ───────────────────────────────────────────────────────

    def _admin_ip(self, chat_id):
        """Report the bot host's outbound public IP and identity."""
        import socket

        public_ip = "(unknown — egress lookup failed)"
        for url in ("https://api.ipify.org", "https://ifconfig.me/ip"):
            try:
                req = Request(url, headers={"User-Agent": "revolut-ynab-bot"})
                with urlopen(req, timeout=8) as resp:
                    candidate = resp.read().decode().strip()
                if candidate:
                    public_ip = candidate
                    break
            except (HTTPError, URLError, OSError) as e:
                ynab.log.warning("admin /ip: %s failed: %s", url, e)

        try:
            hostname = socket.gethostname()
        except Exception:
            hostname = "(unknown)"

        # Best-effort: also list non-loopback local IPs (useful for LAN debug)
        local_ips = []
        try:
            infos = socket.getaddrinfo(hostname, None)
            for fam, _, _, _, sockaddr in infos:
                ip = sockaddr[0]
                if ip and not ip.startswith("127.") and ip != "::1":
                    if ip not in local_ips:
                        local_ips.append(ip)
        except Exception:
            pass

        lines = [
            "🌐 *Bot host info*",
            f"Public IP: `{public_ip}`",
            f"Hostname: `{hostname}`",
        ]
        if local_ips:
            lines.append("Local IPs: " + ", ".join(f"`{x}`" for x in local_ips))
        tg_send(self.token, chat_id, "\n".join(lines))

    # ── Admin: /logs ─────────────────────────────────────────────────────

    def _resolve_log_file(self):
        """Find the path the bot's file-log handler writes to, if any."""
        for h in ynab.log.handlers:
            base = getattr(h, "baseFilename", None)
            if base:
                return Path(base)
        env_path = os.environ.get("LOG_FILE")
        if env_path:
            return Path(env_path).expanduser()
        return None

    def _admin_logs(self, chat_id, text):
        """Send the bot log file as a download. `/logs N` → last N lines."""
        log_path = self._resolve_log_file()
        if not log_path or not log_path.exists():
            tg_send(self.token, chat_id,
                    "No log file is configured (or it doesn't exist yet).\n"
                    "Set `LOG_FILE` in the env to enable file logging.",
                    None)
            return

        parts = text.split()
        tail_n = None
        if len(parts) >= 2:
            try:
                tail_n = int(parts[1])
                if tail_n <= 0:
                    raise ValueError
            except ValueError:
                tg_send(self.token, chat_id,
                        "Usage: `/logs` (full file) or `/logs N` "
                        "(last N lines).")
                return

        size = log_path.stat().st_size
        # Telegram's bot-API document limit is 50 MB. Auto-tail if larger.
        TG_DOC_LIMIT = 45 * 1024 * 1024  # leave headroom
        auto_tailed = False
        if tail_n is None and size > TG_DOC_LIMIT:
            tail_n = 100_000
            auto_tailed = True
            tg_send(self.token, chat_id,
                    f"Log is {size / 1024 / 1024:.1f} MB — sending the last "
                    f"{tail_n:,} lines instead.",
                    None)

        upload_path = log_path
        tmp_path = None
        line_count = None
        if tail_n is not None:
            from collections import deque
            try:
                with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                    last = list(deque(f, maxlen=tail_n))
            except OSError as e:
                tg_send(self.token, chat_id, f"Couldn't read log: {e}", None)
                return
            line_count = len(last)
            tmp_path = self.data_dir / f"_logs_tail_{os.getpid()}.log"
            try:
                tmp_path.write_text("".join(last), encoding="utf-8")
            except OSError as e:
                tg_send(self.token, chat_id, f"Couldn't write temp log: {e}",
                        None)
                return
            upload_path = tmp_path

        if line_count is not None:
            label = "full file" if not auto_tailed and tail_n is None else \
                    f"last {line_count:,} lines"
            caption = f"📜 {log_path.name} — {label}"
        else:
            caption = (f"📜 {log_path.name} "
                       f"({size / 1024:.1f} KB, full file)")

        result = tg_send_document(
            self.token, chat_id, upload_path,
            caption=caption, filename=log_path.name,
        )

        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except OSError:
                pass

        if not result.get("ok"):
            tg_send(self.token, chat_id,
                    f"Failed to send log: {result.get('description', 'unknown')}",
                    None)

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
        lines.append("\nTap a button below — or reply with the number.")
        markup = self._reply_keyboard([b["name"] for b in budget_list],
                                      placeholder="Tap your budget")
        tg_send(self.token, chat_id, "\n".join(lines), None, markup)

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

        # Resolve selection: either an exact (case-insensitive) name match
        # against a reply-keyboard button, or a 1-based number from the body.
        selected = self._match_choice(text, budget_list, key="name")
        if not selected:
            lines = ["Please tap your budget — or reply with the number:\n"]
            for i, b in enumerate(budget_list, 1):
                lines.append(f"  {i}. {b['name']}")
            markup = self._reply_keyboard([b["name"] for b in budget_list],
                                          placeholder="Tap your budget")
            tg_send(self.token, chat_id, "\n".join(lines), None, markup)
            return

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
        lines.append("\nTap an account below — or reply with the number.")
        markup = self._reply_keyboard([a["name"] for a in account_list],
                                      placeholder="Tap your account")
        tg_send(self.token, chat_id, "\n".join(lines), reply_markup=markup)

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

        selected = self._match_choice(text, account_list, key="name")
        if not selected:
            lines = ["Please tap your account — or reply with the number:\n"]
            for i, a in enumerate(account_list, 1):
                bal = a["balance"] / 1000
                lines.append(f"  {i}. {a['name']} ({a['type']}) — {bal:,.2f}")
            markup = self._reply_keyboard([a["name"] for a in account_list],
                                          placeholder="Tap your account")
            tg_send(self.token, chat_id, "\n".join(lines), None, markup)
            return

        upsert_user(self.db, sender_id,
                    account_id=selected["id"],
                    account_name=selected["name"],
                    state="ready",
                    temp_data=None)

        # Remove the reply keyboard now that selection is complete.
        tg_send(self.token, chat_id, (
            f"🎉 All set!\n\n"
            f"  Budget:  {user['budget_name']}\n"
            f"  Account: {selected['name']}\n\n"
            f"You can now send me Revolut CSV files to import.\n"
            f"Type /help to see all commands."
        ), None, {"remove_keyboard": True})

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

    # ── /settings ────────────────────────────────────────────────────────
    #
    # Inline-keyboard menu — fulfills the global "/settings" command Telegram
    # asks every bot to support. Surfaces the per-user settings (auto_approve,
    # crypto setup, full re-setup) in one tap-friendly screen instead of
    # scattering them across separate commands.

    def _render_settings_menu(self, sender_id):
        """Build (text, reply_markup) for the settings menu."""
        user = get_user(self.db, sender_id) or {}
        auto = bool(user.get("auto_approve", 1))

        text = (
            "⚙ *Settings*\n\n"
            "Tap an option below to change it. Changes take effect on the "
            "next imported CSV."
        )
        keyboard = [
            [{
                "text": (f"{'✅' if auto else '⬜'} Auto-approve imports: "
                         f"{'ON' if auto else 'OFF'}"),
                # callback_data flips the value
                "callback_data": f"st:auto:{0 if auto else 1}",
            }],
            [{"text": "🪙 Crypto setup",
              "callback_data": "st:crypto"}],
            [{"text": "🔧 Re-run YNAB setup",
              "callback_data": "st:setup"}],
            [{"text": "✖ Close",
              "callback_data": "st:close"}],
        ]
        return text, {"inline_keyboard": keyboard}

    def _handle_settings(self, chat_id, sender_id):
        text, markup = self._render_settings_menu(sender_id)
        tg_send(self.token, chat_id, text, "Markdown", markup)

    # ── /login ───────────────────────────────────────────────────────────
    #
    # Mints a one-shot URL token and replies with the link. We persist the
    # token (hashed) in the same DB the FastAPI app reads, so the bot's
    # main thread is the one issuing — but the consumer is the web server
    # thread. WAL mode + busy_timeout make this race-safe.

    def _handle_login(self, chat_id, sender_id):
        web = getattr(self, "_web_config", None)
        if web is None or not web.enabled:
            tg_send(self.token, chat_id,
                    "🔒 Web UI is disabled on this server.\n"
                    "Set `WEB_UI_ENABLED=1` (and the matching env vars) "
                    "in the bot's environment, then redeploy.",
                    None)
            return
        if not web.public_url:
            tg_send(self.token, chat_id,
                    "Web UI is enabled but `WEB_UI_PUBLIC_URL` isn't set, "
                    "so I don't know which URL to send you to.",
                    None)
            return
        try:
            from web import auth as web_auth
            token = web_auth.issue_login_token(
                self.db, sender_id,
                ttl_seconds=web.login_ttl,
                ip="bot",
                user_agent="telegram",
            )
        except Exception as e:
            ynab.log.error("bot: /login token issue failed: %s", e)
            tg_send(self.token, chat_id,
                    f"Couldn't mint a login token: {e}", None)
            return
        url = web.login_url(token)
        ttl_min = max(1, web.login_ttl // 60)
        # Disable link preview — Telegram's preview crawler would
        # otherwise fetch the URL itself, validate the token, get a
        # session cookie (which it discards), and leave the user with
        # an "expired" link when they tap it.
        result = tg_send(self.token, chat_id, (
            f"🌐 *Web dashboard*\n\n"
            f"Tap to sign in (single-use, expires in ~{ttl_min} min):\n"
            f"{url}\n\n"
            f"_Don't forward this URL — anyone who clicks it before you "
            f"will land in your account. The bot will delete this "
            f"message automatically once you sign in._"
        ), "Markdown", disable_link_preview=True)
        # Record (chat_id, message_id) on the token row so the web
        # server can delete this DM after the user signs in. Best-effort:
        # if Telegram didn't return a message_id, we just skip the
        # auto-delete.
        try:
            sent_msg = (result or {}).get("result") or {}
            sent_chat_id = (sent_msg.get("chat") or {}).get("id")
            sent_msg_id = sent_msg.get("message_id")
            if sent_chat_id and sent_msg_id:
                from web import auth as web_auth
                web_auth.attach_tg_message(
                    self.db, token, sent_chat_id, sent_msg_id,
                )
        except Exception as e:
            ynab.log.warning(
                "bot: could not record /login message_id (%s) — "
                "auto-delete on sign-in won't fire for this token", e,
            )

    def _handle_settings_callback(self, cq_id, sender_id, chat_id,
                                  message_id, action):
        """Process a tap on the settings menu."""
        if action == "close":
            tg_answer_callback(self.token, cq_id, "Closed.")
            try:
                tg_edit_message(self.token, chat_id, message_id,
                                text="⚙ Settings closed.",
                                reply_markup={"inline_keyboard": []})
            except Exception:
                pass
            return

        if action.startswith("auto:"):
            try:
                new_val = int(action.split(":", 1)[1])
            except ValueError:
                tg_answer_callback(self.token, cq_id)
                return
            upsert_user(self.db, sender_id, auto_approve=1 if new_val else 0)
            tg_answer_callback(
                self.token, cq_id,
                f"Auto-approve {'enabled' if new_val else 'disabled'}.",
            )
            text, markup = self._render_settings_menu(sender_id)
            try:
                tg_edit_message(self.token, chat_id, message_id,
                                text=text, reply_markup=markup)
            except Exception as e:
                ynab.log.warning("bot: settings re-render failed: %s", e)
            return

        if action == "crypto":
            tg_answer_callback(self.token, cq_id, "Opening crypto setup…")
            try:
                tg_edit_message(self.token, chat_id, message_id,
                                text="⚙ Settings closed — opening crypto setup.",
                                reply_markup={"inline_keyboard": []})
            except Exception:
                pass
            self._handle_crypto_setup(chat_id, sender_id)
            return

        if action == "setup":
            tg_answer_callback(self.token, cq_id, "Restarting setup…")
            upsert_user(self.db, sender_id, state="awaiting_token",
                        ynab_token=None, budget_id=None, budget_name=None,
                        account_id=None, account_name=None, temp_data=None)
            try:
                tg_edit_message(self.token, chat_id, message_id,
                                text="⚙ Settings closed — restarting YNAB setup.",
                                reply_markup={"inline_keyboard": []})
            except Exception:
                pass
            tg_send(self.token, chat_id,
                    "Let's set up your account again.\n\n"
                    "Please send me your YNAB Personal Access Token.\n"
                    "(Get it from app.ynab.com → Account Settings → "
                    "Developer Settings)",
                    None)
            return

        # Unknown — just clear spinner
        tg_answer_callback(self.token, cq_id)

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
            "⚙ /settings — Open the settings menu",
            "🌐 /login — Get a one-shot URL to the web dashboard",
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
                "/ip — Show this host's public IP / hostname",
                "/logs `[N]` — Download bot log file (or last N lines)",
            ])
        tg_send(self.token, chat_id, "\n".join(lines))

    # ── Reply-keyboard helpers ───────────────────────────────────────────

    @staticmethod
    def _reply_keyboard(labels, placeholder=None, columns=1):
        """Build a one-time ReplyKeyboardMarkup with the given labels.

        Telegram caps button labels at ~64 chars; longer ones get truncated
        in the UI. We don't enforce that here — caller picks short labels.
        """
        if not labels:
            return None
        rows = []
        if columns <= 1:
            rows = [[{"text": str(l)}] for l in labels]
        else:
            for i in range(0, len(labels), columns):
                rows.append([{"text": str(l)} for l in labels[i:i + columns]])
        markup = {
            "keyboard": rows,
            "one_time_keyboard": True,   # hide after the user taps a button
            "resize_keyboard": True,     # shrink to fit content
            "selective": True,           # only show to the recipient
        }
        if placeholder:
            markup["input_field_placeholder"] = placeholder[:64]
        return markup

    @staticmethod
    def _match_choice(text, items, key):
        """Resolve a user reply to one item in a list.

        Accepts either an exact (case-insensitive, whitespace-trimmed) match
        on `item[key]` or a 1-based number indexing into `items`. Returns
        the matching item or None.
        """
        if not text or not items:
            return None
        # Exact-name match — the reply-keyboard happy path.
        norm = text.strip().casefold()
        for it in items:
            if str(it.get(key, "")).strip().casefold() == norm:
                return it
        # Legacy/typed fallback: 1-based number.
        try:
            idx = int(text.strip()) - 1
        except ValueError:
            return None
        if 0 <= idx < len(items):
            return items[idx]
        return None

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

        if data.startswith("st:"):
            self._handle_settings_callback(
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

    def _register_bot_commands(self):
        """Push command lists to Telegram so users get /-autocomplete.

        Two scopes are pushed:
          - "default" — every user sees the regular command set
          - BotCommandScopeChat for the admin — admin sees user + admin sets

        Failures are logged at WARNING but never crash startup; the bot still
        works without autocomplete.
        """
        user_cmds = [{"command": c, "description": d} for c, d in USER_COMMANDS]
        admin_cmds = [{"command": c, "description": d}
                      for c, d in USER_COMMANDS + ADMIN_EXTRA_COMMANDS]

        # Default scope (visible to everyone). We always overwrite so removed
        # commands disappear from clients and renamed descriptions update.
        result = tg_request(self.token, "setMyCommands", {
            "commands": user_cmds,
            "scope": {"type": "default"},
        })
        if not result.get("ok"):
            ynab.log.warning("bot: setMyCommands(default) failed: %s",
                             result.get("description"))

        # Admin-chat override
        result = tg_request(self.token, "setMyCommands", {
            "commands": admin_cmds,
            "scope": {"type": "chat", "chat_id": self.admin_id},
        })
        if not result.get("ok"):
            ynab.log.warning("bot: setMyCommands(admin) failed: %s",
                             result.get("description"))
        else:
            ynab.log.info(
                "bot: registered %d user commands (default scope) and "
                "%d admin commands (chat scope=%s)",
                len(user_cmds), len(admin_cmds), self.admin_id,
            )

    def run(self):
        """Start the bot with long-polling."""
        me = tg_request(self.token, "getMe")
        if not me.get("ok"):
            print("Error: invalid Telegram bot token.")
            sys.exit(1)
        # Register / refresh slash-command autocomplete with Telegram.
        # Cheap idempotent call — safe to run on every restart.
        self._register_bot_commands()
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

    # ── Optional: web UI (FastAPI on a daemon thread) ────────────────────
    try:
        from web import WebConfig
        from web import server as web_server
    except ImportError as e:
        ynab.log.warning("web: import failed (%s) — web UI disabled", e)
        web_cfg = None
    else:
        web_cfg = WebConfig.from_env(data_dir)
        problems = web_cfg.validate()
        if web_cfg.enabled and problems:
            for p in problems:
                print(f"⚠ web: {p}")
                ynab.log.warning("web: %s", p)
            print("web: refusing to start with the above issues.")
            web_cfg.enabled = False
        if web_cfg.enabled:
            try:
                web_server.serve_in_thread(
                    config=web_cfg,
                    bot_db_path=user_db_path,
                    log=ynab.log,
                    on_ready=lambda: ynab.log.info(
                        "web: serving on %s:%d (public=%s)",
                        web_cfg.host, web_cfg.port, web_cfg.public_url,
                    ),
                )
                print(f"   Web UI: http://{web_cfg.host}:{web_cfg.port} "
                      f"(public {web_cfg.public_url or 'NOT SET'})")
            except Exception as e:
                ynab.log.error("web: failed to start: %s", e)
                web_cfg.enabled = False
    bot._web_config = web_cfg

    bot.run()


if __name__ == "__main__":
    main()
