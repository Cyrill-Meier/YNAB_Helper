"""Web UI for the Revolut → YNAB bot.

Architectural notes:

* Runs in the SAME process as the bot, on a daemon thread, so both share
  the SQLite databases under ``data_dir`` without IPC. WAL mode is
  enabled on every DB open path so concurrent reads from FastAPI worker
  threads don't collide with the bot's writes.
* Auth is two-stage: the bot mints a one-shot URL token via /login, the
  browser exchanges that for a long-lived signed cookie at /auth, and
  every subsequent request authenticates by cookie.
* No external state. State persists in the existing ``bot_users.db`` (a
  new ``web_sessions`` table) so a container restart preserves logins.
"""

from .config import WebConfig

__all__ = ["WebConfig"]
