"""Auth helpers — token generation, hashing, session lifecycle.

Tokens never live on disk in plaintext. We store SHA-256 hex digests in
the ``web_sessions`` table; the cleartext token only exists in transit
(URL fragment for the one-shot login token, cookie for the session
token). A DB leak therefore never lets an attacker hijack a live
session.
"""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
import time
from typing import Optional, Tuple


def hash_token(token):
    """Stable, fast, sufficient. We never need to reverse this."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def new_token():
    """256 bits of urlsafe randomness — collision-free for our scale."""
    return secrets.token_urlsafe(32)


def issue_login_token(conn, telegram_id, ttl_seconds, ip=None, user_agent=None):
    """Mint a one-shot URL token. Returns the cleartext token."""
    token = new_token()
    now = time.time()
    conn.execute(
        "INSERT INTO web_sessions "
        "(token_hash, telegram_id, kind, created_at, expires_at, "
        " user_agent, ip) "
        "VALUES (?, ?, 'login', ?, ?, ?, ?)",
        (hash_token(token), int(telegram_id), now, now + int(ttl_seconds),
         (user_agent or "")[:200], (ip or "")[:64]),
    )
    conn.commit()
    return token


def attach_tg_message(conn, token, tg_chat_id, tg_message_id):
    """Record which Telegram message contained this login URL.

    Used so that ``consume_login_token`` can hand the chat/message IDs
    back to the web server, which then deletes the DM via Telegram's
    ``deleteMessage`` once the user has successfully exchanged the URL
    for a cookie session.
    """
    if not (tg_chat_id and tg_message_id):
        return
    conn.execute(
        "UPDATE web_sessions SET tg_chat_id = ?, tg_message_id = ? "
        "WHERE token_hash = ? AND kind = 'login'",
        (int(tg_chat_id), int(tg_message_id), hash_token(token)),
    )
    conn.commit()


def consume_login_token(conn, token, ip=None, user_agent=None,
                        session_ttl=1800, absolute_ttl=12 * 3600):
    """Validate + delete a one-shot login token, then mint a session token.

    Returns ``(telegram_id, session_token, expires_at, tg_chat_id,
    tg_message_id)`` on success, or ``None``. The tg_* fields may be
    ``None`` if the bot didn't record them (e.g. older login tokens
    minted before the column existed, or sendMessage didn't return the
    message_id).
    """
    if not token:
        return None
    h = hash_token(token)
    now = time.time()
    row = conn.execute(
        "SELECT telegram_id, expires_at, tg_chat_id, tg_message_id "
        "FROM web_sessions WHERE token_hash = ? AND kind = 'login'",
        (h,),
    ).fetchone()
    if not row:
        return None
    if row["expires_at"] < now:
        # Expired login link — clean it up too.
        conn.execute("DELETE FROM web_sessions WHERE token_hash = ?", (h,))
        conn.commit()
        return None
    telegram_id = row["telegram_id"]
    tg_chat_id = row["tg_chat_id"] if "tg_chat_id" in row.keys() else None
    tg_message_id = row["tg_message_id"] if "tg_message_id" in row.keys() else None
    # Burn the login token immediately — single use.
    conn.execute("DELETE FROM web_sessions WHERE token_hash = ?", (h,))

    session_token = new_token()
    expires = now + int(session_ttl)
    conn.execute(
        "INSERT INTO web_sessions "
        "(token_hash, telegram_id, kind, created_at, expires_at, "
        " user_agent, ip) "
        "VALUES (?, ?, 'session', ?, ?, ?, ?)",
        (hash_token(session_token), int(telegram_id), now, expires,
         (user_agent or "")[:200], (ip or "")[:64]),
    )
    conn.commit()
    return telegram_id, session_token, expires, tg_chat_id, tg_message_id


def lookup_session(conn, token, sliding_ttl=1800, absolute_ttl=12 * 3600):
    """Validate a session cookie. Returns (telegram_id, new_expires_at) or None.

    On success extends ``expires_at`` by ``sliding_ttl`` (capped by
    absolute_ttl from ``created_at``). Stale rows are garbage-collected
    opportunistically.
    """
    if not token:
        return None
    h = hash_token(token)
    now = time.time()
    row = conn.execute(
        "SELECT telegram_id, created_at, expires_at FROM web_sessions "
        "WHERE token_hash = ? AND kind = 'session'",
        (h,),
    ).fetchone()
    if not row:
        return None
    if row["expires_at"] < now:
        conn.execute("DELETE FROM web_sessions WHERE token_hash = ?", (h,))
        conn.commit()
        return None
    # Cap by absolute TTL so a stolen cookie can't be refreshed forever.
    abs_deadline = row["created_at"] + int(absolute_ttl)
    if now > abs_deadline:
        conn.execute("DELETE FROM web_sessions WHERE token_hash = ?", (h,))
        conn.commit()
        return None
    new_expires = min(now + int(sliding_ttl), abs_deadline)
    conn.execute(
        "UPDATE web_sessions SET expires_at = ? WHERE token_hash = ?",
        (new_expires, h),
    )
    conn.commit()
    return row["telegram_id"], new_expires


def delete_session(conn, token):
    """Used by /logout."""
    if not token:
        return
    conn.execute(
        "DELETE FROM web_sessions WHERE token_hash = ? AND kind = 'session'",
        (hash_token(token),),
    )
    conn.commit()


def purge_expired(conn):
    """Periodic cleanup; safe to call often."""
    conn.execute(
        "DELETE FROM web_sessions WHERE expires_at < ?",
        (time.time(),),
    )
    conn.commit()


def csrf_token_for(session_token):
    """Deterministically derive a CSRF token from the session token.

    Double-submit pattern: the same value is in the cookie and in
    every state-changing request's ``X-CSRF-Token`` header. An attacker
    on a third-party site can't read the cookie value due to SameSite,
    so they can't put it in the header either.
    """
    return hashlib.sha256(
        b"csrf:" + session_token.encode("utf-8")
    ).hexdigest()[:32]
