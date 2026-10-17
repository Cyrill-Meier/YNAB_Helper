"""/login and /logout_all (DESIGN §8.3, §9.1; F18, F25, F56, F61).

``/login`` needs a ready user and a running web UI. It mints a one-shot login token
(stored only as an HMAC) and sends a message whose **URL button** opens
``<WEB_UI_PUBLIC_URL>/auth?t=<token>``: the token is never part of the text (so no Markdown
or preview crawler can mangle or burn it, F25), the message goes to the user's private chat
only (F56), and its id is recorded so the web server deletes it after sign-in.

If Telegram refuses the message (typically a non-public ``WEB_UI_PUBLIC_URL``: the error
description then echoes the button URL), the error is logged with the token redacted and
the token is burnt at once, so a sign-in link never outlives a failed send.
"""

from __future__ import annotations

import logging

from ynab_helper import net
from ynab_helper.bot.context import Request
from ynab_helper.bot.telegram import inline_keyboard, url_button
from ynab_helper.errors import TelegramError
from ynab_helper.sessions import TokenHasher

log = logging.getLogger(__name__)


def cmd_login(req: Request) -> None:
    web = req.bot.web
    if web.status == "disabled":
        req.reply("🔒 The web dashboard is disabled on this server.")
        return
    if not web.usable():
        req.reply("⚠ The web dashboard is not running right now. Please try again later.")
        return
    config = web.config
    try:
        hasher = config.token_hasher()
        token = req.bot.users.issue_login_token(hasher, req.user_id, ttl_seconds=config.login_ttl)
    except Exception:
        log.exception("could not issue a login token for user %s", req.user_id)
        req.reply("❌ Couldn't create a sign-in link. Please try again.")
        return
    minutes = max(1, int(config.login_ttl) // 60)
    text = (
        "🌐 <b>Web dashboard</b>\n\n"
        f"Tap the button below to sign in (single use, expires in about {minutes} min).\n\n"
        "<i>Don't forward this message — whoever taps the button first is signed in as you. "
        "It is deleted automatically once you sign in.</i>"
    )
    keyboard = inline_keyboard([[url_button("Open dashboard", config.login_url(token))]])
    try:
        message = req.bot.tg.send_message(req.chat_id, text, reply_markup=keyboard)
    except Exception as exc:
        # Telegram echoes a rejected button URL in its error: never log the token.
        reason = net.redact_text(f"{type(exc).__name__}: {exc}", (token,))
        log.warning("sending the login button to %s failed: %s", req.user_id, reason)
        _burn(req, hasher, token)
        if isinstance(exc, TelegramError) and exc.error_code == 400:
            req.reply(
                "❌ Telegram refused the sign-in button. The admin should check that "
                "WEB_UI_PUBLIC_URL is a public https:// address."
            )
        else:
            req.reply("❌ Couldn't send the sign-in link. Please try /login again.")
        return
    message_id = message.get("message_id") if message else None
    chat = message.get("chat") if message else None
    chat_id = chat.get("id") if isinstance(chat, dict) else req.chat_id
    if isinstance(message_id, int) and isinstance(chat_id, int):
        try:
            req.bot.users.attach_login_message(
                hasher, token, chat_id=chat_id, message_id=message_id
            )
        except Exception:
            log.warning("could not record the login message of user %s", req.user_id, exc_info=True)


def _burn(req: Request, hasher: TokenHasher, token: str) -> None:
    """Invalidate a login token whose message may not have reached the user."""
    try:
        req.bot.users.discard_login_token(hasher, token)
    except Exception:
        log.warning("could not discard the unsent login token of user %s", req.user_id)


def cmd_logout_all(req: Request) -> None:
    removed = req.bot.users.revoke_sessions(req.user_id)
    req.reply(
        "🚪 Signed out of the web dashboard everywhere"
        + (f" ({removed} session(s) or sign-in link(s) removed)." if removed else ".")
    )
