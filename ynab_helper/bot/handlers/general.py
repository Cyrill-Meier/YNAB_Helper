"""/help, /start, /cancel and plain text (routed by the user's state)."""

from __future__ import annotations

from ynab_helper.bot import states as S
from ynab_helper.bot.context import Request
from ynab_helper.bot.render import h
from ynab_helper.version import version_line

FALLBACK_TEXT = "Send me a Revolut CSV to import, or use /help for commands."


def help_text(req: Request) -> str:
    state = req.user.state if req.user is not None else None
    lines = [f"<b>Revolut → YNAB Bot</b> — {h(version_line())}", ""]
    if state in S.SETUP_STATES:
        lines += ["ℹ You are in the middle of the YNAB setup — finish it or send /cancel.", ""]
    elif state in S.CRYPTO_STATES:
        lines += ["ℹ You are in the middle of the crypto setup — finish it or send /cancel.", ""]
    lines.append("📎 <b>Send a CSV</b> — import a Revolut statement into YNAB")
    lines += req.bot.registry.help_lines(admin=req.is_admin)
    return "\n".join(lines)


def cmd_help(req: Request) -> None:
    req.reply(help_text(req))


def cmd_cancel(req: Request) -> None:
    from ynab_helper.bot.handlers import crypto, onboarding

    state = req.user.state if req.user is not None else None
    if state in S.SETUP_STATES:
        onboarding.cancel(req)
    elif state in S.CRYPTO_STATES:
        crypto.cancel(req)
    else:
        req.reply("Nothing to cancel.")


def on_text(req: Request) -> None:
    """A message that is neither a command nor a document."""
    from ynab_helper.bot.handlers import crypto, onboarding

    state = req.user.state if req.user is not None else None
    if state in S.SETUP_STATES:
        onboarding.on_text(req)
    elif state in S.CRYPTO_STATES:
        crypto.on_text(req)
    else:
        req.reply(FALLBACK_TEXT)
