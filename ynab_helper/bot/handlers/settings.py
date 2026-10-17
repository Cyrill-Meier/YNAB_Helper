"""/settings menu and /auto_approve (DESIGN §8.3).

The inline menu keeps v1's callback data (``st:auto:0|1``, ``st:crypto``, ``st:setup``,
``st:close``) because such buttons still sit in users' chats; the gate only lets ``ready``
users press them (F03) and the data is parsed strictly. ``st:auto`` carries the target
value (not a toggle), as in v1.
"""

from __future__ import annotations

from typing import Final

from ynab_helper.bot import render
from ynab_helper.bot import states as S
from ynab_helper.bot.context import Request
from ynab_helper.bot.handlers._common import busy, expire_button

_ON: Final = frozenset({"on", "true", "1", "yes"})
_OFF: Final = frozenset({"off", "false", "0", "no"})
_USAGE: Final = "Usage: <code>/auto_approve on</code> or <code>/auto_approve off</code>"


def cmd_settings(req: Request) -> None:
    assert req.user is not None
    text, markup = render.settings_menu(req.user)
    req.reply(text, markup)


def cmd_auto_approve(req: Request) -> None:
    assert req.user is not None
    arg = req.args.split()[0].lower() if req.args else ""
    if not arg:
        state = "ON" if req.user.auto_approve else "OFF"
        req.reply(
            f"Auto-approve is currently <b>{state}</b>.\n\n{_USAGE}\n"
            "When OFF, imported transactions land in YNAB's inbox as unapproved for review."
        )
        return
    if arg not in _ON | _OFF:
        req.reply(_USAGE)
        return
    value = arg in _ON
    if req.bot.users.update_if(req.user_id, states=S.READY_STATES, auto_approve=value) is None:
        req.reply("Please finish what you are doing first (or /cancel).")
        return
    req.reply(f"✅ Auto-approve set to <b>{'ON' if value else 'OFF'}</b>.")


def cb_settings(req: Request) -> None:
    from ynab_helper.bot.handlers import crypto, onboarding

    action = (req.incoming.data or "")[3:]
    if action == "close":
        req.answer("Closed.")
        req.edit("⚙ Settings closed.", fallback=False)
    elif action in ("auto:0", "auto:1"):
        value = action == "auto:1"
        user = req.bot.users.update_if(req.user_id, states=S.READY_STATES, auto_approve=value)
        if user is None:
            expire_button(req, "This menu has expired — send /settings.")
            return
        req.user = user
        req.answer(f"Auto-approve {'enabled' if value else 'disabled'}.")
        text, markup = render.settings_menu(user)
        req.edit(text, markup, fallback=False)
    elif action == "crypto":
        if busy(req):
            return
        req.answer("Opening crypto setup…")
        req.edit("⚙ Settings closed — opening crypto setup.", fallback=False)
        crypto.begin(req)
    elif action == "setup":
        if busy(req):
            return
        req.answer("Restarting setup…")
        req.edit("⚙ Settings closed — restarting YNAB setup.", fallback=False)
        onboarding.begin(req)
    else:
        expire_button(req, "This menu has expired — send /settings.")
