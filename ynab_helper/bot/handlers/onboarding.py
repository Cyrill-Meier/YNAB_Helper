"""Onboarding: YNAB token → budget → account (DESIGN §8.3; F18, F52, F53, F54, F57).

* ``/approve`` (or ``/setup``) puts the user in ``awaiting_token``. Whatever they send then
  is deleted from the chat right away (it is, or may be, their Personal Access Token), and
  validated on a worker with ``GET /user`` and ``GET /budgets``: a 401 asks again, a
  network problem says so — the user is never stuck (F52).
* Budget and account pickers are inline keyboards bound to the flow's nonce
  (``ob:<nonce>:b:<i>`` / ``ob:<nonce>:a:<i>``, ``ob:<nonce>:x`` cancels); typing the
  number or the exact name still works, duplicate names ask for the number. Only open,
  non-deleted accounts are offered (F53).
* The new token and choices live in ``temp_data`` until the account is picked; then token,
  budget and account are committed in one write. Until then the previous configuration
  stays in the user's row, and ``/cancel`` returns to it (F54).
* Switching the budget clears the crypto tracking account (it belongs to the old budget,
  C05). Any change of token, budget or account signs the user out of the web UI and drops
  their in-memory flows (F18).

Writes from workers are conditional on the user still being in the same flow
(:meth:`~ynab_helper.bot.users.UsersRepo.update_if`), so a ``/deny`` or ``/cancel`` that
happens while YNAB is being asked always wins.
"""

from __future__ import annotations

import logging
from typing import Any, Final

from ynab_helper import services
from ynab_helper.bot import states as S
from ynab_helper.bot.context import Request
from ynab_helper.bot.handlers._common import (
    MAX_BUTTONS,
    MAX_CHOICES,
    ascii_int,
    busy,
    callback_parts,
    expire_button,
    match_choice,
    new_flow,
)
from ynab_helper.bot.render import bold, clip, h, money
from ynab_helper.bot.telegram import button, inline_keyboard
from ynab_helper.bot.users import flow_of
from ynab_helper.errors import ConfigError, YnabAuthError, YnabError, YnabRateLimited
from ynab_helper.storage.users import User

log = logging.getLogger(__name__)

FLOW_KIND: Final = "setup"
TOKEN_PROMPT: Final = (
    "Please send me your YNAB Personal Access Token.\n"
    "(Get it from app.ynab.com → Account Settings → Developer Settings)"
)
_REMOVE_REPLY_KEYBOARD: Final = {"remove_keyboard": True}
_MIN_TOKEN: Final = 16
_MAX_TOKEN: Final = 256


def has_previous_config(user: User | None) -> bool:
    return bool(user and user.ynab_token and user.budget_id and user.account_id)


# ── start / cancel ───────────────────────────────────────────────────────────


def begin(req: Request, *, intro: str = "Let's set up your account again.") -> None:
    """Start (or restart) the setup: ``awaiting_token``, previous config kept for /cancel."""
    user = req.bot.users.update_if(
        req.user_id, states=S.READY_STATES | S.SETUP_STATES, state=S.AWAITING_TOKEN, temp_data=None
    )
    if user is None:
        req.reply("Please finish what you are doing first (or /cancel).")
        return
    req.user = user
    keep = (
        "\n\nYour current settings stay saved until the new setup is complete — send /cancel "
        "to keep them."
        if has_previous_config(user)
        else ""
    )
    req.reply(f"{h(intro)}\n\n{h(TOKEN_PROMPT)}{keep}")


def cmd_setup(req: Request) -> None:
    if busy(req):
        return
    begin(req)


def cancel(req: Request) -> None:
    """Leave the setup: back to the previous ready configuration, else to the token step."""
    user = req.user
    if user is None:
        return
    if has_previous_config(user):
        updated = req.bot.users.update_if(
            req.user_id, states=S.SETUP_STATES, state=S.READY, temp_data=None
        )
        text = "Setup cancelled — your previous settings are still active."
    else:
        updated = req.bot.users.update_if(
            req.user_id, states=S.SETUP_STATES, state=S.AWAITING_TOKEN, temp_data=None
        )
        text = "Setup cancelled. Send your YNAB Personal Access Token whenever you're ready."
    if updated is None:
        req.reply("Nothing to cancel.")
        return
    req.user = updated
    if req.incoming.kind == "callback":
        req.answer("Cancelled.")
        req.edit(text, fallback=True)
    else:
        req.reply(text, _REMOVE_REPLY_KEYBOARD)


# ── text replies ─────────────────────────────────────────────────────────────


def on_text(req: Request) -> None:
    user = req.user
    assert user is not None
    if user.state == S.AWAITING_TOKEN:
        _on_token(req)
        return
    flow = flow_of(user)
    if flow is None or flow.get("kind") != FLOW_KIND:
        # A v1 leftover (bare list in temp_data) or a broken flow: start again cleanly.
        restarted = req.bot.users.update_if(
            req.user_id, states={user.state}, state=S.AWAITING_TOKEN, temp_data=None
        )
        if restarted is not None:
            req.reply(
                "Your setup was interrupted by an update. " + h(TOKEN_PROMPT),
                _REMOVE_REPLY_KEYBOARD,
            )
        return
    key = "budgets" if user.state == S.AWAITING_BUDGET else "accounts"
    items = flow.get(key) or []
    index, problem = match_choice(req.text, items)
    if index is None:
        what = "budget" if key == "budgets" else "account"
        if problem == "ambiguous":
            hint = f"Several {what}s have that name — please reply with the number."
        else:
            hint = f"Please tap your {what} — or reply with its number."
        req.reply(hint)
        return
    if user.state == S.AWAITING_BUDGET:
        _choose_budget(req, flow, index)
    else:
        _choose_account(req, flow, index)


def _looks_like_token(text: str) -> bool:
    return (
        _MIN_TOKEN <= len(text) <= _MAX_TOKEN
        and text.isascii()
        and text.isprintable()
        and not any(c.isspace() for c in text)
    )


def _on_token(req: Request) -> None:
    text = req.text
    deleted = True
    if req.message_id is not None:
        # Whatever arrives here may be the token: never leave it in the chat (F57).
        deleted = req.bot.delete_message(req.chat_id, req.message_id)
    warning = "" if deleted else "\n⚠ I could not delete your message — please delete it yourself."
    if not text:
        req.reply(h(TOKEN_PROMPT))
        return
    if not _looks_like_token(text):
        req.reply(
            "That doesn't look like a YNAB Personal Access Token. " + h(TOKEN_PROMPT) + warning
        )
        return
    if busy(req):
        return
    flow = new_flow(FLOW_KIND)
    marked = req.bot.users.update_if(req.user_id, states={S.AWAITING_TOKEN}, temp_data=flow)
    if marked is None:
        return
    req.bot.start_job(
        req,
        "setup",
        lambda: _validate_token(req, text, flow["flow"]),
        working="🔑 Validating token…" + warning,
    )


def _validate_token(req: Request, token: str, flow_id: str) -> None:
    """Worker: check the token with YNAB and show the budget picker."""
    try:
        client = services.token_client_factory(token)
        client.get_user()
        budgets = services.list_budgets(token, client=client)
    except ConfigError as exc:  # e.g. characters a token cannot contain
        req.reply(f"❌ {h(exc.user_message)}")
        return
    except YnabAuthError:
        req.reply("❌ YNAB rejected this token. Please check it and send it again.")
        return
    except YnabRateLimited:
        req.reply("⏳ YNAB's rate limit was reached. Please send the token again in a while.")
        return
    except YnabError as exc:
        req.reply(
            f"❌ Could not reach YNAB ({h(exc.user_message)}). "
            "Please send the token again in a moment."
        )
        return
    if not budgets:
        req.reply(
            "Token is valid, but your YNAB account has no budgets. Create one and send the token again."
        )
        return
    choices = [{"id": b.id, "name": b.name, "currency": b.currency} for b in budgets[:MAX_CHOICES]]
    flow = {**new_flow(FLOW_KIND), "flow": flow_id, "token": token, "budgets": choices}
    user = req.bot.users.update_if(
        req.user_id,
        states={S.AWAITING_TOKEN},
        flow=flow_id,
        state=S.AWAITING_BUDGET,
        temp_data=flow,
    )
    if user is None:
        log.info(
            "setup of user %s changed while the token was checked; result dropped", req.user_id
        )
        return
    lines = ["✅ Token valid! Select your budget:", ""]
    lines += [
        f"  {i}. {h(b['name'])}" + (f" ({h(b['currency'])})" if b.get("currency") else "")
        for i, b in enumerate(choices, 1)
    ]
    lines += ["", "Tap a button below — or reply with the number."]
    rows = [
        [button(clip(b["name"], 60), f"ob:{flow_id}:b:{i}")]
        for i, b in enumerate(choices[:MAX_BUTTONS])
    ]
    rows.append([button("✖ Cancel", f"ob:{flow_id}:x")])
    req.reply("\n".join(lines), inline_keyboard(rows))


# ── budget and account ───────────────────────────────────────────────────────


def _choose_budget(req: Request, flow: dict[str, Any], index: int) -> None:
    budgets = flow.get("budgets") or []
    if not 0 <= index < len(budgets) or not flow.get("token"):
        req.reply("Please send /setup to start again.")
        return
    if busy(req):
        return
    budget = budgets[index]
    req.bot.start_job(
        req,
        "setup",
        lambda: _load_accounts(req, flow, budget),
        working=f"📋 Loading the accounts of {bold(clip(budget['name'], 60))}…",
    )


def _load_accounts(req: Request, flow: dict[str, Any], budget: dict[str, Any]) -> None:
    """Worker: fetch the budget's open accounts and show the account picker."""
    token = str(flow["token"])
    flow_id = str(flow["flow"])
    try:
        accounts = services.list_accounts(
            token, budget["id"], client=services.token_client_factory(token)
        )
    except YnabAuthError:
        restarted = req.bot.users.update_if(
            req.user_id,
            states={S.AWAITING_BUDGET},
            flow=flow_id,
            state=S.AWAITING_TOKEN,
            temp_data=None,
        )
        if restarted is not None:
            req.reply("❌ YNAB no longer accepts that token. " + h(TOKEN_PROMPT))
        return
    except (YnabError, ConfigError) as exc:
        req.reply(
            f"❌ Could not load the accounts ({h(exc.user_message)}). Please pick the budget again."
        )
        return
    if not accounts:
        req.reply(
            f"The budget {bold(budget['name'])} has no open accounts. Please pick another budget."
        )
        return
    currency = budget.get("currency")
    choices = [
        {
            "id": a.id,
            "name": a.name,
            "type": a.type or "",
            "balance": a.balance,
            "on_budget": a.on_budget,
        }
        for a in accounts[:MAX_CHOICES]
    ]
    updated_flow = {**flow, "budget": budget, "accounts": choices}
    user = req.bot.users.update_if(
        req.user_id,
        states={S.AWAITING_BUDGET},
        flow=flow_id,
        state=S.AWAITING_ACCOUNT,
        temp_data=updated_flow,
    )
    if user is None:
        log.info("setup of user %s changed while accounts were loaded; result dropped", req.user_id)
        return
    if req.incoming.kind == "callback":
        # The budget picker has done its job; a failure above keeps it for another tap.
        req.edit(f"Budget: {bold(budget['name'])}", fallback=False)
    lines = [f"Budget: {bold(budget['name'])}", "", "Select your Revolut account:", ""]
    lines += [
        f"  {i}. {h(a['name'])} ({h(a['type'] or '?')}) — {money(a['balance'], currency)}"
        for i, a in enumerate(choices, 1)
    ]
    lines += ["", "Tap an account below — or reply with the number."]
    rows = [
        [button(clip(a["name"], 60), f"ob:{flow_id}:a:{i}")]
        for i, a in enumerate(choices[:MAX_BUTTONS])
    ]
    rows.append([button("✖ Cancel", f"ob:{flow_id}:x")])
    req.reply("\n".join(lines), inline_keyboard(rows))


def _choose_account(req: Request, flow: dict[str, Any], index: int) -> None:
    accounts = flow.get("accounts") or []
    budget = flow.get("budget")
    token = flow.get("token")
    if not 0 <= index < len(accounts) or not isinstance(budget, dict) or not token:
        req.reply("Please send /setup to start again.")
        return
    old = req.user
    assert old is not None
    account = accounts[index]
    fields: dict[str, Any] = {
        "ynab_token": token,
        "budget_id": budget["id"],
        "budget_name": budget["name"],
        "account_id": account["id"],
        "account_name": account["name"],
        "state": S.READY,
        "temp_data": None,
    }
    budget_changed = old.budget_id != budget["id"]
    crypto_reset = budget_changed and bool(old.crypto_account_id)
    if crypto_reset:
        fields.update(crypto_account_id=None, crypto_account_name=None)
    user = req.bot.users.update_if(
        req.user_id, states={S.AWAITING_ACCOUNT}, flow=str(flow["flow"]), **fields
    )
    if user is None:
        expire_button(req, "This setup step has expired.")
        return
    req.user = user
    changed = (old.ynab_token, old.budget_id, old.account_id) != (
        token,
        budget["id"],
        account["id"],
    )
    revoked = 0
    if changed:
        revoked = req.bot.users.revoke_sessions(req.user_id)
        req.bot.forget_user(req.user_id)
    log.info("user %s finished the setup (config changed: %s)", req.user_id, changed)
    if req.incoming.kind == "callback":
        req.answer("Saved.")
        req.edit(f"Account: {bold(account['name'])}", fallback=False)
    currency = budget.get("currency")
    lines = [
        "🎉 All set!",
        "",
        f"  Budget:  {h(budget['name'])}",
        f"  Account: {h(account['name'])} ({h(account.get('type') or '?')}, "
        f"{money(account.get('balance'), currency)})",
        "",
        "You can now send me Revolut CSV files to import.",
        "Type /help to see all commands.",
    ]
    if crypto_reset:
        lines += [
            "",
            "🪙 Your crypto tracking account belonged to the old budget — run /crypto_setup again.",
        ]
    if revoked:
        lines += ["", "🚪 You were signed out of the web dashboard; use /login to sign in again."]
    req.reply("\n".join(lines), _REMOVE_REPLY_KEYBOARD)


# ── buttons ──────────────────────────────────────────────────────────────────


def cb_onboarding(req: Request) -> None:
    """``ob:<nonce>:b:<i>`` (budget), ``ob:<nonce>:a:<i>`` (account), ``ob:<nonce>:x``."""
    expired = "This setup step has expired."
    parts = callback_parts(req, (3, 4))
    user = req.user
    flow = flow_of(user)
    if parts is None or user is None or flow is None or flow.get("flow") != parts[1]:
        expire_button(req, expired)
        return
    if parts[2] == "x" and len(parts) == 3:
        cancel(req)
        return
    index = ascii_int(parts[3], 4) if len(parts) == 4 else None
    if index is None:
        expire_button(req, expired)
        return
    if parts[2] == "b" and user.state == S.AWAITING_BUDGET:
        _choose_budget(req, flow, index)
    elif parts[2] == "a" and user.state == S.AWAITING_ACCOUNT:
        _choose_account(req, flow, index)
    else:
        expire_button(req, expired)


# Used by the admin's /approve.
def approval_text() -> str:
    return "🎉 You've been approved!\n\nLet's get you set up. " + h(TOKEN_PROMPT)


__all__ = [
    "approval_text",
    "begin",
    "cancel",
    "cb_onboarding",
    "cmd_setup",
    "has_previous_config",
    "on_text",
]
