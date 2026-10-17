"""/crypto, /crypto_status and the /crypto_setup flow (DESIGN §8.3; F29, F55, C05).

``/crypto_setup`` offers the budget's open tracking accounts (otherAsset/otherLiability) as
an inline picker (``cs:<nonce>:a:<i>``), then asks for a BTC xpub/ypub/zpub or address
(validated with :mod:`ynab_helper.balance.crypto.btc`: checksum, version, curve) and an ETH
address (EIP-55 aware check). Each step offers *Keep current* (when set), *Skip* and
*Cancel* buttons; typing ``keep``/``skip``/``cancel`` or sending ``/cancel`` works too.
Nothing is saved until the last step, and then in one write.

The flow never traps the user (F55): commands and documents get "finish or /cancel" from
the gate instead of being swallowed, and a crypto setup left alone for
:data:`CRYPTO_FLOW_TTL` seconds simply ends (the next message is handled normally).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Final

from ynab_helper import services
from ynab_helper.balance.crypto import btc, eth
from ynab_helper.bot import render
from ynab_helper.bot import states as S
from ynab_helper.bot.context import Request
from ynab_helper.bot.handlers._common import (
    MAX_BUTTONS,
    ascii_int,
    busy,
    callback_parts,
    expire_button,
    match_choice,
    new_flow,
)
from ynab_helper.bot.render import bold, clip, code, h, mask, money
from ynab_helper.bot.telegram import button, inline_keyboard
from ynab_helper.bot.users import flow_of
from ynab_helper.storage.users import User

log = logging.getLogger(__name__)

FLOW_KIND: Final = "crypto"
#: An unfinished crypto setup ends after this many seconds.
CRYPTO_FLOW_TTL: Final = 1800.0
TRACKING_TYPES: Final = frozenset({"otherAsset", "otherLiability"})

_BTC_PROMPT: Final = (
    "Now send your <b>BTC xpub</b> (ypub/zpub work too) — or a single BTC address.\n"
    "• xpub/ypub/zpub: tracks the whole HD wallet\n"
    "• a single address: tracks just that address"
)
_ETH_PROMPT: Final = "Now send your <b>ETH address</b> (0x…)."


# ── /crypto, /crypto_status ──────────────────────────────────────────────────


def cmd_crypto(req: Request) -> None:
    user = req.user
    assert user is not None
    if not user.crypto_account_id:
        req.reply(
            "🪙 Crypto tracking isn't configured yet.\n"
            "Run /crypto_setup to pick a tracking account and add your BTC xpub or ETH address."
        )
        return
    if not user.btc_xpub and not user.eth_address:
        req.reply("No BTC xpub or ETH address on file. Run /crypto_setup to add one.")
        return
    ctx = req.bot.user_context(user)
    name = user.crypto_account_name

    def job() -> None:
        result = services.crypto_sync(ctx)
        req.reply(render.crypto_result(result, account_name=name))

    req.bot.start_job(
        req, "crypto", job, working="🪙 Syncing the crypto portfolio… (30–60 s for an xpub)"
    )


def cmd_crypto_status(req: Request) -> None:
    assert req.user is not None
    req.reply(render.crypto_status(req.user))


# ── /crypto_setup ────────────────────────────────────────────────────────────


def cmd_crypto_setup(req: Request) -> None:
    begin(req)


def begin(req: Request) -> None:
    """Start (or restart) the crypto setup: list tracking accounts on a worker."""
    user = req.user
    assert user is not None
    if busy(req):
        return
    ctx = req.bot.user_context(user)

    def job() -> None:
        accounts = services.list_accounts(ctx.ynab_token, ctx.budget_id)
        tracking = [a for a in accounts if a.type in TRACKING_TYPES]
        if not tracking:
            req.reply(
                "No open tracking accounts found in your YNAB budget.\n\n"
                "Create one in YNAB first: Add Account → <b>Tracking</b> → Asset, name it e.g. "
                "'Crypto'. Then run /crypto_setup again."
            )
            return
        choices = [
            {"id": a.id, "name": a.name, "balance": a.balance} for a in tracking[:MAX_BUTTONS]
        ]
        flow = new_flow(FLOW_KIND, accounts=choices)
        updated = req.bot.users.update_if(
            req.user_id,
            states=S.READY_STATES | S.CRYPTO_STATES,
            state=S.AWAITING_CRYPTO_ACCOUNT,
            temp_data=flow,
        )
        if updated is None:
            return
        nonce = flow["flow"]
        lines = ["🪙 <b>Crypto setup</b> — pick your tracking account:", ""]
        lines += [
            f"  {i}. {h(a['name'])} — {money(a['balance'])}" for i, a in enumerate(choices, 1)
        ]
        lines += ["", "Tap a button — or reply with the number (/cancel to stop)."]
        rows = [[button(clip(a["name"], 60), f"cs:{nonce}:a:{i}")] for i, a in enumerate(choices)]
        rows.append([button("✖ Cancel", f"cs:{nonce}:x")])
        req.reply("\n".join(lines), inline_keyboard(rows))

    req.bot.start_job(req, "crypto_setup", job, working="🪙 Loading your tracking accounts…")


def cancel(req: Request) -> None:
    updated = req.bot.users.update_if(
        req.user_id, states=S.CRYPTO_STATES, state=S.READY, temp_data=None
    )
    if updated is None:
        req.reply("Nothing to cancel.")
        return
    req.user = updated
    text = "Crypto setup cancelled — nothing was changed."
    if req.incoming.kind == "callback":
        req.answer("Cancelled.")
        req.edit(text)
    else:
        req.reply(text)


def expire_stale_flow(bot: Any, user: User) -> User:
    """End a crypto setup that was left alone (or is a v1 leftover) — F55.

    Called by the gate for every update of a user in a crypto state; returns the (possibly
    updated) user. The user's main configuration is untouched, so they are ``ready`` again.
    """
    if user.state not in S.CRYPTO_STATES:
        return user
    flow = flow_of(user)
    started = flow.get("started") if flow is not None else None
    fresh = (
        flow is not None
        and flow.get("kind") == FLOW_KIND
        and isinstance(started, int | float)
        and 0 <= time.time() - started <= CRYPTO_FLOW_TTL
    )
    if fresh or not (user.ynab_token and user.budget_id and user.account_id):
        return user
    expected = flow.get("flow") if flow is not None else None
    updated = bot.users.update_if(
        user.telegram_id,
        states={user.state},
        flow=expected,
        state=S.READY,
        temp_data=None,
    )
    if updated is None:
        return bot.users.get(user.telegram_id) or user
    log.info("user %s: unfinished crypto setup ended", user.telegram_id)
    bot.send(
        user.telegram_id,  # the private chat, never a stored (v1: maybe group) chat_id
        "ℹ Your unfinished crypto setup was closed — run /crypto_setup to start again.",
    )
    return updated


def on_text(req: Request) -> None:
    user = req.user
    assert user is not None
    flow = flow_of(user)
    if flow is None or flow.get("kind") != FLOW_KIND:
        cancel(req)
        return
    word = req.text.lower()
    if word in ("cancel", "/cancel"):
        cancel(req)
        return
    if user.state == S.AWAITING_CRYPTO_ACCOUNT:
        index, problem = match_choice(req.text, flow.get("accounts") or [])
        if index is None:
            hint = (
                "Several accounts have that name — please reply with the number."
                if problem == "ambiguous"
                else "Please tap your tracking account — or reply with its number."
            )
            req.reply(hint + " (/cancel to stop)")
            return
        _choose_account(req, flow, index)
    elif user.state == S.AWAITING_CRYPTO_BTC:
        if word in ("skip", "keep"):
            _set_btc(req, flow, word)
        else:
            _set_btc(req, flow, "value", req.text)
    elif user.state == S.AWAITING_CRYPTO_ETH:
        if word in ("skip", "keep"):
            _set_eth(req, flow, word)
        else:
            _set_eth(req, flow, "value", req.text)


def _step_keyboard(nonce: str, *, has_current: bool, skip_label: str) -> dict[str, Any]:
    row = []
    if has_current:
        row.append(button("Keep current", f"cs:{nonce}:keep"))
    row.append(button(skip_label, f"cs:{nonce}:skip"))
    return inline_keyboard([row, [button("✖ Cancel", f"cs:{nonce}:x")]])


def _choose_account(req: Request, flow: dict[str, Any], index: int) -> None:
    accounts = flow.get("accounts") or []
    if not 0 <= index < len(accounts):
        expire_button(req, "This crypto setup has expired.")
        return
    account = accounts[index]
    nonce = str(flow["flow"])
    updated = req.bot.users.update_if(
        req.user_id,
        states={S.AWAITING_CRYPTO_ACCOUNT},
        flow=nonce,
        state=S.AWAITING_CRYPTO_BTC,
        temp_data={**flow, "account": account},
    )
    if updated is None:
        expire_button(req, "This crypto setup has expired.")
        return
    req.user = updated
    if req.incoming.kind == "callback":
        req.answer()
        req.edit(f"Tracking account: {bold(account['name'])}", fallback=False)
    current = bool(updated.btc_xpub)
    text = f"✅ Tracking account: {bold(account['name'])}\n\n{_BTC_PROMPT}"
    if current:
        text += f"\n\nCurrent: {code(mask(updated.btc_xpub))}"
    req.reply(text, _step_keyboard(nonce, has_current=current, skip_label="Skip BTC"))


def _set_btc(req: Request, flow: dict[str, Any], action: str, value: str | None = None) -> None:
    user = req.user
    assert user is not None
    nonce = str(flow["flow"])
    if action == "keep":
        btc_value = user.btc_xpub
        if not btc_value:
            req.reply("There is no BTC wallet saved yet — send one, or tap Skip.")
            return
    elif action == "skip":
        btc_value = None
    else:
        text = (value or "").strip()
        try:
            kind, parsed = btc.validate_btc_input(text)
        except btc.InvalidBitcoinInput as exc:
            req.reply(f"❌ {h(exc)}\nSend another one, or type skip / cancel.")
            return
        btc_value = text if kind == "xpub" else str(parsed)
    updated = req.bot.users.update_if(
        req.user_id,
        states={S.AWAITING_CRYPTO_BTC},
        flow=nonce,
        state=S.AWAITING_CRYPTO_ETH,
        temp_data={**flow, "btc": btc_value},
    )
    if updated is None:
        expire_button(req, "This crypto setup has expired.")
        return
    req.user = updated
    if req.incoming.kind == "callback":
        req.answer()
        req.remove_keyboard()
    saved = f"✅ BTC: {code(mask(btc_value))}" if btc_value else "⏭ No BTC."
    current = bool(updated.eth_address)
    text = f"{saved}\n\n{_ETH_PROMPT}"
    if current:
        text += f"\n\nCurrent: {code(mask(updated.eth_address))}"
    req.reply(text, _step_keyboard(nonce, has_current=current, skip_label="Skip ETH"))


def _set_eth(req: Request, flow: dict[str, Any], action: str, value: str | None = None) -> None:
    user = req.user
    assert user is not None
    nonce = str(flow["flow"])
    if action == "keep":
        eth_value = user.eth_address
        if not eth_value:
            req.reply("There is no ETH address saved yet — send one, or tap Skip.")
            return
    elif action == "skip":
        eth_value = None
    else:
        text = (value or "").strip()
        if not eth.is_valid_eth_address(text):
            req.reply(
                "❌ That doesn't look like an Ethereum address (0x followed by 40 hex "
                "characters; check the capitalisation if you typed it).\n"
                "Send another one, or type skip / cancel."
            )
            return
        eth_value = text
    account = flow.get("account")
    if not isinstance(account, dict):
        cancel(req)
        return
    updated = req.bot.users.update_if(
        req.user_id,
        states={S.AWAITING_CRYPTO_ETH},
        flow=nonce,
        crypto_account_id=account["id"],
        crypto_account_name=account["name"],
        btc_xpub=flow.get("btc"),
        eth_address=eth_value,
        state=S.READY,
        temp_data=None,
    )
    if updated is None:
        expire_button(req, "This crypto setup has expired.")
        return
    req.user = updated
    if req.incoming.kind == "callback":
        req.answer("Saved.")
        req.remove_keyboard()
    log.info(
        "user %s finished the crypto setup (btc=%s eth=%s)",
        req.user_id,
        bool(updated.btc_xpub),
        bool(updated.eth_address),
    )
    lines = [
        "🎉 <b>Crypto setup complete!</b>",
        f"Tracking account: {h(updated.crypto_account_name)}",
    ]
    if updated.btc_xpub:
        lines.append(f"BTC: {code(mask(updated.btc_xpub))}")
    if updated.eth_address:
        lines.append(f"ETH: {code(mask(updated.eth_address))}")
    if not updated.btc_xpub and not updated.eth_address:
        lines += [
            "",
            "⚠ No BTC or ETH configured — /crypto has nothing to sync.",
            "Run /crypto_setup again to add one.",
        ]
    else:
        lines += ["", "Run /crypto anytime to sync your portfolio value to YNAB."]
    req.reply("\n".join(lines))


def cb_crypto_setup(req: Request) -> None:
    """``cs:<nonce>:a:<i>``, ``cs:<nonce>:keep``, ``cs:<nonce>:skip``, ``cs:<nonce>:x``."""
    expired = "This crypto setup has expired."
    parts = callback_parts(req, (3, 4))
    user = req.user
    flow = flow_of(user)
    if parts is None or user is None or flow is None or flow.get("flow") != parts[1]:
        expire_button(req, expired)
        return
    action = parts[2]
    if action == "x" and len(parts) == 3:
        cancel(req)
    elif action == "a" and len(parts) == 4 and user.state == S.AWAITING_CRYPTO_ACCOUNT:
        index = ascii_int(parts[3], 4)
        if index is None:
            expire_button(req, expired)
            return
        _choose_account(req, flow, index)
    elif action in ("keep", "skip") and len(parts) == 3 and user.state == S.AWAITING_CRYPTO_BTC:
        _set_btc(req, flow, action)
    elif action in ("keep", "skip") and len(parts) == 3 and user.state == S.AWAITING_CRYPTO_ETH:
        _set_eth(req, flow, action)
    else:
        expire_button(req, expired)
