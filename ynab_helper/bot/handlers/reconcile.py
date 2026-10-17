"""/reconcile: preview, then confirm with a button (DESIGN §6.6, §8.3; F04, F16).

The preview (:func:`ynab_helper.services.reconcile_preview`) shows the statement, the
balances as of the statement date, the difference and any problems. If there is something
to post, the message carries ``rc:<nonce>:ok`` / ``rc:<nonce>:no`` buttons. Only the
user's latest preview is live (a new /reconcile replaces it), each nonce works once, and
it expires with the service token. Applying recomputes everything
(:func:`~ynab_helper.services.reconcile_apply`) and refuses if anything changed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Final

from ynab_helper import services
from ynab_helper.balance.reconcile import RECONCILE_TOKEN_TTL
from ynab_helper.bot import render
from ynab_helper.bot.context import Request
from ynab_helper.bot.handlers._common import busy, callback_parts, expire_button, new_nonce
from ynab_helper.bot.render import h
from ynab_helper.bot.telegram import button, inline_keyboard
from ynab_helper.errors import StaleStateError
from ynab_helper.services import UserContext

log = logging.getLogger(__name__)

MEMORY_KIND: Final = "reconcile"
EXPIRED: Final = "This preview has expired — run /reconcile again."


@dataclass(frozen=True, slots=True)
class PendingReconcile:
    nonce: str
    token: str
    expires_at: float  # bot clock


def cmd_reconcile(req: Request) -> None:
    assert req.user is not None
    ctx = req.bot.user_context(req.user)
    req.bot.start_job(
        req,
        "reconcile",
        lambda: _preview(req, ctx),
        working="🧮 Comparing YNAB with your newest statement…",
    )


def _preview(req: Request, ctx: UserContext) -> None:
    """Worker: compute the preview and offer the confirmation buttons."""
    preview = services.reconcile_preview(ctx)
    text = render.reconcile_preview(preview)
    if preview.token is None or preview.in_sync:
        req.bot.memory.pop(MEMORY_KIND, req.user_id)
        req.reply(text)
        return
    nonce = new_nonce()
    ttl = preview.token_ttl_seconds or RECONCILE_TOKEN_TTL
    req.bot.memory.put(
        MEMORY_KIND,
        req.user_id,
        PendingReconcile(nonce, preview.token, req.bot.clock() + ttl),
    )
    keyboard = inline_keyboard(
        [[button("✅ Post adjustment", f"rc:{nonce}:ok"), button("✖ Cancel", f"rc:{nonce}:no")]]
    )
    req.reply(text, keyboard)


def cb_reconcile(req: Request) -> None:
    parts = callback_parts(req, 3)
    pending = req.bot.memory.get(MEMORY_KIND, req.user_id)
    if (
        parts is None
        or parts[2] not in ("ok", "no")
        or not isinstance(pending, PendingReconcile)
        or pending.nonce != parts[1]
        or req.bot.clock() > pending.expires_at
    ):
        expire_button(req, EXPIRED)
        return
    if parts[2] == "no":
        req.bot.memory.pop(MEMORY_KIND, req.user_id, if_is=pending)
        req.answer("Cancelled.")
        req.edit("✖ Reconcile cancelled — nothing was posted.")
        return
    if busy(req):
        return  # the preview stays valid; the user can press again
    if req.bot.memory.pop(MEMORY_KIND, req.user_id, if_is=pending) is None:
        expire_button(req, EXPIRED)
        return
    assert req.user is not None
    ctx = req.bot.user_context(req.user)
    req.remove_keyboard()
    req.bot.start_job(req, "reconcile", lambda: _apply(req, ctx, pending.token), working="Posting…")


def _apply(req: Request, ctx: UserContext, token: str) -> None:
    """Worker: post the confirmed adjustment."""
    try:
        result = services.reconcile_apply(ctx, token)
    except StaleStateError as exc:
        req.reply(f"⚠ {h(exc.user_message)} Run /reconcile again to see the current numbers.")
        return
    req.reply(render.reconcile_result(result))
