"""/cleanup_pending: strip stale "(pending)" memo markers (DESIGN §6.7; F01, C02, F47).

Only memos of cleared/reconciled transactions are touched and only the marker is removed;
the result lists what was cleaned, what failed and what the rate limit left undone.
"""

from __future__ import annotations

from ynab_helper import services
from ynab_helper.bot import render
from ynab_helper.bot.context import Request


def cmd_cleanup_pending(req: Request) -> None:
    assert req.user is not None
    ctx = req.bot.user_context(req.user)

    def job() -> None:
        result = services.cleanup_pending(ctx)
        req.reply(render.cleanup_result(result))

    req.bot.start_job(req, "cleanup", job, working="🧹 Looking for stale '(pending)' memos…")
