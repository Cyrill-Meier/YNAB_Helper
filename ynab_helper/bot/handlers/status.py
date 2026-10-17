"""/status: balances from the mirror after a throttled sync, last statement, ledger counts."""

from __future__ import annotations

from ynab_helper import services
from ynab_helper.bot import render
from ynab_helper.bot.context import Request


def cmd_status(req: Request) -> None:
    assert req.user is not None
    ctx = req.bot.user_context(req.user)

    def job() -> None:
        req.reply(render.status(services.account_status(ctx)))

    req.bot.start_job(req, "status", job, working="📊 Checking…")
