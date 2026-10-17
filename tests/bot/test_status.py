"""/status on a worker: balances from the mirror, last statement, ledger counts (R10)."""

from __future__ import annotations

from pathlib import Path

from tests.bot.conftest import ACCT, BUDGET, USER, deliver, ready_user, say
from tests.fakes.fake_telegram import FakeTelegram
from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.bot import render
from ynab_helper.bot.app import Bot
from ynab_helper.bot.users import UsersRepo
from ynab_helper.locks import LOCKS

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
BASIC = (FIXTURES / "export_basic.csv").read_bytes()


def test_bot_R10_status_job_reports_balances(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, ynab: FakeYnab
):
    ready_user(users)
    say(bot, tg, USER, "/status")
    first = tg.texts(USER)
    assert first[0] == "📊 Checking…"
    assert first[1].splitlines()[0] == "📊 Revolut"
    assert "No statement imported yet" in first[1]

    tg.document(USER, "statement.csv", BASIC)
    deliver(bot, tg)
    say(bot, tg, USER, "/status")
    lines = tg.texts(USER)[-1].splitlines()
    balance, cleared, uncleared = ynab.balances(BUDGET, ACCT)
    assert balance != 0
    assert lines[:4] == [
        "📊 Revolut",
        f"  Working balance: {render.money(balance, 'CHF')}",
        f"  Cleared: {render.money(cleared, 'CHF')}",
        f"  Uncleared: {render.money(uncleared, 'CHF')}",
    ]
    assert "📄 Last statement: statement.csv, 2026-10-01 → 2026-10-13" in lines
    assert any(line.startswith("📒 Ledger: ") for line in lines)
    assert lines[-1].startswith("🔄 Last sync: ") and "never" not in lines[-1]
    assert LOCKS.snapshot() == {}


def test_bot_R10_status_while_another_operation_runs(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    """A /status job that meets a user operation of the web UI is refused before it starts."""
    ready_user(users)
    with LOCKS.operation(str(USER), "import"):
        say(bot, tg, USER, "/status")
    assert tg.texts(USER) == [render.BUSY_TEXT]
