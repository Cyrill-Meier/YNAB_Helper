"""Button flows on real worker threads behind the real poll loop (:meth:`Bot.run`).

Most bot tests use the inline ``WorkerPool(0)`` for determinism. These run the same flows
the way production does — a 4-thread pool, updates fetched by ``getUpdates`` — so the
interplay of callback answers, "working…" texts and jobs is covered too. The ``tg``
fixture fails a test if any callback answer carries HTML markup (Telegram shows answers as
plain text, so ``<b>`` or ``&amp;`` would appear literally: F96).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from tests.bot.conftest import ADMIN, BUDGET, USER, YNAB_TOKEN, ready_user
from tests.fakes.fake_telegram import FakeTelegram, SentMessage
from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.bot.app import Bot
from ynab_helper.bot.telegram import TelegramClient
from ynab_helper.bot.users import UsersRepo
from ynab_helper.bot.workers import WorkerPool

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
BASIC = (FIXTURES / "export_basic.csv").read_bytes()


def wait_for(predicate: Callable[[], object], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition not reached")


@pytest.fixture
def running_bot(client: TelegramClient, users: UsersRepo, data_dir: Path) -> Iterator[Bot]:
    exits: list[int] = []
    bot = Bot(
        telegram=client,
        users=users,
        admin_id=ADMIN,
        data_dir=data_dir,
        workers=WorkerPool(4),
        hard_exit=exits.append,
    )
    thread = threading.Thread(target=bot.run, daemon=True)
    thread.start()
    yield bot
    assert bot.workers.wait_idle(10)
    bot.request_stop()
    thread.join(10)
    assert not thread.is_alive()
    assert exits == []


def message_with_buttons(tg: FakeTelegram, prefix: str) -> SentMessage | None:
    for message in reversed(tg.sent(USER)):
        if any(d.startswith(prefix) for d in message.button_data()):
            return message
    return None


def answer_texts(tg: FakeTelegram) -> list[str]:
    return [a.get("text", "") for a in tg.answers]


def test_onboarding_with_buttons_on_worker_threads(
    running_bot: Bot, tg: FakeTelegram, users: UsersRepo, ynab: FakeYnab
):
    ynab.add_budget("b2", currency="EUR", name="Tom & Jerry's")
    ynab.add_account("b2", "acct-b2", "Joint")
    tg.message(USER, "hello")
    wait_for(lambda: users.get(USER) is not None)
    tg.message(ADMIN, f"/approve {USER}")
    wait_for(lambda: users.get(USER).state == "awaiting_token")
    tg.message(USER, YNAB_TOKEN)
    wait_for(lambda: users.get(USER).state == "awaiting_budget")
    wait_for(lambda: message_with_buttons(tg, "ob:"))
    budgets = message_with_buttons(tg, "ob:")
    [data] = [b["callback_data"] for b in budgets.buttons if b["text"] == "Tom & Jerry's"]
    tg.callback(USER, data, message_id=budgets.message_id)
    wait_for(lambda: any(":a:" in d for d in (message_with_buttons(tg, "ob:").button_data())))
    # The "working…" text became the answer to the press: plain text, entities resolved.
    assert "📋 Loading the accounts of Tom & Jerry's…" in answer_texts(tg)
    accounts = message_with_buttons(tg, "ob:")
    assert "Joint" in accounts.plain
    [account] = [d for d in accounts.button_data() if d.endswith(":a:0")]
    tg.callback(USER, account, message_id=accounts.message_id)
    wait_for(lambda: users.get(USER).state == "ready")
    user = users.get(USER)
    assert (user.budget_id, user.account_id) == ("b2", "acct-b2")
    wait_for(lambda: len(tg.answers) == 2)  # every press answered exactly once
    assert YNAB_TOKEN not in tg.all_text()


def test_import_and_reconcile_on_worker_threads(
    running_bot: Bot, tg: FakeTelegram, users: UsersRepo, ynab: FakeYnab
):
    ready_user(users)
    tg.document(USER, "statement.csv", BASIC)
    wait_for(lambda: any(t.startswith("✅ Import complete") for t in tg.texts(USER)))
    assert running_bot.workers.wait_idle(10)
    tg.message(USER, "/reconcile")
    wait_for(lambda: message_with_buttons(tg, "rc:"))
    preview = message_with_buttons(tg, "rc:")
    assert "Difference: +2,500.00 CHF" in preview.plain
    ok = next(d for d in preview.button_data() if d.endswith(":ok"))
    tg.callback(USER, ok, message_id=preview.message_id)
    tg.callback(USER, ok, message_id=preview.message_id)  # an impatient second tap
    wait_for(lambda: any("Posted a reconciliation adjustment" in t for t in tg.texts(USER)))
    assert running_bot.workers.wait_idle(10)
    rows = [
        t
        for t in ynab.transactions(BUDGET)
        if t["payee_name"] == "CSV Reconciliation" and not t.get("deleted")
    ]
    assert len(rows) == 1  # posted once
    wait_for(lambda: len(tg.answers) == 2)
    assert answer_texts(tg)[0] == "Posting…"
    assert tg.message_state(USER, preview.message_id).reply_markup == {"inline_keyboard": []}
