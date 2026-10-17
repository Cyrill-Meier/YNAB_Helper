"""/reconcile: preview → confirm button → apply; single use, stale nonces (F04)."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.bot.conftest import ACCT, BUDGET, USER, deliver, press, ready_user, say
from tests.fakes.fake_telegram import FakeTelegram
from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.bot.app import Bot
from ynab_helper.bot.users import UsersRepo

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
BASIC = (FIXTURES / "export_basic.csv").read_bytes()


def recon_rows(ynab: FakeYnab) -> list[dict]:
    return [
        t
        for t in ynab.transactions(BUDGET, account_id=ACCT)
        if t["payee_name"] == "CSV Reconciliation"
    ]


@pytest.fixture
def imported(bot: Bot, tg: FakeTelegram, users: UsersRepo) -> None:
    ready_user(users)
    tg.document(USER, "statement.csv", BASIC)
    deliver(bot, tg)
    assert tg.texts(USER)[1].startswith("✅ Import complete")


def preview_buttons(tg: FakeTelegram) -> list[str]:
    data = tg.last(USER).button_data()
    assert len(data) == 2 and data[0].endswith(":ok") and data[1].endswith(":no")
    return data


@pytest.mark.usefixtures("imported")
def test_F04_preview_then_confirm_posts_once(bot: Bot, tg: FakeTelegram, ynab: FakeYnab):
    say(bot, tg, USER, "/reconcile")
    preview = tg.last(USER).plain
    assert "Statement balance on 2026-10-14: 6,810.60 CHF" in preview
    assert "Difference: +2,500.00 CHF" in preview
    assert recon_rows(ynab) == []  # nothing is posted by the preview
    ok, _ = preview_buttons(tg)
    message_id = tg.last(USER).message_id
    press(bot, tg, USER, ok, message_id=message_id)
    [row] = recon_rows(ynab)
    assert row["amount"] == 2_500_000 and row["cleared"] == "reconciled"
    assert "Posted a reconciliation adjustment of +2,500.00 CHF" in tg.texts(USER)[-1]
    assert tg.message_state(USER, message_id).reply_markup == {"inline_keyboard": []}

    press(bot, tg, USER, ok, message_id=message_id)  # the same button again
    assert len(recon_rows(ynab)) == 1
    assert tg.answers[-1]["text"].startswith("This preview has expired")


@pytest.mark.usefixtures("imported")
def test_cancel_posts_nothing(bot: Bot, tg: FakeTelegram, ynab: FakeYnab):
    say(bot, tg, USER, "/reconcile")
    _, no = preview_buttons(tg)
    press(bot, tg, USER, no)
    assert recon_rows(ynab) == []
    assert tg.edits(USER)[-1]["plain"].startswith("✖ Reconcile cancelled")


@pytest.mark.usefixtures("imported")
def test_an_older_preview_expires_when_a_new_one_is_made(
    bot: Bot, tg: FakeTelegram, ynab: FakeYnab
):
    say(bot, tg, USER, "/reconcile")
    old_ok, _ = preview_buttons(tg)
    old_message = tg.last(USER).message_id
    say(bot, tg, USER, "/reconcile")
    press(bot, tg, USER, old_ok, message_id=old_message)
    assert recon_rows(ynab) == []
    assert tg.answers[-1]["text"].startswith("This preview has expired")
    assert tg.message_state(USER, old_message).reply_markup == {"inline_keyboard": []}


@pytest.mark.usefixtures("imported")
def test_changed_numbers_are_refused_at_apply(bot: Bot, tg: FakeTelegram, ynab: FakeYnab):
    say(bot, tg, USER, "/reconcile")
    ok, _ = preview_buttons(tg)
    ynab.add_transaction(
        BUDGET, ACCT, amount=-1_000, date="2026-10-02", payee_name="Manual", cleared="cleared"
    )
    press(bot, tg, USER, ok)
    assert recon_rows(ynab) == []
    assert "/reconcile again" in tg.texts(USER)[-1]


@pytest.mark.parametrize("data", ["rc:forged00:ok", "rc::ok", "rc:x:maybe", "rc:x:ok:extra"])
@pytest.mark.usefixtures("imported")
def test_forged_reconcile_buttons_do_nothing(bot: Bot, tg: FakeTelegram, ynab: FakeYnab, data: str):
    say(bot, tg, USER, "/reconcile")
    press(bot, tg, USER, data)
    assert recon_rows(ynab) == []


def test_reconcile_without_a_statement_explains(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    say(bot, tg, USER, "/reconcile")
    assert tg.texts(USER)[-1].startswith("❌")
    assert tg.last(USER).buttons == []
