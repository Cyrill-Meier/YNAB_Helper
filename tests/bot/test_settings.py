"""/settings menu buttons (v1 callback data ``st:*``, DESIGN §2/§8.3) — R10."""

from __future__ import annotations

import pytest

from tests.bot.conftest import USER, deliver, press, ready_user, say
from tests.fakes.fake_telegram import FakeTelegram, SentMessage
from ynab_helper.bot import render
from ynab_helper.bot.app import Bot
from ynab_helper.bot.context import Request, parse_update
from ynab_helper.bot.handlers import settings
from ynab_helper.bot.users import UsersRepo
from ynab_helper.locks import LOCKS

EXPIRED = "This menu has expired — send /settings."


def open_menu(bot: Bot, tg: FakeTelegram) -> SentMessage:
    say(bot, tg, USER, "/settings")
    menu = tg.last(USER)
    data = menu.button_data()
    assert data[0] in ("st:auto:0", "st:auto:1")
    assert data[1:] == ["st:crypto", "st:setup", "st:close"]
    return menu


def keyboard_of(tg: FakeTelegram, message: SentMessage) -> list:
    markup = tg.message_state(USER, message.message_id).reply_markup or {}
    return markup.get("inline_keyboard") or []


def test_bot_R10_settings_auto_approve_buttons(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users, auto_approve=True)
    menu = open_menu(bot, tg)
    assert menu.button_data()[0] == "st:auto:0"  # carries the target value, as in v1
    press(bot, tg, USER, "st:auto:0", message_id=menu.message_id)
    assert users.get(USER).auto_approve is False
    assert tg.answers[-1]["text"] == "Auto-approve disabled."
    state = tg.message_state(USER, menu.message_id)
    assert "Auto-approve imports: OFF" in str(state.reply_markup)
    assert state.reply_markup["inline_keyboard"][0][0]["callback_data"] == "st:auto:1"
    press(bot, tg, USER, "st:auto:1", message_id=menu.message_id)
    assert users.get(USER).auto_approve is True
    assert tg.answers[-1]["text"] == "Auto-approve enabled."
    # An old menu still showing "st:auto:1" sets ON again (a target, not a toggle).
    press(bot, tg, USER, "st:auto:1", message_id=menu.message_id)
    assert users.get(USER).auto_approve is True


def test_bot_R10_settings_auto_approve_needs_a_ready_user(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    """The state guard of the write: a user who left ``ready`` between the gate and the
    handler gets the expiry answer, and nothing changes."""
    ready_user(users, auto_approve=True)
    menu = open_menu(bot, tg)
    tg.callback(USER, "st:auto:0", message_id=menu.message_id)
    (update,) = tg.take_updates()
    incoming = parse_update(update)
    assert incoming is not None
    req = Request(bot, incoming, users.get(USER))
    users.update(USER, state="awaiting_crypto_btc")
    settings.cb_settings(req)
    assert users.get(USER).auto_approve is True
    assert tg.answers[-1]["text"] == EXPIRED
    assert not keyboard_of(tg, menu)


def test_bot_R10_settings_close(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    menu = open_menu(bot, tg)
    press(bot, tg, USER, "st:close", message_id=menu.message_id)
    assert tg.answers[-1]["text"] == "Closed."
    state = tg.message_state(USER, menu.message_id)
    assert state.text == "⚙ Settings closed." and state.reply_markup is None


def test_bot_R10_settings_setup_restarts_the_onboarding(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    ready_user(users)
    menu = open_menu(bot, tg)
    press(bot, tg, USER, "st:setup", message_id=menu.message_id)
    assert tg.answers[-1]["text"] == "Restarting setup…"
    assert tg.message_state(USER, menu.message_id).text == (
        "⚙ Settings closed — restarting YNAB setup."
    )
    user = users.get(USER)
    assert user.state == "awaiting_token" and user.account_id == "acct-main"  # kept until done
    assert "Personal Access Token" in tg.texts(USER)[-1]
    say(bot, tg, USER, "/cancel")
    assert users.get(USER).state == "ready"


def test_bot_R10_settings_crypto_opens_the_crypto_setup(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    ready_user(users)
    menu = open_menu(bot, tg)
    press(bot, tg, USER, "st:crypto", message_id=menu.message_id)
    assert tg.answers[-1]["text"] == "Opening crypto setup…"
    assert tg.message_state(USER, menu.message_id).text == (
        "⚙ Settings closed — opening crypto setup."
    )
    assert users.get(USER).state == "awaiting_crypto_account"
    picker = tg.last(USER)
    assert "pick your tracking account" in picker.plain
    assert picker.button_data()[0].endswith(":a:0")


@pytest.mark.parametrize("action", ["st:crypto", "st:setup"])
def test_bot_R10_settings_flows_refuse_while_busy(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, action: str
):
    ready_user(users)
    menu = open_menu(bot, tg)
    with LOCKS.operation(str(USER), "web upload"):
        press(bot, tg, USER, action, message_id=menu.message_id)
    assert tg.answers[-1]["text"] == render.BUSY_TEXT and tg.answers[-1]["show_alert"]
    assert users.get(USER).state == "ready"
    assert tg.message_state(USER, menu.message_id).text.startswith("⚙ <b>Settings</b>")


@pytest.mark.parametrize("data", ["st:bogus", "st:", "st:auto:2", "st:auto", "st:close:1"])
def test_bot_R10_settings_unknown_buttons_expire(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, data: str
):
    ready_user(users, auto_approve=True)
    menu = open_menu(bot, tg)
    tg.callback(USER, data, message_id=menu.message_id)
    deliver(bot, tg)
    assert tg.answers[-1]["text"] == EXPIRED
    assert not keyboard_of(tg, menu)
    assert users.get(USER).auto_approve is True
