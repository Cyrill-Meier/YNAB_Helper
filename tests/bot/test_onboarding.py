"""Onboarding: approve → token → budget → account; failures, /cancel, stale buttons."""

from __future__ import annotations

import json
import threading
import time

import pytest

from tests.bot.conftest import (
    ACCT,
    ADMIN,
    BUDGET,
    CLOSED,
    CRYPTO,
    OTHER,
    USER,
    YNAB_TOKEN,
    add_user,
    deliver,
    press,
    ready_user,
    say,
)
from tests.fakes.fake_telegram import FakeTelegram
from tests.fakes.fake_ynab import FakeYnab
from ynab_helper import services, sessions
from ynab_helper.bot.app import Bot
from ynab_helper.bot.users import UsersRepo
from ynab_helper.bot.workers import WorkerPool

SECRET = "s" * 40


def picker_data(tg: FakeTelegram, prefix: str) -> list[str]:
    for message in reversed(tg.sent(USER)):
        data = [d for d in message.button_data() if d.startswith(prefix)]
        if data:
            return data
    raise AssertionError(f"no buttons with {prefix!r}")


def approve(bot: Bot, tg: FakeTelegram, users: UsersRepo) -> None:
    say(bot, tg, USER, "hello")  # registers as pending
    say(bot, tg, ADMIN, f"/approve {USER}")
    assert users.get(USER).state == "awaiting_token"


def send_token(bot: Bot, tg: FakeTelegram, token: str = YNAB_TOKEN) -> int:
    update_id = tg.message(USER, token)
    message_id = next(
        u["message"]["message_id"] for u in list(tg._updates) if u["update_id"] == update_id
    )
    deliver(bot, tg)
    return message_id


# ── happy path ───────────────────────────────────────────────────────────────


def test_full_onboarding_with_buttons(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    approve(bot, tg, users)
    assert "Personal Access Token" in tg.texts(USER)[-1]
    token_message = send_token(bot, tg)
    assert (USER, token_message) in tg.deleted  # the PAT does not stay in the chat (F57)
    user = users.get(USER)
    assert user.state == "awaiting_budget" and user.ynab_token is None  # staged only
    budgets = picker_data(tg, "ob:")
    assert budgets[0].endswith(":b:0") and budgets[-1].endswith(":x")
    press(bot, tg, USER, budgets[0])
    accounts = picker_data(tg, "ob:")
    assert any(d.endswith(":a:0") for d in accounts)
    listing = tg.texts(USER)[-1]
    assert "Revolut" in listing and "Savings" in listing
    assert "Revolut old" not in listing  # closed accounts are not offered (F53)
    press(bot, tg, USER, accounts[0])
    user = users.get(USER)
    assert user.state == "ready"
    assert (user.ynab_token, user.budget_id, user.account_id) == (YNAB_TOKEN, BUDGET, ACCT)
    assert user.budget_name == "Household" and user.account_name == "Revolut"
    assert user.temp_data is None
    final = tg.texts(USER)[-1]
    assert "All set" in final and "checking" in final  # type (and balance) shown (F53)
    assert YNAB_TOKEN not in tg.all_text()  # the token is never echoed


def test_typed_number_and_name_still_work(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    approve(bot, tg, users)
    send_token(bot, tg)
    say(bot, tg, USER, "household")  # exact name, case-insensitive
    assert users.get(USER).state == "awaiting_account"
    say(bot, tg, USER, "2")  # the second offered account
    user = users.get(USER)
    assert user.state == "ready" and user.account_id == OTHER


def test_F53_duplicate_account_names_ask_for_the_number(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, ynab: FakeYnab
):
    ynab.add_account(BUDGET, "acct-dup", "Revolut")
    ynab.add_account(BUDGET, "acct-del", "Gone", deleted=True)
    approve(bot, tg, users)
    send_token(bot, tg)
    say(bot, tg, USER, "1")
    listing = tg.texts(USER)[-1]
    assert "Gone" not in listing and "Revolut old" not in listing
    say(bot, tg, USER, "Revolut")
    assert users.get(USER).state == "awaiting_account"
    assert "number" in tg.texts(USER)[-1]
    offered = json.loads(users.get(USER).temp_data)["accounts"]
    index = next(i for i, a in enumerate(offered) if a["id"] == "acct-dup") + 1
    say(bot, tg, USER, str(index))
    assert users.get(USER).account_id == "acct-dup"
    assert {a["id"] for a in offered} == {ACCT, OTHER, CRYPTO, "acct-dup"}
    assert CLOSED not in {a["id"] for a in offered}


# ── failures (F52) ───────────────────────────────────────────────────────────


def test_F52_rejected_token_asks_again(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    approve(bot, tg, users)
    message_id = send_token(bot, tg, "wrong-token-0123456789abcdef")
    assert (USER, message_id) in tg.deleted
    assert "rejected" in tg.texts(USER)[-1]
    assert users.get(USER).state == "awaiting_token"
    send_token(bot, tg)
    assert users.get(USER).state == "awaiting_budget"


def test_F52_network_failure_while_validating(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, ynab: FakeYnab
):
    approve(bot, tg, users)
    ynab.raise_network_error_next("GET", r"/user")
    send_token(bot, tg)
    assert "Could not reach YNAB" in tg.texts(USER)[-1]
    assert users.get(USER).state == "awaiting_token"


def test_F52_failure_while_loading_accounts_keeps_the_picker(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, ynab: FakeYnab
):
    approve(bot, tg, users)
    send_token(bot, tg)
    budgets = picker_data(tg, "ob:")
    ynab.fail_next("GET", r"/budgets/[^/]+/accounts", 503)
    press(bot, tg, USER, budgets[0])
    assert "Could not load the accounts" in tg.texts(USER)[-1]
    assert users.get(USER).state == "awaiting_budget"
    press(bot, tg, USER, budgets[0])  # the same button works again
    assert users.get(USER).state == "awaiting_account"


def test_F52_token_revoked_between_steps_goes_back_to_the_token(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, ynab: FakeYnab
):
    approve(bot, tg, users)
    send_token(bot, tg)
    ynab.fail_next("GET", r"/budgets/[^/]+/accounts", 401)
    say(bot, tg, USER, "1")
    assert users.get(USER).state == "awaiting_token"
    assert "no longer accepts" in tg.texts(USER)[-1]


def test_F52_help_and_cancel_work_in_every_step(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    approve(bot, tg, users)
    send_token(bot, tg)
    say(bot, tg, USER, "/help")
    assert "/cancel" in tg.texts(USER)[-1]
    say(bot, tg, USER, "/cancel")
    assert users.get(USER).state == "awaiting_token"  # no previous setup to return to
    assert users.get(USER).temp_data is None


def test_document_during_onboarding_is_refused_not_downloaded(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    approve(bot, tg, users)
    tg.document(USER, "statement.csv", b"Type,Product\n")
    deliver(bot, tg)
    assert tg.calls_of("getFile") == []
    assert "not imported" in tg.texts(USER)[-1]
    assert users.get(USER).state == "awaiting_token"


# ── re-setup and /cancel (F54, F18, C05) ─────────────────────────────────────


def test_F54_setup_keeps_the_old_config_until_done_and_cancel_restores_it(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    ready_user(users, crypto_account_id=CRYPTO, crypto_account_name="Crypto")
    say(bot, tg, USER, "/setup")
    user = users.get(USER)
    assert user.state == "awaiting_token"
    assert (user.ynab_token, user.budget_id, user.account_id) == (YNAB_TOKEN, BUDGET, ACCT)
    send_token(bot, tg)
    say(bot, tg, USER, "/cancel")
    user = users.get(USER)
    assert user.state == "ready"
    assert (user.ynab_token, user.budget_id, user.account_id) == (YNAB_TOKEN, BUDGET, ACCT)
    assert user.crypto_account_id == CRYPTO
    assert "previous settings" in tg.texts(USER)[-1]


def test_cancel_button_in_the_picker(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    say(bot, tg, USER, "/setup")
    send_token(bot, tg)
    cancel = next(d for d in picker_data(tg, "ob:") if d.endswith(":x"))
    press(bot, tg, USER, cancel)
    assert users.get(USER).state == "ready"


def test_F18_changing_the_account_revokes_web_sessions(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    ready_user(users)
    hasher = sessions.TokenHasher(SECRET)
    sessions.issue_login_token(users.conn, hasher, USER, ttl_seconds=300, lock=users.lock)
    say(bot, tg, USER, "/setup")
    send_token(bot, tg)
    say(bot, tg, USER, "1")
    say(bot, tg, USER, "2")  # Savings instead of Revolut
    assert users.get(USER).account_id == OTHER
    assert users.conn.execute("SELECT COUNT(*) FROM web_sessions").fetchone()[0] == 0
    assert "signed out" in tg.texts(USER)[-1]


def test_unchanged_config_keeps_web_sessions(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    hasher = sessions.TokenHasher(SECRET)
    sessions.issue_login_token(users.conn, hasher, USER, ttl_seconds=300, lock=users.lock)
    say(bot, tg, USER, "/setup")
    send_token(bot, tg)
    say(bot, tg, USER, "1")
    say(bot, tg, USER, "1")
    assert users.conn.execute("SELECT COUNT(*) FROM web_sessions").fetchone()[0] == 1


def test_C05_switching_budget_clears_the_crypto_account(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, ynab: FakeYnab
):
    ynab.add_budget("b2", currency="CHF", name="Other budget")
    ynab.add_account("b2", "b2-acct", "Revolut B2")
    ready_user(users, crypto_account_id=CRYPTO, crypto_account_name="Crypto", btc_xpub="xpubX")
    say(bot, tg, USER, "/setup")
    send_token(bot, tg)
    say(bot, tg, USER, "Other budget")
    say(bot, tg, USER, "1")
    user = users.get(USER)
    assert user.budget_id == "b2"
    assert user.crypto_account_id is None and user.crypto_account_name is None
    assert user.btc_xpub == "xpubX"  # wallets are not budget-specific
    assert "/crypto_setup" in tg.texts(USER)[-1]


def test_stale_picker_of_an_earlier_flow_expires(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    approve(bot, tg, users)
    send_token(bot, tg)
    old = picker_data(tg, "ob:")[0]
    say(bot, tg, USER, "/setup")
    send_token(bot, tg)
    old_message = tg.sent(USER)[-3].message_id
    tg.callback(USER, old, message_id=old_message)
    deliver(bot, tg)
    assert users.get(USER).state == "awaiting_budget"
    assert tg.answers[-1]["text"] == "This setup step has expired."


def test_v1_leftover_budget_list_restarts_the_setup(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    add_user(
        users,
        USER,
        "awaiting_budget",
        ynab_token=YNAB_TOKEN,
        temp_data=json.dumps([{"id": BUDGET, "name": "Household"}]),
    )
    say(bot, tg, USER, "1")
    assert users.get(USER).state == "awaiting_token"
    assert "interrupted" in tg.texts(USER)[-1]


# ── races ────────────────────────────────────────────────────────────────────


def test_deny_while_the_token_is_checked_wins(
    tg: FakeTelegram, users: UsersRepo, client, data_dir, ynab: FakeYnab, monkeypatch
):
    """A worker finishing a step must not overwrite the admin's /deny (or a /cancel)."""
    entered, release = threading.Event(), threading.Event()
    real = services.token_client_factory

    def slow_factory(token: str):
        entered.set()
        assert release.wait(5)
        return real(token)

    monkeypatch.setattr(services, "token_client_factory", slow_factory)
    workers = WorkerPool(1)
    bot = Bot(telegram=client, users=users, admin_id=ADMIN, data_dir=data_dir, workers=workers)
    add_user(users, USER, "awaiting_token")
    send_token(bot, tg)
    assert entered.wait(5)
    say(bot, tg, ADMIN, f"/deny {USER}")
    release.set()
    assert workers.wait_idle(5)
    workers.shutdown(1)
    user = users.get(USER)
    assert user.state == "denied"
    assert not any("Select your budget" in t for t in tg.texts(USER))


def test_a_second_token_while_one_is_checked_is_busy(
    tg: FakeTelegram, users: UsersRepo, client, data_dir, monkeypatch
):
    entered, release = threading.Event(), threading.Event()
    real = services.token_client_factory

    def slow_factory(token: str):
        entered.set()
        assert release.wait(5)
        return real(token)

    monkeypatch.setattr(services, "token_client_factory", slow_factory)
    workers = WorkerPool(1)
    bot = Bot(telegram=client, users=users, admin_id=ADMIN, data_dir=data_dir, workers=workers)
    add_user(users, USER, "awaiting_token")
    send_token(bot, tg)
    assert entered.wait(5)
    send_token(bot, tg)
    assert "Another operation" in tg.texts(USER)[-1]
    release.set()
    assert workers.wait_idle(5)
    workers.shutdown(1)
    assert users.get(USER).state == "awaiting_budget"  # the first one completed normally


@pytest.mark.parametrize("text", ["short", "has spaces in it 1234567890", "x" * 300])
def test_non_token_text_is_deleted_and_refused(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, text: str
):
    add_user(users, USER, "awaiting_token")
    message_id = send_token(bot, tg, text)
    assert (USER, message_id) in tg.deleted
    assert "doesn't look like" in tg.texts(USER)[-1]
    assert users.get(USER).state == "awaiting_token"
    time.sleep(0)  # no worker involved
