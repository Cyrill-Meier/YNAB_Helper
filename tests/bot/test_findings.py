"""Regression tests for review findings fixed in the bot that have no other named test."""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

import pytest

from tests.bot.conftest import (
    ADMIN,
    BUDGET,
    CRYPTO,
    USER,
    YNAB_TOKEN,
    add_user,
    deliver,
    ready_user,
    say,
)
from tests.fakes.fake_telegram import FakeTelegram
from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.bot.app import Bot
from ynab_helper.bot.users import UsersRepo

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
BASIC = (FIXTURES / "export_basic.csv").read_bytes()
XPUB = (
    "xpub661MyMwAqRbcFtXgS5sYJABqqG9YLmC4Q1Rdap9gSE8NqtwybGhePY2gZ29ESFjqJoCu1Rupje8YtGqsefD265"
    "TMg7usUDFdp6W1EGMcet8"
)


def test_F15_import_never_touches_stdout(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, capsys: pytest.CaptureFixture[str]
):
    ready_user(users)
    before = sys.stdout
    tg.document(USER, "statement.csv", BASIC)
    deliver(bot, tg)
    assert sys.stdout is before
    captured = capsys.readouterr()
    assert captured.out == ""
    assert tg.texts(USER)[1].startswith("✅ Import complete")  # rendered from the result


def test_F19_telegram_file_name_never_becomes_a_path(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, data_dir: Path
):
    ready_user(users)
    before = sorted(p.name for p in data_dir.iterdir())
    tg.document(USER, "../../bot_users.db.csv", BASIC)
    deliver(bot, tg)
    assert tg.texts(USER)[1].startswith("✅ Import complete")
    stored = list((data_dir / "csv_cache" / str(USER)).iterdir())
    assert len(stored) == 1 and re.fullmatch(
        r"\d{8}T\d{6}Z?_[0-9a-f]{8}\.csv|.+_[0-9a-f]{8}\.csv", stored[0].name
    )
    after = sorted(p.name for p in data_dir.iterdir())
    assert set(after) - set(before) <= {
        "csv_cache",
        "tmp",
        "transactions_4242.db",
        "transactions_4242.db-wal",
        "transactions_4242.db-shm",
        "backups",
    }


def test_F47_cleanup_failures_are_reported(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, ynab: FakeYnab
):
    ready_user(users)
    for day in ("2026-10-01", "2026-10-02"):
        ynab.add_transaction(
            BUDGET,
            "acct-main",
            amount=-5000,
            date=day,
            payee_name="Shop",
            memo="(pending)",
            cleared="cleared",
        )
    ynab.fail_next("PATCH", r"/budgets/[^/]+/transactions", 400, times=1)  # bulk rejected
    ynab.fail_next("PUT", r"/budgets/[^/]+/transactions/.+", 500, times=1)
    say(bot, tg, USER, "/cleanup_pending")
    report = tg.texts(USER)[-1]
    assert "❌ Failed (1)" in report
    assert "Partially applied" in report
    assert "Cleaned 1 of 2" in report


def test_F48_a_typo_in_the_xpub_is_refused_at_setup(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    say(bot, tg, USER, "/crypto_setup")
    say(bot, tg, USER, "1")
    typo = XPUB[:-1] + ("9" if XPUB[-1] != "9" else "8")
    say(bot, tg, USER, typo)
    assert tg.texts(USER)[-1].startswith("❌")
    assert users.get(USER).state == "awaiting_crypto_btc"
    assert users.get(USER).btc_xpub is None


def test_F57_the_token_message_is_deleted(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    add_user(users, USER, "awaiting_token")
    update_id = tg.message(USER, YNAB_TOKEN)
    message_id = next(
        u["message"]["message_id"] for u in tg._updates if u["update_id"] == update_id
    )
    deliver(bot, tg)
    assert (USER, message_id) in tg.deleted
    assert YNAB_TOKEN not in tg.all_text()


def test_F57_undeletable_token_message_is_flagged(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    add_user(users, USER, "awaiting_token")
    tg.fail_next("deleteMessage", 400, "Bad Request: message can't be deleted")
    say(bot, tg, USER, YNAB_TOKEN)
    assert any("please delete it yourself" in t for t in tg.texts(USER))


def test_F92_bot_logs_no_tokens_or_payees(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, caplog: pytest.LogCaptureFixture
):
    add_user(users, USER, "awaiting_token")
    with caplog.at_level(logging.DEBUG):
        say(bot, tg, USER, YNAB_TOKEN)
        say(bot, tg, USER, "1")
        say(bot, tg, USER, "1")
        tg.document(USER, "statement.csv", BASIC)
        deliver(bot, tg)
    bot_lines = "\n".join(
        r.getMessage() for r in caplog.records if r.name.startswith("ynab_helper.bot")
    )
    assert YNAB_TOKEN not in caplog.text
    for payee in ("Coop Pronto", "Migros", "Starbucks", "Anna Example"):
        assert payee not in bot_lines


def test_F95_dedupe_result_is_sent_when_the_edit_fails(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, ynab: FakeYnab
):
    ready_user(users)
    tg.document(USER, "statement.csv", BASIC)
    deliver(bot, tg)
    stray = ynab.add_transaction(
        BUDGET,
        "acct-main",
        amount=-777,
        date="2026-10-03",
        payee_name="Stray",
        import_id="YNAB:-777:2026-10-03:9",
        cleared="cleared",
    )
    say(bot, tg, USER, "/dedupe")
    picker = tg.last(USER)
    nonce = picker.button_data()[0].split(":")[1]
    for action in ("t:0", "del"):
        tg.callback(USER, f"dx:{nonce}:{action}", message_id=picker.message_id)
        deliver(bot, tg)
    tg.fail_next("editMessageText", 400, "Bad Request: message to edit not found", times=2)
    tg.callback(USER, f"dx:{nonce}:ok", message_id=picker.message_id)
    deliver(bot, tg)
    assert stray not in {t["id"] for t in ynab.transactions(BUDGET, account_id="acct-main")}
    assert tg.texts(USER)[-1].startswith("✓ Deleted 1 of 1 transaction")  # sent instead


@pytest.mark.parametrize(
    "command", ["/help", "/settings", "/crypto_status", "/auto_approve", "/users"]
)
def test_F96_no_markdown_markup_reaches_the_user(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, command: str
):
    ready_user(users, crypto_account_id=CRYPTO, crypto_account_name="my_crypto_acct")
    ready_user(users, ADMIN)
    sender = ADMIN if command == "/users" else USER
    say(bot, tg, sender, command)
    message = tg.last(sender)
    assert message.parse_mode == "HTML"
    assert not re.search(r"\*\w|\w\*|`", message.plain)  # no literal Markdown
    if command == "/crypto_status":
        assert "my_crypto_acct" in message.plain  # underscores survive


def test_F96_v1_style_reply_keyboard_is_removed_after_setup(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, ynab: FakeYnab
):
    add_user(users, USER, "awaiting_token")
    say(bot, tg, USER, YNAB_TOKEN)
    say(bot, tg, USER, "1")
    say(bot, tg, USER, "1")
    assert tg.last(USER).reply_markup == {"remove_keyboard": True}
