"""/crypto rendering (F29), /crypto_setup flow (F55, F48 validation) and /crypto_status."""

from __future__ import annotations

import time
from decimal import Decimal

import pytest

from tests.bot.conftest import CRYPTO, USER, deliver, press, ready_user, say
from tests.fakes.fake_telegram import FakeTelegram, check_html
from ynab_helper import services
from ynab_helper.balance.adjust import AdjustmentRefused, AdjustmentResult
from ynab_helper.balance.crypto.sync import BtcSummary, CryptoSyncResult, Holding
from ynab_helper.bot import render
from ynab_helper.bot.app import Bot
from ynab_helper.bot.handlers.crypto import CRYPTO_FLOW_TTL
from ynab_helper.bot.users import UsersRepo

BTC_ADDRESS = "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"
ETH_ADDRESS = "0xde709f2102306220921060314715629080e2fb77"
XPUB = (
    "xpub661MyMwAqRbcFtXgS5sYJABqqG9YLmC4Q1Rdap9gSE8NqtwybGhePY2gZ29ESFjqJoCu1Rupje8YtGqsefD265"
    "TMg7usUDFdp6W1EGMcet8"
)


def adjustment(outcome: str, delta: int) -> AdjustmentResult:
    return AdjustmentResult(
        outcome=outcome,
        delta_milli=delta,
        import_id=None if outcome == "in_sync" else f"CRYPTO:{delta}:2026-10-17:1",
        ynab_tx_id="tx-1" if outcome == "created" else None,
        target_milli=29_090_000,
        current_milli=20_000_000,
        account_id=CRYPTO,
        date="2026-10-17",
        payee="Crypto Portfolio Adjustment",
        memo=None,
        cleared="cleared",
    )


def result(outcome: str = "created", delta: int = 9_090_000) -> CryptoSyncResult:
    return CryptoSyncResult(
        holdings=[
            Holding("BTC", Decimal("0.5"), Decimal("50000.12"), 25_000_060),
            Holding("ETH", Decimal("1.234567890123456789"), Decimal("3300"), 4_074_074),
        ],
        portfolio_milli=29_090_000,
        ynab_before_milli=20_000_000,
        adjustment=adjustment(outcome, delta),
        ynab_uncleared_milli=0,
        zero_balances=["USDC"],
        btc=BtcSummary("xpub", "zpub", 50_000_000, 3, 46),
    )


# ── rendering (F29) ──────────────────────────────────────────────────────────


def test_F29_summary_shows_holdings_total_and_the_balance_before():
    text = render.crypto_result(result(), account_name="Crypto <wallet>")
    plain = check_html(text)
    assert "Crypto sync — Crypto <wallet>" in plain
    assert "BTC 0.5 × 50,000.12 CHF = 25,000.06 CHF" in plain
    assert "ETH 1.23456789 × 3,300.00 CHF = 4,074.07 CHF" in plain
    assert "Portfolio: 29,090.00 CHF" in plain
    assert "YNAB before: 20,000.00 CHF" in plain  # v1 showed the *new* balance here
    assert "Adjustment posted: +9,090.00 CHF → YNAB now 29,090.00 CHF" in plain
    assert "3 used addresses of 46 scanned" in plain


def test_F29_duplicate_is_not_reported_as_an_adjustment():
    plain = check_html(render.crypto_result(result("duplicate", 5_000_000)))
    assert "Already synced today" in plain and "nothing new was posted" in plain
    assert "Adjustment posted" not in plain


def test_in_sync_summary():
    plain = check_html(render.crypto_result(result("in_sync", 3)))
    assert "Already in sync" in plain


def test_crypto_command_renders_the_service_result(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, monkeypatch: pytest.MonkeyPatch
):
    ready_user(users, crypto_account_id=CRYPTO, crypto_account_name="Crypto", btc_xpub=XPUB)
    monkeypatch.setattr(services, "crypto_sync", lambda ctx: result())
    say(bot, tg, USER, "/crypto")
    assert tg.texts(USER)[0].startswith("🪙 Syncing")
    assert "Portfolio: 29,090.00 CHF" in tg.texts(USER)[-1]


def test_crypto_refusal_is_reported(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, monkeypatch: pytest.MonkeyPatch
):
    ready_user(users, crypto_account_id=CRYPTO, btc_xpub=XPUB)

    def refuse(ctx):
        raise AdjustmentRefused(
            "x", reason="btc_unused", user_message="The xpub found no used address."
        )

    monkeypatch.setattr(services, "crypto_sync", refuse)
    say(bot, tg, USER, "/crypto")
    assert tg.texts(USER)[-1] == "❌ The xpub found no used address."


def test_crypto_without_configuration(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    say(bot, tg, USER, "/crypto")
    assert "/crypto_setup" in tg.texts(USER)[-1]


def test_crypto_status_masks_the_xpub(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users, crypto_account_id=CRYPTO, crypto_account_name="Crypto", btc_xpub=XPUB)
    say(bot, tg, USER, "/crypto_status")
    text = tg.texts(USER)[-1]
    assert XPUB not in text and XPUB[:8] in text and XPUB[-6:] in text


# ── /crypto_setup ────────────────────────────────────────────────────────────


def start_setup(bot: Bot, tg: FakeTelegram) -> str:
    say(bot, tg, USER, "/crypto_setup")
    data = tg.last(USER).button_data()
    assert data[0].endswith(":a:0") and data[-1].endswith(":x")
    assert "Crypto" in tg.last(USER).plain and "Savings" not in tg.last(USER).plain
    return data[0].split(":")[1]


def test_crypto_setup_happy_path_commits_at_the_end(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    nonce = start_setup(bot, tg)
    assert users.get(USER).state == "awaiting_crypto_account"
    press(bot, tg, USER, f"cs:{nonce}:a:0")
    assert users.get(USER).state == "awaiting_crypto_btc"
    assert users.get(USER).crypto_account_id is None  # nothing saved yet
    say(bot, tg, USER, "xpub-not-really")
    assert tg.texts(USER)[-1].startswith("❌")
    say(bot, tg, USER, XPUB)
    assert users.get(USER).state == "awaiting_crypto_eth"
    say(bot, tg, USER, "0x1234")
    assert "doesn't look like an Ethereum address" in tg.texts(USER)[-1]
    say(bot, tg, USER, ETH_ADDRESS)
    user = users.get(USER)
    assert user.state == "ready" and user.temp_data is None
    assert (user.crypto_account_id, user.crypto_account_name) == (CRYPTO, "Crypto")
    assert user.btc_xpub == XPUB and user.eth_address == ETH_ADDRESS
    assert XPUB not in tg.all_text(USER)  # only masked


def test_skip_and_keep_buttons(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users, btc_xpub=BTC_ADDRESS, eth_address=ETH_ADDRESS)
    nonce = start_setup(bot, tg)
    press(bot, tg, USER, f"cs:{nonce}:a:0")
    assert f"cs:{nonce}:keep" in tg.last(USER).button_data()
    press(bot, tg, USER, f"cs:{nonce}:keep")
    press(bot, tg, USER, f"cs:{nonce}:skip")
    user = users.get(USER)
    assert user.state == "ready"
    assert user.btc_xpub == BTC_ADDRESS and user.eth_address is None


@pytest.mark.parametrize("how", ["/cancel", "cancel", "button"])
def test_F55_cancel_restores_without_changes(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, how: str
):
    ready_user(users, crypto_account_id="old", crypto_account_name="Old", btc_xpub=BTC_ADDRESS)
    nonce = start_setup(bot, tg)
    press(bot, tg, USER, f"cs:{nonce}:a:0")
    if how == "button":
        press(bot, tg, USER, f"cs:{nonce}:x")
    else:
        say(bot, tg, USER, how)
    user = users.get(USER)
    assert user.state == "ready" and user.temp_data is None
    assert (user.crypto_account_id, user.btc_xpub) == ("old", BTC_ADDRESS)


def test_F55_documents_and_commands_are_not_swallowed(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    start_setup(bot, tg)
    tg.document(USER, "statement.csv", b"Type,Product\n")
    deliver(bot, tg)
    assert "crypto setup" in tg.texts(USER)[-1] and "not imported" in tg.texts(USER)[-1]
    say(bot, tg, USER, "/status")
    assert "crypto setup" in tg.texts(USER)[-1] and "/cancel" in tg.texts(USER)[-1]
    assert tg.calls_of("getFile") == []


def test_F55_an_abandoned_crypto_setup_ends_by_itself(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, monkeypatch: pytest.MonkeyPatch
):
    ready_user(users)
    start_setup(bot, tg)
    real_time = time.time
    monkeypatch.setattr(time, "time", lambda: real_time() + CRYPTO_FLOW_TTL + 60)
    say(bot, tg, USER, "/help")
    assert users.get(USER).state == "ready"
    texts = tg.texts(USER)
    assert any("unfinished crypto setup was closed" in t for t in texts)
    assert "Revolut → YNAB Bot" in texts[-1]  # the command itself ran


def test_stale_crypto_buttons_expire(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    old = start_setup(bot, tg)
    new = start_setup(bot, tg)
    assert old != new
    press(bot, tg, USER, f"cs:{old}:a:0")
    assert users.get(USER).state == "awaiting_crypto_account"
    assert tg.answers[-1]["text"] == "This crypto setup has expired."


def test_no_tracking_account(bot: Bot, tg: FakeTelegram, users: UsersRepo, ynab):
    ynab.edit_account("b1", CRYPTO, closed=True)
    ready_user(users)
    say(bot, tg, USER, "/crypto_setup")
    assert "No open tracking accounts" in tg.texts(USER)[-1]
    assert users.get(USER).state == "ready"


# ── wallet messages never stay in the chat (R04) ─────────────────────────────

# Public test vectors, not real secrets: BIP32 test vector 1's master private key and the
# Bitcoin wiki's example WIF.
XPRV = (
    "xprv9s21ZrQH143K3QTDL4LXw2F7HEK3wJUD2nW2nRk4stbPy6cq3jPPqjiChkVvvNKmPGJxWUtg6LnF5kejMRNN"
    "U3TGtRBeJgk33yuGBxrMPHi"
)
WIF = "5HueCGU8rMjxEXxiPuD5BDku4MkFqeZyd4dZ1jvhTVqvbTLvyTJ"


def send_text(bot: Bot, tg: FakeTelegram, text: str) -> int:
    """Deliver a text message from USER; returns its message id."""
    tg.message(USER, text)
    updates = tg.take_updates()
    for update in updates:
        bot.handle_update(update)
    return updates[-1]["message"]["message_id"]


def at_btc_step(bot: Bot, tg: FakeTelegram, users: UsersRepo) -> str:
    nonce = start_setup(bot, tg)
    press(bot, tg, USER, f"cs:{nonce}:a:0")
    assert users.get(USER).state == "awaiting_crypto_btc"
    return nonce


def test_bot_R04_wallet_messages_are_deleted(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    at_btc_step(bot, tg, users)
    typo = send_text(bot, tg, XPUB[:-1] + ("9" if XPUB[-1] != "9" else "8"))
    assert (USER, typo) in tg.deleted  # refused, but deleted all the same
    xpub = send_text(bot, tg, XPUB)
    assert (USER, xpub) in tg.deleted
    assert users.get(USER).state == "awaiting_crypto_eth"
    assert "delete it yourself" not in tg.texts(USER)[-1]
    eth = send_text(bot, tg, ETH_ADDRESS)
    assert (USER, eth) in tg.deleted
    user = users.get(USER)
    assert user.state == "ready" and (user.btc_xpub, user.eth_address) == (XPUB, ETH_ADDRESS)
    assert tg.deleted == [(USER, typo), (USER, xpub), (USER, eth)]


def test_bot_R04_control_words_stay(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    at_btc_step(bot, tg, users)
    send_text(bot, tg, "skip")
    assert users.get(USER).state == "awaiting_crypto_eth"
    send_text(bot, tg, "Skip")
    assert users.get(USER).state == "ready"
    assert tg.calls_of("deleteMessage") == []


@pytest.mark.parametrize("secret", [XPRV, WIF], ids=["xprv", "wif"])
def test_bot_R04_private_key_is_deleted_and_flagged(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, secret: str
):
    ready_user(users)
    at_btc_step(bot, tg, users)
    message_id = send_text(bot, tg, secret)
    assert tg.deleted == [(USER, message_id)]
    reply = tg.texts(USER)[-1]
    assert "PRIVATE key" in reply and "I deleted your message from this chat" in reply
    assert users.get(USER).state == "awaiting_crypto_btc"
    assert users.get(USER).temp_data is not None and secret not in str(users.get(USER).temp_data)
    assert secret not in tg.all_text()


def test_bot_R04_undeletable_private_key_warns_urgently(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    ready_user(users)
    at_btc_step(bot, tg, users)
    tg.fail_next("deleteMessage", 400, "Bad Request: message can't be deleted")
    send_text(bot, tg, XPRV)
    reply = tg.texts(USER)[-1]
    assert "PRIVATE key" in reply
    assert "delete it from this chat yourself, now" in reply
    assert "move the funds to a new wallet" in reply
    assert tg.deleted == []


def test_bot_R04_undeletable_xpub_or_address_asks_the_user(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    ready_user(users)
    at_btc_step(bot, tg, users)
    tg.fail_next("deleteMessage", 400, "Bad Request: message can't be deleted")
    send_text(bot, tg, XPUB)
    prompt = tg.texts(USER)[-1]
    assert prompt.startswith("✅ BTC:") and "please delete it yourself" in prompt
    tg.fail_next("deleteMessage", 400, "Bad Request: message can't be deleted")
    send_text(bot, tg, "0x1234")
    assert "please delete it yourself" in tg.texts(USER)[-1]
    tg.fail_next("deleteMessage", 400, "Bad Request: message can't be deleted")
    send_text(bot, tg, ETH_ADDRESS)
    done = tg.texts(USER)[-1]
    assert "Crypto setup complete" in done and "please delete it yourself" in done
    assert users.get(USER).state == "ready"
