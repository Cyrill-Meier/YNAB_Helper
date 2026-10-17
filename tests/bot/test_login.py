"""/login (URL button, token never in the text: F25) and /logout_all (F61)."""

from __future__ import annotations

import logging
import threading
import urllib.parse

import pytest

from tests.bot.conftest import USER, ready_user, say
from tests.fakes.fake_telegram import FakeTelegram
from ynab_helper import sessions
from ynab_helper.bot.app import Bot, WebAccess
from ynab_helper.bot.users import UsersRepo
from ynab_helper.web.config import WebConfig

PUBLIC = "https://bot.example.test"
SECRET = "k" * 40


class FakeHandle:
    def __init__(self, *, started: bool = True, failed: BaseException | None = None) -> None:
        self.started = threading.Event()
        if started:
            self.started.set()
        self.failed = failed
        self.stops: list[float] = []

    def wait_started(self, timeout: float) -> bool:
        return self.started.wait(timeout)

    def stop(self, timeout: float = 5.0) -> None:
        self.stops.append(timeout)


def web(handle: FakeHandle | None = None, status: str = "enabled") -> WebAccess:
    config = WebConfig(enabled=True, public_url=PUBLIC, secret_key=SECRET, login_ttl=300)
    return WebAccess(config, handle or FakeHandle(), status, PUBLIC)


def login_rows(users: UsersRepo) -> list[tuple]:
    return [
        tuple(r)
        for r in users.conn.execute(
            "SELECT token_hash, telegram_id, kind, tg_chat_id, tg_message_id FROM web_sessions"
        )
    ]


def test_F25_login_sends_a_url_button_and_never_the_token_in_text(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    ready_user(users)
    bot.web = web()
    for _ in range(30):  # many tokens: some contain '_' (v1 mangled those, F25)
        say(bot, tg, USER, "/login")
    hasher = sessions.TokenHasher(SECRET)
    rows = {row[0]: row for row in login_rows(users)}
    assert len(rows) == 30
    for message in tg.sent(USER):
        params = message.params
        assert params["link_preview_options"] == {"is_disabled": True}
        [button] = message.buttons
        assert button["text"] == "Open dashboard"
        url = urllib.parse.urlsplit(button["url"])
        assert f"{url.scheme}://{url.netloc}" == PUBLIC and url.path == "/auth"
        token = urllib.parse.parse_qs(url.query)["t"][0]
        assert token not in message.text and "/auth" not in message.text
        row = rows[hasher.hash(token)]  # the exact token works: nothing was mangled
        assert row[1:] == (USER, "login", USER, message.message_id)


def test_login_when_the_web_ui_is_disabled(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    bot.web = WebAccess(None, None, "disabled", "turned off")
    say(bot, tg, USER, "/login")
    assert "disabled" in tg.texts(USER)[-1]
    assert login_rows(users) == []


def test_F94_login_when_the_web_ui_failed_or_has_not_started(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    ready_user(users)
    for access in (
        web(FakeHandle(failed=OSError("address in use")), status="failed"),
        web(FakeHandle(started=False), status="failed"),
    ):
        bot.web = access
        say(bot, tg, USER, "/login")
        assert "not running" in tg.texts(USER)[-1]
    assert login_rows(users) == []


def test_a_web_ui_that_starts_late_becomes_usable(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    handle = FakeHandle(started=False)
    bot.web = web(handle, status="failed")
    handle.started.set()
    say(bot, tg, USER, "/login")
    assert tg.last(USER).buttons[0]["text"] == "Open dashboard"


def test_refused_button_url_is_reported_without_the_token(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    ready_user(users)
    bot.web = web()
    tg.fail_next("sendMessage", 400, "Bad Request: BUTTON_URL_INVALID")
    say(bot, tg, USER, "/login")
    assert "refused the sign-in button" in tg.texts(USER)[-1]
    assert login_rows(users) == []  # the unsent token was burnt


def _token_of_rejected_send(tg: FakeTelegram) -> str:
    [rejected] = [c for c in tg.calls_of("sendMessage") if "reply_markup" in c]
    url = rejected["reply_markup"]["inline_keyboard"][0][0]["url"]
    return urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)["t"][0]


def test_F25_F92_a_rejected_login_button_never_logs_the_token_and_burns_it(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, caplog: pytest.LogCaptureFixture
):
    """Telegram's error for a non-public URL echoes the button URL (with the token)."""
    ready_user(users)
    bot.web = web()
    tg.rejected_url_hosts.add(urllib.parse.urlsplit(PUBLIC).hostname)
    caplog.set_level(logging.DEBUG)
    say(bot, tg, USER, "/login")
    assert "refused the sign-in button" in tg.texts(USER)[-1]
    token = _token_of_rejected_send(tg)
    assert "is invalid: Wrong HTTP URL" in caplog.text  # the error itself is logged …
    assert token not in caplog.text  # … with the token redacted
    assert urllib.parse.quote(token, safe="") not in caplog.text
    assert token not in tg.all_text(USER)
    assert login_rows(users) == []
    hasher = sessions.TokenHasher(SECRET)
    result = sessions.consume_login_token(
        users.conn,
        hasher,
        token,
        session_ttl=600,
        absolute_ttl=600,
        ip="203.0.113.9",
        user_agent="attacker",
    )
    assert result is None  # whoever read the log cannot sign in with it


def test_F25_a_login_message_lost_on_the_network_burns_its_token(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, caplog: pytest.LogCaptureFixture
):
    ready_user(users)
    bot.web = web()
    tg.network_error_next("sendMessage")
    say(bot, tg, USER, "/login")
    assert login_rows(users) == []
    assert tg.texts(USER)[-1] == "❌ Couldn't send the sign-in link. Please try /login again."


def test_F61_logout_all_revokes_every_session(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    bot.web = web()
    say(bot, tg, USER, "/login")
    say(bot, tg, USER, "/login")
    assert len(login_rows(users)) == 2
    say(bot, tg, USER, "/logout_all")
    assert login_rows(users) == []
    assert "2 session" in tg.texts(USER)[-1]
