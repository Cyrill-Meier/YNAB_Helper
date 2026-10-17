"""The authorization gate: every command and callback × every kind of sender (F03, F56)."""

from __future__ import annotations

import dataclasses
import time
from pathlib import Path
from typing import Any

import pytest

from tests.bot.conftest import ADMIN, STRANGER, USER, add_user, deliver, ready_user, say
from tests.fakes.fake_telegram import FakeTelegram
from ynab_helper.bot import auth
from ynab_helper.bot.app import Bot
from ynab_helper.bot.commands import Registry, build_registry
from ynab_helper.bot.context import Request
from ynab_helper.bot.telegram import TelegramClient, button, inline_keyboard
from ynab_helper.bot.users import UsersRepo
from ynab_helper.bot.workers import WorkerPool

REGISTRY = build_registry()
USER_COMMANDS = [c.name for c in REGISTRY.commands if not c.admin]
ADMIN_COMMANDS = [c.name for c in REGISTRY.commands if c.admin]
CALLBACKS = [
    "st:auto:0",
    "st:auto:1",
    "st:crypto",
    "st:setup",
    "st:close",
    "dd:ok",
    "dd:t:1",
    "dd:del",
    "dd:noop",
    "dx:nonce123:ok",
    "dx:nonce123:t:0",
    "rc:nonce123:ok",
    "ob:nonce123:b:0",
    "ob:nonce123:a:0",
    "cs:nonce123:a:0",
    "cs:nonce123:skip",
    "zz:garbage",
    "",
]
SETUP_ALLOWED = {"setup", "cancel", "help", "start", "logout_all"}
CRYPTO_ALLOWED = {"crypto_setup", "cancel", "help", "start", "logout_all"}


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def handler(self, name: str):
        def run(req: Request) -> None:
            self.calls.append((name, req.user_id))
            req.answer()

        return run


@pytest.fixture
def recorder() -> Recorder:
    return Recorder()


@pytest.fixture
def gated_bot(bot: Bot, recorder: Recorder) -> Bot:
    """The bot with every handler replaced by a recorder (tests only the gate)."""
    bot.registry = Registry(
        [dataclasses.replace(c, handler=recorder.handler(c.name)) for c in REGISTRY.commands],
        [dataclasses.replace(r, handler=recorder.handler(r.prefix)) for r in REGISTRY.callbacks],
    )
    return bot


def snapshot(users: UsersRepo) -> list[Any]:
    return [dataclasses.astuple(u) for u in users.list_all()]


def keyboard_message(bot: Bot, chat_id: int) -> int:
    message = bot.tg.send_message(
        chat_id, "menu", reply_markup=inline_keyboard([[button("x", "st:close")]])
    )
    assert message is not None
    return message["message_id"]


# ── messages ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("name", USER_COMMANDS + ADMIN_COMMANDS)
def test_unknown_sender_is_registered_pending_and_nothing_runs(
    gated_bot: Bot, tg: FakeTelegram, users: UsersRepo, recorder: Recorder, name: str
):
    tg.message(STRANGER, f"/{name}", username="new_guy", first_name="<b>Eve</b>")
    deliver(gated_bot, tg)
    assert recorder.calls == []
    user = users.get(STRANGER)
    assert user is not None and user.state == "pending"
    assert tg.texts(STRANGER) == [auth.WELCOME_TEXT]
    notice = tg.sent(ADMIN)
    assert len(notice) == 1 and f"/approve {STRANGER}" in notice[0].plain
    assert "<b>Eve</b>" in notice[0].plain  # escaped, shown verbatim (F91)


def test_a_second_message_of_a_pending_sender_does_not_notify_the_admin_again(
    gated_bot: Bot, tg: FakeTelegram
):
    tg.message(STRANGER, "hi")
    tg.message(STRANGER, "hello?")
    deliver(gated_bot, tg)
    assert len(tg.sent(ADMIN)) == 1
    # The welcome starts the quiet interval: the second message gets no reply at all.
    assert tg.texts(STRANGER) == [auth.WELCOME_TEXT]


def test_new_user_notices_are_rate_limited(gated_bot: Bot, tg: FakeTelegram):
    for offset in range(auth.NOTICE_LIMIT + 5):
        tg.message(100_000 + offset, "hi")
    deliver(gated_bot, tg)
    assert len(tg.sent(ADMIN)) == auth.NOTICE_LIMIT


@pytest.mark.parametrize(
    ("state", "text"), [("pending", auth.PENDING_TEXT), ("denied", auth.DENIED_TEXT)]
)
@pytest.mark.parametrize("name", USER_COMMANDS + ADMIN_COMMANDS + ["", "plain text"])
def test_pending_and_denied_users_get_a_fixed_reply(
    gated_bot: Bot,
    tg: FakeTelegram,
    users: UsersRepo,
    recorder: Recorder,
    state: str,
    text: str,
    name: str,
):
    add_user(users, USER, state)
    before = snapshot(users)
    tg.message(USER, f"/{name}" if name and name != "plain text" else (name or "hi"))
    deliver(gated_bot, tg)
    assert recorder.calls == []
    assert tg.texts(USER) == [text]
    assert snapshot(users) == before


@pytest.mark.parametrize("state", ["pending", "denied"])
def test_pending_and_denied_documents_are_not_downloaded(
    gated_bot: Bot, tg: FakeTelegram, users: UsersRepo, state: str
):
    add_user(users, USER, state)
    tg.document(USER, "statement.csv", b"Type,Product\n")
    deliver(gated_bot, tg)
    assert tg.calls_of("getFile") == []


@pytest.mark.parametrize("name", USER_COMMANDS + ADMIN_COMMANDS)
def test_ready_user_runs_user_commands_but_admin_commands_do_not_exist(
    gated_bot: Bot, tg: FakeTelegram, users: UsersRepo, recorder: Recorder, name: str
):
    ready_user(users)
    tg.message(USER, f"/{name}")
    deliver(gated_bot, tg)
    if name in ADMIN_COMMANDS:
        assert recorder.calls == []
        assert tg.texts(USER) == [auth.UNKNOWN_COMMAND]
    else:
        assert recorder.calls == [(name, USER)]


@pytest.mark.parametrize("state", ["awaiting_token", "awaiting_budget", "awaiting_account"])
@pytest.mark.parametrize("name", USER_COMMANDS)
def test_onboarding_users_only_get_onboarding_commands(
    gated_bot: Bot, tg: FakeTelegram, users: UsersRepo, recorder: Recorder, state: str, name: str
):
    add_user(users, USER, state)
    tg.message(USER, f"/{name}")
    deliver(gated_bot, tg)
    if name in SETUP_ALLOWED:
        assert recorder.calls == [(name, USER)]
    else:
        assert recorder.calls == []
        assert tg.texts(USER) == [auth.refusal_for_state(state)]


@pytest.mark.parametrize(
    "state", ["awaiting_crypto_account", "awaiting_crypto_btc", "awaiting_crypto_eth"]
)
@pytest.mark.parametrize("name", USER_COMMANDS)
def test_crypto_setup_users_get_finish_or_cancel(
    gated_bot: Bot, tg: FakeTelegram, users: UsersRepo, recorder: Recorder, state: str, name: str
):
    from ynab_helper.bot.handlers._common import new_flow

    ready_user(users, state=state, temp_data=new_flow("crypto", accounts=[]))
    tg.message(USER, f"/{name}")
    deliver(gated_bot, tg)
    if name in CRYPTO_ALLOWED:
        assert recorder.calls == [(name, USER)]
    else:
        assert recorder.calls == []
        assert "crypto setup" in tg.texts(USER)[-1]


@pytest.mark.parametrize("name", ADMIN_COMMANDS)
@pytest.mark.parametrize("admin_state", [None, "pending", "denied", "awaiting_token", "ready"])
def test_admin_commands_work_for_the_admin_in_any_state(
    gated_bot: Bot,
    tg: FakeTelegram,
    users: UsersRepo,
    recorder: Recorder,
    name: str,
    admin_state: str | None,
):
    if admin_state is not None:
        add_user(users, ADMIN, admin_state)
    tg.message(ADMIN, f"/{name.upper()}@test_bot 123")
    deliver(gated_bot, tg)
    assert recorder.calls == [(name, ADMIN)]
    if admin_state is None:
        assert users.get(ADMIN) is None  # an admin command never registers anyone


@pytest.mark.parametrize("text", ["/ipfoo", "/users2", "/approveme", "/logs_all", "/ip-x"])
def test_admin_commands_match_exactly(
    gated_bot: Bot, tg: FakeTelegram, users: UsersRepo, recorder: Recorder, text: str
):
    ready_user(users, ADMIN)
    tg.message(ADMIN, text)
    deliver(gated_bot, tg)
    assert recorder.calls == []
    assert tg.texts(ADMIN) == [auth.UNKNOWN_COMMAND]


@pytest.mark.parametrize("chat_type", ["group", "supergroup", "channel"])
@pytest.mark.parametrize("sender", [USER, ADMIN, STRANGER])
def test_F56_group_messages_are_ignored_and_nothing_is_written(
    gated_bot: Bot,
    tg: FakeTelegram,
    users: UsersRepo,
    recorder: Recorder,
    chat_type: str,
    sender: int,
):
    ready_user(users, USER)
    ready_user(users, ADMIN)
    before = snapshot(users)
    group = -100123
    tg.message(sender, "/login@test_bot", chat_id=group, chat_type=chat_type)
    tg.message(sender, "/status", chat_id=group, chat_type=chat_type)
    tg.document(sender, "s.csv", b"x", chat_id=group, chat_type=chat_type)
    deliver(gated_bot, tg)
    assert recorder.calls == []
    assert snapshot(users) == before  # no pending row, chat_id not overwritten
    assert tg.texts(group) == [auth.DIRECT_ONLY]  # once per group
    assert tg.sent(sender) == [] and tg.sent(ADMIN) == []
    assert tg.calls_of("getFile") == []


def test_F56_private_chat_must_be_the_senders_own(
    gated_bot: Bot, tg: FakeTelegram, users: UsersRepo, recorder: Recorder
):
    ready_user(users)
    tg.message(USER, "/status", chat_id=USER + 1, chat_type="private")
    deliver(gated_bot, tg)
    assert recorder.calls == []


def test_messages_from_bots_are_ignored(gated_bot: Bot, tg: FakeTelegram, users: UsersRepo):
    tg.message(STRANGER, "/start", is_bot=True)
    deliver(gated_bot, tg)
    assert users.get(STRANGER) is None
    assert tg.sent() == []


# ── callbacks ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("data", CALLBACKS)
@pytest.mark.parametrize("state", [None, "pending", "denied"])
def test_F03_callbacks_of_unknown_pending_denied_users_are_refused(
    gated_bot: Bot,
    tg: FakeTelegram,
    users: UsersRepo,
    recorder: Recorder,
    data: str,
    state: str | None,
):
    if state is not None:
        add_user(users, USER, state)
    before = snapshot(users)
    message_id = keyboard_message(gated_bot, USER)
    tg.callback(USER, data, message_id=message_id)
    deliver(gated_bot, tg)
    assert recorder.calls == []
    assert snapshot(users) == before  # never registers, re-onboards or un-denies
    assert [a.get("text") for a in tg.answers] == [auth.NOT_AUTHORIZED]
    assert tg.message_state(USER, message_id).reply_markup == {"inline_keyboard": []}


@pytest.mark.parametrize("data", CALLBACKS)
def test_F56_callbacks_in_groups_are_refused(
    gated_bot: Bot, tg: FakeTelegram, users: UsersRepo, recorder: Recorder, data: str
):
    ready_user(users)
    before = snapshot(users)
    group = -100777
    message_id = keyboard_message(gated_bot, group)
    tg.callback(USER, data, message_id=message_id, chat_id=group, chat_type="supergroup")
    deliver(gated_bot, tg)
    assert recorder.calls == []
    assert snapshot(users) == before
    assert len(tg.answers) == 1  # always answered
    assert tg.message_state(group, message_id).reply_markup == {"inline_keyboard": []}


ROUTE_STATES = {route.prefix: route.states for route in REGISTRY.callbacks}


@pytest.mark.parametrize("data", CALLBACKS)
@pytest.mark.parametrize(
    "state",
    ["ready", "awaiting_token", "awaiting_budget", "awaiting_account", "awaiting_crypto_btc"],
)
def test_callbacks_run_only_in_their_states(
    gated_bot: Bot,
    tg: FakeTelegram,
    users: UsersRepo,
    recorder: Recorder,
    data: str,
    state: str,
):
    from ynab_helper.bot.handlers._common import new_flow

    temp = new_flow("crypto", accounts=[]) if state.startswith("awaiting_crypto") else None
    ready_user(users, state=state, temp_data=temp)
    message_id = keyboard_message(gated_bot, USER)
    tg.callback(USER, data, message_id=message_id)
    deliver(gated_bot, tg)
    prefix = data.split(":", 1)[0]
    assert len(tg.answers) == 1  # answered exactly once
    if prefix in ROUTE_STATES and state in ROUTE_STATES[prefix]:
        assert recorder.calls == [(prefix, USER)]
    else:
        assert recorder.calls == []
        if prefix in ROUTE_STATES:  # a known button pressed in the wrong state expires
            assert tg.message_state(USER, message_id).reply_markup == {"inline_keyboard": []}


def test_F03_denied_user_cannot_reonboard_with_a_stale_settings_button(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    """v1: a denied user tapped 'Re-run YNAB setup' and was back in onboarding."""
    ready_user(users)
    tg.message(USER, "/settings")
    deliver(bot, tg)
    menu = tg.last(USER)
    assert "st:setup" in menu.button_data()
    tg.message(ADMIN, f"/deny {USER}")
    deliver(bot, tg)  # the admin row does not exist: admin commands still work
    assert users.get(USER).state == "denied"
    tg.callback(USER, "st:setup", message_id=menu.message_id)
    deliver(bot, tg)
    assert users.get(USER).state == "denied"
    assert tg.answers[-1]["text"] == auth.NOT_AUTHORIZED


def test_F03_unknown_sender_cannot_register_through_a_button(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    for data in ("st:setup", "st:auto:1", "st:crypto"):
        tg.callback(STRANGER, data)
    deliver(bot, tg)
    assert users.get(STRANGER) is None
    assert [a["text"] for a in tg.answers] == [auth.NOT_AUTHORIZED] * 3


# ── floods by senders who are not approved ───────────────────────────────────


def clocked_bot(
    tg: FakeTelegram, users: UsersRepo, data_dir: Path, now: list[float], sleeps: list[float]
) -> Bot:
    """A bot whose gate runs on ``now`` and whose client records every 429 wait."""
    client = TelegramClient(tg.token, api_base=tg.base_url, opener=tg.opener, sleep=sleeps.append)
    return Bot(
        telegram=client,
        users=users,
        admin_id=ADMIN,
        data_dir=data_dir,
        workers=WorkerPool(0),
        clock=lambda: now[0],
        hard_exit=lambda code: None,
    )


@pytest.mark.parametrize(
    ("state", "text"), [("pending", auth.PENDING_TEXT), ("denied", auth.DENIED_TEXT)]
)
def test_F03_refusals_are_sent_once_per_interval_per_sender(
    tg: FakeTelegram, users: UsersRepo, data_dir: Path, state: str, text: str
):
    now, sleeps = [1000.0], []
    bot = clocked_bot(tg, users, data_dir, now, sleeps)
    add_user(users, USER, state)
    for _ in range(30):  # e.g. a batch of forwarded messages
        tg.message(USER, "/status")
    deliver(bot, tg)
    assert tg.texts(USER) == [text]
    now[0] += auth.REFUSAL_INTERVAL - 1
    say(bot, tg, USER, "hello?")
    assert tg.texts(USER) == [text]
    now[0] += 2
    say(bot, tg, USER, "hello?")
    assert tg.texts(USER) == [text, text]


def test_F03_a_new_sender_gets_the_welcome_once_then_silence(
    tg: FakeTelegram, users: UsersRepo, data_dir: Path
):
    now, sleeps = [1000.0], []
    bot = clocked_bot(tg, users, data_dir, now, sleeps)
    for _ in range(20):
        tg.message(STRANGER, "hi")
    deliver(bot, tg)
    assert tg.texts(STRANGER) == [auth.WELCOME_TEXT]
    assert len(tg.sent(ADMIN)) == 1
    now[0] += auth.REFUSAL_INTERVAL
    say(bot, tg, STRANGER, "anyone?")
    assert tg.texts(STRANGER) == [auth.WELCOME_TEXT, auth.PENDING_TEXT]


def test_F03_a_refusal_flood_under_429_never_stalls_the_dispatcher(
    tg: FakeTelegram, users: UsersRepo, data_dir: Path
):
    """v2 waits out a 429 once; on the poll thread that would let any stranger stall the
    bot for everyone. Refusals, admin notices, callback answers and keyboard removals for
    senders who are not approved never wait."""
    now, sleeps = [1000.0], []
    bot = clocked_bot(tg, users, data_dir, now, sleeps)
    add_user(users, USER, "pending")
    add_user(users, USER + 1, "denied")
    stale = keyboard_message(bot, USER + 1)  # a button from before the denial
    for method in ("sendMessage", "answerCallbackQuery", "editMessageReplyMarkup"):
        tg.fail_next(method, 429, "Too Many Requests: retry after 30", retry_after=30, times=10_000)
    for _ in range(50):
        tg.message(USER, "/status")
        tg.message(USER + 1, "/import")
        tg.message(STRANGER, "hello")
        tg.callback(USER + 1, "st:setup", message_id=stale)
    before = len(tg.calls_of("sendMessage"))
    started = time.monotonic()
    deliver(bot, tg)
    assert time.monotonic() - started < 5
    assert sleeps == []  # not a single 429 wait on the dispatcher thread
    targets = sorted(c["chat_id"] for c in tg.calls_of("sendMessage")[before:])
    assert targets == sorted([USER, USER + 1, STRANGER, ADMIN])  # one each, no retries
    assert users.get(USER + 1).state == "denied"
    assert users.get(STRANGER).state == "pending"


def test_approved_users_still_get_their_replies_after_a_429(
    tg: FakeTelegram, users: UsersRepo, data_dir: Path
):
    """Only best-effort sends skip the wait: a ready user's reply is retried once."""
    now, sleeps = [1000.0], []
    bot = clocked_bot(tg, users, data_dir, now, sleeps)
    ready_user(users)
    tg.fail_next("sendMessage", 429, "Too Many Requests: retry after 3", retry_after=3)
    say(bot, tg, USER, "/help")
    assert sleeps == [3]
    assert "Revolut → YNAB Bot" in tg.texts(USER)[-1]
