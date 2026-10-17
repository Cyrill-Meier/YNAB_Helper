"""Admin commands: /approve /deny /users /ip /logs /revoke (F18, F91, F93)."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.bot.conftest import ADMIN, USER, add_user, ready_user, say
from tests.fakes.fake_telegram import FakeTelegram, check_html
from ynab_helper import sessions
from ynab_helper.bot.app import Bot
from ynab_helper.bot.handlers.admin import DEFAULT_LOG_LINES, MAX_LOG_LINES, tail_lines
from ynab_helper.bot.handlers.dedupe import MEMORY_KIND
from ynab_helper.bot.users import UsersRepo
from ynab_helper.maintenance.dedupe import DEDUPE_SCANS

SECRET = "z" * 40


def web_rows(users: UsersRepo, telegram_id: int) -> int:
    return users.conn.execute(
        "SELECT COUNT(*) FROM web_sessions WHERE telegram_id = ?", (telegram_id,)
    ).fetchone()[0]


def test_approve_pending_user(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    add_user(users, USER, "pending", first_name="<Ann>")
    say(bot, tg, ADMIN, f"/approve {USER}")
    assert users.get(USER).state == "awaiting_token"
    assert "approved" in tg.texts(ADMIN)[-1] and "<Ann>" in tg.texts(ADMIN)[-1]
    assert "Personal Access Token" in tg.texts(USER)[-1]


def test_F93_approve_does_not_reset_an_onboarded_user(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    say(bot, tg, ADMIN, f"/approve {USER}")
    assert users.get(USER).state == "ready"
    assert "already ready" in tg.texts(ADMIN)[-1]
    assert tg.sent(USER) == []


@pytest.mark.parametrize("args", ["", "abc", "-5", "12x", "9" * 20])
def test_admin_commands_validate_the_id(bot: Bot, tg: FakeTelegram, args: str):
    for command in ("approve", "deny", "revoke"):
        say(bot, tg, ADMIN, f"/{command} {args}".strip())
        reply = tg.texts(ADMIN)[-1]
        assert reply.startswith("Usage") or reply == "Invalid user ID."


def test_unknown_target(bot: Bot, tg: FakeTelegram):
    say(bot, tg, ADMIN, "/approve 777")
    assert tg.texts(ADMIN)[-1] == "User 777 not found."


def test_F18_deny_revokes_sessions_and_forgets_flows(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    hasher = sessions.TokenHasher(SECRET)
    sessions.issue_login_token(users.conn, hasher, USER, ttl_seconds=300, lock=users.lock)
    bot.memory.put(MEMORY_KIND, USER, object())
    DEDUPE_SCANS.register(str(USER), budget_id="b1", account_id="a", candidates=[])
    say(bot, tg, ADMIN, f"/deny {USER}")
    assert users.get(USER).state == "denied"
    assert web_rows(users, USER) == 0
    assert bot.memory.get(MEMORY_KIND, USER) is None
    assert "1 web session" in tg.texts(ADMIN)[-1]
    assert tg.texts(USER)[-1] == "Your access has been denied."


def test_admin_cannot_deny_themselves(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users, ADMIN)
    say(bot, tg, ADMIN, f"/deny {ADMIN}")
    assert users.get(ADMIN).state == "ready"


def test_revoke(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    hasher = sessions.TokenHasher(SECRET)
    for _ in range(2):
        sessions.issue_login_token(users.conn, hasher, USER, ttl_seconds=300, lock=users.lock)
    say(bot, tg, ADMIN, f"/revoke {USER}")
    assert web_rows(users, USER) == 0
    assert "Revoked 2" in tg.texts(ADMIN)[-1]


def test_F91_users_list_escapes_names(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    add_user(
        users, 77, "pending", first_name="[Tap to approve](https://evil.example)", username="a_b_c"
    )
    ready_user(users, account_name="<b>Revolut</b>")
    say(bot, tg, ADMIN, "/users")
    message = tg.last(ADMIN)
    assert "[Tap to approve](https://evil.example)" in message.plain
    assert "@a_b_c" in message.plain and "<b>Revolut</b>" in message.plain
    assert "<a " not in message.text


def test_ip_uses_the_lookup(bot: Bot, tg: FakeTelegram):
    bot.ip_lookup = lambda: "203.0.113.9"
    say(bot, tg, ADMIN, "/ip")
    assert "Public IP: 203.0.113.9" in tg.texts(ADMIN)[-1]


def write_log(path: Path, lines: int, *, start: int = 0) -> None:
    path.write_text(
        "".join(f"line {i} <&>\n" for i in range(start, start + lines)), encoding="utf-8"
    )


def test_logs_sends_the_tail_as_a_document(bot: Bot, tg: FakeTelegram, tmp_path: Path):
    log = tmp_path / "bot.log"
    write_log(log, 1000)
    bot.log_file = log
    say(bot, tg, ADMIN, "/logs")
    [document] = tg.documents
    lines = document["content"].decode().splitlines()
    assert len(lines) == DEFAULT_LOG_LINES and lines[-1] == "line 999 <&>"
    check_html(document["caption"])
    say(bot, tg, ADMIN, "/logs 3")
    assert tg.documents[-1]["content"].decode().splitlines() == [
        "line 997 <&>",
        "line 998 <&>",
        "line 999 <&>",
    ]


def test_logs_reads_rotated_files_and_caps_n(bot: Bot, tg: FakeTelegram, tmp_path: Path):
    log = tmp_path / "bot.log"
    write_log(log.with_name("bot.log.1"), 10, start=0)
    write_log(log, 5, start=10)
    assert [line.strip() for line in tail_lines(log, 8)] == [f"line {i} <&>" for i in range(7, 15)]
    bot.log_file = log
    say(bot, tg, ADMIN, f"/logs {MAX_LOG_LINES * 10}")
    assert len(tg.documents[-1]["content"].decode().splitlines()) == 15
    assert "capped" in tg.documents[-1]["caption"]


@pytest.mark.parametrize("arg", ["0", "-1", "x", "1e5"])
def test_logs_usage(bot: Bot, tg: FakeTelegram, tmp_path: Path, arg: str):
    bot.log_file = tmp_path / "bot.log"
    write_log(bot.log_file, 3)
    say(bot, tg, ADMIN, f"/logs {arg}")
    assert tg.texts(ADMIN)[-1].startswith("Usage")
    assert tg.documents == []


def test_logs_without_a_file(bot: Bot, tg: FakeTelegram, tmp_path: Path):
    bot.log_file = tmp_path / "missing.log"
    say(bot, tg, ADMIN, "/logs")
    assert "No log file" in tg.texts(ADMIN)[-1]


def test_logs_upload_failure_is_reported(bot: Bot, tg: FakeTelegram, tmp_path: Path):
    bot.log_file = tmp_path / "bot.log"
    write_log(bot.log_file, 3)
    tg.fail_next("sendDocument", 413, "Request Entity Too Large")
    say(bot, tg, ADMIN, "/logs")
    assert tg.texts(ADMIN)[-1] == "❌ Failed to send the log: Request Entity Too Large"


def test_F56_dms_never_go_to_a_group_chat_id_stored_by_v1(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    add_user(users, USER, "pending", chat_id=-100555)  # v1 overwrote chat_id from a group
    say(bot, tg, ADMIN, f"/approve {USER}")
    assert tg.sent(-100555) == []
    assert "Personal Access Token" in tg.texts(USER)[-1]
    say(bot, tg, ADMIN, f"/deny {USER}")
    assert tg.sent(-100555) == []
    assert tg.texts(USER)[-1] == "Your access has been denied."


@pytest.mark.parametrize("args", ["٤٢٤٢", "４２４２", "²"])
def test_non_ascii_digits_are_not_ids(bot: Bot, tg: FakeTelegram, users: UsersRepo, args: str):
    add_user(users, USER, "pending")
    say(bot, tg, ADMIN, f"/approve {args}")
    assert tg.texts(ADMIN)[-1] == "Invalid user ID."
    assert users.get(USER).state == "pending"
