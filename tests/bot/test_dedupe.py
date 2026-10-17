"""/dedupe: scan-bound picker, confirmation view, /dedupe_delete parsing (F20, F44, F95)."""

from __future__ import annotations

import itertools
import re
import time
from pathlib import Path

import pytest

from tests.bot.conftest import ACCT, BUDGET, USER, deliver, press, ready_user, say
from tests.fakes.fake_telegram import FakeTelegram
from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.bot.app import Bot
from ynab_helper.bot.handlers.dedupe import SelectionError, parse_selection
from ynab_helper.bot.users import UsersRepo

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
BASIC = (FIXTURES / "export_basic.csv").read_bytes()


_SEQ = itertools.count()


def add_strays(ynab: FakeYnab, count: int, *, payee: str = "Stray") -> list[str]:
    """Importer-looking transactions no statement row accounts for (sorted by date)."""
    ids = []
    for i in range(count):
        n = next(_SEQ)
        day = f"2026-10-{2 + i % 9:02d}"
        ids.append(
            ynab.add_transaction(
                BUDGET,
                ACCT,
                amount=-(1000 + n),
                date=day,
                payee_name=f"{payee} {i}",
                import_id=f"YNAB:-{1000 + n}:{day}:9",
                cleared="cleared",
            )
        )
    return ids


def live_ids(ynab: FakeYnab) -> set[str]:
    return {t["id"] for t in ynab.transactions(BUDGET, account_id=ACCT)}


@pytest.fixture
def imported(bot: Bot, tg: FakeTelegram, users: UsersRepo) -> None:
    ready_user(users)
    tg.document(USER, "statement.csv", BASIC)
    deliver(bot, tg)


def picker(tg: FakeTelegram):
    message = tg.last(USER)
    assert any(d.startswith("dx:") for d in message.button_data())
    return message


def nonce_of(message) -> str:
    return message.button_data()[0].split(":")[1]


@pytest.mark.usefixtures("imported")
def test_pick_confirm_delete(bot: Bot, tg: FakeTelegram, ynab: FakeYnab):
    strays = add_strays(ynab, 2, payee="<b>Stray</b>")
    say(bot, tg, USER, "/dedupe")
    message = picker(tg)
    nonce = nonce_of(message)
    assert "Found 2 transactions" in message.plain
    press(bot, tg, USER, f"dx:{nonce}:t:0", message_id=message.message_id)
    press(bot, tg, USER, f"dx:{nonce}:del", message_id=message.message_id)
    confirm = tg.edits(USER)[-1]
    assert "Confirm deletion" in confirm["plain"]
    assert "<b>Stray</b> 0" in confirm["plain"]  # the selected row is listed, escaped
    assert "<b>Stray</b> 1" not in confirm["plain"]
    press(bot, tg, USER, f"dx:{nonce}:ok", message_id=message.message_id)
    assert strays[0] not in live_ids(ynab) and strays[1] in live_ids(ynab)
    assert tg.edits(USER)[-1]["plain"].startswith("✓ Deleted 1 of 1 transaction")


@pytest.mark.usefixtures("imported")
def test_F20_ok_is_only_accepted_in_the_confirmation_view(
    bot: Bot, tg: FakeTelegram, ynab: FakeYnab
):
    strays = add_strays(ynab, 1)
    say(bot, tg, USER, "/dedupe")
    nonce = nonce_of(picker(tg))
    press(bot, tg, USER, f"dx:{nonce}:t:0")
    press(bot, tg, USER, f"dx:{nonce}:ok")  # forged: no confirmation view shown
    assert strays[0] in live_ids(ynab)
    assert "Delete first" in tg.answers[-1]["text"]


@pytest.mark.usefixtures("imported")
def test_F20_buttons_of_an_older_scan_expire(bot: Bot, tg: FakeTelegram, ynab: FakeYnab):
    """v1: 'Yes, delete' on an old picker deleted the new scan's selection."""
    first = add_strays(ynab, 1, payee="Coffee")
    say(bot, tg, USER, "/dedupe")
    old = picker(tg)
    old_nonce = nonce_of(old)
    press(bot, tg, USER, f"dx:{old_nonce}:t:0", message_id=old.message_id)
    press(bot, tg, USER, f"dx:{old_nonce}:del", message_id=old.message_id)
    add_strays(ynab, 1, payee="Rent")
    say(bot, tg, USER, "/dedupe")
    new = picker(tg)
    press(bot, tg, USER, f"dx:{nonce_of(new)}:t:1", message_id=new.message_id)
    before = live_ids(ynab)
    press(bot, tg, USER, f"dx:{old_nonce}:ok", message_id=old.message_id)
    assert live_ids(ynab) == before and first[0] in before
    assert tg.answers[-1]["text"].startswith("This list has expired")
    assert tg.message_state(USER, old.message_id).reply_markup == {"inline_keyboard": []}


@pytest.mark.parametrize(
    "data", ["dd:ok", "dd:t:0", "dd:all", "dd:del", "dd:c", "dd:p:1", "dd:noop"]
)
@pytest.mark.usefixtures("imported")
def test_legacy_dd_buttons_expire_and_lose_their_keyboard(
    bot: Bot, tg: FakeTelegram, ynab: FakeYnab, data: str
):
    add_strays(ynab, 1)
    say(bot, tg, USER, "/dedupe")
    message = picker(tg)
    before = live_ids(ynab)
    press(bot, tg, USER, data, message_id=message.message_id)
    assert live_ids(ynab) == before
    assert tg.answers[-1]["text"] == "This list has expired — run /dedupe again."
    assert tg.message_state(USER, message.message_id).reply_markup == {"inline_keyboard": []}


@pytest.mark.usefixtures("imported")
def test_paging_and_select_all(bot: Bot, tg: FakeTelegram, ynab: FakeYnab):
    strays = add_strays(ynab, 8)
    say(bot, tg, USER, "/dedupe")
    message = picker(tg)
    nonce = nonce_of(message)
    assert "Page 1/2" in message.plain
    press(bot, tg, USER, f"dx:{nonce}:p:1", message_id=message.message_id)
    assert "Page 2/2" in tg.edits(USER)[-1]["plain"]
    press(bot, tg, USER, f"dx:{nonce}:all", message_id=message.message_id)
    press(bot, tg, USER, f"dx:{nonce}:del", message_id=message.message_id)
    assert "Delete these 8 transactions" in tg.edits(USER)[-1]["plain"]
    press(bot, tg, USER, f"dx:{nonce}:back", message_id=message.message_id)
    press(bot, tg, USER, f"dx:{nonce}:none", message_id=message.message_id)
    press(bot, tg, USER, f"dx:{nonce}:del", message_id=message.message_id)
    assert tg.answers[-1]["text"] == "Nothing selected."
    assert set(strays) <= live_ids(ynab)


@pytest.mark.usefixtures("imported")
def test_cancel_button_and_command(bot: Bot, tg: FakeTelegram, ynab: FakeYnab):
    add_strays(ynab, 1)
    say(bot, tg, USER, "/dedupe")
    message = picker(tg)
    press(bot, tg, USER, f"dx:{nonce_of(message)}:c", message_id=message.message_id)
    assert tg.edits(USER)[-1]["plain"] == "✖ Dedupe cancelled."
    say(bot, tg, USER, "/dedupe_cancel")
    assert tg.texts(USER)[-1].startswith("Nothing pending")


@pytest.mark.usefixtures("imported")
def test_dedupe_delete_text_command_shows_the_confirmation(
    bot: Bot, tg: FakeTelegram, ynab: FakeYnab
):
    strays = add_strays(ynab, 4)
    say(bot, tg, USER, "/dedupe")
    old = picker(tg)
    say(bot, tg, USER, "/dedupe_delete 1,3-4")
    confirm = tg.last(USER)
    assert "Delete these 3 transactions" in confirm.plain
    assert set(strays) <= live_ids(ynab)  # nothing deleted without the confirmation
    assert tg.message_state(USER, old.message_id).reply_markup == {"inline_keyboard": []}
    ok = next(d for d in confirm.button_data() if d.endswith(":ok"))
    press(bot, tg, USER, ok, message_id=confirm.message_id)
    assert len(set(strays) - live_ids(ynab)) == 3


@pytest.mark.usefixtures("imported")
@pytest.mark.parametrize(
    "spec",
    ["1-3000000", "1-99999999999", "0", "5", "2-1", "abc", "1,,x", "1-2-3", ";DROP"],
)
def test_F44_bad_selections_are_rejected_before_expanding(
    bot: Bot, tg: FakeTelegram, ynab: FakeYnab, spec: str
):
    add_strays(ynab, 4)
    say(bot, tg, USER, "/dedupe")
    sent_before = len(tg.sent(USER))
    started = time.perf_counter()
    say(bot, tg, USER, f"/dedupe_delete {spec}")
    assert time.perf_counter() - started < 1.0
    replies = tg.sent(USER)[sent_before:]
    assert len(replies) == 1 and replies[0].plain.startswith("⚠")
    assert len(replies[0].plain) < 200


def test_parse_selection_rules():
    assert parse_selection("1, 3-5", 5) == [1, 3, 4, 5]
    assert parse_selection("2,2,1-2", 5) == [1, 2]
    assert parse_selection("all", 3) == [1, 2, 3]
    assert parse_selection("*", 3) == [1, 2, 3]
    assert parse_selection("1-200", 500) == list(range(1, 201))
    for spec, count in [
        ("all", 201),
        ("1-201", 500),
        ("1-150,151-201", 500),
        ("0", 5),
        ("6", 5),
        ("x", 5),
        ("1" * 600, 5),
        ("1", 0),
    ]:
        with pytest.raises(SelectionError):
            parse_selection(spec, count)


def test_F44_huge_range_is_rejected_without_expanding(monkeypatch: pytest.MonkeyPatch):
    def no_range(*args):  # expansion would call range(); validation must fail first
        raise AssertionError("expanded")

    import ynab_helper.bot.handlers.dedupe as module

    monkeypatch.setattr(module, "range", no_range, raising=False)
    with pytest.raises(SelectionError):
        parse_selection("1-999999", 10)


def test_dedupe_without_a_statement(bot: Bot, tg: FakeTelegram, users: UsersRepo):
    ready_user(users)
    say(bot, tg, USER, "/dedupe")
    assert tg.texts(USER)[-1].startswith("❌")
    say(bot, tg, USER, "/dedupe_delete all")
    assert "Run /dedupe first" in tg.texts(USER)[-1]


def stray_payees(ynab: FakeYnab) -> set[str]:
    return {
        t["payee_name"]
        for t in ynab.transactions(BUDGET, account_id=ACCT)
        if t["payee_name"].startswith("Stray")
    }


def listed(text: str) -> set[str]:
    return set(re.findall(r"Stray \d+", text))


@pytest.mark.usefixtures("imported")
def test_F20_ok_on_an_older_confirmation_never_deletes_another_selection(
    bot: Bot, tg: FakeTelegram, ynab: FakeYnab
):
    """/dedupe_delete posts a new confirmation; if removing the old one's keyboard fails,
    its "Yes, delete" must not delete the *new* selection it never listed."""
    add_strays(ynab, 5)
    say(bot, tg, USER, "/dedupe")
    first = picker(tg)
    nonce = nonce_of(first)
    press(bot, tg, USER, f"dx:{nonce}:t:0", message_id=first.message_id)
    press(bot, tg, USER, f"dx:{nonce}:del", message_id=first.message_id)
    first_listed = listed(tg.edits(USER)[-1]["plain"])
    assert len(first_listed) == 1

    tg.fail_next("editMessageReplyMarkup", 500)  # the old keyboard stays
    say(bot, tg, USER, "/dedupe_delete 2-4")
    second = tg.last(USER)
    second_listed = listed(second.plain)
    assert len(second_listed) == 3 and not first_listed & second_listed
    old_markup = tg.message_state(USER, first.message_id).reply_markup
    assert f"dx:{nonce}:ok" in [
        b["callback_data"] for row in old_markup["inline_keyboard"] for b in row
    ]

    before = stray_payees(ynab)
    press(bot, tg, USER, f"dx:{nonce}:ok", message_id=first.message_id)
    assert stray_payees(ynab) == before  # nothing deleted from the stale confirmation
    assert tg.answers[-1]["text"].startswith("This list has expired")
    assert tg.message_state(USER, first.message_id).reply_markup == {"inline_keyboard": []}

    press(bot, tg, USER, f"dx:{nonce}:ok", message_id=second.message_id)
    assert stray_payees(ynab) == before - second_listed  # exactly what it listed


@pytest.mark.usefixtures("imported")
def test_F20_a_dedupe_delete_whose_confirmation_was_not_sent_changes_nothing(
    bot: Bot, tg: FakeTelegram, ynab: FakeYnab
):
    add_strays(ynab, 3)
    say(bot, tg, USER, "/dedupe")
    first = picker(tg)
    nonce = nonce_of(first)
    press(bot, tg, USER, f"dx:{nonce}:t:0", message_id=first.message_id)
    press(bot, tg, USER, f"dx:{nonce}:del", message_id=first.message_id)
    first_listed = listed(tg.edits(USER)[-1]["plain"])
    tg.fail_next("sendMessage", 500)
    say(bot, tg, USER, "/dedupe_delete 2-3")  # its confirmation never reached the user
    before = stray_payees(ynab)
    press(bot, tg, USER, f"dx:{nonce}:ok", message_id=first.message_id)
    assert stray_payees(ynab) == before - first_listed  # the confirmation that was shown


@pytest.mark.usefixtures("imported")
def test_F20_buttons_on_a_message_that_is_not_the_picker_expire(
    bot: Bot, tg: FakeTelegram, ynab: FakeYnab
):
    add_strays(ynab, 2)
    say(bot, tg, USER, "/dedupe")
    nonce = nonce_of(picker(tg))
    say(bot, tg, USER, "/help")
    other = tg.last(USER).message_id
    press(bot, tg, USER, f"dx:{nonce}:all", message_id=other)  # forged onto another message
    assert tg.answers[-1]["text"].startswith("This list has expired")


@pytest.mark.usefixtures("imported")
def test_bot_R10_dedupe_nothing_found(bot: Bot, tg: FakeTelegram, ynab: FakeYnab):
    """The most common outcome of /dedupe (a clean statement), and an older picker's
    buttons expiring once a scan finds nothing."""
    strays = add_strays(ynab, 1)
    say(bot, tg, USER, "/dedupe")
    old = picker(tg)
    ynab.user_delete_transaction(strays[0])
    say(bot, tg, USER, "/dedupe")
    texts = tg.texts(USER)
    assert texts[-2].startswith("🔎 Looking for stray imports")
    lines = texts[-1].splitlines()
    assert lines[0] == "📅 2026-10-01 → 2026-10-13 · statement.csv"
    match = re.fullmatch(
        r"📋 Statement rows: (\d+) · YNAB transactions in that period: (\d+)", lines[1]
    )
    assert match is not None
    in_window = [
        t
        for t in ynab.transactions(BUDGET, account_id=ACCT)
        if "2026-10-01" <= t["date"] <= "2026-10-13"
    ]
    assert int(match.group(1)) > 10 and int(match.group(2)) == len(in_window)
    assert lines[2:] == ["", "✓ No stray imports found."]
    assert tg.last(USER).reply_markup is None
    press(bot, tg, USER, f"dx:{nonce_of(old)}:t:0", message_id=old.message_id)
    assert tg.answers[-1]["text"] == "This list has expired — run /dedupe again."
