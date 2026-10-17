"""TelegramClient: chunking by UTF-16 units, HTML, errors, 429, downloads, documents."""

from __future__ import annotations

import os
import random
import stat
from pathlib import Path

import pytest

from tests.fakes.fake_telegram import FakeTelegram, check_html
from ynab_helper.bot.telegram import (
    MAX_MESSAGE_UNITS,
    DownloadTooLarge,
    TelegramClient,
    button,
    fit_message,
    html_to_plain,
    inline_keyboard,
    split_message,
    utf16_len,
)
from ynab_helper.errors import TelegramError


def make_client(tg: FakeTelegram, sleeps: list[float] | None = None) -> TelegramClient:
    record = sleeps if sleeps is not None else []
    return TelegramClient(tg.token, api_base=tg.base_url, opener=tg.opener, sleep=record.append)


# ── text helpers ─────────────────────────────────────────────────────────────


def test_utf16_len_counts_surrogate_pairs():
    assert utf16_len("abc") == 3
    assert utf16_len("💰") == 2  # outside the BMP: two UTF-16 code units
    assert utf16_len("é") == 1


def test_split_message_keeps_short_text_whole():
    assert split_message("hello\nworld") == ["hello\nworld"]
    assert split_message("") == []


def test_split_message_by_utf16_units_on_line_boundaries():
    # Each line: 100 emoji = 200 UTF-16 units but only 100 code points. A splitter that
    # counted code points would put 40 lines (8000 units) into one message.
    line = "💰" * 100
    text = "\n".join(f"{line}{i:03d}" for i in range(60))
    chunks = split_message(text)
    assert len(chunks) > 1
    assert all(utf16_len(c) <= MAX_MESSAGE_UNITS for c in chunks)
    assert "\n".join(chunks).split("\n") == text.split("\n")  # nothing lost, order kept


def test_split_message_cuts_an_overlong_line_into_valid_escaped_html():
    hostile = "<b>" + "a&b<c>" * 2000 + "</b>"
    chunks = split_message(hostile, limit=500)
    assert all(utf16_len(c) <= 500 for c in chunks)
    plain = "".join(check_html(c) for c in chunks)  # every piece is valid Telegram HTML
    assert plain == html_to_plain(hostile)


def test_split_message_never_returns_a_whitespace_only_chunk(tg: FakeTelegram):
    """A cut inside a long run of spaces left a chunk Telegram rejects as empty, and the
    rest of the message was lost."""
    text = "a" * MAX_MESSAGE_UNITS + " " * MAX_MESSAGE_UNITS + "b\nnext line"
    chunks = split_message(text)
    assert chunks and all(chunk.strip() for chunk in chunks)
    make_client(tg).send_message(1, text)
    assert "".join(tg.texts(1)).replace(" ", "").replace("\n", "") == "a" * 4096 + "bnextline"


def test_split_message_fuzz_chunks_are_valid_nonempty_and_short():
    rng = random.Random(20261017)
    alphabet = ["a", " ", "  ", "\n", "&", "<", ">", "💰", "é", "<b>x</b>", "\t", "\xa0"]
    for _ in range(200):
        text = "".join(rng.choice(alphabet) for _ in range(rng.randint(1, 3000)))
        html_text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        limit = rng.choice([7, 64, 500, MAX_MESSAGE_UNITS])
        for chunk in split_message(html_text, limit=limit):
            assert chunk.strip()
            assert utf16_len(chunk) <= limit
            check_html(chunk)


def test_fit_message_cuts_on_a_line_boundary():
    text = "\n".join("x" * 100 for _ in range(100))
    fitted = fit_message(text)
    assert utf16_len(fitted) <= MAX_MESSAGE_UNITS
    assert fitted.endswith("…")
    assert all(line in ("x" * 100, "…") for line in fitted.split("\n"))


def test_button_rejects_callback_data_over_64_bytes():
    with pytest.raises(ValueError):
        button("x", "d" * 65)
    assert button("x", "d" * 64)["callback_data"] == "d" * 64


# ── sending ──────────────────────────────────────────────────────────────────


def test_send_message_uses_html_and_disables_previews(tg: FakeTelegram):
    client = make_client(tg)
    message = client.send_message(5, "<b>hi</b> &amp; bye")
    assert message is not None and message["message_id"]
    params = tg.calls_of("sendMessage")[0]
    assert params["parse_mode"] == "HTML"
    assert params["link_preview_options"] == {"is_disabled": True}
    assert tg.last(5).plain == "hi & bye"


def test_long_messages_are_chunked_and_the_keyboard_goes_on_the_last_chunk(tg: FakeTelegram):
    client = make_client(tg)
    text = "\n".join(f"<b>line {i}</b> " + "é" * 80 for i in range(200))
    markup = inline_keyboard([[button("OK", "x:1")]])
    client.send_message(5, text, reply_markup=markup)
    sent = tg.sent(5)
    assert len(sent) >= 4
    assert all(m.reply_markup is None for m in sent[:-1])
    assert sent[-1].reply_markup == markup
    assert "\n".join(m.plain for m in sent) == html_to_plain(text)


def test_html_rejected_by_telegram_is_resent_as_plain_text(tg: FakeTelegram):
    client = make_client(tg)
    client.send_message(5, "1 < 2 & <unknown>tag</unknown>")
    sent = tg.sent(5)
    assert len(sent) == 1
    assert sent[0].parse_mode is None
    assert sent[0].text == "1 < 2 & tag"
    assert len(tg.html_errors) == 1
    tg.html_errors.clear()  # deliberate


def test_429_is_waited_out_once(tg: FakeTelegram):
    sleeps: list[float] = []
    client = make_client(tg, sleeps)
    tg.fail_next("sendMessage", 429, "Too Many Requests: retry after 3", retry_after=3)
    client.send_message(5, "hello")
    assert sleeps == [3]
    assert tg.texts(5) == ["hello"]


def test_a_second_429_is_raised(tg: FakeTelegram):
    sleeps: list[float] = []
    client = make_client(tg, sleeps)
    tg.fail_next("sendMessage", 429, "Too Many Requests", retry_after=2, times=2)
    with pytest.raises(TelegramError) as info:
        client.send_message(5, "hello")
    assert info.value.error_code == 429 and info.value.retry_after == 2
    assert sleeps == [2]


def test_a_long_retry_after_is_not_waited(tg: FakeTelegram):
    sleeps: list[float] = []
    client = make_client(tg, sleeps)
    tg.fail_next("sendMessage", 429, "Too Many Requests", retry_after=31)
    with pytest.raises(TelegramError):
        client.send_message(5, "hello")
    assert sleeps == []


def test_errors_are_typed_and_never_contain_the_token(tg: FakeTelegram):
    client = make_client(tg)
    tg.fail_next("sendMessage", 400, "Bad Request: chat not found")
    with pytest.raises(TelegramError) as info:
        client.send_message(5, "hello")
    assert info.value.error_code == 400
    assert info.value.description == "Bad Request: chat not found"
    assert tg.token not in str(info.value) and tg.token not in info.value.user_message

    tg.network_error_next("getMe")
    with pytest.raises(TelegramError) as info:
        client.get_me()
    assert info.value.retriable
    assert tg.token not in str(info.value)
    assert tg.token not in repr(client)


def test_send_message_is_not_retried_on_a_server_error(tg: FakeTelegram):
    client = make_client(tg)
    tg.fail_next("sendMessage", 502, "Bad Gateway")
    with pytest.raises(TelegramError):
        client.send_message(5, "hello")
    assert len(tg.calls_of("sendMessage")) == 1  # non-idempotent: never resent blindly


def test_edit_not_modified_is_success(tg: FakeTelegram):
    client = make_client(tg)
    message = client.send_message(5, "same")
    assert message is not None
    assert client.edit_message_text(5, message["message_id"], "same") is None
    client.edit_message_reply_markup(5, message["message_id"])  # no keyboard to remove


def test_get_updates_sends_offset_and_allowed_updates(tg: FakeTelegram):
    client = make_client(tg)
    first = tg.message(7, "a")
    tg.message(7, "b")
    updates = client.get_updates(None, timeout=0)
    assert [u["update_id"] for u in updates] == [first, first + 1]
    assert client.get_updates(first + 1, timeout=0)[0]["update_id"] == first + 1
    params = tg.calls_of("getUpdates")[-1]
    assert params["offset"] == first + 1
    assert params["allowed_updates"] == ["message", "callback_query"]


# ── files ────────────────────────────────────────────────────────────────────


def test_download_file_is_private_and_atomic(tg: FakeTelegram, tmp_path: Path):
    client = make_client(tg)
    tg.add_file("f1", b"a,b\n1,2\n")
    info = client.get_file("f1")
    path = client.download_file(info["file_path"], tmp_path / "dl", suffix=".csv")
    assert path.read_bytes() == b"a,b\n1,2\n"
    assert path.suffix == ".csv"
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    assert [p.name for p in (tmp_path / "dl").iterdir()] == [path.name]


def test_download_file_enforces_the_size_cap_and_leaves_nothing(tg: FakeTelegram, tmp_path: Path):
    client = make_client(tg)
    tg.add_file("big", b"x" * 5000)
    info = client.get_file("big")
    with pytest.raises(DownloadTooLarge):
        client.download_file(info["file_path"], tmp_path, max_bytes=1000)
    assert list(tmp_path.iterdir()) == []


def test_download_file_rejects_path_tricks(tg: FakeTelegram, tmp_path: Path):
    client = make_client(tg)
    for bad in ("../secret", "/etc/passwd", "a/../../b", "x?y=1", ""):
        with pytest.raises(TelegramError):
            client.download_file(bad, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_download_of_a_missing_file_fails_cleanly(tg: FakeTelegram, tmp_path: Path):
    client = make_client(tg)
    with pytest.raises(TelegramError) as info:
        client.download_file("documents/nope.csv", tmp_path)
    assert info.value.error_code == 404
    assert list(tmp_path.iterdir()) == []


def test_send_document_uploads_multipart(tg: FakeTelegram):
    client = make_client(tg)
    client.send_document(5, "bot.log", b"line 1\nline 2\n", caption="📜 <b>bot.log</b>")
    assert tg.documents == [
        {
            "chat_id": 5,
            "filename": "bot.log",
            "content": b"line 1\nline 2\n",
            "caption": "📜 <b>bot.log</b>",
        }
    ]


def test_the_loopback_server_speaks_real_http(tg: FakeTelegram):
    base = tg.start()
    try:
        client = TelegramClient(tg.token, api_base=base)
        assert client.get_me()["username"] == tg.username
        client.send_message(9, "over <b>HTTP</b>")
        assert tg.texts(9) == ["over HTTP"]
    finally:
        tg.stop()
