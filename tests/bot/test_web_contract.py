"""Bot ↔ web server contract (shared interface of DESIGN §8.1/§9.1, F25, F94).

Starts the real bot (in-process, fake Telegram) with the real web server on a loopback
port: the startup DM says the web UI is enabled, ``/login`` sends a URL button, signing in
through ``GET``/``POST /auth`` deletes the Telegram login message, and stopping the bot
stops the web server.
"""

from __future__ import annotations

import http.cookiejar
import re
import socket
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from tests.bot.conftest import ADMIN, YNAB_TOKEN
from tests.fakes.fake_telegram import FakeTelegram
from ynab_helper.bot import main as bot_main
from ynab_helper.bot.workers import WorkerPool

pytest.importorskip("uvicorn")
pytest.importorskip("ynab_helper.web.server")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for(predicate, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("condition not reached")


def test_login_round_trip_with_the_real_web_server(tmp_path: Path, tg: FakeTelegram):
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = {
        "TELEGRAM_BOT_TOKEN": tg.token,
        "TELEGRAM_ADMIN_ID": str(ADMIN),
        "TELEGRAM_API_BASE": tg.base_url,
        "BOT_DATA_DIR": str(tmp_path / "bot_data"),
        "LOG_FILE": "",
        "HOME": str(tmp_path),
        "YNAB_TOKEN": YNAB_TOKEN,
        "YNAB_BUDGET_ID": "b1",
        "YNAB_ACCOUNT_ID": "acct-main",
        "WEB_UI_ENABLED": "1",
        "WEB_UI_HOST": "127.0.0.1",
        "WEB_UI_PORT": str(port),
        "WEB_UI_PUBLIC_URL": base,
        "WEB_UI_SECRET_KEY": "w" * 40,
    }
    holder: dict[str, Any] = {}

    def target() -> None:
        holder["code"] = bot_main.run(
            environ=env,
            install_signals=False,
            telegram_opener=tg.opener,
            workers=WorkerPool(0),
            hard_exit=lambda code: None,
            on_bot=lambda bot: holder.setdefault("bot", bot),
        )

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    try:
        wait_for(lambda: any("Bot started" in t for t in tg.texts(ADMIN)))
        startup = next(t for t in tg.texts(ADMIN) if "Bot started" in t)
        assert f"web UI: enabled at {base}" in startup

        tg.message(ADMIN, "/login")
        wait_for(lambda: any(m.buttons for m in tg.sent(ADMIN)))
        login = next(m for m in tg.sent(ADMIN) if m.buttons)
        url = login.buttons[0]["url"]
        token = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)["t"][0]
        assert token not in login.text

        browser = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())
        )
        page = browser.open(url, timeout=10).read().decode()
        fields = dict(re.findall(r'<input[^>]*name="([^"]+)"[^>]*value="([^"]*)"', page))
        request = urllib.request.Request(
            f"{base}/auth",
            data=urllib.parse.urlencode(fields or {"t": token}).encode(),
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response = browser.open(request, timeout=10)
        assert response.status == 200 and response.geturl().endswith("/app/dashboard")
        wait_for(lambda: (ADMIN, login.message_id) in tg.deleted, timeout=10)
    finally:
        bot = holder.get("bot")
        if bot is not None:
            bot.request_stop()
        thread.join(15)
    assert holder.get("code") == 0
    with socket.socket() as probe:  # the web server was stopped with the bot
        assert probe.connect_ex(("127.0.0.1", port)) != 0
