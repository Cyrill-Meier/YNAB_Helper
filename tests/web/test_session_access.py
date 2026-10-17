"""Which bot states keep a web session usable (DESIGN §9.1; F18, R05).

``/crypto_setup`` leaves token, budget and account alone: the web UI keeps working. The
main ``/setup`` keeps the previous configuration until the new one is committed: the web UI
is paused (session row and cookie kept), and ``/cancel`` resumes it. ``/deny`` and an
incomplete configuration still end the session (F18).
"""

from __future__ import annotations

from collections.abc import Iterator
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_telegram import FakeTelegram
from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.bot.app import Bot, WebAccess
from ynab_helper.bot.telegram import TelegramClient
from ynab_helper.bot.users import UsersRepo
from ynab_helper.bot.workers import WorkerPool
from ynab_helper.sessions import SESSION_COOKIE
from ynab_helper.storage import UserStore, connect, open_users_db

from .conftest import BUDGET, USER_ID, TestClient, issue_token, login

ADMIN = 1001
CRYPTO = "acct-crypto"


def _session_rows(users_db: Path) -> list[tuple[Any, ...]]:
    conn = connect(users_db)
    try:
        rows = conn.execute(
            "SELECT token_hash, expires_at FROM web_sessions WHERE kind = 'session'"
        ).fetchall()
        return [tuple(r) for r in rows]
    finally:
        conn.close()


def _set(users_db: Path, **fields: Any) -> None:
    conn = connect(users_db)
    try:
        UserStore(conn).upsert(USER_ID, **fields)
    finally:
        conn.close()


def _state(users_db: Path) -> str:
    conn = connect(users_db)
    try:
        user = UserStore(conn).get(USER_ID)
        assert user is not None
        return user.state
    finally:
        conn.close()


def _cookie_changes(response: Any) -> SimpleCookie:
    cookie = SimpleCookie()
    for header in response.headers.get_list("set-cookie"):
        cookie.load(header)
    return cookie


def _assert_paused(client: TestClient, users_db: Path) -> None:
    """Every kind of request is refused, nothing signs the browser out."""
    rows = _session_rows(users_db)
    api = client.get("/api/me")
    assert api.status_code == 401
    assert api.json()["error"] == "setup_in_progress"
    assert "/cancel" in api.json()["message"]
    page = client.get("/app/dashboard", follow_redirects=False)
    assert (page.status_code, page.headers["location"]) == (303, "/")
    root = client.get("/", follow_redirects=False)
    assert root.status_code == 200
    assert "setup is in progress" in root.text and "/cancel" in root.text
    assert 'href="/"' in root.text
    assert "Send <code>/login</code>" not in root.text  # the bot refuses /login now
    # Login page shown for a stale cookie says "expired"; this one must not.
    assert "Your session has expired" not in root.text
    for response in (api, page, root):
        assert SESSION_COOKIE not in _cookie_changes(response), "cookie must be kept"
    assert client.cookies.get(SESSION_COOKIE)
    assert _session_rows(users_db) == rows  # neither deleted nor slid


# ── state changes as the bot makes them ─────────────────────────────────────


@pytest.mark.parametrize(
    "state", ["awaiting_crypto_account", "awaiting_crypto_btc", "awaiting_crypto_eth"]
)
def test_web_R05_crypto_setup_keeps_the_web_session(
    client: TestClient, users_db, config, clock, state
):
    """v2 before R05: one web request during /crypto_setup deleted the session for good."""
    login(client, users_db, config, clock)
    assert client.get("/api/me").status_code == 200
    _set(users_db, state=state)
    clock.advance(60)
    me = client.get("/api/me")
    assert me.status_code == 200 and me.json()["telegram_id"] == USER_ID
    assert client.get("/app/dashboard", follow_redirects=False).status_code == 200
    _set(users_db, state="ready")  # finished or cancelled
    assert client.get("/api/me").status_code == 200
    assert len(_session_rows(users_db)) == 1


def test_web_R05_sign_in_works_during_the_crypto_setup(client: TestClient, users_db, config, clock):
    _set(users_db, state="awaiting_crypto_account")
    login(client, users_db, config, clock)
    assert client.get("/api/me").status_code == 200


@pytest.mark.parametrize("state", ["awaiting_token", "awaiting_budget", "awaiting_account"])
def test_web_R05_setup_pauses_the_web_ui_and_cancel_resumes_it(
    client: TestClient, users_db, config, clock, state
):
    login(client, users_db, config, clock)
    assert client.get("/api/me").status_code == 200
    _set(users_db, state=state)  # /setup keeps the previous configuration
    clock.advance(60)
    _assert_paused(client, users_db)
    _set(users_db, state="ready")  # /cancel
    assert client.get("/api/me").status_code == 200
    root = client.get("/", follow_redirects=False)
    assert (root.status_code, root.headers["location"]) == (303, "/app/dashboard")


def test_web_R05_paused_session_can_still_sign_out(client: TestClient, users_db, config, clock):
    csrf = login(client, users_db, config, clock)
    _set(users_db, state="awaiting_token")
    response = client.post("/logout", headers={"X-CSRF-Token": csrf}, follow_redirects=False)
    assert (response.status_code, response.headers["location"]) == (303, "/")
    assert _session_rows(users_db) == []


def test_web_R05_stale_logout_form_keeps_a_paused_session(
    client: TestClient, users_db, config, clock
):
    """A logout form with a wrong token may be forged: a paused session is kept too."""
    login(client, users_db, config, clock)
    _set(users_db, state="awaiting_budget")
    response = client.post("/logout", data={"csrf_token": "0" * 32}, follow_redirects=False)
    assert (response.status_code, response.headers["location"]) == (303, "/")
    assert SESSION_COOKIE not in _cookie_changes(response)
    assert len(_session_rows(users_db)) == 1


def test_web_R05_sign_in_is_refused_during_the_setup(client: TestClient, users_db, config, clock):
    _set(users_db, state="awaiting_account")
    token = issue_token(users_db, config, now=clock())
    assert client.post("/auth", data={"t": token}, follow_redirects=False).status_code == 400
    assert _session_rows(users_db) == []


def test_web_R05_setup_without_a_complete_configuration_ends_the_session(
    client: TestClient, users_db, config, clock
):
    """F18 still holds: v1's /setup cleared token, budget and account."""
    login(client, users_db, config, clock)
    _set(users_db, state="awaiting_token", ynab_token=None, budget_id=None, account_id=None)
    response = client.get("/api/me")
    assert (response.status_code, response.json()["error"]) == (401, "session_expired")
    assert _session_rows(users_db) == []


# ── end to end with the real bot ────────────────────────────────────────────


@pytest.fixture
def bot_chat(users_db: Path, data_dir: Path, fake: FakeYnab) -> Iterator[tuple[Bot, FakeTelegram]]:
    """The real bot on the web's users DB, behind a fake Telegram (inline workers)."""
    fake.add_account(BUDGET, CRYPTO, "Crypto", type="otherAsset", on_budget=False)
    tg = FakeTelegram()
    conn = open_users_db(users_db, backup_dir=data_dir / "backups", check_same_thread=False)
    _set(users_db, chat_id=USER_ID)

    def no_exit(code: int) -> None:  # pragma: no cover - only reached on a hung test
        raise AssertionError(f"hard exit {code}")

    bot = Bot(
        telegram=TelegramClient(
            tg.token, api_base=tg.base_url, opener=tg.opener, sleep=lambda s: None
        ),
        users=UsersRepo(conn),
        admin_id=ADMIN,
        data_dir=data_dir,
        workers=WorkerPool(0),
        web=WebAccess(),
        hard_exit=no_exit,
    )
    try:
        yield bot, tg
    finally:
        conn.close()


def _say(chat: tuple[Bot, FakeTelegram], text: str) -> str:
    bot, tg = chat
    tg.message(USER_ID, text)
    for update in tg.take_updates():
        bot.handle_update(update)
    return tg.texts(USER_ID)[-1]


@pytest.mark.parametrize("command", ["/crypto_setup", "/setup"])
def test_web_R05_bot_flow_then_cancel_keeps_the_web_sign_in(
    client: TestClient, users_db, config, clock, bot_chat, command
):
    """The reproduction from the review: sign in, start the flow in Telegram, use the web
    UI, send /cancel — the browser is still signed in."""
    login(client, users_db, config, clock)
    assert client.get("/api/me").status_code == 200
    _say(bot_chat, command)
    assert _state(users_db) != "ready"
    during = client.get("/api/me")
    if command == "/crypto_setup":
        assert during.status_code == 200
    else:
        assert (during.status_code, during.json()["error"]) == (401, "setup_in_progress")
    _say(bot_chat, "/cancel")
    assert _state(users_db) == "ready"
    assert client.get("/api/me").status_code == 200
    assert len(_session_rows(users_db)) == 1
