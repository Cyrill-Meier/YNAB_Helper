"""Fixtures for the web backend tests: a users DB with a ready user, the fake YNAB behind
the service layer, the app with a controllable clock, and a signed-in TestClient."""

from __future__ import annotations

import re
import time
import warnings
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FAKE_TOKEN, FakeYnab
from ynab_helper import services, sessions
from ynab_helper.services import UserContext
from ynab_helper.storage import UserStore, connect, open_users_db
from ynab_helper.web.config import WebConfig

with warnings.catch_warnings():
    # starlette.testclient warns (once, at import) that httpx is deprecated for httpx2.
    warnings.simplefilter("ignore")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

from ynab_helper.web.app import make_app

USER_ID = 42
OTHER_ID = 43
BUDGET = "b1"
ACCT = "acct-main"
OTHER = "acct-other"
SECRET = "web-test-secret-key-0123456789abcdefghij"
T0 = 1_792_000_000.0  # 2026-10-15T… UTC; tests move it forward
FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
CSRF_RE = re.compile(r'<meta name="csrf-token" content="([0-9a-f]+)">')


class Clock:
    """A settable epoch clock (``clock()`` → seconds)."""

    def __init__(self, now: float = T0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def fake() -> FakeYnab:
    f = FakeYnab()
    f.add_budget(BUDGET, currency="CHF")
    f.add_account(BUDGET, ACCT, "Revolut")
    f.add_account(BUDGET, OTHER, "Visa", type="creditCard")
    return f


@pytest.fixture(autouse=True)
def _fake_clients(monkeypatch: pytest.MonkeyPatch, fake: FakeYnab) -> None:
    monkeypatch.setattr(services, "client_factory", lambda ctx: fake.client(max_retries=0))
    monkeypatch.setattr(
        services, "token_client_factory", lambda token: fake.client(token=token, max_retries=0)
    )


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    path = tmp_path / "bot_data"
    path.mkdir()
    return path


@pytest.fixture
def users_db(data_dir: Path) -> Path:
    path = data_dir / "bot_users.db"
    conn = open_users_db(path, backup_dir=data_dir / "backups")
    try:
        make_ready(conn, USER_ID)
    finally:
        conn.close()
    return path


def make_ready(conn: Any, telegram_id: int, **fields: Any) -> None:
    values: dict[str, Any] = {
        "state": "ready",
        "ynab_token": FAKE_TOKEN,
        "budget_id": BUDGET,
        "budget_name": "Budget b1",
        "account_id": ACCT,
        "account_name": "Revolut",
        "first_name": "Ann",
        "username": "ann",
    }
    values.update(fields)
    UserStore(conn).upsert(telegram_id, **values)


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def config() -> WebConfig:
    return WebConfig(
        enabled=True,
        public_url="http://testserver",
        secret_key=SECRET,
        login_ttl=300,
        session_ttl=1800,
        session_absolute_ttl=43200,
    )


@dataclass
class Deleted:
    calls: list[tuple[int, int]] = field(default_factory=list)

    def __call__(self, chat_id: int, message_id: int) -> None:
        self.calls.append((chat_id, message_id))


@pytest.fixture
def deleted() -> Deleted:
    return Deleted()


@pytest.fixture
def app(
    config: WebConfig, users_db: Path, data_dir: Path, clock: Clock, deleted: Deleted
) -> Iterator[FastAPI]:
    application = make_app(
        config,
        users_db_path=users_db,
        data_dir=data_dir,
        version_line="v2.0.0 (test)",
        cache_bust="2.0.0-test",
        delete_telegram_message=deleted,
        clock=clock,
    )
    yield application
    state = application.state.web
    state.syncer.close()
    state.syncer.wait_idle(str(USER_ID), 5)


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    with TestClient(app) as c:
        yield c


def ctx_for(data_dir: Path, telegram_id: int = USER_ID) -> UserContext:
    return UserContext.from_user_row(
        {
            "telegram_id": telegram_id,
            "ynab_token": FAKE_TOKEN,
            "budget_id": BUDGET,
            "account_id": ACCT,
        },
        data_dir,
    )


def issue_token(
    users_db: Path,
    config: WebConfig,
    telegram_id: int = USER_ID,
    *,
    now: float = T0,
    chat_id: int | None = None,
    message_id: int | None = None,
) -> str:
    conn = connect(users_db)
    try:
        token = sessions.issue_login_token(
            conn, config.token_hasher(), telegram_id, ttl_seconds=config.login_ttl, now=now
        )
        if chat_id is not None:
            sessions.attach_tg_message(
                conn, config.token_hasher(), token, chat_id=chat_id, message_id=message_id
            )
        return token
    finally:
        conn.close()


def login(client: TestClient, users_db: Path, config: WebConfig, clock: Clock, **kw: Any) -> str:
    """Sign in through POST /auth; returns the CSRF token of the new session."""
    token = issue_token(users_db, config, now=clock(), **kw)
    response = client.post("/auth", data={"t": token}, follow_redirects=False)
    assert response.status_code == 303, response.text
    cookie = client.cookies.get(sessions.SESSION_COOKIE)
    assert cookie
    return config.token_hasher().csrf_for(cookie)


@pytest.fixture
def csrf(client: TestClient, users_db: Path, config: WebConfig, clock: Clock) -> str:
    return login(client, users_db, config, clock)


def wait_background(app: FastAPI, telegram_id: int = USER_ID, timeout: float = 5.0) -> None:
    """Wait for the background sync a read may have started."""
    assert app.state.web.syncer.wait_idle(str(telegram_id), timeout)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def wait_for(predicate: Any, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()
