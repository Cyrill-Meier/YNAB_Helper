"""Fixtures for the frontend tests: the real app (templates + static files) with a ready user
behind the fake YNAB, a signed-in TestClient, and helpers to parse the rendered HTML."""

from __future__ import annotations

import shutil
import warnings
from collections.abc import Iterator
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FAKE_TOKEN, FakeYnab
from ynab_helper import services, sessions
from ynab_helper.storage import UserStore, connect, open_users_db
from ynab_helper.web.config import WebConfig

with warnings.catch_warnings():
    warnings.simplefilter("ignore")  # starlette.testclient's httpx deprecation notice
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

from ynab_helper.web.app import STATIC_DIR, TEMPLATES_DIR, make_app

REPO_ROOT = Path(__file__).resolve().parents[2]
JS_DIR = STATIC_DIR / "js"
USER_ID = 42
BUDGET = "b1"
ACCT = "acct-main"
SECRET = "frontend-test-secret-key-0123456789abcdef"
CACHE_BUST = "2.0.0-fe"
STATIC_BASE = f"/static/{CACHE_BUST}"
#: Classic (non-module) scripts: loaded with a plain <script src>.
CLASSIC_SCRIPTS = frozenset({"theme.js", "auth.js"})

__all__ = [
    "CACHE_BUST",
    "CLASSIC_SCRIPTS",
    "JS_DIR",
    "REPO_ROOT",
    "STATIC_BASE",
    "STATIC_DIR",
    "TEMPLATES_DIR",
    "Element",
    "PageParser",
    "js_files",
    "login",
    "make_ready",
    "node_path",
    "parse",
]


@pytest.fixture
def fake() -> FakeYnab:
    f = FakeYnab()
    f.add_budget(BUDGET, currency="CHF")
    f.add_account(BUDGET, ACCT, "Revolut")
    return f


@pytest.fixture(autouse=True)
def _fake_clients(monkeypatch: pytest.MonkeyPatch, fake: FakeYnab) -> None:
    monkeypatch.setattr(services, "client_factory", lambda ctx: fake.client(max_retries=0))
    monkeypatch.setattr(
        services, "token_client_factory", lambda token: fake.client(token=token, max_retries=0)
    )


def make_ready(conn: Any, telegram_id: int = USER_ID, **fields: Any) -> None:
    values: dict[str, Any] = {
        "state": "ready",
        "ynab_token": FAKE_TOKEN,
        "budget_id": BUDGET,
        "budget_name": "Household",
        "account_id": ACCT,
        "account_name": "Revolut",
        "first_name": "Ann",
        "username": "ann",
    }
    values.update(fields)
    UserStore(conn).upsert(telegram_id, **values)


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
        make_ready(conn)
    finally:
        conn.close()
    return path


@pytest.fixture
def config() -> WebConfig:
    return WebConfig(enabled=True, public_url="http://testserver", secret_key=SECRET)


@pytest.fixture
def app(config: WebConfig, users_db: Path, data_dir: Path) -> Iterator[FastAPI]:
    application = make_app(
        config,
        users_db_path=users_db,
        data_dir=data_dir,
        version_line="v2.0.0 (frontend-test)",
        cache_bust=CACHE_BUST,
    )
    yield application
    state = application.state.web
    state.syncer.close()
    state.syncer.wait_idle(str(USER_ID), 5)


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    with TestClient(app) as c:
        yield c


def issue_token(users_db: Path, config: WebConfig, telegram_id: int = USER_ID) -> str:
    conn = connect(users_db)
    try:
        return sessions.issue_login_token(
            conn, config.token_hasher(), telegram_id, ttl_seconds=config.login_ttl
        )
    finally:
        conn.close()


def login(client: TestClient, users_db: Path, config: WebConfig) -> str:
    """Sign in through POST /auth; returns the session's CSRF token."""
    token = issue_token(users_db, config)
    response = client.post("/auth", data={"t": token}, follow_redirects=False)
    assert response.status_code == 303, response.text
    cookie = client.cookies.get(sessions.SESSION_COOKIE)
    assert cookie
    return config.token_hasher().csrf_for(cookie)


@pytest.fixture
def signed_in(client: TestClient, users_db: Path, config: WebConfig) -> TestClient:
    login(client, users_db, config)
    return client


# ── HTML parsing ─────────────────────────────────────────────────────────────


class Element:
    """A parsed start tag with its attributes, text and ancestry."""

    def __init__(self, tag: str, attrs: list[tuple[str, str | None]], parent: Element | None):
        self.tag = tag
        self.attrs: dict[str, str] = {k: (v if v is not None else "") for k, v in attrs}
        self.raw_attrs = attrs
        self.parent = parent
        self.children: list[Element] = []
        self.text_parts: list[str] = []

    @property
    def text(self) -> str:
        """All text inside the element (descendants included)."""
        return "".join(self.text_parts) + "".join(c.text for c in self.children)

    def ancestors(self) -> list[Element]:
        out = []
        node = self.parent
        while node is not None:
            out.append(node)
            node = node.parent
        return out

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<{self.tag} {self.attrs}>"


_VOID = frozenset(
    [
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "source",
        "track",
        "wbr",
        "path",
        "circle",
        "rect",
    ]
)


class PageParser(HTMLParser):
    """Builds a light element tree; keeps raw <script>/<style> bodies for inspection."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = Element("#root", [], None)
        self.stack: list[Element] = [self.root]
        self.elements: list[Element] = []
        self.texts: list[tuple[str, Element]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        parent = self.stack[-1]
        element = Element(tag, attrs, parent)
        parent.children.append(element)
        self.elements.append(element)
        if tag not in _VOID:
            self.stack.append(element)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        parent = self.stack[-1]
        element = Element(tag, attrs, parent)
        parent.children.append(element)
        self.elements.append(element)

    def handle_endtag(self, tag: str) -> None:
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                return

    def handle_data(self, data: str) -> None:
        node = self.stack[-1]
        node.text_parts.append(data)
        if data.strip():
            self.texts.append((data, node))

    def find_all(self, tag: str | None = None, **attrs: str) -> list[Element]:
        return [
            e
            for e in self.elements
            if (tag is None or e.tag == tag) and all(e.attrs.get(k) == v for k, v in attrs.items())
        ]

    def by_id(self, element_id: str) -> Element | None:
        for e in self.elements:
            if e.attrs.get("id") == element_id:
                return e
        return None


def parse(html: str) -> PageParser:
    parser = PageParser()
    parser.feed(html)
    parser.close()
    return parser


def js_files() -> list[Path]:
    """Every JavaScript file the app serves."""
    return sorted(JS_DIR.rglob("*.js"))


def node_path() -> str | None:
    return shutil.which("node")
