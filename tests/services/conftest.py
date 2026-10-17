"""Fixtures for the service-layer tests: a fake YNAB wired in through ``client_factory``."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FAKE_TOKEN, FakeYnab
from ynab_helper import services
from ynab_helper.services import UserContext
from ynab_helper.storage import connect

BUDGET = "b1"
ACCT = "acct-main"
OTHER = "acct-other"
FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture
def fake() -> FakeYnab:
    f = FakeYnab()
    f.add_budget(BUDGET, currency="CHF")
    f.add_account(BUDGET, ACCT, "Revolut")
    f.add_account(BUDGET, OTHER, "Revolut EUR")
    return f


@pytest.fixture(autouse=True)
def _fake_clients(monkeypatch: pytest.MonkeyPatch, fake: FakeYnab) -> None:
    # No client retries: an injected fault must reach the code under test.
    monkeypatch.setattr(services, "client_factory", lambda ctx: fake.client(max_retries=0))


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    path = tmp_path / "bot_data"
    path.mkdir()
    return path


def make_ctx(data_dir: Path, account_id: str = ACCT, **overrides: Any) -> UserContext:
    values: dict[str, Any] = {
        "telegram_id": 42,
        "ynab_token": FAKE_TOKEN,
        "budget_id": BUDGET,
        "account_id": account_id,
        "auto_approve": True,
    }
    values.update(overrides)
    return UserContext.from_user_row(values, data_dir)


@pytest.fixture
def ctx(data_dir: Path) -> UserContext:
    return make_ctx(data_dir)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def db_rows(ctx: UserContext, *tables: str) -> list[tuple[Any, ...]]:
    """Every row of ``tables`` in the user's DB (for before/after comparisons)."""
    conn = connect(ctx.db_path)
    try:
        return [
            (table, *tuple(row))
            for table in tables
            for row in conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2")
        ]
    finally:
        conn.close()


@pytest.fixture
def raw_db(ctx: UserContext) -> Iterator[sqlite3.Connection]:
    conn = connect(ctx.db_path)
    try:
        yield conn
    finally:
        conn.close()
