"""Fixtures for the bot tests: a fake Telegram, a fake YNAB, a users DB and a Bot wired to
them with inline (synchronous) workers."""

from __future__ import annotations

import logging
import os
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_telegram import FakeTelegram
from tests.fakes.fake_ynab import FakeYnab
from ynab_helper import logging_setup, services
from ynab_helper.balance.reconcile import RECONCILE_TOKENS
from ynab_helper.bot.app import Bot, WebAccess
from ynab_helper.bot.telegram import TelegramClient
from ynab_helper.bot.users import UsersRepo
from ynab_helper.bot.workers import WorkerPool
from ynab_helper.locks import LOCKS
from ynab_helper.maintenance.dedupe import DEDUPE_SCANS
from ynab_helper.storage import open_users_db
from ynab_helper.storage.users import User

ADMIN = 1001
USER = 4242
STRANGER = 6666
YNAB_TOKEN = "ynab-test-token-0123456789abcdef"
BUDGET = "b1"
ACCT = "acct-main"
OTHER = "acct-other"
CLOSED = "acct-closed"
CRYPTO = "acct-crypto"


_TOUCHED_LOGGERS = (*logging_setup.NOISY_LOGGERS, logging_setup.AUDIT_LOGGER)


@pytest.fixture(autouse=True)
def _restore_process_state() -> Iterator[None]:
    """The startup tests run ``setup_logging`` and set the umask: put everything back."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    saved = {
        name: (logging.getLogger(name).level, logging.getLogger(name).propagate)
        for name in _TOUCHED_LOGGERS
    }
    umask = os.umask(0o022)
    os.umask(umask)
    yield
    os.umask(umask)
    logging_setup.reset_logging()
    logging_setup.clear_redactions()
    for handler in list(root.handlers):
        if handler not in saved_handlers:
            root.removeHandler(handler)
    for handler in saved_handlers:
        if handler not in root.handlers:
            root.addHandler(handler)
    root.setLevel(saved_level)
    for name, (level, propagate) in saved.items():
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = propagate


@pytest.fixture
def tg() -> Iterator[FakeTelegram]:
    fake = FakeTelegram()
    yield fake
    # The client resends rejected HTML as plain text, which would hide a formatter bug.
    assert fake.html_errors == [], f"Telegram would reject this HTML: {fake.html_errors[:3]}"
    # Callback answers are plain text: markup there would be shown literally.
    assert fake.markup_in_answers == [], f"markup in answers: {fake.markup_in_answers[:3]}"


@pytest.fixture
def ynab() -> FakeYnab:
    fake = FakeYnab(token=YNAB_TOKEN)
    fake.add_budget(BUDGET, currency="CHF", name="Household")
    fake.add_account(BUDGET, ACCT, "Revolut")
    fake.add_account(BUDGET, OTHER, "Savings", type="savings")
    fake.add_account(BUDGET, CLOSED, "Revolut old", closed=True)
    fake.add_account(BUDGET, CRYPTO, "Crypto", type="otherAsset", on_budget=False)
    return fake


@pytest.fixture(autouse=True)
def _wire_services(monkeypatch: pytest.MonkeyPatch, ynab: FakeYnab) -> Iterator[None]:
    monkeypatch.setattr(
        services, "client_factory", lambda ctx: ynab.client(token=ctx.ynab_token, max_retries=0)
    )
    monkeypatch.setattr(
        services, "token_client_factory", lambda token: ynab.client(token=token, max_retries=0)
    )
    DEDUPE_SCANS.clear()
    RECONCILE_TOKENS.clear()
    yield
    DEDUPE_SCANS.clear()
    RECONCILE_TOKENS.clear()
    assert LOCKS.snapshot() == {}, "a test leaked a per-user lock"


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    path = tmp_path / "bot_data"
    path.mkdir()
    return path


@pytest.fixture
def users(data_dir: Path) -> Iterator[UsersRepo]:
    conn = open_users_db(
        data_dir / "bot_users.db", backup_dir=data_dir / "backups", check_same_thread=False
    )
    try:
        yield UsersRepo(conn)
    finally:
        conn.close()


@pytest.fixture
def client(tg: FakeTelegram) -> TelegramClient:
    return TelegramClient(tg.token, api_base=tg.base_url, opener=tg.opener, sleep=lambda s: None)


@pytest.fixture
def bot(client: TelegramClient, users: UsersRepo, data_dir: Path) -> Bot:
    return Bot(
        telegram=client,
        users=users,
        admin_id=ADMIN,
        data_dir=data_dir,
        workers=WorkerPool(0),
        web=WebAccess(),
        hard_exit=_no_exit,
    )


def _no_exit(code: int) -> None:  # pragma: no cover - only reached on a hung test
    raise AssertionError(f"hard exit {code}")


# ── helpers ──────────────────────────────────────────────────────────────────


def add_user(users: UsersRepo, telegram_id: int, state: str, **fields: Any) -> User:
    fields.setdefault("chat_id", telegram_id)
    fields.setdefault("first_name", f"User{telegram_id}")
    return users.update(telegram_id, state=state, **fields)


def ready_user(
    users: UsersRepo, telegram_id: int = USER, *, state: str = "ready", **fields: Any
) -> User:
    """A fully configured user (in ``state``, default ready)."""
    values: dict[str, Any] = {
        "ynab_token": YNAB_TOKEN,
        "budget_id": BUDGET,
        "budget_name": "Household",
        "account_id": ACCT,
        "account_name": "Revolut",
    }
    values.update(fields)
    return add_user(users, telegram_id, state, **values)


def deliver(bot: Bot, tg: FakeTelegram) -> int:
    """Hand every queued update to the bot (in order); returns how many."""
    updates = tg.take_updates()
    for update in updates:
        bot.handle_update(update)
    return len(updates)


def say(bot: Bot, tg: FakeTelegram, from_id: int, text: str, **kwargs: Any) -> None:
    tg.message(from_id, text, **kwargs)
    deliver(bot, tg)


def press(bot: Bot, tg: FakeTelegram, from_id: int, data: str, **kwargs: Any) -> None:
    tg.callback(from_id, data, **kwargs)
    deliver(bot, tg)


def row_count(path: Path, table: str) -> int:
    conn = sqlite3.connect(path)
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        conn.close()
