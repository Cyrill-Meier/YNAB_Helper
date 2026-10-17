"""Startup: config validation, migration of a v1 data dir, admin auto-register, web UI
status in the startup DM (F57, F60, F94)."""

from __future__ import annotations

import sqlite3
import stat
import sys
import threading
import time
import types
from pathlib import Path
from typing import Any

import pytest

from tests.bot.conftest import ADMIN, YNAB_TOKEN
from tests.fakes.fake_telegram import FakeTelegram
from tests.storage.helpers import PROD_TX_DDL, PROD_USERS_DDL, legacy_row, make_legacy_db
from ynab_helper.bot import main as bot_main
from ynab_helper.bot.app import Bot
from ynab_helper.bot.workers import WorkerPool
from ynab_helper.storage import UserStore, connect

PUBLIC = "https://bot.example.test"


def base_env(data_dir: Path, tg: FakeTelegram, **extra: str) -> dict[str, str]:
    env = {
        "TELEGRAM_BOT_TOKEN": tg.token,
        "TELEGRAM_ADMIN_ID": str(ADMIN),
        "TELEGRAM_API_BASE": tg.base_url,
        "BOT_DATA_DIR": str(data_dir),
        "LOG_FILE": "",
        "BUILD_SHA": "abc1234def",
        "BUILD_DATE": "2026-10-16T20:00:00Z",
        "HOME": str(data_dir.parent),
    }
    env.update(extra)
    return env


class Started:
    """Runs bot_main.run in a thread and stops it once the startup DM arrived."""

    def __init__(self, env: dict[str, str], tg: FakeTelegram) -> None:
        self.bot: Bot | None = None
        self.code: int | None = None
        self.tg = tg
        self.seen = len(self._dms())

        def capture(bot: Bot) -> None:
            self.bot = bot

        def target() -> None:
            self.code = bot_main.run(
                environ=env,
                install_signals=False,
                telegram_opener=tg.opener,
                workers=WorkerPool(0),
                hard_exit=lambda code: None,
                on_bot=capture,
            )

        self.thread = threading.Thread(target=target, daemon=True)
        self.thread.start()

    def _dms(self) -> list[str]:
        return [t for t in self.tg.texts(ADMIN) if "Bot started" in t]

    def startup_dm(self, timeout: float = 15) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            texts = self._dms()
            if len(texts) > self.seen and self.bot is not None:
                return texts[-1]
            if not self.thread.is_alive():
                raise AssertionError(f"bot ended early with {self.code}")
            time.sleep(0.01)
        raise AssertionError("no startup DM")

    def stop(self) -> int:
        assert self.bot is not None
        self.bot.request_stop()
        self.thread.join(10)
        assert not self.thread.is_alive()
        assert self.code is not None
        return self.code


def run_once(env: dict[str, str], tg: FakeTelegram) -> tuple[str, Started]:
    started = Started(env, tg)
    dm = started.startup_dm()
    assert started.stop() == 0
    return dm, started


# ── configuration ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("missing", ["TELEGRAM_BOT_TOKEN", "TELEGRAM_ADMIN_ID"])
def test_missing_telegram_settings_exit_1(
    tmp_path: Path, tg: FakeTelegram, capsys: pytest.CaptureFixture[str], missing: str
):
    env = base_env(tmp_path / "bot_data", tg)
    del env[missing]
    assert bot_main.run(environ=env, install_signals=False) == 1
    err = capsys.readouterr().err
    assert missing in err
    assert not (tmp_path / "bot_data").exists()  # nothing created before validation


def test_malformed_admin_id_exits_1(tmp_path: Path, tg: FakeTelegram, capsys):
    env = base_env(tmp_path / "bot_data", tg, TELEGRAM_ADMIN_ID="12abc")
    assert bot_main.run(environ=env, install_signals=False) == 1
    assert "TELEGRAM_ADMIN_ID" in capsys.readouterr().err


# ── migration of a v1 data dir ───────────────────────────────────────────────


def make_v1_data_dir(data_dir: Path) -> None:
    data_dir.mkdir(parents=True)
    users = sqlite3.connect(data_dir / "bot_users.db")
    users.executescript(PROD_USERS_DDL)
    users.execute(
        "INSERT INTO users (telegram_id, chat_id, first_name, state, created_at, updated_at, "
        "ynab_token, budget_id, account_id) VALUES (42, 42, 'Old', 'approved', 'x', 'x', NULL, NULL, NULL)"
    )
    users.execute(
        "INSERT INTO users (telegram_id, chat_id, first_name, state, created_at, updated_at, "
        "ynab_token, budget_id, account_id) VALUES (43, 43, 'Ready', 'ready', 'x', 'x', 't', 'b', 'acct-main')"
    )
    users.commit()
    users.close()
    make_legacy_db(
        data_dir / "transactions_43.db",
        PROD_TX_DDL,
        [legacy_row("YNAB:-10000:2026-08-01:1", ynab_tx_id="y1")],
    )
    (data_dir / "csv_cache" / "43").mkdir(parents=True)
    (data_dir / "csv_cache" / "43" / "old.csv").write_text("Type,Product\n")
    (data_dir / "telegram_offset").write_text("500")


def test_startup_migrates_a_v1_data_dir_before_running(tmp_path: Path, tg: FakeTelegram):
    data_dir = tmp_path / "bot_data"
    make_v1_data_dir(data_dir)
    dm, _started = run_once(base_env(data_dir, tg, WEB_UI_ENABLED="0"), tg)
    assert dm.startswith(
        "🚀 Bot started v2.0.0 (abc1234, 2026-10-16 20:00 UTC) — web UI: disabled ("
    )
    assert "WEB_UI_ENABLED" in dm and "Ready users: 1" in dm
    conn = connect(data_dir / "bot_users.db")
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
        assert UserStore(conn).get(42).state == "awaiting_token"  # legacy 'approved'
    finally:
        conn.close()
    tx = connect(data_dir / "transactions_43.db")
    try:
        assert tx.execute("PRAGMA user_version").fetchone()[0] == 1
        rows = tx.execute("SELECT account_id, key, status FROM import_ledger").fetchall()
        assert [tuple(r) for r in rows] == [("acct-main", "YNAB:-10000:2026-08-01:1", "ACTIVE")]
        assert tx.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 1  # kept
    finally:
        tx.close()
    assert any((data_dir / "backups").iterdir())
    # The poll loop continued from v1's persisted offset.
    assert tg.calls_of("getUpdates")[0]["offset"] == 500


def test_migration_failures_are_reported_to_the_admin(tmp_path: Path, tg: FakeTelegram):
    data_dir = tmp_path / "bot_data"
    make_v1_data_dir(data_dir)
    (data_dir / "transactions_77.db").write_bytes(b"this is not a database" * 100)
    dm, _ = run_once(base_env(data_dir, tg), tg)
    assert "⚠ Migration of transactions_77.db failed" in dm


def test_F57_files_are_private(tmp_path: Path, tg: FakeTelegram):
    data_dir = tmp_path / "bot_data"
    run_once(base_env(data_dir, tg), tg)
    for name in ("bot_users.db", "telegram_offset", ".heartbeat"):
        path = data_dir / name
        if path.exists():
            assert stat.S_IMODE(path.stat().st_mode) & 0o077 == 0, name


# ── admin auto-register (v1 semantics) ───────────────────────────────────────


def ynab_env(**extra: str) -> dict[str, str]:
    values = {
        "YNAB_TOKEN": YNAB_TOKEN,
        "YNAB_BUDGET_ID": "b1",
        "YNAB_ACCOUNT_ID": "acct-main",
        "YNAB_CRYPTO_ACCOUNT_ID": "acct-crypto",
    }
    values.update(extra)
    return values


def admin_row(data_dir: Path):
    conn = connect(data_dir / "bot_users.db")
    try:
        return UserStore(conn).get(ADMIN)
    finally:
        conn.close()


def test_admin_is_auto_registered_when_missing(tmp_path: Path, tg: FakeTelegram):
    data_dir = tmp_path / "bot_data"
    dm, _ = run_once(base_env(data_dir, tg, **ynab_env()), tg)
    admin = admin_row(data_dir)
    assert admin.state == "ready" and admin.account_id == "acct-main"
    assert admin.crypto_account_id == "acct-crypto"
    assert "Ready users: 1" in dm


def test_a_ready_admin_is_never_overwritten(tmp_path: Path, tg: FakeTelegram):
    data_dir = tmp_path / "bot_data"
    run_once(base_env(data_dir, tg, **ynab_env()), tg)
    conn = connect(data_dir / "bot_users.db")
    UserStore(conn).upsert(ADMIN, account_id="chosen-in-bot")
    conn.close()
    run_once(base_env(data_dir, tg, **ynab_env()), tg)
    assert admin_row(data_dir).account_id == "chosen-in-bot"


def test_an_unfinished_admin_setup_is_replaced_by_the_environment(tmp_path: Path, tg: FakeTelegram):
    data_dir = tmp_path / "bot_data"
    run_once(base_env(data_dir, tg, **ynab_env()), tg)
    conn = connect(data_dir / "bot_users.db")
    UserStore(conn).upsert(ADMIN, state="awaiting_budget", temp_data={"v": 2})
    conn.close()
    run_once(base_env(data_dir, tg, **ynab_env()), tg)
    admin = admin_row(data_dir)
    assert admin.state == "ready" and admin.temp_data is None


# ── web UI status in the startup DM (F94, F60) ───────────────────────────────


class FakeHandle:
    def __init__(self, *, failed: BaseException | None = None, start: bool = True) -> None:
        self.started = threading.Event()
        if start and failed is None:
            self.started.set()
        self.failed = failed
        self.stopped = 0

    def wait_started(self, timeout: float) -> bool:
        return self.started.wait(timeout)

    def stop(self, timeout: float = 5.0) -> None:
        self.stopped += 1


@pytest.fixture
def fake_server(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    state: dict[str, Any] = {"calls": [], "handle": FakeHandle(), "raise": None}
    module = types.ModuleType("ynab_helper.web.server")

    def serve_in_thread(config, **kwargs):
        state["calls"].append((config, kwargs))
        # What the web server would find on disk when it opens its own connections.
        state["versions"] = {
            path.name: user_version(path) for path in sorted(kwargs["data_dir"].glob("*.db"))
        }
        if state["raise"] is not None:
            raise state["raise"]
        return state["handle"]

    module.serve_in_thread = serve_in_thread  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "ynab_helper.web.server", module)
    return state


def user_version(path: Path) -> int:
    conn = sqlite3.connect(path)
    try:
        return int(conn.execute("PRAGMA user_version").fetchone()[0])
    finally:
        conn.close()


def web_env(**extra: str) -> dict[str, str]:
    values = {"WEB_UI_ENABLED": "1", "WEB_UI_PUBLIC_URL": PUBLIC, "WEB_UI_SECRET_KEY": "q" * 40}
    values.update(extra)
    return values


def test_F94_web_enabled(tmp_path: Path, tg: FakeTelegram, fake_server: dict[str, Any]):
    data_dir = tmp_path / "bot_data"
    started = Started(base_env(data_dir, tg, **web_env()), tg)
    dm = started.startup_dm()
    assert f"web UI: enabled at {PUBLIC}" in dm
    [(config, kwargs)] = fake_server["calls"]
    assert config.public_url == PUBLIC
    assert kwargs["users_db_path"] == data_dir / "bot_users.db"
    assert kwargs["data_dir"] == data_dir
    assert kwargs["version_line"].startswith("v2.0.0 (abc1234")
    assert kwargs["cache_bust"] == "2.0.0-abc1234"
    assert kwargs["users_lock"] is started.bot.users.lock
    message = started.bot.tg.send_message(ADMIN, "login link")
    kwargs["delete_telegram_message"](ADMIN, message["message_id"])
    assert (ADMIN, message["message_id"]) in tg.deleted
    assert started.stop() == 0
    assert fake_server["handle"].stopped == 1  # stopped once at shutdown


def test_v1_data_dir_is_migrated_before_the_web_server_starts(
    tmp_path: Path, tg: FakeTelegram, fake_server: dict[str, Any]
):
    """DESIGN §8.1: the web thread must never see a v1-shaped bot_users.db or
    transactions_*.db (its own connections do not migrate)."""
    data_dir = tmp_path / "bot_data"
    make_v1_data_dir(data_dir)
    assert user_version(data_dir / "bot_users.db") == 0
    dm, _ = run_once(base_env(data_dir, tg, **web_env()), tg)
    assert f"web UI: enabled at {PUBLIC}" in dm
    assert fake_server["versions"] == {"bot_users.db": 1, "transactions_43.db": 1}


def test_F94_web_failure_is_reported(tmp_path: Path, tg: FakeTelegram, fake_server: dict[str, Any]):
    fake_server["handle"] = FakeHandle(failed=OSError(48, "Address already in use"))
    dm, started = run_once(base_env(tmp_path / "bot_data", tg, **web_env()), tg)
    assert "web UI: failed (OSError: [Errno 48] Address already in use)" in dm
    assert not started.bot.web.usable()


def test_F94_web_import_error_is_reported(
    tmp_path: Path, tg: FakeTelegram, fake_server: dict[str, Any]
):
    fake_server["raise"] = ImportError("No module named 'fastapi'")
    dm, _ = run_once(base_env(tmp_path / "bot_data", tg, **web_env()), tg)
    assert "web UI: failed (ImportError: No module named 'fastapi')" in dm


def test_F94_web_not_started_in_time(
    tmp_path: Path, tg: FakeTelegram, fake_server: dict[str, Any], monkeypatch
):
    monkeypatch.setattr(bot_main, "WEB_START_TIMEOUT", 0.3)
    fake_server["handle"] = FakeHandle(start=False)
    dm, _ = run_once(base_env(tmp_path / "bot_data", tg, **web_env()), tg)
    assert "web UI: failed (not serving after" in dm


def test_F60_web_config_problems_disable_the_web_ui(
    tmp_path: Path, tg: FakeTelegram, fake_server: dict[str, Any]
):
    env = base_env(
        tmp_path / "bot_data", tg, **web_env(WEB_UI_SECRET_KEY="SeCrEtX9", WEB_UI_PORT="80x")
    )
    dm, _ = run_once(env, tg)
    assert "web UI: disabled (configuration problem:" in dm
    assert "SeCrEtX9" not in dm  # the secret is never echoed
    assert fake_server["calls"] == []


def test_a_malformed_bot_token_exits_1_before_touching_data(
    tmp_path: Path, tg: FakeTelegram, capsys
):
    env = base_env(tmp_path / "bot_data", tg, TELEGRAM_BOT_TOKEN="123:abc def")
    assert bot_main.run(environ=env, install_signals=False) == 1
    assert "TELEGRAM_BOT_TOKEN" in capsys.readouterr().err
    assert not (tmp_path / "bot_data").exists()
