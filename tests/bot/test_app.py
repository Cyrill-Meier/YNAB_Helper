"""Bot runtime: offset per update, backoff, heartbeat, stop/SIGTERM, answers (F26, F40, F43, F69)."""

from __future__ import annotations

import dataclasses
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from tests.bot.conftest import ADMIN, USER, deliver, ready_user
from tests.conftest import REPO_ROOT
from tests.fakes.fake_telegram import FakeTelegram
from ynab_helper.bot import app as app_module
from ynab_helper.bot.app import Bot, Heartbeat
from ynab_helper.bot.commands import Registry, build_registry
from ynab_helper.bot.telegram import TelegramClient
from ynab_helper.bot.users import UsersRepo
from ynab_helper.bot.workers import WorkerPool


def run_in_thread(bot: Bot, **kwargs: Any) -> threading.Thread:
    thread = threading.Thread(target=bot.run, kwargs=kwargs, daemon=True)
    thread.start()
    return thread


def wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition not reached")


def stop(bot: Bot, thread: threading.Thread) -> None:
    bot.request_stop()
    thread.join(5)
    assert not thread.is_alive()


@pytest.fixture
def ready(users: UsersRepo) -> None:
    ready_user(users)


# ── offset (F40) ─────────────────────────────────────────────────────────────


@pytest.mark.usefixtures("ready")
def test_F40_offset_is_persisted_after_each_handled_update(bot: Bot, tg: FakeTelegram):
    saved: list[int] = []
    real_save = bot.save_offset

    def record(offset: int) -> None:
        saved.append(offset)
        real_save(offset)

    bot.save_offset = record  # type: ignore[method-assign]
    ids = [tg.message(USER, "/help") for _ in range(3)]
    thread = run_in_thread(bot)
    wait_for(lambda: len(tg.sent(USER)) == 3)
    wait_for(lambda: len(saved) == 3)
    stop(bot, thread)
    assert saved == [i + 1 for i in ids]
    assert bot.offset_path.read_text() == str(ids[-1] + 1)


@pytest.mark.usefixtures("ready")
def test_F40_a_stop_mid_batch_leaves_the_rest_unconfirmed(bot: Bot, tg: FakeTelegram):
    ids = [tg.message(USER, "/help") for _ in range(3)]
    real = bot.handle_update

    def handle(update: dict) -> None:
        real(update)
        bot.request_stop()  # SIGTERM while the first update is handled

    bot.handle_update = handle  # type: ignore[method-assign]
    thread = run_in_thread(bot)
    thread.join(5)
    assert not thread.is_alive()
    assert bot.offset_path.read_text() == str(ids[0] + 1)
    assert len(tg.sent(USER)) == 1


@pytest.mark.usefixtures("ready")
def test_restart_resumes_after_the_persisted_offset(
    tg: FakeTelegram, users: UsersRepo, client: TelegramClient, data_dir: Path
):
    def make() -> Bot:
        return Bot(
            telegram=client, users=users, admin_id=ADMIN, data_dir=data_dir, workers=WorkerPool(0)
        )

    first = make()
    tg.message(USER, "/help")
    thread = run_in_thread(first)
    wait_for(lambda: len(tg.sent(USER)) == 1)
    stop(first, thread)
    tg.message(USER, "/help")
    second = make()
    thread = run_in_thread(second)
    wait_for(lambda: len(tg.sent(USER)) == 2)
    time.sleep(0.2)
    stop(second, thread)
    assert len(tg.sent(USER)) == 2  # the first update was not handled twice


def test_malformed_offset_file_starts_from_zero(bot: Bot):
    bot.offset_path.write_text("garbage")
    assert bot.load_offset() == 0
    bot.save_offset(42)
    assert bot.load_offset() == 42
    assert not bot.offset_path.with_name("telegram_offset.tmp").exists()


# ── backoff (F26) ────────────────────────────────────────────────────────────


def test_F26_getupdates_failures_back_off_exponentially(
    tg: FakeTelegram, users: UsersRepo, client: TelegramClient, data_dir: Path
):
    waits: list[float] = []

    def wait(seconds: float) -> bool:
        waits.append(seconds)
        return True

    bot = Bot(
        telegram=client,
        users=users,
        admin_id=ADMIN,
        data_dir=data_dir,
        workers=WorkerPool(0),
        wait=wait,
    )
    tg.network_error_next("getUpdates", times=8)
    thread = run_in_thread(bot)
    wait_for(lambda: len(waits) == 8)
    wait_for(lambda: len(tg.calls_of("getUpdates")) >= 1)  # recovered after the 8th failure
    tg.fail_next("getUpdates", 409, "Conflict: terminated by other getUpdates request")
    wait_for(lambda: len(waits) == 9)
    stop(bot, thread)
    assert waits == [1, 2, 4, 8, 16, 32, 60, 60, 1]  # capped at 60 s, reset after success


def test_F26_429_retry_after_is_honoured(
    tg: FakeTelegram, users: UsersRepo, client: TelegramClient, data_dir: Path
):
    waits: list[float] = []
    bot = Bot(
        telegram=client,
        users=users,
        admin_id=ADMIN,
        data_dir=data_dir,
        workers=WorkerPool(0),
        wait=lambda s: waits.append(s) or True,
    )
    tg.fail_next("getUpdates", 429, "Too Many Requests", retry_after=120)
    thread = run_in_thread(bot)
    wait_for(lambda: waits)
    stop(bot, thread)
    assert waits[0] == 120


def test_getme_failures_never_end_the_bot(
    tg: FakeTelegram, users: UsersRepo, client: TelegramClient, data_dir: Path
):
    waits: list[float] = []
    bot = Bot(
        telegram=client,
        users=users,
        admin_id=ADMIN,
        data_dir=data_dir,
        workers=WorkerPool(0),
        wait=lambda s: waits.append(s) or True,
    )
    tg.network_error_next("getMe", times=5)
    thread = run_in_thread(bot, startup_message="🚀 Bot started")
    wait_for(lambda: tg.texts(ADMIN) == ["🚀 Bot started"])
    stop(bot, thread)
    assert waits == [1, 2, 4, 8, 16]
    scopes = [c["scope"]["type"] for c in tg.commands]
    assert scopes == ["default", "chat"]
    admin_menu = {c["command"] for c in tg.commands[1]["commands"]}
    user_menu = {c["command"] for c in tg.commands[0]["commands"]}
    assert {"approve", "deny", "users", "ip", "logs", "revoke"} <= admin_menu
    assert not user_menu & {"approve", "deny", "users", "ip", "logs", "revoke"}
    assert "dedupe_delete" not in user_menu | admin_menu
    assert "dedupe_cancel" not in user_menu | admin_menu
    assert {"cancel", "logout_all"} <= user_menu


def test_F26_backoff_is_defined_for_any_number_of_failures():
    """2**1024 overflowed a float: after ~17 h of outage the bot crashed out of run()."""
    assert [Bot._backoff(n) for n in range(8)] == [1, 2, 4, 8, 16, 32, 60, 60]
    for attempt in (1023, 1024, 1100, 10**9):
        assert Bot._backoff(attempt) == 60
    assert Bot._backoff(-5) == 1


@pytest.mark.parametrize("method", ["getUpdates", "getMe"])
def test_F26_a_very_long_outage_never_ends_the_bot(
    tg: FakeTelegram, users: UsersRepo, client: TelegramClient, data_dir: Path, method: str
):
    """1100 consecutive failures (about 18 h at the 60 s cap: a revoked bot token, a
    Telegram or network outage) must leave the bot polling, not crashed."""
    failures = 1100
    waits: list[float] = []
    bot = Bot(
        telegram=client,
        users=users,
        admin_id=ADMIN,
        data_dir=data_dir,
        workers=WorkerPool(0),
        wait=lambda s: waits.append(s) or True,
        hard_exit=lambda code: None,
    )
    status, description = (401, "Unauthorized") if method == "getUpdates" else (502, "Bad Gateway")
    tg.fail_next(method, status, description, times=failures)
    thread = run_in_thread(bot, startup_message="🚀 Bot started")
    wait_for(lambda: len(waits) >= failures, timeout=60)
    polls_needed = failures + 1 if method == "getUpdates" else 1
    wait_for(lambda: len(tg.calls_of("getUpdates")) > polls_needed, timeout=10)  # recovered
    assert thread.is_alive()
    stop(bot, thread)
    assert len(waits) == failures
    assert max(waits) == 60 and waits[-1] == 60
    assert tg.texts(ADMIN) == ["🚀 Bot started"]


def test_stop_during_startup_backoff_is_quick(
    tg: FakeTelegram, users: UsersRepo, client: TelegramClient, data_dir: Path
):
    bot = Bot(
        telegram=client, users=users, admin_id=ADMIN, data_dir=data_dir, workers=WorkerPool(0)
    )
    tg.network_error_next("getMe", times=1000)
    thread = run_in_thread(bot)
    wait_for(lambda: len(tg.calls) >= 0 and tg.pending_updates == 0)
    time.sleep(0.3)
    started = time.monotonic()
    stop(bot, thread)
    assert time.monotonic() - started < 1.0


# ── heartbeat (F69) ──────────────────────────────────────────────────────────


@pytest.mark.usefixtures("ready")
def test_F69_heartbeat_is_touched_by_the_loop(
    tg: FakeTelegram, users: UsersRepo, client: TelegramClient, data_dir: Path
):
    path = data_dir / ".heartbeat"
    bot = Bot(
        telegram=client,
        users=users,
        admin_id=ADMIN,
        data_dir=data_dir,
        workers=WorkerPool(0),
        heartbeat_path=path,
    )
    thread = run_in_thread(bot)
    wait_for(path.exists)
    os.utime(path, (1, 1))
    wait_for(lambda: path.stat().st_mtime > 1)  # touched again on the next iteration
    stop(bot, thread)


def test_heartbeat_stall_detection():
    now = [100.0]
    beat = Heartbeat(None, clock=lambda: now[0])
    beat.progress()
    now[0] += 119
    assert not beat.stalled(120)
    now[0] += 2
    assert beat.stalled(120)


def test_supervisor_ends_a_hung_shutdown(
    tg: FakeTelegram, users: UsersRepo, client: TelegramClient, data_dir: Path, monkeypatch
):
    monkeypatch.setattr(app_module, "HARD_EXIT_AFTER", 0.5)
    exits: list[int] = []
    release = threading.Event()
    bot = Bot(
        telegram=client,
        users=users,
        admin_id=ADMIN,
        data_dir=data_dir,
        workers=WorkerPool(0),
        hard_exit=exits.append,
    )
    entered = threading.Event()

    def stuck(update: dict) -> None:
        entered.set()
        release.wait(10)

    bot.handle_update = stuck  # type: ignore[method-assign]
    ready_user(users)
    tg.message(USER, "/help")
    thread = run_in_thread(bot)
    assert entered.wait(5)
    bot.request_stop()
    wait_for(lambda: exits == [0], timeout=3)
    release.set()
    thread.join(5)


# ── dispatch robustness ──────────────────────────────────────────────────────


@pytest.mark.usefixtures("ready")
def test_callbacks_are_answered_even_when_the_handler_fails(bot: Bot, tg: FakeTelegram):
    def boom(req) -> None:
        raise RuntimeError("kaputt")

    base = build_registry()
    bot.registry = Registry(
        base.commands,
        [dataclasses.replace(r, handler=boom) if r.prefix == "st" else r for r in base.callbacks],
    )
    tg.callback(USER, "st:close")
    deliver(bot, tg)
    assert len(tg.answers) == 1


@pytest.mark.usefixtures("ready")
def test_handler_exceptions_reach_the_user_as_a_short_error(
    bot: Bot, tg: FakeTelegram, monkeypatch
):
    from ynab_helper.bot.handlers import general

    monkeypatch.setattr(general, "help_text", lambda req: (_ for _ in ()).throw(RuntimeError("x")))
    tg.message(USER, "/help")
    deliver(bot, tg)
    assert tg.texts(USER)[-1].startswith("❌ Something went wrong")


def test_unsupported_and_malformed_updates_are_ignored(bot: Bot, tg: FakeTelegram):
    for update in (
        {"update_id": 1, "edited_message": {}},
        {"update_id": 2, "message": "nope"},
        {"update_id": "x"},
        {"update_id": 3, "message": {"chat": {"id": 1, "type": "private"}}},
        {"update_id": 4, "callback_query": {"id": "c", "data": "st:close"}},
    ):
        bot.handle_update(update)
    assert tg.sent() == []


# ── workers (F43) ────────────────────────────────────────────────────────────


def test_F43_slow_jobs_do_not_block_other_users(
    tg: FakeTelegram, users: UsersRepo, client: TelegramClient, data_dir: Path, monkeypatch
):
    from ynab_helper.bot.handlers import status as status_handler

    release = threading.Event()

    def slow(req) -> None:
        req.bot.start_job(req, "status", lambda: (release.wait(5), req.reply("done")), working="…")

    monkeypatch.setattr(status_handler, "cmd_status", slow)
    workers = WorkerPool(2)
    bot = Bot(telegram=client, users=users, admin_id=ADMIN, data_dir=data_dir, workers=workers)
    bot.registry = build_registry()
    ready_user(users, USER)
    ready_user(users, USER + 1)
    tg.message(USER, "/status")
    tg.message(USER, "/status")  # a second job of the same user is refused, not queued
    tg.message(USER + 1, "/help")
    deliver(bot, tg)
    assert tg.texts(USER)[-1] == "⏳ Another operation is still running."
    assert "Revolut → YNAB Bot" in tg.texts(USER + 1)[-1]  # answered while USER's job runs
    release.set()
    assert workers.wait_idle(5)
    workers.shutdown(1)
    assert "done" in tg.texts(USER)


def test_shutdown_abandons_jobs_after_the_grace_period():
    pool = WorkerPool(1)
    block, entered = threading.Event(), threading.Event()
    ran: list[str] = []
    assert pool.submit("u", "slow", lambda: (entered.set(), block.wait(10)))
    assert entered.wait(5)
    assert pool.submit("v", "queued", lambda: ran.append("v"))
    started = time.monotonic()
    report = pool.shutdown(0.3)
    assert time.monotonic() - started < 1.0
    assert report.finished is False
    assert report.dropped == (("v", "queued"),)
    assert report.abandoned == (("u", "slow"),)
    assert not pool.submit("w", "late", lambda: None)
    block.set()
    assert ran == []  # the queued job was dropped, not run after the stop


# ── shutdown with work in flight ─────────────────────────────────────────────

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.mark.usefixtures("ready")
def test_shutdown_tells_users_their_job_was_dropped_or_cut_off(
    tg: FakeTelegram,
    users: UsersRepo,
    client: TelegramClient,
    data_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Their updates are already confirmed, so Telegram will not send them again: without a
    notice the user only ever saw "Importing …". The abandoned download (a bank statement)
    is deleted on the next start."""
    from ynab_helper import services

    monkeypatch.setattr(app_module, "JOB_GRACE", 0.3)
    entered, release = threading.Event(), threading.Event()

    def blocked_import(*args: Any, **kwargs: Any) -> Any:
        entered.set()
        release.wait(10)
        raise RuntimeError("released after the test")

    monkeypatch.setattr(services, "import_csv", blocked_import)
    ready_user(users, USER + 1)
    exits: list[int] = []
    bot = Bot(
        telegram=client,
        users=users,
        admin_id=ADMIN,
        data_dir=data_dir,
        workers=WorkerPool(1),
        hard_exit=exits.append,
    )
    tmp = data_dir / "tmp"
    try:
        thread = run_in_thread(bot)
        tg.document(USER, "s.csv", (FIXTURES / "export_basic.csv").read_bytes())
        assert entered.wait(5)
        tg.message(USER + 1, "/status")  # queued behind the import on the single worker
        wait_for(lambda: tg.texts(USER + 1) == ["📊 Checking…"])
        assert [p.name.startswith(".download-") for p in tmp.iterdir()] == [True]
        stop(bot, thread)
        assert exits == []
        assert tg.texts(USER)[-1] == (
            "⚠ The bot restarted while your import was running, so it may be incomplete. "
            "Please send the file again in a minute."
        )
        assert tg.texts(USER + 1)[-1] == (
            "⚠ The bot restarted before your status check could start. "
            "Please try again in a minute."
        )
        assert bot.load_offset() > 0  # both updates were confirmed

        again = Bot(
            telegram=client,
            users=users,
            admin_id=ADMIN,
            data_dir=data_dir,
            workers=WorkerPool(0),
            hard_exit=exits.append,
        )
        thread = run_in_thread(again)
        wait_for(lambda: list(tmp.iterdir()) == [])
        stop(again, thread)
    finally:
        release.set()


def test_sweep_keeps_files_that_are_not_downloads(data_dir: Path):
    from ynab_helper.bot.handlers import importing

    tmp = data_dir / "tmp"
    tmp.mkdir()
    (tmp / ".download-abc.csv").write_text("statement")
    (tmp / ".download-def.part").write_text("partial")
    (tmp / "keep.txt").write_text("not ours")
    assert importing.sweep_downloads(data_dir) == 2
    assert [p.name for p in tmp.iterdir()] == ["keep.txt"]
    assert importing.sweep_downloads(data_dir / "missing") == 0


# ── the real process (SIGTERM, unreachable Telegram) ─────────────────────────


def bot_env(tmp_path: Path, api_base: str) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("YNAB_", "CRYPTO_", "WEB_UI_", "TELEGRAM_", "LOG_", "BOT_"))
    }
    env.update(
        {
            "TELEGRAM_BOT_TOKEN": "123456:fake-telegram-token",
            "TELEGRAM_ADMIN_ID": str(ADMIN),
            "TELEGRAM_API_BASE": api_base,
            "BOT_DATA_DIR": str(tmp_path / "bot_data"),
            "LOG_FILE": "",
            "WEB_UI_ENABLED": "0",
            "HOME": str(tmp_path),
            "PYTHONPATH": str(REPO_ROOT),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    return env


def start_bot_process(tmp_path: Path, api_base: str) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, str(REPO_ROOT / "revolut_ynab_bot.py")],
        cwd=tmp_path,
        env=bot_env(tmp_path, api_base),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def terminate(process: subprocess.Popen[str]) -> tuple[float, int, str]:
    started = time.monotonic()
    process.send_signal(signal.SIGTERM)
    try:
        _, err = process.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        raise
    return time.monotonic() - started, process.returncode, err


def test_F40_sigterm_stops_the_bot_quickly(tg: FakeTelegram, tmp_path: Path):
    base = tg.start()
    process = start_bot_process(tmp_path, base)
    try:
        wait_for(lambda: any("Bot started" in t for t in tg.texts(ADMIN)), timeout=30)
        wait_for(lambda: len(tg.calls_of("getUpdates")) >= 2, timeout=10)
        elapsed, code, err = terminate(process)
    finally:
        if process.poll() is None:
            process.kill()
        tg.stop()
    assert code == 0, err
    assert elapsed < 8.0
    assert (tmp_path / "bot_data" / ".heartbeat").exists()
    startup = next(t for t in tg.texts(ADMIN) if "Bot started" in t)
    assert "web UI: disabled (turned off" in startup


def test_unreachable_telegram_keeps_the_bot_up(tmp_path: Path):
    """The CI smoke test runs the image with --network none: the bot must stay up."""
    process = start_bot_process(tmp_path, "http://127.0.0.1:9")
    try:
        wait_for(lambda: (tmp_path / "bot_data" / "bot_users.db").exists(), timeout=30)
        time.sleep(3)
        assert process.poll() is None
        elapsed, code, err = terminate(process)
    finally:
        if process.poll() is None:
            process.kill()
    assert code == 0, err
    assert elapsed < 8.0
    assert "getMe failed" in err


def test_callbacks_are_answered_even_when_the_gate_fails(bot: Bot, tg: FakeTelegram, monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(bot.gate, "check_callback", broken)
    tg.callback(USER, "st:close")
    deliver(bot, tg)
    assert len(tg.answers) == 1


@pytest.mark.usefixtures("ready")
def test_malformed_update_ids_never_break_the_loop(bot: Bot, tg: FakeTelegram):
    good = {
        "update_id": 77,
        "message": {
            "message_id": 1,
            "from": {"id": USER},
            "chat": {"id": USER, "type": "private"},
            "text": "/help",
        },
    }
    batches = [[{"update_id": "zzz", "message": {}}, good, {"update_id": None}]]
    real = bot.tg

    class Stub:
        def __getattr__(self, name: str) -> Any:
            return getattr(real, name)

        def get_updates(self, offset: int | None) -> list[dict[str, Any]]:
            if batches:
                return batches.pop()
            bot.request_stop()
            return []

    bot.tg = Stub()  # type: ignore[assignment]
    thread = run_in_thread(bot)
    thread.join(5)
    assert not thread.is_alive()
    assert bot.offset_path.read_text() == "78"
    assert len(tg.sent(USER)) == 1


def test_a_ready_row_without_a_complete_setup_goes_back_to_the_token_step(
    bot: Bot, tg: FakeTelegram, users: UsersRepo
):
    users.update(USER, state="ready", chat_id=USER, ynab_token=None, budget_id="b1")
    tg.message(USER, "/status")
    deliver(bot, tg)
    assert users.get(USER).state == "awaiting_token"
    texts = tg.texts(USER)
    assert "setup is incomplete" in texts[0]
    assert texts[-1] == "Please send your YNAB Personal Access Token first (or /cancel)."


# ── one definition of "busy" (R22) ───────────────────────────────────────────


def test_bot_R22_is_busy_definition(bot: Bot, users: UsersRepo, monkeypatch):
    from ynab_helper.locks import LOCKS

    ready_user(users)
    assert bot.is_busy(USER) is None
    with LOCKS.operation(str(USER), "web upload"):
        assert bot.is_busy(USER) == "web upload"
    for refresh in sorted(app_module.BACKGROUND_OPS):  # waited out by the jobs (R14)
        with LOCKS.operation(str(USER), refresh):
            assert bot.is_busy(USER) is None
    monkeypatch.setattr(bot.workers, "busy", lambda key: "import" if key == str(USER) else None)
    assert bot.is_busy(USER) == "import"
    assert bot.is_busy(USER + 1) is None


def test_bot_R22_handlers_and_jobs_share_one_busy_check(
    bot: Bot, tg: FakeTelegram, users: UsersRepo, monkeypatch
):
    """Handlers' ``busy()`` and ``start_job`` both ask :meth:`Bot.is_busy` and answer
    through :meth:`Bot.reply_busy`, so a change to either applies to every path."""
    ready_user(users)
    monkeypatch.setattr(Bot, "is_busy", lambda self, telegram_id: "something")
    tg.message(USER, "/setup")  # handlers/_common.busy
    tg.message(USER, "/status")  # Bot.start_job
    deliver(bot, tg)
    assert tg.texts(USER) == ["⏳ Another operation is still running."] * 2
    assert users.get(USER).state == "ready"
    tg.callback(USER, "st:setup")  # a button press gets the alert
    deliver(bot, tg)
    assert tg.answers[-1]["text"] == "⏳ Another operation is still running."
    assert tg.answers[-1]["show_alert"] is True
    monkeypatch.setattr(Bot, "is_busy", lambda self, telegram_id: None)
    tg.message(USER, "/setup")
    deliver(bot, tg)
    assert users.get(USER).state == "awaiting_token"


# ── dead API removed (R21) ───────────────────────────────────────────────────


def test_bot_R21_signal_path_is_main_and_dead_api_is_gone(bot: Bot):
    """SIGTERM/SIGINT reach the bot only through ``bot.main._StopRequest`` →
    :meth:`Bot.request_stop`; the unused duplicates are gone."""
    from ynab_helper.bot.context import Request
    from ynab_helper.bot.main import _StopRequest

    stop = _StopRequest()
    stop.bot = bot
    stop(signal.SIGTERM, None)
    assert stop.requested and bot.stopping
    assert not hasattr(Bot, "install_signal_handlers")
    assert not hasattr(Request, "refresh_user")
    assert not hasattr(TelegramClient, "call") and not hasattr(TelegramClient, "api_base")
