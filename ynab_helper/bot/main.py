"""Bot entry point (DESIGN §8.1 "Startup"; F57, F60, F94).

:func:`main` is what ``python3 revolut_ynab_bot.py``, ``python3 -m ynab_helper bot`` and the
container entrypoint run. In order:

1. SIGTERM/SIGINT handlers (a stop request is honoured in every later step);
2. ``.env`` (existing variables win) → :class:`~ynab_helper.config.Settings`; missing or
   malformed ``TELEGRAM_*`` variables → exit 1 with a clear message;
3. ``umask 077`` (every file the bot creates is private, F57) and logging (rotating file);
4. :func:`ynab_helper.storage.migrate_data_dir` — the users DB and every
   ``transactions_*.db`` are migrated (with backups) **before** the web thread starts; a
   per-user failure is reported to the admin, a users-DB failure stops the bot;
5. admin auto-registration from ``YNAB_*`` (only when the admin row is missing or not
   ready — v1 semantics);
6. the web UI, if ``WEB_UI_*`` is usable: :func:`ynab_helper.web.server.serve_in_thread`
   (imported lazily, so the bot runs without FastAPI loaded), waiting up to 10 s for it to
   accept connections. A startup failure is reported, never fatal (F94);
7. the bot: ``getMe`` (retried with backoff, never fatal), command menus, the admin's
   startup DM "🚀 Bot started <version> — web UI: …", then the poll loop until stopped.
"""

from __future__ import annotations

import importlib
import logging
import os
import signal
import sys
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Final

from ynab_helper import config
from ynab_helper.bot.app import Bot, WebAccess, stderr_line
from ynab_helper.bot.render import h
from ynab_helper.bot.telegram import TelegramClient
from ynab_helper.bot.users import UsersRepo
from ynab_helper.bot.workers import WorkerPool
from ynab_helper.config import Settings
from ynab_helper.logging_setup import setup_logging
from ynab_helper.storage import migrate_data_dir, open_users_db
from ynab_helper.storage.db import MigrationError
from ynab_helper.version import cache_bust_token, version_line
from ynab_helper.web.config import WebConfig, web_disabled_reason

__all__ = ["auto_register_admin", "main", "run", "startup_message"]

log = logging.getLogger(__name__)

#: How long startup waits for the web server to accept connections.
WEB_START_TIMEOUT: Final = 10.0
#: After the bot returned, the process must be gone this soon (docker stop budget).
EXIT_WATCHDOG: Final = 1.0
_PRIVATE_UMASK: Final = 0o077
_MAX_ERROR: Final = 200


class _StopRequest:
    """Set by the signal handlers; forwarded to the bot once it exists."""

    def __init__(self) -> None:
        self.requested = False
        self.bot: Bot | None = None

    def __call__(self, signum: int, _frame: object) -> None:
        self.requested = True  # only flag writes: nothing here may take a lock
        if self.bot is not None:
            self.bot.request_stop()


def _describe(exc: BaseException) -> str:
    text = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
    return text[:_MAX_ERROR]


def auto_register_admin(users: UsersRepo, settings: Settings) -> bool:
    """Make the admin a ready user from ``YNAB_TOKEN``/``YNAB_BUDGET_ID``/``YNAB_ACCOUNT_ID``
    (plus the optional crypto variables) — only when the admin row is missing or not ready,
    so a working setup chosen in the bot is never overwritten (v1). Returns True if written.
    """
    admin_id = settings.telegram.admin_id
    ynab = settings.ynab
    if admin_id is None or not ynab.complete:
        return False
    existing = users.get(admin_id)
    if existing is not None and existing.state == "ready":
        return False
    fields: dict[str, Any] = {
        "state": "ready",
        "ynab_token": ynab.token,
        "budget_id": ynab.budget_id,
        "account_id": ynab.account_id,
        "first_name": (existing.first_name if existing and existing.first_name else "Admin"),
        "temp_data": None,
    }
    crypto = settings.crypto
    if crypto.account_id:
        fields["crypto_account_id"] = crypto.account_id
    elif existing is not None and existing.budget_id and existing.budget_id != ynab.budget_id:
        fields["crypto_account_id"] = None  # it belonged to the other budget (C05)
        fields["crypto_account_name"] = None
    if crypto.btc_xpub:
        fields["btc_xpub"] = crypto.btc_xpub
    if crypto.eth_address:
        fields["eth_address"] = crypto.eth_address
    users.update(admin_id, **fields)
    log.info("admin %s auto-registered from the environment", admin_id)
    return True


def startup_message(
    *,
    version: str,
    web: WebAccess,
    ready_users: int,
    migration_failures: Mapping[Path, str],
) -> str:
    """The admin's startup DM (also the deploy notification, DESIGN §2.7)."""
    lines = [
        f"🚀 Bot started {h(version)} — web UI: {h(web.summary())}",
        f"Ready users: {ready_users}",
    ]
    for path, error in sorted(migration_failures.items()):
        lines.append(
            f"⚠ Migration of {h(path.name)} failed: {h(error[:_MAX_ERROR])} — that user's "
            "data is not usable until this is fixed (see the log)."
        )
    return "\n".join(lines)


def _wait_for_web(handle: Any, stop: _StopRequest, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while not stop.requested:
        if getattr(handle, "failed", None) is not None:
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        if handle.wait_started(min(0.25, remaining)):
            return getattr(handle, "failed", None) is None
    return False


def start_web(
    settings: Settings,
    *,
    users_lock: threading.RLock,
    telegram: TelegramClient,
    stop: _StopRequest,
    version: str,
) -> WebAccess:
    """Start the web UI if its configuration is usable; never raises (F60, F94)."""
    cfg, problems = WebConfig.from_settings(settings)
    for problem in problems:
        log.warning("web: %s", problem)
    reason = web_disabled_reason(cfg, problems)
    if reason is not None:
        log.info("web UI disabled: %s", reason)
        return WebAccess(cfg, None, "disabled", reason)

    def delete_telegram_message(chat_id: int, message_id: int) -> None:
        try:
            telegram.delete_message(chat_id, message_id)
        except Exception as exc:
            log.info("could not delete the login message %s: %s", message_id, exc)

    try:
        server = importlib.import_module("ynab_helper.web.server")
        handle = server.serve_in_thread(
            cfg,
            users_db_path=settings.users_db_path,
            data_dir=settings.data_dir,
            version_line=version,
            cache_bust=cache_bust_token(settings.environ),
            delete_telegram_message=delete_telegram_message,
            users_lock=users_lock,
        )
    except BaseException as exc:  # ImportError, a bind error raised early, SystemExit …
        log.error("web UI failed to start: %s", _describe(exc), exc_info=True)
        return WebAccess(cfg, None, "failed", _describe(exc))
    if _wait_for_web(handle, stop, WEB_START_TIMEOUT):
        log.info("web UI serving on %s:%d (public %s)", cfg.host, cfg.port, cfg.public_url)
        return WebAccess(cfg, handle, "enabled", cfg.public_url)
    failed = getattr(handle, "failed", None)
    if failed is not None:
        log.error("web UI failed to start: %s", _describe(failed))
        try:
            handle.stop(timeout=2.0)
        except Exception:
            log.exception("stopping the failed web server raised")
        return WebAccess(cfg, None, "failed", _describe(failed))
    if stop.requested:
        return WebAccess(cfg, handle, "failed", "stopped during startup")
    log.error("web UI did not start within %.0fs", WEB_START_TIMEOUT)
    return WebAccess(cfg, handle, "failed", f"not serving after {WEB_START_TIMEOUT:.0f}s")


def _arm_exit_watchdog(hard_exit: Callable[[int], None], code: int, delay: float) -> None:
    """End the process ``delay`` seconds from now unless it exited by itself — so a thread
    that is not a daemon (a library's) can never hold ``docker stop`` past its grace period."""

    def expire() -> None:
        time.sleep(delay)
        log.error("process still alive %.1fs after the bot stopped; exiting now", delay)
        for handler in logging.getLogger().handlers:
            try:
                handler.flush()
            except Exception:
                pass
        hard_exit(code)

    threading.Thread(target=expire, name="bot-exit-watchdog", daemon=True).start()


def run(
    *,
    environ: Mapping[str, str] | None = None,
    load_env_file: bool = True,
    install_signals: bool = True,
    telegram_opener: Any = None,
    workers: WorkerPool | None = None,
    hard_exit: Callable[[int], None] = os._exit,
    on_bot: Callable[[Bot], None] | None = None,
) -> int:
    """Run the bot (the parameters exist for tests); returns the process exit code."""
    stop = _StopRequest()
    production = install_signals and threading.current_thread() is threading.main_thread()
    if production:
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        code = _run(stop, environ, load_env_file, telegram_opener, workers, hard_exit, on_bot)
        _arm_exit_watchdog(hard_exit, code, EXIT_WATCHDOG)
        return code
    return _run(stop, environ, load_env_file, telegram_opener, workers, hard_exit, on_bot)


def _run(
    stop: _StopRequest,
    environ: Mapping[str, str] | None,
    load_env_file: bool,
    telegram_opener: Any,
    workers: WorkerPool | None,
    hard_exit: Callable[[int], None],
    on_bot: Callable[[Bot], None] | None,
) -> int:

    if environ is None and load_env_file:
        config.load_dotenv()
    settings = Settings.from_env(environ)
    errors = settings.validate_for_bot()
    if errors:
        for error in errors:
            sys.stderr.write(f"Error: {error}\n")
        sys.stderr.write("Set them in your .env file or as environment variables.\n")
        return 1
    admin_id = settings.telegram.admin_id
    assert admin_id is not None
    try:
        telegram = TelegramClient(
            settings.telegram.bot_token,
            api_base=settings.telegram.api_base,
            opener=telegram_opener,
        )
    except ValueError as exc:  # e.g. whitespace or "/" in the token: never a valid token
        sys.stderr.write(f"Error: TELEGRAM_BOT_TOKEN / TELEGRAM_API_BASE: {exc}.\n")
        return 1

    old_umask = os.umask(_PRIVATE_UMASK)
    os.umask(old_umask | _PRIVATE_UMASK)
    log_file = setup_logging(
        settings.log.level, settings.log.file, secrets=settings.secret_values()
    )
    for problem in settings.problems:
        log.warning("configuration: %s", problem)
    version = version_line(settings.environ)

    data_dir = settings.data_dir
    try:
        data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        migration = migrate_data_dir(data_dir, backup_dir=settings.backups_dir)
    except (MigrationError, OSError) as exc:
        log.critical("cannot prepare the data directory %s: %s", data_dir, exc, exc_info=True)
        sys.stderr.write(f"Error: cannot prepare the bot's data in {data_dir}: {exc}\n")
        return 1
    log.info(
        "data dir %s ready: users DB %s, %d transactions DB(s), %d failure(s)",
        data_dir,
        "migrated" if migration.users.changed else "current",
        len(migration.tx),
        len(migration.failures),
    )
    if stop.requested:
        return 0

    users_lock = threading.RLock()
    conn = open_users_db(
        settings.users_db_path, backup_dir=settings.backups_dir, check_same_thread=False
    )
    web = WebAccess()
    try:
        users = UsersRepo(conn, users_lock)
        auto_register_admin(users, settings)
        if stop.requested:
            return 0
        web = start_web(
            settings, users_lock=users_lock, telegram=telegram, stop=stop, version=version
        )
        bot = Bot(
            telegram=telegram,
            users=users,
            admin_id=admin_id,
            data_dir=data_dir,
            workers=workers,
            web=web,
            heartbeat_path=settings.heartbeat_path,
            log_file=log_file,
            hard_exit=hard_exit,
        )
        stop.bot = bot
        if stop.requested:
            bot.request_stop()
        if on_bot is not None:
            on_bot(bot)
        ready = sum(1 for user in users.list_all() if user.is_ready)
        message = startup_message(
            version=version,
            web=web,
            ready_users=ready,
            migration_failures=migration.failures,
        )
        stderr_line(f"🤖 Revolut → YNAB bot {version} starting (data dir {data_dir})")
        log.info("bot %s starting; web UI: %s", version, web.summary())
        return bot.run(startup_message=message)
    finally:
        stop.bot = None
        web.stop(2.0)
        conn.close()


def main() -> int:
    """Entry point: run the bot with the process environment."""
    return run()
