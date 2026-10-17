"""Bot runtime: dispatcher, background jobs and the long-poll loop (DESIGN §8.1).

**Dispatch.** :meth:`Bot.handle_update` parses an update, runs it through the
authorization gate (:mod:`ynab_helper.bot.auth`) and calls the handler from the registry
(:mod:`ynab_helper.bot.commands`). Callback queries are always answered, even when a
handler fails. A handler exception is logged with its traceback and the user gets a short
error; it never reaches the poll loop.

**Slow work** (YNAB, blockchains, downloads) runs on :class:`~ynab_helper.bot.workers.WorkerPool`
via :meth:`Bot.start_job`: the user gets an immediate "working…" reply, a second job of the
same user — or one while the web UI holds the user's service lock for an operation — gets
"⏳ Another operation is still running." (F16, F43). A mirror refresh nobody asked the bot
for (:data:`BACKGROUND_OPS`: the sync every web page view triggers) is not such an
operation: the job waits for it on its worker, up to :data:`BACKGROUND_WAIT` seconds as
the web's own actions do, instead of dropping a statement the user just sent (R14).
:meth:`Bot.is_busy` is the one definition of "busy" for the jobs and the handlers' checks.
Errors are reported with their ``user_message``.

**Polling** (F26, F40, F69):

* ``getUpdates`` long-polls on a helper thread while the main thread waits in short ticks,
  so SIGTERM is noticed within a quarter second even mid-poll;
* the offset is persisted (atomic write of ``telegram_offset``) after **each** handled
  update — at-least-once delivery; imports are idempotent through YNAB's import ids;
* failures back off exponentially from 1 s to 60 s (a 429's ``retry_after`` is honoured);
  so does ``getMe`` at startup — the bot never exits because Telegram is unreachable, however
  long the outage lasts (the exponent is capped, so the delay never overflows);
* replies the poll thread sends on a stranger's behalf (refusals, admin notices, callback
  answers) never wait out a 429, so a flood cannot stall dispatch for everyone;
* the heartbeat file is touched on every loop iteration, and every 15 s by a supervisor
  thread as long as the loop keeps making progress (a long poll counts, a stuck handler
  does not), so the Docker health check sees a hung loop.

**Shutdown.** SIGTERM/SIGINT only set a flag (nothing that takes a lock runs in the signal
handler). The loop stops after the current update, queued jobs are dropped, running ones
get a few seconds, the users whose job was dropped or cut off are told to try again
(best effort, within :data:`NOTICE_BUDGET`), the web server is stopped, and
:meth:`Bot.run` returns. Should anything hang, the supervisor ends the process
:data:`HARD_EXIT_AFTER` seconds after the signal, so ``docker stop`` (10 s grace) never has
to SIGKILL it. Statement downloads an interrupted import left in ``<data>/tmp`` are deleted
at the next start, before polling.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from ynab_helper.bot import render
from ynab_helper.bot.auth import AuthGate
from ynab_helper.bot.commands import Registry, build_registry
from ynab_helper.bot.context import Incoming, Request, parse_command, parse_update
from ynab_helper.bot.telegram import TelegramClient, html_to_plain
from ynab_helper.bot.users import UsersRepo
from ynab_helper.bot.workers import ShutdownReport, WorkerPool
from ynab_helper.errors import (
    Busy,
    ConfigError,
    CsvError,
    StaleStateError,
    TelegramError,
    YnabAuthError,
    YnabHelperError,
)
from ynab_helper.locks import LOCKS
from ynab_helper.maintenance.dedupe import DEDUPE_SCANS
from ynab_helper.services import UserContext
from ynab_helper.services.status import OP_STATUS, OP_SYNC
from ynab_helper.storage.users import User

__all__ = ["BACKGROUND_OPS", "BACKGROUND_WAIT", "HARD_EXIT_AFTER", "Bot", "FlowMemory", "WebAccess"]

log = logging.getLogger(__name__)

OFFSET_FILENAME: Final = "telegram_offset"
#: Poll-loop wait granularity (how fast a stop request is noticed).
TICK: Final = 0.25
#: Backoff bounds for getUpdates/getMe failures (F26).
BACKOFF_MIN: Final = 1.0
BACKOFF_MAX: Final = 60.0
#: The backoff exponent stops growing here (2**16 s is far past the cap), so an outage of
#: any length never overflows the delay computation.
_BACKOFF_EXPONENT_CAP: Final = 16
#: Longest wait a Telegram 429 may impose on the poll loop.
MAX_POLL_RETRY_AFTER: Final = 300.0
#: Supervisor heartbeat interval and how long the loop may go without progress.
HEARTBEAT_INTERVAL: Final = 15.0
STALL_LIMIT: Final = 120.0
#: Time running jobs get at shutdown, and the web server's stop timeout.
JOB_GRACE: Final = 4.0
WEB_STOP_TIMEOUT: Final = 2.0
#: Total time for telling users that the shutdown dropped or cut off their job.
NOTICE_BUDGET: Final = 1.0
#: The process ends this many seconds after a stop signal, whatever still runs.
HARD_EXIT_AFTER: Final = 7.5
#: Lock holders that only refresh the user's mirror and end within YNAB's timeouts (the
#: web's background sync, a status read): a job waits for them instead of answering busy.
BACKGROUND_OPS: Final = frozenset({OP_SYNC, OP_STATUS})
#: How long a job waits for such a refresh (the web's actions wait as long: IDLE_WAIT).
BACKGROUND_WAIT: Final = 10.0
_BACKGROUND_POLL: Final = 0.05

_GENERIC_ERROR: Final = "❌ Something went wrong — the error was logged. Please try again."
_EXPECTED_ERRORS: Final = (CsvError, ConfigError, StaleStateError)


# ── web UI access (for /login and the shutdown) ──────────────────────────────


@dataclass
class WebAccess:
    """The web UI as the bot sees it: its config, the server handle and a status line for
    the admin (``enabled`` / ``disabled`` / ``failed`` plus details)."""

    config: Any = None  # ynab_helper.web.config.WebConfig
    handle: Any = None  # WebServerHandle from ynab_helper.web.server.serve_in_thread
    status: str = "disabled"
    detail: str = ""

    def usable(self) -> bool:
        """True while the server runs: it started (possibly after the startup DM) and has
        not failed since (``/login`` needs this)."""
        handle = self.handle
        if self.status == "disabled" or handle is None or self.config is None:
            return False
        started = getattr(handle, "started", None)
        if started is None or not started.is_set():
            return False
        return getattr(handle, "failed", None) is None

    def summary(self) -> str:
        if self.status == "enabled":
            return f"enabled at {self.detail}"
        return f"{self.status} ({self.detail})" if self.detail else self.status

    def stop(self, timeout: float) -> None:
        """Stop the web server (once; later calls do nothing)."""
        handle, self.handle = self.handle, None
        if handle is None:
            return
        try:
            handle.stop(timeout=timeout)
        except Exception:
            log.exception("stopping the web server failed")


# ── per-user in-memory flow state ────────────────────────────────────────────


class FlowMemory:
    """In-memory state of multi-step flows (reconcile previews, dedupe pickers), one entry
    per ``(kind, user)``. Lost on restart by design: every entry has a nonce, and a button
    with an unknown nonce simply expires."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[tuple[str, int], Any] = {}

    def put(self, kind: str, telegram_id: int, value: Any) -> None:
        with self._lock:
            self._data[(kind, telegram_id)] = value

    def get(self, kind: str, telegram_id: int) -> Any:
        with self._lock:
            return self._data.get((kind, telegram_id))

    def pop(self, kind: str, telegram_id: int, *, if_is: Any = None) -> Any:
        """Remove and return the entry (only if it is ``if_is``, when given)."""
        with self._lock:
            value = self._data.get((kind, telegram_id))
            if value is None or (if_is is not None and value is not if_is):
                return None
            del self._data[(kind, telegram_id)]
            return value

    def forget(self, telegram_id: int) -> None:
        with self._lock:
            for key in [k for k in self._data if k[1] == telegram_id]:
                del self._data[key]


# ── heartbeat ────────────────────────────────────────────────────────────────


class Heartbeat:
    """Touches the heartbeat file; :meth:`progress` marks the loop as alive."""

    def __init__(self, path: Path | None, clock: Callable[[], float] = time.monotonic) -> None:
        self.path = path
        self._clock = clock
        self.last_progress = clock()
        self._failing = False

    def progress(self) -> None:
        self.last_progress = self._clock()

    def beat(self) -> None:
        self.progress()
        self.touch()

    def touch(self) -> None:
        if self.path is None:
            return
        try:
            self.path.touch(exist_ok=True)
            os.utime(self.path)
        except OSError as exc:
            if not self._failing:
                log.warning("cannot touch the heartbeat file %s: %s", self.path, exc)
            self._failing = True
        else:
            self._failing = False

    def stalled(self, limit: float) -> bool:
        return self._clock() - self.last_progress > limit


# ── the bot ──────────────────────────────────────────────────────────────────


class Bot:
    """The Telegram bot: dispatcher, jobs and runtime (see the module docstring)."""

    def __init__(
        self,
        *,
        telegram: TelegramClient,
        users: UsersRepo,
        admin_id: int,
        data_dir: Path,
        registry: Registry | None = None,
        workers: WorkerPool | None = None,
        web: WebAccess | None = None,
        heartbeat_path: Path | None = None,
        log_file: Path | None = None,
        wait: Callable[[float], bool] | None = None,
        hard_exit: Callable[[int], None] = os._exit,
        clock: Callable[[], float] = time.monotonic,
        ip_lookup: Callable[[], str] | None = None,
        background_wait: float = BACKGROUND_WAIT,
    ) -> None:
        self.tg = telegram
        self.users = users
        self.admin_id = admin_id
        self.data_dir = Path(data_dir)
        self.registry = registry or build_registry()
        self.workers = workers if workers is not None else WorkerPool()
        self.web = web or WebAccess()
        self.log_file = log_file
        self.memory = FlowMemory()
        self.ip_lookup = ip_lookup
        self.background_wait = float(background_wait)
        self.bot_username: str | None = None
        self.clock = clock
        self.gate = AuthGate(users, admin_id=admin_id, normalize=self._normalize_user, clock=clock)
        self.heartbeat = Heartbeat(heartbeat_path, clock)
        self._wait_impl = wait or self._interruptible_wait
        self._hard_exit = hard_exit
        self._stop_flag = False
        self._stop_at: float | None = None
        self._finished = threading.Event()
        self.offset_path = self.data_dir / OFFSET_FILENAME

    # ── replies (never raise) ──

    def send(
        self,
        chat_id: int,
        text: str,
        markup: Mapping[str, Any] | None = None,
        *,
        best_effort: bool = False,
        timeout: float | None = None,
    ) -> dict[str, Any] | None:
        """Send a message; None if it failed (logged). ``best_effort`` messages are not
        retried after a 429 (refusals, notices: nothing may park the poll thread)."""
        try:
            return self.tg.send_message(
                chat_id,
                text,
                reply_markup=markup,
                wait_on_rate_limit=not best_effort,
                timeout=timeout,
            )
        except TelegramError as exc:
            log.warning("sending a message to %s failed: %s", chat_id, exc)
        except Exception:
            log.exception("sending a message to %s failed", chat_id)
        return None

    def edit(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        markup: Mapping[str, Any] | None = None,
        *,
        fallback: bool = True,
    ) -> bool:
        """Edit a message; if that fails and ``fallback``, send ``text`` as a new message."""
        try:
            self.tg.edit_message_text(chat_id, message_id, text, reply_markup=markup)
            return True
        except TelegramError as exc:
            log.info("editing message %s failed: %s", message_id, exc)
        except Exception:
            log.exception("editing message %s failed", message_id)
        if fallback:
            self.send(chat_id, text, markup)
        return False

    def remove_keyboard(self, chat_id: int, message_id: int, *, best_effort: bool = False) -> bool:
        """Remove a message's inline keyboard; False if that failed (logged)."""
        try:
            self.tg.edit_message_reply_markup(
                chat_id, message_id, wait_on_rate_limit=not best_effort
            )
            return True
        except TelegramError as exc:
            log.info("removing the keyboard of message %s failed: %s", message_id, exc)
        except Exception:
            log.exception("removing the keyboard of message %s failed", message_id)
        return False

    def answer_callback(self, callback_id: str, text: str | None, *, alert: bool = False) -> None:
        """Answer a button press (plain text). Never waits out a 429: an answer is only
        useful within seconds, and the poll thread must not stall on it."""
        try:
            self.tg.answer_callback_query(
                callback_id, text, show_alert=alert, wait_on_rate_limit=False
            )
        except TelegramError as exc:
            log.info("answering a callback failed: %s", exc)
        except Exception:
            log.exception("answering a callback failed")

    def delete_message(self, chat_id: int, message_id: int) -> bool:
        try:
            return self.tg.delete_message(chat_id, message_id)
        except TelegramError as exc:
            log.info("deleting message %s failed: %s", message_id, exc)
        except Exception:
            log.exception("deleting message %s failed", message_id)
        return False

    def notify_admin(self, text: str, *, best_effort: bool = False) -> None:
        self.send(self.admin_id, text, best_effort=best_effort)

    # ── users ──

    def user_context(self, user: User) -> UserContext:
        """The service context of a ready user (ConfigError if the setup is incomplete)."""
        return UserContext.from_user_row(user, self.data_dir)

    def forget_user(self, telegram_id: int) -> None:
        """Drop every in-memory flow of a user (deny, setup change)."""
        self.memory.forget(telegram_id)
        DEDUPE_SCANS.discard(str(telegram_id))

    def _normalize_user(self, user: User) -> User:
        """Repair states that would trap a user (run by the gate for every update)."""
        from ynab_helper.bot.handlers import crypto, onboarding

        if user.state == "ready" and not user.is_ready:
            # A ready row without token, budget or account (hand-edited or v1 damage):
            # services would refuse every command, so send the user back to the token step.
            updated = self.users.update_if(
                user.telegram_id, states={"ready"}, state="awaiting_token", temp_data=None
            )
            if updated is not None:
                log.warning("user %s was ready without a complete setup", user.telegram_id)
                self.send(
                    user.telegram_id,  # the private chat (a v1 chat_id may be a group: F56)
                    "Your YNAB setup is incomplete. " + render.h(onboarding.TOKEN_PROMPT),
                )
                return updated
            return self.users.get(user.telegram_id) or user
        return crypto.expire_stale_flow(self, user)

    # ── jobs ──

    def is_busy(self, telegram_id: int) -> str | None:
        """The operation a new one of this user must not overlap, or None: a bot job queued
        or running for them, or the service lock held by anything but a
        :data:`BACKGROUND_OPS` refresh (jobs wait those out, see :meth:`start_job`)."""
        key = str(telegram_id)
        job = self.workers.busy(key)
        if job:
            return job
        holder = LOCKS.current(key)
        return None if holder is None or holder in BACKGROUND_OPS else holder

    @staticmethod
    def reply_busy(req: Request) -> None:
        """Tell the sender another operation runs (an alert for a button press)."""
        if req.incoming.kind == "callback":
            req.answer(render.BUSY_TEXT, alert=True)
        else:
            req.reply(render.BUSY_TEXT)

    def _await_background(self, key: str) -> bool:
        """Wait (on a worker) until no :data:`BACKGROUND_OPS` refresh holds ``key``'s lock;
        False if one still does after :attr:`background_wait` seconds or at shutdown.
        Real time, not :attr:`clock` (a test double there must not stall a worker)."""
        deadline = time.monotonic() + self.background_wait
        while LOCKS.current(key) in BACKGROUND_OPS:
            if self._stop_flag or time.monotonic() >= deadline:
                return False
            time.sleep(_BACKGROUND_POLL)
        return True

    def _run_patiently(self, key: str, job: Callable[[], None]) -> None:
        """Run ``job`` once no background refresh holds the user's lock; if one took the lock
        while the job ran (its service raised Busy naming it), wait for it and run the job
        once more. Safe because a job's Busy comes from its service's lock acquisition,
        which precedes anything the service writes and anything the job reports."""
        for attempt in (0, 1):
            if not self._await_background(key):
                raise Busy(LOCKS.current(key) or OP_SYNC)
            try:
                job()
                return
            except Busy as exc:
                if attempt or exc.operation not in BACKGROUND_OPS:
                    raise
                log.info("job of user %s met a background %s; running it again", key, exc.operation)

    def start_job(
        self,
        req: Request,
        op: str,
        job: Callable[[], None],
        *,
        working: str | None = None,
    ) -> bool:
        """Run ``job`` on a worker for the sender (see the module docstring).

        ``working`` (HTML) is sent before the job starts; for a button press it becomes the
        callback answer, which Telegram shows as plain text, so its markup is stripped.
        Returns False if the user already has an operation running (:meth:`is_busy`). A
        background refresh holding the user's lock is waited out on the worker (R14).
        """
        key = str(req.user_id)
        if self.is_busy(req.user_id):
            self.reply_busy(req)
            return False
        if working:
            if req.incoming.kind == "callback":
                req.answer(html_to_plain(working))
            else:
                req.reply(working)

        def run() -> None:
            try:
                self._run_patiently(key, job)
            except Busy:
                req.reply(render.BUSY_TEXT)
            except YnabAuthError as exc:
                log.warning("%s for user %s: YNAB rejected the token", op, key)
                req.reply(f"❌ {render.h(exc.user_message)} Run /setup to enter a new token.")
            except _EXPECTED_ERRORS as exc:
                log.info("%s for user %s refused: %s", op, key, exc)
                req.reply(f"❌ {render.h(exc.user_message)}")
            except YnabHelperError as exc:
                log.warning("%s for user %s failed: %s", op, key, exc, exc_info=True)
                req.reply(f"❌ {render.h(exc.user_message)}")
            except Exception:
                log.exception("%s for user %s failed", op, key)
                req.reply(_GENERIC_ERROR)

        if not self.workers.submit(key, op, run):
            self.reply_busy(req)
            return False
        return True

    # ── dispatch ──

    def handle_update(self, update: Mapping[str, Any]) -> None:
        """Handle one update (never raises)."""
        incoming = parse_update(update)
        if incoming is None:
            log.debug("ignored an update of an unsupported type")
            return
        try:
            if incoming.kind == "callback":
                self._on_callback(incoming)
            else:
                self._on_message(incoming)
        except Exception:
            log.exception("handling update %s failed", incoming.update_id)
            if incoming.kind == "message" and incoming.is_private and incoming.chat_id:
                self.send(incoming.chat_id, _GENERIC_ERROR, best_effort=True)

    def _on_message(self, incoming: Incoming) -> None:
        parsed = parse_command(incoming.text) if incoming.document is None else None
        is_admin = incoming.from_id is not None and incoming.from_id == self.admin_id
        command = (
            self.registry.command(parsed.name, is_admin=is_admin)
            if parsed is not None and parsed.name
            else None
        )
        decision = self.gate.check_message(incoming, parsed, command)
        if decision.admin_notice:
            self.notify_admin(decision.admin_notice, best_effort=True)
        if not decision.allow:
            # Refusals go to people who may not be approved: best effort, never a 429 wait.
            target = decision.reply_chat or (incoming.chat_id if incoming.is_private else None)
            if decision.text and target is not None:
                self.send(target, decision.text, best_effort=True)
            return
        user = decision.user
        if user is not None and user.chat_id != incoming.chat_id and incoming.chat_id:
            user = self.users.update(user.telegram_id, chat_id=incoming.chat_id)
        req = Request(self, incoming, user, parsed)
        what = f"/{command.name}" if command else ("document" if incoming.document else "text")
        log.info("update %s: %s from user %s", incoming.update_id, what, incoming.from_id)

        from ynab_helper.bot.handlers import general, importing

        if command is not None:
            command.handler(req)
        elif incoming.document is not None:
            importing.on_document(req)
        else:
            general.on_text(req)

    def _on_callback(self, incoming: Incoming) -> None:
        req = Request(self, incoming, None)
        try:  # the press is answered whatever happens below
            route = self.registry.callback(incoming.data or "")
            decision = self.gate.check_callback(incoming, route)
            req.user = decision.user
            if not decision.allow or route is None:
                req.answer(decision.text)
                if decision.remove_keyboard and incoming.chat_id and incoming.message_id:
                    self.remove_keyboard(incoming.chat_id, incoming.message_id, best_effort=True)
                return
            log.info(
                "update %s: button %s: from user %s",
                incoming.update_id,
                route.prefix,
                incoming.from_id,
            )
            route.handler(req)
        finally:
            req.answer()

    # ── runtime ──

    @property
    def stopping(self) -> bool:
        return self._stop_flag

    def request_stop(self) -> None:
        """Ask the loop to stop (safe from signal handlers: only sets a flag)."""
        self._stop_flag = True

    def _interruptible_wait(self, seconds: float) -> bool:
        """Sleep up to ``seconds`` in short ticks; False as soon as a stop is requested."""
        deadline = self.clock() + seconds
        while not self._stop_flag:
            remaining = deadline - self.clock()
            if remaining <= 0:
                return True
            time.sleep(min(TICK, remaining))
            self.heartbeat.progress()
        return False

    def _wait(self, seconds: float) -> bool:
        return self._wait_impl(seconds) and not self._stop_flag

    @staticmethod
    def _backoff(attempt: int, error: BaseException | None = None) -> float:
        """Delay before retry ``attempt`` (0-based): 1, 2, 4 … capped at 60 s, or a 429's
        ``retry_after`` (up to :data:`MAX_POLL_RETRY_AFTER`). Defined for any ``attempt``."""
        exponent = min(max(attempt, 0), _BACKOFF_EXPONENT_CAP)
        delay = min(BACKOFF_MAX, BACKOFF_MIN * (2**exponent))
        retry_after = getattr(error, "retry_after", None)
        if isinstance(error, TelegramError) and error.error_code == 429 and retry_after:
            delay = max(delay, min(float(retry_after), MAX_POLL_RETRY_AFTER))
        return delay

    def run(self, *, startup_message: str | None = None) -> int:
        """Start up, poll until a stop is requested, shut down. Returns the exit code (0)."""
        supervisor = threading.Thread(target=self._supervise, name="bot-supervisor", daemon=True)
        supervisor.start()
        self.heartbeat.beat()
        try:
            self._sweep_downloads()
            self.workers.start()
            if self._startup(startup_message):
                self._loop()
        finally:
            self._shutdown()
            self._finished.set()
        return 0

    def _supervise(self) -> None:
        """Keep the heartbeat fresh while the loop progresses; end a hung shutdown."""
        next_beat = self.clock() + HEARTBEAT_INTERVAL
        while not self._finished.wait(TICK):
            now = self.clock()
            if self._stop_flag:
                if self._stop_at is None:
                    self._stop_at = now
                elif now - self._stop_at > HARD_EXIT_AFTER:
                    log.error("shutdown took longer than %.1fs; exiting now", HARD_EXIT_AFTER)
                    for handler in logging.getLogger().handlers:
                        try:
                            handler.flush()
                        except Exception:
                            pass
                    self._hard_exit(0)
                    return
            if now >= next_beat:
                next_beat = now + HEARTBEAT_INTERVAL
                if not self.heartbeat.stalled(STALL_LIMIT):
                    self.heartbeat.touch()
                else:
                    log.error("the poll loop made no progress for %.0fs", STALL_LIMIT)

    def _startup(self, startup_message: str | None) -> bool:
        """getMe with backoff (never gives up), register commands, DM the admin."""
        attempt = 0
        while not self._stop_flag:
            self.heartbeat.beat()
            try:
                me = self._interruptibly(self.tg.get_me)
            except Exception as exc:
                delay = self._backoff(attempt, exc)
                attempt = min(attempt + 1, _BACKOFF_EXPONENT_CAP)
                log.warning("Telegram getMe failed (%s); retrying in %.0fs", exc, delay)
                if not self._wait(delay):
                    return False
                continue
            if me is None:  # stop requested while waiting
                return False
            self.bot_username = me.get("username") if isinstance(me.get("username"), str) else None
            break
        if self._stop_flag:
            return False
        log.info("bot @%s connected to Telegram", self.bot_username or "?")
        self.register_commands()
        if startup_message:
            self.notify_admin(startup_message)
        return True

    def register_commands(self) -> None:
        """Push the command menus (default scope; the admin's chat scope). Best effort."""
        for menu, scope in (
            (self.registry.menu(admin=False), {"type": "default"}),
            (self.registry.menu(admin=True), {"type": "chat", "chat_id": self.admin_id}),
        ):
            try:
                self.tg.set_my_commands(menu, scope=scope)
            except Exception as exc:
                log.warning("setMyCommands(%s) failed: %s", scope["type"], exc)

    def _loop(self) -> None:
        offset = self.load_offset()
        attempt = 0
        while not self._stop_flag:
            self.heartbeat.beat()
            try:
                updates = self._poll(offset)
            except Exception as exc:
                delay = self._backoff(attempt, exc)
                attempt = min(attempt + 1, _BACKOFF_EXPONENT_CAP)
                log.warning("Telegram getUpdates failed (%s); retrying in %.0fs", exc, delay)
                if not self._wait(delay):
                    break
                continue
            if updates is None:  # stop requested while polling
                break
            attempt = 0
            for update in sorted(updates, key=_update_order):
                if self._stop_flag:
                    break
                update_id = update.get("update_id")
                if not isinstance(update_id, int) or isinstance(update_id, bool):
                    continue
                if update_id < offset:
                    continue
                self.heartbeat.beat()
                self.handle_update(update)
                offset = update_id + 1
                self.save_offset(offset)

    def _poll(self, offset: int) -> list[dict[str, Any]] | None:
        """getUpdates (see :meth:`_interruptibly`); None if a stop was requested."""
        return self._interruptibly(lambda: self.tg.get_updates(offset or None))

    def _interruptibly(self, call: Callable[[], Any]) -> Any:
        """Run a (long) Telegram call on a helper thread while this thread waits in short
        ticks, so a stop request is noticed at once. Returns None if a stop was requested
        (the call is abandoned: its updates stay unconfirmed); re-raises its exception."""
        box: dict[str, Any] = {}
        done = threading.Event()

        def target() -> None:
            try:
                box["result"] = call()
            except Exception as exc:  # handed to the caller
                box["error"] = exc
            finally:
                done.set()

        threading.Thread(target=target, name="bot-poll", daemon=True).start()
        while not done.wait(TICK):
            if self._stop_flag:
                return None
            self.heartbeat.progress()
        if "error" in box:
            raise box["error"]
        if "result" not in box:
            raise RuntimeError("the Telegram call ended without a result")
        return box["result"]

    def _shutdown(self) -> None:
        log.info("bot stopping")
        report = self.workers.shutdown(JOB_GRACE)
        self._tell_interrupted(report)
        self.web.stop(WEB_STOP_TIMEOUT)
        log.info("bot stopped")

    def _tell_interrupted(self, report: ShutdownReport) -> None:
        """Tell every user whose job the shutdown dropped or cut off to try again — their
        update is already confirmed, so Telegram will not deliver it again. Best effort:
        no 429 waits, at most :data:`NOTICE_BUDGET` seconds in total."""
        notices = [(job, False) for job in report.dropped]
        notices += [(job, True) for job in report.abandoned]
        if not notices:
            return
        deadline = time.monotonic() + NOTICE_BUDGET
        for (key, op), started in notices:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                log.warning("shutdown: no time left to tell user %s about %s", key, op)
                continue
            if not (key.isascii() and key.lstrip("-").isdigit()):
                continue
            # Jobs are keyed by the sender's id, which is their private chat (F56).
            self.send(
                int(key),
                render.interrupted(op, started=started),
                best_effort=True,
                timeout=remaining,
            )

    def _sweep_downloads(self) -> None:
        """Delete statement downloads an interrupted import left behind (financial data)."""
        from ynab_helper.bot.handlers import importing

        removed = importing.sweep_downloads(self.data_dir)
        if removed:
            log.info("removed %d stale statement download(s)", removed)

    # ── offset ──

    def load_offset(self) -> int:
        """The persisted next ``getUpdates`` offset (0 if none or unreadable)."""
        try:
            text = self.offset_path.read_text(encoding="ascii").strip()
        except FileNotFoundError:
            return 0
        except (OSError, UnicodeDecodeError) as exc:
            log.warning(
                "cannot read %s (%s); starting from the oldest update", self.offset_path, exc
            )
            return 0
        if not (text.isascii() and text.isdigit()):
            log.warning("ignoring a malformed %s", self.offset_path.name)
            return 0
        return int(text)

    def save_offset(self, offset: int) -> None:
        """Atomically persist ``offset`` (tmp file + fsync + rename)."""
        tmp = self.offset_path.with_name(self.offset_path.name + ".tmp")
        try:
            with open(tmp, "w", encoding="ascii") as handle:
                handle.write(str(int(offset)))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.offset_path)
        except OSError as exc:
            log.warning("cannot save the Telegram offset: %s", exc)


def _update_order(update: Mapping[str, Any]) -> int:
    value = update.get("update_id")
    return value if isinstance(value, int) and not isinstance(value, bool) else -1


def stderr_line(text: str) -> None:
    """The bot's one startup line on stderr (everything else goes through logging)."""
    try:
        sys.stderr.write(text + "\n")
        sys.stderr.flush()
    except (OSError, ValueError):
        pass
