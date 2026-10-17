"""The authorization gate (DESIGN §8.2; findings F03, F56).

Every message **and** every callback query passes :class:`AuthGate` before any handler
runs; there is no other path to a handler.

* Only private chats count: an update whose chat is not ``private`` (or whose chat id is
  not the sender's id) is ignored — for a callback the chat is ``callback_query.message.chat``.
  A command in a group gets one "Please message me directly." per group; nothing is written.
* The sender is ``from.id`` (never the chat). Bots are ignored.
* An unknown sender's **message** registers them as ``pending`` and notifies the admin —
  once per sender (the row exists afterwards) and at most :data:`NOTICE_LIMIT` notices per
  :data:`NOTICE_WINDOW` seconds overall. A pending or denied sender (or one in an unknown
  state) gets the fixed reply at most once per :data:`REFUSAL_INTERVAL` seconds; further
  messages are dropped silently, so forwarding a hundred messages costs one reply, not a
  hundred (the dispatcher also sends refusals without waiting out a 429).
  An unknown, pending or denied sender's **callback**
  gets "Not authorized", the keyboard is removed and nothing is written: buttons can never
  register, re-onboard or un-deny anyone (F03).
* Each command and callback route declares the user states it accepts (see
  :mod:`ynab_helper.bot.commands`); anything else gets a short explanation (callbacks:
  "expired" plus keyboard removal).
* Admin commands match exactly and only for ``TELEGRAM_ADMIN_ID``; they work whatever the
  admin's own user state is (v1), for everyone else they do not exist.

The gate writes only the ``pending`` row of a new sender; the dispatcher sends the texts
the returned :class:`Decision` carries.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

from ynab_helper.bot import render
from ynab_helper.bot import states as S
from ynab_helper.bot.commands import CallbackRoute, Command
from ynab_helper.bot.context import Incoming, ParsedCommand
from ynab_helper.bot.users import UsersRepo
from ynab_helper.storage.users import User

__all__ = [
    "DENIED_TEXT",
    "NOT_AUTHORIZED",
    "PENDING_TEXT",
    "WELCOME_TEXT",
    "AuthGate",
    "Decision",
    "RateLimiter",
]

log = logging.getLogger(__name__)

NOT_AUTHORIZED: Final = "Not authorized."
DIRECT_ONLY: Final = "Please message me directly."
WELCOME_TEXT: Final = (
    "Welcome! Your access request has been sent to the admin.\n"
    "You'll be notified once you're approved."
)
PENDING_TEXT: Final = "Your request is still pending admin approval. Hang tight!"
DENIED_TEXT: Final = (
    "Your access has been denied. Contact the admin if you think this is a mistake."
)
UNKNOWN_COMMAND: Final = "Unknown command. Send /help for available commands."
UNKNOWN_BUTTON: Final = "This button is no longer valid."
#: At most this many new-user notices reach the admin per window (flood protection).
NOTICE_LIMIT: Final = 10
NOTICE_WINDOW: Final = 600.0
#: A sender who is not approved gets the same refusal at most once per this many seconds.
REFUSAL_INTERVAL: Final = 300.0
_MAX_GROUPS: Final = 1024
_MAX_REFUSED_SENDERS: Final = 4096

_SETUP_REFUSALS: Final = {
    S.AWAITING_TOKEN: "Please send your YNAB Personal Access Token first (or /cancel).",
    S.AWAITING_BUDGET: "Please pick your budget first (or /cancel).",
    S.AWAITING_ACCOUNT: "Please pick your account first (or /cancel).",
}
_CRYPTO_REFUSAL: Final = "Please finish the crypto setup first, or send /cancel."


@dataclass(frozen=True, slots=True)
class Decision:
    """What the gate decided.

    ``allow`` — run the handler. Otherwise ``text`` (if any) is the reply to the sender (a
    message) or the callback answer, ``reply_chat`` overrides where a message reply goes
    (a group), ``remove_keyboard`` asks to strip the pressed button's keyboard and
    ``admin_notice`` is a DM for the admin (new access request).
    """

    allow: bool
    user: User | None = None
    text: str | None = None
    reply_chat: int | None = None
    remove_keyboard: bool = False
    admin_notice: str | None = None


_IGNORE: Final = Decision(False)


class RateLimiter:
    """At most ``limit`` events per sliding ``window`` seconds (thread-safe)."""

    def __init__(
        self, limit: int, window: float, *, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._limit = limit
        self._window = window
        self._clock = clock
        self._events: deque[float] = deque()
        self._lock = threading.Lock()

    def allow(self) -> bool:
        with self._lock:
            now = self._clock()
            while self._events and now - self._events[0] >= self._window:
                self._events.popleft()
            if len(self._events) >= self._limit:
                return False
            self._events.append(now)
            return True


def refusal_for_state(state: str) -> str:
    """Why a command is not available in ``state`` (an approved user)."""
    if state in _SETUP_REFUSALS:
        return _SETUP_REFUSALS[state]
    if state in S.CRYPTO_STATES:
        return _CRYPTO_REFUSAL
    return "That is not available right now."


def document_refusal(state: str) -> str:
    if state in S.CRYPTO_STATES:
        return "Please finish the crypto setup or send /cancel first — your file was not imported."
    return "Please finish the setup first (or /cancel) — your file was not imported."


class AuthGate:
    """See the module docstring."""

    def __init__(
        self,
        users: UsersRepo,
        *,
        admin_id: int,
        normalize: Callable[[User], User] | None = None,
        notices: RateLimiter | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.users = users
        self.admin_id = admin_id
        self._normalize = normalize or (lambda user: user)
        self._notices = notices or RateLimiter(NOTICE_LIMIT, NOTICE_WINDOW, clock=clock)
        self._clock = clock
        self._groups: OrderedDict[int, None] = OrderedDict()
        self._groups_lock = threading.Lock()
        self._refused: OrderedDict[int, float] = OrderedDict()  # sender → last refusal sent
        self._refused_lock = threading.Lock()

    # ── helpers ──

    def _load(self, telegram_id: int) -> User | None:
        user = self.users.get(telegram_id)
        return self._normalize(user) if user is not None else None

    def _first_time_in_group(self, chat_id: int | None) -> bool:
        if chat_id is None:
            return False
        with self._groups_lock:
            if chat_id in self._groups:
                return False
            self._groups[chat_id] = None
            while len(self._groups) > _MAX_GROUPS:
                self._groups.popitem(last=False)
            return True

    def _refusal(self, telegram_id: int, text: str) -> str | None:
        """``text`` if this sender has not been refused within :data:`REFUSAL_INTERVAL`
        seconds (and records it), else None: the message is dropped without a reply."""
        now = self._clock()
        with self._refused_lock:
            last = self._refused.get(telegram_id)
            if last is not None and now - last < REFUSAL_INTERVAL:
                return None
            self._refused[telegram_id] = now
            self._refused.move_to_end(telegram_id)
            while len(self._refused) > _MAX_REFUSED_SENDERS:
                self._refused.popitem(last=False)
        return text

    def _refuse(self, user: User, text: str) -> Decision:
        reply = self._refusal(user.telegram_id, text)
        if reply is None:
            log.debug(
                "dropped a message of %s user %s (refusal sent recently)",
                user.state,
                user.telegram_id,
            )
        return Decision(False, user, text=reply)

    # ── messages ──

    def check_message(
        self, incoming: Incoming, parsed: ParsedCommand | None, command: Command | None
    ) -> Decision:
        """Gate a message. ``parsed`` is the slash command (None for plain text and
        documents), ``command`` its registry entry (None if unknown to this sender)."""
        if incoming.from_id is None or incoming.is_bot:
            return _IGNORE
        if not incoming.is_private:
            log.info("ignored a message from %s in a %s chat", incoming.from_id, incoming.chat_type)
            if parsed is not None and self._first_time_in_group(incoming.chat_id):
                return Decision(False, text=DIRECT_ONLY, reply_chat=incoming.chat_id)
            return _IGNORE

        if command is not None and command.admin:
            # Registry.command() only returns admin commands to the admin.
            return Decision(True, self._load(incoming.from_id))

        user = self._load(incoming.from_id)
        if user is None:
            user, created = self.users.register_pending(
                incoming.from_id,
                chat_id=incoming.chat_id or incoming.from_id,
                username=incoming.username,
                first_name=incoming.first_name,
            )
            if created:
                log.info("new access request from user %s", incoming.from_id)
                notice = None
                if self._notices.allow():
                    notice = render.new_user_notice(
                        incoming.from_id, incoming.first_name, incoming.username
                    )
                else:
                    log.warning(
                        "new-user notice for %s not sent to the admin (rate limit)",
                        incoming.from_id,
                    )
                self._refusal(user.telegram_id, WELCOME_TEXT)  # starts the quiet interval
                return Decision(False, user, text=WELCOME_TEXT, admin_notice=notice)
        if user.state == S.PENDING:
            return self._refuse(user, PENDING_TEXT)
        if user.state == S.DENIED:
            return self._refuse(user, DENIED_TEXT)
        if user.state not in S.APPROVED_STATES:
            log.warning("user %s has an unknown state %r", user.telegram_id, user.state)
            return self._refuse(user, "Your account needs attention — contact the admin.")

        if parsed is not None:
            if command is None:
                return Decision(False, user, text=UNKNOWN_COMMAND)
            if user.state not in command.states:
                return Decision(False, user, text=refusal_for_state(user.state))
            return Decision(True, user)
        if incoming.document is not None and user.state not in S.READY_STATES:
            return Decision(False, user, text=document_refusal(user.state))
        return Decision(True, user)

    # ── callbacks ──

    def check_callback(self, incoming: Incoming, route: CallbackRoute | None) -> Decision:
        """Gate a button press. Every non-allowed press is answered (``text``)."""
        if incoming.from_id is None or incoming.is_bot:
            return Decision(False, text=NOT_AUTHORIZED)
        if not incoming.is_private:
            log.info("refused a button press of %s outside a private chat", incoming.from_id)
            return Decision(False, text=DIRECT_ONLY, remove_keyboard=True)
        user = self._load(incoming.from_id)
        if user is None or user.state not in S.APPROVED_STATES:
            log.info(
                "refused a button press of %s (%s)",
                incoming.from_id,
                user.state if user is not None else "unknown user",
            )
            return Decision(False, user, text=NOT_AUTHORIZED, remove_keyboard=True)
        if route is None:
            return Decision(False, user, text=UNKNOWN_BUTTON)
        if user.state not in route.states:
            return Decision(False, user, text=route.expired_text, remove_keyboard=True)
        return Decision(True, user)
