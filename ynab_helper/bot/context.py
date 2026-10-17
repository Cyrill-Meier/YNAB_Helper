"""Parsed Telegram updates and the request object handed to every handler.

:func:`parse_update` turns a raw update into an :class:`Incoming` (or None for update types
the bot does not handle). Callback queries take their chat from ``callback_query.message``;
the sender is always ``from.id`` — never the chat — so a reply can only reach the person who
wrote (F56).

A :class:`Request` bundles the incoming update, the sender's ``users`` row and the parsed
command, and offers ``reply``/``answer``/``edit`` helpers that never raise on Telegram
errors (they are logged), so a failed send can never abort a handler half-way.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

from ynab_helper.storage.users import User

if TYPE_CHECKING:
    from ynab_helper.bot.app import Bot

__all__ = ["Incoming", "ParsedCommand", "Request", "parse_command", "parse_update"]

log = logging.getLogger(__name__)

_COMMAND_RE: Final = re.compile(r"[a-z0-9_]{1,32}")
_MAX_ARGS: Final = 1000


def _int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _str(value: Any, limit: int = 256) -> str | None:
    return value[:limit] if isinstance(value, str) else None


@dataclass(frozen=True, slots=True)
class Incoming:
    """The parts of an update the bot uses (``kind`` is ``message`` or ``callback``)."""

    update_id: int
    kind: str
    from_id: int | None
    is_bot: bool
    username: str | None
    first_name: str | None
    chat_id: int | None
    chat_type: str | None
    message_id: int | None  # the message, or the message carrying the pressed button
    text: str | None  # message text (None for documents and callbacks)
    document: Mapping[str, Any] | None = field(default=None, repr=False)
    callback_id: str | None = None
    data: str | None = None  # callback_data

    @property
    def is_private(self) -> bool:
        """A one-to-one chat with the sender (``chat.type == "private"`` and ``chat.id ==
        from.id``) — the only place the bot talks (F56)."""
        return (
            self.chat_type == "private"
            and self.chat_id is not None
            and self.from_id is not None
            and self.chat_id == self.from_id
        )


def parse_update(update: Mapping[str, Any]) -> Incoming | None:
    """Extract an :class:`Incoming` from a raw update; None if it is not a message or a
    callback query (or is malformed)."""
    update_id = _int(update.get("update_id"))
    if update_id is None:
        return None
    callback = update.get("callback_query")
    if isinstance(callback, Mapping):
        sender = callback.get("from") if isinstance(callback.get("from"), Mapping) else {}
        message = callback.get("message") if isinstance(callback.get("message"), Mapping) else {}
        chat = message.get("chat") if isinstance(message.get("chat"), Mapping) else {}
        return Incoming(
            update_id=update_id,
            kind="callback",
            from_id=_int(sender.get("id")),
            is_bot=sender.get("is_bot") is True,
            username=_str(sender.get("username"), 64),
            first_name=_str(sender.get("first_name"), 128),
            chat_id=_int(chat.get("id")),
            chat_type=_str(chat.get("type"), 32),
            message_id=_int(message.get("message_id")),
            text=None,
            callback_id=_str(callback.get("id"), 128),
            data=_str(callback.get("data"), 128) or "",
        )
    message = update.get("message")
    if not isinstance(message, Mapping):
        return None
    sender = message.get("from") if isinstance(message.get("from"), Mapping) else {}
    chat = message.get("chat") if isinstance(message.get("chat"), Mapping) else {}
    document = message.get("document")
    return Incoming(
        update_id=update_id,
        kind="message",
        from_id=_int(sender.get("id")),
        is_bot=sender.get("is_bot") is True,
        username=_str(sender.get("username"), 64),
        first_name=_str(sender.get("first_name"), 128),
        chat_id=_int(chat.get("id")),
        chat_type=_str(chat.get("type"), 32),
        message_id=_int(message.get("message_id")),
        text=_str(message.get("text"), 8192),
        document=document if isinstance(document, Mapping) else None,
    )


@dataclass(frozen=True, slots=True)
class ParsedCommand:
    """``/name@bot args`` → ``name`` (lower case, no ``@bot`` suffix) and ``args``.

    ``name`` is empty for something that starts with ``/`` but is not a valid command name;
    such text never matches a command (exact matching: ``/ip`` ≠ ``/ipfoo``).
    """

    name: str
    args: str


def parse_command(text: str | None) -> ParsedCommand | None:
    """Parse a bot command; None if ``text`` does not start with ``/``."""
    if not text:
        return None
    stripped = text.strip()
    if not stripped.startswith("/"):
        return None
    parts = stripped.split(None, 1)
    head = parts[0][1:]
    args = parts[1].strip()[:_MAX_ARGS] if len(parts) > 1 else ""
    name = head.split("@", 1)[0].lower()
    if not _COMMAND_RE.fullmatch(name):
        name = ""
    return ParsedCommand(name=name, args=args)


class Request:
    """One update being handled: who sent it, their ``users`` row and reply helpers."""

    def __init__(
        self,
        bot: Bot,
        incoming: Incoming,
        user: User | None,
        command: ParsedCommand | None = None,
    ) -> None:
        self.bot = bot
        self.incoming = incoming
        self.user = user
        self.command = command
        self.answered = False

    # ── identity ──

    @property
    def user_id(self) -> int:
        assert self.incoming.from_id is not None
        return self.incoming.from_id

    @property
    def chat_id(self) -> int:
        assert self.incoming.chat_id is not None
        return self.incoming.chat_id

    @property
    def message_id(self) -> int | None:
        return self.incoming.message_id

    @property
    def is_admin(self) -> bool:
        return self.incoming.from_id == self.bot.admin_id

    @property
    def args(self) -> str:
        return self.command.args if self.command is not None else ""

    @property
    def text(self) -> str:
        return (self.incoming.text or "").strip()

    def refresh_user(self) -> User | None:
        """Re-read the sender's row (after a write)."""
        self.user = self.bot.users.get(self.user_id)
        return self.user

    # ── replies (never raise) ──

    def reply(self, text: str, markup: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
        return self.bot.send(self.chat_id, text, markup)

    def answer(self, text: str | None = None, *, alert: bool = False) -> None:
        """Answer the callback query once (later calls are ignored)."""
        if self.answered or self.incoming.callback_id is None:
            return
        self.answered = True
        self.bot.answer_callback(self.incoming.callback_id, text, alert=alert)

    def edit(
        self, text: str, markup: Mapping[str, Any] | None = None, *, fallback: bool = True
    ) -> bool:
        """Edit the message carrying the pressed button; on failure send ``text`` as a new
        message when ``fallback`` (results must never get lost: F95)."""
        if self.message_id is None:
            if fallback:
                self.reply(text, markup)
            return False
        return self.bot.edit(self.chat_id, self.message_id, text, markup, fallback=fallback)

    def remove_keyboard(self) -> None:
        if self.message_id is not None:
            self.bot.remove_keyboard(self.chat_id, self.message_id)
