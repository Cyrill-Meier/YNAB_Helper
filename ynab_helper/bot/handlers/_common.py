"""Helpers shared by the handlers: flow nonces, choice matching, callback parsing."""

from __future__ import annotations

import secrets
import time
from collections.abc import Mapping, Sequence
from typing import Any, Final

from ynab_helper.bot.context import Request
from ynab_helper.bot.users import TEMP_VERSION

#: Choices kept for an onboarding picker (buttons are shown for the first ``MAX_BUTTONS``).
MAX_CHOICES: Final = 200
MAX_BUTTONS: Final = 40


def ascii_int(text: str | None, max_digits: int) -> int | None:
    """``text`` as a non-negative int if it is 1..``max_digits`` ASCII digits, else None
    (``str.isdigit`` also accepts other scripts' digits and superscripts)."""
    if text is None or not 0 < len(text) <= max_digits or not (text.isascii() and text.isdigit()):
        return None
    return int(text)


def new_nonce() -> str:
    """A short random id binding buttons to one flow or preview (8 URL-safe characters)."""
    return secrets.token_urlsafe(6)


def new_flow(kind: str, **fields: Any) -> dict[str, Any]:
    """A fresh ``temp_data`` flow record."""
    return {"v": TEMP_VERSION, "kind": kind, "flow": new_nonce(), "started": time.time(), **fields}


def match_choice(text: str, items: Sequence[Mapping[str, Any]]) -> tuple[int | None, str | None]:
    """Resolve a typed reply to an index of ``items``: a 1-based number or an exact
    (case-insensitive) name. Returns ``(index, None)`` or ``(None, problem)`` where the
    problem is ``"ambiguous"``, ``"range"`` or None (no match)."""
    value = text.strip()
    if not value:
        return None, None
    number = ascii_int(value, 4)
    if number is not None:
        index = number - 1
        return (index, None) if 0 <= index < len(items) else (None, "range")
    wanted = value.casefold()
    hits = [
        i for i, item in enumerate(items) if str(item.get("name", "")).strip().casefold() == wanted
    ]
    if len(hits) == 1:
        return hits[0], None
    return None, ("ambiguous" if hits else None)


def callback_parts(req: Request, expected_parts: int | tuple[int, ...]) -> list[str] | None:
    """``callback_data`` split on ``:`` if it has the expected number of parts."""
    parts = (req.incoming.data or "").split(":")
    counts = (expected_parts,) if isinstance(expected_parts, int) else expected_parts
    return parts if len(parts) in counts else None


def expire_button(req: Request, text: str) -> None:
    """Answer a stale button and remove its keyboard."""
    req.answer(text)
    req.remove_keyboard()


def busy(req: Request) -> bool:
    """True (and the user is told) if the user already has an operation running — the
    same check :meth:`Bot.start_job <ynab_helper.bot.app.Bot.start_job>` makes."""
    if req.bot.is_busy(req.user_id) is None:
        return False
    req.bot.reply_busy(req)
    return True
