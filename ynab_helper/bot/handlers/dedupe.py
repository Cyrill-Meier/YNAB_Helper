"""/dedupe: pick stray importer transactions and delete them after a confirmation
(DESIGN §6.7, §8.3; F13, F20, F44, F46, F95).

The scan runs on a worker (:func:`ynab_helper.services.dedupe_scan`) and the picker's
buttons carry the scan id as nonce: ``dx:<scan>:t:<i>`` toggles, ``dx:<scan>:p:<page>``
pages, ``dx:<scan>:all|none|del|back|ok|c`` act. A button of an older scan (or one that
was replaced by a scan in the web UI) expires instead of acting on the new list (F20).
The picker lives on exactly one message (:attr:`Picker.message_id`); a press on any other
message carrying the same scan id — e.g. an older confirmation whose keyboard could not be
removed after ``/dedupe_delete`` — expires too. ``ok`` is only accepted in the
confirmation view, which lists the selected transactions, so "Yes, delete" always deletes
exactly the rows on the message that was pressed. At most 200 can be deleted at a time.

Buttons of v1 pickers (``dd:*``) still sit in chats; they expire and lose their keyboard.

``/dedupe_delete 1,3-5|all`` validates every number and range against the candidate count
**before** expanding anything (at most 200 indices in total, F44) and then shows the same
confirmation view — it never deletes without a confirmation. The new selection only takes
effect once that confirmation message was sent. ``/dedupe_cancel`` drops the selection.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Final

from ynab_helper import services
from ynab_helper.bot import render
from ynab_helper.bot.context import Request
from ynab_helper.bot.handlers._common import ascii_int, busy, callback_parts, expire_button
from ynab_helper.bot.render import h
from ynab_helper.errors import StaleStateError
from ynab_helper.maintenance.dedupe import MAX_DELETE_IDS, DedupeScan
from ynab_helper.services import UserContext

log = logging.getLogger(__name__)

MEMORY_KIND: Final = "dedupe"
EXPIRED: Final = "This list has expired — run /dedupe again."
_MAX_SPEC: Final = 500
_PIECE_RE: Final = re.compile(r"([0-9]{1,6})(?:-([0-9]{1,6}))?")


class SelectionError(ValueError):
    """A /dedupe_delete selection that cannot be used (``str()`` is shown to the user)."""


@dataclass(slots=True)
class Picker:
    """The user's live dedupe picker."""

    scan: DedupeScan
    selected: set[int] = field(default_factory=set)
    page: int = 0
    confirming: bool = False
    message_id: int | None = None

    @property
    def nonce(self) -> str:
        return self.scan.scan_id

    def view(self) -> tuple[str, dict]:
        if self.confirming:
            return render.dedupe_confirm(self.scan, self.selected, nonce=self.nonce)
        return render.dedupe_picker(self.scan, self.selected, self.page, nonce=self.nonce)


def parse_selection(spec: str, count: int, *, limit: int = MAX_DELETE_IDS) -> list[int]:
    """1-based indices from ``1,3-5`` or ``all``; every piece is checked against ``count``
    and the running total against ``limit`` before any range is expanded (F44)."""
    text = spec.strip().lower()
    if len(text) > _MAX_SPEC:
        raise SelectionError("That selection is too long.")
    if count < 1:
        raise SelectionError("There is nothing to select.")
    if text in ("all", "*"):
        if count > limit:
            raise SelectionError(
                f"There are {count} candidates; at most {limit} can be deleted at a time. "
                f"Use a range such as 1-{limit}."
            )
        return list(range(1, count + 1))
    pieces = [p for p in re.sub(r"\s+", "", text).split(",") if p]
    if not pieces:
        raise SelectionError("Couldn't parse the selection. Use 1,3,5 or 2-4 or all.")
    ranges: list[tuple[int, int]] = []
    total = 0
    for piece in pieces:
        match = _PIECE_RE.fullmatch(piece)
        if match is None:
            raise SelectionError("Couldn't parse the selection. Use 1,3,5 or 2-4 or all.")
        start = int(match.group(1))
        end = int(match.group(2)) if match.group(2) else start
        if start > end:
            raise SelectionError(f"Invalid range {start}-{end}.")
        if start < 1 or end > count:
            raise SelectionError(f"Out of range: {piece} (valid 1–{count}).")
        total += end - start + 1
        if total > limit:
            raise SelectionError(f"At most {limit} transactions can be deleted at a time.")
        ranges.append((start, end))
    return sorted({i for start, end in ranges for i in range(start, end + 1)})


# ── commands ─────────────────────────────────────────────────────────────────


def cmd_dedupe(req: Request) -> None:
    assert req.user is not None
    ctx = req.bot.user_context(req.user)
    req.bot.start_job(
        req,
        "dedupe",
        lambda: _scan(req, ctx),
        working="🔎 Looking for stray imports in your newest statement's period…",
    )


def _scan(req: Request, ctx: UserContext) -> None:
    """Worker: scan and post the picker."""
    scan = services.dedupe_scan(ctx)
    if not scan.candidates:
        req.bot.memory.pop(MEMORY_KIND, req.user_id)
        req.reply(render.dedupe_empty(scan))
        return
    picker = Picker(scan)
    text, markup = picker.view()
    message = req.reply(text, markup)
    message_id = message.get("message_id") if message else None
    if not isinstance(message_id, int):
        req.bot.memory.pop(MEMORY_KIND, req.user_id)  # the older picker's scan is gone too
        return
    # Only now does the picker go live: it is bound to the message that shows it.
    picker.message_id = message_id
    req.bot.memory.put(MEMORY_KIND, req.user_id, picker)


def cmd_dedupe_delete(req: Request) -> None:
    picker = req.bot.memory.get(MEMORY_KIND, req.user_id)
    if not isinstance(picker, Picker):
        req.reply("No duplicate list is pending. Run /dedupe first.")
        return
    if not req.args:
        req.reply(
            "Tip: use the buttons above — or send /dedupe_delete 1,3-5 or /dedupe_delete all."
        )
        return
    try:
        indices = parse_selection(req.args, len(picker.scan.candidates))
    except SelectionError as exc:
        req.reply(f"⚠ {h(exc)}")
        return
    selected = {i - 1 for i in indices}
    text, markup = render.dedupe_confirm(picker.scan, selected, nonce=picker.nonce)
    message = req.reply(text, markup)
    message_id = message.get("message_id") if message else None
    if not isinstance(message_id, int):
        return  # nothing was shown: the picker keeps its selection and its message
    previous = picker.message_id
    # The confirmation message now *is* the picker; presses on the old message expire.
    picker.selected = selected
    picker.confirming = True
    picker.message_id = message_id
    if previous is not None and previous != message_id:
        req.bot.remove_keyboard(req.chat_id, previous)


def cmd_dedupe_cancel(req: Request) -> None:
    picker = req.bot.memory.pop(MEMORY_KIND, req.user_id)
    if not isinstance(picker, Picker):
        req.reply("Nothing pending. Run /dedupe first to scan.")
        return
    if picker.message_id is not None:
        req.bot.edit(req.chat_id, picker.message_id, "✖ Dedupe cancelled.", fallback=False)
    req.reply("🗑 Dedupe selection cleared.")


# ── buttons ──────────────────────────────────────────────────────────────────


def cb_legacy_dedupe(req: Request) -> None:
    """v1 picker buttons (``dd:*``): expired."""
    expire_button(req, EXPIRED)


def cb_dedupe(req: Request) -> None:
    parts = callback_parts(req, (3, 4))
    picker = req.bot.memory.get(MEMORY_KIND, req.user_id)
    if (
        parts is None
        or not isinstance(picker, Picker)
        or picker.nonce != parts[1]
        or picker.message_id is None
        or req.message_id != picker.message_id  # an older view of the same scan (F20)
    ):
        expire_button(req, EXPIRED)
        return
    action = parts[2]
    argument = parts[3] if len(parts) == 4 else None
    count = len(picker.scan.candidates)

    if action in ("t", "p"):
        number = ascii_int(argument, 6)
        if number is None:
            expire_button(req, EXPIRED)
            return
        if action == "t":
            if picker.confirming:
                req.answer("Go back to change the selection.")
                return
            if not 0 <= number < count:
                req.answer()
                return
            if number in picker.selected:
                picker.selected.discard(number)
            elif len(picker.selected) >= MAX_DELETE_IDS:
                req.answer(f"At most {MAX_DELETE_IDS} at a time.", alert=True)
                return
            else:
                picker.selected.add(number)
        else:
            if picker.confirming:
                req.answer()
                return
            picker.page = max(0, min(number, render.dedupe_pages(count) - 1))
        req.answer()
        _refresh(req, picker)
        return
    if argument is not None:
        expire_button(req, EXPIRED)
        return
    if action == "all":
        if picker.confirming:
            req.answer()
            return
        picker.selected = set(range(min(count, MAX_DELETE_IDS)))
        req.answer(
            "All selected." if count <= MAX_DELETE_IDS else f"The first {MAX_DELETE_IDS} selected."
        )
        _refresh(req, picker)
    elif action == "none":
        if picker.confirming:
            req.answer()
            return
        picker.selected.clear()
        req.answer("Cleared.")
        _refresh(req, picker)
    elif action == "del":
        if not picker.selected:
            req.answer("Nothing selected.", alert=True)
            return
        picker.confirming = True
        req.answer()
        _refresh(req, picker)
    elif action == "back":
        picker.confirming = False
        req.answer()
        _refresh(req, picker)
    elif action == "c":
        req.bot.memory.pop(MEMORY_KIND, req.user_id, if_is=picker)
        req.answer("Cancelled.")
        req.edit("✖ Dedupe cancelled.", fallback=False)
    elif action == "ok":
        _confirm(req, picker)
    else:
        expire_button(req, EXPIRED)


def _refresh(req: Request, picker: Picker) -> None:
    text, markup = picker.view()
    req.edit(text, markup, fallback=False)


def _confirm(req: Request, picker: Picker) -> None:
    if not picker.confirming:
        req.answer("Press 🗑 Delete first to review the selection.", alert=True)
        return
    selected = sorted(i for i in picker.selected if 0 <= i < len(picker.scan.candidates))
    if not selected:
        req.answer("Nothing selected.", alert=True)
        return
    if busy(req):
        return  # the confirmation stays; the user can press again
    if req.bot.memory.pop(MEMORY_KIND, req.user_id, if_is=picker) is None:
        expire_button(req, EXPIRED)
        return
    assert req.user is not None
    ctx = req.bot.user_context(req.user)
    ids = [picker.scan.candidates[i].id for i in selected]
    req.edit(f"🗑 Deleting {render.plural(len(ids), 'transaction')}…", fallback=False)
    req.bot.start_job(
        req, "dedupe", lambda: _delete(req, ctx, picker.scan.scan_id, ids), working="Deleting…"
    )


def _delete(req: Request, ctx: UserContext, scan_id: str, ids: list[str]) -> None:
    """Worker: delete the confirmed selection and report it on the picker message."""
    try:
        result = services.dedupe_delete(ctx, scan_id, ids)
    except StaleStateError as exc:
        req.edit(f"⚠ {h(exc.user_message)} Run /dedupe again.")
        return
    req.edit(render.dedupe_delete_result(result))
