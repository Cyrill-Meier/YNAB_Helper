"""Telegram Bot API client (docs/DESIGN.md §8.1).

:class:`TelegramClient` is a thin, thread-safe wrapper over :func:`ynab_helper.net.request_json`:

* every call has a socket timeout and a per-attempt deadline; ``getUpdates`` gets a deadline
  longer than the long-poll hold time;
* nothing is retried by ``net`` (``retries=0``: ``sendMessage`` and friends are not
  idempotent), except that a Telegram ``429`` is waited out **once** when its
  ``retry_after`` is at most 30 s — Telegram rejected the request, so resending is safe.
  Best-effort calls (refusals to strangers, callback answers, shutdown notices) pass
  ``wait_on_rate_limit=False`` so a flood can never park the poll thread in a 429 wait;
* failures surface as :class:`~ynab_helper.errors.TelegramError` with ``error_code``,
  ``description`` and ``retry_after``; the bot token (part of every URL) is redacted from
  every message;
* messages are sent with ``parse_mode="HTML"`` and link previews disabled. Callers escape
  dynamic content with :func:`ynab_helper.bot.render.h` and close their tags on the line
  they open them, so :func:`split_message` can cut long texts on line boundaries into
  chunks of at most 4096 UTF-16 code units (Telegram's unit of length);
* :meth:`TelegramClient.download_file` streams a document into a private temp file with a
  size cap and only renames it into place once complete.

The transport is :func:`urllib.request.urlopen` unless an ``opener`` is injected (tests use
:class:`tests.fakes.fake_telegram.FakeTelegram`); ``api_base`` comes from
``TELEGRAM_API_BASE`` so the CI smoke test can point the bot at an unreachable or fake API.
"""

from __future__ import annotations

import html
import http.client
import logging
import os
import re
import secrets
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Final

from ynab_helper import net
from ynab_helper.config import DEFAULT_TELEGRAM_API_BASE
from ynab_helper.errors import ExternalServiceError, TelegramError

__all__ = [
    "ALLOWED_UPDATES",
    "MAX_CALLBACK_DATA",
    "MAX_DOWNLOAD_BYTES",
    "MAX_MESSAGE_UNITS",
    "REMOVE_INLINE_KEYBOARD",
    "DownloadTooLarge",
    "TelegramClient",
    "button",
    "fit_message",
    "html_to_plain",
    "inline_keyboard",
    "split_message",
    "url_button",
    "utf16_len",
]

log = logging.getLogger(__name__)

#: Longest message text Telegram accepts, in UTF-16 code units.
MAX_MESSAGE_UNITS: Final = 4096
#: Longest document caption, in UTF-16 code units.
MAX_CAPTION_UNITS: Final = 1024
#: Longest callback answer text Telegram shows.
MAX_CALLBACK_ANSWER: Final = 200
#: Longest ``callback_data`` of an inline button, in bytes.
MAX_CALLBACK_DATA: Final = 64
#: Largest file the bot downloads (a Revolut statement is far smaller).
MAX_DOWNLOAD_BYTES: Final = 25 * 1024 * 1024
#: A 429 asking for a longer wait than this is not waited out.
MAX_RETRY_AFTER: Final = 30.0
DEFAULT_TIMEOUT: Final = 15.0
#: Long-poll hold time of ``getUpdates`` (seconds).
DEFAULT_POLL_TIMEOUT: Final = 50
ALLOWED_UPDATES: Final[tuple[str, ...]] = ("message", "callback_query")
#: An empty inline keyboard: removes the buttons of a message.
REMOVE_INLINE_KEYBOARD: Final[Mapping[str, Any]] = {"inline_keyboard": []}

_DOCUMENT_TIMEOUT: Final = 60.0
_DOWNLOAD_CHUNK: Final = 64 * 1024
_FILE_PATH_RE: Final = re.compile(r"[A-Za-z0-9_.\-/]{1,512}")
_TAG_RE: Final = re.compile(r"<[^<>]*>")
_ELLIPSIS: Final = "…"


class DownloadTooLarge(TelegramError):
    """The file to download exceeds the size cap (nothing is kept on disk)."""

    def __init__(self, limit: int) -> None:
        super().__init__(f"file larger than {limit} bytes", description="file too large")
        self.user_message = f"The file is larger than {limit // (1024 * 1024)} MB."
        self.limit = limit


# ── text helpers ─────────────────────────────────────────────────────────────


def utf16_len(text: str) -> int:
    """Length of ``text`` in UTF-16 code units (how Telegram measures messages)."""
    return len(text.encode("utf-16-le", "surrogatepass")) // 2


def html_to_plain(text: str) -> str:
    """Strip HTML tags and resolve entities (fallback when Telegram rejects our markup)."""
    return html.unescape(_TAG_RE.sub("", text))


def _split_long_line(line: str, limit: int) -> list[str]:
    """Cut one over-long line into escaped plain-text pieces of at most ``limit`` units.

    A cut inside a tag or entity would break the HTML, so the line loses its markup: it is
    reduced to plain text and every piece is escaped again.
    """
    pieces: list[str] = []
    current: list[str] = []
    size = 0
    for char in html_to_plain(line):
        escaped = html.escape(char, quote=False)
        units = utf16_len(escaped)
        if size + units > limit and current:
            pieces.append("".join(current))
            current, size = [], 0
        current.append(escaped)
        size += units
    if current:
        pieces.append("".join(current))
    # A piece of nothing but whitespace would be rejected as an empty message.
    return [piece for piece in pieces if piece.strip()]


def split_message(text: str, limit: int = MAX_MESSAGE_UNITS) -> list[str]:
    """Split ``text`` on line boundaries into chunks of at most ``limit`` UTF-16 units.

    Lines are never cut unless a single line is longer than ``limit`` (then it is sent as
    escaped plain text in several pieces). Blank lines at a chunk edge are dropped and
    empty chunks are never returned (Telegram rejects empty messages).
    """
    if limit < 1:
        raise ValueError("limit must be positive")
    if utf16_len(text) <= limit:
        return [text] if text.strip() else []
    chunks: list[str] = []
    current: list[str] = []
    size = 0

    def flush() -> None:
        nonlocal current, size
        chunk = "\n".join(current).strip("\n")
        if chunk.strip():
            chunks.append(chunk)
        current, size = [], 0

    for line in text.split("\n"):
        units = utf16_len(line)
        if units > limit:
            flush()
            chunks.extend(_split_long_line(line, limit))
            continue
        extra = units + (1 if current else 0)
        if size + extra > limit:
            flush()
            current, size = [line], units
        else:
            current.append(line)
            size += extra
    flush()
    return chunks


def fit_message(text: str, limit: int = MAX_MESSAGE_UNITS) -> str:
    """``text`` cut on a line boundary to fit one message (edits cannot be split)."""
    if utf16_len(text) <= limit:
        return text
    kept: list[str] = []
    size = utf16_len(_ELLIPSIS) + 1
    for line in text.split("\n"):
        units = utf16_len(line) + 1
        if size + units > limit:
            break
        kept.append(line)
        size += units
    if not kept:  # a single huge first line
        first = _split_long_line(text.split("\n", 1)[0], limit - utf16_len(_ELLIPSIS) - 1)
        kept = first[:1]
    return "\n".join([*kept, _ELLIPSIS])


# ── keyboards ────────────────────────────────────────────────────────────────


def button(text: str, data: str) -> dict[str, str]:
    """An inline button with ``callback_data`` (at most 64 bytes, checked)."""
    if len(data.encode("utf-8")) > MAX_CALLBACK_DATA:
        raise ValueError(f"callback_data longer than {MAX_CALLBACK_DATA} bytes: {data!r}")
    return {"text": text, "callback_data": data}


def url_button(text: str, url: str) -> dict[str, str]:
    """An inline button that opens ``url``."""
    return {"text": text, "url": url}


def inline_keyboard(rows: Iterable[Iterable[Mapping[str, str]]]) -> dict[str, Any]:
    """``{"inline_keyboard": [[button, …], …]}`` (empty rows dropped)."""
    keyboard = [[dict(b) for b in row] for row in rows]
    return {"inline_keyboard": [row for row in keyboard if row]}


# ── errors ───────────────────────────────────────────────────────────────────


def _is_parse_error(exc: TelegramError) -> bool:
    text = (exc.description or "").lower()
    return exc.error_code == 400 and ("can't parse entities" in text or "can't find end" in text)


def _is_not_modified(exc: TelegramError) -> bool:
    return exc.error_code == 400 and "message is not modified" in (exc.description or "").lower()


def _positive_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if number >= 0 and number == number and number != float("inf") else None


def _status_error(method: str, exc: net.HttpStatusError) -> TelegramError:
    payload = exc.payload if isinstance(exc.payload, Mapping) else {}
    code = payload.get("error_code")
    error_code = code if isinstance(code, int) and not isinstance(code, bool) else exc.status
    raw_description = payload.get("description")
    description = str(raw_description)[:200] if raw_description is not None else None
    parameters = payload.get("parameters")
    retry_after = None
    if isinstance(parameters, Mapping):
        retry_after = _positive_float(parameters.get("retry_after"))
    if retry_after is None:
        retry_after = exc.retry_after
    return TelegramError(
        f"Telegram {method} failed: HTTP {exc.status}"
        + (f": {description}" if description else ""),
        error_code=error_code,
        description=description,
        retry_after=retry_after,
        retriable=exc.retriable or error_code == 429,
    )


# ── client ───────────────────────────────────────────────────────────────────


class TelegramClient:
    """Minimal, thread-safe Telegram Bot API client (see the module docstring)."""

    def __init__(
        self,
        token: str,
        *,
        api_base: str = DEFAULT_TELEGRAM_API_BASE,
        timeout: float = DEFAULT_TIMEOUT,
        poll_timeout: int = DEFAULT_POLL_TIMEOUT,
        opener: net.Opener | None = None,
        sleep: Callable[[float], None] = time.sleep,
        max_retry_after: float = MAX_RETRY_AFTER,
    ) -> None:
        if not isinstance(token, str) or not token or any(c.isspace() or c == "/" for c in token):
            raise ValueError("invalid Telegram bot token")
        base = api_base.rstrip("/")
        parts = urllib.parse.urlsplit(base)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise ValueError("api_base must be an absolute http(s) URL")
        self._token = token
        self._base = base
        self._timeout = float(timeout)
        self.poll_timeout = int(poll_timeout)
        self._opener = opener
        self._sleep = sleep
        self._max_retry_after = float(max_retry_after)

    def __repr__(self) -> str:
        return f"TelegramClient(api_base={self._base!r}, token=<redacted>)"

    @property
    def api_base(self) -> str:
        return self._base

    # ── generic call ──

    def call(
        self,
        method: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        total_timeout: float | None = None,
        wait_on_rate_limit: bool = True,
    ) -> Any:
        """Call ``method`` with a JSON body and return its ``result`` (see the module doc)."""
        return self._call(
            method,
            json_body=dict(params or {}),
            timeout=timeout,
            total_timeout=total_timeout,
            wait_on_rate_limit=wait_on_rate_limit,
        )

    def _call(
        self,
        method: str,
        *,
        json_body: Any = None,
        data: bytes | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
        total_timeout: float | None = None,
        wait_on_rate_limit: bool = True,
    ) -> Any:
        """One API call. A 429 is waited out once (``retry_after`` ≤ the cap) unless
        ``wait_on_rate_limit`` is False — then it is raised at once."""
        url = f"{self._base}/bot{self._token}/{method}"
        socket_timeout = self._timeout if timeout is None else timeout
        for attempt in (0, 1):
            try:
                reply = net.request_json(
                    "POST",
                    url,
                    json_body=json_body,
                    data=data,
                    headers=headers,
                    timeout=socket_timeout,
                    total_timeout=total_timeout,
                    retries=0,
                    service="Telegram",
                    redact=(self._token,),
                    opener=self._opener,
                )
            except net.HttpStatusError as exc:
                error = _status_error(method, exc)
                if (
                    attempt == 0
                    and wait_on_rate_limit
                    and error.error_code == 429
                    and error.retry_after is not None
                    and error.retry_after <= self._max_retry_after
                ):
                    log.warning(
                        "Telegram %s rate limited; retrying once in %.1fs",
                        method,
                        error.retry_after,
                    )
                    self._sleep(error.retry_after)
                    continue
                raise error from None
            except ExternalServiceError as exc:
                raise TelegramError(
                    f"Telegram {method} failed: {net.redact_text(str(exc), (self._token,))}",
                    retriable=exc.retriable,
                ) from None
            return self._result(method, reply)
        raise AssertionError("unreachable")  # pragma: no cover

    @staticmethod
    def _result(method: str, reply: Any) -> Any:
        if not isinstance(reply, Mapping) or reply.get("ok") is not True:
            payload = reply if isinstance(reply, Mapping) else {}
            code = payload.get("error_code")
            raw_description = payload.get("description")
            raise TelegramError(
                f"Telegram {method} returned an unexpected reply",
                error_code=code if isinstance(code, int) and not isinstance(code, bool) else None,
                description=str(raw_description)[:200] if raw_description is not None else None,
            )
        return reply.get("result")

    # ── bot info, updates, commands ──

    def get_me(self) -> dict[str, Any]:
        result = self._call("getMe", json_body={})
        if not isinstance(result, dict):
            raise TelegramError("Telegram getMe returned no bot")
        return result

    def get_updates(
        self,
        offset: int | None,
        *,
        timeout: int | None = None,
        allowed_updates: Sequence[str] = ALLOWED_UPDATES,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Long-poll for updates (``timeout`` seconds hold; default :attr:`poll_timeout`)."""
        hold = self.poll_timeout if timeout is None else int(timeout)
        params: dict[str, Any] = {
            "timeout": hold,
            "allowed_updates": list(allowed_updates),
            "limit": limit,
        }
        if offset:
            params["offset"] = int(offset)
        result = self._call(
            "getUpdates",
            json_body=params,
            timeout=hold + self._timeout,
            total_timeout=hold + 2 * self._timeout,
        )
        if not isinstance(result, list):
            raise TelegramError("Telegram getUpdates returned no list")
        return [update for update in result if isinstance(update, dict)]

    def set_my_commands(
        self, commands: Sequence[tuple[str, str]], *, scope: Mapping[str, Any] | None = None
    ) -> bool:
        params: dict[str, Any] = {
            "commands": [{"command": name, "description": desc} for name, desc in commands]
        }
        if scope is not None:
            params["scope"] = dict(scope)
        return self._call("setMyCommands", json_body=params) is True

    # ── messages ──

    def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_markup: Mapping[str, Any] | None = None,
        parse_mode: str | None = "HTML",
        disable_preview: bool = True,
        wait_on_rate_limit: bool = True,
        timeout: float | None = None,
    ) -> dict[str, Any] | None:
        """Send ``text`` (split into ≤ 4096-unit chunks); returns the last chunk's Message.

        ``reply_markup`` goes on the last chunk only. If Telegram rejects the HTML (a bug in
        a formatter), the chunk is resent once as plain text so the user always gets it.
        ``wait_on_rate_limit=False`` raises a 429 instead of waiting it out; ``timeout``
        bounds each request (socket and total), e.g. for notices sent while shutting down.
        """
        call_options: dict[str, Any] = {"wait_on_rate_limit": wait_on_rate_limit}
        if timeout is not None:
            call_options["timeout"] = timeout
            call_options["total_timeout"] = timeout
        chunks = split_message(text) or ["…"]
        last: dict[str, Any] | None = None
        for index, chunk in enumerate(chunks):
            params: dict[str, Any] = {"chat_id": chat_id, "text": chunk}
            if parse_mode:
                params["parse_mode"] = parse_mode
            if disable_preview:
                params["link_preview_options"] = {"is_disabled": True}
            if reply_markup is not None and index == len(chunks) - 1:
                params["reply_markup"] = dict(reply_markup)
            try:
                result = self._call("sendMessage", json_body=params, **call_options)
            except TelegramError as exc:
                if not (parse_mode and _is_parse_error(exc)):
                    raise
                log.error(
                    "Telegram rejected a message's HTML (%s); resending it as plain text", exc
                )
                params.pop("parse_mode")
                params["text"] = html_to_plain(chunk) or "…"
                result = self._call("sendMessage", json_body=params, **call_options)
            last = result if isinstance(result, dict) else None
        return last

    def edit_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        reply_markup: Mapping[str, Any] | None = None,
        parse_mode: str | None = "HTML",
    ) -> dict[str, Any] | None:
        """Replace a message's text (cut to one message). "Not modified" counts as success."""
        params: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": fit_message(text) or "…",
            "link_preview_options": {"is_disabled": True},
        }
        if parse_mode:
            params["parse_mode"] = parse_mode
        if reply_markup is not None:
            params["reply_markup"] = dict(reply_markup)
        try:
            result = self._call("editMessageText", json_body=params)
        except TelegramError as exc:
            if _is_not_modified(exc):
                return None
            raise
        return result if isinstance(result, dict) else None

    def edit_message_reply_markup(
        self,
        chat_id: int,
        message_id: int,
        reply_markup: Mapping[str, Any] | None = None,
        *,
        wait_on_rate_limit: bool = True,
    ) -> None:
        """Replace (default: remove) the inline keyboard of a message."""
        params = {
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": dict(reply_markup or REMOVE_INLINE_KEYBOARD),
        }
        try:
            self._call(
                "editMessageReplyMarkup", json_body=params, wait_on_rate_limit=wait_on_rate_limit
            )
        except TelegramError as exc:
            if not _is_not_modified(exc):
                raise

    def answer_callback_query(
        self,
        callback_query_id: str,
        text: str | None = None,
        *,
        show_alert: bool = False,
        wait_on_rate_limit: bool = True,
    ) -> None:
        """Answer a button press. ``text`` is shown as plain text (no parse mode)."""
        params: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            params["text"] = text[:MAX_CALLBACK_ANSWER]
        if show_alert:
            params["show_alert"] = True
        self._call("answerCallbackQuery", json_body=params, wait_on_rate_limit=wait_on_rate_limit)

    def delete_message(self, chat_id: int, message_id: int) -> bool:
        return (
            self._call("deleteMessage", json_body={"chat_id": chat_id, "message_id": message_id})
            is True
        )

    def send_document(
        self,
        chat_id: int,
        filename: str,
        content: bytes,
        *,
        caption: str | None = None,
        content_type: str = "text/plain",
    ) -> dict[str, Any] | None:
        """Upload ``content`` as a document (multipart/form-data)."""
        boundary = f"ynabhelper{secrets.token_hex(12)}"
        safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", filename)[:100] or "file"
        fields: list[tuple[str, str]] = [("chat_id", str(chat_id))]
        if caption:
            fields.append(("caption", fit_message(caption, MAX_CAPTION_UNITS)))
            fields.append(("parse_mode", "HTML"))
        parts: list[bytes] = []
        for name, value in fields:
            parts.append(
                f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
                + value.encode("utf-8")
                + b"\r\n"
            )
        parts.append(
            (
                f'--{boundary}\r\nContent-Disposition: form-data; name="document"; '
                f'filename="{safe_name}"\r\nContent-Type: {content_type}\r\n\r\n'
            ).encode()
            + content
            + b"\r\n"
        )
        parts.append(f"--{boundary}--\r\n".encode())
        result = self._call(
            "sendDocument",
            data=b"".join(parts),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            timeout=_DOCUMENT_TIMEOUT,
            total_timeout=2 * _DOCUMENT_TIMEOUT,
        )
        return result if isinstance(result, dict) else None

    # ── files ──

    def get_file(self, file_id: str) -> dict[str, Any]:
        result = self._call("getFile", json_body={"file_id": file_id})
        if not isinstance(result, dict):
            raise TelegramError("Telegram getFile returned no file")
        return result

    def download_file(
        self,
        file_path: str,
        dest_dir: str | os.PathLike[str],
        *,
        max_bytes: int = MAX_DOWNLOAD_BYTES,
        suffix: str = ".download",
    ) -> Path:
        """Download ``file_path`` (from :meth:`get_file`) into a new private file in
        ``dest_dir`` and return its path.

        The bytes go to a ``0600`` temp file that is renamed into place only when complete;
        on any failure (including the ``max_bytes`` cap: :class:`DownloadTooLarge`) nothing
        is left behind. The caller deletes the returned file.
        """
        if (
            not isinstance(file_path, str)
            or not _FILE_PATH_RE.fullmatch(file_path)
            or file_path.startswith("/")
            or ".." in file_path.split("/")
        ):
            raise TelegramError("Telegram returned an invalid file path")
        url = f"{self._base}/file/bot{self._token}/{urllib.parse.quote(file_path, safe='/')}"
        directory = Path(dest_dir)
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd, tmp_name = tempfile.mkstemp(dir=directory, prefix=".download-", suffix=".part")
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as out:
                self._stream(url, out, max_bytes)
                out.flush()
                os.fsync(out.fileno())
            final = tmp.with_name(tmp.name.removesuffix(".part") + suffix)
            os.replace(tmp, final)
            return final
        except BaseException:
            try:
                tmp.unlink()
            except OSError:
                pass
            raise

    def _stream(self, url: str, out: Any, max_bytes: int) -> None:
        request = urllib.request.Request(url, headers={"User-Agent": net.USER_AGENT}, method="GET")
        opener = self._opener or urllib.request.urlopen
        deadline = time.monotonic() + 4 * self._timeout
        try:
            with opener(request, timeout=self._timeout, context=None) as resp:
                status = int(getattr(resp, "status", None) or resp.getcode())
                if not 200 <= status < 300:
                    raise TelegramError(
                        f"Telegram file download failed: HTTP {status}", error_code=status
                    )
                length = resp.headers.get("Content-Length") if resp.headers else None
                if length and length.isascii() and length.isdigit() and int(length) > max_bytes:
                    raise DownloadTooLarge(max_bytes)
                read = getattr(resp, "read1", None) or resp.read
                size = 0
                while True:
                    if time.monotonic() > deadline:
                        raise TelegramError("Telegram file download timed out", retriable=True)
                    chunk = read(_DOWNLOAD_CHUNK)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > max_bytes:
                        raise DownloadTooLarge(max_bytes)
                    out.write(chunk)
        except urllib.error.HTTPError as exc:
            exc.close()
            raise TelegramError(
                f"Telegram file download failed: HTTP {exc.code}", error_code=exc.code
            ) from None
        except (OSError, http.client.HTTPException) as exc:
            reason = net.redact_text(f"{type(exc).__name__}: {exc}", (self._token,))[:200]
            raise TelegramError(
                f"Telegram file download failed: {reason}", retriable=True
            ) from None
