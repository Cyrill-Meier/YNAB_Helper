"""Stateful in-memory fake of the Telegram Bot API (docs/DESIGN.md §12).

Plug it into the real client in-process::

    fake = FakeTelegram()
    client = TelegramClient(fake.token, api_base=fake.base_url, opener=fake.opener)

or serve it over loopback HTTP for a subprocess (``TELEGRAM_API_BASE=fake.start()``)::

    base_url = fake.start()        # http://127.0.0.1:<port>
    ...
    fake.stop()

What it models:

* ``getUpdates`` serves queued updates (built with :meth:`message`, :meth:`document`,
  :meth:`callback`), drops those below ``offset`` (confirmation, as Telegram does) and holds
  an empty poll for at most ``max_hold`` seconds;
* ``sendMessage`` / ``editMessageText`` / ``editMessageReplyMarkup`` / ``deleteMessage`` /
  ``answerCallbackQuery`` / ``sendDocument`` / ``getFile`` / ``setMyCommands`` / ``getMe``
  with Telegram's validations that matter to the bot: **strict HTML** checking for
  ``parse_mode=HTML`` (unknown or unbalanced tags, bare ``<``, ``>`` or ``&`` → 400
  "can't parse entities"), 4096 UTF-16 units per message, ``callback_data`` ≤ 64 bytes,
  "message is not modified", answering a callback query twice → 400, deleting an unknown
  message → 400; a callback answer is plain text, so HTML tags or entities in it (which
  Telegram would show literally) are recorded in :attr:`FakeTelegram.markup_in_answers`;
* file downloads from ``/file/bot<token>/<path>``;
* the bot token in the URL (wrong token → 401).

Inspection: :attr:`calls`, :meth:`sent`, :meth:`texts`, :meth:`edits`, :attr:`answers`,
:attr:`deleted`, :attr:`documents`, :attr:`commands`, :meth:`message_state`.

Fault injection: :meth:`fail_next` (HTTP status with Telegram's error body, optional
``retry_after``) and :meth:`network_error_next` (the opener raises).
"""

from __future__ import annotations

import email.parser
import email.policy
import html
import io
import itertools
import json
import re
import threading
import time
import urllib.error
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

__all__ = ["FakeTelegram", "HtmlError", "SentMessage", "check_html"]

FAKE_BOT_TOKEN = "123456:fake-telegram-token"
_ALLOWED_TAGS = frozenset(
    {
        "b",
        "strong",
        "i",
        "em",
        "u",
        "ins",
        "s",
        "strike",
        "del",
        "code",
        "pre",
        "a",
        "span",
        "tg-spoiler",
        "tg-emoji",
        "blockquote",
    }
)
_TAG_RE = re.compile(r"<(/?)([a-z][a-z0-9-]*)((?:\s+[a-z-]+=\"[^\"<>]*\")*)\s*>")
_ENTITY_RE = re.compile(r"&(?:lt|gt|amp|quot|#[0-9]{1,7}|#x[0-9a-fA-F]{1,6});")
#: HTML markup that would show up literally in a (plain-text) callback answer.
_ANSWER_MARKUP_RE = re.compile(r"</?[a-z][a-z0-9-]*(?:\s[^<>]*)?>|" + _ENTITY_RE.pattern)


class HtmlError(ValueError):
    """What Telegram would answer with "can't parse entities"."""


def check_html(text: str) -> str:
    """Validate Telegram HTML strictly; returns the plain text. Raises :class:`HtmlError`."""
    stack: list[str] = []
    plain: list[str] = []
    i = 0
    while i < len(text):
        char = text[i]
        if char == "<":
            match = _TAG_RE.match(text, i)
            if match is None:
                raise HtmlError(f"unexpected '<' at offset {i}")
            closing, name = match.group(1), match.group(2)
            if name not in _ALLOWED_TAGS:
                raise HtmlError(f'unsupported start tag "{name}" at offset {i}')
            if closing:
                if not stack or stack[-1] != name:
                    raise HtmlError(f"unmatched end tag at offset {i}")
                stack.pop()
            else:
                stack.append(name)
            i = match.end()
            continue
        if char == "&":
            match = _ENTITY_RE.match(text, i)
            if match is None:
                raise HtmlError(f"bare '&' at offset {i}")
            plain.append(html.unescape(match.group(0)))
            i = match.end()
            continue
        if char == ">":
            raise HtmlError(f"bare '>' at offset {i}")
        plain.append(char)
        i += 1
    if stack:
        raise HtmlError(f'can\'t find end tag corresponding to start tag "{stack[-1]}"')
    return "".join(plain)


def _utf16(text: str) -> int:
    return len(text.encode("utf-16-le", "surrogatepass")) // 2


class _ApiError(Exception):
    def __init__(self, status: int, description: str, parameters: dict[str, Any] | None = None):
        super().__init__(description)
        self.status = status
        self.description = description
        self.parameters = parameters


@dataclass
class SentMessage:
    """A message the bot sent (state after later edits is in :meth:`FakeTelegram.message_state`)."""

    chat_id: int
    message_id: int
    text: str
    plain: str
    reply_markup: dict[str, Any] | None
    parse_mode: str | None
    params: dict[str, Any] = field(repr=False)

    @property
    def buttons(self) -> list[dict[str, Any]]:
        markup = self.reply_markup or {}
        return [b for row in markup.get("inline_keyboard", []) for b in row]

    def button_data(self) -> list[str]:
        return [b["callback_data"] for b in self.buttons if "callback_data" in b]


@dataclass
class _Fault:
    method: str | None
    remaining: int
    status: int | None = None
    description: str = "Internal Server Error"
    retry_after: float | None = None
    exc: BaseException | None = None


@dataclass
class _Message:
    chat_id: int
    message_id: int
    text: str | None
    reply_markup: dict[str, Any] | None
    from_bot: bool
    deleted: bool = False


class _Response:
    """What ``urllib.request.urlopen`` returns, enough for :mod:`ynab_helper.net`."""

    def __init__(self, status: int, headers: Mapping[str, str], body: bytes) -> None:
        self.status = status
        self.headers = dict(headers)
        self._body = io.BytesIO(body)

    def getcode(self) -> int:
        return self.status

    def read(self, n: int = -1) -> bytes:
        return self._body.read(n)

    def read1(self, n: int = -1) -> bytes:
        return self._body.read1(n)

    def close(self) -> None:
        self._body.close()

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class FakeTelegram:
    """See the module docstring."""

    def __init__(
        self,
        token: str = FAKE_BOT_TOKEN,
        *,
        bot_id: int = 999000,
        username: str = "test_bot",
        max_hold: float = 0.05,
        base_url: str = "https://api.telegram.test",
    ) -> None:
        self.token = token
        self.bot_id = bot_id
        self.username = username
        self.max_hold = max_hold
        self.base_url = base_url
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.answers: list[dict[str, Any]] = []
        self.deleted: list[tuple[int, int]] = []
        self.documents: list[dict[str, Any]] = []
        self.commands: list[dict[str, Any]] = []
        self.files: dict[str, tuple[str, bytes]] = {}
        #: Texts Telegram would have rejected as broken HTML (a formatter bug).
        self.html_errors: list[str] = []
        #: Callback answers containing HTML markup, which Telegram shows verbatim.
        self.markup_in_answers: list[str] = []
        #: URL-button hosts Telegram refuses (like a non-public WEB_UI_PUBLIC_URL); the
        #: error echoes the whole URL, as the real API does.
        self.rejected_url_hosts: set[str] = set()
        self._sent: list[SentMessage] = []
        self._edits: list[dict[str, Any]] = []
        self._messages: dict[tuple[int, int], _Message] = {}
        self._updates: list[dict[str, Any]] = []
        self._callbacks: dict[str, bool] = {}  # id → answered
        self._faults: list[_Fault] = []
        self._ids = itertools.count(1)
        self._update_ids = itertools.count(1000)
        self._cond = threading.Condition()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    # ── building updates ─────────────────────────────────────────────────────

    def push(self, update: dict[str, Any]) -> int:
        """Queue a raw update (an ``update_id`` is assigned if missing)."""
        with self._cond:
            update.setdefault("update_id", next(self._update_ids))
            self._updates.append(update)
            self._cond.notify_all()
            return int(update["update_id"])

    @staticmethod
    def _user(from_id: int, username: str | None, first_name: str, is_bot: bool) -> dict[str, Any]:
        user: dict[str, Any] = {"id": from_id, "is_bot": is_bot, "first_name": first_name}
        if username:
            user["username"] = username
        return user

    def _chat(self, from_id: int, chat_id: int | None, chat_type: str) -> dict[str, Any]:
        cid = from_id if chat_id is None else chat_id
        return {"id": cid, "type": chat_type}

    def message(
        self,
        from_id: int,
        text: str | None = None,
        *,
        chat_id: int | None = None,
        chat_type: str = "private",
        username: str | None = None,
        first_name: str = "Tester",
        is_bot: bool = False,
        document: dict[str, Any] | None = None,
    ) -> int:
        """Queue a message from ``from_id`` (private chat by default); returns its update id."""
        chat = self._chat(from_id, chat_id, chat_type)
        with self._cond:
            message_id = next(self._ids)
            self._messages[(chat["id"], message_id)] = _Message(
                chat["id"], message_id, text, None, from_bot=False
            )
        message: dict[str, Any] = {
            "message_id": message_id,
            "from": self._user(from_id, username, first_name, is_bot),
            "chat": chat,
            "date": int(time.time()),
        }
        if text is not None:
            message["text"] = text
        if document is not None:
            message["document"] = document
        return self.push({"message": message})

    def add_file(self, file_id: str, content: bytes, *, file_path: str | None = None) -> None:
        with self._cond:
            self.files[file_id] = (file_path or f"documents/{file_id}.csv", content)

    def document(
        self,
        from_id: int,
        file_name: str,
        content: bytes | None = None,
        *,
        file_id: str | None = None,
        file_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """Queue a document message; ``content`` is registered for download."""
        fid = file_id or f"file-{next(self._ids)}"
        if content is not None:
            self.add_file(fid, content)
        doc: dict[str, Any] = {"file_id": fid, "file_unique_id": f"u-{fid}", "file_name": file_name}
        size = (
            file_size if file_size is not None else (len(content) if content is not None else None)
        )
        if size is not None:
            doc["file_size"] = size
        return self.message(from_id, None, document=doc, **kwargs)

    def callback(
        self,
        from_id: int,
        data: str,
        *,
        message_id: int | None = None,
        chat_id: int | None = None,
        chat_type: str = "private",
        username: str | None = None,
        first_name: str = "Tester",
    ) -> int:
        """Queue a button press on ``message_id`` (default: the bot's last message to the
        chat); returns its update id."""
        chat = self._chat(from_id, chat_id, chat_type)
        if message_id is None:
            sent = [m for m in self._sent if m.chat_id == chat["id"]]
            message_id = sent[-1].message_id if sent else next(self._ids)
        callback_id = f"cb-{next(self._ids)}"
        with self._cond:
            self._callbacks[callback_id] = False
        return self.push(
            {
                "callback_query": {
                    "id": callback_id,
                    "from": self._user(from_id, username, first_name, False),
                    "message": {"message_id": message_id, "chat": chat, "date": 0},
                    "chat_instance": "ci",
                    "data": data,
                }
            }
        )

    @property
    def pending_updates(self) -> int:
        with self._cond:
            return len(self._updates)

    def take_updates(self) -> list[dict[str, Any]]:
        """Remove and return every queued update (to hand them to the bot directly)."""
        with self._cond:
            updates, self._updates = self._updates, []
            return updates

    # ── inspection ───────────────────────────────────────────────────────────

    def sent(self, chat_id: int | None = None) -> list[SentMessage]:
        with self._cond:
            return [m for m in self._sent if chat_id is None or m.chat_id == chat_id]

    def texts(self, chat_id: int | None = None) -> list[str]:
        """Plain text (tags stripped, entities resolved) of every sent message."""
        return [m.plain for m in self.sent(chat_id)]

    def last(self, chat_id: int | None = None) -> SentMessage:
        sent = self.sent(chat_id)
        assert sent, "no message was sent"
        return sent[-1]

    def edits(self, chat_id: int | None = None) -> list[dict[str, Any]]:
        with self._cond:
            return [e for e in self._edits if chat_id is None or e["chat_id"] == chat_id]

    def message_state(self, chat_id: int, message_id: int) -> _Message:
        with self._cond:
            return self._messages[(chat_id, message_id)]

    def calls_of(self, method: str) -> list[dict[str, Any]]:
        with self._cond:
            return [params for name, params in self.calls if name == method]

    def all_text(self, chat_id: int | None = None) -> str:
        """Every sent and edited text (plain), joined — handy for "never contains" checks."""
        parts = self.texts(chat_id)
        parts += [e["plain"] for e in self.edits(chat_id)]
        return "\n".join(parts)

    # ── faults ───────────────────────────────────────────────────────────────

    def fail_next(
        self,
        method: str | None,
        status: int = 500,
        description: str = "Internal Server Error",
        *,
        retry_after: float | None = None,
        times: int = 1,
    ) -> None:
        """Answer the next ``times`` calls of ``method`` (None = any) with an error."""
        with self._cond:
            self._faults.append(
                _Fault(
                    method, times, status=status, description=description, retry_after=retry_after
                )
            )

    def network_error_next(
        self, method: str | None = None, *, times: int = 1, exc: BaseException | None = None
    ) -> None:
        """Make the opener raise (default ``URLError(ConnectionRefusedError)``)."""
        with self._cond:
            self._faults.append(
                _Fault(
                    method,
                    times,
                    exc=exc or urllib.error.URLError(ConnectionRefusedError(111, "refused")),
                )
            )

    def _take_fault(self, method: str) -> _Fault | None:
        for fault in self._faults:
            if fault.remaining > 0 and fault.method in (None, method):
                fault.remaining -= 1
                if fault.remaining == 0:
                    self._faults.remove(fault)
                return fault
        return None

    # ── transport ────────────────────────────────────────────────────────────

    def opener(self, request: Any, timeout: float | None = None, context: Any = None) -> _Response:
        """``urllib.request.urlopen`` replacement for in-process use."""
        url = request.full_url
        parts = urllib.parse.urlsplit(url)
        method_name = parts.path.rsplit("/", 1)[-1]
        with self._cond:
            fault = self._take_fault(method_name)
        if fault is not None and fault.exc is not None:
            raise fault.exc
        headers = {k.lower(): v for k, v in request.header_items()}
        status, out_headers, body = self._handle(
            request.get_method(), parts.path, headers, request.data, fault
        )
        return _Response(status, out_headers, body)

    def handle(
        self, http_method: str, path: str, headers: Mapping[str, str], body: bytes | None
    ) -> tuple[int, dict[str, str], bytes]:
        """Serve one request (used by the loopback server)."""
        method_name = path.rsplit("/", 1)[-1]
        with self._cond:
            fault = self._take_fault(method_name)
        if fault is not None and fault.exc is not None:
            return 502, {}, b""
        return self._handle(
            http_method, path, {k.lower(): v for k, v in headers.items()}, body, fault
        )

    def _handle(
        self,
        http_method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes | None,
        fault: _Fault | None,
    ) -> tuple[int, dict[str, str], bytes]:
        file_match = re.fullmatch(r"/file/bot([^/]+)/(.+)", path)
        if file_match:
            if file_match.group(1) != self.token:
                return 401, {}, b"Unauthorized"
            return self._download(urllib.parse.unquote(file_match.group(2)))
        api_match = re.fullmatch(r"/bot([^/]+)/([A-Za-z]+)", path)
        if api_match is None:
            return self._error(_ApiError(404, "Not Found"))
        if api_match.group(1) != self.token:
            return self._error(_ApiError(401, "Unauthorized"))
        method = api_match.group(2)
        try:
            params = self._params(headers, body)
        except ValueError:
            return self._error(_ApiError(400, "Bad Request: invalid body"))
        with self._cond:
            self.calls.append((method, params))
        if fault is not None and fault.status is not None:
            parameters = (
                {"retry_after": fault.retry_after} if fault.retry_after is not None else None
            )
            return self._error(_ApiError(fault.status, fault.description, parameters))
        handler = getattr(self, f"_api_{method}", None)
        if handler is None:
            return self._error(_ApiError(404, "Not Found: method not found"))
        try:
            result = handler(params)
        except _ApiError as exc:
            return self._error(exc)
        return (
            200,
            {"Content-Type": "application/json"},
            json.dumps({"ok": True, "result": result}).encode(),
        )

    @staticmethod
    def _error(exc: _ApiError) -> tuple[int, dict[str, str], bytes]:
        payload: dict[str, Any] = {
            "ok": False,
            "error_code": exc.status,
            "description": exc.description,
        }
        if exc.parameters:
            payload["parameters"] = exc.parameters
        return exc.status, {"Content-Type": "application/json"}, json.dumps(payload).encode()

    @staticmethod
    def _params(headers: Mapping[str, str], body: bytes | None) -> dict[str, Any]:
        if not body:
            return {}
        content_type = headers.get("content-type", "")
        if content_type.startswith("multipart/form-data"):
            parser = email.parser.BytesParser(policy=email.policy.HTTP)
            message = parser.parsebytes(
                b"Content-Type: " + content_type.encode() + b"\r\n\r\n" + body
            )
            params: dict[str, Any] = {}
            for part in message.iter_parts():
                name = part.get_param("name", header="content-disposition")
                filename = part.get_filename()
                payload = part.get_payload(decode=True) or b""
                if filename:
                    params[name] = {"filename": filename, "content": payload}
                else:
                    params[name] = payload.decode("utf-8")
            return params
        value = json.loads(body.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("body is not an object")
        return value

    def _download(self, file_path: str) -> tuple[int, dict[str, str], bytes]:
        with self._cond:
            for stored_path, content in self.files.values():
                if stored_path == file_path:
                    return 200, {"Content-Length": str(len(content))}, content
        return 404, {}, b"Not Found"

    # ── API methods ──────────────────────────────────────────────────────────

    def _api_getMe(self, params: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": self.bot_id,
            "is_bot": True,
            "first_name": "Test Bot",
            "username": self.username,
        }

    def _api_getUpdates(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        offset = int(params.get("offset") or 0)
        limit = int(params.get("limit") or 100)
        hold = min(float(params.get("timeout") or 0), self.max_hold)
        deadline = time.monotonic() + hold
        with self._cond:
            while True:
                self._updates = [u for u in self._updates if u["update_id"] >= offset]
                if self._updates:
                    return [json.loads(json.dumps(u)) for u in self._updates[:limit]]
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return []
                self._cond.wait(remaining)

    def _check_markup(self, markup: Any) -> None:
        if markup is None:
            return
        if not isinstance(markup, dict):
            raise _ApiError(400, "Bad Request: invalid reply_markup")
        for row in markup.get("inline_keyboard", []):
            for button in row:
                if not button.get("text"):
                    raise _ApiError(
                        400, "Bad Request: text buttons are unallowed in the inline keyboard"
                    )
                data = button.get("callback_data")
                url = button.get("url")
                if data is not None and len(data.encode()) > 64:
                    raise _ApiError(400, "Bad Request: BUTTON_DATA_INVALID")
                if url is not None and (
                    not re.match(r"https?://[^\s/]+", url)
                    or urllib.parse.urlsplit(url).hostname in self.rejected_url_hosts
                ):
                    raise _ApiError(
                        400,
                        f"Bad Request: inline keyboard button URL '{url}' is invalid: "
                        "Wrong HTTP URL",
                    )
                if data is None and url is None:
                    raise _ApiError(400, "Bad Request: can't parse inline keyboard button")

    def _check_text(self, text: Any, parse_mode: Any) -> str:
        if not isinstance(text, str) or not text.strip():
            raise _ApiError(400, "Bad Request: message text is empty")
        plain = text
        if parse_mode == "HTML":
            try:
                plain = check_html(text)
            except HtmlError as exc:
                with self._cond:
                    self.html_errors.append(text)
                raise _ApiError(400, f"Bad Request: can't parse entities: {exc}") from None
        if _utf16(plain) > 4096:
            raise _ApiError(400, "Bad Request: message is too long")
        return plain

    def _api_sendMessage(self, params: dict[str, Any]) -> dict[str, Any]:
        chat_id = int(params["chat_id"])
        plain = self._check_text(params.get("text"), params.get("parse_mode"))
        markup = params.get("reply_markup")
        self._check_markup(markup)
        with self._cond:
            message_id = next(self._ids)
            self._messages[(chat_id, message_id)] = _Message(
                chat_id, message_id, params["text"], markup, from_bot=True
            )
            self._sent.append(
                SentMessage(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=params["text"],
                    plain=plain,
                    reply_markup=markup,
                    parse_mode=params.get("parse_mode"),
                    params=params,
                )
            )
        return {
            "message_id": message_id,
            "chat": {"id": chat_id, "type": "private"},
            "date": int(time.time()),
            "text": plain,
        }

    def _bot_message(self, params: dict[str, Any], verb: str) -> _Message:
        key = (int(params["chat_id"]), int(params["message_id"]))
        message = self._messages.get(key)
        if message is None or message.deleted or not message.from_bot:
            raise _ApiError(400, f"Bad Request: message to {verb} not found")
        return message

    def _api_editMessageText(self, params: dict[str, Any]) -> dict[str, Any]:
        plain = self._check_text(params.get("text"), params.get("parse_mode"))
        markup = params.get("reply_markup")
        self._check_markup(markup)
        with self._cond:
            message = self._bot_message(params, "edit")
            if message.text == params["text"] and message.reply_markup == markup:
                raise _ApiError(400, "Bad Request: message is not modified")
            message.text = params["text"]
            message.reply_markup = markup
            self._edits.append(
                {
                    "chat_id": message.chat_id,
                    "message_id": message.message_id,
                    "text": params["text"],
                    "plain": plain,
                    "reply_markup": markup,
                }
            )
        return {"message_id": message.message_id, "chat": {"id": message.chat_id}, "text": plain}

    def _api_editMessageReplyMarkup(self, params: dict[str, Any]) -> dict[str, Any]:
        markup = params.get("reply_markup")
        self._check_markup(markup)
        with self._cond:
            message = self._bot_message(params, "edit")
            if message.reply_markup == markup or (
                not (message.reply_markup or {}).get("inline_keyboard")
                and not (markup or {}).get("inline_keyboard")
            ):
                raise _ApiError(400, "Bad Request: message is not modified")
            message.reply_markup = markup
            self._edits.append(
                {
                    "chat_id": message.chat_id,
                    "message_id": message.message_id,
                    "text": None,
                    "plain": "",
                    "reply_markup": markup,
                }
            )
        return {"message_id": message.message_id, "chat": {"id": message.chat_id}}

    def _api_answerCallbackQuery(self, params: dict[str, Any]) -> bool:
        callback_id = params.get("callback_query_id")
        with self._cond:
            if self._callbacks.get(callback_id) is not False:
                raise _ApiError(
                    400,
                    "Bad Request: query is too old and response timeout expired or query ID is invalid",
                )
            self._callbacks[callback_id] = True
            self.answers.append(params)
            text = params.get("text")
            if isinstance(text, str) and _ANSWER_MARKUP_RE.search(text):
                self.markup_in_answers.append(text)
        return True

    def _api_deleteMessage(self, params: dict[str, Any]) -> bool:
        key = (int(params["chat_id"]), int(params["message_id"]))
        with self._cond:
            message = self._messages.get(key)
            if message is None or message.deleted:
                raise _ApiError(400, "Bad Request: message to delete not found")
            message.deleted = True
            self.deleted.append(key)
        return True

    def _api_sendDocument(self, params: dict[str, Any]) -> dict[str, Any]:
        document = params.get("document")
        if not isinstance(document, dict):
            raise _ApiError(400, "Bad Request: there is no document in the request")
        caption = params.get("caption")
        if caption and params.get("parse_mode") == "HTML":
            try:
                check_html(caption)
            except HtmlError as exc:
                raise _ApiError(400, f"Bad Request: can't parse entities: {exc}") from None
        with self._cond:
            message_id = next(self._ids)
            self.documents.append(
                {
                    "chat_id": int(params["chat_id"]),
                    "filename": document["filename"],
                    "content": document["content"],
                    "caption": caption,
                }
            )
        return {"message_id": message_id, "chat": {"id": int(params["chat_id"])}}

    def _api_getFile(self, params: dict[str, Any]) -> dict[str, Any]:
        file_id = params.get("file_id")
        with self._cond:
            if file_id not in self.files:
                raise _ApiError(400, "Bad Request: invalid file_id")
            path, content = self.files[file_id]
        return {
            "file_id": file_id,
            "file_unique_id": f"u-{file_id}",
            "file_size": len(content),
            "file_path": path,
        }

    def _api_setMyCommands(self, params: dict[str, Any]) -> bool:
        with self._cond:
            self.commands.append(params)
        return True

    # ── loopback HTTP mode ───────────────────────────────────────────────────

    def start(self, port: int = 0) -> str:
        """Serve the fake on ``127.0.0.1:<port>``; returns the base URL."""
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def _serve(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else None
                status, headers, payload = fake.handle(
                    self.command, urllib.parse.urlsplit(self.path).path, dict(self.headers), body
                )
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = _serve
            do_POST = _serve

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        host, bound_port = self._server.server_address[:2]
        self.base_url = f"http://{host}:{bound_port}"
        return self.base_url

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
