"""Scripted stand-in for ``urllib.request.urlopen`` used with ``net.request_json(opener=…)``."""

from __future__ import annotations

import email.message
import io
import json
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FakeResponse:
    """Minimal ``http.client.HTTPResponse`` look-alike (``read`` only, no ``read1``)."""

    status: int = 200
    body: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)
    read_error: BaseException | None = None
    pos: int = 0
    reads: list[int] = field(default_factory=list)

    def read(self, amt: int | None = -1) -> bytes:
        if self.read_error is not None:
            raise self.read_error
        end = len(self.body) if amt is None or amt < 0 else self.pos + amt
        chunk = self.body[self.pos : end]
        self.pos += len(chunk)
        self.reads.append(len(chunk))
        return chunk

    def getcode(self) -> int:
        return self.status

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


@dataclass
class TricklingResponse(FakeResponse):
    """Delivers its body in ``step``-byte ``read1`` calls, advancing ``clock`` per call.

    Mimics a server that sends a byte now and then: every single read finishes well within
    the socket timeout, yet the whole body takes ``len(body) / step * seconds_per_read``.
    Optionally reports ``length`` like ``http.client.HTTPResponse`` (bytes still expected
    per ``Content-Length``).
    """

    step: int = 1
    seconds_per_read: float = 0.0
    clock: FakeClock | None = None
    length: int | None = None

    def read1(self, amt: int = -1) -> bytes:
        if self.clock is not None:
            self.clock.now += self.seconds_per_read
        limit = self.step if amt is None or amt < 0 else min(amt, self.step)
        chunk = self.body[self.pos : self.pos + limit]
        self.pos += len(chunk)
        self.reads.append(len(chunk))
        if self.length is not None:
            self.length = max(0, self.length - len(chunk))
        return chunk

    def read(self, amt: int | None = -1) -> bytes:  # pragma: no cover - must not be used
        raise AssertionError("request_json must prefer read1() so it can check its deadline")


class FakeClock:
    """Monotonic clock under test control."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


@dataclass
class OpenerCall:
    request: urllib.request.Request
    timeout: float | None
    context: Any

    @property
    def url(self) -> str:
        return self.request.full_url

    @property
    def method(self) -> str:
        return self.request.get_method()

    @property
    def body(self) -> bytes | None:
        return self.request.data  # type: ignore[return-value]

    def header(self, name: str) -> str | None:
        return self.request.get_header(name.capitalize())


Outcome = FakeResponse | BaseException | Callable[[urllib.request.Request], FakeResponse]


class FakeOpener:
    """Returns (or raises) the scripted outcomes in order and records every call."""

    def __init__(self, *outcomes: Outcome) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[OpenerCall] = []

    def __call__(
        self, request: urllib.request.Request, *, timeout: float | None = None, context: Any = None
    ) -> FakeResponse:
        self.calls.append(OpenerCall(request, timeout, context))
        if not self.outcomes:
            raise AssertionError(f"unexpected request {request.get_method()} {request.full_url}")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        if callable(outcome) and not isinstance(outcome, FakeResponse):
            return outcome(request)
        return outcome


def json_response(
    obj: Any, status: int = 200, headers: Mapping[str, str] | None = None
) -> FakeResponse:
    return FakeResponse(status=status, body=json.dumps(obj).encode(), headers=dict(headers or {}))


def http_error(
    url: str, code: int, body: bytes | str = b"", headers: Mapping[str, str] | None = None
) -> urllib.error.HTTPError:
    msg = email.message.Message()
    for name, value in (headers or {}).items():
        msg[name] = value
    raw = body.encode() if isinstance(body, str) else body
    return urllib.error.HTTPError(url, code, f"HTTP {code}", msg, io.BytesIO(raw))


class SleepRecorder:
    """Records requested sleeps instead of sleeping."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
