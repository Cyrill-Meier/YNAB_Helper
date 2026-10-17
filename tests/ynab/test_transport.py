"""The default urllib transport, exercised against a loopback HTTP server."""

from __future__ import annotations

import json
import socket
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from ynab_helper import __version__
from ynab_helper.errors import YnabError, YnabNetworkError, YnabNotFound
from ynab_helper.ynab import client as client_module
from ynab_helper.ynab.client import YnabClient, urllib_transport

TOKEN = "loopback-token-123"


class _Handler(BaseHTTPRequestHandler):
    server: _Server

    def log_message(self, format: str, *args: Any) -> None:  # keep test output clean
        pass

    def _send(self, status: int, payload: Any, headers: dict[str, str] | None = None) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        self.server.seen.append(
            {"method": "GET", "path": self.path, "headers": dict(self.headers.items())}
        )
        if self.path == "/v1/user":
            self._send(200, {"data": {"user": {"id": "u1"}}}, {"X-Rate-Limit": "7/200"})
        elif self.path == "/v1/budgets/missing/settings":
            self._send(
                404,
                {
                    "error": {
                        "id": "404.2",
                        "name": "resource_not_found",
                        "detail": "Resource not found",
                    }
                },
            )
        elif self.path == "/v1/user-redirect":
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/v1/target")
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif self.path == "/v1/target":
            self._send(200, {"data": {"user": {"id": "redirected"}}})
        elif self.path == "/v1/slow":
            time.sleep(0.6)
            self._send(200, {"data": {}})
        elif self.path == "/v1/close":
            self.close_connection = True  # read the request, answer nothing
        elif self.path == "/v1/trickle":
            body = json.dumps({"data": {"user": {"id": "slowpoke"}}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                for byte in body:  # one byte every 50 ms: each read is well within timeout
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    time.sleep(0.05)
            except OSError:
                pass  # the client gave up
        else:
            self._send(404, {"error": {"id": "404.1", "name": "not_found", "detail": "nope"}})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.server.seen.append(
            {
                "method": "POST",
                "path": self.path,
                "headers": dict(self.headers.items()),
                "body": body,
            }
        )
        self._send(
            201,
            {
                "data": {
                    "transaction_ids": [],
                    "transactions": [],
                    "duplicate_import_ids": ["YNAB:1:2026-10-01:1"],
                    "server_knowledge": 9,
                }
            },
        )


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self.seen: list[dict[str, Any]] = []


@pytest.fixture
def server() -> Iterator[_Server]:
    srv = _Server()
    thread = threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    thread.start()
    try:
        yield srv
    finally:
        srv.shutdown()
        srv.server_close()


def _client(server: _Server, **kwargs: Any) -> YnabClient:
    kwargs.setdefault("sleep", lambda _s: None)
    return YnabClient(TOKEN, base_url=f"http://127.0.0.1:{server.server_port}/v1", **kwargs)


def test_success_headers_and_rate_limit(server: _Server) -> None:
    client = _client(server)
    assert client.get_user() == {"id": "u1"}
    assert client.rate_limit_used == 7
    headers = {k.lower(): v for k, v in server.seen[0]["headers"].items()}
    assert headers["authorization"] == f"Bearer {TOKEN}"
    assert headers["user-agent"] == f"ynab-helper/{__version__}"


def test_http_error_status_is_returned_and_mapped(server: _Server) -> None:
    with pytest.raises(YnabNotFound) as info:
        _client(server).get_budget_currency("missing")
    assert info.value.error_id == "404.2"


def test_post_body_is_sent_as_json(server: _Server) -> None:
    result = _client(server).create_transactions(
        "b1",
        [
            {
                "account_id": "a",
                "date": "2026-10-01",
                "amount": 1,
                "import_id": "YNAB:1:2026-10-01:1",
            }
        ],
    )
    assert result.duplicate_import_ids == ["YNAB:1:2026-10-01:1"]
    assert result.server_knowledge == 9
    sent = json.loads(server.seen[0]["body"])
    assert sent["transactions"][0]["amount"] == 1


def test_redirects_are_not_followed(server: _Server) -> None:
    # Following would replay the Authorization header to wherever Location points.
    status, _headers, _body = urllib_transport(
        "GET",
        f"http://127.0.0.1:{server.server_port}/v1/user-redirect",
        {"Authorization": f"Bearer {TOKEN}"},
        None,
        5.0,
    )
    assert status == 302
    assert [s["path"] for s in server.seen] == ["/v1/user-redirect"]


def test_timeout_is_network_error(server: _Server) -> None:
    client = _client(server, timeout=0.2, max_retries=0)
    started = time.monotonic()
    with pytest.raises(YnabNetworkError):
        client._request("GET", "/slow")
    assert time.monotonic() - started < 0.6


def test_connection_closed_without_response_is_network_error(server: _Server) -> None:
    client = _client(server, max_retries=1)
    with pytest.raises(YnabNetworkError):
        client._request("GET", "/close")
    assert [s["path"] for s in server.seen] == ["/v1/close", "/v1/close"]


def test_connection_refused_is_network_error() -> None:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()  # nothing listens on this port now
    client = YnabClient(TOKEN, base_url=f"http://127.0.0.1:{port}/v1", max_retries=0)
    with pytest.raises(YnabNetworkError) as info:
        client.get_user()
    assert TOKEN not in str(info.value)


def test_unknown_path_is_typed(server: _Server) -> None:
    with pytest.raises(YnabError):
        _client(server)._request("GET", "/does-not-exist")


def test_oversized_body_is_not_retried(server: _Server, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client_module, "MAX_RESPONSE_BYTES", 16)
    client = _client(server, max_retries=2)
    with pytest.raises(YnabNetworkError) as info:
        client.get_user()
    assert info.value.retriable is False
    assert "large" in info.value.user_message
    assert [s["path"] for s in server.seen] == ["/v1/user"]  # one download, not three


def test_core_R15_trickling_body_hits_the_per_attempt_deadline(server: _Server) -> None:
    # Every byte arrives well within the 0.2 s socket timeout, but the whole body would take
    # ~2.5 s: the attempt must end at its deadline (3 × timeout) as a retriable network error.
    client = _client(server, timeout=0.2, max_retries=0)
    started = time.monotonic()
    with pytest.raises(YnabNetworkError) as info:
        client._request("GET", "/trickle")
    elapsed = time.monotonic() - started
    assert 0.5 <= elapsed < 1.5
    assert info.value.retriable is True


def test_core_R15_read_capped_checks_the_deadline_between_reads() -> None:
    now = [0.0]

    class Stream:
        def __init__(self) -> None:
            self.reads = 0

        def read1(self, _n: int) -> bytes:
            self.reads += 1
            now[0] += 1.0  # each socket read takes one "second"
            return b"x"

    stream = Stream()
    with pytest.raises(client_module.ResponseDeadlineExceeded):
        client_module._read_capped(stream, deadline=3.0, clock=lambda: now[0])
    assert stream.reads == 3
    assert isinstance(client_module.ResponseDeadlineExceeded(), OSError)


def test_core_R15_read_capped_prefers_single_socket_reads() -> None:
    class Stream:
        def __init__(self) -> None:
            self.parts = [b'{"a"', b":1}", b""]

        def read1(self, _n: int) -> bytes:
            return self.parts.pop(0)

        def read(self, _n: int) -> bytes:  # would block until n bytes arrived
            raise AssertionError("read() must not be used when read1() exists")

    assert client_module._read_capped(Stream(), deadline=None) == b'{"a":1}'
