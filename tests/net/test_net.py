"""Tests for ynab_helper.net.request_json (DESIGN §4.3; findings F42, F78)."""

from __future__ import annotations

import http.client
import json
import logging
import math
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tests.net.fake_opener import (
    FakeClock,
    FakeOpener,
    FakeResponse,
    SleepRecorder,
    TricklingResponse,
    http_error,
    json_response,
)
from ynab_helper import __version__, net
from ynab_helper.errors import ExternalServiceError

URL = "https://api.example.com/v1/thing"


def call(opener: FakeOpener, sleep: SleepRecorder | None = None, **kwargs):
    kwargs.setdefault("retries", 3)
    return net.request_json(
        kwargs.pop("method", "GET"),
        kwargs.pop("url", URL),
        opener=opener,
        sleep=sleep or SleepRecorder(),
        **kwargs,
    )


# ── happy path ───────────────────────────────────────────────────────────────


def test_get_returns_decoded_json_and_sends_defaults():
    opener = FakeOpener(json_response({"ok": True, "n": 3}))
    assert call(opener, timeout=7) == {"ok": True, "n": 3}
    (c,) = opener.calls
    assert c.method == "GET"
    assert c.url == URL
    assert c.timeout == 7.0
    assert c.body is None
    assert c.header("User-Agent") == f"ynab-helper/{__version__}"
    assert c.header("Accept") == "application/json"


def test_post_json_body_is_encoded_with_content_type():
    opener = FakeOpener(json_response({"result": "0x1"}))
    call(opener, method="post", json_body={"jsonrpc": "2.0", "params": ["ä"]})
    (c,) = opener.calls
    assert c.method == "POST"
    assert c.header("Content-Type") == "application/json"
    assert json.loads(c.body) == {"jsonrpc": "2.0", "params": ["ä"]}


def test_raw_data_body_and_custom_headers_override_defaults():
    opener = FakeOpener(json_response([]))
    call(
        opener,
        method="POST",
        data=b"a=1",
        headers={"Content-Type": "application/x-www-form-urlencoded", "user-agent": "custom"},
    )
    (c,) = opener.calls
    assert c.body == b"a=1"
    assert c.header("Content-Type") == "application/x-www-form-urlencoded"
    assert c.header("User-Agent") == "custom"


def test_json_body_and_data_are_mutually_exclusive():
    opener = FakeOpener()
    with pytest.raises(ValueError, match="either"):
        call(opener, json_body={}, data=b"x")
    assert opener.calls == []


def test_empty_body_returns_none():
    opener = FakeOpener(FakeResponse(status=204, body=b""))
    assert call(opener) is None


def test_2xx_other_than_200_is_success():
    opener = FakeOpener(json_response({"created": 1}, status=201))
    assert call(opener) == {"created": 1}


def test_parse_float_decimal_keeps_exact_values():
    opener = FakeOpener(FakeResponse(body=b'{"chf": 0.1, "n": 5}'))
    result = call(opener, parse_float=Decimal)
    assert result == {"chf": Decimal("0.1"), "n": 5}
    assert isinstance(result["chf"], Decimal)


def test_ssl_context_is_passed_to_the_opener():
    ctx = ssl.create_default_context()
    opener = FakeOpener(json_response({}))
    call(opener, ssl_context=ctx)
    assert opener.calls[0].context is ctx


def test_default_opener_is_urlopen_looked_up_at_call_time(monkeypatch):
    fake = FakeOpener(json_response({"via": "patched"}))
    monkeypatch.setattr(urllib.request, "urlopen", fake)
    assert net.request_json("GET", URL, sleep=SleepRecorder()) == {"via": "patched"}
    assert len(fake.calls) == 1


# ── argument validation ──────────────────────────────────────────────────────


@pytest.mark.parametrize("timeout", [None, 0, -1, math.inf, math.nan, True, "5"])
def test_every_call_requires_a_finite_positive_timeout(timeout):
    opener = FakeOpener()
    with pytest.raises(ValueError, match="timeout"):
        call(opener, timeout=timeout)
    assert opener.calls == []


@pytest.mark.parametrize(
    "url",
    ["ftp://example.com/x", "file:///etc/passwd", "/relative", "https://", "https://a b.com/"],
)
def test_rejects_non_http_urls(url):
    with pytest.raises(ValueError):
        call(FakeOpener(), url=url)


@pytest.mark.parametrize(
    "url",
    [
        "https://localhost:abc/v1/api/portfolio/accounts",
        "https://localhost:70000/x",
        "https://localhost:-1/x",
        "https://:443/x",
    ],
)
def test_malformed_port_or_missing_host_is_rejected_before_any_attempt(url):
    """A bad port is a permanent configuration error, not a network blip to retry."""
    opener, sleep = FakeOpener(), SleepRecorder()
    with pytest.raises(ValueError, match=r"port|host"):
        call(opener, sleep, url=url, retries=2)
    assert opener.calls == []
    assert sleep.calls == []


def test_invalid_port_through_real_urllib_is_not_retried():
    sleep = SleepRecorder()
    with pytest.raises(ValueError, match="invalid port"):
        net.request_json("GET", "https://localhost:abc/x", retries=2, sleep=sleep)
    assert sleep.calls == []


def test_http_client_invalid_url_is_not_retried():
    """Defence in depth: should urllib still reject the URL, waiting will not fix it."""
    sleep = SleepRecorder()
    opener = FakeOpener(http.client.InvalidURL("nonnumeric port: 'abc'"), json_response({}))
    with pytest.raises(ExternalServiceError) as info:
        call(opener, sleep)
    assert info.value.retriable is False
    assert len(opener.calls) == 1
    assert sleep.calls == []


@pytest.mark.parametrize("total_timeout", [0, -1, math.inf, math.nan, True, "5"])
def test_rejects_invalid_total_timeout(total_timeout):
    opener = FakeOpener()
    with pytest.raises(ValueError, match="total_timeout"):
        call(opener, total_timeout=total_timeout)
    assert opener.calls == []


@pytest.mark.parametrize("retries", [-1, 1.5, True])
def test_rejects_invalid_retries(retries):
    with pytest.raises(ValueError, match="retries"):
        call(FakeOpener(), retries=retries)


# ── HTTP status handling ─────────────────────────────────────────────────────


def test_retriable_status_is_retried_with_exponential_backoff():
    sleep = SleepRecorder()
    opener = FakeOpener(http_error(URL, 503), http_error(URL, 502), json_response({"ok": 1}))
    assert call(opener, sleep, backoff=2.0, max_backoff=30) == {"ok": 1}
    assert len(opener.calls) == 3
    assert len(sleep.calls) == 2
    # Attempt n waits between 50 % and 100 % of backoff * 2**n.
    assert 1.0 <= sleep.calls[0] <= 2.0
    assert 2.0 <= sleep.calls[1] <= 4.0


def test_non_2xx_response_returned_without_exception_is_still_an_error():
    sleep = SleepRecorder()
    opener = FakeOpener(FakeResponse(status=500, body=b"oops"), json_response({"ok": 1}))
    assert call(opener, sleep) == {"ok": 1}
    assert len(sleep.calls) == 1


def test_retry_after_seconds_is_honoured_exactly():
    sleep = SleepRecorder()
    opener = FakeOpener(http_error(URL, 429, headers={"Retry-After": "7"}), json_response(1))
    assert call(opener, sleep) == 1
    assert sleep.calls == [7.0]


def test_retry_after_above_max_backoff_raises_without_waiting():
    sleep = SleepRecorder()
    opener = FakeOpener(http_error(URL, 429, headers={"Retry-After": "120"}), json_response(1))
    with pytest.raises(net.HttpStatusError) as info:
        call(opener, sleep, max_backoff=30)
    assert info.value.status == 429
    assert info.value.retriable is True
    assert info.value.retry_after == 120.0
    assert sleep.calls == []
    assert len(opener.calls) == 1


def test_non_retriable_status_raises_immediately_with_payload():
    sleep = SleepRecorder()
    body = json.dumps({"ok": False, "error_code": 404, "description": "Not Found"})
    opener = FakeOpener(http_error(URL, 404, body))
    with pytest.raises(net.HttpStatusError) as info:
        call(opener, sleep, service="Telegram")
    exc = info.value
    assert isinstance(exc, ExternalServiceError)
    assert (exc.service, exc.status, exc.retriable) == ("Telegram", 404, False)
    assert exc.payload == {"ok": False, "error_code": 404, "description": "Not Found"}
    assert "HTTP 404" in str(exc)
    assert "Not Found" in str(exc)
    assert exc.user_message == "Telegram returned an error (HTTP 404)."
    assert sleep.calls == []
    assert len(opener.calls) == 1


def test_retries_exhausted_raises_retriable_error():
    sleep = SleepRecorder()
    opener = FakeOpener(*[http_error(URL, 500) for _ in range(3)])
    with pytest.raises(net.HttpStatusError) as info:
        call(opener, sleep, retries=2)
    assert info.value.status == 500
    assert info.value.retriable is True
    assert "try again later" in info.value.user_message
    assert len(opener.calls) == 3
    assert len(sleep.calls) == 2


def test_retries_zero_means_a_single_attempt():
    opener = FakeOpener(http_error(URL, 503))
    with pytest.raises(net.HttpStatusError):
        call(opener, retries=0)
    assert len(opener.calls) == 1


def test_custom_retry_statuses():
    opener = FakeOpener(http_error(URL, 503))
    with pytest.raises(net.HttpStatusError) as info:
        call(opener, retry_statuses=(429,))
    assert info.value.retriable is False
    assert len(opener.calls) == 1


def test_F42_http_errors_raise_a_typed_exception_instead_of_exiting():
    """v1's CoinGecko call ran sys.exit(1) on HTTPError; now it is a typed, catchable error."""
    opener = FakeOpener(*[http_error(URL, 429, "rate limited") for _ in range(4)])
    with pytest.raises(ExternalServiceError) as info:
        call(opener, service="CoinGecko", retries=3)
    assert not isinstance(info.value, SystemExit)
    assert info.value.service == "CoinGecko"
    assert info.value.retriable is True


# ── network failures ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "error",
    [
        urllib.error.URLError("connection refused"),
        urllib.error.URLError(TimeoutError("timed out")),
        ConnectionResetError(54, "reset by peer"),
        TimeoutError("timed out"),
        http.client.RemoteDisconnected("closed"),
        http.client.IncompleteRead(b"par"),
        http.client.BadStatusLine("garbage"),
        OSError(65, "No route to host"),
    ],
    ids=lambda e: type(e).__name__,
)
def test_network_errors_are_retried(error):
    sleep = SleepRecorder()
    opener = FakeOpener(error, json_response({"ok": True}))
    assert call(opener, sleep) == {"ok": True}
    assert len(sleep.calls) == 1


def test_F78_timeout_while_reading_the_body_is_retried():
    """A TimeoutError raised by resp.read() (not urlopen) used to escape the retry loop."""
    sleep = SleepRecorder()
    opener = FakeOpener(
        FakeResponse(status=200, read_error=TimeoutError("The read operation timed out")),
        json_response({"chain_stats": {}}),
    )
    assert call(opener, sleep) == {"chain_stats": {}}
    assert len(opener.calls) == 2


def test_network_error_after_retries_is_a_typed_retriable_error():
    opener = FakeOpener(*[TimeoutError("timed out") for _ in range(3)])
    with pytest.raises(ExternalServiceError) as info:
        call(opener, retries=2, timeout=4, service="Blockstream")
    exc = info.value
    assert type(exc) is ExternalServiceError
    assert exc.status is None
    assert exc.retriable is True
    assert exc.service == "Blockstream"
    assert "timed out after 4s" in str(exc)
    assert "Could not reach Blockstream" in exc.user_message


def test_certificate_verification_errors_are_not_retried():
    error = urllib.error.URLError(ssl.SSLCertVerificationError(1, "self-signed certificate"))
    opener = FakeOpener(error, json_response({}))
    with pytest.raises(ExternalServiceError) as info:
        call(opener)
    assert info.value.retriable is False
    assert len(opener.calls) == 1


# ── body decoding ────────────────────────────────────────────────────────────


def test_invalid_json_is_retried_then_raised():
    sleep = SleepRecorder()
    opener = FakeOpener(*[FakeResponse(body=b"<html>busy</html>") for _ in range(3)])
    with pytest.raises(ExternalServiceError) as info:
        call(opener, sleep, retries=2)
    assert "invalid JSON" in str(info.value)
    assert info.value.status == 200
    assert len(opener.calls) == 3


@pytest.mark.parametrize("literal", [b"NaN", b"Infinity", b"-Infinity"])
def test_non_finite_json_constants_are_rejected(literal):
    opener = FakeOpener(FakeResponse(body=b'{"chf": ' + literal + b"}"))
    with pytest.raises(ExternalServiceError, match="invalid JSON"):
        call(opener, retries=0)


def test_response_size_limit():
    opener = FakeOpener(FakeResponse(body=b"[1,2,3,4]"), FakeResponse(body=b"[1,2,3,45]"))
    assert call(opener, max_response_bytes=9) == [1, 2, 3, 4]
    with pytest.raises(ExternalServiceError, match="more than 9 bytes") as info:
        call(opener, max_response_bytes=9)
    assert info.value.retriable is False


def test_body_is_read_in_bounded_chunks():
    body = json.dumps(list(range(40_000))).encode()
    response = FakeResponse(body=body)
    assert call(FakeOpener(response)) == list(range(40_000))
    assert len(response.reads) > 2
    assert max(response.reads) <= 64 * 1024


def test_truncated_body_is_a_retriable_error_not_a_short_value():
    """http.client returns a short body silently; '12' of '1234' would parse as 12."""
    sleep = SleepRecorder()
    short = TricklingResponse(body=b"12", step=8, length=4)
    opener = FakeOpener(short, json_response(1234))
    assert call(opener, sleep) == 1234
    assert len(opener.calls) == 2
    assert len(sleep.calls) == 1


def test_truncated_body_after_retries_raises():
    opener = FakeOpener(*[TricklingResponse(body=b"12", step=8, length=4) for _ in range(2)])
    with pytest.raises(ExternalServiceError) as info:
        call(opener, retries=1)
    assert info.value.retriable is True
    assert "IncompleteRead" in str(info.value)


# ── per-attempt deadline ─────────────────────────────────────────────────────


def test_trickling_body_is_cut_off_by_the_default_deadline():
    """Each read finishes within ``timeout``; the whole body would take 2.1 s at timeout=0.2."""
    clock = FakeClock()
    body = b"[" + b"1," * 20 + b"1]"
    started = clock.now
    response = TricklingResponse(body=body, step=1, seconds_per_read=0.05, clock=clock)
    with pytest.raises(ExternalServiceError) as info:
        call(FakeOpener(response), timeout=0.2, retries=0, clock=clock)
    exc = info.value
    assert type(exc) is ExternalServiceError
    assert exc.retriable is True
    assert exc.status is None
    assert "timed out" in str(exc)
    assert "0.6s" in str(exc)  # DEFAULT_TOTAL_TIMEOUT_FACTOR * timeout
    assert "did not respond in time" in exc.user_message
    # The deadline is checked before every read, so it is overrun by at most one read.
    assert clock.now - started <= 0.6 + 0.05 + 1e-9
    assert response.pos < len(body)


def test_deadline_miss_is_retried_like_a_timeout():
    clock = FakeClock()
    sleep = SleepRecorder()
    slow = TricklingResponse(body=b"[1,2,3]", step=1, seconds_per_read=1.0, clock=clock)
    opener = FakeOpener(slow, json_response([1, 2, 3]))
    assert call(opener, sleep, timeout=10, total_timeout=2, clock=clock) == [1, 2, 3]
    assert len(opener.calls) == 2
    assert len(sleep.calls) == 1


def test_body_within_the_deadline_is_read_completely_via_read1():
    clock = FakeClock()
    body = json.dumps({"chain_stats": {"funded_txo_sum": 5}}).encode()
    response = TricklingResponse(
        body=body, step=4, seconds_per_read=0.01, clock=clock, length=len(body)
    )
    assert call(FakeOpener(response), timeout=1, clock=clock) == {
        "chain_stats": {"funded_txo_sum": 5}
    }
    assert response.length == 0


def test_deadline_counts_the_time_until_the_headers_arrived():
    clock = FakeClock()

    def slow_headers(_request):
        clock.now += 61.0  # e.g. a server dribbling its headers
        return json_response({"late": True})

    with pytest.raises(ExternalServiceError, match="timed out"):
        call(FakeOpener(slow_headers), timeout=20, retries=0, clock=clock)


def test_error_body_that_misses_the_deadline_keeps_the_status():
    clock = FakeClock()
    body = b'{"description": "Not Found"}'
    response = TricklingResponse(status=404, body=body, step=1, seconds_per_read=1.0, clock=clock)
    with pytest.raises(net.HttpStatusError) as info:
        call(FakeOpener(response), timeout=1, clock=clock)
    assert info.value.status == 404
    assert info.value.retriable is False
    assert info.value.payload is None


# ── redaction ────────────────────────────────────────────────────────────────


def test_secrets_are_redacted_from_messages_and_logs(caplog):
    secret = "s3cr3t/K3y with+chars"
    quoted = urllib.parse.quote(secret, safe="")
    url = f"https://rpc.example.com/eth/{quoted}?key={quoted}"
    opener = FakeOpener(
        urllib.error.URLError(f"failed to connect to {url}"),
        http_error(url, 503, f"bad key {secret}"),
    )
    caplog.set_level(logging.DEBUG, logger="ynab_helper.net")
    with pytest.raises(ExternalServiceError) as info:
        call(opener, url=url, retries=1, redact=[secret], service="Ethereum RPC")
    exc = info.value
    texts = [str(exc), exc.user_message, repr(exc.args), caplog.text]
    for text in texts:
        assert secret not in text
        assert quoted not in text
    assert net.REDACTED in str(exc)
    assert net.REDACTED in caplog.text


def test_secret_cut_by_the_error_body_read_limit_does_not_leak_a_prefix():
    """The 64 KiB error-body cap can split a secret; whitespace collapsing would then move
    the unredactable prefix into the message snippet."""
    secret = "SECRETKEY-0123456789abcdefghij"
    limit = 64 * 1024
    start = limit - 10  # the secret straddles the read limit
    body = b" " * start + secret.encode() + b" trailing"
    opener = FakeOpener(FakeResponse(status=400, body=body))
    with pytest.raises(net.HttpStatusError) as info:
        call(opener, redact=[secret])
    for text in (str(info.value), info.value.user_message):
        assert "SECRET" not in text
        assert secret[:10] not in text


def test_secret_before_the_snippet_cut_is_redacted_whole():
    secret = "SECRETKEY-0123456789abcdefghij"
    body = ("x" * 190 + " " + secret).encode()  # secret straddles the 200-char snippet cut
    opener = FakeOpener(http_error(URL, 400, body))
    with pytest.raises(net.HttpStatusError) as info:
        call(opener, redact=[secret])
    assert "SECRET" not in str(info.value)


def test_errors_do_not_chain_the_original_exception():
    """Chained urllib exceptions carry the raw URL (HTTPError.url) into tracebacks."""
    opener = FakeOpener(http_error(URL + "?token=abc", 400))
    with pytest.raises(ExternalServiceError) as info:
        call(opener, url=URL + "?token=abc", redact=["abc"])
    assert info.value.__cause__ is None
    assert info.value.__suppress_context__ is True


def test_redact_text_helper():
    assert net.redact_text("key=abc&x=abc", ["abc"]) == "key=***&x=***"
    assert net.redact_text("a b", ["", "zzz"]) == "a b"
    assert net.redact_text("x=a%2Fb", ["a/b"]) == "x=***"


# ── backoff helpers ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("attempt", range(12))
def test_backoff_delay_is_jittered_and_capped(attempt):
    for _ in range(20):
        delay = net.backoff_delay(attempt, 1.0, 30.0)
        nominal = min(30.0, 2.0**attempt)
        assert nominal / 2 <= delay <= nominal


def test_parse_retry_after():
    now = datetime(2026, 10, 16, 12, 0, 0, tzinfo=UTC)
    assert net.parse_retry_after(None) is None
    assert net.parse_retry_after("") is None
    assert net.parse_retry_after("soon") is None
    assert net.parse_retry_after("1.5") == 1.5
    assert net.parse_retry_after("-5") == 0.0
    assert net.parse_retry_after("inf") is None
    assert net.parse_retry_after("Fri, 16 Oct 2026 12:00:10 GMT", now=now) == 10.0
    assert net.parse_retry_after("Fri, 16 Oct 2026 11:00:00 GMT", now=now) == 0.0


def test_retry_after_http_date_is_honoured(monkeypatch):
    sleep = SleepRecorder()
    when = datetime.now(UTC).timestamp() + 5
    header = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(when))
    opener = FakeOpener(http_error(URL, 503, headers={"Retry-After": header}), json_response(1))
    assert call(opener, sleep) == 1
    assert len(sleep.calls) == 1
    assert 3.0 <= sleep.calls[0] <= 6.0


# ── real sockets on loopback ─────────────────────────────────────────────────


class _SilentServer:
    """Accepts TCP connections on loopback and never answers (a hung upstream)."""

    def __init__(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.port = self.sock.getsockname()[1]
        self.accepted: list[socket.socket] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        self.sock.settimeout(0.05)
        while not self._stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except OSError:
                continue
            self.accepted.append(conn)

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)
        for conn in self.accepted:
            conn.close()
        self.sock.close()


def test_F42_hung_server_times_out_instead_of_blocking_forever():
    server = _SilentServer()
    try:
        started = time.monotonic()
        with pytest.raises(ExternalServiceError) as info:
            net.request_json(
                "GET",
                f"http://127.0.0.1:{server.port}/simple/price",
                timeout=0.3,
                retries=1,
                sleep=lambda _s: None,
                service="CoinGecko",
            )
        elapsed = time.monotonic() - started
    finally:
        server.close()
    assert info.value.retriable is True
    assert "timed out" in str(info.value)
    assert elapsed < 5


class _DripServer:
    """Loopback HTTP server that sends a valid response one body byte every ``interval``."""

    def __init__(self, body: bytes, interval: float) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(1)
        self.port = self.sock.getsockname()[1]
        self.body = body
        self.interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        self.sock.settimeout(5)
        try:
            conn, _ = self.sock.accept()
        except OSError:
            return
        with conn:
            conn.recv(65536)
            head = (
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Content-Length: " + str(len(self.body)).encode() + b"\r\n\r\n"
            )
            try:
                conn.sendall(head)
                for i in range(len(self.body)):
                    if self._stop.wait(self.interval):
                        return
                    conn.sendall(self.body[i : i + 1])
            except OSError:
                return

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)
        self.sock.close()


def test_trickling_server_on_real_sockets_is_cut_off_by_the_deadline():
    """Reviewer probe: one byte per 50 ms kept a timeout=0.2 call alive for 2.3 s."""
    server = _DripServer(b"[" + b"1," * 20 + b"1]", interval=0.05)
    try:
        started = time.monotonic()
        with pytest.raises(ExternalServiceError, match="timed out") as info:
            net.request_json(
                "GET",
                f"http://127.0.0.1:{server.port}/x",
                timeout=0.2,
                total_timeout=0.4,
                retries=0,
                sleep=lambda _s: None,
            )
        elapsed = time.monotonic() - started
    finally:
        server.close()
    assert info.value.retriable is True
    assert elapsed < 0.4 + 0.2 + 0.5  # deadline + one blocked read + scheduling slack


class _ShortBodyServer:
    """Announces ``Content-Length: 4`` but sends ``12`` and closes the connection."""

    def __init__(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(4)
        self.port = self.sock.getsockname()[1]
        self.served = 0
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        self.sock.settimeout(5)
        while self.served < 2:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            with conn:
                conn.recv(65536)
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n12")
            self.served += 1

    def close(self) -> None:
        self._thread.join(timeout=2)
        self.sock.close()


def test_truncated_body_on_real_sockets_is_not_returned_as_a_number():
    server = _ShortBodyServer()
    try:
        with pytest.raises(ExternalServiceError) as info:
            net.request_json(
                "GET",
                f"http://127.0.0.1:{server.port}/balance",
                timeout=2,
                retries=1,
                sleep=lambda _s: None,
            )
    finally:
        server.close()
    assert info.value.retriable is True
    assert "IncompleteRead" in str(info.value)
    assert server.served == 2
