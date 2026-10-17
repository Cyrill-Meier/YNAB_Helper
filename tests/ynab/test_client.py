"""YnabClient: error mapping, retries, rate limits, chunking, secrecy (DESIGN §4.2, F17)."""

from __future__ import annotations

import http.client
import json
import logging
import math
import ssl
import urllib.error
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from email.utils import format_datetime
from typing import Any

import pytest

from tests.fakes.fake_ynab import FakeYnab
from tests.ynab.conftest import ACCOUNT, BUDGET, OTHER_ACCOUNT, new_tx
from ynab_helper import __version__
from ynab_helper.errors import (
    ConfigError,
    ExternalServiceError,
    YnabAuthError,
    YnabError,
    YnabNetworkError,
    YnabNotFound,
    YnabRateLimited,
    YnabServerError,
    YnabValidationError,
)
from ynab_helper.net import parse_retry_after
from ynab_helper.ynab.client import ResponseTooLarge, YnabClient, _parse_rate_limit
from ynab_helper.ynab.models import YnabTransaction

SECRET = "sEcReT-PAT-0123456789abcdef"

Response = tuple[int, Mapping[str, str], bytes]


class ScriptedTransport:
    """Returns (or raises) pre-programmed responses and records every request."""

    def __init__(self, *responses: Response | BaseException) -> None:
        self.responses = list(responses)
        self.requests: list[dict[str, Any]] = []

    def __call__(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> Response:
        self.requests.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "body": body,
                "timeout": timeout,
            }
        )
        item = self.responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


def ok(data: Any, status: int = 200, headers: Mapping[str, str] | None = None) -> Response:
    return status, dict(headers or {}), json.dumps({"data": data}).encode()


def err(
    status: int,
    error_id: str = "x",
    name: str = "some_error",
    detail: str = "Something failed",
    headers: Mapping[str, str] | None = None,
) -> Response:
    body = {"error": {"id": error_id, "name": name, "detail": detail}}
    return status, dict(headers or {}), json.dumps(body).encode()


def scripted_client(transport: ScriptedTransport, **kwargs: Any) -> tuple[YnabClient, list[float]]:
    slept: list[float] = []
    kwargs.setdefault("sleep", slept.append)
    kwargs.setdefault("jitter", lambda: 1.0)
    return YnabClient(SECRET, transport=transport, **kwargs), slept


# ── construction ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("token", ["", "   ", None])
def test_empty_token_rejected(token: Any) -> None:
    with pytest.raises(ConfigError):
        YnabClient(token)


@pytest.mark.parametrize("token", ["abc def", "abc\r\nX-Evil: 1", "tök", "abc\x00"])
def test_token_with_invalid_characters_rejected_without_echo(token: str) -> None:
    with pytest.raises(ConfigError) as info:
        YnabClient(token)
    assert token not in str(info.value)
    assert token not in info.value.user_message


def test_token_surrounding_whitespace_is_stripped() -> None:
    transport = ScriptedTransport(ok({"user": {"id": "u"}}))
    YnabClient(f"  {SECRET}\n", transport=transport).get_user()
    assert transport.requests[0]["headers"]["Authorization"] == f"Bearer {SECRET}"


@pytest.mark.parametrize("url", ["ftp://api.ynab.com/v1", "api.ynab.com/v1", "https://"])
def test_bad_base_url_rejected(url: str) -> None:
    with pytest.raises(ConfigError):
        YnabClient(SECRET, base_url=url)


def test_repr_hides_token() -> None:
    client = YnabClient(SECRET, transport=ScriptedTransport())
    assert SECRET not in repr(client)
    assert SECRET not in str(client)


# ── request shape ───────────────────────────────────────────────────────────


def test_request_headers_url_and_timeout() -> None:
    transport = ScriptedTransport(ok({"user": {"id": "u1"}}))
    client, _ = scripted_client(transport, timeout=12.5)
    assert client.get_user() == {"id": "u1"}
    req = transport.requests[0]
    assert req["method"] == "GET"
    assert req["url"] == "https://api.ynab.com/v1/user"
    assert req["timeout"] == 12.5
    assert req["body"] is None
    assert req["headers"]["Authorization"] == f"Bearer {SECRET}"
    assert req["headers"]["User-Agent"] == f"ynab-helper/{__version__}"
    assert req["headers"]["Accept"] == "application/json"
    assert "Content-Type" not in req["headers"]


def test_json_body_is_utf8_with_content_type(fake: FakeYnab) -> None:
    client = fake.client()
    client.create_transactions(
        BUDGET, [new_tx(import_id="YNAB:-1000:2026-10-01:1", memo="Café ☕")]
    )
    post = fake.requests[-1]
    assert post.headers["content-type"] == "application/json; charset=utf-8"
    assert post.body["transactions"][0]["memo"] == "Café ☕"
    assert fake.transactions(BUDGET)[0]["memo"] == "Café ☕"


def test_path_segments_are_quoted(fake: FakeYnab) -> None:
    client = fake.client()
    # A crafted id must not be able to address another resource.
    with pytest.raises(YnabNotFound):
        client.delete_transaction(BUDGET, "../accounts")
    assert fake.calls[-1].path == f"/budgets/{BUDGET}/transactions/..%2Faccounts"
    assert fake.calls[-1].method == "DELETE"


def test_empty_ids_rejected_before_any_request(fake: FakeYnab) -> None:
    client = fake.client()
    with pytest.raises(ValueError):
        client.get_account(BUDGET, "")
    with pytest.raises(ValueError):
        client.update_transaction("", "x", {})
    assert fake.calls == []


# ── status → error mapping ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("status", "cls"),
    [
        (400, YnabValidationError),
        (401, YnabAuthError),
        (403, YnabAuthError),
        (404, YnabNotFound),
        (409, YnabValidationError),
        (422, YnabValidationError),
        (429, YnabRateLimited),
        (500, YnabServerError),
        (502, YnabServerError),
        (503, YnabServerError),
        (418, YnabError),
        (302, YnabError),
    ],
)
def test_status_mapping(status: int, cls: type[YnabError]) -> None:
    transport = ScriptedTransport(*[err(status, "id-1", "name_x", "detail text")] * 3)
    client, _ = scripted_client(transport)
    with pytest.raises(cls) as info:
        client.get_budgets()
    exc = info.value
    assert type(exc) is cls
    assert isinstance(exc, ExternalServiceError)
    assert exc.service == "YNAB"
    assert exc.status == status
    assert exc.error_id == "id-1"
    assert exc.detail == "detail text"
    assert "HTTP" in str(exc) and str(status) in str(exc)
    assert exc.user_message
    assert exc.retriable is (status in (429, 500, 502, 503))


def test_validation_error_user_message_carries_ynab_detail() -> None:
    transport = ScriptedTransport(err(400, "400", "bad_request", "date must not be in the future"))
    client, _ = scripted_client(transport)
    with pytest.raises(YnabValidationError) as info:
        client.get_budgets()
    assert "date must not be in the future" in info.value.user_message


def test_error_with_non_json_body() -> None:
    transport = ScriptedTransport((404, {}, b"<html>nope</html>"))
    client, _ = scripted_client(transport)
    with pytest.raises(YnabNotFound) as info:
        client.get_budgets()
    assert info.value.error_id is None
    assert info.value.detail is None
    assert "<html>" not in info.value.user_message


@pytest.mark.parametrize(
    "response",
    [
        (200, {}, b"<html>gateway</html>"),
        (200, {}, b""),
        (200, {}, b"\xff\xfe"),
        (200, {}, b'{"nodata": 1}'),
        (200, {}, b'{"data": []}'),
        (200, {}, b"[1, 2]"),
    ],
)
def test_unparseable_success_is_typed_error_not_retried(response: Response) -> None:
    transport = ScriptedTransport(response)
    client, slept = scripted_client(transport)
    with pytest.raises(YnabError) as info:
        client.get_budgets()
    assert type(info.value) is YnabError
    assert len(transport.requests) == 1
    assert slept == []


def test_missing_payload_key_is_typed_error() -> None:
    transport = ScriptedTransport(ok({"something": []}))
    client, _ = scripted_client(transport)
    with pytest.raises(YnabError, match="budgets"):
        client.get_budgets()


def test_malformed_transaction_in_response_is_typed_error() -> None:
    bad = {"id": "t", "date": "2026-01-01", "amount": 1.5, "account_id": "a", "cleared": "cleared"}
    transport = ScriptedTransport(ok({"transactions": [bad], "server_knowledge": 1}))
    client, _ = scripted_client(transport)
    with pytest.raises(YnabError, match="malformed"):
        client.get_transactions(BUDGET)


def test_missing_server_knowledge_is_typed_error() -> None:
    transport = ScriptedTransport(ok({"transactions": []}))
    client, _ = scripted_client(transport)
    with pytest.raises(YnabError, match="server_knowledge"):
        client.get_transactions(BUDGET)


# ── retries ─────────────────────────────────────────────────────────────────


def test_5xx_retried_then_success() -> None:
    transport = ScriptedTransport(err(503), err(500), ok({"budgets": []}))
    client, slept = scripted_client(transport)
    assert client.get_budgets() == []
    assert len(transport.requests) == 3
    assert slept == [1.0, 2.0]  # exponential with jitter fixed at its maximum


def test_5xx_retries_exhausted_raises_server_error() -> None:
    transport = ScriptedTransport(err(500), err(502), err(503))
    client, slept = scripted_client(transport)
    with pytest.raises(YnabServerError) as info:
        client.get_budgets()
    assert info.value.status == 503
    assert len(transport.requests) == 3  # 1 + max_retries(2)
    assert len(slept) == 2


def test_max_retries_zero_disables_retries() -> None:
    transport = ScriptedTransport(err(500))
    client, slept = scripted_client(transport, max_retries=0)
    with pytest.raises(YnabServerError):
        client.get_budgets()
    assert len(transport.requests) == 1
    assert slept == []


@pytest.mark.parametrize(("jitter", "expected"), [(0.0, [0.5, 1.0]), (0.5, [0.75, 1.5])])
def test_backoff_is_jittered_exponential(jitter: float, expected: list[float]) -> None:
    transport = ScriptedTransport(err(500), err(500), ok({"budgets": []}))
    client, slept = scripted_client(transport, jitter=lambda: jitter)
    client.get_budgets()
    assert slept == expected


def test_backoff_is_capped() -> None:
    transport = ScriptedTransport(err(500), err(500), ok({"budgets": []}))
    client, slept = scripted_client(transport, backoff_base=60.0)
    client.get_budgets()
    assert slept == [10.0, 10.0]


def test_retry_after_honoured_when_short() -> None:
    transport = ScriptedTransport(err(503, headers={"Retry-After": "3"}), ok({"budgets": []}))
    client, slept = scripted_client(transport)
    client.get_budgets()
    assert slept == [3.0]


def test_retry_after_http_date_honoured() -> None:
    when = format_datetime(datetime.now(UTC) + timedelta(seconds=5), usegmt=True)
    transport = ScriptedTransport(err(503, headers={"retry-after": when}), ok({"budgets": []}))
    client, slept = scripted_client(transport)
    client.get_budgets()
    assert len(slept) == 1
    assert 0 <= slept[0] <= 5.0


def test_long_retry_after_gives_up_instead_of_waiting() -> None:
    transport = ScriptedTransport(err(503, headers={"Retry-After": "120"}), ok({"budgets": []}))
    client, slept = scripted_client(transport)
    with pytest.raises(YnabServerError):
        client.get_budgets()
    assert len(transport.requests) == 1
    assert slept == []


def test_retry_after_of_exactly_10s_is_honoured() -> None:
    transport = ScriptedTransport(err(503, headers={"Retry-After": "10"}), ok({"budgets": []}))
    client, slept = scripted_client(transport)
    assert client.get_budgets() == []
    assert slept == [10.0]


def test_retry_after_just_above_10s_gives_up() -> None:
    transport = ScriptedTransport(err(503, headers={"Retry-After": "10.5"}), ok({"budgets": []}))
    client, slept = scripted_client(transport)
    with pytest.raises(YnabServerError):
        client.get_budgets()
    assert slept == []
    assert len(transport.requests) == 1


def test_requests_sent_counts_every_attempt() -> None:
    transport = ScriptedTransport(
        TimeoutError("timed out"), err(500), ok({"budgets": []}), err(404)
    )
    client, _ = scripted_client(transport)
    assert client.requests_sent == 0
    client.get_budgets()
    assert client.requests_sent == 3  # the network error and the 5xx count too
    with pytest.raises(YnabNotFound):
        client.get_budgets()
    assert client.requests_sent == 4


def test_payload_errors_send_nothing_and_count_nothing(fake: FakeYnab) -> None:
    client = fake.client()
    with pytest.raises(ValueError):
        client.create_transactions(BUDGET, [new_tx(amount=1.5)])
    assert client.requests_sent == 0


@pytest.mark.parametrize(
    "exc",
    [
        TimeoutError("timed out"),
        ConnectionResetError("reset"),
        urllib.error.URLError("dns failure"),
        http.client.RemoteDisconnected("closed"),
        http.client.IncompleteRead(b"partial"),
        OSError("unreachable"),
    ],
)
def test_network_errors_are_retried_and_typed(exc: BaseException) -> None:
    transport = ScriptedTransport(exc, exc, exc)
    client, slept = scripted_client(transport)
    with pytest.raises(YnabNetworkError) as info:
        client.get_budgets()
    assert len(transport.requests) == 3
    assert len(slept) == 2
    assert info.value.retriable is True
    assert info.value.status is None
    assert type(exc).__name__ in str(info.value)
    # The transport exception is neither chained nor attached as context.
    assert info.value.__cause__ is None
    assert info.value.__context__ is None


def test_network_error_then_success() -> None:
    transport = ScriptedTransport(TimeoutError("timed out"), ok({"budgets": [{"id": "b"}]}))
    client, slept = scripted_client(transport)
    assert client.get_budgets() == [{"id": "b"}]
    assert slept == [1.0]


@pytest.mark.parametrize(
    "exc",
    [
        ssl.SSLCertVerificationError("certificate verify failed: self-signed certificate"),
        urllib.error.URLError(ssl.SSLCertVerificationError("certificate verify failed")),
        ResponseTooLarge("response body exceeds 67108864 bytes"),
        http.client.InvalidURL("nonnumeric port"),
    ],
)
def test_permanent_network_errors_are_not_retried(exc: BaseException) -> None:
    # A failing certificate or an oversized body fails identically on every attempt; a
    # retry only delays the error (and re-downloads up to 64 MB).
    transport = ScriptedTransport(exc, ok({"budgets": []}))
    client, slept = scripted_client(transport)
    with pytest.raises(YnabNetworkError) as info:
        client.get_budgets()
    assert info.value.retriable is False
    assert len(transport.requests) == 1
    assert slept == []
    assert "network problem" not in info.value.user_message


def test_certificate_error_user_message_names_the_cause() -> None:
    exc = urllib.error.URLError(ssl.SSLCertVerificationError("certificate verify failed"))
    client, _ = scripted_client(ScriptedTransport(exc))
    with pytest.raises(YnabNetworkError) as info:
        client.get_budgets()
    assert "certificate" in info.value.user_message


def test_non_certificate_ssl_error_is_retried() -> None:
    # e.g. a connection cut during the handshake: transient.
    exc = ssl.SSLError("UNEXPECTED_EOF_WHILE_READING")
    transport = ScriptedTransport(exc, ok({"budgets": []}))
    client, slept = scripted_client(transport)
    assert client.get_budgets() == []
    assert slept == [1.0]


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422])
def test_4xx_never_retried(status: int) -> None:
    transport = ScriptedTransport(err(status))
    client, slept = scripted_client(transport)
    with pytest.raises(YnabError):
        client.get_budgets()
    assert len(transport.requests) == 1
    assert slept == []


def test_programming_errors_from_transport_propagate() -> None:
    transport = ScriptedTransport(KeyError("bug"))
    client, _ = scripted_client(transport)
    with pytest.raises(KeyError):
        client.get_budgets()


def test_post_without_import_ids_is_not_retried() -> None:
    # Replaying a non-deduplicable create after an ambiguous failure could duplicate money.
    transport = ScriptedTransport(err(500), ok({"transactions": []}, status=201))
    client, slept = scripted_client(transport)
    with pytest.raises(YnabServerError):
        client.create_transactions(BUDGET, [new_tx()])
    assert len(transport.requests) == 1
    assert slept == []


def test_post_with_import_ids_is_retried(fake: FakeYnab) -> None:
    fake.fail_next("POST", r"/budgets/[^/]+/transactions", 503)
    result = fake.client().create_transactions(
        BUDGET, [new_tx(import_id="YNAB:-1000:2026-10-01:1")]
    )
    assert len(result.created) == 1
    assert len(fake.calls_matching("POST")) == 2


def test_lost_post_response_is_retried_without_duplicate(fake: FakeYnab) -> None:
    """YNAB applied the POST but the response was lost: the retry must not double-create."""
    fake.raise_network_error_next("POST", after_apply=True)
    iid = "YNAB:-1000:2026-10-01:1"
    result = fake.client().create_transactions(BUDGET, [new_tx(import_id=iid)])
    assert result.created == []
    assert result.duplicate_import_ids == [iid]
    assert len(fake.transactions(BUDGET)) == 1


def test_put_patch_delete_are_retried(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", cleared="uncleared")
    client = fake.client()
    fake.fail_next("PUT", None, 500)
    assert client.update_transaction(BUDGET, tx_id, {"memo": "x"}).memo == "x"
    fake.fail_next("PATCH", None, 502)
    assert client.update_transactions(BUDGET, [{"id": tx_id, "memo": "y"}])[0].memo == "y"
    fake.raise_network_error_next("DELETE")
    assert client.delete_transaction(BUDGET, tx_id).deleted is True
    assert len(fake.slept) == 3


def test_delete_retry_after_lost_response_reports_not_found(fake: FakeYnab) -> None:
    # Callers treat 404 on DELETE as "already removed"; the error must stay typed.
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01")
    fake.raise_network_error_next("DELETE", after_apply=True)
    with pytest.raises(YnabNotFound):
        fake.client().delete_transaction(BUDGET, tx_id)
    assert fake.tx(tx_id)["deleted"] is True


# ── 429 / rate limit ────────────────────────────────────────────────────────


def test_F17_429_is_never_retried_and_typed() -> None:
    transport = ScriptedTransport(
        err(429, "429", "too_many_requests", "Too many requests", {"Retry-After": "1800"})
    )
    client, slept = scripted_client(transport)
    with pytest.raises(YnabRateLimited) as info:
        client.get_budgets()
    assert info.value.retry_after == 1800.0
    assert info.value.status == 429
    assert info.value.retriable is True
    assert "rate limit" in info.value.user_message.lower()
    assert len(transport.requests) == 1
    assert slept == []


def test_429_without_retry_after(fake: FakeYnab) -> None:
    fake.fail_next(None, None, 429)
    client = fake.client()
    with pytest.raises(YnabRateLimited) as info:
        client.create_transactions(BUDGET, [new_tx(import_id="YNAB:1:2026-10-01:1")])
    assert info.value.retry_after is None
    assert fake.transactions(BUDGET) == []
    assert fake.slept == []


def test_F17_quota_exhaustion_mid_import_stops_cleanly(fake: FakeYnab) -> None:
    """300 rows against a 2-request quota: typed error, nothing half-recorded, no hammering."""
    fake.quota = 2
    client = fake.client()
    txs = [
        new_tx(amount=-(1000 + i), import_id=f"YNAB:-{1000 + i}:2026-10-01:1") for i in range(300)
    ]
    seen = []
    with pytest.raises(YnabRateLimited):
        for chunk in client.iter_create_transactions(BUDGET, txs):
            seen.append(chunk)
    assert len(seen) == 2
    assert len(fake.transactions(BUDGET)) == 200
    assert len(fake.calls) == 3  # no retries after the 429


def test_F17_transport_errors_never_escape_raw() -> None:
    """v1 leaked RemoteDisconnected / JSONDecodeError / TimeoutError past its error contract."""
    transport = ScriptedTransport(
        http.client.RemoteDisconnected("Remote end closed connection without response"),
        TimeoutError("read timed out"),
        (200, {}, b"<html>gateway</html>"),
    )
    client, _ = scripted_client(transport)
    with pytest.raises(YnabError):
        client.get_budgets()
    transport.responses = [(200, {}, b"<html>gateway</html>")]
    with pytest.raises(YnabError):
        client.get_budgets()


def test_F17_bulk_patch_instead_of_one_request_per_row(fake: FakeYnab) -> None:
    ids = [
        fake.add_transaction(
            BUDGET, ACCOUNT, amount=-i, date="2026-10-01", cleared="uncleared", memo="(pending)"
        )
        for i in range(1, 301)
    ]
    fake.reset_log()
    updated = fake.client().update_transactions(
        BUDGET, [{"id": i, "cleared": "cleared", "memo": None} for i in ids]
    )
    assert len(updated) == 300
    assert [len(c.body["transactions"]) for c in fake.calls] == [50] * 6
    assert {c.method for c in fake.calls} == {"PATCH"}
    assert all(t["cleared"] == "cleared" for t in fake.transactions(BUDGET))


def test_rate_limit_header_parsed(fake: FakeYnab) -> None:
    client = fake.client()
    assert client.rate_limit_used is None
    client.get_budgets()
    client.get_user()
    assert (client.rate_limit_used, client.rate_limit_total) == (2, 200)


def test_rate_limit_header_parsed_from_error_responses_too() -> None:
    transport = ScriptedTransport(err(404, headers={"x-rate-limit": "199/200"}))
    client, _ = scripted_client(transport)
    with pytest.raises(YnabNotFound):
        client.get_budgets()
    assert client.rate_limit_used == 199


def test_malformed_rate_limit_header_ignored() -> None:
    transport = ScriptedTransport(
        ok({"budgets": []}, headers={"X-Rate-Limit": "36/200"}),
        ok({"budgets": []}, headers={"X-Rate-Limit": "garbage"}),
    )
    client, _ = scripted_client(transport)
    client.get_budgets()
    client.get_budgets()
    assert client.rate_limit_used == 36


def test_rate_limit_near_exhaustion_logged(caplog: pytest.LogCaptureFixture) -> None:
    transport = ScriptedTransport(ok({"budgets": []}, headers={"X-Rate-Limit": "190/200"}))
    client, _ = scripted_client(transport)
    with caplog.at_level(logging.WARNING, logger="ynab_helper.ynab.client"):
        client.get_budgets()
    assert "190/200" in caplog.text


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("36/200", (36, 200)),
        (" 1/200 ", (1, 200)),
        ("36", None),
        ("a/b", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_rate_limit(value: str | None, expected: tuple[int, int] | None) -> None:
    assert _parse_rate_limit(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("5", 5.0),
        ("0.5", 0.5),
        ("-3", 0.0),
        ("", None),
        (None, None),
        ("soon", None),
        ("nan", None),
    ],
)
def test_retry_after_parser_contract(value: str | None, expected: float | None) -> None:
    # The client delegates to net.parse_retry_after; these are the semantics it relies on.
    assert parse_retry_after(value) == expected


def test_retry_after_parser_http_date() -> None:
    now = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
    header = format_datetime(now + timedelta(seconds=30), usegmt=True)
    assert parse_retry_after(header, now=now) == 30.0
    past = format_datetime(now - timedelta(seconds=30), usegmt=True)
    assert parse_retry_after(past, now=now) == 0.0


# ── secrecy ─────────────────────────────────────────────────────────────────


def _assert_secret_free(exc: BaseException, log_text: str) -> None:
    texts = [str(exc), repr(exc), getattr(exc, "user_message", ""), str(getattr(exc, "detail", ""))]
    for text in texts:
        assert SECRET not in text
    assert exc.__cause__ is None or SECRET not in str(exc.__cause__)
    assert exc.__context__ is None or SECRET not in str(exc.__context__)
    assert SECRET not in log_text


@pytest.mark.parametrize(
    "responses",
    [
        [err(401, "401", "unauthorized", f"token {SECRET} is invalid")],
        [err(400, "400", "bad_request", f"Bearer {SECRET}")],
        [err(500, detail=SECRET)] * 3,
        [OSError(f"proxy said Authorization: Bearer {SECRET}")] * 3,
        [urllib.error.URLError(f"https://x/?t={SECRET}")] * 3,
        [err(429, detail=SECRET)],
    ],
)
def test_token_never_in_errors_or_logs(
    responses: list[Response | BaseException], caplog: pytest.LogCaptureFixture
) -> None:
    transport = ScriptedTransport(*responses)
    client, _ = scripted_client(transport)
    with caplog.at_level(logging.DEBUG), pytest.raises(YnabError) as info:
        client.get_budgets()
    _assert_secret_free(info.value, caplog.text)


def test_token_not_logged_on_success_path(caplog: pytest.LogCaptureFixture, fake: FakeYnab) -> None:
    fake.token = SECRET
    client = fake.client()
    with caplog.at_level(logging.DEBUG):
        client.get_budgets()
        client.get_transactions(BUDGET)
    assert SECRET not in caplog.text
    assert "GET /budgets" in caplog.text


# ── endpoints against the fake ──────────────────────────────────────────────


def test_get_user_and_budgets(fake: FakeYnab) -> None:
    client = fake.client()
    assert client.get_user()["id"] == fake.user_id
    budgets = client.get_budgets()
    assert [b["id"] for b in budgets] == [BUDGET]
    assert budgets[0]["currency_format"]["iso_code"] == "CHF"


def test_get_budget_currency(fake: FakeYnab) -> None:
    fake.add_budget("eur", currency="EUR")
    fake.add_budget("none", currency=None)
    client = fake.client()
    assert client.get_budget_currency(BUDGET) == "CHF"
    assert client.get_budget_currency("eur") == "EUR"
    assert client.get_budget_currency("none") is None
    assert fake.calls[-1].path == "/budgets/none/settings"
    with pytest.raises(YnabNotFound):
        client.get_budget_currency("missing")


def test_wrong_token_is_auth_error(fake: FakeYnab) -> None:
    client = fake.client(token="other-token")
    with pytest.raises(YnabAuthError):
        client.get_user()


def test_get_accounts_full_and_delta(fake: FakeYnab) -> None:
    client = fake.client()
    accounts, knowledge = client.get_accounts(BUDGET)
    assert {a.id for a in accounts} == {ACCOUNT, OTHER_ACCOUNT}
    assert knowledge == fake.server_knowledge(BUDGET)
    fake.add_transaction(BUDGET, ACCOUNT, amount=-5000, date="2026-10-01")
    delta, knowledge2 = client.get_accounts(BUDGET, last_knowledge=knowledge)
    assert [a.id for a in delta] == [ACCOUNT]
    assert delta[0].balance == -5000
    assert knowledge2 > knowledge
    assert fake.calls[-1].query == {"last_knowledge_of_server": str(knowledge)}


def test_get_account(fake: FakeYnab) -> None:
    fake.add_transaction(BUDGET, ACCOUNT, amount=-5000, date="2026-10-01", cleared="cleared")
    fake.add_transaction(BUDGET, ACCOUNT, amount=-700, date="2026-10-02", cleared="uncleared")
    acc = fake.client().get_account(BUDGET, ACCOUNT)
    assert (acc.balance, acc.cleared_balance, acc.uncleared_balance) == (-5700, -5000, -700)
    with pytest.raises(YnabNotFound):
        fake.client().get_account(BUDGET, "nope")


def test_get_transactions_budget_wide_and_per_account(fake: FakeYnab) -> None:
    fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-09-01")
    fake.add_transaction(BUDGET, OTHER_ACCOUNT, amount=-2, date="2026-10-01")
    client = fake.client()
    all_txs, knowledge = client.get_transactions(BUDGET)
    assert sorted(t.amount for t in all_txs) == [-2, -1]
    assert knowledge == fake.server_knowledge(BUDGET)
    assert fake.calls[-1].path == f"/budgets/{BUDGET}/transactions"
    assert fake.calls[-1].query == {}

    acct_txs, _ = client.get_transactions(BUDGET, account_id=OTHER_ACCOUNT)
    assert [t.amount for t in acct_txs] == [-2]
    assert fake.calls[-1].path == f"/budgets/{BUDGET}/accounts/{OTHER_ACCOUNT}/transactions"

    since, _ = client.get_transactions(BUDGET, since_date=date(2026, 9, 15), last_knowledge=0)
    assert [t.amount for t in since] == [-2]
    assert fake.calls[-1].query == {"since_date": "2026-09-15", "last_knowledge_of_server": "0"}

    since_str, _ = client.get_transactions(BUDGET, since_date="2026-09-01")
    assert len(since_str) == 2


def test_get_transactions_delta_includes_deleted(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01")
    client = fake.client()
    _, knowledge = client.get_transactions(BUDGET)
    client.delete_transaction(BUDGET, tx_id)
    delta, _ = client.get_transactions(BUDGET, last_knowledge=knowledge)
    assert [(t.id, t.deleted) for t in delta] == [(tx_id, True)]
    full, _ = client.get_transactions(BUDGET)
    assert full == []


def test_get_transactions_since_datetime_uses_its_calendar_date(fake: FakeYnab) -> None:
    fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-09-30")
    fake.add_transaction(BUDGET, ACCOUNT, amount=-2, date="2026-10-01")
    since, _ = fake.client().get_transactions(BUDGET, since_date=datetime(2026, 10, 1, 12, 0))
    assert fake.calls[-1].query == {"since_date": "2026-10-01"}
    # The time of day is dropped, so a transaction on that day is never skipped.
    assert [t.amount for t in since] == [-2]


@pytest.mark.parametrize("bad", ["2026-13-01", "01.10.2026", "2026-10-01T00:00:00", "", 20261001])
def test_invalid_since_date_rejected_before_any_request(fake: FakeYnab, bad: Any) -> None:
    with pytest.raises(ValueError):
        fake.client().get_transactions(BUDGET, since_date=bad)
    assert fake.calls == []


def test_create_transactions_returns_created_and_duplicates(fake: FakeYnab) -> None:
    client = fake.client()
    first = client.create_transactions(BUDGET, [new_tx(import_id="YNAB:-1000:2026-10-01:1")])
    assert [t.import_id for t in first.created] == ["YNAB:-1000:2026-10-01:1"]
    assert all(isinstance(t, YnabTransaction) for t in first.created)
    assert first.duplicate_import_ids == []
    assert first.server_knowledge == fake.server_knowledge(BUDGET)

    again = client.create_transactions(
        BUDGET,
        [new_tx(import_id="YNAB:-1000:2026-10-01:1"), new_tx(import_id="YNAB:-1000:2026-10-01:2")],
    )
    assert [t.import_id for t in again.created] == ["YNAB:-1000:2026-10-01:2"]
    assert again.duplicate_import_ids == ["YNAB:-1000:2026-10-01:1"]


def test_create_transactions_empty_makes_no_request(fake: FakeYnab) -> None:
    result = fake.client().create_transactions(BUDGET, [])
    assert result.created == [] and result.duplicate_import_ids == []
    assert fake.calls == []


def _gen(items: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    yield from items


def test_create_transactions_accepts_a_generator(fake: FakeYnab) -> None:
    """Regression: validation used to consume a generator, so nothing was sent, silently."""
    rows = [new_tx(amount=-i, import_id=f"YNAB:-{i}:2026-10-01:1") for i in range(1, 151)]
    result = fake.client().create_transactions(BUDGET, _gen(rows))
    assert len(result.created) == 150
    assert [len(c.body["transactions"]) for c in fake.calls_matching("POST")] == [100, 50]
    assert len(fake.transactions(BUDGET)) == 150


def test_update_transactions_accepts_a_generator(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", cleared="uncleared")
    out = fake.client().update_transactions(BUDGET, _gen([{"id": tx_id, "cleared": "cleared"}]))
    assert [t.cleared for t in out] == ["cleared"]
    assert fake.tx(tx_id)["cleared"] == "cleared"


def test_iter_methods_validate_eagerly(fake: FakeYnab) -> None:
    client = fake.client()
    # The error surfaces at the call, not at the first next(), and nothing is sent.
    with pytest.raises(ValueError):
        client.iter_create_transactions(BUDGET, [new_tx(import_id="ok"), new_tx(amount=1.0)])
    with pytest.raises(ValueError):
        client.iter_update_transactions(BUDGET, [{"memo": "no id"}])
    assert fake.calls == []


@pytest.mark.parametrize("bad", [new_tx(), "not a list", b"bytes", [new_tx(), "row"], [None]])
def test_non_sequence_payloads_rejected(fake: FakeYnab, bad: Any) -> None:
    client = fake.client()
    with pytest.raises(TypeError):
        client.create_transactions(BUDGET, bad)
    with pytest.raises(TypeError):
        client.update_transactions(BUDGET, bad)
    assert fake.calls == []


def test_create_transactions_chunked_by_100(fake: FakeYnab) -> None:
    txs = [new_tx(amount=-i, import_id=f"YNAB:-{i}:2026-10-01:1") for i in range(1, 251)]
    txs[120]["import_id"] = txs[10]["import_id"]  # duplicate across chunks
    result = fake.client().create_transactions(BUDGET, txs)
    posts = fake.calls_matching("POST")
    assert [len(c.body["transactions"]) for c in posts] == [100, 100, 50]
    assert len(result.created) == 249
    assert result.duplicate_import_ids == [txs[10]["import_id"]]
    assert result.server_knowledge == fake.server_knowledge(BUDGET)


def test_iter_create_yields_per_chunk_and_keeps_earlier_chunks_on_failure(fake: FakeYnab) -> None:
    txs = [new_tx(amount=-i, import_id=f"YNAB:-{i}:2026-10-01:1") for i in range(1, 151)]
    client = fake.client()
    chunks = client.iter_create_transactions(BUDGET, txs)
    first = next(chunks)
    assert len(first.created) == 100
    fake.fail_next("POST", None, 400)
    with pytest.raises(YnabValidationError):
        next(chunks)
    assert len(fake.transactions(BUDGET)) == 100


@pytest.mark.parametrize(
    "bad",
    [
        {"amount": 10.0},
        {"amount": True},
        {"amount": "10"},
        {"import_id": "YNAB:" + "9" * 40},
        {"import_id": 5},
        {"import_id": ""},
        {"account_id": None},
        {"date": ""},
        {"subtransactions": [{"amount": -500.0}, {"amount": -500}]},
        {"amount": math.nan},
    ],
)
def test_create_payload_validated_before_sending(fake: FakeYnab, bad: dict[str, Any]) -> None:
    tx = new_tx(import_id="YNAB:-1:2026-10-01:1")
    tx.update(bad)
    with pytest.raises(ValueError):
        fake.client().create_transactions(BUDGET, [new_tx(import_id="ok"), tx])
    assert fake.calls == []


def test_import_id_of_exactly_36_chars_accepted(fake: FakeYnab) -> None:
    iid = "YNAB:-1234567890:2026-10-01:123456"
    iid = iid + "x" * (36 - len(iid))
    assert len(iid) == 36
    result = fake.client().create_transactions(BUDGET, [new_tx(import_id=iid)])
    assert result.created[0].import_id == iid


def test_update_transactions_bulk_chunked_by_50(fake: FakeYnab) -> None:
    ids = [
        fake.add_transaction(BUDGET, ACCOUNT, amount=-i, date="2026-10-01") for i in range(1, 121)
    ]
    fake.reset_log()
    out = fake.client().update_transactions(BUDGET, [{"id": i, "memo": "m"} for i in ids])
    assert [len(c.body["transactions"]) for c in fake.calls] == [50, 50, 20]
    assert all(
        c.method == "PATCH" and c.path == f"/budgets/{BUDGET}/transactions" for c in fake.calls
    )
    assert [t.id for t in out] == ids
    assert all(t.memo == "m" for t in out)


def test_update_transactions_requires_ids(fake: FakeYnab) -> None:
    with pytest.raises(ValueError):
        fake.client().update_transactions(
            BUDGET, [{"import_id": "YNAB:1:2026-01-01:1", "memo": "x"}]
        )
    assert fake.client().update_transactions(BUDGET, []) == []
    assert fake.calls == []


def test_update_transactions_rejects_duplicate_ids(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01")
    updates = [{"id": tx_id, "memo": "a"}] + [{"id": "x" + str(i)} for i in range(60)]
    updates.append({"id": tx_id, "memo": "b"})  # same id in a later chunk
    with pytest.raises(ValueError, match="twice"):
        fake.client().update_transactions(BUDGET, updates)
    assert fake.calls == []


@pytest.mark.parametrize(
    "fields",
    [
        {"amount": math.nan},
        {"amount": math.inf},
        {"amount": 10.0},
        {"amount": True},
        {"amount": "10"},
        {"amount": None},
        {"import_id": "YNAB:" + "9" * 40},
        {"import_id": ""},
        {"subtransactions": [{"amount": 1.5}]},
        {"subtransactions": []},
        {"memo": math.nan},  # caught by the strict JSON encoder
        {"date": date(2026, 10, 1)},  # not JSON-serialisable
    ],
)
def test_update_payloads_validated_before_sending(fake: FakeYnab, fields: dict[str, Any]) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01")
    client = fake.client()
    with pytest.raises((ValueError, TypeError)):
        client.update_transaction(BUDGET, tx_id, fields)
    with pytest.raises((ValueError, TypeError)):
        client.update_transactions(BUDGET, [{"id": tx_id, **fields}])
    assert fake.calls == []
    assert fake.tx(tx_id)["amount"] == -1


def test_non_json_value_in_a_later_chunk_sends_nothing(fake: FakeYnab) -> None:
    rows = [new_tx(amount=-i, import_id=f"YNAB:-{i}:2026-10-01:1") for i in range(1, 151)]
    rows[140]["memo"] = math.nan  # in the second chunk
    with pytest.raises(ValueError):
        fake.client().create_transactions(BUDGET, rows)
    assert fake.calls == []


def test_iter_update_keeps_earlier_chunks_when_a_later_one_fails(fake: FakeYnab) -> None:
    ids = [
        fake.add_transaction(BUDGET, ACCOUNT, amount=-i, date="2026-10-01", cleared="uncleared")
        for i in range(1, 76)
    ]
    client = fake.client()
    chunks = client.iter_update_transactions(BUDGET, [{"id": i, "cleared": "cleared"} for i in ids])
    first = next(chunks)
    assert len(first) == 50
    fake.fail_next("PATCH", None, 400)
    with pytest.raises(YnabValidationError):
        next(chunks)
    cleared = [fake.tx(i)["cleared"] for i in ids]
    assert cleared == ["cleared"] * 50 + ["uncleared"] * 25


def test_update_transactions_bulk_404_is_typed(fake: FakeYnab) -> None:
    good = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", memo="old")
    with pytest.raises(YnabNotFound):
        fake.client().update_transactions(BUDGET, [{"id": good, "memo": "new"}, {"id": "gone"}])
    assert fake.tx(good)["memo"] == "old"  # atomic: nothing applied


def test_update_transaction_uses_put_with_transaction_wrapper(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", cleared="uncleared")
    updated = fake.client().update_transaction(BUDGET, tx_id, {"cleared": "cleared"})
    call = fake.calls[-1]
    assert (call.method, call.path) == ("PUT", f"/budgets/{BUDGET}/transactions/{tx_id}")
    assert call.body == {"transaction": {"cleared": "cleared"}}
    assert updated.cleared == "cleared"
    assert updated.amount == -1


def test_update_transaction_on_deleted_is_not_found(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", deleted=True)
    with pytest.raises(YnabNotFound):
        fake.client().update_transaction(BUDGET, tx_id, {"memo": "x"})


def test_delete_transaction(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01")
    deleted = fake.client().delete_transaction(BUDGET, tx_id)
    assert deleted.id == tx_id and deleted.deleted is True
    assert fake.calls[-1][:2] == ("DELETE", f"/budgets/{BUDGET}/transactions/{tx_id}")
    with pytest.raises(YnabNotFound):
        fake.client().delete_transaction(BUDGET, tx_id)
