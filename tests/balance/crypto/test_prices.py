"""Tests for ynab_helper.balance.crypto.prices (DESIGN §6.6; findings F05, F42)."""

from __future__ import annotations

import functools
import urllib.parse
from decimal import Decimal

import pytest

from tests.net.fake_opener import FakeOpener, FakeResponse, SleepRecorder, http_error
from ynab_helper import net
from ynab_helper.balance.crypto import prices
from ynab_helper.errors import ExternalServiceError


class FakeRequest:
    def __init__(self, reply) -> None:
        self.reply = reply
        self.calls: list[tuple[tuple, dict]] = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if isinstance(self.reply, BaseException):
            raise self.reply
        return self.reply

    def query(self) -> dict[str, list[str]]:
        (_method, url), _ = self.calls[0]
        return urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)


REPLY = {
    "bitcoin": {"chf": Decimal("51234.5")},
    "ethereum": {"chf": 2000},
    "tether": {"chf": Decimal("0.8012")},
    "usd-coin": {"chf": Decimal("0.80")},
    "aave": {"chf": Decimal("120.12")},
}


def test_prices_for_every_symbol_as_decimals():
    fake = FakeRequest(REPLY)
    result = prices.fetch_prices_chf(["BTC", "ETH", "USDT", "aUSDT"], request=fake)
    assert result == {
        "BTC": Decimal("51234.5"),
        "ETH": Decimal(2000),
        "USDT": Decimal("0.8012"),
        "aUSDT": Decimal("0.8012"),  # priced as tether, like v1
    }
    assert all(isinstance(v, Decimal) for v in result.values())


def test_request_shape_one_call_sorted_unique_ids_with_timeout():
    fake = FakeRequest(REPLY)
    prices.fetch_prices_chf({"USDT", "BTC", "aUSDT", "AAVE"}, request=fake)
    assert len(fake.calls) == 1
    (method, url), kwargs = fake.calls[0]
    assert method == "GET"
    assert url.startswith(prices.COINGECKO_SIMPLE_PRICE_URL + "?")
    assert fake.query() == {"ids": ["aave,bitcoin,tether"], "vs_currencies": ["chf"]}
    assert kwargs["timeout"] == 15.0
    assert kwargs["service"] == "CoinGecko"
    assert kwargs["parse_float"] is Decimal


def test_coingecko_ids_come_from_one_table():
    assert prices.COINGECKO_IDS == {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "AAVE": "aave",
        "USDC": "usd-coin",
        "USDT": "tether",
        "aUSDT": "tether",
    }


def test_no_symbols_means_no_request():
    fake = FakeRequest(REPLY)
    assert prices.fetch_prices_chf([], request=fake) == {}
    assert fake.calls == []


def test_unknown_symbol_is_rejected_before_any_request():
    fake = FakeRequest(REPLY)
    with pytest.raises(ValueError, match="DOGE"):
        prices.fetch_prices_chf(["BTC", "DOGE"], request=fake)
    assert fake.calls == []


def test_F05_empty_response_raises_instead_of_valuing_holdings_at_zero():
    """v1 skipped unpriced holdings, so an empty reply zeroed the crypto account."""
    with pytest.raises(ExternalServiceError) as info:
        prices.fetch_prices_chf(["BTC", "ETH"], request=FakeRequest({}))
    exc = info.value
    assert exc.service == "CoinGecko"
    assert "BTC" in str(exc) and "ETH" in str(exc)
    assert "nothing was posted" in exc.user_message


def test_F05_one_missing_symbol_fails_the_whole_lookup():
    reply = {k: v for k, v in REPLY.items() if k != "bitcoin"}
    with pytest.raises(ExternalServiceError, match="BTC"):
        prices.fetch_prices_chf(["BTC", "ETH"], request=FakeRequest(reply))


@pytest.mark.parametrize(
    "value",
    [
        0,
        Decimal(0),
        -1,
        Decimal("-0.01"),
        Decimal("NaN"),
        Decimal("Infinity"),
        float("nan"),
        float("inf"),
        None,
        "51234.5",
        True,
        [1],
    ],
    ids=repr,
)
def test_F05_non_positive_or_non_finite_or_non_numeric_prices_are_rejected(value):
    reply = dict(REPLY, bitcoin={"chf": value})
    with pytest.raises(ExternalServiceError, match="BTC"):
        prices.fetch_prices_chf(["BTC"], request=FakeRequest(reply))


@pytest.mark.parametrize(
    "reply",
    [None, [], "oops", {"bitcoin": None}, {"bitcoin": {"usd": 1}}, {"bitcoin": []}],
    ids=repr,
)
def test_F05_malformed_replies_are_rejected(reply):
    with pytest.raises(ExternalServiceError):
        prices.fetch_prices_chf(["BTC"], request=FakeRequest(reply))


def test_float_prices_are_converted_via_shortest_repr():
    reply = {"bitcoin": {"chf": 0.1}}
    assert prices.fetch_prices_chf(["BTC"], request=FakeRequest(reply)) == {"BTC": Decimal("0.1")}


def test_request_errors_propagate_as_typed_errors():
    error = ExternalServiceError("HTTP 429", service="CoinGecko", status=429, retriable=True)
    with pytest.raises(ExternalServiceError, match="429"):
        prices.fetch_prices_chf(["BTC"], request=FakeRequest(error))


def test_F42_through_net_exact_decimals_and_a_timeout_on_every_call():
    body = b'{"bitcoin": {"chf": 51234.123456789012345}, "ethereum": {"chf": 2000}}'
    opener = FakeOpener(FakeResponse(body=body))
    request = functools.partial(net.request_json, opener=opener, sleep=SleepRecorder())
    result = prices.fetch_prices_chf(["BTC", "ETH"], request=request)
    assert result["BTC"] == Decimal("51234.123456789012345")
    assert opener.calls[0].timeout == 15.0


def test_F42_rate_limit_is_retried_then_raised_as_typed_error_not_exit():
    url = prices.COINGECKO_SIMPLE_PRICE_URL
    opener = FakeOpener(
        *[http_error(url, 429, '{"status": {"error_code": 429}}') for _ in range(4)]
    )
    sleep = SleepRecorder()
    request = functools.partial(net.request_json, opener=opener, sleep=sleep)
    with pytest.raises(ExternalServiceError) as info:
        prices.fetch_prices_chf(["BTC"], request=request)
    assert info.value.status == 429
    assert info.value.retriable is True
    assert len(opener.calls) == 4  # 1 + 3 retries
    assert len(sleep.calls) == 3


def test_nan_literal_from_the_wire_is_rejected():
    opener = FakeOpener(*[FakeResponse(body=b'{"bitcoin": {"chf": NaN}}') for _ in range(4)])
    request = functools.partial(net.request_json, opener=opener, sleep=SleepRecorder())
    with pytest.raises(ExternalServiceError):
        prices.fetch_prices_chf(["BTC"], request=request)
