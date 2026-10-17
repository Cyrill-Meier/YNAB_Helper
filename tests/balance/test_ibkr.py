"""Tests for ynab_helper.balance.ibkr (DESIGN §6.6; finding F80 and the
"/portfolio/accounts first" coverage gap)."""

from __future__ import annotations

import functools
import ssl
from decimal import Decimal

import pytest

from tests.net.fake_opener import FakeOpener, SleepRecorder, http_error, json_response
from ynab_helper import net
from ynab_helper.balance import ibkr
from ynab_helper.errors import ConfigError, ExternalServiceError

BASE = "https://localhost:5050"
ACCOUNTS = [{"id": "U1234567", "accountId": "U1234567", "currency": "CHF", "displayName": "Main"}]
LEDGER = {
    "USD": {"netliquidationvalue": Decimal("1000.5"), "currency": "USD"},
    "BASE": {"netliquidationvalue": Decimal("123456.78"), "currency": "BASE"},
    "CHF": {"netliquidationvalue": Decimal("5000"), "currency": "CHF"},
}


class FakeGateway:
    """Fake ``request`` callable routed by URL path."""

    def __init__(self, routes: dict[str, object]) -> None:
        self.routes = routes
        self.calls: list[tuple[str, str, dict]] = []

    def __call__(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        path = url.split("/v1/api", 1)[1]
        reply = self.routes[path]
        if isinstance(reply, BaseException):
            raise reply
        return reply

    @property
    def paths(self) -> list[str]:
        return [url.split("/v1/api", 1)[1] for _, url, _ in self.calls]


def gateway(accounts=ACCOUNTS, ledger=LEDGER, account="U1234567") -> FakeGateway:
    return FakeGateway({"/portfolio/accounts": accounts, f"/portfolio/{account}/ledger": ledger})


# ── TLS decision (F80) ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "url",
    ["https://localhost:5050", "https://127.0.0.1:5000", "https://[::1]:5050", "https://LOCALHOST"],
)
def test_tls_verification_is_off_only_for_loopback(url):
    context = ibkr.tls_context_for(url)
    assert context is not None
    assert context.verify_mode == ssl.CERT_NONE
    assert context.check_hostname is False
    assert ibkr.is_loopback_host(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://ibkr.lan:5000",
        "https://192.168.1.10:5000",
        "https://127.0.0.2:5050",
        "https://localhost.evil.example",
        "https://[::2]:5050",
    ],
)
def test_F80_tls_is_verified_for_any_other_host(url):
    context = ibkr.tls_context_for(url)
    assert context is not None
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    assert not ibkr.is_loopback_host(url)


def test_F80_client_passes_a_verifying_context_for_remote_gateways():
    fake = gateway()
    client = ibkr.IbkrClient("https://ibkr.lan:5000/", request=fake)
    assert client.verifies_tls
    client.accounts()
    _, url, kwargs = fake.calls[0]
    assert url == "https://ibkr.lan:5000/v1/api/portfolio/accounts"
    assert kwargs["ssl_context"].verify_mode == ssl.CERT_REQUIRED


def test_loopback_client_uses_the_unverified_context():
    fake = gateway()
    client = ibkr.IbkrClient(BASE, request=fake)
    assert not client.verifies_tls
    client.accounts()
    assert fake.calls[0][2]["ssl_context"].verify_mode == ssl.CERT_NONE


def test_F80_plain_http_to_a_remote_host_is_refused():
    with pytest.raises(ConfigError, match="unencrypted") as info:
        ibkr.IbkrClient("http://ibkr.lan:5000", request=gateway())
    assert "https" in info.value.user_message


def test_plain_http_to_localhost_needs_no_tls_context():
    assert ibkr.tls_context_for("http://localhost:5000") is None
    client = ibkr.IbkrClient("http://localhost:5000", request=gateway())
    assert client.verifies_tls  # nothing to verify, nothing disabled


@pytest.mark.parametrize(
    "url",
    [
        "",
        "localhost:5050",
        "ftp://localhost",
        "https://",
        "https://localhost:abc",  # reviewer probe: was retried as a network error
        "https://localhost:70000",
        "https://:5050",
    ],
)
def test_invalid_base_urls_are_config_errors(url):
    fake = gateway()
    with pytest.raises(ConfigError) as info:
        ibkr.IbkrClient(url, request=fake)
    assert info.value.user_message == "IBKR_BASE_URL must look like https://localhost:5050."
    assert fake.calls == []
    with pytest.raises(ConfigError):
        ibkr.tls_context_for(url)
    with pytest.raises(ConfigError):
        ibkr.is_loopback_host(url)


# ── accounts before ledger ───────────────────────────────────────────────────


def test_nav_calls_portfolio_accounts_first():
    fake = gateway()
    client = ibkr.IbkrClient(BASE, request=fake)
    assert client.nav_chf("U1234567") == Decimal("123456.78")
    assert fake.paths == ["/portfolio/accounts", "/portfolio/U1234567/ledger"]


def test_accounts_are_fetched_once_per_client():
    fake = gateway()
    client = ibkr.IbkrClient(BASE, request=fake)
    client.accounts()
    client.nav_chf("U1234567")
    client.nav_chf("U1234567")
    assert fake.paths == [
        "/portfolio/accounts",
        "/portfolio/U1234567/ledger",
        "/portfolio/U1234567/ledger",
    ]


def test_request_policy_and_decimal_parsing():
    fake = gateway()
    ibkr.IbkrClient(BASE, request=fake).nav_chf("U1234567")
    for method, _url, kwargs in fake.calls:
        assert method == "GET"
        assert kwargs["timeout"] == 10.0
        assert kwargs["service"] == "IBKR"
        assert kwargs["parse_float"] is Decimal


def test_accounts_parsing_and_default_account():
    fake = gateway(
        accounts=[
            {"accountId": "U1", "currency": "CHF", "accountTitle": "Joint"},
            {"id": "U2"},
            {"nothing": "useful"},
            "garbage",
        ]
    )
    client = ibkr.IbkrClient(BASE, request=fake)
    assert client.accounts() == [
        ibkr.IbkrAccount("U1", "CHF", "Joint"),
        ibkr.IbkrAccount("U2", None, None),
    ]
    assert client.default_account_id() == "U1"


@pytest.mark.parametrize("accounts", [[], [{"foo": 1}], {"accounts": []}, None])
def test_no_accounts_is_an_error_not_a_silent_zero(accounts):
    client = ibkr.IbkrClient(BASE, request=gateway(accounts=accounts))
    with pytest.raises(ExternalServiceError):
        client.default_account_id()


def test_unknown_account_is_a_config_error_and_ledger_is_not_requested():
    fake = gateway()
    client = ibkr.IbkrClient(BASE, request=fake)
    with pytest.raises(ConfigError, match="U999"):
        client.nav_chf("U999")
    assert fake.paths == ["/portfolio/accounts"]


def test_non_chf_base_currency_is_refused():
    fake = gateway(accounts=[{"accountId": "U1234567", "currency": "USD"}])
    client = ibkr.IbkrClient(BASE, request=fake)
    with pytest.raises(ConfigError, match="USD"):
        client.nav_chf("U1234567")
    assert fake.paths == ["/portfolio/accounts"]


def test_unknown_base_currency_is_accepted_like_v1():
    fake = gateway(accounts=[{"accountId": "U1234567"}])
    assert ibkr.IbkrClient(BASE, request=fake).nav_chf("U1234567") == Decimal("123456.78")


def test_account_id_is_url_quoted():
    fake = FakeGateway(
        {"/portfolio/accounts": [{"accountId": "U1/../x"}], "/portfolio/U1%2F..%2Fx/ledger": LEDGER}
    )
    assert ibkr.IbkrClient(BASE, request=fake).nav_chf("U1/../x") == Decimal("123456.78")


# ── ledger → NAV (v1 logic) ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("ledger", "expected"),
    [
        (LEDGER, Decimal("123456.78")),
        ({"BASE": {"netliquidationvalue": 0}, "CHF": {"netliquidationvalue": 5000}}, Decimal(5000)),
        ({"CHF": {"netliquidationvalue": Decimal("42.5")}}, Decimal("42.5")),
        ({"BASE": {"netliquidationvalue": 0}}, Decimal(0)),
        ({"BASE": {"netliquidationvalue": -10}}, Decimal(-10)),
        ({"BASE": {"netliquidationvalue": 1234.56}}, Decimal("1234.56")),
    ],
    ids=["base", "base-zero-chf-fallback", "chf-only", "zero", "negative", "float"],
)
def test_nav_from_ledger(ledger, expected):
    client = ibkr.IbkrClient(BASE, request=gateway(ledger=ledger))
    assert client.nav_chf("U1234567") == expected


@pytest.mark.parametrize(
    "ledger",
    [
        {},
        {"USD": {"netliquidationvalue": 100}},
        {"BASE": {"currency": "BASE"}},
        {"BASE": {"netliquidationvalue": "123"}},
        {"BASE": {"netliquidationvalue": None}},
        {"BASE": {"netliquidationvalue": True}},
        {"BASE": {"netliquidationvalue": Decimal("NaN")}},
        [],
        None,
    ],
    ids=["empty", "usd-only", "no-nlv", "string", "null", "bool", "nan", "list", "none"],
)
def test_malformed_ledger_raises_instead_of_returning_zero(ledger):
    client = ibkr.IbkrClient(BASE, request=gateway(ledger=ledger))
    with pytest.raises(ExternalServiceError) as info:
        client.nav_chf("U1234567")
    assert "nothing was posted" in info.value.user_message


# ── error messages ───────────────────────────────────────────────────────────


def test_expired_session_message():
    error = ExternalServiceError("HTTP 401", service="IBKR", status=401)
    client = ibkr.IbkrClient(BASE, request=FakeGateway({"/portfolio/accounts": error}))
    with pytest.raises(ExternalServiceError) as info:
        client.accounts()
    assert "Re-authenticate" in info.value.user_message
    assert info.value.status == 401


def test_unreachable_gateway_message_through_net():
    opener = FakeOpener(*[ConnectionRefusedError(61, "Connection refused") for _ in range(3)])
    request = functools.partial(net.request_json, opener=opener, sleep=SleepRecorder())
    client = ibkr.IbkrClient(BASE, request=request)
    with pytest.raises(ExternalServiceError) as info:
        client.accounts()
    assert "Cannot reach the IB Gateway" in info.value.user_message
    assert info.value.retriable is True
    assert info.value.__cause__ is None
    assert len(opener.calls) == 3  # 1 + 2 retries
    assert opener.calls[0].context.verify_mode == ssl.CERT_NONE


def test_end_to_end_through_net_with_fake_opener():
    url = f"{BASE}/v1/api/portfolio"
    opener = FakeOpener(
        json_response(ACCOUNTS),
        http_error(f"{url}/U1234567/ledger", 503),
        json_response({"BASE": {"netliquidationvalue": 98765.43}}),
    )
    request = functools.partial(net.request_json, opener=opener, sleep=SleepRecorder())
    nav = ibkr.IbkrClient(BASE, request=request).nav_chf("U1234567")
    assert nav == Decimal("98765.43")
    assert [c.url for c in opener.calls] == [
        f"{url}/accounts",
        f"{url}/U1234567/ledger",
        f"{url}/U1234567/ledger",
    ]
