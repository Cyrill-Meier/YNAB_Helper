"""Tests for ynab_helper.balance.crypto.eth (DESIGN §6.6; finding F79)."""

from __future__ import annotations

import functools
import json
import logging
from decimal import Decimal

import pytest

from tests.net.fake_opener import FakeOpener, SleepRecorder, http_error, json_response
from ynab_helper import net
from ynab_helper.balance.crypto import eth
from ynab_helper.errors import ConfigError, ExternalServiceError

WALLET = "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"
ANKR_KEY = "abcdef0123456789secretkey"


class FakeRpc:
    """Fake ``request`` callable: per-URL scripted replies (or exceptions)."""

    def __init__(self, replies: dict[str, list]) -> None:
        self.replies = {url: list(items) for url, items in replies.items()}
        self.calls: list[tuple[str, str, dict]] = []

    def __call__(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        queue = self.replies.get(url)
        if not queue:
            raise ExternalServiceError(f"no reply scripted for {url}", service="test")
        reply = queue.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply

    @property
    def urls(self) -> list[str]:
        return [url for _, url, _ in self.calls]


def ok(result) -> dict:
    return {"jsonrpc": "2.0", "id": 1, "result": result}


def word(value: int) -> str:
    return "0x" + format(value, "064x")


# ── EIP-55 ───────────────────────────────────────────────────────────────────

EIP55_VECTORS = [
    "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
    "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
    "0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
    "0xD1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb",
]


@pytest.mark.parametrize("address", EIP55_VECTORS)
def test_eip55_checksum_vectors(address):
    assert eth.to_checksum_address(address.lower()) == address
    assert eth.to_checksum_address(address.upper().replace("0X", "0x")) == address
    assert eth.is_valid_eth_address(address)


@pytest.mark.parametrize(
    "address",
    [
        "0x52908400098527886E0F7030069857D2E4169EE7",  # all caps: no checksum
        "0x8617E340B3D01FA5F11F306F4090FD50E238070D",
        "0xde709f2102306220921060314715629080e2fb77",  # all lower: no checksum
        "0x27b1fdb04752bbc536007a920d24acb045561c26",
        "0x" + "0" * 40,
    ],
)
def test_single_case_addresses_are_valid_without_checksum(address):
    assert eth.is_valid_eth_address(address)


@pytest.mark.parametrize(
    "address",
    [
        "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAeD",  # one letter's case flipped
        "0x5AAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
        "0X5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",  # upper-case prefix
        "5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",  # no prefix
        "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAe",  # 39 hex
        "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed0",  # 41 hex
        "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAeg",  # non-hex
        "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed\n",  # trailing newline
        "",
        None,
        12345,
    ],
)
def test_invalid_eth_addresses(address):
    assert not eth.is_valid_eth_address(address)


def test_to_checksum_address_rejects_garbage():
    with pytest.raises(ValueError):
        eth.to_checksum_address("0x1234")


# ── token table ──────────────────────────────────────────────────────────────


def test_erc20_table_matches_v1_tokens():
    table = {t.symbol: (t.contract, t.decimals, t.coingecko_id) for t in eth.ERC20_TOKENS}
    assert table == {
        "AAVE": ("0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", 18, "aave"),
        "USDC": ("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 6, "usd-coin"),
        "USDT": ("0xdAC17F958D2ee523a2206206994597C13D831ec7", 6, "tether"),
        "aUSDT": ("0x23878914EFE38d27C4D67Ab83ed1b93A74D4086a", 6, "tether"),
    }


def test_erc20_contracts_carry_valid_eip55_checksums():
    for token in eth.ERC20_TOKENS:
        assert eth.to_checksum_address(token.contract) == token.contract
        assert isinstance(token.decimals, int)


# ── RPC endpoints and failover ───────────────────────────────────────────────


def test_default_endpoints_are_v1_list_in_order():
    rpc = eth.EthRpc(request=FakeRpc({}))
    assert rpc.endpoints == eth.DEFAULT_RPC_URLS
    assert rpc.endpoints[0] == "https://ethereum.publicnode.com"
    assert rpc.endpoints[-1] == "https://rpc.ankr.com/eth"


def test_ankr_key_url_is_tried_first_and_redacted_in_endpoints():
    keyed = f"https://rpc.ankr.com/eth/{ANKR_KEY}"
    fake = FakeRpc({keyed: [ok("0x1")]})
    rpc = eth.EthRpc(ankr_api_key=f"  {ANKR_KEY}\n", request=fake)
    assert rpc.endpoints[0] == "https://rpc.ankr.com/eth/***"
    assert rpc.endpoints[1:] == eth.DEFAULT_RPC_URLS
    assert rpc.get_balance(WALLET) == 1
    method, url, kwargs = fake.calls[0]
    assert (method, url) == ("POST", keyed)
    assert kwargs["redact"] == (ANKR_KEY,)


def test_ankr_key_with_url_characters_is_a_config_error():
    with pytest.raises(ConfigError):
        eth.EthRpc(ankr_api_key="abc/../x", request=FakeRpc({}))


def test_request_payload_and_policy():
    fake = FakeRpc({"https://a": [ok("0x0de0b6b3a7640000")]})
    rpc = eth.EthRpc(["https://a"], request=fake)
    assert rpc.get_balance(WALLET) == 10**18
    method, _url, kwargs = fake.calls[0]
    assert method == "POST"
    assert kwargs["json_body"]["method"] == "eth_getBalance"
    assert kwargs["json_body"]["params"] == [WALLET.lower(), "latest"]
    assert kwargs["json_body"]["jsonrpc"] == "2.0"
    assert kwargs["timeout"] == 10.0
    assert kwargs["retries"] == 0
    assert kwargs["service"] == "Ethereum RPC"


@pytest.mark.parametrize(
    "bad_reply",
    [
        ok("0x"),
        ok(None),
        ok(""),
        ok("123"),
        ok("0xzz"),
        ok(17),
        ok("0x" + "f" * 65),  # does not fit uint256
        {"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "Unauthorized"}},
        {"jsonrpc": "2.0", "id": 1},
        ["not", "an", "object"],
        None,
    ],
    ids=[
        "0x",
        "null",
        "empty",
        "no-prefix",
        "non-hex",
        "number",
        "overflow",
        "rpc-error",
        "no-result",
        "list",
        "none",
    ],
)
def test_F79_malformed_result_fails_over_to_the_next_endpoint(bad_reply):
    """v1 returned data['result'] unchecked; int('0x', 16) then aborted the whole sync."""
    fake = FakeRpc({"https://a": [bad_reply], "https://b": [ok("0x2a")]})
    rpc = eth.EthRpc(["https://a", "https://b"], request=fake)
    assert rpc.get_balance(WALLET) == 42
    assert fake.urls == ["https://a", "https://b"]


def test_F79_transport_errors_fail_over_too():
    fake = FakeRpc(
        {
            "https://a": [ExternalServiceError("HTTP 503", service="Ethereum RPC", status=503)],
            "https://b": [ok("0x")],
            "https://c": [ok(word(5))],
        }
    )
    rpc = eth.EthRpc(["https://a", "https://b", "https://c"], request=fake)
    assert rpc.get_balance(WALLET) == 5


def test_all_endpoints_failing_raises_with_the_last_errors_and_no_secret(caplog):
    keyed = f"https://rpc.ankr.com/eth/{ANKR_KEY}"
    urls = ["https://a", "https://b", "https://c"]
    fake = FakeRpc(
        {
            keyed: [ExternalServiceError(f"failed {keyed}", service="x")],
            "https://a": [ok("0x")],
            "https://b": [{"error": {"message": f"bad key {ANKR_KEY}"}}],
            "https://c": [ok(None)],
        }
    )
    caplog.set_level(logging.DEBUG)
    rpc = eth.EthRpc(urls, ankr_api_key=ANKR_KEY, request=fake)
    with pytest.raises(ExternalServiceError) as info:
        rpc.get_balance(WALLET)
    exc = info.value
    assert exc.service == "Ethereum RPC"
    assert exc.retriable is True
    assert "eth_getBalance" in str(exc)
    assert "https://c" in str(exc)
    assert "https://a" in str(exc)
    assert ANKR_KEY not in str(exc)
    assert ANKR_KEY not in exc.user_message
    assert ANKR_KEY not in caplog.text
    assert len(fake.calls) == 4


@pytest.mark.parametrize("key_start", range(100, 215))
def test_ankr_key_straddling_the_reason_cut_is_not_leaked(caplog, key_start):
    """Reviewer probe: the RPC error was cut to 120 chars *before* redaction, so a key
    crossing the cut left its first characters in the exception and the warning log.
    The key is placed at every position around both the old and the current cut."""
    key = "0123456789abcdef0123456789abcdef"
    keyed = f"https://rpc.ankr.com/eth/{key}"
    # reason == "RPC error: " + message; put the key at index key_start of the reason.
    prefix = "RPC error: " + "x" * (key_start - len("RPC error: ") - 1 - len(keyed) + len(key))
    message = prefix[len("RPC error: ") :] + f" {keyed}"
    assert (prefix + f" {keyed}").index(key) == key_start
    fake = FakeRpc(
        {
            keyed: [{"jsonrpc": "2.0", "id": 1, "error": {"message": message}}],
            "https://a": [ExternalServiceError("down", service="x")],
        }
    )
    caplog.set_level(logging.DEBUG)
    rpc = eth.EthRpc(["https://a"], ankr_api_key=key, request=fake)
    with pytest.raises(ExternalServiceError) as info:
        rpc.get_balance(WALLET)
    for text in (str(info.value), info.value.user_message, caplog.text):
        assert "eth/0" not in text  # any prefix of the key right after the URL path


def test_long_untrusted_values_are_summarised_not_cut():
    key = "k" * 30 + "SECRETKEY"
    fake = FakeRpc(
        {
            f"https://rpc.ankr.com/eth/{key}": [ok("y" * 35 + key + "z" * 100)],
            "https://a": [ok("0x05")],
        }
    )
    rpc = eth.EthRpc(["https://a"], ankr_api_key=key, request=fake)
    assert rpc.get_balance(WALLET) == 5
    with pytest.raises(eth.RpcReplyError, match="str, 176 characters") as info:
        eth.parse_uint256("y" * 35 + key + "z" * 100)
    assert "SECRET" not in str(info.value)
    with pytest.raises(eth.RpcReplyError, match="'0xzz'"):
        eth.parse_uint256("0xzz")


def test_rpc_reply_error_is_a_public_value_error():
    assert issubclass(eth.RpcReplyError, ValueError)
    with pytest.raises(ValueError):
        eth.parse_uint256("0x")


@pytest.mark.parametrize(
    "parse",
    [
        lambda value: int(value, 16),  # ValueError on "garbage"
        lambda value: value["balance"],  # TypeError: string indices must be integers
        lambda value: {"a": 1}[value],  # KeyError
        lambda value: 1 // (len(value) - len("garbage")),  # ZeroDivisionError
    ],
    ids=["ValueError", "TypeError", "KeyError", "ArithmeticError"],
)
def test_F79_custom_parse_errors_fail_over_to_the_next_endpoint(parse):
    """Reviewer probe: only the private reply error failed over; a caller's own parse
    raising ValueError escaped on the first endpoint — F79 again for any new call site."""
    fake = FakeRpc({"https://a": [ok("garbage")], "https://b": [ok("good")]})
    rpc = eth.EthRpc(["https://a", "https://b"], request=fake)

    def tolerant(value):
        return "parsed" if value == "good" else parse(value)

    assert rpc.call("eth_blockNumber", [], tolerant) == "parsed"
    assert fake.urls == ["https://a", "https://b"]


def test_custom_parse_failing_everywhere_names_the_exception_type():
    fake = FakeRpc({"https://a": [ok("garbage")], "https://b": [ok("junk")]})
    rpc = eth.EthRpc(["https://a", "https://b"], request=fake)
    with pytest.raises(ExternalServiceError) as info:
        rpc.call("eth_blockNumber", [], lambda value: int(value, 16))
    assert "malformed result (ValueError" in str(info.value)
    assert len(fake.calls) == 2


def test_unexpected_exceptions_from_parse_still_propagate():
    """Only the documented parse failures fail over; anything else is a bug to surface."""

    def broken(_value):
        raise RuntimeError("bug")

    fake = FakeRpc({"https://a": [ok("0x1")], "https://b": [ok("0x1")]})
    rpc = eth.EthRpc(["https://a", "https://b"], request=fake)
    with pytest.raises(RuntimeError, match="bug"):
        rpc.call("eth_blockNumber", [], broken)
    assert fake.urls == ["https://a"]


def test_invalid_request_arguments_are_not_treated_as_endpoint_failures():
    """A ValueError from request_json (bad URL) is a programming error, not a failover."""
    request = functools.partial(net.request_json, opener=FakeOpener(), sleep=SleepRecorder())
    rpc = eth.EthRpc(["https://bad:port", "https://b"], request=request)
    with pytest.raises(ValueError, match="invalid port"):
        rpc.get_balance(WALLET)


def test_ankr_key_redacted_end_to_end_through_net(caplog):
    keyed = f"https://rpc.ankr.com/eth/{ANKR_KEY}"
    opener = FakeOpener(
        http_error(keyed, 401, f"invalid key {ANKR_KEY}"), json_response(ok("0x10"))
    )
    request = functools.partial(net.request_json, opener=opener, sleep=SleepRecorder())
    caplog.set_level(logging.DEBUG)
    rpc = eth.EthRpc(["https://ethereum.publicnode.com"], ankr_api_key=ANKR_KEY, request=request)
    assert rpc.get_balance(WALLET) == 16
    assert ANKR_KEY not in caplog.text
    assert "rpc.ankr.com/eth/***" in caplog.text


def test_erc20_balance_of_calldata():
    token = eth.ERC20_TOKENS[1]
    fake = FakeRpc({"https://a": [ok(word(1_234_567))]})
    rpc = eth.EthRpc(["https://a"], request=fake)
    assert rpc.erc20_balance(WALLET, token.contract) == 1_234_567
    params = fake.calls[0][2]["json_body"]["params"]
    assert fake.calls[0][2]["json_body"]["method"] == "eth_call"
    assert params[1] == "latest"
    assert params[0]["to"] == token.contract.lower()
    data = params[0]["data"]
    assert data == "0x70a08231" + "0" * 24 + WALLET[2:].lower()
    assert len(data) == 2 + 8 + 64


def test_invalid_address_never_reaches_the_network():
    fake = FakeRpc({})
    rpc = eth.EthRpc(["https://a"], request=fake)
    with pytest.raises(ValueError, match="Invalid Ethereum address"):
        rpc.get_balance("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAeD")
    with pytest.raises(ValueError):
        eth.fetch_wallet_balances("not-an-address", rpc=rpc)
    assert fake.calls == []


# ── wallet balances ──────────────────────────────────────────────────────────


def wallet_rpc(eth_wei: int, token_raw: dict[str, int]) -> tuple[eth.EthRpc, FakeRpc]:
    replies = [ok(hex(eth_wei))] + [ok(word(token_raw.get(t.symbol, 0))) for t in eth.ERC20_TOKENS]
    fake = FakeRpc({"https://a": replies})
    return eth.EthRpc(["https://a"], request=fake), fake


def test_fetch_wallet_balances_uses_integers_and_token_decimals():
    rpc, fake = wallet_rpc(
        1_500_000_000_000_000_000, {"USDC": 2_500_000, "AAVE": 3 * 10**18 + 1, "USDT": 0}
    )
    balances = eth.fetch_wallet_balances(WALLET, rpc=rpc)
    assert set(balances) == {"ETH", "USDC", "AAVE"}  # zero USDT/aUSDT omitted (v1 parity)
    assert balances["ETH"] == eth.TokenBalance("ETH", 1_500_000_000_000_000_000, 18)
    assert balances["USDC"] == eth.TokenBalance("USDC", 2_500_000, 6)
    assert balances["ETH"].amount == Decimal("1.5")
    assert balances["USDC"].amount == Decimal("2.5")
    assert balances["AAVE"].amount == Decimal("3.000000000000000001")
    for balance in balances.values():
        assert isinstance(balance.raw, int)
        assert isinstance(balance.decimals, int)
    assert len(fake.calls) == 1 + len(eth.ERC20_TOKENS)


def test_eth_is_reported_even_when_zero():
    rpc, _ = wallet_rpc(0, {})
    balances = eth.fetch_wallet_balances(WALLET, rpc=rpc)
    assert balances == {"ETH": eth.TokenBalance("ETH", 0, 18)}


def test_token_failure_on_every_endpoint_fails_the_whole_fetch():
    fake = FakeRpc({"https://a": [ok("0x1"), ok("0x")]})
    rpc = eth.EthRpc(["https://a"], request=fake)
    with pytest.raises(ExternalServiceError, match="eth_call"):
        eth.fetch_wallet_balances(WALLET, rpc=rpc)


MAX_UINT256 = 2**256 - 1


@pytest.mark.parametrize(
    ("raw", "decimals", "expected"),
    [
        (0, 18, "0E-18"),
        (1, 18, "1E-18"),
        (123456789012345678901234567890123, 18, "123456789012345.678901234567890123"),
        (MAX_UINT256, 6, f"{str(MAX_UINT256)[:-6]}.{str(MAX_UINT256)[-6:]}"),
        (5, 0, "5"),
    ],
)
def test_scale_units_is_exact_beyond_default_decimal_precision(raw, decimals, expected):
    amount = eth.scale_units(raw, decimals)
    assert str(amount) == expected
    assert amount.as_tuple().exponent == -decimals


def test_scale_units_rejects_negative_values():
    with pytest.raises(ValueError):
        eth.scale_units(-1, 6)


def test_real_net_json_decoding_with_eth_rpc():
    body = json.dumps(ok("0x0")).encode()
    opener = FakeOpener(json_response(json.loads(body)))
    request = functools.partial(net.request_json, opener=opener, sleep=SleepRecorder())
    rpc = eth.EthRpc(["https://ethereum.publicnode.com"], request=request)
    assert rpc.get_balance(WALLET) == 0
    sent = json.loads(opener.calls[0].body)
    assert sent["method"] == "eth_getBalance"
    assert opener.calls[0].timeout == 10.0
