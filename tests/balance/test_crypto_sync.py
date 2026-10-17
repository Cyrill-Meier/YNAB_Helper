"""Tests for ynab_helper.balance.crypto.sync (DESIGN §6.6; F05, F28, F29, F48, F49)."""

from __future__ import annotations

import functools
import json
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest

from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.balance import adjust
from ynab_helper.balance.adjust import AdjustmentRefused
from ynab_helper.balance.crypto import eth, sync
from ynab_helper.balance.crypto.prices import fetch_prices_chf
from ynab_helper.balance.crypto.sync import Holding, crypto_sync
from ynab_helper.errors import ConfigError, ExternalServiceError

B = "budget-1"
A = "acct-crypto"
TODAY = date(2026, 10, 16)

# Mnemonic "abandon ×11 about" (BIP49 / BIP84 test vectors), as in tests/balance/crypto.
BIP49_YPUB = "ypub6Ww3ibxVfGzLrAH1PNcjyAWenMTbbAosGNB6VvmSEgytSER9azLDWCxoJwW7Ke7icmizBMXrzBx9979FfaHxHcrArf3zbeJJJUZPf663zsP"
BIP49_RECEIVE_0 = "37VucYSaXLCAsxYyAPfbSi9eh4iEcbShgf"
BIP84_ZPUB = "zpub6rFR7y4Q2AijBEqTUquhVz398htDFrtymD9xYYfG1m4wAcvPhXNfE3EfH1r1ADqtfSdVCToUG868RvUUkgDKf31mGDtKsAYz2oz2AGutZYs"
BIP84_RECEIVE_0 = "bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu"
ETH_ADDRESS = "0x" + "ab" * 20
USDC = next(t for t in eth.ERC20_TOKENS if t.symbol == "USDC")


class FakeChain:
    """Stands in for BlockstreamClient: ``address -> (sats, tx_count)``."""

    def __init__(self, stats: Mapping[str, tuple[int, int]] | None = None) -> None:
        self.stats = dict(stats or {})
        self.queried: list[str] = []

    def address_stats(self, address: str) -> tuple[int, int]:
        self.queried.append(address)
        return self.stats.get(address, (0, 0))


class FakeEth:
    """Stands in for EthRpc."""

    def __init__(self, wei: int = 0, tokens: Mapping[str, int] | None = None) -> None:
        self.wei = wei
        self.tokens = {k.lower(): v for k, v in (tokens or {}).items()}
        self.calls = 0

    def get_balance(self, address: str) -> int:
        self.calls += 1
        return self.wei

    def erc20_balance(self, address: str, contract: str) -> int:
        self.calls += 1
        return self.tokens.get(contract.lower(), 0)


class Prices:
    def __init__(self, prices: Mapping[str, Any]) -> None:
        self.prices = dict(prices)
        self.requested: list[list[str]] = []

    def __call__(self, symbols: Iterable[str]) -> dict[str, Any]:
        wanted = list(symbols)
        self.requested.append(wanted)
        return {s: self.prices[s] for s in wanted if s in self.prices}


PRICES = {"BTC": Decimal("80000"), "ETH": Decimal("3000"), "USDC": Decimal("0.88")}


@pytest.fixture
def fake() -> FakeYnab:
    f = FakeYnab()
    f.add_budget(B)
    f.add_account(B, A, "Crypto", type="otherAsset", on_budget=False)
    f.add_account(B, "checking", "Checking")
    return f


def seed_balance(fake: FakeYnab, milli: int) -> None:
    fake.add_transaction(B, A, amount=milli, date="2026-10-01", cleared="cleared")


def run(fake: FakeYnab, **kwargs: Any) -> sync.CryptoSyncResult:
    kwargs.setdefault("blockstream", FakeChain({BIP84_RECEIVE_0: (50_000_000, 3)}))
    kwargs.setdefault("eth_rpc", FakeEth(wei=2 * 10**18))
    kwargs.setdefault("price_fetcher", Prices(PRICES))
    kwargs.setdefault("today", TODAY)
    kwargs.setdefault("btc_xpub", BIP84_RECEIVE_0)
    kwargs.setdefault("eth_address", ETH_ADDRESS)
    return crypto_sync(fake.client(), B, A, **kwargs)


def posts(fake: FakeYnab) -> list[Any]:
    return fake.calls_matching("POST")


def test_F29_result_is_structured(fake):
    seed_balance(fake, 20_000_000)
    result = run(fake)
    assert result.holdings == [
        Holding("BTC", Decimal("0.5"), Decimal("80000"), 40_000_000),
        Holding("ETH", Decimal("2"), Decimal("3000"), 6_000_000),
    ]
    assert isinstance(result.holdings[0].amount, Decimal)
    assert result.portfolio_milli == 46_000_000
    assert result.ynab_before_milli == 20_000_000
    adj = result.adjustment
    assert adj.outcome == "created"
    assert adj.delta_milli == 26_000_000
    assert adj.import_id == "CRYPTO:26000000:2026-10-16:1"
    assert result.ynab_after_milli == 46_000_000
    assert result.btc is not None and result.btc.source == "address"
    tx = fake.tx(adj.ynab_tx_id)
    assert tx["payee_name"] == "Crypto Portfolio Adjustment"
    assert tx["cleared"] == "cleared"
    assert tx["memo"] == "BTC 0.5@80,000 | ETH 2@3,000"
    assert tx["date"] == "2026-10-16"
    json.dumps(result.to_dict())  # JSON-safe


def test_F29_duplicate_is_reported_as_such(fake):
    seed_balance(fake, 20_000_000)
    first = run(fake)
    fake.user_delete_transaction(first.adjustment.ynab_tx_id)  # same delta again today
    second = run(fake)
    assert second.adjustment.outcome == "duplicate"
    assert second.ynab_after_milli == second.ynab_before_milli == 20_000_000


def test_second_run_after_adjustment_is_in_sync(fake):
    seed_balance(fake, 20_000_000)
    run(fake)
    again = run(fake)
    assert again.adjustment.outcome == "in_sync"
    assert len(posts(fake)) == 1


@pytest.mark.parametrize(
    "prices",
    [
        {"BTC": Decimal("80000")},  # ETH missing
        {},  # empty reply
        {"BTC": Decimal("80000"), "ETH": Decimal("0")},
        {"BTC": Decimal("80000"), "ETH": Decimal("NaN")},
        {"BTC": Decimal("80000"), "ETH": Decimal("-1")},
        {"BTC": Decimal("80000"), "ETH": 3000.0},  # not a Decimal
    ],
)
def test_F05_missing_or_bad_price_never_posts(fake, prices):
    seed_balance(fake, 30_000_000)
    with pytest.raises(ExternalServiceError) as info:
        run(fake, price_fetcher=Prices(prices))
    assert "nothing was posted" in info.value.user_message
    assert posts(fake) == []


def test_F05_empty_coingecko_reply_never_posts(fake):
    seed_balance(fake, 30_000_000)
    fetcher = functools.partial(fetch_prices_chf, request=lambda *a, **k: {})
    with pytest.raises(ExternalServiceError):
        run(fake, price_fetcher=fetcher)
    assert posts(fake) == []


def test_prices_are_requested_only_for_held_assets(fake):
    prices = Prices(PRICES)
    result = run(fake, eth_rpc=FakeEth(wei=0), price_fetcher=prices)
    assert prices.requested == [["BTC"]]
    assert result.zero_balances == ["ETH"]
    assert [h.symbol for h in result.holdings] == ["BTC"]


def test_tokens_follow_eth_in_display_order(fake):
    result = run(
        fake, eth_rpc=FakeEth(wei=10**18, tokens={USDC.contract: 1_500_000}), btc_xpub=None
    )
    assert [h.symbol for h in result.holdings] == ["ETH", "USDC"]
    assert result.holdings[1] == Holding("USDC", Decimal("1.5"), Decimal("0.88"), 1_320)


def test_zero_address_guard_refuses_unused_xpub_when_ynab_holds_value(fake):
    seed_balance(fake, 30_000_000)
    chain = FakeChain()  # nothing ever used
    with pytest.raises(AdjustmentRefused) as info:
        run(fake, btc_xpub=BIP84_ZPUB, eth_address=None, blockstream=chain)
    assert info.value.reason == "btc_unused"
    assert "Nothing was posted" in info.value.user_message
    assert posts(fake) == []
    assert BIP84_ZPUB not in info.value.user_message


def test_zero_address_guard_can_be_overridden(fake):
    seed_balance(fake, 30_000_000)
    result = run(
        fake, btc_xpub=BIP84_ZPUB, eth_address=None, blockstream=FakeChain(), allow_zero=True
    )
    assert result.adjustment.outcome == "created"
    assert result.adjustment.delta_milli == -30_000_000
    assert result.warnings


def test_zero_address_guard_does_not_block_an_empty_ynab_account(fake):
    result = run(fake, btc_xpub=BIP84_ZPUB, eth_address=None, blockstream=FakeChain())
    assert result.adjustment.outcome == "in_sync"
    assert result.btc is not None and result.btc.used_addresses == 0


def test_unused_single_address_is_guarded_too(fake):
    seed_balance(fake, 30_000_000)
    with pytest.raises(AdjustmentRefused):
        run(fake, btc_xpub=BIP84_RECEIVE_0, eth_address=None, blockstream=FakeChain())


def test_used_but_emptied_wallet_may_post_a_full_withdrawal(fake):
    seed_balance(fake, 30_000_000)
    chain = FakeChain({BIP84_RECEIVE_0: (0, 4)})  # used, now empty
    result = run(fake, btc_xpub=BIP84_ZPUB, eth_address=None, blockstream=chain)
    assert result.adjustment.delta_milli == -30_000_000
    assert result.adjustment.outcome == "created"


def test_F28_ypub_is_scanned_as_nested_segwit(fake):
    chain = FakeChain({BIP49_RECEIVE_0: (10_000_000, 1)})
    result = run(fake, btc_xpub=BIP49_YPUB, eth_address=None, blockstream=chain)
    assert result.btc is not None
    assert (result.btc.kind, result.btc.balance_sats, result.btc.used_addresses) == (
        "ypub",
        10_000_000,
        1,
    )
    assert result.holdings == [Holding("BTC", Decimal("0.1"), Decimal("80000"), 8_000_000)]
    assert chain.queried[0] == BIP49_RECEIVE_0
    assert all(addr.startswith("3") for addr in chain.queried)
    assert result.adjustment.delta_milli == 8_000_000


def test_F49_compares_against_working_balance(fake):
    fake.add_transaction(B, A, amount=20_000_000, date="2026-10-01", cleared="reconciled")
    # Transfer Checking → Crypto: uncleared on the crypto side, already in the wallet value.
    fake.add_transaction(B, "checking", amount=-6_000_000, date="2026-10-10", transfer_account_id=A)
    result = run(fake, btc_xpub=BIP84_RECEIVE_0, eth_address=None)
    assert result.portfolio_milli == 40_000_000
    assert result.ynab_before_milli == 26_000_000  # cleared 20,000 + uncleared 6,000
    assert result.ynab_uncleared_milli == 6_000_000
    # v1 compared with cleared (20,000) and double counted the transfer.
    assert result.adjustment.delta_milli == 40_000_000 - 26_000_000
    assert any("uncleared" in w for w in result.warnings)


def test_F49_in_sync_with_uncleared_transfer(fake):
    fake.add_transaction(B, A, amount=34_000_000, date="2026-10-01", cleared="cleared")
    fake.add_transaction(B, "checking", amount=-6_000_000, date="2026-10-10", transfer_account_id=A)
    result = run(fake, btc_xpub=BIP84_RECEIVE_0, eth_address=None)
    assert result.adjustment.outcome == "in_sync"
    assert posts(fake) == []


def test_F48_typo_in_xpub_is_rejected_before_any_request(fake):
    typo = BIP84_ZPUB[:-5] + ("A" if BIP84_ZPUB[-5] != "A" else "B") + BIP84_ZPUB[-4:]
    chain, rpc = FakeChain(), FakeEth()
    with pytest.raises(ConfigError) as info:
        run(fake, btc_xpub=typo, eth_address=ETH_ADDRESS, blockstream=chain, eth_rpc=rpc)
    assert typo not in info.value.user_message
    assert chain.queried == [] and rpc.calls == 0 and fake.calls == []


@pytest.mark.parametrize("address", ["0x123", "0x" + "Ab" * 20, "not an address"])
def test_invalid_eth_address_is_rejected_before_any_request(fake, address):
    rpc = FakeEth()
    with pytest.raises(ConfigError):
        run(fake, btc_xpub=None, eth_address=address, eth_rpc=rpc)
    assert rpc.calls == 0 and fake.calls == []


def test_no_wallet_configured(fake):
    with pytest.raises(ConfigError):
        run(fake, btc_xpub=None, eth_address=" ")
    assert fake.calls == []


def test_values_are_quantized_half_even(fake):
    # 1 sat × 50,000 = 0.0005 CHF = 0.5 milli → 0; 3 sats → 1.5 milli → 2.
    prices = Prices({"BTC": Decimal("50000")})
    one = run(
        fake,
        btc_xpub=BIP84_RECEIVE_0,
        eth_address=None,
        price_fetcher=prices,
        blockstream=FakeChain({BIP84_RECEIVE_0: (1, 1)}),
    )
    three = run(
        fake,
        btc_xpub=BIP84_RECEIVE_0,
        eth_address=None,
        price_fetcher=prices,
        blockstream=FakeChain({BIP84_RECEIVE_0: (3, 1)}),
    )
    assert one.holdings[0].value_milli == 0
    assert three.holdings[0].value_milli == 2


def test_eighteen_decimal_amounts_are_exact(fake):
    wei = 1_234_567_890_123_456_789
    result = run(
        fake,
        btc_xpub=None,
        eth_address=ETH_ADDRESS,
        eth_rpc=FakeEth(wei=wei),
        price_fetcher=Prices({"ETH": Decimal("2345.6789012")}),
    )
    holding = result.holdings[0]
    assert holding.amount == Decimal("1.234567890123456789")
    exact = Decimal("1.234567890123456789") * Decimal("2345.6789012")
    assert holding.value_milli == int((exact * 1000).to_integral_value())


def test_dry_run_reads_but_does_not_post(fake):
    seed_balance(fake, 20_000_000)
    result = run(fake, dry_run=True)
    assert result.adjustment.outcome == "dry_run"
    assert result.adjustment.import_id == "CRYPTO:26000000:2026-10-16:1"
    assert posts(fake) == []
    assert result.ynab_after_milli == 20_000_000


def test_ankr_key_is_passed_to_the_default_rpc(fake, monkeypatch):
    created: list[dict[str, Any]] = []

    class RecordingRpc(FakeEth):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            created.append(kwargs)
            super().__init__(wei=10**18)

    monkeypatch.setattr(sync.eth, "EthRpc", RecordingRpc)
    run(fake, btc_xpub=None, eth_rpc=None, ankr_api_key="k3y")
    assert created == [{"ankr_api_key": "k3y"}]


def test_default_today_is_the_local_date(fake, monkeypatch):
    monkeypatch.setattr(adjust, "local_today", lambda: date(2027, 1, 2))
    seed_balance(fake, 20_000_000)
    result = run(fake, today=None)
    assert result.adjustment.import_id == "CRYPTO:26000000:2027-01-02:1"


def test_non_chf_budget_is_refused_before_any_request(fake):
    chain, rpc = FakeChain(), FakeEth()
    with pytest.raises(ConfigError) as info:
        run(fake, blockstream=chain, eth_rpc=rpc, budget_currency="EUR")
    assert "EUR" in info.value.user_message
    assert chain.queried == [] and rpc.calls == 0 and fake.calls == []


def test_chf_budget_is_accepted(fake):
    assert run(fake, budget_currency="chf").adjustment.outcome == "created"


def test_datetime_today_keeps_the_v1_import_id(fake):
    seed_balance(fake, 20_000_000)
    result = run(fake, today=datetime(2026, 10, 16, 21, 45))
    assert result.adjustment.import_id == "CRYPTO:26000000:2026-10-16:1"
    assert fake.tx(result.adjustment.ynab_tx_id)["date"] == "2026-10-16"


def test_invalid_today_is_rejected_before_any_request(fake):
    chain, rpc = FakeChain(), FakeEth()
    with pytest.raises(TypeError):
        run(fake, blockstream=chain, eth_rpc=rpc, today=20261016)
    assert chain.queried == [] and rpc.calls == 0 and fake.calls == []
