"""Crypto portfolio → YNAB tracking account (DESIGN §6.6; findings F05, F28, F29, F48, F49).

:func:`crypto_sync` values the configured wallets from public data and posts one
``Crypto Portfolio Adjustment`` for the difference to the account's working balance:

1. Inputs are validated before any network call (bad checksum, private key, testnet …
   raise :class:`~ynab_helper.errors.ConfigError`; F48).
2. BTC: an xpub/ypub/zpub is scanned for every script type its SLIP-132 version implies
   (F28); a single address is looked up directly. ETH: native balance plus the tracked
   ERC-20 tokens, through :class:`~ynab_helper.balance.crypto.eth.EthRpc` (with the
   optional Ankr key).
3. Every holding with a positive amount needs a positive, finite CHF price — otherwise the
   sync raises and posts nothing (F05). Amounts and prices are ``Decimal``; values are
   quantized to milliunits with ``ROUND_HALF_EVEN``.
4. YNAB's **working** balance (cleared + uncleared) is the comparison base (F49).
5. If the wallet scan found no address that was ever used while YNAB holds a non-zero
   value, the input is probably wrong (typo'd key, a Taproot-only wallet the scanner does
   not derive): :class:`~ynab_helper.balance.adjust.AdjustmentRefused` unless
   ``allow_zero=True``.
6. The result is a structured :class:`CryptoSyncResult` (F29) — nothing is printed.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date as Date
from decimal import Decimal, localcontext
from typing import Any, Final, NamedTuple

from ynab_helper.balance.adjust import (
    OUTCOME_CREATED,
    PAYEE_CRYPTO,
    PREFIX_CRYPTO,
    AdjustmentRefused,
    AdjustmentResult,
    account_balances,
    check_valuation_currency,
    clip_memo,
    local_day,
    post_balance_adjustment,
)
from ynab_helper.balance.crypto import btc, eth
from ynab_helper.balance.crypto.prices import BTC_SYMBOL, fetch_prices_chf
from ynab_helper.errors import ConfigError, ExternalServiceError
from ynab_helper.money import decimal_to_milli, format_money
from ynab_helper.ynab.client import YnabClient

__all__ = [
    "BTC_DECIMALS",
    "VALUATION_CURRENCY",
    "BtcSummary",
    "CryptoSyncResult",
    "Holding",
    "PriceFetcher",
    "crypto_sync",
]

log = logging.getLogger(__name__)

BTC_DECIMALS: Final = 8
#: CoinGecko prices are fetched in CHF.
VALUATION_CURRENCY: Final = "CHF"
#: Enough digits that ``amount × price`` (18-decimal tokens included) is exact.
_DECIMAL_PRECISION: Final = 80

PriceFetcher = Callable[[Iterable[str]], Mapping[str, Decimal]]


class Holding(NamedTuple):
    """One priced holding: ``(symbol, amount, price CHF, value in milliunits)``."""

    symbol: str
    amount: Decimal
    price: Decimal
    value_milli: int


@dataclass(frozen=True, slots=True)
class BtcSummary:
    """How the BTC balance was obtained (no key material)."""

    source: str  # "xpub" | "address"
    kind: str  # "xpub" | "ypub" | "zpub" | "address"
    balance_sats: int
    used_addresses: int  # addresses with any transaction (0 or 1 for a single address)
    scanned: int


@dataclass(frozen=True, slots=True)
class CryptoSyncResult:
    """Structured outcome of :func:`crypto_sync` (F29).

    ``holdings`` lists every holding with a positive amount in display order (BTC, ETH,
    then tokens alphabetically); ``zero_balances`` the checked symbols that hold nothing.
    ``ynab_before_milli`` is YNAB's working balance before the adjustment.
    """

    holdings: list[Holding]
    portfolio_milli: int
    ynab_before_milli: int
    adjustment: AdjustmentResult
    ynab_uncleared_milli: int = 0
    zero_balances: list[str] = field(default_factory=list)
    btc: BtcSummary | None = None
    warnings: list[str] = field(default_factory=list)

    @property
    def ynab_after_milli(self) -> int:
        """YNAB's working balance after this run (unchanged unless a transaction was created)."""
        if self.adjustment.outcome == OUTCOME_CREATED:
            return self.ynab_before_milli + self.adjustment.delta_milli
        return self.ynab_before_milli

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe representation (Decimals as strings)."""
        return {
            "holdings": [
                {
                    "symbol": h.symbol,
                    "amount": str(h.amount),
                    "price": str(h.price),
                    "value_milli": h.value_milli,
                }
                for h in self.holdings
            ],
            "portfolio_milli": self.portfolio_milli,
            "ynab_before_milli": self.ynab_before_milli,
            "ynab_after_milli": self.ynab_after_milli,
            "ynab_uncleared_milli": self.ynab_uncleared_milli,
            "zero_balances": list(self.zero_balances),
            "btc": None
            if self.btc is None
            else {
                "source": self.btc.source,
                "kind": self.btc.kind,
                "balance_sats": self.btc.balance_sats,
                "used_addresses": self.btc.used_addresses,
                "scanned": self.btc.scanned,
            },
            "warnings": list(self.warnings),
            "adjustment": self.adjustment.to_dict(),
        }


# ── input validation ───────────────────────────────────────────────────────


def _validate_btc(value: str | None) -> btc.BtcInput | None:
    if value is None or not value.strip():
        return None
    try:
        return btc.validate_btc_input(value)
    except btc.InvalidBitcoinInput as exc:
        raise ConfigError(
            f"invalid Bitcoin key/address: {exc}",
            user_message=f"The configured Bitcoin wallet is invalid: {exc}",
        ) from None


def _validate_eth(value: str | None) -> str | None:
    if value is None or not value.strip():
        return None
    address = value.strip()
    if not eth.is_valid_eth_address(address):
        raise ConfigError(
            "invalid Ethereum address",
            user_message="The configured Ethereum address is invalid: expected 0x followed by "
            "40 hex characters (check the capitalisation if you typed it by hand).",
        )
    return address


# ── valuation ──────────────────────────────────────────────────────────────


def _btc_balance(
    wallet: btc.BtcInput, blockstream: btc.BlockstreamClient
) -> tuple[Decimal, BtcSummary]:
    if wallet[0] == "xpub":
        key = wallet[1]
        scan = btc.scan_xpub(key, blockstream.address_stats)
        summary = BtcSummary(
            source="xpub",
            kind=key.version_kind,
            balance_sats=scan.balance_sats,
            used_addresses=scan.used_addresses,
            scanned=scan.scanned,
        )
    else:
        sats, tx_count = blockstream.address_stats(wallet[1])
        summary = BtcSummary(
            source="address",
            kind="address",
            balance_sats=sats,
            used_addresses=1 if tx_count > 0 else 0,
            scanned=1,
        )
    return eth.scale_units(summary.balance_sats, BTC_DECIMALS), summary


def _display_order(symbols: Iterable[str]) -> list[str]:
    present = set(symbols)
    first = [s for s in (BTC_SYMBOL, eth.ETH_SYMBOL) if s in present]
    return first + sorted(present - set(first))


def _checked_prices(held: list[str], prices: Mapping[str, Decimal]) -> dict[str, Decimal]:
    """A positive finite Decimal price for every held symbol, or raise (F05)."""
    checked: dict[str, Decimal] = {}
    missing: list[str] = []
    for symbol in held:
        price = prices.get(symbol)
        if not isinstance(price, Decimal) or not price.is_finite() or price <= 0:
            missing.append(symbol)
        else:
            checked[symbol] = price
    if missing:
        raise ExternalServiceError(
            f"no usable CHF price for {', '.join(missing)}",
            service="CoinGecko",
            retriable=True,
            user_message=f"No CHF price available for {', '.join(missing)} right now — "
            "nothing was posted. Please try again later.",
        )
    return checked


def _memo_part(holding: Holding) -> str:
    # Display only, formatted like v1's floats ("BTC 0.5@80,000"); the value itself is
    # computed with Decimals.
    return f"{holding.symbol} {float(holding.amount):.6g}@{float(holding.price):,.0f}"


def crypto_sync(
    client: YnabClient,
    budget_id: str,
    crypto_account_id: str,
    *,
    btc_xpub: str | None = None,
    eth_address: str | None = None,
    ankr_api_key: str | None = None,
    dry_run: bool = False,
    allow_zero: bool = False,
    blockstream: btc.BlockstreamClient | None = None,
    eth_rpc: eth.EthRpc | None = None,
    price_fetcher: PriceFetcher | None = None,
    today: Date | None = None,
    budget_currency: str | None = None,
) -> CryptoSyncResult:
    """Value the wallets and post the difference to ``crypto_account_id``.

    ``btc_xpub`` is an xpub/ypub/zpub or a single address; ``eth_address`` an Ethereum
    address. ``blockstream``, ``eth_rpc`` and ``price_fetcher`` are injectable for tests
    (defaults: Blockstream, public RPCs with ``ankr_api_key`` first, CoinGecko).
    ``today`` (local date, default today; a ``datetime`` counts as its local day) dates the
    adjustment and its import_id. Prices are in CHF, so a
    known ``budget_currency`` other than CHF is refused up front. A dry run still reads
    wallets, prices and YNAB, but writes nothing.

    Raises:
        ConfigError: no wallet configured, an invalid key/address, or a non-CHF budget
            (all before any request).
        ExternalServiceError: a chain/price lookup failed, or a held asset has no price.
        AdjustmentRefused: the wallet scan found no used address while YNAB holds a value
            (unless ``allow_zero``).
        YnabError: reading the account or posting the adjustment failed.
    """
    day = local_day(today)
    check_valuation_currency(budget_currency, VALUATION_CURRENCY)
    btc_wallet = _validate_btc(btc_xpub)
    eth_wallet = _validate_eth(eth_address)
    if btc_wallet is None and eth_wallet is None:
        raise ConfigError(
            "no crypto wallet configured",
            user_message="No crypto wallet is configured. Add a Bitcoin key/address or an "
            "Ethereum address first.",
        )

    amounts: dict[str, Decimal] = {}
    btc_summary: BtcSummary | None = None
    if btc_wallet is not None:
        amounts[BTC_SYMBOL], btc_summary = _btc_balance(
            btc_wallet, blockstream or btc.BlockstreamClient()
        )
    if eth_wallet is not None:
        rpc = eth_rpc or eth.EthRpc(ankr_api_key=ankr_api_key)
        for symbol, balance in eth.fetch_wallet_balances(eth_wallet, rpc=rpc).items():
            amounts[symbol] = balance.amount

    order = _display_order(amounts)
    held = [s for s in order if amounts[s] > 0]
    zero = [s for s in order if amounts[s] <= 0]
    fetch = price_fetcher or fetch_prices_chf
    prices = _checked_prices(held, fetch(held) if held else {})

    holdings: list[Holding] = []
    with localcontext() as ctx:
        ctx.prec = _DECIMAL_PRECISION
        for symbol in held:
            value = amounts[symbol] * prices[symbol]
            holdings.append(
                Holding(symbol, amounts[symbol], prices[symbol], decimal_to_milli(value))
            )
    portfolio = sum(h.value_milli for h in holdings)

    balances = account_balances(client, budget_id, crypto_account_id)
    warnings: list[str] = []
    if btc_summary is not None and btc_summary.used_addresses == 0:
        if balances.working_milli != 0 and not allow_zero:
            what = "address" if btc_summary.source == "address" else f"{btc_summary.kind} key"
            raise AdjustmentRefused(
                f"BTC {what} has no used address while YNAB holds {balances.working_milli}",
                reason="btc_unused",
                user_message=f"The Bitcoin {what} shows no transactions at all "
                f"({btc_summary.scanned} address(es) checked), but YNAB holds "
                f"{format_money(balances.working_milli, 'CHF')}. Nothing was posted. Check the "
                "configured key (Taproot wallets are not supported) or confirm that the "
                "wallet is really empty.",
            )
        warnings.append("No used Bitcoin address was found for the configured wallet.")
    if balances.uncleared_milli:
        warnings.append(
            f"The account has uncleared transactions "
            f"({format_money(balances.uncleared_milli, 'CHF', sign=True)}); they are counted "
            "in the balance that was compared."
        )

    memo = clip_memo(" | ".join(_memo_part(h) for h in holdings))
    adjustment = post_balance_adjustment(
        client,
        budget_id,
        crypto_account_id,
        target_milli=portfolio,
        current_milli=balances.working_milli,
        payee=PAYEE_CRYPTO,
        memo=memo,
        prefix=PREFIX_CRYPTO,
        date=day,
        cleared="cleared",
        dry_run=dry_run,
    )
    log.info(
        "crypto sync: portfolio=%s ynab=%s delta=%s outcome=%s",
        format_money(portfolio),
        format_money(balances.working_milli),
        format_money(adjustment.delta_milli, sign=True),
        adjustment.outcome,
    )
    return CryptoSyncResult(
        holdings=holdings,
        portfolio_milli=portfolio,
        ynab_before_milli=balances.working_milli,
        adjustment=adjustment,
        ynab_uncleared_milli=balances.uncleared_milli,
        zero_balances=zero,
        btc=btc_summary,
        warnings=warnings,
    )
