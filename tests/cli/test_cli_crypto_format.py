"""R23: the CLI shows crypto prices and amounts exactly like the bot's /crypto.

The CLI used to format prices with ``f"{price:,.2f}"`` (half-even: ``0.125`` → ``0.12``)
and amounts at full precision, while the bot goes through :func:`money.format_money`
(half away from zero: ``0.13``) and shows at most 8 decimals.
"""

from __future__ import annotations

import re
from decimal import Decimal

import pytest

from ynab_helper import cli
from ynab_helper.balance.adjust import AdjustmentResult
from ynab_helper.balance.crypto.sync import CryptoSyncResult, Holding
from ynab_helper.bot import render


def _result(*holdings: Holding) -> CryptoSyncResult:
    return CryptoSyncResult(
        holdings=list(holdings),
        portfolio_milli=sum(h.value_milli for h in holdings),
        ynab_before_milli=0,
        adjustment=AdjustmentResult(
            outcome="in_sync",
            delta_milli=0,
            import_id=None,
            ynab_tx_id=None,
            target_milli=0,
            current_milli=0,
            account_id="acct-crypto",
            date="2026-10-17",
            payee="Crypto Portfolio Adjustment",
            memo=None,
            cleared="cleared",
        ),
    )


def _cli_holding(line: str) -> tuple[str, str, str]:
    match = re.fullmatch(r"\s+(\S+)\s+(\S+) × \s*(\S+) = \s*(\S+)", line)
    assert match, line
    return match[2], match[3], match[4]


def _bot_holding(text: str, symbol: str) -> tuple[str, str, str]:
    match = re.search(rf"• {symbol} (\S+) × (\S+) CHF = (\S+) CHF", text)
    assert match, text
    return match[1], match[2], match[3]


@pytest.mark.parametrize(
    ("price", "shown"),
    [
        (Decimal("0.125"), "0.13"),
        (Decimal("1234.565"), "1,234.57"),
        (Decimal("1.005"), "1.01"),
        (Decimal("95432.1234567"), "95,432.12"),
        (Decimal("0.0001"), "0.00"),
    ],
)
def test_runtime_infra_R23_cli_prices_round_half_away_from_zero(price: Decimal, shown: str):
    lines = cli.format_crypto_result(_result(Holding("SOL", Decimal(1), price, 0)))
    assert _cli_holding(lines[1])[1] == shown


def test_runtime_infra_R23_cli_amounts_have_at_most_eight_decimals():
    holdings = [
        Holding("ETH", Decimal("0.123456789012345678"), Decimal(3300), 407_407),
        Holding("BTC", Decimal("0.50000000"), Decimal(1), 500),
        Holding("USDC", Decimal("1E+3"), Decimal(1), 1_000_000),
    ]
    lines = cli.format_crypto_result(_result(*holdings))
    assert [_cli_holding(line)[0] for line in lines[1:4]] == ["0.12345679", "0.5", "1000"]


def test_runtime_infra_R23_cli_and_bot_show_the_same_holding_digits():
    holdings = [
        Holding("BTC", Decimal("0.5"), Decimal("1234.565"), 617_283),
        Holding("ETH", Decimal("0.123456789012345678"), Decimal("0.125"), 15),
        Holding("SOL", Decimal("12.3"), Decimal("1.005"), 12_362),
        Holding("PEPE", Decimal("1000000"), Decimal("0.00001234"), 12_340),
        Holding("WBTC", Decimal("0.00000001"), Decimal("95432.1234567"), 1),
    ]
    result = _result(*holdings)
    cli_lines = cli.format_crypto_result(result)
    bot_text = render.crypto_result(result)
    for index, holding in enumerate(holdings, start=1):
        assert _cli_holding(cli_lines[index]) == _bot_holding(bot_text, holding.symbol)


def test_runtime_infra_R23_a_price_money_cannot_represent_is_shown_as_is():
    lines = cli.format_crypto_result(_result(Holding("XYZ", Decimal(1), Decimal("NaN"), 0)))
    assert _cli_holding(lines[1])[1] == "NaN"
