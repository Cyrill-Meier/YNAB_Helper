"""CHF prices from CoinGecko — complete or not at all (DESIGN §6.6, F05, F42).

v1 skipped a symbol CoinGecko did not price, so the holding was valued at 0 and the
crypto sync posted an adjustment wiping its value. :func:`fetch_prices_chf` returns a
positive, finite :class:`~decimal.Decimal` for **every** requested symbol or raises.
"""

from __future__ import annotations

import urllib.parse
from collections.abc import Callable, Iterable, Mapping
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Final

from ynab_helper import net
from ynab_helper.balance.crypto.eth import ERC20_TOKENS, ETH_COINGECKO_ID, ETH_SYMBOL
from ynab_helper.errors import ExternalServiceError

COINGECKO_SIMPLE_PRICE_URL: Final = "https://api.coingecko.com/api/v3/simple/price"
BTC_SYMBOL: Final = "BTC"
BTC_COINGECKO_ID: Final = "bitcoin"
VS_CURRENCY: Final = "chf"

#: Symbol → CoinGecko id, derived from the native coins plus the single ERC-20 table.
COINGECKO_IDS: Final[Mapping[str, str]] = MappingProxyType(
    {
        BTC_SYMBOL: BTC_COINGECKO_ID,
        ETH_SYMBOL: ETH_COINGECKO_ID,
        **{token.symbol: token.coingecko_id for token in ERC20_TOKENS},
    }
)


def coingecko_id(symbol: str) -> str:
    """CoinGecko id of a tracked symbol.

    Raises:
        ValueError: the symbol is not tracked (a programming error: symbols come from
            our own tables).
    """
    try:
        return COINGECKO_IDS[symbol]
    except KeyError:
        raise ValueError(f"no CoinGecko id for symbol {symbol!r}") from None


def fetch_prices_chf(
    symbols: Iterable[str],
    *,
    request: Callable[..., Any] = net.request_json,
    url: str = COINGECKO_SIMPLE_PRICE_URL,
    timeout: float = 15.0,
    retries: int = 3,
) -> dict[str, Decimal]:
    """CHF price of each symbol (one request for all of them).

    Returns:
        ``{symbol: price}`` with an entry for every requested symbol; ``{}`` (and no
        request) when ``symbols`` is empty.

    Raises:
        ValueError: an unknown symbol (checked before any request).
        ExternalServiceError: the request failed, or any symbol is missing, non-numeric,
            non-finite or not positive in the reply. Never a partial result.
    """
    wanted = sorted(set(symbols))
    if not wanted:
        return {}
    ids = {symbol: coingecko_id(symbol) for symbol in wanted}
    query = urllib.parse.urlencode(
        {"ids": ",".join(sorted(set(ids.values()))), "vs_currencies": VS_CURRENCY},
        safe=",",
    )
    reply = request(
        "GET",
        f"{url}?{query}",
        timeout=timeout,
        retries=retries,
        service="CoinGecko",
        parse_float=Decimal,
    )
    prices: dict[str, Decimal] = {}
    problems: list[str] = []
    for symbol, cg_id in ids.items():
        price = _price_from_reply(reply, cg_id)
        if price is None:
            problems.append(symbol)
        else:
            prices[symbol] = price
    if problems:
        raise ExternalServiceError(
            f"CoinGecko returned no usable CHF price for {', '.join(problems)}",
            service="CoinGecko",
            retriable=True,
            user_message=f"No CHF price available for {', '.join(problems)} right now — "
            "nothing was posted. Please try again later.",
        )
    return prices


def _price_from_reply(reply: Any, cg_id: str) -> Decimal | None:
    """The positive finite CHF price for ``cg_id`` in a CoinGecko reply, else ``None``."""
    if not isinstance(reply, dict):
        return None
    entry = reply.get(cg_id)
    if not isinstance(entry, dict):
        return None
    value = entry.get(VS_CURRENCY)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        price = Decimal(value)
    elif isinstance(value, Decimal):
        price = value
    elif isinstance(value, float):
        # Only if a caller's request function ignores parse_float; str() keeps the
        # shortest repr instead of the binary expansion.
        price = Decimal(str(value))
    else:
        return None
    if not price.is_finite() or price <= 0:
        return None
    return price
