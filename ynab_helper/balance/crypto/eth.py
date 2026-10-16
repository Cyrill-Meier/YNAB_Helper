"""Ethereum wallet balances over public JSON-RPC with endpoint failover (DESIGN §6.6).

* :data:`ERC20_TOKENS` is the single table of tracked tokens (symbol, contract, decimals,
  CoinGecko id) — the same tokens as v1; :mod:`.prices` derives its id mapping from it.
* :class:`EthRpc` tries the endpoints in order (an Ankr URL with the user's key first when
  configured). A transport error, a JSON-RPC error *or a malformed result* (``"0x"``,
  ``null``, non-hex — F79) moves on to the next endpoint; only when all fail does it raise.
  The Ankr key never appears in messages or logs: untrusted text is redacted *before* it
  is shortened, so a key straddling the cut cannot leak a prefix.
* Balances are integers (wei / token base units) with their decimals — no floats.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final, TypeVar

from ynab_helper import net
from ynab_helper.errors import ConfigError, ExternalServiceError

log = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Erc20Token:
    """One tracked ERC-20 token."""

    symbol: str
    contract: str  # EIP-55 checksummed
    decimals: int
    coingecko_id: str


#: Tracked tokens (v1 parity). aUSDT (Aave interest-bearing USDT) is priced as tether.
ERC20_TOKENS: Final[tuple[Erc20Token, ...]] = (
    Erc20Token("AAVE", "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", 18, "aave"),
    Erc20Token("USDC", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 6, "usd-coin"),
    Erc20Token("USDT", "0xdAC17F958D2ee523a2206206994597C13D831ec7", 6, "tether"),
    Erc20Token("aUSDT", "0x23878914EFE38d27C4D67Ab83ed1b93A74D4086a", 6, "tether"),
)

ETH_SYMBOL: Final = "ETH"
ETH_DECIMALS: Final = 18
ETH_COINGECKO_ID: Final = "ethereum"

#: v1's public endpoints, ordered by observed reliability. Unauthenticated Ankr answers
#: "-32000 Unauthorized" nowadays, so it stays last.
DEFAULT_RPC_URLS: Final[tuple[str, ...]] = (
    "https://ethereum.publicnode.com",
    "https://eth.drpc.org",
    "https://rpc.payload.de",
    "https://1rpc.io/eth",
    "https://eth.meowrpc.com",
    "https://cloudflare-eth.com",
    "https://eth.llamarpc.com",
    "https://rpc.ankr.com/eth",
)
ANKR_KEYED_URL: Final = "https://rpc.ankr.com/eth/{key}"

_BALANCE_OF_SELECTOR: Final = "0x70a08231"  # keccak("balanceOf(address)")[:4]
_ADDRESS_RE: Final = re.compile(r"0x[0-9a-fA-F]{40}")
# JSON-RPC quantities/words for a uint256: at least one and at most 64 hex digits.
_UINT256_RE: Final = re.compile(r"0x[0-9a-fA-F]{1,64}")
_ANKR_KEY_RE: Final = re.compile(r"[A-Za-z0-9_-]{1,256}")
_MAX_ERRORS_IN_MESSAGE: Final = 3
# Per-endpoint failure reasons are cut to this length, after redaction.
_MAX_REASON_CHARS: Final = 200
# Untrusted values up to this length are echoed in full; longer ones are only summarised
# (never cut, because a cut can split a secret and leave a prefix the redactor misses).
_MAX_ECHO_CHARS: Final = 80


# ── addresses (EIP-55) ───────────────────────────────────────────────────────


def _resolve_keccak() -> Callable[[bytes], bytes] | None:
    try:
        from Crypto.Hash import keccak
    except ImportError:  # pragma: no cover - pycryptodome is a pinned dependency
        return None
    return lambda data: keccak.new(digest_bits=256, data=data).digest()


_KECCAK256 = _resolve_keccak()


def keccak256(data: bytes) -> bytes:
    """Ethereum's Keccak-256 (not NIST SHA3-256, which pads differently)."""
    if _KECCAK256 is None:  # pragma: no cover
        raise ConfigError(
            "Keccak-256 is unavailable: install pycryptodome",
            user_message="Ethereum address checks are unavailable on this server.",
        )
    return _KECCAK256(data)


def to_checksum_address(address: str) -> str:
    """EIP-55 mixed-case form of a ``0x`` + 40 hex address (any input case).

    Raises:
        ValueError: not ``0x`` followed by 40 hex digits.
    """
    if not isinstance(address, str) or not _ADDRESS_RE.fullmatch(address):
        raise ValueError("not an Ethereum address (0x + 40 hex characters)")
    body = address[2:].lower()
    digest = keccak256(body.encode("ascii")).hex()
    return "0x" + "".join(
        ch.upper() if ch.isalpha() and int(digest[i], 16) >= 8 else ch for i, ch in enumerate(body)
    )


def is_valid_eth_address(value: object) -> bool:
    """``0x`` + 40 hex digits; a mixed-case address must carry a valid EIP-55 checksum.

    All-lower and all-upper addresses carry no checksum and are accepted as typed.
    """
    if not isinstance(value, str) or not _ADDRESS_RE.fullmatch(value):
        return False
    body = value[2:]
    if body == body.lower() or body == body.upper():
        return True
    return value == to_checksum_address(value)


def _require_address(address: str) -> str:
    if not is_valid_eth_address(address):
        raise ValueError(
            "Invalid Ethereum address: expected 0x followed by 40 hex characters "
            "(check the capitalisation if you typed it by hand)."
        )
    return address.lower()


# ── JSON-RPC client ──────────────────────────────────────────────────────────


class RpcReplyError(ValueError):
    """An endpoint's JSON-RPC reply is unusable (error object, no result, malformed result).

    :meth:`EthRpc.call` fails over to the next endpoint on it. Messages may quote the
    untrusted reply verbatim; :meth:`EthRpc.call` redacts them before shortening.
    """


#: Exceptions from a ``parse`` callable that :meth:`EthRpc.call` treats as a malformed
#: result (fail over) rather than a bug to propagate — e.g. ``int(value, 16)`` raising
#: ``ValueError`` on garbage, or ``value["x"]`` raising ``KeyError``/``TypeError``.
PARSE_FAILURES: Final[tuple[type[Exception], ...]] = (
    ValueError,  # includes RpcReplyError
    TypeError,
    LookupError,
    ArithmeticError,
)


def _describe(value: Any) -> str:
    """``repr`` of an untrusted value, or only its type and size when that is long."""
    text = repr(value)
    if len(text) <= _MAX_ECHO_CHARS:
        return text
    return f"<{type(value).__name__}, {len(text)} characters>"


def parse_uint256(result: Any) -> int:
    """Parse a hex quantity / 32-byte word. ``"0x"``, ``None`` or non-hex are rejected.

    Raises:
        RpcReplyError: not ``0x`` followed by 1–64 hex digits.
    """
    if not isinstance(result, str) or not _UINT256_RE.fullmatch(result):
        raise RpcReplyError(f"malformed result {_describe(result)}")
    return int(result, 16)


def _extract_result(reply: Any) -> Any:
    if not isinstance(reply, dict):
        raise RpcReplyError("reply is not a JSON-RPC object")
    error = reply.get("error")
    if error is not None:
        message = error.get("message") if isinstance(error, dict) else error
        # Not shortened here: EthRpc.call redacts first, then cuts.
        raise RpcReplyError(f"RPC error: {message}")
    if "result" not in reply:
        raise RpcReplyError("reply has neither result nor error")
    return reply["result"]


class EthRpc:
    """Ethereum JSON-RPC with ordered failover across endpoints."""

    def __init__(
        self,
        urls: Sequence[str] | None = None,
        *,
        ankr_api_key: str | None = None,
        request: Callable[..., Any] = net.request_json,
        timeout: float = 10.0,
        retries: int = 0,
    ) -> None:
        """
        Args:
            urls: endpoints in priority order (default :data:`DEFAULT_RPC_URLS`).
            ankr_api_key: optional ``ANKR_API_KEY``; its keyed URL is tried first.
            request: :func:`ynab_helper.net.request_json`-compatible callable.
            timeout: per-request timeout in seconds.
            retries: per-endpoint retries; failover to the next endpoint is the main retry.

        Raises:
            ConfigError: the Ankr key contains characters that cannot be part of a URL path.
        """
        endpoints = list(DEFAULT_RPC_URLS if urls is None else urls)
        key = (ankr_api_key or "").strip()
        self._secrets: tuple[str, ...] = ()
        if key:
            if not _ANKR_KEY_RE.fullmatch(key):
                raise ConfigError(
                    "ANKR_API_KEY contains invalid characters",
                    user_message="ANKR_API_KEY is malformed (letters, digits, - and _ only).",
                )
            endpoints.insert(0, ANKR_KEYED_URL.format(key=key))
            self._secrets = (key,)
        if not endpoints:
            raise ConfigError("no Ethereum RPC endpoints configured")
        self._urls = tuple(endpoints)
        self._request = request
        self._timeout = timeout
        self._retries = retries
        self._next_id = 0

    @property
    def endpoints(self) -> tuple[str, ...]:
        """Endpoint URLs in try order, with the API key redacted (safe to display)."""
        return tuple(self._redact(url) for url in self._urls)

    def _redact(self, text: str) -> str:
        return net.redact_text(text, self._secrets)

    def _safe_reason(self, text: str) -> str:
        """One-line, redacted, then shortened failure reason (safe for messages and logs)."""
        # Order matters: shortening first could split the key so that it no longer
        # matches the redactor. Whitespace is collapsed first; keys contain none.
        return self._redact(" ".join(text.split()))[:_MAX_REASON_CHARS]

    def call(self, method: str, params: list[Any], parse: Callable[[Any], T]) -> T:
        """Call ``method`` and return ``parse(result)`` from the first endpoint that works.

        ``parse`` runs inside the failover loop: if it raises one of
        :data:`PARSE_FAILURES` (``RpcReplyError``, ``ValueError``, ``TypeError``,
        ``LookupError``, ``ArithmeticError``), the result counts as malformed and the next
        endpoint is tried (F79), exactly like ``"0x"``/``null`` or a JSON-RPC error.

        Raises:
            ExternalServiceError: every endpoint failed (message lists the last few errors).
            ValueError: the request itself is invalid (e.g. a malformed endpoint URL) —
                a programming error, not an endpoint failure.
        """
        self._next_id += 1
        payload = {"jsonrpc": "2.0", "id": self._next_id, "method": method, "params": params}
        errors: list[str] = []
        for url in self._urls:
            safe_url = self._redact(url)
            try:
                reply = self._request(
                    "POST",
                    url,
                    json_body=payload,
                    timeout=self._timeout,
                    retries=self._retries,
                    service="Ethereum RPC",
                    redact=self._secrets,
                )
            except ExternalServiceError as exc:
                reason = self._safe_reason(str(exc))
            else:
                try:
                    return parse(_extract_result(reply))
                except PARSE_FAILURES as exc:
                    reason = self._safe_reason(_parse_failure_reason(exc))
            errors.append(f"{safe_url}: {reason}")
            log.warning("ETH RPC %s via %s failed: %s", method, safe_url, reason)
        raise ExternalServiceError(
            f"All Ethereum RPC endpoints failed for {method}: "
            + " | ".join(errors[-_MAX_ERRORS_IN_MESSAGE:]),
            service="Ethereum RPC",
            retriable=True,
            user_message="Could not read the Ethereum wallet: every RPC endpoint failed. "
            "Please try again later.",
        )

    def get_balance(self, address: str) -> int:
        """Native ETH balance in wei at the latest block.

        Raises:
            ValueError: invalid address.
            ExternalServiceError: all endpoints failed.
        """
        return self.call("eth_getBalance", [_require_address(address), "latest"], parse_uint256)

    def erc20_balance(self, address: str, contract: str) -> int:
        """``balanceOf(address)`` of an ERC-20 contract, in the token's base units."""
        owner = _require_address(address)
        token = _require_address(contract)
        calldata = _BALANCE_OF_SELECTOR + owner[2:].rjust(64, "0")
        return self.call("eth_call", [{"to": token, "data": calldata}, "latest"], parse_uint256)


def _parse_failure_reason(exc: Exception) -> str:
    if isinstance(exc, RpcReplyError):
        return str(exc)
    return f"malformed result ({type(exc).__name__}: {exc})"


# ── wallet balances ──────────────────────────────────────────────────────────


def scale_units(raw: int, decimals: int) -> Decimal:
    """Exact ``raw / 10**decimals`` as a Decimal (no context rounding, no floats)."""
    if raw < 0 or decimals < 0:
        raise ValueError("raw and decimals must be non-negative")
    digits = tuple(int(d) for d in str(raw))
    return Decimal((0, digits, -decimals))


@dataclass(frozen=True, slots=True)
class TokenBalance:
    """A holding in integer base units (wei for ETH)."""

    symbol: str
    raw: int
    decimals: int

    @property
    def amount(self) -> Decimal:
        """Exact amount in whole tokens."""
        return scale_units(self.raw, self.decimals)


def fetch_wallet_balances(
    address: str,
    *,
    rpc: EthRpc | None = None,
    tokens: Iterable[Erc20Token] = ERC20_TOKENS,
) -> dict[str, TokenBalance]:
    """Balances of an Ethereum wallet: ETH always, tokens only when non-zero (v1 parity).

    Raises:
        ValueError: invalid address (no network call is made).
        ExternalServiceError: a balance could not be read from any endpoint. No partial
            result is returned — a missing token must never read as zero.
    """
    _require_address(address)
    client = rpc or EthRpc()
    balances = {ETH_SYMBOL: TokenBalance(ETH_SYMBOL, client.get_balance(address), ETH_DECIMALS)}
    for token in tokens:
        raw = client.erc20_balance(address, token.contract)
        if raw > 0:
            balances[token.symbol] = TokenBalance(token.symbol, raw, token.decimals)
    return balances
