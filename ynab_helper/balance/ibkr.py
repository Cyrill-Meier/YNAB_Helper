"""Interactive Brokers Client Portal Gateway: account discovery and NAV (DESIGN §6.6).

* TLS: the gateway ships a self-signed certificate, so verification is disabled **only**
  when it runs on this machine (``localhost``, ``127.0.0.1``, ``::1``). Any other host is
  verified normally, and plain ``http://`` to a non-local host is refused (F80) — an
  on-path attacker could otherwise feed a fake ledger that becomes a YNAB adjustment.
* ``/portfolio/accounts`` is always called before any ``/portfolio/{id}/…`` endpoint, as
  IBKR requires on a fresh gateway session.
* :meth:`IbkrClient.nav_chf` keeps v1's ledger logic: ``BASE.netliquidationvalue``,
  falling back to the ``CHF`` segment when BASE is missing or zero. It refuses accounts
  whose base currency is known and is not CHF instead of mislabelling the value.

The orchestration (compare with YNAB, post the adjustment, never post NAV ≤ 0) lives in
the brokerage sync, not here.
"""

from __future__ import annotations

import logging
import ssl
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final

from ynab_helper import net
from ynab_helper.errors import ConfigError, ExternalServiceError

log = logging.getLogger(__name__)

DEFAULT_IBKR_BASE_URL: Final = "https://localhost:5050"
LOOPBACK_HOSTS: Final = frozenset({"localhost", "127.0.0.1", "::1"})
NAV_CURRENCY: Final = "CHF"
_API_PREFIX: Final = "/v1/api"
_SERVICE: Final = "IBKR"


def _hostname(base_url: str) -> tuple[str, str]:
    parts = urllib.parse.urlsplit(base_url)
    invalid = parts.scheme not in ("http", "https") or not parts.hostname
    if not invalid:
        try:
            parts.port  # noqa: B018 - evaluated for its validation side effect
        except ValueError:
            # Caught here so a typo becomes a ConfigError at start-up instead of a
            # request error on every sync.
            invalid = True
    if invalid:
        raise ConfigError(
            f"IBKR_BASE_URL must be an http(s) URL with a valid port, got {base_url!r}",
            user_message="IBKR_BASE_URL must look like https://localhost:5050.",
        )
    return parts.scheme, (parts.hostname or "").lower()


def is_loopback_host(base_url: str) -> bool:
    """True if ``base_url`` points at this machine (exactly localhost/127.0.0.1/::1)."""
    return _hostname(base_url)[1] in LOOPBACK_HOSTS


def tls_context_for(base_url: str) -> ssl.SSLContext | None:
    """TLS context for the gateway at ``base_url``.

    Returns:
        An unverified context for a loopback ``https`` gateway (self-signed certificate),
        a default *verifying* context for any other ``https`` host, and ``None`` for
        ``http://localhost``-style URLs (no TLS).

    Raises:
        ConfigError: invalid URL, or plain ``http`` to a non-loopback host.
    """
    scheme, host = _hostname(base_url)
    loopback = host in LOOPBACK_HOSTS
    if scheme == "http":
        if not loopback:
            raise ConfigError(
                f"refusing unencrypted IBKR connection to {host}",
                user_message="IBKR_BASE_URL must use https unless the gateway runs on localhost.",
            )
        return None
    if loopback:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context
    return ssl.create_default_context()


@dataclass(frozen=True, slots=True)
class IbkrAccount:
    """An account visible to the gateway session."""

    account_id: str
    currency: str | None  # base currency as reported by /portfolio/accounts
    display_name: str | None = None


class IbkrClient:
    """Minimal read-only client for the IB Client Portal Gateway."""

    def __init__(
        self,
        base_url: str = DEFAULT_IBKR_BASE_URL,
        *,
        request: Callable[..., Any] = net.request_json,
        timeout: float = 10.0,
        retries: int = 2,
        backoff: float = 0.5,
    ) -> None:
        """
        Raises:
            ConfigError: invalid ``base_url`` or plain http to a remote host.
        """
        self._base_url = base_url.rstrip("/")
        self._ssl_context = tls_context_for(self._base_url)
        self._request = request
        self._timeout = timeout
        self._retries = retries
        self._backoff = backoff
        self._accounts: list[IbkrAccount] | None = None

    @property
    def verifies_tls(self) -> bool:
        """Whether certificates are verified for this gateway (False only for loopback)."""
        return self._ssl_context is None or self._ssl_context.verify_mode != ssl.CERT_NONE

    def accounts(self) -> list[IbkrAccount]:
        """List the accounts of the gateway session (``GET /portfolio/accounts``).

        Raises:
            ExternalServiceError: gateway unreachable, session expired, malformed reply or
                no accounts (usually: not logged in to the gateway).
        """
        reply = self._get("/portfolio/accounts")
        if not isinstance(reply, list):
            raise self._malformed("/portfolio/accounts did not return a list")
        accounts = [_parse_account(item) for item in reply if isinstance(item, dict)]
        accounts = [a for a in accounts if a is not None]
        if not accounts:
            raise ExternalServiceError(
                "IBKR gateway returned no accounts",
                service=_SERVICE,
                retriable=True,
                user_message=f"No IBKR accounts are visible. Log in to the Client Portal "
                f"Gateway at {self._base_url} and try again.",
            )
        self._accounts = accounts
        return list(accounts)

    def default_account_id(self) -> str:
        """The first account of the session (v1's choice when no account id is configured)."""
        return self._ensure_accounts()[0].account_id

    def nav_chf(self, account_id: str) -> Decimal:
        """Net liquidation value of ``account_id`` in CHF.

        Calls ``/portfolio/accounts`` first if this client has not done so yet.

        Raises:
            ConfigError: the account is not visible to the session, or its base currency
                is known and is not CHF.
            ExternalServiceError: gateway errors, or a ledger without a usable NAV.
        """
        account = self._find_account(account_id)
        if account.currency and account.currency.upper() != NAV_CURRENCY:
            raise ConfigError(
                f"IBKR account {account_id} has base currency {account.currency}",
                user_message=f"IBKR account {account_id} reports in {account.currency}; "
                f"only {NAV_CURRENCY} base accounts are supported.",
            )
        ledger = self._get(f"/portfolio/{urllib.parse.quote(account_id, safe='')}/ledger")
        return self._nav_from_ledger(ledger)

    # ── internals ────────────────────────────────────────────────────────

    def _ensure_accounts(self) -> list[IbkrAccount]:
        if self._accounts is None:
            self.accounts()
        assert self._accounts is not None
        return self._accounts

    def _find_account(self, account_id: str) -> IbkrAccount:
        if not account_id:
            raise ConfigError("IBKR account id is empty")
        accounts = self._ensure_accounts()
        for account in accounts:
            if account.account_id == account_id:
                return account
        available = ", ".join(a.account_id for a in accounts)
        raise ConfigError(
            f"IBKR account {account_id} is not visible to the gateway (available: {available})",
            user_message=f"IBKR account {account_id} is not available in this gateway session "
            f"(available: {available}).",
        )

    def _nav_from_ledger(self, ledger: Any) -> Decimal:
        if not isinstance(ledger, dict):
            raise self._malformed("ledger is not an object")
        found_any = False
        nav = Decimal(0)
        for segment in ("BASE", NAV_CURRENCY):
            entry = ledger.get(segment)
            if not isinstance(entry, dict) or "netliquidationvalue" not in entry:
                continue
            found_any = True
            nav = _to_decimal(entry["netliquidationvalue"])
            if nav is None:
                raise self._malformed(f"{segment}.netliquidationvalue is not a finite number")
            if nav != 0:
                return nav
        if not found_any:
            raise self._malformed("ledger has no BASE or CHF net liquidation value")
        return nav

    def _get(self, path: str) -> Any:
        url = f"{self._base_url}{_API_PREFIX}{path}"
        try:
            return self._request(
                "GET",
                url,
                timeout=self._timeout,
                retries=self._retries,
                backoff=self._backoff,
                service=_SERVICE,
                ssl_context=self._ssl_context,
                parse_float=Decimal,
            )
        except ExternalServiceError as exc:
            raise self._explain(exc) from None

    def _explain(self, exc: ExternalServiceError) -> ExternalServiceError:
        """Attach an actionable user message (v1 printed the same hints)."""
        if exc.status == 401:
            hint = f"The IBKR gateway session expired. Re-authenticate at {self._base_url}."
        elif exc.status is None:
            hint = (
                f"Cannot reach the IB Gateway at {self._base_url}. Make sure the Client "
                "Portal Gateway is running and you are logged in."
            )
        else:
            hint = f"The IB Gateway returned an error (HTTP {exc.status})."
        return ExternalServiceError(
            str(exc),
            service=_SERVICE,
            status=exc.status,
            retriable=exc.retriable,
            user_message=hint,
        )

    def _malformed(self, reason: str) -> ExternalServiceError:
        return ExternalServiceError(
            f"IBKR returned an unexpected response: {reason}",
            service=_SERVICE,
            retriable=True,
            user_message="The IB Gateway returned an unexpected response; nothing was posted.",
        )


def _parse_account(item: dict[str, Any]) -> IbkrAccount | None:
    account_id = item.get("accountId") or item.get("id")
    if not isinstance(account_id, str) or not account_id:
        return None
    currency = item.get("currency")
    name = item.get("displayName") or item.get("accountTitle")
    return IbkrAccount(
        account_id=account_id,
        currency=currency if isinstance(currency, str) and currency else None,
        display_name=name if isinstance(name, str) else None,
    )


def _to_decimal(value: Any) -> Decimal | None:
    """A finite Decimal from a JSON number (Decimal/int/float), else ``None``."""
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, float):
        result = Decimal(str(value))
    else:
        return None
    return result if result.is_finite() else None
