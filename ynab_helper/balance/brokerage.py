"""Interactive Brokers NAV → YNAB tracking account (DESIGN §6.6; findings F49, F80).

:func:`brokerage_sync` reads the account's net liquidation value (CHF) from the Client
Portal Gateway (:class:`~ynab_helper.balance.ibkr.IbkrClient`, which verifies TLS for any
non-loopback host) and posts one ``IBKR Portfolio Adjustment`` for the difference to the
YNAB account's **working** balance (cleared + uncleared, F49). A NAV of zero or less is
never posted: it means the gateway session or account access is broken, not that the
portfolio is worthless (v1 guard).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date as Date
from decimal import Decimal
from typing import Any

from ynab_helper.balance.adjust import (
    OUTCOME_CREATED,
    PAYEE_IBKR,
    PREFIX_IBKR,
    AdjustmentRefused,
    AdjustmentResult,
    account_balances,
    check_valuation_currency,
    local_day,
    post_balance_adjustment,
)
from ynab_helper.balance.ibkr import NAV_CURRENCY, IbkrClient
from ynab_helper.money import decimal_to_milli, format_money
from ynab_helper.ynab.client import YnabClient

__all__ = ["BrokerageSyncResult", "brokerage_sync"]

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BrokerageSyncResult:
    """Structured outcome of :func:`brokerage_sync` (amounts in milliunits)."""

    ibkr_account_id: str
    nav: Decimal  # CHF, as reported by the gateway
    nav_milli: int
    ynab_before_milli: int  # working balance before the adjustment
    ynab_uncleared_milli: int
    adjustment: AdjustmentResult

    @property
    def ynab_after_milli(self) -> int:
        if self.adjustment.outcome == OUTCOME_CREATED:
            return self.ynab_before_milli + self.adjustment.delta_milli
        return self.ynab_before_milli

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe representation (the NAV as a string)."""
        return {
            "ibkr_account_id": self.ibkr_account_id,
            "nav": str(self.nav),
            "nav_milli": self.nav_milli,
            "ynab_before_milli": self.ynab_before_milli,
            "ynab_after_milli": self.ynab_after_milli,
            "ynab_uncleared_milli": self.ynab_uncleared_milli,
            "adjustment": self.adjustment.to_dict(),
        }


def brokerage_sync(
    client: YnabClient,
    budget_id: str,
    brokerage_account_id: str,
    *,
    ibkr: IbkrClient,
    ibkr_account_id: str | None = None,
    dry_run: bool = False,
    today: Date | None = None,
    budget_currency: str | None = None,
) -> BrokerageSyncResult:
    """Post the difference between the IBKR NAV and the YNAB account's working balance.

    ``ibkr_account_id`` defaults to the first account of the gateway session (v1).
    ``today`` (local date, default today; a ``datetime`` counts as its local day) dates the
    adjustment and its import_id ``IBKR:{delta}:{today}:1``.
    The NAV is in CHF, so a known ``budget_currency`` other than CHF is refused up front.

    Raises:
        ConfigError / ExternalServiceError: gateway problems (see :class:`IbkrClient`), or
            a non-CHF budget.
        AdjustmentRefused: NAV is zero or negative (YNAB is not even read).
        YnabError: reading the YNAB account or posting failed.
    """
    day = local_day(today)
    check_valuation_currency(budget_currency, NAV_CURRENCY)
    account_id = ibkr_account_id or ibkr.default_account_id()
    nav = ibkr.nav_chf(account_id)
    if not nav.is_finite() or nav <= 0:
        raise AdjustmentRefused(
            f"IBKR NAV for {account_id} is {nav}",
            reason="nav_not_positive",
            user_message=f"IBKR reported a net asset value of {nav} {NAV_CURRENCY} for account "
            f"{account_id}; nothing was posted. Check the gateway login and account access.",
        )
    nav_milli = decimal_to_milli(nav)
    balances = account_balances(client, budget_id, brokerage_account_id)
    adjustment = post_balance_adjustment(
        client,
        budget_id,
        brokerage_account_id,
        target_milli=nav_milli,
        current_milli=balances.working_milli,
        payee=PAYEE_IBKR,
        memo=f"NAV {nav:,.2f} {NAV_CURRENCY} (account {account_id})",
        prefix=PREFIX_IBKR,
        date=day,
        cleared="cleared",
        dry_run=dry_run,
    )
    log.info(
        "brokerage sync: account=%s nav=%s ynab=%s delta=%s outcome=%s",
        account_id,
        format_money(nav_milli),
        format_money(balances.working_milli),
        format_money(adjustment.delta_milli, sign=True),
        adjustment.outcome,
    )
    return BrokerageSyncResult(
        ibkr_account_id=account_id,
        nav=nav,
        nav_milli=nav_milli,
        ynab_before_milli=balances.working_milli,
        ynab_uncleared_milli=balances.uncleared_milli,
        adjustment=adjustment,
    )
