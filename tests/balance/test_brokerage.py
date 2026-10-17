"""Tests for ynab_helper.balance.brokerage (DESIGN §6.6; findings F49, F80)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest

from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.balance.adjust import AdjustmentRefused
from ynab_helper.balance.brokerage import brokerage_sync
from ynab_helper.balance.ibkr import IbkrClient
from ynab_helper.errors import ConfigError

B = "budget-1"
A = "acct-ibkr"
TODAY = date(2026, 10, 16)
ACCOUNTS = [
    {"id": "U1234567", "accountId": "U1234567", "currency": "CHF"},
    {"id": "U7654321", "accountId": "U7654321", "currency": "CHF"},
]


class Gateway:
    """Fake ``request`` callable for IbkrClient, routed by API path."""

    def __init__(self, navs: dict[str, Any]) -> None:
        self.navs = navs
        self.paths: list[str] = []

    def __call__(self, method: str, url: str, **kwargs: Any) -> Any:
        path = url.split("/v1/api", 1)[1]
        self.paths.append(path)
        if path == "/portfolio/accounts":
            return ACCOUNTS
        account = path.split("/")[2]
        return {"BASE": {"netliquidationvalue": self.navs[account]}}


@pytest.fixture
def fake() -> FakeYnab:
    f = FakeYnab()
    f.add_budget(B)
    f.add_account(B, A, "IBKR", type="otherAsset", on_budget=False)
    f.add_account(B, "checking", "Checking")
    return f


def ibkr(**navs: Any) -> tuple[IbkrClient, Gateway]:
    gateway = Gateway({"U1234567": Decimal("123456.78"), **navs})
    return IbkrClient("https://localhost:5050", request=gateway), gateway


def test_posts_nav_difference_with_v1_shape(fake):
    fake.add_transaction(B, A, amount=100_000_000, date="2026-10-01", cleared="cleared")
    client, gateway = ibkr()
    result = brokerage_sync(fake.client(), B, A, ibkr=client, today=TODAY)
    assert gateway.paths == ["/portfolio/accounts", "/portfolio/U1234567/ledger"]
    assert result.ibkr_account_id == "U1234567"
    assert result.nav == Decimal("123456.78")
    assert result.nav_milli == 123_456_780
    assert result.ynab_before_milli == 100_000_000
    adj = result.adjustment
    assert adj.outcome == "created"
    assert adj.delta_milli == 23_456_780
    assert adj.import_id == "IBKR:23456780:2026-10-16:1"
    tx = fake.tx(adj.ynab_tx_id)
    assert tx["payee_name"] == "IBKR Portfolio Adjustment"
    assert tx["memo"] == "NAV 123,456.78 CHF (account U1234567)"
    assert tx["cleared"] == "cleared"
    assert tx["date"] == "2026-10-16"
    assert result.ynab_after_milli == 123_456_780
    assert result.to_dict()["nav"] == "123456.78"


@pytest.mark.parametrize("nav", [Decimal("0"), Decimal("-5.00")])
def test_nav_not_positive_never_posts_and_never_reads_ynab(fake, nav):
    client, _ = ibkr(U1234567=nav)
    with pytest.raises(AdjustmentRefused) as info:
        brokerage_sync(fake.client(), B, A, ibkr=client, today=TODAY)
    assert info.value.reason == "nav_not_positive"
    assert "nothing was posted" in info.value.user_message
    assert fake.calls == []


def test_F49_uncleared_transfer_is_not_counted_twice(fake):
    fake.add_transaction(B, A, amount=118_456_780, date="2026-10-01", cleared="reconciled")
    fake.add_transaction(B, "checking", amount=-5_000_000, date="2026-10-10", transfer_account_id=A)
    client, _ = ibkr()
    result = brokerage_sync(fake.client(), B, A, ibkr=client, today=TODAY)
    assert result.ynab_before_milli == 123_456_780
    assert result.ynab_uncleared_milli == 5_000_000
    assert result.adjustment.outcome == "in_sync"  # v1 posted +5,000.00 here
    assert fake.calls_matching("POST") == []


def test_explicit_account_id(fake):
    client, gateway = ibkr(U7654321=Decimal("10.00"))
    result = brokerage_sync(
        fake.client(), B, A, ibkr=client, ibkr_account_id="U7654321", today=TODAY
    )
    assert gateway.paths == ["/portfolio/accounts", "/portfolio/U7654321/ledger"]
    assert result.adjustment.delta_milli == 10_000


def test_unknown_account_id_is_a_config_error(fake):
    client, _ = ibkr()
    with pytest.raises(ConfigError):
        brokerage_sync(fake.client(), B, A, ibkr=client, ibkr_account_id="U0000000")
    assert fake.calls == []


def test_dry_run_does_not_post(fake):
    client, _ = ibkr()
    result = brokerage_sync(fake.client(), B, A, ibkr=client, dry_run=True, today=TODAY)
    assert result.adjustment.outcome == "dry_run"
    assert result.adjustment.import_id == "IBKR:123456780:2026-10-16:1"
    assert fake.calls_matching("POST") == []


def test_same_day_rerun_is_in_sync(fake):
    client, _ = ibkr()
    brokerage_sync(fake.client(), B, A, ibkr=client, today=TODAY)
    again = brokerage_sync(fake.client(), B, A, ibkr=client, today=TODAY)
    assert again.adjustment.outcome == "in_sync"
    assert len(fake.calls_matching("POST")) == 1


def test_non_chf_budget_is_refused_before_any_request(fake):
    client, gateway = ibkr()
    with pytest.raises(ConfigError):
        brokerage_sync(fake.client(), B, A, ibkr=client, budget_currency="EUR")
    assert gateway.paths == [] and fake.calls == []


def test_datetime_today_keeps_the_v1_import_id(fake):
    client, _ = ibkr()
    result = brokerage_sync(fake.client(), B, A, ibkr=client, today=datetime(2026, 10, 16, 9, 30))
    assert result.adjustment.import_id == "IBKR:123456780:2026-10-16:1"
    assert fake.tx(result.adjustment.ynab_tx_id)["date"] == "2026-10-16"


def test_invalid_today_is_rejected_before_any_request(fake):
    client, gateway = ibkr()
    with pytest.raises(ValueError):
        brokerage_sync(fake.client(), B, A, ibkr=client, today="16.10.2026")  # type: ignore[arg-type]
    assert gateway.paths == [] and fake.calls == []
