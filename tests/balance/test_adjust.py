"""Tests for ynab_helper.balance.adjust (DESIGN §6.6; finding F49)."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any

import pytest

from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.balance import adjust
from ynab_helper.balance.adjust import (
    AdjustmentResult,
    account_balances,
    adjustment_import_id,
    post_balance_adjustment,
    working_balance,
)
from ynab_helper.errors import ConfigError, YnabError, YnabRateLimited, YnabValidationError
from ynab_helper.ynab.models import YnabAccount

B = "budget-1"
A = "acct-crypto"
DAY = "2026-10-16"


@pytest.fixture
def fake() -> FakeYnab:
    f = FakeYnab()
    f.add_budget(B)
    f.add_account(B, A, "Crypto", type="otherAsset", on_budget=False)
    return f


def post(fake: FakeYnab, **overrides: Any) -> AdjustmentResult:
    kwargs: dict[str, Any] = {
        "target_milli": 50_000_000,
        "current_milli": 20_000_000,
        "payee": adjust.PAYEE_CRYPTO,
        "memo": "BTC 0.5@80,000",
        "prefix": adjust.PREFIX_CRYPTO,
        "date": DAY,
        "cleared": "cleared",
        "dry_run": False,
    }
    kwargs.update(overrides)
    return post_balance_adjustment(fake.client(), B, A, **kwargs)


def posts(fake: FakeYnab) -> list[Any]:
    return fake.calls_matching("POST")


@pytest.mark.parametrize("current", [50_000_000, 50_000_000 - 9, 50_000_000 + 9])
def test_difference_below_one_cent_is_in_sync(fake, current):
    result = post(fake, current_milli=current)
    assert result.outcome == adjust.OUTCOME_IN_SYNC
    assert result.in_sync and not result.posted
    assert result.import_id is None and result.ynab_tx_id is None
    assert posts(fake) == []


def test_exactly_one_cent_is_posted(fake):
    result = post(fake, target_milli=10, current_milli=0)
    assert result.outcome == adjust.OUTCOME_CREATED
    assert result.delta_milli == 10


def test_created_adjustment_payload_and_legacy_import_id(fake):
    result = post(fake, target_milli=0, current_milli=30_000_000)
    assert result.outcome == adjust.OUTCOME_CREATED
    assert result.delta_milli == -30_000_000
    assert result.import_id == "CRYPTO:-30000000:2026-10-16:1"
    tx = fake.tx(result.ynab_tx_id)
    assert tx["amount"] == -30_000_000
    assert tx["date"] == DAY
    assert tx["payee_name"] == "Crypto Portfolio Adjustment"
    assert tx["memo"] == "BTC 0.5@80,000"
    assert tx["cleared"] == "cleared"
    assert tx["approved"] is True
    assert tx["import_id"] == "CRYPTO:-30000000:2026-10-16:1"
    (call,) = posts(fake)
    (sent,) = call.body["transactions"]
    assert set(sent) == {
        "account_id",
        "date",
        "amount",
        "payee_name",
        "memo",
        "cleared",
        "approved",
        "import_id",
    }


@pytest.mark.parametrize(
    ("prefix", "payee", "cleared"),
    [
        ("CRYPTO", "Crypto Portfolio Adjustment", "cleared"),
        ("IBKR", "IBKR Portfolio Adjustment", "cleared"),
        ("RECON", "CSV Reconciliation", "reconciled"),
    ],
)
def test_import_id_format_is_v1_identical(fake, prefix, payee, cleared):
    result = post(
        fake, prefix=prefix, payee=payee, cleared=cleared, target_milli=1234, current_milli=0
    )
    # v1: f"{PREFIX}:{delta_milli}:{date}:1"
    assert result.import_id == f"{prefix}:1234:{DAY}:1"
    tx = fake.tx(result.ynab_tx_id)
    assert (tx["payee_name"], tx["cleared"]) == (payee, cleared)


def test_adjustment_import_id_helper():
    assert (
        adjustment_import_id("RECON", -4950000, date(2026, 3, 31)) == "RECON:-4950000:2026-03-31:1"
    )
    with pytest.raises(ValueError):
        adjustment_import_id("YNAB", 1, DAY)
    with pytest.raises(ValueError):
        adjustment_import_id("RECON", -(10**30), DAY)  # longer than 36 characters


# ── a datetime passed as the day (review: v1 import_id format) ─────────────


def test_datetime_day_is_reduced_to_its_date_in_the_import_id():
    """datetime subclasses date; v2 leaked the time part: IBKR:1234:2026-10-16T12:00:00:1."""
    assert adjustment_import_id("IBKR", 1234, datetime(2026, 10, 16, 12, 0)) == (
        "IBKR:1234:2026-10-16:1"
    )


def test_datetime_date_posts_a_v1_shaped_adjustment(fake):
    result = post(fake, date=datetime(2026, 10, 16, 9, 30), prefix=adjust.PREFIX_IBKR)
    assert result.outcome == "created"
    assert result.date == DAY
    assert result.import_id == "IBKR:30000000:2026-10-16:1"
    assert fake.tx(result.ynab_tx_id)["date"] == DAY


def test_aware_datetime_is_taken_in_local_time():
    late_utc = datetime(2026, 10, 16, 23, 30, tzinfo=UTC)
    assert adjust.local_day(late_utc) == late_utc.astimezone().date()
    plus_14 = datetime(2026, 10, 17, 0, 30, tzinfo=timezone(timedelta(hours=14)))
    assert adjust.local_day(plus_14) == plus_14.astimezone().date()


def test_local_day_defaults_to_local_today(monkeypatch):
    monkeypatch.setattr(adjust, "local_today", lambda: date(2027, 1, 2))
    assert adjust.local_day() == date(2027, 1, 2)
    assert adjust.local_day(None) == date(2027, 1, 2)
    assert adjust.local_day(date(2026, 3, 31)) == date(2026, 3, 31)
    assert adjust.local_day("2026-03-31") == date(2026, 3, 31)


@pytest.mark.parametrize("bad", ["2026-10-16T09:30:00", "20261016", "2026-W42-5", "", "2026-02-30"])
def test_local_day_accepts_only_plain_iso_days(bad):
    with pytest.raises(ValueError):
        adjust.local_day(bad)


def test_local_day_rejects_other_types():
    with pytest.raises(TypeError):
        adjust.local_day(20261016)  # type: ignore[arg-type]


def test_duplicate_is_an_outcome_not_an_error(fake):
    first = post(fake)
    second = post(fake)
    assert first.outcome == adjust.OUTCOME_CREATED
    assert second.outcome == adjust.OUTCOME_DUPLICATE
    assert second.import_id == first.import_id
    assert second.ynab_tx_id is None
    assert len(fake.transactions(B, account_id=A)) == 1


def test_duplicate_of_a_deleted_adjustment(fake):
    first = post(fake)
    fake.user_delete_transaction(first.ynab_tx_id)
    assert post(fake).outcome == adjust.OUTCOME_DUPLICATE


def test_409_is_a_duplicate_only_if_ynab_holds_the_import_id(fake):
    existing = fake.add_transaction(
        B, A, amount=30_000_000, date=DAY, import_id="CRYPTO:30000000:2026-10-16:1"
    )
    fake.fail_next("POST", r"/budgets/[^/]+/transactions", 409)
    result = post(fake)
    assert result.outcome == adjust.OUTCOME_DUPLICATE
    assert result.ynab_tx_id == existing


def test_409_without_existing_transaction_raises(fake):
    fake.fail_next("POST", r"/budgets/[^/]+/transactions", 409)
    with pytest.raises(YnabValidationError):
        post(fake)
    assert fake.transactions(B, account_id=A) == []


def test_dry_run_writes_nothing(fake):
    result = post(fake, dry_run=True)
    assert result.outcome == adjust.OUTCOME_DRY_RUN
    assert result.import_id == "CRYPTO:30000000:2026-10-16:1"
    assert result.delta_milli == 30_000_000
    assert fake.calls == []


def test_memo_is_clipped_to_200_characters(fake):
    result = post(fake, memo="x" * 250)
    assert result.memo == "x" * 197 + "..."
    assert len(fake.tx(result.ynab_tx_id)["memo"]) == 200


def test_empty_memo_is_not_sent(fake):
    result = post(fake, memo="")
    assert result.memo is None
    (call,) = posts(fake)
    assert "memo" not in call.body["transactions"][0]


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"prefix": "YNAB"}, ValueError),
        ({"date": "2026-13-01"}, ValueError),
        ({"date": "16.10.2026"}, ValueError),
        ({"date": "2026-10-16T09:30:00"}, ValueError),
        ({"date": None}, ValueError),
        ({"cleared": "maybe"}, ValueError),
        ({"payee": " "}, ValueError),
        ({"target_milli": True}, TypeError),
        ({"current_milli": 1.5}, TypeError),
    ],
)
def test_invalid_arguments_send_nothing(fake, overrides, error):
    with pytest.raises(error):
        post(fake, **overrides)
    assert fake.calls == []


def test_rate_limit_propagates(fake):
    fake.fail_next("POST", None, 429)
    with pytest.raises(YnabRateLimited):
        post(fake)


def test_audit_line_for_created_adjustment(fake, caplog):
    with caplog.at_level(logging.INFO, logger="ynab_helper.audit"):
        result = post(fake)
    lines = [r.getMessage() for r in caplog.records if r.name == "ynab_helper.audit"]
    assert len(lines) == 1
    assert result.import_id in lines[0] and result.ynab_tx_id in lines[0]


def test_to_dict_is_plain_data(fake):
    data = post(fake).to_dict()
    assert data["outcome"] == "created"
    assert data["delta_milli"] == 30_000_000


# ── balances (F49) ─────────────────────────────────────────────────────────


def test_F49_working_balance_includes_uncleared(fake):
    fake.add_account(B, "checking", "Checking")
    fake.add_transaction(B, A, amount=20_000_000, date="2026-10-01", cleared="reconciled")
    # A transfer into the tracking account: its side here is typically still uncleared.
    fake.add_transaction(B, "checking", amount=-5_000_000, date="2026-10-10", transfer_account_id=A)
    balances = account_balances(fake.client(), B, A)
    assert balances.cleared_milli == 20_000_000
    assert balances.uncleared_milli == 5_000_000
    assert balances.working_milli == 25_000_000
    assert working_balance(fake.client(), B, A) == 25_000_000


class _StubClient:
    def __init__(self, account: dict[str, Any]) -> None:
        self._account = account

    def get_account(self, budget_id: str, account_id: str) -> YnabAccount:
        return YnabAccount.from_api(self._account)


@pytest.mark.parametrize(
    "fields",
    [
        {"balance": 5000},
        {"balance": 5000, "cleared_balance": 5000},
        {},
    ],
)
def test_F49_missing_balance_fields_raise_instead_of_zero(fields):
    client = _StubClient({"id": A, "name": "Crypto", **fields})
    with pytest.raises(YnabError):
        working_balance(client, B, A)  # type: ignore[arg-type]


def test_inconsistent_balances_raise():
    client = _StubClient(
        {"id": A, "name": "Crypto", "balance": 1, "cleared_balance": 5, "uncleared_balance": 5}
    )
    with pytest.raises(YnabError):
        working_balance(client, B, A)  # type: ignore[arg-type]


def test_deleted_account_is_a_config_error():
    client = _StubClient(
        {
            "id": A,
            "name": "Crypto",
            "deleted": True,
            "balance": 0,
            "cleared_balance": 0,
            "uncleared_balance": 0,
        }
    )
    with pytest.raises(ConfigError):
        working_balance(client, B, A)  # type: ignore[arg-type]


def test_check_valuation_currency():
    adjust.check_valuation_currency(None, "CHF")
    adjust.check_valuation_currency(" chf ", "CHF")
    with pytest.raises(ConfigError):
        adjust.check_valuation_currency("EUR", "CHF")
