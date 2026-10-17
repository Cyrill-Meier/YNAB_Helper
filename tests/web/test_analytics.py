"""Pure analytics over mirror rows (DESIGN §9.3; F30, F65, C03, F98)."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pytest

from ynab_helper.web import analytics
from ynab_helper.web.analytics import (
    categories,
    spending,
    spending_window,
    subscriptions,
)

TODAY = date(2026, 10, 17)
CASH = "acct-cash"
CARD = "acct-card"
SAVINGS = "acct-savings"
TRACKING = "acct-crypto"
_ids = iter(range(1, 10_000))


def row(
    day: str,
    amount: float,
    *,
    category: str | None = "Groceries",
    payee: str = "Migros",
    account: str = CASH,
    on_budget: bool = True,
    transfer: str | None = None,
    import_id: str | None = None,
    parent: str | None = None,
    id: str | None = None,
) -> dict[str, Any]:
    return {
        "id": id or f"t{next(_ids)}",
        "parent_id": parent,
        "date": day,
        "amount": round(amount * 1000),
        "payee_name": payee,
        "memo": None,
        "category_name": category,
        "cleared": "cleared",
        "import_id": import_id,
        "transfer_account_id": transfer,
        "account_id": account,
        "account_name": account,
        "on_budget": 1 if on_budget else 0,
        "classification": "cash" if on_budget else "tracking",
    }


def _by_category(report) -> dict[str | None, Any]:
    return {c.category: c for c in report.categories}


# ── F30: what counts as spending ────────────────────────────────────────────


def test_F30_transfers_card_payments_adjustments_and_splits_are_not_spending():
    rows = [
        row("2026-09-03", -100.0, account=CARD),  # Migros on the Visa: real spending
        row("2026-09-20", -100.0, category=None, payee="Transfer : Visa", transfer=CARD),
        row(
            "2026-09-20", 100.0, category=None, payee="Transfer : Cash", transfer=CASH, account=CARD
        ),
        row("2026-09-21", -500.0, category=None, payee="Transfer : Savings", transfer=SAVINGS),
        row("2026-09-21", 500.0, category=None, account=SAVINGS, transfer=CASH),
        row(
            "2026-09-28",
            -2000.0,
            category=None,
            payee="Crypto Portfolio Adjustment",
            account=TRACKING,
            on_budget=False,
            import_id="CRYPTO:-2000000:2026-09-28:1",
        ),
        row(
            "2026-09-29",
            -12.0,
            category=None,
            payee="CSV Reconciliation",
            import_id="RECON:-12000:2026-09-29:1",
        ),
        row(
            "2026-09-29",
            -40.0,
            category=None,
            payee="IBKR Portfolio Adjustment",
            import_id="IBKR:-40000:2026-09-29:1",
        ),
        # A split Amazon purchase arrives as its two subtransaction lines.
        row("2026-09-10", -45.0, category="Shopping", payee="Amazon", parent="p1", id="s1"),
        row("2026-09-10", -15.0, category="Household", payee="Amazon", parent="p1", id="s2"),
        # Purchase + full refund in the same month nets to zero.
        row("2026-09-11", -80.0, category="Clothing", payee="Zalando"),
        row("2026-09-25", 80.0, category="Clothing", payee="Zalando"),
        # Income and YNAB's own balance entries are not spending.
        row("2026-09-25", 6000.0, category="Inflow: Ready to Assign", payee="Employer"),
        row("2026-09-01", -300.0, category=None, payee="Starting Balance", account=CARD),
    ]
    report = spending(rows, today=TODAY, months=1)
    cats = _by_category(report)
    assert set(cats) == {"Groceries", "Shopping", "Household", "Clothing"}
    assert cats["Groceries"].total == 100_000
    assert (cats["Shopping"].total, cats["Household"].total) == (45_000, 15_000)
    assert cats["Clothing"].total == 0
    assert report.total == 160_000  # v1 reported 2,840 for the same kind of data


def test_F30_tracking_accounts_never_count():
    rows = [
        row("2026-09-05", -50.0, account=TRACKING, on_budget=False, category="Groceries"),
        row("2026-09-05", -50.0),
    ]
    assert spending(rows, today=TODAY, months=1).total == 50_000


def test_refund_in_a_later_month_nets_in_that_month():
    rows = [
        row("2026-08-11", -80.0, category="Clothing"),
        row("2026-09-02", 30.0, category="Clothing"),
    ]
    report = spending(rows, today=TODAY, months=2)
    clothing = _by_category(report)["Clothing"]
    assert clothing.by_month == (80_000, -30_000)
    assert clothing.total == 50_000


def test_uncategorized_inflows_are_not_refunds():
    rows = [
        row("2026-09-02", -20.0, category=None),
        row("2026-09-03", 500.0, category=None, payee="Top-Up"),
    ]
    report = spending(rows, today=TODAY, months=1)
    assert _by_category(report)[None].total == 20_000


def test_account_filter():
    rows = [row("2026-09-02", -20.0, account=CASH), row("2026-09-03", -30.0, account=CARD)]
    assert spending(rows, today=TODAY, months=1, account_ids={CARD}).total == 30_000
    assert spending(rows, today=TODAY, months=1, account_ids=()).total == 50_000


# ── F65: the month window ───────────────────────────────────────────────────


def test_F65_window_is_n_complete_months_plus_month_to_date():
    window = spending_window(TODAY, 6)
    assert window.months == ("2026-04", "2026-05", "2026-06", "2026-07", "2026-08", "2026-09")
    assert window.start == date(2026, 4, 1)
    assert (window.mtd_month, window.mtd_start) == ("2026-10", date(2026, 10, 1))
    january = spending_window(date(2026, 1, 1), 2)
    assert january.months == ("2025-11", "2025-12")
    assert january.mtd_month == "2026-01"


def test_F65_average_uses_complete_months_and_mtd_is_separate():
    rows = [row(f"2026-{m:02d}-10", -3000.0) for m in range(4, 10)]
    rows.append(row("2026-10-02", -100.0))  # month-to-date (2 Oct)
    rows.append(row("2026-03-31", -999.0))  # before the window
    report = spending(rows, today=TODAY, months=6)
    assert report.per_month_total == (3_000_000,) * 6
    assert report.average() == 3_000_000  # v1: ~2,600 (averaged over the partial month)
    assert report.month_to_date_total == 100_000
    assert len(report.months) == 6  # v1 returned N+1 buckets


def test_F65_future_dated_rows_are_excluded_everywhere():
    rows = [
        row("2026-09-10", -30.0, category="Health"),
        row("2026-10-20", -30.0, category="Health", payee="Dentist"),  # after today
    ]
    report = spending(rows, today=TODAY, months=1)
    health = _by_category(report)["Health"]
    assert health.total == sum(health.by_month) == 30_000
    assert health.month_to_date == 0
    assert report.to_dict()["total"] == sum(report.to_dict()["per_month_total"])


def test_spending_to_dict_shape():
    data = spending([row("2026-09-10", -12.5)], today=TODAY, months=1).to_dict()
    assert data["months"] == ["2026-09"]
    assert data["month_to_date"] == {
        "month": "2026-10",
        "from": "2026-10-01",
        "through": "2026-10-17",
        "total": 0,
    }
    assert data["categories"] == [
        {
            "category": "Groceries",
            "total": 12_500,
            "average": 12_500,
            "by_month": [12_500],
            "month_to_date": 0,
            "count": 1,
        }
    ]


def test_months_are_clamped():
    assert len(spending([], today=TODAY, months=0).months) == 1
    assert len(spending([], today=TODAY, months=99).months) == analytics.MAX_SPENDING_MONTHS


# ── categories ──────────────────────────────────────────────────────────────


def test_categories_count_lines_and_net_spend():
    rows = [
        row("2026-09-10", -45.0, category="Shopping", parent="p1", id="s1"),
        row("2026-09-10", -15.0, category="Household", parent="p1", id="s2"),
        row("2026-09-11", -80.0, category="Shopping"),
        row("2026-09-12", 20.0, category="Shopping"),
        row("2026-09-13", -5.0, category=None),
        row("2026-09-14", -100.0, category=None, transfer=CARD),  # transfers are not listed
        row("2026-11-01", -9.0, category="Shopping"),  # future: counted, not spent
        row("2026-09-15", -7.0, category=None, account=TRACKING, on_budget=False),
    ]
    report = categories(rows, today=TODAY)
    by_name = {c.name: c for c in report.items}
    assert (by_name["Shopping"].count, by_name["Shopping"].spent) == (4, 105_000)
    assert (by_name["Household"].count, by_name["Household"].spent) == (1, 15_000)
    assert report.uncategorized == 1
    assert [c.name for c in report.items] == ["Household", "Shopping"]


# ── subscriptions (C03, F98) ────────────────────────────────────────────────


def _monthly(payees: list[str], amounts: list[float], start_month: int = 4) -> list[dict]:
    return [
        row(f"2026-{start_month + i:02d}-03", -amount, payee=payee, category="Subscriptions")
        for i, (payee, amount) in enumerate(zip(payees, amounts, strict=True))
    ]


def test_C03_case_scrambled_payees_are_ordered_by_date():
    rows = _monthly(
        ["Netflix", "Netflix", "Netflix", "NETFLIX", "NETFLIX", " netflix "],
        [15.90, 15.90, 15.90, 17.90, 17.90, 17.90],
    )
    rows.reverse()  # input order must not matter either
    report = subscriptions(rows, today=TODAY, lookback_months=13)
    (item,) = report.items
    assert (item.first, item.last) == ("2026-04-03", "2026-09-03")
    assert item.amount_latest == 17_900  # v1 showed the old 15.90
    assert [a for _, a in item.history] == [15_900] * 3 + [17_900] * 3
    assert item.price_changed and item.cadence == "monthly" and item.occurrences == 6


def test_amounts_within_five_cents_are_the_same_price():
    rows = _monthly(["Spotify"] * 4, [12.95, 12.99, 12.95, 12.91])
    (item,) = subscriptions(rows, today=TODAY).items
    assert not item.price_changed
    rows = _monthly(["Spotify"] * 4, [12.95, 12.95, 12.95, 13.95])
    (item,) = subscriptions(rows, today=TODAY).items
    assert item.price_changed


def test_cadence_buckets():
    weekly = [row(f"2026-09-{d:02d}", -9.0, payee="Gym") for d in (1, 8, 15, 22, 29)]
    biweekly = [
        row(f"2026-{m}", -50.0, payee="Cleaner") for m in ("07-01", "07-15", "07-29", "08-12")
    ]
    irregular = [row(f"2026-{m}", -50.0, payee="Random") for m in ("07-01", "07-20", "09-29")]
    report = subscriptions(weekly + biweekly + irregular, today=TODAY)
    assert [(s.payee_name, s.cadence) for s in report.items] == [
        ("Gym", "weekly"),
        ("Cleaner", "biweekly"),
    ]
    gym = report.items[0]
    assert gym.monthly_equivalent == round(9_000 * 52 / 12)
    assert gym.next_expected == "2026-10-06"


def test_F98_yearly_needs_two_charges():
    rows = [
        row("2025-10-05", -40.0, payee="Autobahn Vignette", category="Transport"),
        row("2026-10-04", -40.0, payee="Autobahn Vignette", category="Transport"),
    ]
    found = subscriptions(rows, today=TODAY, lookback_months=13)
    assert [(s.payee_name, s.cadence, s.occurrences) for s in found.items] == [
        ("Autobahn Vignette", "yearly", 2)
    ]
    assert found.yearly_detectable
    assert found.items[0].monthly_equivalent == round(40_000 / 12)
    assert found.items[0].next_expected == "2027-10-03"  # the 364-day gap again
    single = subscriptions(rows[1:], today=TODAY, lookback_months=13)
    assert single.items == ()


def _shift(day: date, months: int) -> date:
    first = analytics.add_months(day, months)
    return first.replace(day=min(day.day, 28))


@pytest.mark.parametrize("months_ago", range(13))
def test_F98_yearly_is_found_whatever_month_it_was_charged(months_ago):
    """v1 (and a plain 13-month window) only found a yearly charge while its newest
    occurrence was in the current or previous month."""
    newest = _shift(TODAY.replace(day=15), -months_ago)
    if newest > TODAY:
        newest = newest.replace(day=10)
    previous = _shift(newest, -12)
    rows = [
        row(previous.isoformat(), -190.0, payee="SBB Halbtax", category="Transport"),
        row(newest.isoformat(), -190.0, payee="SBB Halbtax", category="Transport"),
    ]
    report = subscriptions(rows, today=TODAY)  # the default look-back
    assert [(s.payee_name, s.cadence) for s in report.items] == [("SBB Halbtax", "yearly")]
    (item,) = report.items
    assert (item.first, item.last) == (previous.isoformat(), newest.isoformat())
    assert item.active


def test_F98_yearly_with_a_late_charge_in_the_window_and_three_years_of_history():
    rows = [
        row(day, -99.0, payee="Domain renewal", category="Internet")
        for day in ("2024-09-20", "2025-09-22", "2026-09-30")
    ]
    (item,) = subscriptions(rows, today=TODAY).items
    assert item.cadence == "yearly" and item.occurrences == 3


def test_F98_short_lookback_lists_yearly_charges_inside_it_only():
    rows = [
        row("2025-08-04", -40.0, payee="Vignette"),
        row("2026-08-04", -40.0, payee="Vignette"),
    ]
    inside = subscriptions(rows, today=TODAY, lookback_months=3)
    assert [s.cadence for s in inside.items] == ["yearly"]
    assert not inside.yearly_detectable  # not every yearly subscription is guaranteed
    outside = subscriptions(rows, today=TODAY, lookback_months=1)
    assert outside.items == ()
    assert analytics.subscription_history_start(TODAY, 13) == date(2024, 8, 1)
    assert outside.to_dict()["history_since"] == "2025-08-01"


def test_regular_shopping_at_varying_amounts_is_not_a_subscription():
    import random

    rng = random.Random(3)
    rows = []
    day = date(2025, 10, 3)
    while day <= TODAY:
        rows.append(row(day.isoformat(), -rng.uniform(40, 120), payee="Denner"))
        rows.append(row(day.isoformat(), -rng.choice([4.4, 8.8, 6.0, 12.6]), payee="SBB CFF FFS"))
        day += timedelta(days=14 + rng.randint(-1, 1))
    rows += _monthly(["Spotify"] * 6, [12.95] * 6)
    report = subscriptions(rows, today=TODAY)
    assert [s.payee_name for s in report.items] == ["Spotify"]
    assert report.to_dict()["monthly_total"] == 12_950  # groceries/tickets not counted


def test_foreign_currency_subscription_with_exchange_rate_noise_is_found():
    # A USD subscription billed in CHF: the amount moves with the rate every month.
    amounts = [18.12, 18.31, 17.95, 18.44, 18.02, 18.27, 18.19]
    rows = [
        row(f"2026-{3 + i:02d}-07", -amount, payee="GitHub", category="Software")
        for i, amount in enumerate(amounts)
    ]
    (item,) = subscriptions(rows, today=TODAY).items
    assert item.cadence == "monthly" and item.price_changed  # 0.05 tolerance for display


def test_a_few_price_steps_still_count_as_a_subscription():
    amounts = [9.0, 9.0, 9.0, 11.0, 11.0, 11.0, 11.0, 13.0, 13.0, 13.0]
    rows = [
        row(f"2025-{12 + i:02d}-05" if i == 0 else f"2026-{i:02d}-05", -a, payee="Cloud")
        for i, a in enumerate(amounts)
    ]
    (item,) = subscriptions(rows, today=TODAY).items
    assert item.amount_latest == 13_000 and item.price_changed


def test_monthly_still_needs_min_occurrences():
    rows = _monthly(["Netflix"] * 2, [15.90, 15.90])
    assert subscriptions(rows, today=TODAY, min_occurrences=3).items == ()
    assert len(subscriptions(rows, today=TODAY, min_occurrences=2).items) == 1


def test_subscriptions_exclude_transfers_adjustments_and_tracking_accounts():
    rows = []
    for m in range(4, 10):
        day = f"2026-{m:02d}-26"
        rows.append(row(day, -500.0, payee="Transfer : Savings", category=None, transfer=SAVINGS))
        rows.append(row(day, -100.0, payee="Transfer : Visa", category=None, transfer=CARD))
        rows.append(
            row(
                day,
                -50.0,
                payee="Crypto Portfolio Adjustment",
                account=TRACKING,
                on_budget=False,
                import_id=f"CRYPTO:-50000:{day}:1",
                category=None,
            )
        )
        rows.append(row(day, -12.0, payee="Bank fee", account=TRACKING, on_budget=False))
    assert subscriptions(rows, today=TODAY).items == ()


def test_split_lines_are_one_charge():
    rows = []
    for m in range(4, 10):
        parent = f"p{m}"
        day = f"2026-{m:02d}-05"
        rows.append(row(day, -30.0, payee="Swisscom", category="Phone", parent=parent))
        rows.append(row(day, -19.9, payee="Swisscom", category="TV", parent=parent))
    (item,) = subscriptions(rows, today=TODAY).items
    assert item.amount_latest == 49_900 and item.occurrences == 6
    assert item.categories == ("Phone", "TV")


def test_future_rows_and_rows_before_the_lookback_are_ignored():
    rows = _monthly(["Netflix"] * 6, [15.9] * 6)
    rows.append(row("2026-11-03", -15.9, payee="Netflix"))
    rows.append(row("2020-01-03", -15.9, payee="Netflix"))
    (item,) = subscriptions(rows, today=TODAY).items
    assert item.occurrences == 6 and item.last == "2026-09-03"


def test_inactive_subscription_is_flagged():
    rows = _monthly(["OldService"] * 4, [9.0] * 4, start_month=1)  # Jan..Apr
    (item,) = subscriptions(rows, today=TODAY).items
    assert not item.active
    assert subscriptions(rows, today=TODAY).to_dict()["monthly_total"] == 0


@pytest.mark.parametrize(
    ("payee", "key"), [("  Netflix   Inc ", "netflix inc"), ("NETFLIX", "netflix"), (None, "")]
)
def test_normalise_payee(payee, key):
    assert analytics.normalise_payee(payee) == key
