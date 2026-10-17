"""Spending, categories and subscriptions from the YNAB mirror (docs/DESIGN.md §9.3).

Pure functions: no I/O, no clock (``today`` is a parameter). The input rows are
:meth:`ynab_helper.storage.mirror.MirrorStore.analytics_rows` with ``expand_splits=True``:
live transactions joined with their account, a split parent replaced by its live
subtransactions (``parent_id`` set, payee/memo falling back to the parent's). Keys used:
``id, parent_id, date, amount, payee_name, category_name, import_id, transfer_account_id,
account_id, account_name, on_budget``. Amounts are integer milliunits (negative =
outflow); every amount returned here is integer milliunits too.

What counts as spending (F30) — a *spending line* is a line that

* lives in an **on-budget** account (tracking accounts hold valuations, not spending),
* is **not a transfer** (``transfer_account_id`` set: savings moves, credit-card payments),
* is **not one of the importer's balance adjustments** (import_id ``RECON:``, ``CRYPTO:``
  or ``IBKR:``, or their payees) nor a YNAB system entry (``Starting Balance``,
  ``Reconciliation Balance Adjustment``),
* is **not income** (category ``Inflow: Ready to Assign``),
* is dated **today or earlier** (F65: future-dated rows are never counted).

Outflows add to their category; inflows in a *named* category are refunds and are netted
against it (a month's net value can therefore be negative). Uncategorized inflows are not
refunds of anything and are ignored.

The spending window (F65) is N **complete** calendar months before the current month plus
the current month-to-date, reported separately; averages use the complete months only.

Subscriptions group outflow charges by normalised payee (case-folded, whitespace
collapsed), sort every group by date in Python (C03), and classify the median gap between
charges as weekly 6–8, biweekly 12–16, monthly 25–35 or yearly 350–380 days (v1's
buckets). A group is listed when its newest charge falls inside the look-back window.
Weekly to monthly cadences are decided on the charges inside the window and need
``min_occurrences`` of them. A yearly charge appears only once in a one-year window, so
the yearly cadence is decided over the window plus :data:`YEARLY_HISTORY_MONTHS` more
months of history and needs two charges (F98: v1 — and a plain 13-month window — missed a
yearly subscription for most of the year). The amounts must be steady: at most one price
change per three charges, where amounts within 5 % (at least 0.05 currency units) count as
the same price, so card charges in a foreign currency pass while supermarket visits do
not. ``price_changed`` uses the strict 0.05-unit tolerance.

Categories count *transactions* exactly as ``GET /api/transactions?category=`` lists them
(a split counts once per category it contains), and ``uncategorized`` counts what
``category=__none__`` lists: on-budget, non-transfer transactions without a category (or a
split with an uncategorized line) — including future-dated ones and the app's own
adjustments, which YNAB also shows as uncategorized.
"""

from __future__ import annotations

import itertools
import statistics
from collections import defaultdict
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Final

from ynab_helper.balance.adjust import PAYEE_CRYPTO, PAYEE_IBKR, PAYEE_RECON

__all__ = [
    "ADJUSTMENT_IMPORT_PREFIXES",
    "ADJUSTMENT_PAYEES",
    "CADENCES",
    "INFLOW_CATEGORY",
    "MAX_LOOKBACK_MONTHS",
    "MAX_SPENDING_MONTHS",
    "PRICE_TOLERANCE",
    "SYSTEM_PAYEES",
    "YEARLY_HISTORY_MONTHS",
    "YEARLY_MIN_LOOKBACK",
    "Cadence",
    "CategoriesReport",
    "CategoryInfo",
    "CategorySpending",
    "SpendingReport",
    "Subscription",
    "SubscriptionsReport",
    "add_months",
    "categories",
    "is_adjustment",
    "is_spending_line",
    "month_key",
    "normalise_payee",
    "spending",
    "spending_window",
    "subscription_history_start",
    "subscriptions",
]

Row = Mapping[str, Any]

INFLOW_CATEGORY: Final = "Inflow: Ready to Assign"
ADJUSTMENT_IMPORT_PREFIXES: Final = ("RECON:", "CRYPTO:", "IBKR:")
ADJUSTMENT_PAYEES: Final = frozenset({PAYEE_RECON, PAYEE_CRYPTO, PAYEE_IBKR})
#: Entries YNAB itself creates; they are balances, not spending.
SYSTEM_PAYEES: Final = frozenset({"Starting Balance", "Reconciliation Balance Adjustment"})
#: 0.05 currency units: amounts closer than this are the same subscription price.
PRICE_TOLERANCE: Final = 50
#: For the steadiness check, amounts within this share of each other are the same price
#: (exchange-rate noise of a subscription billed in a foreign currency).
SIMILAR_AMOUNT_SHARE: Final = 0.05
MAX_SPENDING_MONTHS: Final = 24
MAX_LOOKBACK_MONTHS: Final = 36
#: Look-back from which every active yearly subscription (its newest charge within the
#: last year, with a month of slack for a late charge) is listed.
YEARLY_MIN_LOOKBACK: Final = 13
#: Months of history before the look-back used to find the previous yearly charge
#: (yearly gaps are up to 380 days).
YEARLY_HISTORY_MONTHS: Final = 13


@dataclass(frozen=True, slots=True)
class Cadence:
    """A subscription cadence: median gap ``min_days``–``max_days``; ``per_month`` converts
    one charge to a monthly cost."""

    name: str
    min_days: int
    max_days: int
    per_month: float


CADENCES: Final = (
    Cadence("weekly", 6, 8, 52 / 12),
    Cadence("biweekly", 12, 16, 26 / 12),
    Cadence("monthly", 25, 35, 1.0),
    Cadence("yearly", 350, 380, 1 / 12),
)
_CADENCE_ORDER: Final = {c.name: i for i, c in enumerate(CADENCES)}


# ── helpers ──────────────────────────────────────────────────────────────────


def add_months(day: date, months: int) -> date:
    """The first day of the month ``months`` months after ``day``'s month."""
    index = day.year * 12 + (day.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def month_key(day: date | str) -> str:
    """``YYYY-MM`` of a date (or an ISO date string)."""
    return day.isoformat()[:7] if isinstance(day, date) else str(day)[:7]


def normalise_payee(payee: str | None) -> str:
    """Grouping key of a payee: case-folded with whitespace trimmed and collapsed."""
    return " ".join(str(payee or "").split()).casefold()


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _category(row: Row) -> str | None:
    name = _text(row.get("category_name"))
    return name or None


def is_adjustment(row: Row) -> bool:
    """A balance adjustment posted by this app (reconcile, crypto, IBKR) or by YNAB."""
    import_id = row.get("import_id")
    if isinstance(import_id, str) and import_id.startswith(ADJUSTMENT_IMPORT_PREFIXES):
        return True
    payee = _text(row.get("payee_name"))
    return payee in ADJUSTMENT_PAYEES or payee in SYSTEM_PAYEES


def _valid(row: Row, today: date) -> bool:
    day = row.get("date")
    amount = row.get("amount")
    return (
        isinstance(day, str)
        and len(day) == 10
        and day <= today.isoformat()
        and isinstance(amount, int)
        and not isinstance(amount, bool)
    )


def is_spending_line(row: Row, today: date) -> bool:
    """Whether a line can count as spending (see the module docstring)."""
    return (
        _valid(row, today)
        and bool(row.get("on_budget"))
        and not row.get("transfer_account_id")
        and not is_adjustment(row)
        and _category(row) != INFLOW_CATEGORY
    )


def _in_accounts(row: Row, account_ids: Collection[str] | None) -> bool:
    return not account_ids or row.get("account_id") in account_ids


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, int(value)))


# ── spending ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class SpendingWindow:
    """``months`` complete months (oldest first) starting ``start``; the month-to-date runs
    from ``mtd_start`` to ``today``."""

    today: date
    months: tuple[str, ...]
    start: date
    mtd_month: str
    mtd_start: date


def spending_window(today: date, months: int) -> SpendingWindow:
    """N complete calendar months before ``today``'s month, plus the month-to-date."""
    count = _clamp(months, 1, MAX_SPENDING_MONTHS)
    mtd_start = today.replace(day=1)
    start = add_months(mtd_start, -count)
    keys = tuple(month_key(add_months(start, i)) for i in range(count))
    return SpendingWindow(today, keys, start, month_key(today), mtd_start)


@dataclass(frozen=True, slots=True)
class CategorySpending:
    """Net spending of one category (positive = money spent); ``category`` None means
    uncategorized."""

    category: str | None
    by_month: tuple[int, ...]
    month_to_date: int
    count: int

    @property
    def total(self) -> int:
        return sum(self.by_month)

    def average(self) -> int:
        return round(self.total / len(self.by_month)) if self.by_month else 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "total": self.total,
            "average": self.average(),
            "by_month": list(self.by_month),
            "month_to_date": self.month_to_date,
            "count": self.count,
        }


@dataclass(frozen=True, slots=True)
class SpendingReport:
    """Spending per category and month (milliunits, positive = money out)."""

    window: SpendingWindow
    categories: tuple[CategorySpending, ...]

    @property
    def months(self) -> tuple[str, ...]:
        return self.window.months

    @property
    def per_month_total(self) -> tuple[int, ...]:
        return tuple(
            sum(c.by_month[i] for c in self.categories) for i in range(len(self.window.months))
        )

    @property
    def total(self) -> int:
        return sum(self.per_month_total)

    @property
    def month_to_date_total(self) -> int:
        return sum(c.month_to_date for c in self.categories)

    def average(self) -> int:
        return round(self.total / len(self.window.months)) if self.window.months else 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "today": self.window.today.isoformat(),
            "months": list(self.window.months),
            "per_month_total": list(self.per_month_total),
            "total": self.total,
            "average": self.average(),
            "month_to_date": {
                "month": self.window.mtd_month,
                "from": self.window.mtd_start.isoformat(),
                "through": self.window.today.isoformat(),
                "total": self.month_to_date_total,
            },
            "categories": [c.to_dict() for c in self.categories],
        }


def spending(
    rows: Iterable[Row],
    *,
    today: date,
    months: int = 6,
    account_ids: Collection[str] | None = None,
) -> SpendingReport:
    """Net spending per category over N complete months plus the month-to-date (F30, F65)."""
    window = spending_window(today, months)
    index = {key: i for i, key in enumerate(window.months)}
    start = window.start.isoformat()
    mtd_start = window.mtd_start.isoformat()
    by_month: dict[str | None, list[int]] = defaultdict(lambda: [0] * len(window.months))
    mtd: dict[str | None, int] = defaultdict(int)
    counts: dict[str | None, int] = defaultdict(int)
    for row in rows:
        if not _in_accounts(row, account_ids) or not is_spending_line(row, today):
            continue
        day: str = row["date"]
        if day < start:
            continue
        category = _category(row)
        amount: int = row["amount"]
        if amount < 0:
            value = -amount
        elif amount > 0 and category is not None:
            value = -amount  # a refund nets against its category
        else:
            continue
        if day >= mtd_start:
            mtd[category] += value
        else:
            by_month[category][index[day[:7]]] += value
        counts[category] += 1
    names = set(by_month) | set(mtd)
    items = [
        CategorySpending(
            category=name,
            by_month=tuple(by_month[name]) if name in by_month else (0,) * len(window.months),
            month_to_date=mtd.get(name, 0),
            count=counts[name],
        )
        for name in names
    ]
    items.sort(key=lambda c: (-c.total, -c.month_to_date, c.category is None, c.category or ""))
    return SpendingReport(window, tuple(items))


# ── categories ───────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class CategoryInfo:
    """A category seen in the mirror: ``count`` transactions the transactions list shows
    for it, ``spent`` its net spending over all time (§9.3 rules; positive = money out)."""

    name: str
    count: int
    spent: int

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "count": self.count, "spent": self.spent}


@dataclass(frozen=True, slots=True)
class CategoriesReport:
    items: tuple[CategoryInfo, ...]
    uncategorized: int  # transactions ``category=__none__`` lists

    def to_dict(self) -> dict[str, Any]:
        return {"items": [c.to_dict() for c in self.items], "uncategorized": self.uncategorized}


def _transaction_key(row: Row) -> str:
    """The transaction a line belongs to (a split's lines share their parent)."""
    return str(row.get("parent_id") or row.get("id"))


def categories(
    rows: Iterable[Row], *, today: date, account_ids: Collection[str] | None = None
) -> CategoriesReport:
    """Every category name present (split lines included), with count and net spend.

    ``count`` is the number of transactions ``GET /api/transactions?category=<name>``
    lists (a split once per category it contains; future-dated and categorized transfers
    included, as the list shows them); ``spent`` follows the spending rules (no future
    lines, no transfers). ``uncategorized`` is what ``category=__none__`` lists: on-budget
    transactions that are not transfers and have an uncategorized line (YNAB never
    categorizes tracking-account entries; the app's adjustments are included, YNAB shows
    them as uncategorized too).
    """
    members: dict[str, set[str]] = defaultdict(set)
    spent: dict[str, int] = defaultdict(int)
    uncategorized: set[str] = set()
    for row in rows:
        if not _in_accounts(row, account_ids):
            continue
        name = _category(row)
        if name is None:
            if row.get("on_budget") and not row.get("transfer_account_id"):
                uncategorized.add(_transaction_key(row))
            continue
        members[name].add(_transaction_key(row))
        if is_spending_line(row, today):
            spent[name] += -row["amount"]
    items = sorted(
        (CategoryInfo(name, len(keys), spent.get(name, 0)) for name, keys in members.items()),
        key=lambda c: c.name.casefold(),
    )
    return CategoriesReport(tuple(items), len(uncategorized))


# ── subscriptions ────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class _Charge:
    id: str
    date: str
    amount: int  # positive milliunits (money out)
    payee: str
    categories: tuple[str, ...]
    account_name: str | None


@dataclass(frozen=True, slots=True)
class Subscription:
    """A recurring outflow (amounts positive milliunits)."""

    payee_name: str
    cadence: str
    median_days: float
    occurrences: int
    first: str
    last: str
    amount_min: int
    amount_max: int
    amount_latest: int
    history: tuple[tuple[str, int], ...]  # (date, amount), oldest first
    price_changed: bool
    categories: tuple[str, ...]
    accounts: tuple[str, ...]
    next_expected: str
    active: bool
    monthly_equivalent: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "payee_name": self.payee_name,
            "cadence": self.cadence,
            "median_days": self.median_days,
            "occurrences": self.occurrences,
            "first": self.first,
            "last": self.last,
            "amount_min": self.amount_min,
            "amount_max": self.amount_max,
            "amount_latest": self.amount_latest,
            "history": [{"date": d, "amount": a} for d, a in self.history],
            "price_changed": self.price_changed,
            "categories": list(self.categories),
            "accounts": list(self.accounts),
            "next_expected": self.next_expected,
            "active": self.active,
            "monthly_equivalent": self.monthly_equivalent,
        }


@dataclass(frozen=True, slots=True)
class SubscriptionsReport:
    today: date
    since: date
    lookback_months: int
    min_occurrences: int
    items: tuple[Subscription, ...]

    @property
    def history_since(self) -> date:
        """The oldest charge date looked at (yearly cadences need the extra history)."""
        return subscription_history_start(self.today, self.lookback_months)

    @property
    def yearly_detectable(self) -> bool:
        """Every active yearly subscription is listed (its newest charge is at most a year
        and a month old, so it lies inside the window). With a shorter look-back only the
        yearly subscriptions charged inside it are."""
        return self.lookback_months >= YEARLY_MIN_LOOKBACK

    def to_dict(self) -> dict[str, Any]:
        return {
            "today": self.today.isoformat(),
            "since": self.since.isoformat(),
            "history_since": self.history_since.isoformat(),
            "lookback_months": self.lookback_months,
            "min_occurrences": self.min_occurrences,
            "yearly_detectable": self.yearly_detectable,
            "monthly_total": sum(s.monthly_equivalent for s in self.items if s.active),
            "items": [s.to_dict() for s in self.items],
        }


def _charges(rows: Iterable[Row], *, today: date, since: str, account_ids) -> list[_Charge]:
    """Outflow charges: the lines of one split are merged back into one charge."""
    lines: dict[str, list[Row]] = defaultdict(list)
    for row in rows:
        if not _in_accounts(row, account_ids) or not _valid(row, today):
            continue
        if row["date"] < since or not row.get("on_budget"):
            continue
        if row.get("transfer_account_id") or is_adjustment(row):
            continue
        if _category(row) == INFLOW_CATEGORY:
            continue
        lines[str(row.get("parent_id") or row.get("id"))].append(row)
    charges = []
    for charge_id, parts in lines.items():
        amount = sum(int(p["amount"]) for p in parts)
        payee = _text(parts[0].get("payee_name"))
        if amount >= 0 or not payee:
            continue
        cats = tuple(sorted({c for p in parts if (c := _category(p)) is not None}))
        charges.append(
            _Charge(
                id=charge_id,
                date=parts[0]["date"],
                amount=-amount,
                payee=payee,
                categories=cats,
                account_name=parts[0].get("account_name"),
            )
        )
    return charges


def _cadence_of(median_days: float) -> Cadence | None:
    for cadence in CADENCES:
        if cadence.min_days <= median_days <= cadence.max_days:
            return cadence
    return None


def _median_gap(charges: list[_Charge]) -> float | None:
    """Median days between consecutive charges (sorted by date); None for fewer than 2."""
    if len(charges) < 2:
        return None
    days = [date.fromisoformat(c.date) for c in charges]
    return float(statistics.median((b - a).days for a, b in itertools.pairwise(days)))


def _similar(a: int, b: int) -> bool:
    return abs(a - b) <= max(PRICE_TOLERANCE, round(max(a, b) * SIMILAR_AMOUNT_SHARE))


def _steady(amounts: list[int]) -> bool:
    """At most one price change per three charges (at least one change is allowed)."""
    changes = sum(1 for a, b in itertools.pairwise(amounts) if not _similar(a, b))
    return changes <= max(1, (len(amounts) - 1) // 3)


def subscription_history_start(today: date, lookback_months: int) -> date:
    """The oldest date :func:`subscriptions` looks at: the look-back window plus
    :data:`YEARLY_HISTORY_MONTHS` (to find the previous charge of a yearly subscription)."""
    lookback = _clamp(lookback_months, 1, MAX_LOOKBACK_MONTHS)
    return add_months(today.replace(day=1), -(lookback + YEARLY_HISTORY_MONTHS))


def subscriptions(
    rows: Iterable[Row],
    *,
    today: date,
    lookback_months: int = YEARLY_MIN_LOOKBACK,
    min_occurrences: int = 3,
    account_ids: Collection[str] | None = None,
) -> SubscriptionsReport:
    """Recurring outflows whose newest charge is within the look-back window (C03, F30,
    F98). ``rows`` should reach back to :func:`subscription_history_start`."""
    lookback = _clamp(lookback_months, 1, MAX_LOOKBACK_MONTHS)
    minimum = _clamp(min_occurrences, 2, 12)
    since = add_months(today.replace(day=1), -lookback)
    since_text = since.isoformat()
    history = subscription_history_start(today, lookback).isoformat()
    groups: dict[str, list[_Charge]] = defaultdict(list)
    for charge in _charges(rows, today=today, since=history, account_ids=account_ids):
        groups[normalise_payee(charge.payee)].append(charge)

    found: list[Subscription] = []
    for group in groups.values():
        group.sort(key=lambda c: (c.date, c.id))  # C03: order by date within the group
        recent = [c for c in group if c.date >= since_text]
        if not recent:
            continue
        gap = _median_gap(recent)
        cadence = _cadence_of(gap) if gap is not None else None
        charges = recent
        if cadence is None or cadence.name == "yearly":
            # A yearly charge shows up once in a one-year window: decide over the history.
            longer = _median_gap(group)
            yearly = _cadence_of(longer) if longer is not None else None
            if yearly is not None and yearly.name == "yearly":
                cadence, charges, gap = yearly, group, longer
        if cadence is None or gap is None:
            continue
        needed = 2 if cadence.name == "yearly" else minimum
        if len(charges) < needed:
            continue
        amounts = [c.amount for c in charges]
        if not _steady(amounts):
            continue  # e.g. groceries every other week: regular, but not a subscription
        days = [date.fromisoformat(c.date) for c in charges]
        distinct: list[int] = []
        for amount in amounts:
            if not distinct or abs(distinct[-1] - amount) > PRICE_TOLERANCE:
                distinct.append(amount)
        latest = charges[-1]
        since_last = (today - days[-1]).days
        found.append(
            Subscription(
                payee_name=latest.payee,
                cadence=cadence.name,
                median_days=gap,
                occurrences=len(charges),
                first=charges[0].date,
                last=latest.date,
                amount_min=min(amounts),
                amount_max=max(amounts),
                amount_latest=latest.amount,
                history=tuple((c.date, c.amount) for c in charges),
                price_changed=len(distinct) > 1,
                categories=tuple(sorted({cat for c in charges for cat in c.categories})),
                accounts=tuple(sorted({c.account_name for c in charges if c.account_name})),
                next_expected=(days[-1] + timedelta(days=round(gap))).isoformat(),
                active=since_last <= int(cadence.max_days * 1.5),
                monthly_equivalent=round(latest.amount * cadence.per_month),
            )
        )
    found.sort(
        key=lambda s: (_CADENCE_ORDER[s.cadence], -s.amount_latest, normalise_payee(s.payee_name))
    )
    return SubscriptionsReport(today, since, lookback, minimum, tuple(found))
