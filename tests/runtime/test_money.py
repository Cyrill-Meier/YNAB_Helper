"""money.py: milliunit conversions and formatting."""

from __future__ import annotations

import random
from decimal import Decimal

import pytest

from ynab_helper.money import (
    decimal_to_milli,
    format_amount_plain,
    format_money,
    milli_to_decimal,
)


@pytest.mark.parametrize(
    ("milli", "expected"),
    [
        (0, Decimal("0.000")),
        (1, Decimal("0.001")),
        (-1234560, Decimal("-1234.560")),
        (10**15 + 1, Decimal("1000000000000.001")),
    ],
)
def test_milli_to_decimal_exact(milli: int, expected: Decimal) -> None:
    result = milli_to_decimal(milli)
    assert result == expected
    assert result.as_tuple().exponent == -3


@pytest.mark.parametrize("bad", [1.5, True, "1", None, Decimal(1)])
def test_milli_to_decimal_rejects_non_int(bad: object) -> None:
    with pytest.raises(TypeError):
        milli_to_decimal(bad)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("1.2345"), 1234),  # half-even: 4 is even
        (Decimal("1.2355"), 1236),  # half-even: 5 is odd → up
        (Decimal("-1.2345"), -1234),
        (Decimal("-1.2355"), -1236),
        (Decimal("0.0005"), 0),
        (Decimal("0.0015"), 2),
        (Decimal("12.34"), 12340),
        (Decimal("-0.0004"), 0),
        (Decimal("1E+3"), 1000000),
        (Decimal("1234.567890123456789"), 1234568),
        (7, 7000),
        (-7, -7000),
        ("12.3456", 12346),
        (" -0.5 ", -500),
    ],
)
def test_decimal_to_milli(value: Decimal | int | str, expected: int) -> None:
    result = decimal_to_milli(value)
    assert result == expected
    assert type(result) is int


@pytest.mark.parametrize("bad", [0.1, True, None, [1]])
def test_decimal_to_milli_rejects_floats_and_others(bad: object) -> None:
    with pytest.raises(TypeError):
        decimal_to_milli(bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", [Decimal("NaN"), Decimal("Infinity"), "inf", "abc", "", "1e999999"])
def test_decimal_to_milli_rejects_non_finite_and_garbage(bad: object) -> None:
    with pytest.raises(ValueError):
        decimal_to_milli(bad)  # type: ignore[arg-type]


def test_round_trip_random() -> None:
    rng = random.Random(7)
    for _ in range(2000):
        milli = rng.randint(-(10**13), 10**13)
        assert decimal_to_milli(milli_to_decimal(milli)) == milli


@pytest.mark.parametrize(
    ("milli", "currency", "sign", "expected"),
    [
        (-1234560, "CHF", False, "-1,234.56 CHF"),
        (1234560, "CHF", False, "1,234.56 CHF"),
        (1234560, None, True, "+1,234.56"),
        (-1234560, None, True, "-1,234.56"),
        (0, None, False, "0.00"),
        (0, None, True, "+0.00"),
        (-4, None, False, "0.00"),  # never "-0.00"
        (-4, "EUR", True, "+0.00 EUR"),
        (1235, None, False, "1.24"),  # half away from zero (like Intl.NumberFormat)
        (1245, None, False, "1.25"),
        (-1245, None, False, "-1.25"),
        (999995, None, False, "1,000.00"),
        (1234567890123, "CHF", False, "1,234,567,890.12 CHF"),
        (12000, "", False, "12.00"),
    ],
)
def test_format_money(milli: int, currency: str | None, sign: bool, expected: str) -> None:
    assert format_money(milli, currency, sign=sign) == expected


def test_format_money_decimals() -> None:
    assert format_money(1234567, decimals=0) == "1,235"
    assert format_money(1234567, decimals=3) == "1,234.567"
    with pytest.raises(ValueError):
        format_money(1, decimals=4)


def test_format_money_matches_v1_for_cent_amounts() -> None:
    """v1 printed f"{milli / 1000:,.2f}" (and :+.2f for changes); for YNAB's 2-decimal
    currencies (milliunits divisible by 10) v2 prints exactly the same text."""
    rng = random.Random(11)
    for _ in range(3000):
        milli = rng.randint(-(10**11), 10**11) * 10
        assert format_money(milli) == f"{milli / 1000:,.2f}"
        assert format_money(milli, "CHF") == f"{milli / 1000:,.2f} CHF"
        assert format_money(milli, sign=True) == f"{milli / 1000:+,.2f}"


@pytest.mark.parametrize("bad", [1.5, True, "100", None])
def test_format_money_rejects_non_int(bad: object) -> None:
    with pytest.raises(TypeError):
        format_money(bad)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("milli", "expected"),
    [
        (-1234560, "-1234.56"),
        (1234560, "1234.56"),
        (0, "0.00"),
        (-1, "0.00"),
        (5, "0.01"),
        (-5, "-0.01"),
    ],
)
def test_format_amount_plain(milli: int, expected: str) -> None:
    assert format_amount_plain(milli) == expected
