"""Milliunit helpers.

YNAB stores amounts as integer milliunits (1.234 CHF = 1234). Everything stored or sent
to YNAB is an ``int``; conversions from arbitrary-precision values go through ``Decimal``
with an explicit rounding mode. Binary floats are rejected so that a value such as
``0.1 + 0.2`` can never silently become a wrong milliunit amount.

Display formatting matches v1 (``f"{x:,.2f}"`` / ``f"{x:+.2f}"``, optional `` CHF``
suffix) but rounds half away from zero, like the web UI's ``Intl.NumberFormat``.
"""

from __future__ import annotations

from decimal import ROUND_HALF_EVEN, ROUND_HALF_UP, Decimal, InvalidOperation, localcontext
from typing import Final

MILLI_PER_UNIT: Final[int] = 1000
_MILLI_QUANTUM: Final[Decimal] = Decimal("0.001")
# Generous precision so quantize() never fails for any realistic amount.
_PRECISION: Final[int] = 60


def _require_int(milli: object) -> int:
    # bool is an int subclass; True as "1 milliunit" is always a bug.
    if isinstance(milli, bool) or not isinstance(milli, int):
        raise TypeError(f"milliunits must be an int, got {type(milli).__name__}")
    return milli


def milli_to_decimal(milli: int) -> Decimal:
    """Exact conversion: ``-1234560`` → ``Decimal("-1234.560")``."""
    # String construction is exact regardless of the active decimal context.
    return Decimal(f"{_require_int(milli)}e-3")


def decimal_to_milli(value: Decimal | int | str) -> int:
    """Convert currency units to milliunits, rounding to 0.001 with ROUND_HALF_EVEN.

    Accepts ``Decimal``, ``int`` or a decimal string (``"12.3456"``); rejects ``float``
    (``TypeError``) and non-finite or unparsable values (``ValueError``).
    """
    if isinstance(value, bool | float):
        raise TypeError(f"use Decimal or str for money, not {type(value).__name__}")
    if isinstance(value, Decimal):
        number = value
    elif isinstance(value, int):
        number = Decimal(value)
    elif isinstance(value, str):
        try:
            number = Decimal(value.strip())
        except InvalidOperation as exc:
            raise ValueError(f"not a decimal amount: {value!r}") from exc
    else:
        raise TypeError(f"cannot convert {type(value).__name__} to milliunits")
    if not number.is_finite():
        raise ValueError(f"amount must be finite, got {number}")
    with localcontext() as ctx:
        ctx.prec = _PRECISION
        try:
            quantized = number.quantize(_MILLI_QUANTUM, rounding=ROUND_HALF_EVEN)
        except InvalidOperation as exc:
            raise ValueError("amount is too large") from exc
        return int(quantized.scaleb(3))


def _rounded_units(milli: int, decimals: int) -> Decimal:
    if not 0 <= decimals <= 3:
        raise ValueError("decimals must be between 0 and 3")
    quantum = Decimal(1).scaleb(-decimals)
    with localcontext() as ctx:
        ctx.prec = _PRECISION
        units = milli_to_decimal(milli).quantize(quantum, rounding=ROUND_HALF_UP)
    # Rounding a small negative amount gives -0.00; never show a negative zero.
    return units.copy_abs() if units.is_zero() else units


def format_money(
    milli: int, currency: str | None = None, *, sign: bool = False, decimals: int = 2
) -> str:
    """Human-readable amount: ``format_money(-1234560, "CHF")`` → ``"-1,234.56 CHF"``.

    ``sign=True`` always shows the sign (``"+12.50"``, ``"+0.00"``), as v1 did for changes.
    ``currency`` (an ISO code) is appended after a space when given.
    """
    units = _rounded_units(_require_int(milli), decimals)
    spec = f"{'+' if sign else ''},.{decimals}f"
    text = format(units, spec)
    return f"{text} {currency}" if currency else text


def format_amount_plain(milli: int, *, decimals: int = 2) -> str:
    """Machine-friendly amount without grouping: ``-1234560`` → ``"-1234.56"``."""
    units = _rounded_units(_require_int(milli), decimals)
    return format(units, f".{decimals}f")
