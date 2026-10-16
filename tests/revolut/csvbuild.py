"""Tiny builders for synthetic Revolut CSV content used by the tests."""

from __future__ import annotations

import csv
import io
from pathlib import Path

#: tests/fixtures (shared directory; other suites may add their own files there).
FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"

#: The parser fixtures documented in tests/fixtures/README.md.
PARSER_FIXTURES = (
    "export_bad_rows.csv",
    "export_basic.csv",
    "export_declined.csv",
    "export_eur.csv",
    "export_fees.csv",
    "export_legacy_no_time.csv",
    "export_mixed_currency.csv",
    "export_other_products.csv",
    "export_pending_then_completed_a.csv",
    "export_pending_then_completed_b.csv",
    "export_same_amount_same_day_a.csv",
    "export_same_amount_same_day_b.csv",
    "export_tz_shift.csv",
)

HEADER = [
    "Type",
    "Product",
    "Started Date",
    "Completed Date",
    "Description",
    "Amount",
    "Fee",
    "Currency",
    "State",
    "Balance",
]


def row(
    *,
    type: str = "Card Payment",
    product: str = "Current",
    started: str = "2026-10-01 10:00:00",
    completed: str = "2026-10-01 10:01:00",
    desc: str = "Coop",
    amount: str = "-1.00",
    fee: str = "0.00",
    currency: str = "CHF",
    state: str = "COMPLETED",
    balance: str = "100.00",
) -> list[str]:
    """One data row in Revolut column order (all cells as rendered text)."""
    return [type, product, started, completed, desc, amount, fee, currency, state, balance]


def make_csv(*rows: list[str], header: list[str] = HEADER, newline: str = "\n") -> bytes:
    """Render ``rows`` under ``header`` as UTF-8 CSV bytes (quoted where needed)."""
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator=newline)
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")
