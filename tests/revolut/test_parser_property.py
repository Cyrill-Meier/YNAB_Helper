"""Seeded randomized comparison of the v2 parser with the frozen v1 parser (DESIGN §6.1).

Each case writes a few thousand synthetic rows — amounts with 0-4 decimals, fees (incl.
empty and unparseable ones), every state, tracked and untracked products, heavy
same-amount/same-day duplication, odd descriptions/types, date-only timestamps, LF or
CRLF line endings, with or without a BOM — and asserts that keys, amounts, dates, memos,
payees and cleared values are byte-identical with v1 for every allowlisted row, and that
non-allowlisted rows consumed exactly the occurrence numbers v1 gave them.
"""

from __future__ import annotations

import csv
import io
import random
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from tests.revolut.oracle import assert_matches_v1, v1_parse
from ynab_helper.revolut.parser import parse_export

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
STATES = ["COMPLETED"] * 12 + ["PENDING"] * 4 + ["REVERTED"] * 2 + ["DECLINED", "FAILED", ""]
PRODUCTS = ["Current"] * 14 + [""] * 2 + ["Savings", "Deposit", " Current "]
TYPES = [
    "Card Payment",
    "Card Payment",
    "Card Payment",
    "Transfer",
    "Topup",
    "Exchange",
    "ATM",
    "Card Refund",
    "Fee",
    "",
    "  Transfer  ",
    "Card Payment ",
]
DESCRIPTIONS = [
    "Coop Pronto",
    "Migros Zuerich HB",
    "SBB CFF FFS",
    "Starbucks Bern",
    "Kiosk, Bahnhof Bern",
    'Bistro "Le Coin"',
    "Café Crème ☕",
    "  padded payee  ",
    "",
    "",
    "Line\r\nbreak",
    "x" * 150,
    "Ünïcödé 💳 " * 12,
    "|pipe|",
    "(pending)",
]
AMOUNT_POOL = ["-3.60", "-4.50", "-4.5", "-12.95", "100", "-0.01", "-1.0005", "0.0025", "-0.00"]


def _amount(rng: random.Random) -> str:
    if rng.random() < 0.45:
        return rng.choice(AMOUNT_POOL)  # forces same-amount collisions
    decimals = rng.randint(0, 4)
    magnitude = rng.choice([1, 10, 100, 1000, 100000])
    value = rng.uniform(0, magnitude)
    sign = "-" if rng.random() < 0.85 else ""
    return f"{sign}{value:.{decimals}f}"


def _fee(rng: random.Random) -> str:
    roll = rng.random()
    if roll < 0.6:
        return "0.00"
    if roll < 0.7:
        return ""
    if roll < 0.75:
        return "0"
    if roll < 0.78:
        return "n/a"  # unparseable: v1 treats it as 0 (and so must v2)
    return f"{rng.uniform(0, 5):.{rng.randint(0, 4)}f}"


def _type(rng: random.Random) -> str:
    if rng.random() < 0.002:
        return "VeryLongType" * 20  # > 200 chars: memo truncation
    return rng.choice(TYPES)


def _rows(rng: random.Random, count: int) -> list[list[str]]:
    days = [datetime(2026, 9, 1) + timedelta(days=d) for d in range(12)]
    rows = []
    for _ in range(count):
        started = rng.choice(days) + timedelta(seconds=rng.randint(0, 86399))
        state = rng.choice(STATES)
        date_only = rng.random() < 0.08
        fmt = "%Y-%m-%d" if date_only else "%Y-%m-%d %H:%M:%S"
        completed = ""
        balance = ""
        if state == "COMPLETED":
            completed = (started + timedelta(hours=rng.randint(0, 60))).strftime(fmt)
            balance = f"{rng.uniform(-500, 9000):.2f}"
        rows.append(
            [
                _type(rng),
                rng.choice(PRODUCTS),
                started.strftime(fmt),
                completed,
                rng.choice(DESCRIPTIONS),
                _amount(rng),
                _fee(rng),
                "CHF",
                state,
                balance,
            ]
        )
    # Mostly completed-date order with interleaved pendings, but never strictly sorted
    # by Started Date (production fact §2.9): sort by a jittered key.
    rows.sort(key=lambda r: (r[3] or r[2], rng.random()))
    # And some extra local shuffling so file order differs from any timestamp order.
    for _ in range(count // 10):
        i = rng.randrange(count - 1)
        rows[i], rows[i + 1] = rows[i + 1], rows[i]
    return rows


def _render(rng: random.Random, rows: list[list[str]]) -> bytes:
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator=rng.choice(["\n", "\r\n"]))
    writer.writerow(HEADER)
    writer.writerows(rows)
    data = buf.getvalue().encode("utf-8")
    return b"\xef\xbb\xbf" + data if rng.random() < 0.5 else data


@pytest.mark.parametrize("seed", [1, 2, 3, 20261016])
def test_random_exports_match_v1(seed: int, tmp_path: Path) -> None:
    rng = random.Random(seed)
    data = _render(rng, _rows(rng, 3000))
    path = tmp_path / f"random_{seed}.csv"
    path.write_bytes(data)

    export = parse_export(path)
    assert_matches_v1(export, v1_parse(path))

    # Sanity: the generator really exercised the interesting paths.
    states = export.state_counts()
    assert states["COMPLETED"] > 1000 and states["PENDING"] > 200 and states["REVERTED"] > 100
    assert sum(1 for s in export.skipped if s.code == "unsupported_state") > 100
    assert sum(export.other_products.values()) > 100
    assert any(r.key.endswith(":5") for r in export.rows)  # deep same-day duplicates
    assert any(r.fee and r.memo and "Fee: " in r.memo for r in export.rows)
    assert any(not r.started_has_time for r in export.rows)
    assert export.skipped and all(s.code == "unsupported_state" for s in export.skipped)
    # Byte source parses identically.
    assert parse_export(data).rows == export.rows
