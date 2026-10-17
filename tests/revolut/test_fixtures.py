"""The synthetic fixtures must look like real Revolut exports (DESIGN §2.9).

Verified production facts: rows are ordered by Completed Date ascending; PENDING and
REVERTED rows have an empty Completed Date and Balance and are interleaved; Started Date
is not monotonic; Balance is the running balance of the row's product.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime
from decimal import Decimal

import pytest

from tests.revolut.csvbuild import FIXTURES, HEADER, PARSER_FIXTURES

ALL = list(PARSER_FIXTURES)
#: Hand-written fixtures that deliberately break the rules.
IRREGULAR = {"export_bad_rows.csv"}
REGULAR = [name for name in ALL if name not in IRREGULAR]


def read_rows(name: str) -> list[dict[str, str]]:
    text = (FIXTURES / name).read_bytes().decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text, newline=None)))


def ts(value: str) -> datetime:
    fmt = "%Y-%m-%d %H:%M:%S" if " " in value else "%Y-%m-%d"
    return datetime.strptime(value, fmt)


#: The one fixture saved with a UTF-8 BOM and CRLF line endings (README).
BOM_CRLF = "export_eur.csv"


def test_eur_fixture_keeps_its_bom_and_crlf_bytes() -> None:
    """Guards the BOM+CRLF golden coverage against eol normalisation (core.autocrlf, a
    .gitattributes change or an editor), which would leave every other test green."""
    data = (FIXTURES / BOM_CRLF).read_bytes()
    assert data.startswith(b"\xef\xbb\xbf")
    assert b"\r\n" in data
    assert data.count(b"\n") == data.count(b"\r\n")  # every line break is CRLF
    assert data.count(b"\r") == data.count(b"\r\n")  # and no bare CR
    quoted = data.split(b'"')[1::2]  # the fixture has no escaped ("") quotes
    assert any(b"\r\n" in field for field in quoted)  # the embedded line break is CRLF too


@pytest.mark.parametrize("name", [n for n in ALL if n != BOM_CRLF])
def test_other_fixtures_are_plain_lf_without_bom(name: str) -> None:
    data = (FIXTURES / name).read_bytes()
    assert not data.startswith(b"\xef\xbb\xbf")
    assert b"\r" not in data


def test_fixture_set_is_documented() -> None:
    readme = (FIXTURES / "README.md").read_text(encoding="utf-8")
    for name in ALL:
        assert (FIXTURES / name).is_file(), name
        assert f"`{name}`" in readme, f"{name} is not documented in tests/fixtures/README.md"


@pytest.mark.parametrize("name", ALL)
def test_header_is_the_revolut_header(name: str) -> None:
    first_line = (FIXTURES / name).read_bytes().decode("utf-8-sig").splitlines()[0]
    assert first_line == ",".join(HEADER)


@pytest.mark.parametrize("name", REGULAR)
def test_rows_follow_production_ordering(name: str) -> None:
    rows = read_rows(name)
    completed = [ts(r["Completed Date"]) for r in rows if r["State"] == "COMPLETED"]
    assert completed == sorted(completed), "COMPLETED rows must be in Completed Date order"
    for r in rows:
        if r["State"] == "COMPLETED":
            assert r["Completed Date"] and r["Balance"]
        else:
            assert r["Completed Date"] == "" and r["Balance"] == "", r


@pytest.mark.parametrize("name", REGULAR)
def test_balances_are_running_per_product_and_currency(name: str) -> None:
    last: dict[tuple[str, str], Decimal] = {}
    for r in read_rows(name):
        if r["State"] != "COMPLETED":
            continue
        account = (r["Product"], r["Currency"])
        balance = Decimal(r["Balance"])
        if account in last:
            delta = Decimal(r["Amount"]) - Decimal(r["Fee"] or "0")
            # Balances are rendered to cents; sub-cent test amounts round.
            assert abs(balance - (last[account] + delta)) < Decimal("0.01"), r
        last[account] = balance


@pytest.mark.parametrize(
    "name",
    [
        "export_basic.csv",
        "export_tz_shift.csv",
        "export_pending_then_completed_b.csv",
        "export_same_amount_same_day_b.csv",
    ],
)
def test_started_date_is_not_monotonic(name: str) -> None:
    started = [ts(r["Started Date"]) for r in read_rows(name)]
    assert started != sorted(started)


def test_pending_and_reverted_rows_are_interleaved() -> None:
    states = [r["State"] for r in read_rows("export_basic.csv")]
    first_open = min(i for i, s in enumerate(states) if s != "COMPLETED")
    assert "COMPLETED" in states[first_open:]  # not merely appended at the end
    assert {"PENDING", "REVERTED"} <= set(states)


def test_tz_shift_fixture_is_basic_rendered_nine_hours_later() -> None:
    basic, shifted = read_rows("export_basic.csv"), read_rows("export_tz_shift.csv")
    assert len(basic) == len(shifted)
    for a, b in zip(basic, shifted, strict=True):
        assert (ts(b["Started Date"]) - ts(a["Started Date"])).total_seconds() == 9 * 3600
        if a["Completed Date"]:
            assert (ts(b["Completed Date"]) - ts(a["Completed Date"])).total_seconds() == 9 * 3600
        for column in ("Type", "Description", "Amount", "Fee", "Currency", "State", "Balance"):
            assert a[column] == b[column]


def test_pending_then_completed_pair_describes_one_period() -> None:
    before = {r["Started Date"]: r for r in read_rows("export_pending_then_completed_a.csv")}
    after = {r["Started Date"]: r for r in read_rows("export_pending_then_completed_b.csv")}
    assert set(before) < set(after)  # b adds one new row
    transitions = {
        (before[k]["State"], after[k]["State"], before[k]["Amount"] == after[k]["Amount"])
        for k in before
    }
    assert ("PENDING", "COMPLETED", True) in transitions  # completes and moves
    assert ("PENDING", "COMPLETED", False) in transitions  # settles at another amount
    assert ("PENDING", "REVERTED", True) in transitions  # hold released


def test_fixtures_contain_no_real_identifiers() -> None:
    """Synthetic data only: no IBANs, card numbers or e-mail addresses."""
    import re

    pattern = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b|\b\d{13,19}\b|@")
    for name in ALL:
        text = (FIXTURES / name).read_text(encoding="utf-8-sig")
        assert not pattern.search(text), name
