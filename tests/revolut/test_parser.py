"""Unit tests for ynab_helper.revolut.parser (DESIGN §6.1) and regression tests F14/F37/F85."""

from __future__ import annotations

import csv
import hashlib
import time
from datetime import datetime
from pathlib import Path

import pytest

from tests.legacy import v1_revolut_to_ynab as v1
from tests.revolut.csvbuild import HEADER, make_csv, row
from ynab_helper.errors import CsvError
from ynab_helper.revolut import parser
from ynab_helper.revolut.parser import (
    PENDING_MARKER,
    STATE_ALLOWLIST,
    has_pending_marker,
    looks_like_revolut_csv,
    parse_export,
    parse_filename_window,
    strip_pending_marker,
)


def parse_one(**fields: str):
    export = parse_export(make_csv(row(**fields)))
    assert len(export.rows) == 1, export.skipped
    return export.rows[0]


# ─── File level ──────────────────────────────────────────────────────────────


def test_basic_row_fields() -> None:
    r = parse_one(
        type="Transfer",
        started="2026-10-01 09:30:15",
        completed="2026-10-02 06:00:00",
        desc="  To Anna Example ",
        amount="-120.5",
        balance="1234.56",
        currency=" chf ",
    )
    assert r.line_no == 1
    assert r.key == "YNAB:-120500:2026-10-01:1"
    assert r.type == "Transfer"
    assert r.product == "Current"
    assert r.description == "To Anna Example"
    assert r.payee == "To Anna Example"
    assert r.started_at == datetime(2026, 10, 1, 9, 30, 15)
    assert r.started_at_text == "2026-10-01 09:30:15"
    assert r.started_has_time is True
    assert r.completed_at == datetime(2026, 10, 2, 6, 0, 0)
    assert r.date == "2026-10-01"
    assert r.amount == -120500
    assert r.fee == 0.0
    assert r.currency == "CHF"
    assert r.state == "COMPLETED"
    assert r.cleared == "cleared"
    assert r.balance == 1234560
    assert r.memo == "Transfer"


def test_header_only_file_is_valid_and_empty() -> None:
    export = parse_export(make_csv())
    assert export.rows == [] and export.skipped == []
    assert export.currency is None and export.window is None
    assert export.data_rows == 0


@pytest.mark.parametrize("content", [b"", b"\xef\xbb\xbf", b"\n\n"])
def test_empty_file_is_rejected(content: bytes) -> None:
    with pytest.raises(CsvError, match="empty"):
        parse_export(content)


def test_missing_required_column_is_rejected() -> None:
    header = [h for h in HEADER if h != "State"]
    with pytest.raises(CsvError) as info:
        parse_export(make_csv(header=header))
    assert "State" in info.value.user_message


def test_header_names_are_exact() -> None:
    header = ["amount" if h == "Amount" else h for h in HEADER]
    with pytest.raises(CsvError, match="Amount"):
        parse_export(make_csv(header=header))


def test_duplicate_columns_are_rejected() -> None:
    with pytest.raises(CsvError, match="duplicate column\\(s\\) Amount"):
        parse_export(make_csv(header=[*HEADER, "Amount"]))


def test_duplicate_unknown_or_empty_columns_are_tolerated() -> None:
    lines = [",".join([*HEADER, "", "", "Note", "Note"]), ",".join([*row(), "", "", "a", "b"])]
    (r,) = parse_export(("\n".join(lines) + "\n").encode()).rows
    assert r.amount == -1000


def test_optional_columns_may_be_missing() -> None:
    header = ["Started Date", "Completed Date", "Description", "Amount", "State"]
    data = make_csv(["2026-10-01 10:00:00", "", "Coop", "-2", "PENDING"], header=header)
    export = parse_export(data)
    (r,) = export.rows
    assert (r.type, r.product, r.fee, r.currency, r.balance) == ("", "", 0.0, "", None)
    assert r.memo == PENDING_MARKER
    assert export.currency is None


def test_bom_and_crlf_are_tolerated() -> None:
    data = b"\xef\xbb\xbf" + make_csv(row(), row(started="2026-10-02 10:00:00"), newline="\r\n")
    export = parse_export(data)
    assert [r.date for r in export.rows] == ["2026-10-01", "2026-10-02"]
    assert export.rows[0].type == "Card Payment"


def test_crlf_inside_quoted_field_is_normalised_like_v1() -> None:
    export = parse_export(make_csv(row(desc="Line\r\nbreak"), newline="\r\n"))
    assert export.rows[0].payee == "Line\nbreak"


def test_line_numbers_count_data_rows_and_skip_blank_lines() -> None:
    text = "\n".join([",".join(HEADER), ",".join(row()), "", "", ",".join(row(amount="-2"))])
    export = parse_export(text.encode())
    assert [(r.line_no, r.amount) for r in export.rows] == [(1, -1000), (2, -2000)]


def test_size_limit_for_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    data = make_csv(row(), row())
    monkeypatch.setattr(parser, "MAX_CSV_BYTES", len(data) - 1)
    with pytest.raises(CsvError, match="limit is"):
        parse_export(data)


def test_size_limit_for_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    data = make_csv(row(), row())
    path = tmp_path / "big.csv"
    path.write_bytes(data)
    monkeypatch.setattr(parser, "MAX_CSV_BYTES", len(data) - 1)
    with pytest.raises(CsvError) as info:
        parse_export(path)
    assert info.value.user_message == "The statement is larger than 25 MB."


def test_real_size_limit_is_25_mib() -> None:
    assert parser.MAX_CSV_BYTES == 25 * 1024 * 1024
    with pytest.raises(CsvError):
        parse_export(make_csv() + b"x" * parser.MAX_CSV_BYTES)


def test_unreadable_path_is_a_csv_error(tmp_path: Path) -> None:
    with pytest.raises(CsvError) as info:
        parse_export(tmp_path / "missing.csv")
    assert info.value.user_message == "The statement file could not be read."
    with pytest.raises(CsvError):
        parse_export(tmp_path)  # a directory


def test_str_path_and_filename_window_default(tmp_path: Path) -> None:
    path = tmp_path / "account-statement_2026-10-01_2026-10-16_en_abc123.csv"
    path.write_bytes(make_csv(row()))
    export = parse_export(str(path))
    assert export.filename_window == ("2026-10-01", "2026-10-16")
    assert parse_export(path, filename="renamed.csv").filename_window is None


def test_sha256_is_of_raw_bytes() -> None:
    data = make_csv(row())
    assert parse_export(data).sha256 == hashlib.sha256(data).hexdigest()


def test_malformed_csv_field_too_large_is_a_csv_error() -> None:
    data = make_csv(row(desc="x" * (csv.field_size_limit() + 10)))
    with pytest.raises(CsvError, match="malformed CSV") as info:
        parse_export(data)
    assert "not a valid CSV" in info.value.user_message


def test_malformed_csv_error_names_the_data_row_and_the_file_line() -> None:
    """Data rows and file lines differ by the header and by quoted line breaks; the error
    names both (the DictReader's own line counter is stale at that point)."""
    big = "x" * (csv.field_size_limit() + 10)
    data = make_csv(row(desc="two\nlines"), row(), row(desc=big))
    with pytest.raises(CsvError) as info:
        parse_export(data)
    assert info.value.user_message == (
        "The statement is not a valid CSV file (data row 3, file line 5)."
    )
    with pytest.raises(CsvError) as info:
        parse_export(make_csv(header=[*HEADER, big]))
    assert info.value.user_message == "The statement is not a valid CSV file (header, file line 1)."


def test_skip_and_warning_locations_are_data_rows() -> None:
    """line_no (and every message) counts records after the header, not file lines."""
    export = parse_export(make_csv(row(desc="two\nlines"), row(amount="x"), row(fee="?")))
    assert [(s.line_no, s.code) for s in export.skipped] == [(2, "invalid_amount")]
    assert export.warnings == ["data row 3: unparseable Fee '?' treated as 0"]
    assert [r.line_no for r in export.rows] == [1, 3]


# ─── Header size (linear-time checks) ────────────────────────────────────────


def _wide_header(extra: list[str]) -> bytes:
    return (",".join([*parser.REQUIRED_COLUMNS, *extra]) + "\n").encode()


def test_header_column_limit_is_inclusive() -> None:
    assert parser.MAX_COLUMNS == 64
    fill = [f"Extra {i}" for i in range(parser.MAX_COLUMNS - len(HEADER))]
    lines = [",".join([*HEADER, *fill]), ",".join([*row(), *("" for _ in fill)])]
    (r,) = parse_export(("\n".join(lines) + "\n").encode()).rows
    assert r.amount == -1000
    with pytest.raises(CsvError) as info:
        parse_export(make_csv(row(), header=[*HEADER, *fill, "One too many"]))
    assert "more than 64 columns" in info.value.user_message


def test_header_with_many_duplicate_columns_is_rejected_quickly() -> None:
    """A header repeating a known column ~100k times passes the upload sniff; the old
    duplicate check was quadratic (≈45 s here, hours for a 25 MB header)."""
    data = _wide_header(["Amount"] * 100_000)
    assert looks_like_revolut_csv(data)
    started = time.perf_counter()
    with pytest.raises(CsvError, match="columns, limit is 64"):
        parse_export(data)
    assert time.perf_counter() - started < 5


def test_duplicate_check_is_linear_even_without_the_column_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(parser, "MAX_COLUMNS", 10**9)
    data = _wide_header(["Amount"] * 100_000)
    started = time.perf_counter()
    with pytest.raises(CsvError, match="duplicate column\\(s\\) Amount"):
        parse_export(data)
    assert time.perf_counter() - started < 5


def test_wide_header_with_many_short_rows_is_rejected_quickly() -> None:
    """DictReader pads each short row to the header width: unique extra columns times
    many rows would be quadratic without the column limit."""
    data = _wide_header([f"c{i}" for i in range(100_000)]) + b"a\n" * 50_000
    started = time.perf_counter()
    with pytest.raises(CsvError, match="columns, limit is 64"):
        parse_export(data)
    assert time.perf_counter() - started < 5


def test_nul_bytes_do_not_crash() -> None:
    (r,) = parse_export(make_csv(row(desc="Co\x00op"))).rows
    assert r.payee == "Co\x00op"


def test_F85_non_utf8_file_is_rejected_with_csv_error() -> None:
    data = make_csv(row(desc="Café")).replace("Café".encode(), "Café".encode("latin-1"))
    with pytest.raises(CsvError) as info:
        parse_export(data)
    assert "UTF-8" in info.value.user_message


def test_utf16_file_is_rejected() -> None:
    data = make_csv(row()).decode().encode("utf-16")
    with pytest.raises(CsvError):
        parse_export(data)


# ─── Products, states, skipped rows ──────────────────────────────────────────


def test_other_products_are_counted_and_do_not_consume_occurrences() -> None:
    export = parse_export(
        make_csv(
            row(product="Savings", amount="-1.00"),
            row(product="Deposit", amount="-1.00", started="garbage", state="WEIRD"),
            row(product=" Current ", amount="-1.00"),
            row(product="", amount="-1.00"),
            row(product="Savings"),
        )
    )
    assert export.other_products == {"Deposit": 1, "Savings": 2}
    assert [r.key for r in export.rows] == ["YNAB:-1000:2026-10-01:1", "YNAB:-1000:2026-10-01:2"]
    assert export.skipped == []
    assert export.data_rows == 5


def test_F37_non_allowlisted_states_are_skipped_reported_and_counted() -> None:
    export = parse_export(
        make_csv(
            row(state="DECLINED", amount="-250"),
            row(state="FAILED", amount="-250"),
            row(state="", amount="-250"),
            row(state="COMPLETED", amount="-250"),
            row(state="PENDING", amount="-250"),
            row(state="REVERTED", amount="-250"),
        )
    )
    assert [(r.key, r.state) for r in export.rows] == [
        ("YNAB:-250000:2026-10-01:4", "COMPLETED"),
        ("YNAB:-250000:2026-10-01:5", "PENDING"),
        ("YNAB:-250000:2026-10-01:6", "REVERTED"),
    ]
    assert [(s.line_no, s.code, s.key) for s in export.skipped] == [
        (1, "unsupported_state", "YNAB:-250000:2026-10-01:1"),
        (2, "unsupported_state", "YNAB:-250000:2026-10-01:2"),
        (3, "unsupported_state", "YNAB:-250000:2026-10-01:3"),
    ]
    assert "DECLINED" in export.skipped[0].reason
    assert "(empty)" in export.skipped[2].reason
    assert export.skipped[0].as_tuple() == (1, export.skipped[0].reason)


def test_state_allowlist_mapping() -> None:
    assert dict(STATE_ALLOWLIST) == {
        "COMPLETED": "cleared",
        "PENDING": "uncleared",
        "REVERTED": None,
    }
    with pytest.raises(TypeError):
        STATE_ALLOWLIST["DECLINED"] = "uncleared"  # type: ignore[index]


def test_reverted_rows_are_kept_with_v1_cleared_value() -> None:
    r = parse_one(state="REVERTED", completed="", balance="")
    assert (r.state, r.cleared, r.completed_at, r.balance, r.memo) == (
        "REVERTED",
        "uncleared",
        None,
        None,
        None,
    )


def test_state_is_stripped_and_upper_cased_but_memo_stays_v1_identical() -> None:
    """Deviation from v1 (documented): v1 compared State case-sensitively, so a
    lower-case 'completed' was imported as uncleared. v2 upper-cases the state; the memo
    keeps v1's case-sensitive '(pending)' rule so it stays byte-identical."""
    completed = parse_one(state=" completed ")
    assert (completed.state, completed.cleared) == ("COMPLETED", "cleared")
    pending = parse_one(state="pending", completed="", balance="")
    assert (pending.state, pending.cleared, pending.memo) == ("PENDING", "uncleared", None)
    canonical = parse_one(state=" PENDING ", completed="", balance="")
    assert canonical.memo == PENDING_MARKER


@pytest.mark.parametrize(
    ("amount", "fee", "code"),
    [
        ("nan", "0.00", "non_finite_amount"),
        ("inf", "0.00", "non_finite_amount"),
        ("-inf", "0.00", "non_finite_amount"),
        ("-1.00", "nan", "non_finite_amount"),
        ("-1.00", "inf", "non_finite_amount"),
        ("1e309", "0.00", "non_finite_amount"),
        ("1e306", "0.00", "non_finite_amount"),  # finite, but *1000 overflows
        ("-1e15", "0.00", "absurd_amount"),
        ("-1000000000.01", "0.00", "absurd_amount"),
        ("-1.00", "2000000000", "absurd_amount"),
        ("abc", "0.00", "invalid_amount"),
        ("", "0.00", "invalid_amount"),
        ("1,50", "0.00", "invalid_amount"),
    ],
)
def test_F85_bad_amounts_are_reported_not_raised(amount: str, fee: str, code: str) -> None:
    export = parse_export(make_csv(row(amount=amount, fee=fee), row(amount="-2.00")))
    assert [r.amount for r in export.rows] == [-2000]
    (skip,) = export.skipped
    assert (skip.line_no, skip.code) == (1, code)
    # Rows v1 crashed on, or rejected before counting, never consume an occurrence;
    # absurd-but-finite rows did consume one in v1 and keep doing so.
    assert (skip.key is not None) == (code == "absurd_amount")


def test_amount_limit_is_inclusive_and_key_fits_ynab() -> None:
    r = parse_one(amount="-1000000000")
    assert r.amount == -1_000_000_000_000
    assert len(r.key) <= 36


@pytest.mark.parametrize(
    ("started", "code"),
    [
        ("", "missing_date"),
        ("   ", "missing_date"),
        ("2026-13-01 10:00:00", "invalid_date"),
        ("2026-10-01T10:00:00", "invalid_date"),
        ("01.10.2026", "invalid_date"),
        ("1969-12-31 23:59:59", "implausible_date"),
    ],
)
def test_bad_started_dates_are_reported(started: str, code: str) -> None:
    export = parse_export(make_csv(row(started=started)))
    assert export.rows == []
    assert [s.code for s in export.skipped] == [code]


def test_min_year_is_accepted() -> None:
    assert parse_one(started="1970-01-01 00:00:00").date == "1970-01-01"


def test_F85_short_rows_are_reported_not_raised() -> None:
    lines = [
        ",".join(HEADER),
        "Card Payment,Current,2026-10-01 10:00:00",  # crashed v1 (Description None)
        "Card Payment,Current,2026-10-01 10:00:00,,Coop,-1.00,0.00,CHF",  # State None
        "Card Payment,Current,2026-10-01 10:00:00,2026-10-01 10:01:00,Coop,-1.00,0.00,CHF,COMPLETED",
    ]
    export = parse_export(("\n".join(lines) + "\n").encode())
    assert [s.code for s in export.skipped] == ["incomplete_row", "incomplete_row"]
    # A row missing only Balance was accepted by v1 and still is.
    (r,) = export.rows
    assert (r.line_no, r.key, r.balance) == (3, "YNAB:-1000:2026-10-01:1", None)


def test_extra_columns_are_ignored() -> None:
    lines = [",".join(HEADER), ",".join(row()) + ",extra,cells"]
    (r,) = parse_export(("\n".join(lines) + "\n").encode()).rows
    assert r.amount == -1000


# ─── Amount, fee, memo, payee (v1 rules) ─────────────────────────────────────


@pytest.mark.parametrize(
    ("amount", "fee", "milli", "memo"),
    [
        ("-200.00", "1.25", -201250, "Fee: 1.25"),
        ("-1.0", "1.0", -2000, "Fee: 1.0"),
        ("-10", "2", -12000, "Fee: 2.0"),
        ("-9.995", "0.005", -10000, "Fee: 0.005"),
        ("-0.0025", "0.0005", -3, "Fee: 0.0005"),
        ("-25.00", "0.00", -25000, None),
        ("-25.00", "", -25000, None),
        ("-25.00", "-0.00", -25000, None),
        ("-25.00", "1e-05", -25000, "Fee: 1e-05"),
        ("0.0025", "0", 2, None),  # banker's rounding of 2.5
        ("0.0035", "0", 4, None),  # 3.5 → 4
        ("-1.0005", "0", -1000, None),  # binary float -1000.4999…
        ("1_000", "0", 1000000, None),  # float() accepts underscores (as in v1)
    ],
)
def test_amount_and_fee_follow_v1_float_formula(
    amount: str, fee: str, milli: int, memo: str | None
) -> None:
    r = parse_one(amount=amount, fee=fee)
    assert r.amount == milli
    assert r.memo == memo
    expected = round((float(amount) - float(fee or "0")) * 1000)
    assert r.amount == expected


def test_unparseable_fee_is_zero_with_warning() -> None:
    export = parse_export(make_csv(row(fee="n/a")))
    (r,) = export.rows
    assert (r.fee, r.amount, r.memo) == (0.0, -1000, None)
    assert export.warnings == ["data row 1: unparseable Fee 'n/a' treated as 0"]


def test_warnings_are_capped() -> None:
    export = parse_export(make_csv(*[row(fee="x") for _ in range(60)]))
    assert len(export.rows) == 60
    assert len(export.warnings) == 51
    assert export.warnings[-1] == "… and 10 more warning(s)"


@pytest.mark.parametrize(
    ("type_", "state", "fee", "memo"),
    [
        ("Card Payment", "COMPLETED", "0", None),
        ("Card Payment", "PENDING", "0", "(pending)"),
        ("Card Payment", "PENDING", "0.5", "(pending) | Fee: 0.5"),
        ("Transfer", "PENDING", "0.5", "Transfer | (pending) | Fee: 0.5"),
        ("  Transfer  ", "COMPLETED", "0", "Transfer"),
        ("Card Payment ", "COMPLETED", "0", None),  # stripped before the comparison
        ("card payment", "COMPLETED", "0", "card payment"),  # case-sensitive like v1
        ("", "COMPLETED", "0", None),
    ],
)
def test_memo(type_: str, state: str, fee: str, memo: str | None) -> None:
    assert parse_one(type=type_, state=state, fee=fee).memo == memo


def test_memo_is_truncated_to_200() -> None:
    r = parse_one(type="T" * 250, state="PENDING")
    assert r.memo == "T" * 200


@pytest.mark.parametrize(
    ("header", "type_", "desc", "payee"),
    [
        (HEADER, "Transfer", "  Anna  ", "Anna"),
        (HEADER, "Transfer", "   ", "Transfer"),
        (HEADER, "  Transfer ", "", "  Transfer "),  # v1: Type fallback is unstripped
        (HEADER, "", "", ""),  # v1: an empty Type column gives an empty payee
        (HEADER, "Transfer", "x" * 150, "x" * 100),
    ],
)
def test_payee_follows_v1(header: list[str], type_: str, desc: str, payee: str) -> None:
    assert parse_one(type=type_, desc=desc).payee == payee


def test_payee_falls_back_to_unknown_without_type_column() -> None:
    header = [h for h in HEADER if h != "Type"]
    data = make_csv(row(desc="")[1:], header=header)
    (r,) = parse_export(data).rows
    assert (r.payee, r.type, r.memo) == ("Unknown", "", None)


def test_occurrence_counter_is_per_amount_and_date_in_file_order() -> None:
    export = parse_export(
        make_csv(
            row(started="2026-10-02 23:00:00", amount="-4.50"),
            row(started="2026-10-01 08:00:00", amount="-4.50"),
            row(started="2026-10-02 07:00:00", amount="-4.5"),
            row(started="2026-10-02 07:00:00", amount="-4.51"),
            row(started="2026-10-02", amount="-4.50"),
        )
    )
    assert [r.key for r in export.rows] == [
        "YNAB:-4500:2026-10-02:1",
        "YNAB:-4500:2026-10-01:1",
        "YNAB:-4500:2026-10-02:2",
        "YNAB:-4510:2026-10-02:1",
        "YNAB:-4500:2026-10-02:3",
    ]
    assert export.rows[-1].started_has_time is False
    assert export.rows[-1].started_at_text == "2026-10-02 00:00:00"


def test_lenient_v1_timestamp_forms_are_normalised() -> None:
    r = parse_one(started="2026-1-5  3:04:05")
    assert (r.date, r.started_at_text, r.key) == (
        "2026-01-05",
        "2026-01-05 03:04:05",
        "YNAB:-1000:2026-01-05:1",
    )


# ─── Completed Date, Balance, currency, window ───────────────────────────────


def test_completed_date_and_balance() -> None:
    export = parse_export(
        make_csv(
            row(completed="2026-10-03", balance="0.0025"),
            row(completed="nope", balance="lots"),
            row(completed="", balance="", state="PENDING"),
            row(balance="1e12"),
            row(balance="-1e400"),
        )
    )
    first, bad, pending, huge, inf = export.rows
    assert (first.completed_at, first.balance) == (datetime(2026, 10, 3), 2)
    assert (bad.completed_at, bad.balance) == (None, None)
    assert (pending.completed_at, pending.balance) == (None, None)
    assert huge.balance is None and inf.balance is None
    assert export.warnings == [
        "data row 2: unparseable Completed Date 'nope' ignored",
        "data row 2: unparseable Balance 'lots' ignored",
        "data row 4: Balance '1e12' out of range ignored",
        "data row 5: Balance '-1e400' out of range ignored",
    ]


@pytest.mark.parametrize("balance", ["1e9", "-1e9", "1000000000", "1000000000.00", "1e15"])
def test_F85_absurd_balance_is_ignored(balance: str) -> None:
    """F85: a Balance of '1e9' made v1's /reconcile post a huge reconciled adjustment."""
    export = parse_export(make_csv(row(balance=balance)))
    (r,) = export.rows
    assert r.balance is None
    assert export.warnings == [f"data row 1: Balance {balance!r} out of range ignored"]


def test_balance_just_below_the_bound_is_kept() -> None:
    assert parser.MAX_ABS_BALANCE == 1_000_000_000
    assert parse_one(balance="999999999.99").balance == 999_999_999_990
    assert parse_one(balance="-999999999.99").balance == -999_999_999_990


def test_balance_uses_the_float_formula() -> None:
    assert parse_one(balance="1234.5675").balance == round(1234.5675 * 1000)
    assert parse_one(balance="-0.01").balance == -10


def test_F14_currency_is_parsed() -> None:
    export = parse_export(make_csv(row(currency="eur"), row(currency=" EUR")))
    assert export.currency == "EUR"
    assert {r.currency for r in export.rows} == {"EUR"}


@pytest.mark.parametrize(("first", "second"), [("CHF", "EUR"), ("CHF", ""), ("", "USD")])
def test_F14_mixed_currency_file_is_rejected(first: str, second: str) -> None:
    with pytest.raises(CsvError) as info:
        parse_export(make_csv(row(currency=first), row(currency=second)))
    assert "mixes currencies" in info.value.user_message


def test_F14_mixed_currency_message_is_bounded() -> None:
    codes = ["CHF", "EUR", "USD", "GBP", "JPY", "SEK", "NOK", "DKK"]
    with pytest.raises(CsvError) as info:
        parse_export(make_csv(*[row(currency=c) for c in codes]))
    assert info.value.user_message == (
        "This statement mixes currencies (CHF, DKK, EUR, GBP, JPY and 3 more). "
        "Export each Revolut currency account separately."
    )


@pytest.mark.parametrize(
    "currency",
    ["X" * 100_000, "CHFX", "CH", "C1F", "€", "ÇHF", "CH F", "chf\x00", "Swiss francs"],
)
def test_F14_invalid_currency_is_rejected_with_a_short_message(currency: str) -> None:
    with pytest.raises(CsvError) as info:
        parse_export(make_csv(row(currency="CHF"), row(currency=currency)))
    message = info.value.user_message
    assert "invalid Currency" in message and "data row 2" in message
    assert len(message) < 200 and len(str(info.value)) < 200


def test_invalid_currency_of_rows_that_are_not_imported_is_ignored() -> None:
    export = parse_export(
        make_csv(
            row(currency="CHF"),
            row(currency="X" * 1000, product="Savings"),
            row(currency="not a code", state="DECLINED"),
            row(currency="??", amount="nan"),
        )
    )
    assert export.currency == "CHF" and len(export.rows) == 1


def test_currency_of_skipped_and_other_product_rows_is_ignored() -> None:
    export = parse_export(
        make_csv(
            row(currency="CHF"),
            row(currency="EUR", product="Savings"),
            row(currency="EUR", state="DECLINED"),
        )
    )
    assert export.currency == "CHF"


def test_window_is_over_imported_rows_only() -> None:
    export = parse_export(
        make_csv(
            row(started="2026-09-01 10:00:00", state="DECLINED"),
            row(started="2026-10-05 10:00:00", state="PENDING", completed="", balance=""),
            row(started="2026-10-02 10:00:00"),
            row(started="2026-11-01 10:00:00", product="Savings"),
            row(started="2026-10-09 10:00:00", state="REVERTED", completed="", balance=""),
        )
    )
    assert export.window == ("2026-10-02", "2026-10-09")


def test_F46_window_comes_from_rows_not_from_the_filename() -> None:
    export = parse_export(
        make_csv(row(started="2026-10-02 10:00:00")),
        filename="account-statement_2020-01-01_2026-12-31_en_x.csv",
    )
    assert export.filename_window == ("2020-01-01", "2026-12-31")
    assert export.window == ("2026-10-02", "2026-10-02")


def test_state_counts() -> None:
    export = parse_export(make_csv(row(), row(), row(state="PENDING", completed="", balance="")))
    assert export.state_counts() == {"COMPLETED": 2, "PENDING": 1}


# ─── looks_like_revolut_csv ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    "head",
    [
        make_csv(row()),
        b"\xef\xbb\xbf" + make_csv(row(), newline="\r\n"),
        b",".join(f'"{h}"'.encode() for h in HEADER) + b"\r\n",
        ",".join(HEADER).encode(),  # header only, no newline
        b"Started Date,Completed Date,Description,Amount,State\n",
        make_csv(row(desc="Café"))[:120],  # may cut a multi-byte char
    ],
)
def test_looks_like_revolut_csv_accepts(head: bytes) -> None:
    assert looks_like_revolut_csv(head)


@pytest.mark.parametrize(
    "head",
    [
        b"",
        b"date,amount,payee\n2026-01-01,1,x\n",
        b"Type,Product,Started Date,Completed Date,Description,Amount,Fee\n",
        b"type,product,started date,completed date,description,amount,state\n",
        ",".join(HEADER).encode("utf-16"),
        b"\x00\x01\x02\xff\xfe",
        b"PK\x03\x04 zip file",
        b"\n" + ",".join(HEADER).encode(),  # header must be the first line
        b'"Started Date,Completed Date,Description,Amount,State\n',  # broken quoting
    ],
)
def test_looks_like_revolut_csv_rejects(head: bytes) -> None:
    assert not looks_like_revolut_csv(head)


def test_looks_like_revolut_csv_only_sniffs_the_head() -> None:
    padding = b"x" * parser.HEADER_SNIFF_BYTES
    assert not looks_like_revolut_csv(padding + b"," + ",".join(HEADER).encode())


# ─── parse_filename_window ───────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("name", "window"),
    [
        ("account-statement_2026-01-01_2026-10-16_en-us_abc123.csv", ("2026-01-01", "2026-10-16")),
        ("ACCOUNT-STATEMENT_2026-01-01_2026-10-16.CSV", ("2026-01-01", "2026-10-16")),
        ("copy of account-statement_2026-01-01_2026-10-16 (1).csv", ("2026-01-01", "2026-10-16")),
        ("../../account-statement_2026-01-01_2026-10-16_x.csv", ("2026-01-01", "2026-10-16")),
        ("account-statement_2026-01-01_2026-10-16_x.csv/evil.csv", None),
        ("dir\\account-statement_2026-01-01_2026-10-16_x.csv", ("2026-01-01", "2026-10-16")),
        ("account-statement_2026-02-30_2026-10-16_x.csv", None),
        ("account-statement_2026-10-16_2026-01-01_x.csv", None),
        ("statement.csv", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_filename_window(name: str | None, window: tuple[str, str] | None) -> None:
    assert parse_filename_window(name) == window


def test_parse_filename_window_accepts_paths() -> None:
    assert parse_filename_window(Path("/x/account-statement_2026-01-01_2026-01-31_a.csv")) == (
        "2026-01-01",
        "2026-01-31",
    )


# ─── strip_pending_marker ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("memo", "expected"),
    [
        (None, None),
        ("", ""),
        ("(pending)", ""),
        ("Transfer | (pending)", "Transfer"),
        ("(pending) | Fee: 0.5", "Fee: 0.5"),
        ("Transfer | (pending) | Fee: 0.5", "Transfer | Fee: 0.5"),
        # User edits around the marker survive untouched.
        ("Groceries | (pending) | split with Anna", "Groceries | split with Anna"),
        ("(pending) | my note", "my note"),
        ("  my note | (pending)", "  my note"),
        ("my note | (pending) |", "my note |"),
        ("waiting (pending) for refund", "waiting for refund"),
        ("Coop (pending)", "Coop"),
        ("(pending) Coop", "Coop"),
        ("a|(pending)|b", "a|b"),
        ("(pending) | (pending)", ""),
        ("x (pending) | y | (pending)", "x | y"),
        ("Note | |", "Note | |"),  # no marker: unchanged (v1 stripped the pipes)
        ("(Pending) is case-sensitive", "(Pending) is case-sensitive"),
        ("pending", "pending"),
    ],
)
def test_strip_pending_marker(memo: str | None, expected: str | None) -> None:
    assert strip_pending_marker(memo) == expected


def test_strip_pending_marker_returns_same_object_without_marker() -> None:
    memo = "no marker here"
    assert strip_pending_marker(memo) is memo


@pytest.mark.parametrize("type_", ["Card Payment", "Transfer", "Topup", "", "T" * 30])
@pytest.mark.parametrize("fee", [0.0, 0.5, 1.0, 1e-05])
def test_stripping_a_pending_memo_gives_the_completed_memo(type_: str, fee: float) -> None:
    pending = parser._legacy_memo(type_, "PENDING", fee)
    completed = parser._legacy_memo(type_, "COMPLETED", fee)
    assert has_pending_marker(pending)
    assert strip_pending_marker(pending) == (completed or "")
    # Same result as v1 on the importer's own memos.
    assert strip_pending_marker(pending) == v1._strip_pending_marker(pending)


def test_has_pending_marker() -> None:
    assert not has_pending_marker(None)
    assert not has_pending_marker("")
    assert not has_pending_marker("pending")
    assert has_pending_marker("x | (pending)")
