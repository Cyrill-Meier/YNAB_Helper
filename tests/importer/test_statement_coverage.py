"""Which ledger entries a statement must show (ExportFrame.coverage) — found by the
production dry run on 2026-10-17.

Revolut selects completed rows by *Completed* Date, so a statement for 10-01..10-16 can
contain rows that *started* on 09-29 (they completed in October) while other transactions
that started on 09-29 completed in September and are legitimately absent. Using the first
Started date as the coverage start reported those as "no longer in the Revolut statement"
and — for a pending among them — swept a real transaction from YNAB.
"""

from __future__ import annotations

from datetime import date, datetime

from ynab_helper.importer import ActionKind
from ynab_helper.importer.planner import ExportFrame, statement_range_is_current
from ynab_helper.revolut import parse_export

from .helpers import HEADER, entry, plan, ytx_for

END_OF_10_15 = datetime(2026, 10, 15, 23, 59, 59)

OCT_NAME = "account-statement_2026-10-01_2026-10-16_en-us_abc123.csv"


def _csv(rows: list[tuple[str, str, str, str, str]]) -> bytes:
    """rows: (started, completed or '', description, amount, state)."""
    lines = [",".join(HEADER)]
    for started, completed, desc, amount, state in rows:
        balance = "100.00" if state == "COMPLETED" else ""
        lines.append(
            f"Card Payment,Current,{started},{completed},{desc},{amount},0.00,CHF,{state},{balance}"
        )
    return ("\n".join(lines) + "\n").encode()


# Like the real statement: the first rows started before the range, completed inside it.
OCT_ROWS = [
    ("2026-09-29 10:00:00", "2026-10-01 09:00:00", "Taxi", "-20.00", "COMPLETED"),
    ("2026-09-30 20:00:00", "2026-10-02 08:00:00", "Hotel Hold Release", "-35.00", "COMPLETED"),
    ("2026-10-05 12:00:00", "2026-10-05 12:00:30", "Coop", "-12.40", "COMPLETED"),
    ("2026-10-15 18:00:00", "", "Restaurant", "-60.00", "PENDING"),
]


def _completed_before_range():
    """Entries for transactions that started 09-29/09-30 and completed in September."""
    done = entry(
        "YNAB:-20000:2026-09-29:1",
        csv_payee="Sunrise",
        started_at="2026-09-29 08:00:00",
    )
    pending = entry(
        "YNAB:-150000:2026-09-30:1",
        csv_payee="Hotel",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-09-30 14:00:00",
    )
    return done, pending


def test_coverage_starts_at_the_range_not_at_the_first_started_date() -> None:
    exp = parse_export(_csv(OCT_ROWS), filename=OCT_NAME)
    assert exp.window == ("2026-09-29", "2026-10-15")
    # The first completion (10-01 09:00) is later than the range start (10-01 00:00).
    assert ExportFrame.of(exp).coverage == (datetime(2026, 10, 1, 9, 0, 0), END_OF_10_15)


def test_coverage_without_a_file_name_starts_at_the_first_completed_date() -> None:
    exp = parse_export(_csv(OCT_ROWS))
    assert exp.filename_window is None
    assert ExportFrame.of(exp).coverage == (datetime(2026, 10, 1, 9, 0, 0), END_OF_10_15)


def test_transactions_completed_before_the_range_are_not_vanished_or_swept() -> None:
    """The dry-run case: no 'no longer in the statement' warning, and the pending that
    completed before the range is never deleted from YNAB."""
    done, pending = _completed_before_range()
    for exp in (parse_export(_csv(OCT_ROWS), filename=OCT_NAME), parse_export(_csv(OCT_ROWS))):
        p = plan(exp, [done, pending], [ytx_for(done), ytx_for(pending)])
        assert p.by_kind(ActionKind.DELETE) == []
        assert not [w for w in p.warnings if "no longer in" in w or "vanished" in w]


def test_a_pending_that_vanished_inside_the_range_is_still_swept() -> None:
    gone = entry(
        "YNAB:-57480:2026-10-03:1",
        csv_payee="Tanta",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-10-03 21:00:00",
    )
    exp = parse_export(_csv(OCT_ROWS), filename=OCT_NAME)
    p = plan(exp, [gone], [ytx_for(gone)])
    delete = p.by_kind(ActionKind.DELETE)
    assert [(a.reason, a.ynab_tx_id) for a in delete] == [("superseded", gone.ynab_tx_id)]


def test_a_custom_range_exported_later_covers_nothing() -> None:
    """Range 10-01..10-10 exported on 10-16 (a pending row from 10-15 proves it): a
    transaction that completed between 10-10 and the export is missing, so nothing may be
    swept or reported as vanished."""
    name = "account-statement_2026-10-01_2026-10-10_en-us_abc123.csv"
    exp = parse_export(_csv(OCT_ROWS), filename=name)
    assert ExportFrame.of(exp).coverage is None
    later = entry(
        "YNAB:-9000:2026-10-09:1",
        csv_payee="Shop",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-10-09 10:00:00",
    )
    p = plan(exp, [later], [ytx_for(later)])
    assert p.by_kind(ActionKind.DELETE) == []
    assert not [w for w in p.warnings if "no longer in" in w]


def test_entries_after_the_range_end_are_not_covered() -> None:
    rows = [r for r in OCT_ROWS if r[4] == "COMPLETED"]
    name = "account-statement_2026-10-01_2026-10-10_en-us_abc123.csv"
    exp = parse_export(_csv(rows), filename=name)
    assert ExportFrame.of(exp).coverage == (
        datetime(2026, 10, 1, 9, 0, 0),
        datetime(2026, 10, 5, 23, 59, 59),
    )
    after = entry(
        "YNAB:-9000:2026-10-12:1",
        csv_payee="Shop",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-10-12 10:00:00",
    )
    p = plan(exp, [after], [ytx_for(after)])
    assert p.by_kind(ActionKind.DELETE) == []


def test_coverage_starts_at_the_first_completion_instant_not_its_date() -> None:
    """Whatever timezone Revolut cuts the range in, a transaction that started before the
    first completion may have completed before the range began (review S4)."""
    early = entry(
        "YNAB:-5000:2026-10-01:1",
        csv_payee="Night Bus",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-10-01 00:10:00",
    )
    exp = parse_export(_csv(OCT_ROWS), filename=OCT_NAME)
    p = plan(exp, [early], [ytx_for(early)])
    assert p.by_kind(ActionKind.DELETE) == []


def test_a_pending_only_statement_without_a_range_covers_nothing() -> None:
    """A renamed file with only pending rows proves nothing missing (review S2)."""
    rows = [r for r in OCT_ROWS if r[4] == "PENDING"]
    exp = parse_export(_csv(rows), filename="renamed.csv")
    assert ExportFrame.of(exp).coverage is None
    gone = entry(
        "YNAB:-9000:2026-10-15:1",
        csv_payee="Shop",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-10-15 10:00:00",
    )
    assert plan(exp, [gone], [ytx_for(gone)]).by_kind(ActionKind.DELETE) == []


def test_a_pending_only_statement_with_a_range_still_covers_it() -> None:
    rows = [r for r in OCT_ROWS if r[4] == "PENDING"]
    exp = parse_export(_csv(rows), filename=OCT_NAME)
    assert ExportFrame.of(exp).coverage == (datetime(2026, 10, 15, 0, 0, 0), END_OF_10_15)


def test_a_date_only_statement_with_rows_after_its_range_covers_nothing() -> None:
    """Date-only exports have no timestamps, so a pending row dated after the range end is
    the only evidence of a later export (review S6)."""
    rows = [
        ("2026-10-02", "2026-10-02", "Coop", "-12.40", "COMPLETED"),
        ("2026-10-15", "", "Restaurant", "-60.00", "PENDING"),
    ]
    name = "account-statement_2026-10-01_2026-10-10_en-us_abc123.csv"
    exp = parse_export(_csv(rows), filename=name)
    assert ExportFrame.of(exp).coverage is None


def test_statement_range_is_current() -> None:
    sep = parse_export(
        _csv([("2026-09-20 10:00:00", "2026-09-20 10:00:30", "Coop", "-1.00", "COMPLETED")]),
        filename="account-statement_2026-09-01_2026-09-30_en-us_abc123.csv",
    )
    assert statement_range_is_current(sep, date(2026, 9, 30))
    assert statement_range_is_current(sep, date(2026, 10, 1))  # one day of slack
    assert not statement_range_is_current(sep, date(2026, 10, 2))  # review S1
    renamed = parse_export(_csv(OCT_ROWS), filename="renamed.csv")
    assert statement_range_is_current(renamed, date(2027, 1, 1))  # no range: no claim


def test_a_statement_whose_range_ended_in_the_past_sweeps_nothing() -> None:
    """A September statement downloaded on 10-02 omits a 09-29 pending that completed on
    10-01: it must not be swept (review S1). Creates and matching are unaffected."""
    sep_rows = [
        ("2026-09-02 10:00:00", "2026-09-02 10:00:30", "Coop", "-12.40", "COMPLETED"),
        ("2026-09-30 18:00:00", "2026-09-30 18:00:30", "Migros", "-30.00", "COMPLETED"),
    ]
    exp = parse_export(
        _csv(sep_rows), filename="account-statement_2026-09-01_2026-09-30_en-us_abc123.csv"
    )
    hotel = entry(
        "YNAB:-150000:2026-09-29:1",
        csv_payee="Hotel",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-09-29 14:00:00",
    )
    current = plan(exp, [hotel], [ytx_for(hotel)])
    assert [a.ynab_tx_id for a in current.by_kind(ActionKind.DELETE)] == [hotel.ynab_tx_id]
    past = plan(exp, [hotel], [ytx_for(hotel)], range_is_current=False)
    assert past.by_kind(ActionKind.DELETE) == []
    assert not [w for w in past.warnings if "no longer in" in w]
    assert len(past.by_kind(ActionKind.CREATE)) == len(current.by_kind(ActionKind.CREATE)) == 2
