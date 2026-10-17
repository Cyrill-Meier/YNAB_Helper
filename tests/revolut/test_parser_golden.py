"""Golden tests: v2 parser vs the frozen v1 parser on every fixture (DESIGN §6.1, §12).

Two independent guards:

* ``assert_matches_v1`` re-runs the frozen v1 ``parse_revolut_csv`` as an oracle and
  compares import_id, amount, date, memo, payee and cleared byte for byte;
* ``FROZEN`` pins the expected keys/states/memos, so a change to the oracle copy (or to
  a fixture) cannot silently move the goalposts.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from tests.revolut.csvbuild import FIXTURES, PARSER_FIXTURES
from tests.revolut.oracle import assert_matches_v1, v1_parse
from ynab_helper.errors import CsvError
from ynab_helper.revolut.parser import parse_export

#: (key, state, memo) per row, in file order. Generated once from v2 and verified
#: against the v1 oracle; edit only together with the fixture.
FROZEN = {
    "export_basic": [
        ("YNAB:500000:2026-10-01:1", "COMPLETED", "Topup"),
        ("YNAB:-8400:2026-10-01:1", "COMPLETED", None),
        ("YNAB:-120000:2026-10-02:1", "COMPLETED", "Transfer"),
        ("YNAB:-42150:2026-10-01:1", "COMPLETED", None),
        ("YNAB:-3600:2026-10-03:1", "COMPLETED", None),
        ("YNAB:-3600:2026-10-03:2", "COMPLETED", None),
        ("YNAB:-100000:2026-10-04:1", "COMPLETED", "Exchange"),
        ("YNAB:-3500:2026-10-02:1", "COMPLETED", None),
        ("YNAB:-27800:2026-10-05:1", "COMPLETED", None),
        ("YNAB:59000:2026-10-06:1", "COMPLETED", "Card Refund"),
        ("YNAB:-4500:2026-10-07:1", "COMPLETED", None),
        ("YNAB:-4500:2026-10-08:1", "COMPLETED", None),
        ("YNAB:-4500:2026-10-08:2", "COMPLETED", None),
        ("YNAB:4200000:2026-10-09:1", "COMPLETED", "Transfer"),
        ("YNAB:-4500:2026-10-09:1", "COMPLETED", None),
        ("YNAB:-150000:2026-10-10:1", "REVERTED", None),
        ("YNAB:-45300:2026-10-11:1", "PENDING", "(pending)"),
        ("YNAB:-89990:2026-10-11:1", "PENDING", "(pending)"),
        ("YNAB:-12950:2026-10-11:1", "COMPLETED", None),
        ("YNAB:-100000:2026-10-12:1", "COMPLETED", "ATM"),
        ("YNAB:-10:2026-10-12:1", "PENDING", "(pending)"),
        ("YNAB:-8400:2026-10-13:1", "PENDING", "(pending)"),
        ("YNAB:-8400:2026-10-13:2", "COMPLETED", None),
    ],
    "export_tz_shift": [
        ("YNAB:500000:2026-10-01:1", "COMPLETED", "Topup"),
        ("YNAB:-8400:2026-10-01:1", "COMPLETED", None),
        ("YNAB:-120000:2026-10-02:1", "COMPLETED", "Transfer"),
        ("YNAB:-42150:2026-10-02:1", "COMPLETED", None),
        ("YNAB:-3600:2026-10-03:1", "COMPLETED", None),
        ("YNAB:-3600:2026-10-04:1", "COMPLETED", None),
        ("YNAB:-100000:2026-10-04:1", "COMPLETED", "Exchange"),
        ("YNAB:-3500:2026-10-03:1", "COMPLETED", None),
        ("YNAB:-27800:2026-10-05:1", "COMPLETED", None),
        ("YNAB:59000:2026-10-07:1", "COMPLETED", "Card Refund"),
        ("YNAB:-4500:2026-10-07:1", "COMPLETED", None),
        ("YNAB:-4500:2026-10-08:1", "COMPLETED", None),
        ("YNAB:-4500:2026-10-09:1", "COMPLETED", None),
        ("YNAB:4200000:2026-10-10:1", "COMPLETED", "Transfer"),
        ("YNAB:-4500:2026-10-09:2", "COMPLETED", None),
        ("YNAB:-150000:2026-10-10:1", "REVERTED", None),
        ("YNAB:-45300:2026-10-12:1", "PENDING", "(pending)"),
        ("YNAB:-89990:2026-10-12:1", "PENDING", "(pending)"),
        ("YNAB:-12950:2026-10-11:1", "COMPLETED", None),
        ("YNAB:-100000:2026-10-12:1", "COMPLETED", "ATM"),
        ("YNAB:-10:2026-10-13:1", "PENDING", "(pending)"),
        ("YNAB:-8400:2026-10-13:1", "PENDING", "(pending)"),
        ("YNAB:-8400:2026-10-13:2", "COMPLETED", None),
    ],
    "export_pending_then_completed_a": [
        ("YNAB:300000:2026-10-01:1", "COMPLETED", "Topup"),
        ("YNAB:-150000:2026-10-10:1", "PENDING", "(pending)"),
        ("YNAB:-11200:2026-10-10:1", "COMPLETED", None),
        ("YNAB:-45300:2026-10-11:1", "PENDING", "(pending)"),
        ("YNAB:-89990:2026-10-11:1", "PENDING", "(pending)"),
        ("YNAB:-6800:2026-10-12:1", "COMPLETED", None),
    ],
    "export_pending_then_completed_b": [
        ("YNAB:300000:2026-10-01:1", "COMPLETED", "Topup"),
        ("YNAB:-150000:2026-10-10:1", "REVERTED", None),
        ("YNAB:-11200:2026-10-10:1", "COMPLETED", None),
        ("YNAB:-6800:2026-10-12:1", "COMPLETED", None),
        ("YNAB:-45300:2026-10-11:1", "COMPLETED", None),
        ("YNAB:-90410:2026-10-11:1", "COMPLETED", None),
        ("YNAB:-11200:2026-10-13:1", "PENDING", "(pending)"),
    ],
    "export_same_amount_same_day_a": [
        ("YNAB:-4500:2026-10-05:1", "COMPLETED", None),
        ("YNAB:-3600:2026-10-05:1", "PENDING", "(pending)"),
        ("YNAB:-4500:2026-10-05:2", "PENDING", "(pending)"),
        ("YNAB:-4500:2026-10-05:3", "COMPLETED", None),
        ("YNAB:-3600:2026-10-05:2", "COMPLETED", None),
    ],
    "export_same_amount_same_day_b": [
        ("YNAB:-4500:2026-10-05:1", "COMPLETED", None),
        ("YNAB:-4500:2026-10-05:2", "COMPLETED", None),
        ("YNAB:-3600:2026-10-05:1", "COMPLETED", None),
        ("YNAB:-3800:2026-10-05:1", "COMPLETED", None),
        ("YNAB:-4500:2026-10-05:3", "COMPLETED", None),
    ],
    "export_fees": [
        ("YNAB:1000000:2026-09-01:1", "COMPLETED", "Topup"),
        ("YNAB:-201250:2026-09-02:1", "COMPLETED", "ATM | Fee: 1.25"),
        ("YNAB:-500370:2026-09-03:1", "COMPLETED", "Exchange | Fee: 0.37"),
        ("YNAB:-2000:2026-09-04:1", "COMPLETED", "Transfer | Fee: 1.0"),
        ("YNAB:-25000:2026-09-05:1", "COMPLETED", None),
        ("YNAB:-25000:2026-09-05:2", "COMPLETED", None),
        ("YNAB:-25000:2026-09-06:1", "COMPLETED", None),
        ("YNAB:-12000:2026-09-07:1", "COMPLETED", "Fee: 2.0"),
        ("YNAB:-10000:2026-09-07:1", "COMPLETED", "Fee: 0.005"),
        ("YNAB:-40200:2026-09-08:1", "PENDING", "(pending) | Fee: 0.2"),
        ("YNAB:-15500:2026-09-08:1", "COMPLETED", "Transfer | Fee: 0.5"),
        ("YNAB:-3:2026-09-09:1", "COMPLETED", "Fee: 0.0005"),
        ("YNAB:-1000:2026-09-09:1", "COMPLETED", None),
    ],
    "export_other_products": [
        ("YNAB:2000000:2026-08-01:1", "COMPLETED", "Topup"),
        ("YNAB:-100000:2026-08-02:1", "COMPLETED", "Transfer"),
        ("YNAB:-100000:2026-08-02:2", "COMPLETED", None),
        ("YNAB:-500000:2026-08-03:1", "COMPLETED", "Transfer"),
        ("YNAB:-100000:2026-08-03:1", "COMPLETED", None),
        ("YNAB:100120:2026-08-04:1", "COMPLETED", "Transfer"),
    ],
    "export_eur": [
        ("YNAB:300000:2026-07-01:1", "COMPLETED", "Topup"),
        ("YNAB:-4200:2026-07-02:1", "COMPLETED", None),
        ("YNAB:-38500:2026-07-03:1", "COMPLETED", None),
        ("YNAB:-15000:2026-07-04:1", "PENDING", "(pending)"),
    ],
    "export_declined": [
        ("YNAB:300000:2026-10-01:1", "COMPLETED", "Topup"),
        ("YNAB:-250000:2026-10-02:2", "COMPLETED", None),
        ("YNAB:-80000:2026-10-03:2", "PENDING", "(pending)"),
        ("YNAB:-20000:2026-10-04:1", "REVERTED", "Transfer"),
        ("YNAB:-20000:2026-10-04:2", "COMPLETED", "Transfer"),
    ],
    "export_legacy_no_time": [
        ("YNAB:100000:2021-03-01:1", "COMPLETED", "Topup"),
        ("YNAB:-12500:2021-03-02:1", "COMPLETED", None),
        ("YNAB:-12500:2021-03-02:2", "COMPLETED", None),
        ("YNAB:-7250:2021-03-04:1", "PENDING", "(pending)"),
        ("YNAB:-30000:2021-03-05:1", "COMPLETED", "Transfer"),
    ],
}


#: Every parser fixture except the two that v1 cannot parse (mixed currency is a
#: CsvError in v2; the bad-rows file crashes v1 and is covered separately below).
ORACLE_FIXTURES = sorted(
    set(PARSER_FIXTURES) - {"export_mixed_currency.csv", "export_bad_rows.csv"}
)


def test_every_fixture_is_covered() -> None:
    assert {f"{name}.csv" for name in FROZEN} == set(ORACLE_FIXTURES)
    assert all((FIXTURES / name).is_file() for name in PARSER_FIXTURES)


@pytest.mark.parametrize("name", ORACLE_FIXTURES)
def test_C04_fixture_matches_v1_oracle(name: str) -> None:
    path = FIXTURES / name
    export = parse_export(path)
    assert_matches_v1(export, v1_parse(path))


@pytest.mark.parametrize("name", ORACLE_FIXTURES)
def test_fixture_matches_frozen_expectations(name: str) -> None:
    export = parse_export(FIXTURES / name)
    assert [(r.key, r.state, r.memo) for r in export.rows] == FROZEN[name.removesuffix(".csv")]


@pytest.mark.parametrize("name", ORACLE_FIXTURES)
def test_bytes_source_equals_path_source(name: str) -> None:
    path = FIXTURES / name
    from_path = parse_export(path)
    from_bytes = parse_export(path.read_bytes())
    assert from_bytes.rows == from_path.rows
    assert from_bytes.skipped == from_path.skipped
    assert from_bytes.sha256 == from_path.sha256
    assert from_bytes.window == from_path.window


def test_bad_rows_fixture_keeps_v1_keys_for_rows_v1_would_accept(tmp_path: Path) -> None:
    """v1 crashes on the bad-rows fixture; on its v1-acceptable subset keys agree."""
    lines = (FIXTURES / "export_bad_rows.csv").read_text(encoding="utf-8").splitlines()
    # Rows v1 crashes on (nan/inf, the short row) or that v2 refuses by design (a
    # pre-1970 date; a lower-case state, which v2 upper-cases).
    excluded = ("nan", "-inf", "inf", "1969-12-31", "2026-10-02 10:11:00\n", "pending,")
    acceptable = [lines[0]] + [
        line for line in lines[1:] if not any(marker in line + "\n" for marker in excluded)
    ]
    subset = tmp_path / "subset.csv"
    subset.write_text("\n".join(acceptable) + "\n", encoding="utf-8")
    export = parse_export(subset)
    legacy = v1_parse(subset)
    # v1 kept the absurd -1e15 row; v2 skips it (it consumed an occurrence in both).
    legacy = [t for t in legacy if abs(t["amount"]) <= 10**12]
    assert_matches_v1(export, legacy)
    assert len(export.rows) >= 6
    assert {s.code for s in export.skipped} >= {"unsupported_state", "absurd_amount"}


def test_mixed_currency_fixture_is_rejected() -> None:
    with pytest.raises(CsvError) as info:
        parse_export(FIXTURES / "export_mixed_currency.csv")
    assert "CHF" in info.value.user_message and "EUR" in info.value.user_message


def test_C01_fixture_reproduces_the_renumbering_collision() -> None:
    """When an earlier same-day pending settles at another amount, a later row inherits
    its key. The parser must reproduce v1's keys faithfully; identity is the started
    timestamp (the planner's job), which the parser exposes unchanged."""
    before = {
        r.started_at_text: r
        for r in parse_export(FIXTURES / "export_same_amount_same_day_a.csv").rows
    }
    after = {
        r.started_at_text: r
        for r in parse_export(FIXTURES / "export_same_amount_same_day_b.csv").rows
    }
    sbb_pending, sbb_evening = before["2026-10-05 08:10:00"], before["2026-10-05 17:40:00"]
    assert (sbb_pending.key, sbb_pending.state) == ("YNAB:-3600:2026-10-05:1", "PENDING")
    assert sbb_evening.key == "YNAB:-3600:2026-10-05:2"
    # After settlement the evening ride takes over the pending's key ...
    assert after["2026-10-05 17:40:00"].key == "YNAB:-3600:2026-10-05:1"
    # ... and the settled pending has a new amount under the same started timestamp.
    settled = after["2026-10-05 08:10:00"]
    assert (settled.key, settled.amount, settled.state) == (
        "YNAB:-3800:2026-10-05:1",
        -3800,
        "COMPLETED",
    )
    # Completed-date ordering swaps two same-amount coffees: keys :2 and :3 trade places.
    assert before["2026-10-05 12:30:00"].key == "YNAB:-4500:2026-10-05:2"
    assert before["2026-10-05 16:45:00"].key == "YNAB:-4500:2026-10-05:3"
    assert after["2026-10-05 12:30:00"].key == "YNAB:-4500:2026-10-05:3"
    assert after["2026-10-05 16:45:00"].key == "YNAB:-4500:2026-10-05:2"


def test_F34_fixture_timezone_shift_renumbers_same_amount_rows() -> None:
    basic = parse_export(FIXTURES / "export_basic.csv").rows
    shifted = parse_export(FIXTURES / "export_tz_shift.csv").rows
    assert len(basic) == len(shifted)
    for a, b in zip(basic, shifted, strict=True):
        assert b.started_at - a.started_at == timedelta(hours=9)
        assert (a.amount, a.state, a.payee) == (b.amount, b.state, b.payee)
    late = next(i for i, r in enumerate(basic) if r.started_at_text == "2026-10-08 23:30:00")
    nextday = next(i for i, r in enumerate(basic) if r.started_at_text == "2026-10-09 10:00:00")
    assert basic[late].key == "YNAB:-4500:2026-10-08:2"
    assert basic[nextday].key == "YNAB:-4500:2026-10-09:1"
    # Rendered 9 h later, the late coffee crosses midnight and takes the next day's key.
    assert shifted[late].key == "YNAB:-4500:2026-10-09:1"
    assert shifted[nextday].key == "YNAB:-4500:2026-10-09:2"
