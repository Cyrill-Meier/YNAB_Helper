"""Planner step 1 (export offsets) and step 2 (matching passes 1-4), DESIGN §6.2."""

from __future__ import annotations

import dataclasses
from datetime import timedelta

import pytest

from ynab_helper.importer import ActionKind, MatchMethod
from ynab_helper.importer.planner import (
    export_offsets,
    is_timezone_offset,
    learn_offsets,
    vote_offsets,
)

from .helpers import (
    ACCT,
    OTHER_ACCT,
    entry,
    entry_for,
    export,
    kinds,
    plan,
    row_by,
    ytx_for,
)

H = 3600


def methods(p) -> list[tuple[str, str, str]]:
    return [(m.row_key, m.entry_key, m.method.value) for m in p.matches]


# ── step 1: offsets ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, False),  # zero is always allowed but never "observed"
        (900, True),
        (-900, True),
        (9 * H, True),
        (-(5 * H + 45 * 60), True),
        (14 * H, True),
        (14 * H + 900, True),  # two renderings differ by up to 26h (Sydney vs Los Angeles)
        (24 * H, True),
        (-26 * H, True),
        (26 * H + 900, False),
        (600, False),  # not a multiple of 15 minutes
    ],
)
def test_is_timezone_offset(seconds: int, expected: bool) -> None:
    assert is_timezone_offset(seconds) is expected


def test_learn_offsets_counts_key_equal_timezone_shaped_deltas() -> None:
    exp = export(
        ("2026-10-05 17:00:00", "A", "-1.00"),  # +9h vs its entry
        ("2026-10-05 18:00:00", "B", "-2.00"),  # +9h
        ("2026-10-05 08:07:00", "C", "-3.00"),  # +7 min: not timezone-shaped
        ("2026-10-05 09:00:00", "D", "-4.00"),  # same instant: 0 is never counted
        ("2026-10-05 10:00:00", "E", "-5.00"),  # entry SUPERSEDED: not learned from
    )
    entries = [
        entry("YNAB:-1000:2026-10-05:1", started_at="2026-10-05 08:00:00"),
        entry("YNAB:-2000:2026-10-05:1", started_at="2026-10-05 09:00:00"),
        entry("YNAB:-3000:2026-10-05:1", started_at="2026-10-05 08:00:00"),
        entry("YNAB:-4000:2026-10-05:1", started_at="2026-10-05 09:00:00"),
        entry("YNAB:-5000:2026-10-05:1", status="SUPERSEDED", started_at="2026-10-05 01:00:00"),
    ]
    assert learn_offsets(exp.rows, entries) == {9 * H: 2}


def test_learn_offsets_ignores_entries_without_started_at_and_date_only_rows() -> None:
    exp = export(("2026-10-05", "A", "-1.00"), ("2026-10-05 17:00:00", "B", "-2.00"))
    entries = [
        entry("YNAB:-1000:2026-10-05:1", started_at="2026-10-04 15:00:00"),
        entry("YNAB:-2000:2026-10-05:1", started_at=None, identity="legacy"),
    ]
    assert learn_offsets(exp.rows, entries) == {}


def test_vote_offsets_need_same_amount_payee_and_entry_inside_window() -> None:
    exp = export(
        ("2026-08-09 23:00:00", "Coffee", "-4.50"),
        ("2026-08-10 14:30:00", "Coffee", "-4.50"),
    )
    entries = [
        entry("YNAB:-4500:2026-08-10:1", csv_payee="Coffee", started_at="2026-08-10 08:00:00"),
        entry("YNAB:-4500:2026-08-10:2", csv_payee="coffee ", started_at="2026-08-10 23:30:00"),
        # other payee, and outside the window: no votes
        entry("YNAB:-4500:2026-08-10:3", csv_payee="Tea", started_at="2026-08-10 05:00:00"),
        entry("YNAB:-4500:2026-08-08:1", csv_payee="Coffee", started_at="2026-08-08 23:00:00"),
    ]
    votes = vote_offsets(exp, entries)
    assert votes == {-9 * H: 2, 6 * H + 30 * 60: 1, -(24 * H + 30 * 60): 1}
    # The key-equal pair (a renumbering collision) is a single +6.5h observation; the true
    # offset (-9h) is supported by both transactions of the group. A single non-distinctive
    # vote (-24.5h, round times) allows nothing.
    offsets = export_offsets(exp, entries)
    assert offsets == {-9 * H: 2, 6 * H + 30 * 60: 1}


def test_rows_and_entries_with_a_same_instant_twin_do_not_vote() -> None:
    # Twice-daily round-time transfers: every imported row has its entry at d = 0, so the
    # 12h/24h distances between different transfers are no evidence of a timezone.
    def transfer(started: str) -> tuple[str, ...]:
        return (started, "To Savings", "-50", "COMPLETED", "Transfer")

    exp = export(
        transfer("2026-08-31 06:00:00"),
        transfer("2026-08-31 18:00:00"),
        transfer("2026-09-01 06:00:00"),
    )
    entries = [
        entry_for(exp.rows[0], key="YNAB:-50000:2026-08-31:1"),
        entry_for(exp.rows[1], key="YNAB:-50000:2026-08-31:2"),
    ]
    assert vote_offsets(exp, entries) == {}
    assert export_offsets(exp, entries) == {}


def test_a_distinctive_single_pair_allows_its_offset() -> None:
    # One overlapping row that crossed midnight (keys differ, one vote): its timestamp has
    # seconds, so an exact 15-minute multiple to another transaction is no coincidence.
    exp = export(
        ("2026-10-08 15:30:10", "Bar Rossi", "-18.00"),
        ("2026-10-09 11:00:10", "Shell", "-60.00"),
    )
    e = entry(
        "YNAB:-18000:2026-10-09:1",
        csv_payee="Bar Rossi",
        started_at="2026-10-09 00:30:10",
    )
    assert vote_offsets(exp, [e]) == {-9 * H: 1}
    assert export_offsets(exp, [e]) == {-9 * H: 1}
    p = plan(exp, [e], [ytx_for(e, cleared="cleared")])
    assert methods(p) == [("YNAB:-18000:2026-10-08:1", e.key, "timestamp")]
    assert [a.payload["payee_name"] for a in p.by_kind(ActionKind.CREATE)] == ["Shell"]


def test_a_distinctive_pair_with_two_candidates_allows_nothing() -> None:
    exp = export(
        ("2026-10-08 15:30:10", "Bar Rossi", "-18.00"),
        ("2026-10-09 15:30:10", "Bar Rossi", "-18.00"),
    )
    # (key :2 so that no key-equal pair teaches an offset on its own)
    e = entry("YNAB:-18000:2026-10-09:2", csv_payee="Bar Rossi", started_at="2026-10-09 06:30:10")
    assert vote_offsets(exp, [e]) == {-15 * H: 1, 9 * H: 1}
    assert export_offsets(exp, [e]) == {}


def test_entries_one_day_outside_the_window_vote_with_distinctive_pairs_only() -> None:
    # Eastward travel with back-to-back statements: the previous statement ended 09-07 (home
    # rendering); this one starts 09-08 in a +8h rendering, so both overlapping rows crossed
    # midnight and their entries lie one day before the window.
    exp = export(
        ("2026-09-08 01:12:31", "Migros", "-22.10"),
        ("2026-09-08 04:45:09", "Tanta", "-57.50"),
        ("2026-09-09 03:00:41", "Kiosk", "-4.00"),
    )
    migros = entry("YNAB:-22100:2026-09-07:1", csv_payee="Migros", started_at="2026-09-07 17:12:31")
    tanta = entry("YNAB:-57500:2026-09-07:1", csv_payee="Tanta", started_at="2026-09-07 20:45:09")
    assert export_offsets(exp, [migros, tanta]) == {8 * H: 2}
    p = plan(exp, [migros, tanta], [ytx_for(migros), ytx_for(tanta)])
    assert {m.entry_key: m.offset_seconds for m in p.matches} == {
        migros.key: 8 * H,
        tanta.key: 8 * H,
    }
    assert [a.payload["payee_name"] for a in p.by_kind(ActionKind.CREATE)] == ["Kiosk"]
    # Round times outside the window never vote.
    rounded = dataclasses.replace(migros, started_at="2026-09-07 17:12:00")
    exp_round = export(("2026-09-08 01:12:00", "Migros", "-22.10"))
    assert vote_offsets(exp_round, [rounded]) == {}


def test_a_lone_distinctive_pair_outside_the_window_needs_an_uncontradicted_shift() -> None:
    # The statement's only overlap with the ledger is one row that crossed midnight (fuzz:
    # eastward travel, back-to-back statements). Without the offset it would be created again.
    exp = export(
        ("2026-09-08 01:12:31", "Migros", "-22.10"),
        ("2026-09-09 03:00:41", "Kiosk", "-4.00"),
    )
    migros = entry("YNAB:-22100:2026-09-07:1", csv_payee="Migros", started_at="2026-09-07 17:12:31")
    assert export_offsets(exp, [migros]) == {8 * H: 1}
    p = plan(exp, [migros], [ytx_for(migros)])
    assert [(m.entry_key, m.offset_seconds) for m in p.matches] == [(migros.key, 8 * H)]
    assert [a.payload["payee_name"] for a in p.by_kind(ActionKind.CREATE)] == ["Kiosk"]

    # Contradicted: rendered at +8h the statement would also show this completed 22:40 row
    # (09-08 06:40:05), and it does not — the pair is a coincidence, not a timezone.
    coop = entry("YNAB:-9900:2026-09-07:1", csv_payee="Coop", started_at="2026-09-07 22:40:05")
    assert export_offsets(exp, [migros, coop]) == {}
    # A completed entry that the shifted statement would not cover yet is no contradiction
    # (it started after the statement's newest row, 09-09 03:00:41 at +8h).
    late = entry("YNAB:-9900:2026-09-08:1", csv_payee="Coop", started_at="2026-09-08 20:00:05")
    assert export_offsets(exp, [migros, late]) == {8 * H: 1}
    # A vanished pending is no contradiction either (it may have been superseded).
    gone = dataclasses.replace(coop, csv_state="PENDING")
    assert export_offsets(exp, [migros, gone]) == {8 * H: 1}
    # Rows at the very instant of an entry say the statement is in the ledger's frame.
    kiosk = entry_for(exp.rows[1], csv_payee="Kiosk")
    assert export_offsets(exp, [migros, kiosk]) == {}
    # Two candidates for the entry: ambiguous, nothing allowed.
    twice = export(
        ("2026-09-08 01:12:31", "Migros", "-22.10"),
        ("2026-09-08 02:42:31", "Migros", "-22.10"),
    )
    assert export_offsets(twice, [migros]) == {}


def test_a_single_vote_does_not_allow_an_offset() -> None:
    exp = export(("2026-09-01 06:00:00", "To Savings", "-500", "COMPLETED", "Transfer"))
    entries = [
        entry(
            "YNAB:-500000:2026-08-31:1",
            csv_payee="To Savings",
            csv_date="2026-09-01",  # inside the window for the purpose of this test
            started_at="2026-08-31 18:00:00",
        )
    ]
    assert vote_offsets(exp, entries) == {12 * H: 1}
    assert export_offsets(exp, entries) == {}


# ── pass 1: timestamp identity ───────────────────────────────────────────────


def test_pass1_same_instant_matches_even_when_the_key_was_renumbered() -> None:
    exp = export(("2026-10-05 17:40:00", "SBB", "-3.60"))
    e = entry("YNAB:-3600:2026-10-05:2", csv_payee="SBB", started_at="2026-10-05 17:40:00")
    p = plan(exp, [e], [ytx_for(e, cleared="cleared")])
    assert methods(p) == [("YNAB:-3600:2026-10-05:1", e.key, "timestamp")]
    assert p.matches[0].offset_seconds == 0
    assert kinds(p) == []


def test_pass1_uses_a_learned_offset() -> None:
    # Two rows reveal +9h by key; the third crossed midnight and is matched by timestamp.
    exp = export(
        ("2026-10-05 17:00:00", "A", "-1.00"),
        ("2026-10-05 18:00:00", "B", "-2.00"),
        ("2026-10-06 08:30:00", "C", "-3.00"),
    )
    a = entry("YNAB:-1000:2026-10-05:1", csv_payee="A", started_at="2026-10-05 08:00:00")
    b = entry("YNAB:-2000:2026-10-05:1", csv_payee="B", started_at="2026-10-05 09:00:00")
    c = entry("YNAB:-3000:2026-10-05:1", csv_payee="C", started_at="2026-10-05 23:30:00")
    p = plan(exp, [a, b, c], [ytx_for(x, cleared="cleared") for x in (a, b, c)])
    assert {m.entry_key: (m.method, m.offset_seconds) for m in p.matches} == {
        a.key: (MatchMethod.TIMESTAMP, 9 * H),
        b.key: (MatchMethod.TIMESTAMP, 9 * H),
        c.key: (MatchMethod.TIMESTAMP, 9 * H),
    }
    assert not p.by_kind(ActionKind.CREATE)


def test_pass1_unlearned_offset_is_not_a_match() -> None:
    # F10 variant: a round-time transfer 12h after a ledger one is a *new* transaction.
    exp = export(("2026-09-01 06:00:00", "To Savings", "-500", "COMPLETED", "Transfer"))
    e = entry("YNAB:-500000:2026-08-31:1", csv_payee="To Savings", started_at="2026-08-31 18:00:00")
    p = plan(exp, [e], [ytx_for(e, cleared="cleared")])
    assert p.matches == []
    assert kinds(p) == [("CREATE", "new")]


def test_pass1_needs_equal_amounts() -> None:
    exp = export(("2026-10-05 08:00:00", "Shop", "-1.01"))
    e = entry("YNAB:-1000:2026-10-05:1", started_at="2026-10-05 08:00:00", csv_state="COMPLETED")
    p = plan(exp, [e], [ytx_for(e, cleared="cleared")])
    assert all(m.method is not MatchMethod.TIMESTAMP for m in p.matches)


def test_pass1_prefers_zero_offset_over_a_learned_one() -> None:
    # Row R1 could be entry X at +9h or entry Y at 0: d = 0 wins.
    exp = export(
        ("2026-10-05 17:00:00", "Shop", "-1.00"),
        ("2026-10-04 20:00:00", "Other", "-7.00"),
    )
    learn = entry(
        "YNAB:-7000:2026-10-04:1", csv_payee="Other", started_at="2026-10-04 11:00:00"
    )  # teaches +9h
    x = entry("YNAB:-1000:2026-10-05:2", started_at="2026-10-05 08:00:00")
    y = entry("YNAB:-1000:2026-10-05:3", started_at="2026-10-05 17:00:00")
    p = plan(exp, [learn, x, y], [ytx_for(e, cleared="cleared") for e in (learn, x, y)])
    by_row = {m.row_key: (m.entry_key, m.offset_seconds) for m in p.matches}
    assert by_row["YNAB:-1000:2026-10-05:1"] == (y.key, 0)


def test_pass1_prefers_the_most_frequent_offset() -> None:
    # +9h is seen twice, +2h once: the ambiguous row goes to the +9h entry.
    exp = export(
        ("2026-10-05 17:00:00", "A", "-1.00"),
        ("2026-10-05 18:00:00", "B", "-2.00"),
        ("2026-10-05 13:00:00", "C", "-3.00"),
        ("2026-10-06 10:00:00", "D", "-9.00"),
    )
    a = entry("YNAB:-1000:2026-10-05:1", csv_payee="A", started_at="2026-10-05 08:00:00")
    b = entry("YNAB:-2000:2026-10-05:1", csv_payee="B", started_at="2026-10-05 09:00:00")
    c = entry("YNAB:-3000:2026-10-05:1", csv_payee="C", started_at="2026-10-05 11:00:00")
    d9 = entry("YNAB:-9000:2026-10-06:7", csv_payee="X", started_at="2026-10-06 01:00:00")
    d2 = entry("YNAB:-9000:2026-10-06:8", csv_payee="X", started_at="2026-10-06 08:00:00")
    entries = [a, b, c, d9, d2]
    p = plan(exp, entries, [ytx_for(e, cleared="cleared") for e in entries])
    assert p.offsets == {2 * H: 1, 9 * H: 2}
    by_row = {m.row_key: m.entry_key for m in p.matches}
    assert by_row["YNAB:-9000:2026-10-06:1"] == d9.key


def test_pass1_tie_broken_by_key_equality_then_file_order() -> None:
    exp = export(
        ("2026-10-05 08:00:00", "Shop", "-1.00"),
        ("2026-10-05 08:00:00", "Shop", "-1.00"),
    )
    e1 = entry("YNAB:-1000:2026-10-05:2", started_at="2026-10-05 08:00:00")
    e2 = entry("YNAB:-1000:2026-10-05:9", started_at="2026-10-05 08:00:00")
    p = plan(exp, [e1, e2], [ytx_for(e, cleared="cleared") for e in (e1, e2)])
    by_row = {m.row_key: m.entry_key for m in p.matches}
    # Row :2 has an entry with its own key; row :1 (first in file) takes the other one.
    assert by_row == {"YNAB:-1000:2026-10-05:2": e1.key, "YNAB:-1000:2026-10-05:1": e2.key}


def test_pass1_prefers_a_live_entry_over_a_removed_one_with_the_same_timestamp() -> None:
    exp = export(("2026-10-05 08:00:00", "Shop", "-1.00"))
    removed = entry(
        "YNAB:-1000:2026-10-05:1",
        status="SUPERSEDED",
        ynab_tx_id="y-old",
        started_at="2026-10-05 08:00:00",
    )
    live = entry("YNAB:-1000:2026-10-05:2", started_at="2026-10-05 08:00:00")
    p = plan(exp, [removed, live], [ytx_for(live, cleared="cleared")])
    assert methods(p) == [("YNAB:-1000:2026-10-05:1", live.key, "timestamp")]
    assert kinds(p) == []  # never "re-created" from the tombstone while the live one exists


def test_pass1_each_side_matched_at_most_once() -> None:
    exp = export(("2026-10-05 08:00:00", "Shop", "-1.00"), ("2026-10-05 08:00:00", "Shop", "-1.00"))
    e = entry("YNAB:-1000:2026-10-05:1", started_at="2026-10-05 08:00:00")
    p = plan(exp, [e], [ytx_for(e, cleared="cleared")])
    assert len(p.matches) == 1
    assert kinds(p) == [("CREATE", "new")]


# ── pass 2: key identity (legacy) ────────────────────────────────────────────


def test_pass2_legacy_entry_matched_by_key_and_backfilled() -> None:
    exp = export(("2026-10-05 08:00:00", "Coop", "-12.50"))
    legacy = entry(
        "YNAB:-12500:2026-10-05:1",
        identity="legacy",
        started_at=None,
        csv_payee="Coop (renamed in YNAB)",
        csv_type=None,
    )
    p = plan(exp, [legacy], [ytx_for(legacy, cleared="reconciled")])
    assert methods(p) == [(legacy.key, legacy.key, "key")]
    (action,) = p.actions
    assert action.kind.value == "LEDGER"
    assert dict(action.ledger_updates) == {
        "csv_payee": "Coop",
        "csv_type": "Card Payment",
        "identity": "csv",
        "started_at": "2026-10-05 08:00:00",
    }


def test_pass2_key_collision_with_both_timestamps_is_not_a_match() -> None:
    # Same key, both timestamped, 9h33m apart (not an offset): renumbering (C01).
    exp = export(("2026-10-05 17:40:00", "SBB", "-3.60"))
    e = entry(
        "YNAB:-3600:2026-10-05:1",
        csv_payee="SBB",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-10-05 08:07:00",
        csv_date="2026-10-04",  # outside the window: the sweep is not involved
    )
    p = plan(exp, [e], [ytx_for(e)])
    assert p.matches == []
    create = p.by_kind(ActionKind.CREATE)[0]
    assert create.import_id == "YNAB:-3600:2026-10-05:2"  # key used by the ledger → next n


def test_pass2_date_only_row_matches_a_timestamped_entry_by_key() -> None:
    exp = export(("2021-03-02", "Coop", "-12.5"))
    e = entry("YNAB:-12500:2021-03-02:1", csv_payee="Coop", started_at="2021-03-02 10:00:00")
    p = plan(exp, [e], [ytx_for(e, cleared="cleared")])
    assert methods(p) == [(e.key, e.key, "key")]
    # A date-only row never overwrites a real timestamp with a midnight placeholder.
    assert all("started_at" not in a.ledger_updates for a in p.actions)


def test_F02_rekeyed_tombstones_are_never_matched() -> None:
    exp = export(("2026-10-05 08:00:00", "Shop", "-1.00"))
    tomb = entry(
        "YNAB:-1000:2026-10-05:1",
        status="REKEYED",
        ynab_tx_id="y-1",
        rekeyed_to="YNAB:-1000:2026-10-06:1",
        started_at=None,
    )
    p = plan(exp, [tomb], [])
    assert p.matches == []
    create = p.by_kind(ActionKind.CREATE)[0]
    assert create.import_id == "YNAB:-1000:2026-10-05:2"  # the tombstone's key stays reserved


# ── pass 3: legacy date shift ────────────────────────────────────────────────

_SHIFT_ROW = ("2026-10-06 01:15:00", "Kiosk Bern", "-3.50")
_IN_WINDOW = ("2026-10-04 12:00:00", "Other", "-9.00")  # stretches the window back


def _legacy(day: str = "2026-10-05", **kw):
    values = {
        "identity": "legacy",
        "started_at": None,
        "csv_payee": " kiosk BERN ",
        "csv_date": day,
    }
    values.update(kw)
    return entry(f"YNAB:-3500:{day}:1", **values)


def test_pass3_matches_a_legacy_row_shifted_across_midnight() -> None:
    exp = export(_IN_WINDOW, _SHIFT_ROW)
    e = _legacy()
    p = plan(exp, [e], [ytx_for(e, cleared="cleared")])
    assert methods(p) == [("YNAB:-3500:2026-10-06:1", e.key, "date_shift")]
    (update,) = p.by_kind(ActionKind.UPDATE)
    assert dict(update.fields) == {"id": e.ynab_tx_id, "date": "2026-10-06"}


@pytest.mark.parametrize(
    ("rows", "entry_kw", "why"),
    [
        (
            (_IN_WINDOW, _SHIFT_ROW),
            {"identity": "csv", "started_at": "2026-10-05 16:15:00"},
            "a csv entry with started_at is identified by its timestamp only",
        ),
        (
            (_IN_WINDOW, _SHIFT_ROW),
            {"identity": "csv", "csv_payee": "Kiosk Zurich"},
            "csv entry: CSV payee differs",
        ),
        ((_IN_WINDOW, _SHIFT_ROW), {"csv_amount": -3600}, "amount differs"),
        ((_IN_WINDOW, ("2026-10-07 01:15:00", "Kiosk Bern", "-3.50")), {}, "two days apart"),
        (
            (_IN_WINDOW, _SHIFT_ROW, ("2026-10-05 12:00:00", "Kiosk Bern", "-3.50")),
            {},
            "entry key still in the CSV",
        ),
        ((_SHIFT_ROW,), {}, "entry date outside the window (F10)"),
        ((_IN_WINDOW, ("2026-10-06 09:15:00", "Kiosk Bern", "-3.50")), {}, "not near midnight"),
        (
            (("2026-10-04 23:15:00", "Kiosk Bern", "-3.50"), _IN_WINDOW, _SHIFT_ROW),
            {},
            "two candidate rows",
        ),
        ((_IN_WINDOW, ("2026-10-06 22:15:00", "Kiosk Bern", "-3.50")), {}, "wrong direction"),
    ],
)
def test_pass3_conditions(rows, entry_kw, why) -> None:
    exp = export(*rows)
    e = _legacy(**entry_kw)
    p = plan(exp, [e], [ytx_for(e, cleared="cleared")])
    assert [m for m in p.matches if m.method is MatchMethod.DATE_SHIFT] == [], why


@pytest.mark.parametrize(
    "entry_kw",
    [
        # v1 copied started_at by key, so a legacy entry's timestamp may be a sibling's.
        {"started_at": "2026-10-05 09:00:00"},
        # F11: v1's web sync copied YNAB's renamed payee into the legacy row.
        {"csv_payee": "Kiosk"},
    ],
)
def test_pass3_legacy_entries_need_no_timestamp_and_no_csv_payee(entry_kw) -> None:
    exp = export(_IN_WINDOW, _SHIFT_ROW)
    e = _legacy(**entry_kw)
    p = plan(exp, [e], [ytx_for(e, cleared="cleared")])
    assert methods(p) == [("YNAB:-3500:2026-10-06:1", e.key, "date_shift")]
    assert [a.payload["payee_name"] for a in p.by_kind(ActionKind.CREATE)] == ["Other"]


def test_pass3_two_candidate_entries_is_ambiguous() -> None:
    exp = export(_IN_WINDOW, _SHIFT_ROW)
    e1 = _legacy("2026-10-05")
    e2 = dataclasses.replace(e1, key="YNAB:-3500:2026-10-05:2", ynab_tx_id="y-second")
    p = plan(exp, [e1, e2], [ytx_for(e, cleared="cleared") for e in (e1, e2)])
    assert [m for m in p.matches if m.method is MatchMethod.DATE_SHIFT] == []
    # Control: with only one of them the row is matched.
    p = plan(exp, [e1], [ytx_for(e1, cleared="cleared")])
    assert [m.method for m in p.matches] == [MatchMethod.DATE_SHIFT]


def test_F10_window_boundary_legacy_date_shift_is_not_matched() -> None:
    # Legacy SBB -2.80 on 08-31 22:40; the next export starts 09-01 and has a different
    # SBB -2.80 at 09-01 22:15. v1 re-dated the old one and created nothing.
    exp = export(("2026-09-01 22:15:00", "SBB CFF FFS", "-2.80"))
    legacy = entry(
        "YNAB:-2800:2026-08-31:1",
        identity="legacy",
        started_at=None,
        csv_payee="SBB CFF FFS",
    )
    p = plan(exp, [legacy], [ytx_for(legacy, cleared="cleared")])
    assert p.matches == []
    assert [(a.kind.value, a.import_id) for a in p.actions] == [
        ("CREATE", "YNAB:-2800:2026-09-01:1")
    ]


# ── pass 4: settlement ───────────────────────────────────────────────────────


def _pending(**kw):
    values = {
        "csv_payee": "Amazon EU",
        "csv_state": "PENDING",
        "pushed_cleared": "uncleared",
        "started_at": "2026-10-11 22:15:00",
    }
    values.update(kw)
    return entry("YNAB:-89990:2026-10-11:1", **values)


_SETTLED = ("2026-10-11 22:15:00", "Amazon EU", "-90.41")
_RETIMED = ("2026-10-12 06:00:00", "Amazon EU", "-90.41")  # settled under a new timestamp
_PAD = ("2026-10-10 09:00:00", "Pad", "-1.00")


def test_pass4_settlement_updates_the_amount() -> None:
    exp = export(_PAD, _SETTLED)
    e = _pending()
    p = plan(exp, [e], [ytx_for(e, memo="(pending)")])
    assert methods(p) == [("YNAB:-90410:2026-10-11:1", e.key, "settlement")]
    assert kinds(p) == [("CREATE", "new"), ("UPDATE", "settlement")]
    update = p.by_kind(ActionKind.UPDATE)[0]
    assert dict(update.fields) == {
        "id": e.ynab_tx_id,
        "amount": -90410,
        "cleared": "cleared",
        "memo": "",
    }


@pytest.mark.parametrize(
    ("rows", "entry_kw", "why"),
    [
        ((_PAD, _SETTLED), {"csv_state": "COMPLETED"}, "entry not pending"),
        ((_PAD, _SETTLED), {"status": "SUPERSEDED", "ynab_tx_id": None}, "entry removed by us"),
        ((_PAD, _SETTLED), {"status": "REVERTED", "ynab_tx_id": None}, "entry reverted"),
        ((_PAD, _SETTLED), {"csv_payee": "Amazon US"}, "payee differs"),
        ((_PAD, ("2026-10-13 22:15:00", "Amazon EU", "-90.41")), {}, "two days apart"),
        (
            (_SETTLED,),
            {"csv_date": "2026-10-10", "started_at": "2026-10-10 22:15:00"},
            "entry date outside window",
        ),
        ((_PAD, ("2026-10-11 22:15:00", "Amazon EU", "90.41")), {}, "sign flip (refund)"),
        (
            (_PAD, _RETIMED, ("2026-10-12 10:00:00", "Amazon EU", "-12.00")),
            {},
            "two candidate rows",
        ),
        (
            (_PAD, ("2026-10-12 09:00:00", "Amazon EU", "-90.41", "REVERTED")),
            {},
            "REVERTED row at another instant",
        ),
    ],
)
def test_pass4_conditions(rows, entry_kw, why) -> None:
    exp = export(*rows)
    e = _pending(**entry_kw)
    txs = [ytx_for(e)] if e.ynab_tx_id else []
    p = plan(exp, [e], txs)
    assert [m for m in p.matches if m.method is MatchMethod.SETTLEMENT] == [], why


def test_pass4_requoted_then_reverted_pending_is_matched_and_deleted() -> None:
    # The pending was re-quoted by FX (-89.99 → -90.41) and then reverted: the REVERTED row
    # carries the new amount at the pending's very instant.
    exp = export(_PAD, ("2026-10-11 22:15:00", "Amazon EU", "-90.41", "REVERTED"))
    e = _pending()
    p = plan(exp, [e], [ytx_for(e, memo="(pending)")])
    assert methods(p) == [("YNAB:-90410:2026-10-11:1", e.key, "settlement")]
    (delete,) = p.by_kind(ActionKind.DELETE)
    assert (delete.reason, delete.ynab_tx_id) == ("reverted", e.ynab_tx_id)
    assert delete.ledger_updates["status"] == "REVERTED"


def test_pass4_window_test_shifts_the_entry_by_the_export_offset() -> None:
    # A pending first seen in a Los Angeles rendering (09-01 15:49:04) settles; the next
    # (Zurich, +9h) statement starts 09-02. The entry's own date is outside the window but its
    # instant in the statement's rendering (09-02 00:49:04) is inside: still a settlement.
    fillers = [(f"2026-09-0{d} 12:0{d}:1{d}", f"Shop{d}", f"-1{d}.25") for d in range(2, 6)]
    exp = export(*fillers, ("2026-09-02 00:49:04", "Amazon EU", "-196.65"))
    filler_entries = [
        entry_for(r, started_at=(r.started_at - timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S"))
        for r in exp.rows
        if r.payee.startswith("Shop")
    ]
    pending = entry(
        "YNAB:-194130:2026-09-01:1",
        csv_payee="Amazon EU",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-09-01 15:49:04",
    )
    entries = [*filler_entries, pending]
    p = plan(exp, entries, [ytx_for(e) for e in entries])
    assert p.frame_offset == 9 * H
    assert [m.method for m in p.matches if m.entry_key == pending.key] == [MatchMethod.SETTLEMENT]
    assert not p.by_kind(ActionKind.CREATE)


def test_pass4_same_instant_candidate_wins_over_other_rows() -> None:
    exp = export(_PAD, _SETTLED, ("2026-10-12 10:00:00", "Amazon EU", "-12.00"))
    e = _pending()
    p = plan(exp, [e], [ytx_for(e)])
    assert methods(p) == [("YNAB:-90410:2026-10-11:1", e.key, "settlement")]


def test_pass4_two_candidate_entries_is_ambiguous() -> None:
    exp = export(_PAD, _RETIMED, ("2026-10-12 11:00:00", "Pad", "-1.00"))
    e1 = _pending()
    e2 = entry(
        "YNAB:-50000:2026-10-12:1",
        csv_payee="amazon eu",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-10-12 10:00:00",
    )
    p = plan(exp, [e1, e2], [ytx_for(e1), ytx_for(e2)])
    assert [m for m in p.matches if m.method is MatchMethod.SETTLEMENT] == []
    # Control: with only one of them the row is matched.
    p = plan(exp, [e1], [ytx_for(e1)])
    assert [m.method for m in p.matches] == [MatchMethod.SETTLEMENT]


# ── scope ────────────────────────────────────────────────────────────────────


def test_F12_other_accounts_entries_are_ignored() -> None:
    exp = export(("2026-10-05 08:00:00", "Shop", "-1.00"))
    foreign = entry(
        "YNAB:-1000:2026-10-05:1", account_id=OTHER_ACCT, started_at="2026-10-05 08:00:00"
    )
    p = plan(exp, [foreign], [])
    assert p.matches == []
    create = p.by_kind(ActionKind.CREATE)[0]
    # import_ids are unique per account: the other account's key does not block this one.
    assert create.import_id == "YNAB:-1000:2026-10-05:1"
    assert create.payload["account_id"] == ACCT
    assert p.counters["ledger_entries"] == 0


def test_unclaimed_legacy_entries_are_considered_but_the_accounts_own_entry_wins() -> None:
    exp = export(("2026-10-05 08:00:00", "Shop", "-1.00"), ("2026-10-06 09:00:00", "Shop", "-2.00"))
    own = entry("YNAB:-1000:2026-10-05:1", started_at=None, identity="legacy")
    shadow = entry(
        "YNAB:-1000:2026-10-05:1",
        account_id="",
        started_at=None,
        identity="legacy",
        ynab_tx_id="y-shadow",
    )
    unclaimed = entry("YNAB:-2000:2026-10-06:1", account_id="", started_at=None, identity="legacy")
    p = plan(exp, [shadow, own, unclaimed], [ytx_for(own, cleared="cleared"), ytx_for(unclaimed)])
    assert {(m.entry_key, m.method.value) for m in p.matches} == {
        (own.key, "key"),
        (unclaimed.key, "key"),
    }
    by_key = {a.entry_key: a for a in p.actions}
    assert by_key[own.key].account_id == ACCT
    assert by_key[unclaimed.key].account_id == ""


def test_matching_is_by_csv_side_fields_only() -> None:
    # F11: YNAB renamed the payee and the user changed the date; matching is unaffected.
    exp = export(_PAD, _SETTLED)
    e = _pending()
    y = ytx_for(e, payee_name="Amazon", memo="(pending)")
    p = plan(exp, [e], [y])
    assert [m.method for m in p.matches] == [MatchMethod.SETTLEMENT]


def test_row_by_helper_finds_rows() -> None:
    exp = export(_PAD, _SETTLED)
    assert row_by(exp, started="2026-10-11 22:15:00").amount == -90410
    assert entry_for(exp.rows[0]).csv_payee == "Pad"


# ── pass 2: legacy (amount, date) groups are assigned as a whole ─────────────


def _legacy_entry(key: str, state: str, payee: str = "SBB CFF FFS", **kw):
    values = {
        "identity": "legacy",
        "started_at": None,
        "csv_payee": payee,
        "csv_state": state,
        "pushed_cleared": "cleared" if state == "COMPLETED" else "uncleared",
    }
    values.update(kw)
    return entry(key, **values)


def test_C01_legacy_entries_settled_sibling_takes_the_completed_entry() -> None:
    # Legacy ledger (no timestamps): 08:10 pending (:1) and 17:40 completed (:2). The pending
    # settled at -3.80, so the 17:40 ride now carries key :1.
    k1 = _legacy_entry("YNAB:-3600:2026-10-05:1", "PENDING")
    k2 = _legacy_entry("YNAB:-3600:2026-10-05:2", "COMPLETED")
    exp = export(
        ("2026-10-04 09:00:00", "Pad", "-1.00"),
        ("2026-10-05 17:40:00", "SBB CFF FFS", "-3.60"),
        ("2026-10-05 08:10:00", "SBB CFF FFS", "-3.80"),
    )
    assert row_by(exp, started="2026-10-05 17:40:00").key == k1.key
    y1 = ytx_for(k1, memo="(pending)")
    y2 = ytx_for(k2)
    p = plan(exp, [k1, k2], [y1, y2])
    by_entry = {m.entry_key: (m.row_key, m.method.value) for m in p.matches}
    assert by_entry == {
        k2.key: ("YNAB:-3600:2026-10-05:1", "key"),
        k1.key: ("YNAB:-3800:2026-10-05:1", "settlement"),
    }
    (update,) = p.by_kind(ActionKind.UPDATE)
    assert dict(update.fields) == {"id": y1.id, "cleared": "cleared", "memo": "", "amount": -3800}
    assert [a.payload["payee_name"] for a in p.by_kind(ActionKind.CREATE)] == ["Pad"]


def test_pass2_new_pending_shifting_occurrences_is_created_not_matched() -> None:
    k1 = _legacy_entry("YNAB:-4500:2026-10-05:1", "COMPLETED", payee="Starbucks")
    k2 = _legacy_entry("YNAB:-4500:2026-10-05:2", "COMPLETED", payee="Starbucks")
    exp = export(
        ("2026-10-05 08:00:00", "Starbucks", "-4.50"),
        ("2026-10-05 10:00:00", "Starbucks", "-4.50", "PENDING"),
        ("2026-10-05 12:00:00", "Starbucks", "-4.50"),
    )
    assert [r.key[-1] for r in exp.rows] == ["1", "2", "3"]
    p = plan(exp, [k1, k2], [ytx_for(k1), ytx_for(k2)])
    by_row = {m.row_key: m.entry_key for m in p.matches}
    assert by_row == {"YNAB:-4500:2026-10-05:1": k1.key, "YNAB:-4500:2026-10-05:3": k2.key}
    (create,) = p.by_kind(ActionKind.CREATE)
    assert (create.row.started_at_text, create.payload["cleared"]) == (
        "2026-10-05 10:00:00",
        "uncleared",
    )
    assert create.import_id == "YNAB:-4500:2026-10-05:3"  # :1/:2 are taken by the ledger


def test_pass2_group_prefers_the_same_csv_payee() -> None:
    coop = _legacy_entry("YNAB:-3600:2026-10-05:1", "COMPLETED", payee="Coop")
    sbb = _legacy_entry("YNAB:-3600:2026-10-05:2", "COMPLETED", payee="SBB")
    # This export lists the SBB ride first, so the keys are swapped.
    exp = export(
        ("2026-10-05 07:00:00", "SBB", "-3.60"),
        ("2026-10-05 09:00:00", "Coop", "-3.60"),
    )
    p = plan(exp, [coop, sbb], [ytx_for(coop), ytx_for(sbb)])
    assert {m.row_key: m.entry_key for m in p.matches} == {
        "YNAB:-3600:2026-10-05:1": sbb.key,
        "YNAB:-3600:2026-10-05:2": coop.key,
    }
    assert p.ynab_actions == []


def test_pass2_group_with_matching_counts_keeps_key_identity() -> None:
    k1 = _legacy_entry("YNAB:-3600:2026-10-05:1", "PENDING")
    k2 = _legacy_entry("YNAB:-3600:2026-10-05:2", "COMPLETED")
    exp = export(
        ("2026-10-05 08:10:00", "SBB CFF FFS", "-3.60"),
        ("2026-10-05 17:40:00", "SBB CFF FFS", "-3.60"),
    )
    p = plan(exp, [k1, k2], [ytx_for(k1, memo="(pending)"), ytx_for(k2)])
    assert {m.row_key: m.entry_key for m in p.matches} == {k1.key: k1.key, k2.key: k2.key}
    (update,) = p.by_kind(ActionKind.UPDATE)
    assert update.entry_key == k1.key and update.reason == "cleared"


@pytest.mark.parametrize("deleted_key", ["YNAB:-4500:2026-08-09:1", "YNAB:-4500:2026-08-09:2"])
def test_pass2_group_prefers_a_live_pending_over_a_deleted_entry(deleted_key: str) -> None:
    # Review finding: a DELETED_IN_YNAB legacy entry (COMPLETED) took the completed row ahead
    # of the live pending it belongs to; the sweep then deleted the live pending and the
    # coffee disappeared from YNAB. Live entries rank first, whichever key either holds.
    live_key = next(
        k for k in ("YNAB:-4500:2026-08-09:1", "YNAB:-4500:2026-08-09:2") if k != deleted_key
    )
    dead = _legacy_entry(
        deleted_key, "COMPLETED", payee="Starbucks", status="DELETED_IN_YNAB", ynab_tx_id="y-dead"
    )
    live = _legacy_entry(live_key, "PENDING", payee="Starbucks")
    exp = export(
        ("2026-08-08 09:00:00", "Pad", "-1.00"),
        ("2026-08-09 15:00:00", "Starbucks", "-4.50"),
        ("2026-08-10 09:00:00", "Pad", "-2.00"),
    )
    y_live = ytx_for(live, memo="(pending)", category_name="Coffee")
    p = plan(exp, [dead, live], [ytx_for(dead, deleted=True, cleared="cleared"), y_live])
    assert [m.entry_key for m in p.matches] == [live.key]
    assert [(a.kind.value, a.reason, a.ynab_tx_id) for a in p.ynab_actions if a.ynab_tx_id] == [
        ("UPDATE", "cleared", y_live.id)
    ]
    assert not p.by_kind(ActionKind.DELETE)


def test_pass2_v1_entries_sharing_a_started_at_are_assigned_as_a_group() -> None:
    # Review blocker: v1 wrote started_at by key and renumbering moved keys between rows, so
    # two legacy entries of one (amount, date) group can carry the same (sibling's) timestamp.
    # One row matches by timestamp; the other must still find the remaining entry.
    shared = "2026-09-03 17:43:25"
    k1 = _legacy_entry("YNAB:-3600:2026-09-03:1", "COMPLETED", started_at=shared)
    k2 = _legacy_entry("YNAB:-3600:2026-09-03:2", "COMPLETED", started_at=shared)
    exp = export(
        ("2026-09-03 17:43:25", "SBB CFF FFS", "-3.60"),
        ("2026-09-03 08:30:04", "SBB CFF FFS", "-3.60"),
    )
    p = plan(exp, [k1, k2], [ytx_for(k1), ytx_for(k2)])
    assert {m.entry_key for m in p.matches} == {k1.key, k2.key}
    assert p.ynab_actions == []
    # Both entries now carry their own row's timestamp.
    started = {a.entry_key: a.ledger_updates.get("started_at") for a in p.actions}
    assert sorted(v or shared for v in started.values()) == [
        "2026-09-03 08:30:04",
        "2026-09-03 17:43:25",
    ]


def test_pass2_date_only_rows_match_by_the_entries_current_amount() -> None:
    # Review finding: after a settlement changed csv_amount, re-importing the same date-only
    # statement found nothing (groups came from the key) and created the row again.
    settled = entry(
        "YNAB:-7250:2021-03-04:1",
        csv_payee="Migros",
        csv_amount=-7400,
        csv_state="COMPLETED",
        started_at=None,
    )
    exp = export(("2021-03-01", "Pad", "-1.00"), ("2021-03-04", "Migros", "-7.40"))
    p = plan(exp, [settled], [ytx_for(settled)])
    assert [(m.entry_key, m.method.value) for m in p.matches] == [(settled.key, "key")]
    assert [a.payload["payee_name"] for a in p.by_kind(ActionKind.CREATE)] == ["Pad"]
    # A timestamped entry is matched by a date-only row the same way.
    timed = dataclasses.replace(settled, started_at="2021-03-04 22:15:00")
    p = plan(exp, [timed], [ytx_for(timed)])
    assert [m.entry_key for m in p.matches] == [timed.key]


def test_pass1_ranks_zero_by_evidence_in_a_shifted_statement() -> None:
    # A statement rendered -9h: a round-time row coincides (d = 0) with an unrelated entry of
    # the same amount; the true offset has far more evidence and wins.
    fillers = [(f"2026-09-1{d} 03:0{d}:1{d}", f"Shop{d}", f"-1{d}.25") for d in range(1, 5)]
    exp = export(*fillers, ("2026-09-15 04:30:00", "Uber", "-4.50"))
    shifted = [
        entry_for(r, started_at=(r.started_at + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S"))
        for r in exp.rows
        if r.payee.startswith("Shop")
    ]
    uber = entry("YNAB:-4500:2026-09-15:1", csv_payee="Uber", started_at="2026-09-15 13:30:00")
    tanta = entry("YNAB:-4500:2026-09-15:2", csv_payee="Tanta", started_at="2026-09-15 04:30:00")
    entries = [*shifted, uber, tanta]
    p = plan(exp, entries, [ytx_for(e, cleared="cleared") for e in entries])
    assert {m.entry_key: m.offset_seconds for m in p.matches if "4500" in m.entry_key} == {
        uber.key: -9 * H
    }
