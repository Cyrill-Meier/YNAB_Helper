"""Truth-based import sequences through the real ledger store (planner review regressions).

Real Revolut transactions (:class:`T`) are rendered into statements — in any timezone and
window, as Revolut renders them — and imported one after another through :class:`StoreSim`
(SQLite ledger, ``refresh_from_mirror`` before each plan, "duplicate import_id" answers
resolved after the creates). After every import YNAB must show exactly the live transactions
(:func:`truth`), and importing the same statement again must not write anything.

Covered: older statements (F36, also inside the one-day ``allow_sweep`` slack), ledger writes
lost after a POST with a fresh import_id, date-only statements after a settlement, timezone
shifts (window tests in the statement's rendering, offsets learned from midnight-crossing
rows, renderings up to 26 h apart), adoption guards, and a seeded random month of statements.
"""

from __future__ import annotations

import copy
import random
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from ynab_helper.importer import ActionKind, MatchMethod, Plan
from ynab_helper.revolut import parse_export

from .helpers import (
    HEADER,
    Sim,
    StoreSim,
    T,
    at,
    done,
    entry,
    export,
    plan,
    render,
    truth,
    ytx,
    ytx_for,
)


@pytest.fixture
def sim(tmp_path: Path) -> Iterator[StoreSim]:
    s = StoreSim(tmp_path)
    yield s
    s.close()


def _import(sim: StoreSim, txs: list[T], **render_kw) -> Plan:
    """Import the statement of ``txs``; YNAB must then show the truth, and importing the same
    statement again must write nothing."""
    exp = render(txs, **render_kw)
    p = sim.run(exp)
    assert sim.state() == truth(txs), [(a.kind.value, a.reason) for a in p.ynab_actions]
    again = sim.writes(exp)
    assert again == [], again
    return p


def _live(sim: StoreSim, payee: str) -> list:
    return [t for t in sim.live() if t.payee_name == payee]


# ── a realistic fortnight ────────────────────────────────────────────────────


def test_daily_statements_with_settlements_requotes_holds_and_reverts(sim: StoreSim) -> None:
    sbb1 = T(at("2026-10-01 08:10:05"), "SBB CFF FFS", "-3.60")
    hotel = T(at("2026-10-01 15:00:00"), "Hotel Bellevue", "-200.00")
    amzn1 = T(at("2026-10-01 21:15:44"), "Amazon EU", "-30.00")
    amzn2 = T(at("2026-10-01 21:16:10"), "Amazon EU", "-40.00")
    txs = [
        done("2026-10-01 08:01:12", "Starbucks", "-4.50"),
        sbb1,
        done("2026-10-01 17:40:33", "SBB CFF FFS", "-3.60"),
        hotel,
        amzn1,
        amzn2,
    ]
    _import(sim, txs, end="2026-10-01")
    # Day 2: the 08:10 ride settles at -3.80 and takes another key (C01); an FX re-quote of a
    # pending; a restaurant pending; two coffees.
    sbb1.complete(at("2026-10-02 06:00:00"), "-3.80")
    amzn1.amount = "-30.10"
    rest = T(at("2026-10-02 20:30:00"), "Tanta", "-50.00")
    coffee = T(at("2026-10-02 15:00:30"), "Starbucks", "-4.50")
    txs += [rest, done("2026-10-02 08:00:30", "Starbucks", "-4.50"), coffee]
    _import(sim, txs, end="2026-10-02")
    # Day 3: the hotel hold is released and charged as a new row; the restaurant settles with
    # the tip in the same row; the re-quoted pending settles; the coffee completes.
    hotel.state = "REVERTED"
    rest.complete(at("2026-10-03 05:00:00"), "-57.50")
    amzn1.complete(at("2026-10-03 07:00:00"), "-30.15")
    coffee.complete(at("2026-10-03 07:00:01"))
    txs.append(done("2026-10-03 11:00:00", "Hotel Bellevue", "-180.00"))
    _import(sim, txs, end="2026-10-03")
    # Day 4/5: another settlement; a card check reverted; a ride settled with a surcharge.
    amzn2.complete(at("2026-10-04 06:00:00"), "-40.20")
    check = T(at("2026-10-04 12:00:00"), "Uber", "-1.00")
    uber = T(at("2026-10-04 12:30:00"), "Uber", "-23.40")
    txs += [check, uber]
    _import(sim, txs, end="2026-10-04")
    check.state = "REVERTED"
    uber.complete(at("2026-10-05 06:00:00"), "-24.10")
    _import(sim, txs, end="2026-10-05")
    # A 7-day window that no longer reaches day 1-2 changes nothing.
    _import(sim, txs, start="2026-10-03", end="2026-10-05")


# ── F36: older statements, also inside the one-day allow_sweep slack ─────────


def test_F36_older_statement_from_the_same_day_never_sweeps_a_later_pending(
    sim: StoreSim,
) -> None:
    txs = [
        done("2026-10-01 09:00:00", "Pad", "-1.00"),
        done("2026-10-10 08:00:00", "Coop", "-4.50"),
    ]
    morning = render(txs, end="2026-10-10")  # generated 10-10 around 09:00
    sim.run(morning)
    hotel = T(at("2026-10-10 20:00:00"), "Hotel", "-150.00")
    txs.append(hotel)
    _import(sim, txs, end="2026-10-10")  # the evening statement
    (y,) = _live(sim, "Hotel")
    sim.user_edit(y.id, category_name="Travel", memo="(pending) | conf #123")
    p = sim.run(morning)  # the morning file is sent again by mistake
    assert p.ynab_actions == []
    assert p.stale_export and not p.allow_sweep
    assert sim.txs[y.id].category_name == "Travel" and not sim.txs[y.id].deleted
    assert sim.state() == truth(txs)


def test_F36_older_statement_from_the_previous_day_never_sweeps_a_later_pending(
    sim: StoreSim,
) -> None:
    txs = [done("2026-10-10 08:00:00", "Coop", "-10.00")]
    older = render(txs, end="2026-10-10")
    sim.run(older)
    txs += [
        T(at("2026-10-10 20:00:00"), "Hotel", "-300.00"),
        done("2026-10-11 08:00:00", "Coop", "-5.00"),
    ]
    _import(sim, txs, end="2026-10-11")
    p = sim.run(older)
    assert p.ynab_actions == [] and p.stale_export
    assert sim.state() == truth(txs)


def test_F36_older_statement_never_resurrects_a_superseded_pending(sim: StoreSim) -> None:
    hold = T(at("2026-10-08 15:00:00"), "Hotel Bellevue", "-300.00")
    txs = [
        done("2026-10-08 10:00:00", "Coop", "-10.00"),
        hold,
        done("2026-10-09 10:00:00", "Denner", "-4.00"),
    ]
    old_txs = copy.deepcopy(txs)
    _import(sim, txs, end="2026-10-09")
    hold.state = "GONE"  # released; the final charge comes two days later as a new row
    txs.append(done("2026-10-10 11:00:13", "Hotel Bellevue", "-280.00"))
    _import(sim, txs, end="2026-10-10")
    n = len(sim.calls)
    p = sim.run(render(old_txs, end="2026-10-09"))  # yesterday's file, inside the slack
    assert sim.calls[n:] == [] and p.ynab_actions == []
    assert sim.state() == truth(txs)


@pytest.mark.parametrize("later_days", [0, 13])
def test_F36_older_statement_never_reverts_a_requoted_pending(
    sim: StoreSim, later_days: int
) -> None:
    deposit = T(at("2026-10-01 10:00:00"), "Hertz", "-500.00")
    txs = [deposit, done("2026-10-02 10:00:00", "Coop", "-10.00")]
    old_txs = copy.deepcopy(txs)
    _import(sim, txs, end="2026-10-03")
    deposit.amount = "-512.40"  # FX re-quote of the long-running hold
    newer_day = datetime(2026, 10, 3) + timedelta(days=later_days)
    txs.append(done(f"{newer_day:%Y-%m-%d} 10:00:00", "Migros", "-20.00"))
    _import(sim, txs, end=f"{newer_day:%Y-%m-%d}")
    n = len(sim.calls)
    p = sim.run(render(old_txs, end="2026-10-03"))
    assert sim.calls[n:] == [] and p.stale_export
    assert sim.state() == truth(txs)


def test_F36_older_statement_after_a_redated_settlement_creates_no_phantom_pending(
    sim: StoreSim,
) -> None:
    pending = T(at("2026-10-09 21:00:00"), "Tanta", "-50.00")
    txs = [done("2026-10-08 10:00:00", "Coop", "-10.00"), pending]
    old_txs = copy.deepcopy(txs)
    _import(sim, txs, end="2026-10-09")
    # Revolut shows the settled charge with the capture time (next day) and the tip.
    txs[1] = done("2026-10-10 09:03:17", "Tanta", "-57.50")
    txs.append(done("2026-10-16 10:00:00", "Migros", "-20.00"))
    p = _import(sim, txs, end="2026-10-16")
    assert MatchMethod.SETTLEMENT in {m.method for m in p.matches}
    p = sim.run(render(old_txs, end="2026-10-09"))
    assert p.ynab_actions == [] and [s.code for s in p.skipped_rows] == ["pending_older_export"]
    assert sim.state() == truth(txs)
    # Later statements whose window does not reach 10-09 have nothing to clean up.
    txs.append(done("2026-10-20 10:00:00", "Migros", "-1.00"))
    _import(sim, txs, start="2026-10-12", end="2026-10-20")


@pytest.mark.parametrize("with_later_row", [True, False])
def test_F36_older_statement_after_a_retimed_settlement_creates_no_pending(
    sim: StoreSim, with_later_row: bool
) -> None:
    # The settlement carries a new Started Date (not 15-minute aligned): matched by pass 4.
    # Re-sending the older statement must not create its pending again — neither when the
    # newer statement clearly is newer (allow_sweep False) nor inside the slack.
    pad = done("2026-10-01 09:00:00", "Pad", "-1.00")
    txs = [pad, T(at("2026-10-11 22:15:00"), "Amazon EU", "-89.99")]
    old_txs = copy.deepcopy(txs)
    _import(sim, txs, end="2026-10-11")
    txs[1] = done("2026-10-12 06:00:07", "Amazon EU", "-90.41")
    if with_later_row:
        txs.append(done("2026-10-13 09:00:00", "Pad", "-2.00"))
    _import(sim, txs, end="2026-10-13")
    assert len(_live(sim, "Amazon EU")) == 1
    p = sim.run(render(old_txs, end="2026-10-11"))
    assert p.ynab_actions == [] and p.stale_export
    assert sim.state() == truth(txs)


def test_F36_statement_showing_a_completed_row_still_pending_is_older(sim: StoreSim) -> None:
    # Re-sent within the day: no ledger entry started after it, but it shows pending a row a
    # later statement showed completed (at the same instant) — Revolut never goes back to
    # pending, so the whole statement is older: the re-quote is not reverted either.
    migros = T(at("2026-10-05 04:12:08"), "Migros", "-123.00")
    uber = T(at("2026-10-05 19:48:32"), "Uber", "-79.00")
    txs = [done("2026-10-01 09:00:00", "Pad", "-1.00"), migros, uber]
    older = render(txs, end="2026-10-05")
    _import(sim, txs, end="2026-10-05")
    migros.amount = "-124.20"  # FX re-quote of a pending
    uber.complete(at("2026-10-05 21:00:00"), "-80.00")
    _import(sim, txs, end="2026-10-05")
    n = len(sim.calls)
    p = sim.run(older)
    assert sim.calls[n:] == [] and p.stale_export and not p.allow_sweep
    assert any("Uber" in w and "still pending in it" in w for w in p.warnings)
    assert sim.state() == truth(txs)


def test_a_pending_row_on_a_migrated_completed_entry_is_no_proof_of_age() -> None:
    # v1 left 174 rows PENDING but reconciled; the migration records them as COMPLETED from
    # YNAB's cleared flag, not from a statement. Such an entry matched by a still-pending row
    # must not make the whole statement "older" (that would hold back every new pending).
    exp = export(
        ("2026-10-01 09:00:00", "Pad", "-1.00"),
        ("2026-10-05 12:00:11", "Hotel", "-90.00", "PENDING"),
        ("2026-10-06 08:00:00", "Coop", "-5.00", "PENDING"),
    )
    hotel = exp.rows[1]
    migrated = entry(
        hotel.key,
        identity="legacy",
        started_at=hotel.started_at_text,
        csv_payee="Hotel",
        csv_state="COMPLETED",
    )
    pad = entry(exp.rows[0].key, started_at="2026-10-01 09:00:00", csv_payee="Pad")
    txs = [ytx_for(migrated, cleared="reconciled"), ytx_for(pad)]
    p = plan(exp, [migrated, pad], txs)
    assert not p.stale_export
    assert [a.payload["payee_name"] for a in p.by_kind(ActionKind.CREATE)] == ["Coop"]
    # The same pair with a statement-derived COMPLETED state is proof: Coop is held back.
    seen = entry(
        hotel.key, started_at=hotel.started_at_text, csv_payee="Hotel", csv_state="COMPLETED"
    )
    p = plan(exp, [seen, pad], [ytx_for(seen, cleared="cleared"), ytx_for(pad)])
    assert p.stale_export and not p.by_kind(ActionKind.CREATE)
    assert [s.code for s in p.skipped_rows] == ["pending_older_export"]


# ── a ledger write lost after a POST with a fresh import_id ──────────────────


def test_lost_ledger_write_after_recreating_a_reappeared_row_converges(sim: StoreSim) -> None:
    hold = T(at("2026-10-02 14:00:00"), "Hotel", "-100.00")
    txs = [
        done("2026-10-01 09:00:00", "Pad", "-1.00"),
        hold,
        done("2026-10-03 09:00:00", "Pad", "-2.00"),
    ]
    _import(sim, txs, end="2026-10-03")
    hold.state = "GONE"
    _import(sim, txs, end="2026-10-03")  # superseded → deleted from YNAB
    hold.complete(at("2026-10-04 08:00:00"))  # … and it shows up again, completed
    p = sim.run_losing_ledger_writes(render(txs, end="2026-10-04"))
    (create,) = p.by_kind(ActionKind.CREATE)
    assert create.import_id == "YNAB:-100000:2026-10-02:2"  # :1 is YNAB's deleted one
    assert sim.state() == truth(txs)
    # The next import recognises its own earlier POST: no second Hotel.
    p = _import(sim, txs, end="2026-10-04")
    (link,) = p.by_kind(ActionKind.LINK)
    assert (link.reason, link.entry_key, link.replaces_key) == (
        "adopted",
        "YNAB:-100000:2026-10-02:2",
        "YNAB:-100000:2026-10-02:1",
    )
    assert len(_live(sim, "Hotel")) == 1
    assert sim.ledger.get(sim.account_id, "YNAB:-100000:2026-10-02:1").status == "REKEYED"


def test_lost_ledger_write_after_a_renumbering_collision_create_converges(
    sim: StoreSim,
) -> None:
    # C01 flow, then a third ride whose natural key (:2) another entry holds → fresh id :3.
    early = T(at("2026-10-05 08:10:00"), "SBB CFF FFS", "-3.60")
    txs = [
        done("2026-10-04 09:00:00", "Pad", "-1.00"),
        early,
        done("2026-10-05 17:40:00", "SBB CFF FFS", "-3.60"),
    ]
    _import(sim, txs, end="2026-10-05")
    early.complete(at("2026-10-06 06:00:00"), "-3.80")
    _import(sim, txs, end="2026-10-06")
    txs.append(done("2026-10-05 20:00:00", "SBB CFF FFS", "-3.60"))
    p = sim.run_losing_ledger_writes(render(txs, end="2026-10-06"))
    assert [a.import_id for a in p.by_kind(ActionKind.CREATE)] == ["YNAB:-3600:2026-10-05:3"]
    assert len(_live(sim, "SBB CFF FFS")) == 3
    _import(sim, txs, end="2026-10-06")
    assert len(_live(sim, "SBB CFF FFS")) == 3


def test_lost_ledger_write_after_reposting_an_unlinked_entry_converges() -> None:
    # An UNLINKED entry whose own import_id a tombstone also claims is POSTed under a fresh
    # id; when that ledger write is lost, the next import links the transaction instead of
    # POSTing a third id.
    exp = export(("2026-10-01 09:00:00", "Pad", "-1.00"), ("2026-10-05 12:00:00", "Shop", "-10.00"))
    row = exp.rows[1]
    s = Sim()
    s.add_entry(entry(row.key, status="UNLINKED", ynab_tx_id=None, started_at=row.started_at_text))
    s.add_entry(
        entry(
            "YNAB:P:-10000:2026-10-05:1",
            status="REKEYED",
            ynab_import_id=row.key,
            ynab_tx_id=None,
            rekeyed_to="elsewhere",
        )
    )
    p = s.run_losing_ledger_writes(exp)
    assert [(a.reason, a.import_id) for a in p.by_kind(ActionKind.CREATE)] == [
        ("new", "YNAB:-1000:2026-10-01:1"),
        ("unlinked_missing", "YNAB:-10000:2026-10-05:2"),
    ]
    p = s.run(exp)
    assert p.ynab_actions == []
    assert sorted(a.kind.value for a in p.actions) == ["LINK", "LINK"]
    assert len([t for t in s.live() if t.payee_name == "Shop"]) == 1
    assert s.get(row.key).status == "ACTIVE"
    assert s.run(exp).actions == []


# ── adoption after a lost ledger: by amount and date first, then by key (R02) ─


def test_core_R02_a_settled_transaction_goes_to_its_settled_row_not_to_its_old_key() -> None:
    # YNAB holds the 08:10 ride created as YNAB:-3600:2026-10-05:1 and since settled (PATCHed)
    # to -3.80; the ledger lost it. After the renumbering the 17:40 ride carries that key. The
    # settled row adopts it (amount and date match); the new ride is created under a fresh id.
    y1 = ytx(
        "y1",
        amount=-3800,
        cleared="cleared",
        import_id="YNAB:-3600:2026-10-05:1",
        payee_name="SBB CFF FFS",
    )
    exp = export(
        ("2026-10-05 17:40:00", "SBB CFF FFS", "-3.60"),
        ("2026-10-05 08:10:00", "SBB CFF FFS", "-3.80"),
    )
    p = plan(exp, [], [y1])
    (link,) = [a for a in p.actions if a.reason == "adopted"]
    assert (link.row.amount, link.ynab_tx_id, link.entry_key) == (
        -3800,
        "y1",
        "YNAB:-3600:2026-10-05:1",
    )
    assert [a.import_id for a in p.by_kind(ActionKind.CREATE)] == ["YNAB:-3600:2026-10-05:2"]
    assert p.by_kind(ActionKind.UPDATE) == []  # nothing to change on the settled ride


def test_core_R02_adoption_by_amount_never_crosses_signs_or_days() -> None:
    # Created as a -36.00 charge two days earlier, or as a +3.80 refund: not this row.
    far = ytx("y1", amount=-3800, date="2026-10-05", import_id="YNAB:-3600:2026-10-03:1")
    flipped = ytx("y2", amount=-3800, date="2026-10-05", import_id="YNAB:3800:2026-10-05:1")
    exp = export(("2026-10-05 08:10:00", "SBB CFF FFS", "-3.80"))
    p = plan(exp, [], [far, flipped])
    assert [a for a in p.actions if a.reason == "adopted"] == []
    assert [a.import_id for a in p.by_kind(ActionKind.CREATE)] == ["YNAB:-3800:2026-10-05:1"]


# ── date-only statements after a settlement ──────────────────────────────────


def _date_only(*lines: str):
    return parse_export(("\n".join([",".join(HEADER), *lines]) + "\n").encode())


def test_date_only_statement_after_a_settlement_reimports_without_writes(sim: StoreSim) -> None:
    a = _date_only(
        "Topup,Current,2021-03-01,2021-03-01,Top-Up by *4242,100,0.00,CHF,COMPLETED,100.00",
        "Card Payment,Current,2021-03-04,,Migros,-7.25,0.00,CHF,PENDING,",
    )
    b = _date_only(
        "Topup,Current,2021-03-01,2021-03-01,Top-Up by *4242,100,0.00,CHF,COMPLETED,100.00",
        "Card Payment,Current,2021-03-04,2021-03-05,Migros,-7.40,0.00,CHF,COMPLETED,92.60",
    )
    sim.run(a)
    p = sim.run(b)
    assert [a.reason for a in p.ynab_actions] == ["settlement"]
    assert sim.writes(b) == []
    assert sorted(t.amount for t in sim.live()) == [-7400, 100000]


@pytest.mark.parametrize("started_at", [None, "2026-10-11 22:15:00"])
def test_date_only_settlement_of_a_ledger_pending_is_idempotent(started_at: str | None) -> None:
    s = Sim()
    pending = s.add_entry(
        entry(
            "YNAB:-89990:2026-10-11:1",
            csv_payee="Amazon EU",
            csv_state="PENDING",
            pushed_cleared="uncleared",
            started_at=started_at,
            identity="legacy" if started_at is None else "csv",
        )
    )
    s.add_tx(ytx_for(pending, memo="(pending)"))
    exp = export(("2026-10-10", "Pad", "-1.00"), ("2026-10-11", "Amazon EU", "-90.41"))
    p = s.run(exp)
    assert [m.method for m in p.matches if m.entry_key == pending.key] == [MatchMethod.SETTLEMENT]
    p = s.run(exp)
    assert p.ynab_actions == []
    assert len([t for t in s.live() if t.payee_name == "Amazon EU"]) == 1


# ── timezone shifts ──────────────────────────────────────────────────────────


def _base() -> list[T]:
    return [
        done("2026-10-05 09:12:13", "Coop", "-12.30"),
        done("2026-10-06 13:22:01", "Migros", "-22.10"),
        done("2026-10-07 18:01:09", "Denner", "-7.90"),
        done("2026-10-08 11:11:11", "Kiosk", "-3.10"),
    ]


def test_still_pending_hold_before_a_shifted_window_is_not_swept(sim: StoreSim) -> None:
    # A statement rendered in Las Vegas (-9h) for 10-05..10-08 starts at 10-05 09:00 home
    # time: the hold that started 10-05 08:00 home is still pending but outside it.
    txs = [*_base(), T(at("2026-10-05 08:00:00"), "Hertz", "-500.00")]
    _import(sim, txs, start="2026-10-05", end="2026-10-08")
    p = _import(sim, txs, tz_hours=-9, start="2026-10-05", end="2026-10-08")
    assert not p.by_kind(ActionKind.DELETE)


def test_single_overlap_row_crossing_midnight_in_a_shifted_statement(sim: StoreSim) -> None:
    txs = [done("2026-10-09 00:30:10", "Bar Rossi", "-18.00")]
    _import(sim, txs, start="2026-10-09", end="2026-10-09")
    txs.append(done("2026-10-09 20:00:10", "Shell", "-60.00"))
    _import(sim, txs, tz_hours=-9, start="2026-10-08", end="2026-10-09")


@pytest.mark.parametrize("state", ["PENDING", "COMPLETED"])
def test_lone_overlap_row_crossing_midnight_into_the_window(sim: StoreSim, state: str) -> None:
    # Eastward (+1h), back-to-back statements: the only overlapping row is the previous
    # day's 23:19:53 ride, which this statement shows on its first day at 00:19:53.
    ride = T(at("2026-09-02 23:19:53"), "SBB CFF FFS", "-155.11")
    if state == "COMPLETED":
        ride.complete(at("2026-09-02 23:20:30"))
    txs = [done("2026-09-02 10:00:41", "Coop", "-4.10"), ride]
    _import(sim, txs, start="2026-09-02", end="2026-09-02")
    txs.append(done("2026-09-03 01:24:14", "SBB CFF FFS", "-50.00"))
    p = _import(sim, txs, tz_hours=1, start="2026-09-03", end="2026-09-03")
    assert [(m.method, m.offset_seconds) for m in p.matches] == [(MatchMethod.TIMESTAMP, 3600)]


def test_straggler_crossing_midnight_after_a_trip(sim: StoreSim) -> None:
    # Back home from Las Vegas: a completed row first seen in LA (08-31 17:27) is in no
    # intermediate statement; the next home statement reaches one day further back.
    fillers = [done(f"2026-09-0{d} 12:0{d}:1{d}", f"Shop{d}", f"-1{d}.25") for d in range(2, 8)]
    txs = [done("2026-09-01 02:27:35", "Hotel Bellevue", "-3.80"), *fillers]
    _import(sim, txs, tz_hours=-9, start="2026-08-31", end="2026-09-07")
    _import(sim, txs, start="2026-09-02", end="2026-09-07")
    _import(sim, txs, start="2026-09-01", end="2026-09-07")


def test_pending_from_abroad_settling_at_the_start_of_a_home_statement(sim: StoreSim) -> None:
    fillers = [done(f"2026-09-0{d} 12:0{d}:1{d}", f"Shop{d}", f"-1{d}.25") for d in range(2, 8)]
    amazon = T(at("2026-09-02 00:49:04"), "Amazon EU", "-194.13")
    txs = [amazon, *fillers]
    _import(sim, txs, tz_hours=-9, start="2026-09-01", end="2026-09-07")
    (y,) = _live(sim, "Amazon EU")
    amazon.complete(at("2026-09-03 06:00:00"), "-196.65")
    p = _import(sim, txs, start="2026-09-02", end="2026-09-07")
    assert [a.reason for a in p.ynab_actions if a.ynab_tx_id == y.id] == ["settlement"]


def test_two_foreign_renderings_more_than_14h_apart(sim: StoreSim) -> None:
    # Round-the-world trip: a statement rendered in Sydney (+9h), then one in Los Angeles
    # (-9h): the same instants are 18h apart.
    fillers = [done(f"2026-09-0{d} 12:0{d}:1{d}", f"Shop{d}", f"-1{d}.25") for d in range(2, 8)]
    _import(sim, fillers, tz_hours=9, start="2026-09-01", end="2026-09-09")
    p = _import(sim, fillers, tz_hours=-9, start="2026-09-01", end="2026-09-09")
    assert {m.offset_seconds for m in p.matches} == {-18 * 3600}
    # And home again: every entry stays in one frame, so this is a plain shift too.
    _import(sim, fillers, start="2026-09-01", end="2026-09-09")


# ── a seeded random month (lifecycles, travel, re-sent older statements) ─────

_MERCHANTS = ["Starbucks", "SBB CFF FFS", "Coop", "Migros", "Amazon EU", "Tanta", "Uber"]
_AMOUNTS = ["-4.50", "-3.60", "-3.80", "-12.30", "-20.00", "-7.90"]


def _new_day(rng: random.Random, day: datetime) -> list[T]:
    out: list[T] = []
    for _ in range(rng.randint(0, 5)):
        started = day.replace(hour=rng.randint(0, 23), minute=rng.randint(0, 59))
        started = started.replace(second=rng.randint(0, 59))
        amount = (
            rng.choice(_AMOUNTS)
            if rng.random() < 0.7
            else f"-{rng.randint(1, 300)}.{rng.randint(0, 99):02d}"
        )
        payee = rng.choice(_MERCHANTS)
        if rng.random() < 0.15:
            out.append(
                T(started, payee, amount, "COMPLETED", started + timedelta(seconds=30), "Transfer")
            )
            continue
        t = T(started, payee, amount)
        t.settle_in = rng.choice([0, 0, 1, 1, 2, 3])  # type: ignore[attr-defined]
        t.fx = rng.random() < 0.2  # type: ignore[attr-defined]
        t.revert = rng.random() < 0.05  # type: ignore[attr-defined]
        t.requote = rng.random() < 0.1  # type: ignore[attr-defined]
        out.append(t)
    return out


def _age(rng: random.Random, txs: list[T], now: datetime) -> None:
    for t in txs:
        if t.state != "PENDING":
            continue
        age = (now - t.started).days
        if getattr(t, "requote", False) and age >= 1 and not getattr(t, "requoted", False):
            t.amount = f"{float(t.amount) * 1.01:.2f}"
            t.requoted = True  # type: ignore[attr-defined]
        if age >= getattr(t, "settle_in", 0):
            if getattr(t, "revert", False):
                t.state = "REVERTED"
            else:
                if getattr(t, "fx", False):
                    t.amount = f"{float(t.amount) * 1.013:.2f}"
                t.complete(
                    max(now - timedelta(hours=rng.randint(0, 8)), t.started + timedelta(minutes=1))
                )


@pytest.mark.parametrize("seed", range(12))
def test_random_month_of_statements_converges(tmp_path: Path, seed: int) -> None:
    rng = random.Random(seed)
    s = StoreSim(tmp_path)
    try:
        txs: list[T] = []
        seen: dict[int, T] = {}
        statements = []
        tz = 0
        for d in range(24):
            day = datetime(2026, 9, 1) + timedelta(days=d)
            txs += _new_day(rng, day)
            now = day + timedelta(hours=23, minutes=59)
            _age(rng, txs, now)
            if rng.random() < 0.2:
                tz = rng.choice([0, 0, -9, -6, 1, 2, 8, 9])
            start = (day - timedelta(days=rng.randint(7, 14))).date().isoformat()
            end = day.date().isoformat()
            visible = [t for t in txs if t.started <= now]
            exp = render(visible, tz_hours=tz, start=start, end=end)
            if not exp.rows:
                continue
            statements.append(exp)
            for t in visible:
                if start <= (t.started + timedelta(hours=tz)).date().isoformat() <= end:
                    seen[id(t)] = t
            p = s.run(exp)
            assert s.state() == truth(seen.values()), (
                d,
                tz,
                [(a.kind.value, a.reason) for a in p.ynab_actions],
            )
            assert s.writes(exp) == [], (d, "not idempotent")
            if len(statements) > 3 and rng.random() < 0.15:
                before = s.state()
                s.run(rng.choice(statements[:-1]))  # an older statement sent again
                assert s.state() == before, (d, "older statement changed YNAB")
    finally:
        s.close()
