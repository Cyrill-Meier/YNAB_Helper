"""End-to-end planner scenarios and finding regressions (F01 F02 F08 F09 F11 F12 F33 F34 F35
F36 F37 C01), run through :class:`Sim` (in-memory YNAB + ledger + executor semantics)."""

from __future__ import annotations

import dataclasses
import random

from ynab_helper.importer import ActionKind, ImportResult, MatchMethod, Plan, plan_import
from ynab_helper.storage.ledger import LedgerStatus
from ynab_helper.storage.mirror import MirrorView

from .helpers import (
    ACCT,
    OTHER_ACCT,
    Sim,
    entry,
    export,
    fixture,
    kinds,
    plan,
    row_by,
    ynab_kinds,
    ytx_for,
)


def _calls_after(sim: Sim, n: int) -> list[tuple]:
    return sim.calls[n:]


def _expected_total(exp) -> int:
    """What YNAB should hold for an export: every non-reverted row."""
    return sum(r.amount for r in exp.rows if r.state != "REVERTED")


def _tx_by_import_id(sim: Sim, import_id: str):
    (tx,) = [t for t in sim.txs.values() if t.import_id == import_id]
    return tx


def _active_links(sim: Sim) -> list[str]:
    return sorted(
        e.ynab_tx_id for e in sim.entries() if e.status == LedgerStatus.ACTIVE and e.ynab_tx_id
    )


# ── idempotency / determinism ────────────────────────────────────────────────


def test_reimporting_the_same_export_makes_no_ynab_calls() -> None:
    for name in (
        "export_basic.csv",
        "export_declined.csv",
        "export_fees.csv",
        "export_legacy_no_time.csv",
        "export_other_products.csv",
        "export_same_amount_same_day_a.csv",
    ):
        sim = Sim()
        exp = fixture(name)
        first = sim.run(exp)
        assert first.by_kind(ActionKind.CREATE), name
        n = len(sim.calls)
        again = sim.run(exp)
        assert again.actions == [], (name, kinds(again))
        assert _calls_after(sim, n) == []
        assert sim.balance() == _expected_total(exp), name


def test_idempotent_after_a_sequence_of_exports() -> None:
    sim = Sim()
    for name in (
        "export_pending_then_completed_a.csv",
        "export_pending_then_completed_b.csv",
        "export_same_amount_same_day_a.csv",
        "export_same_amount_same_day_b.csv",
    ):
        exp = fixture(name)
        sim.run(exp)
        replan = sim.plan(exp)
        assert replan.actions == [], (name, kinds(replan))
    # Every live transaction is linked from exactly one ACTIVE entry.
    links = _active_links(sim)
    assert links == sorted({t.id for t in sim.live()})


def _signature(p: Plan) -> list[tuple]:
    return [
        (
            a.kind.value,
            a.reason,
            a.account_id,
            a.entry_key,
            a.ynab_tx_id,
            tuple(sorted(a.payload.items())),
            tuple(sorted(a.fields.items())),
            tuple(sorted(a.ledger_updates.items())),
            a.replaces_key,
        )
        for a in p.actions
    ]


def test_planning_is_deterministic_and_independent_of_input_order() -> None:
    sim = Sim()
    sim.run(fixture("export_basic.csv"))
    exp = fixture("export_tz_shift.csv")
    reference = sim.plan(exp)
    assert reference.ynab_actions
    rng = random.Random(7)
    for _ in range(5):
        entries = sim.entries()
        rng.shuffle(entries)
        txs = list(sim.txs.values())
        rng.shuffle(txs)
        again = plan_import(exp, entries, MirrorView(ACCT, txs), sim.options(exp))
        assert _signature(again) == _signature(reference)
        assert again.warnings == reference.warnings
        assert again.matches == reference.matches


# ── F01 / F33 / C02: never downgrade cleared, never wipe memos ───────────────


def test_F01_reconciled_never_downgraded_and_memo_never_wiped() -> None:
    sim = Sim()
    exp = fixture("export_basic.csv")
    sim.run(exp)
    # The user reconciles everything and annotates one transaction.
    for tx in sim.live():
        if tx.cleared == "cleared":
            sim.user_edit(tx.id, cleared="reconciled")
    coop = _tx_by_import_id(sim, "YNAB:-8400:2026-10-01:1")
    sim.user_edit(coop.id, memo="dinner w/ Anna")
    n = len(sim.calls)
    p = sim.run(exp)
    assert p.ynab_actions == []
    assert _calls_after(sim, n) == []
    assert sim.txs[coop.id].memo == "dinner w/ Anna"
    assert sim.txs[coop.id].cleared == "reconciled"


def test_F01_pending_completed_after_reconcile_only_strips_the_marker() -> None:
    sim = Sim()
    sim.run(fixture("export_pending_then_completed_a.csv"))
    migros = _tx_by_import_id(sim, "YNAB:-45300:2026-10-11:1")
    # The user reconciled it while pending and wrote around the marker.
    sim.user_edit(migros.id, cleared="reconciled", memo="groceries | (pending)")
    n = len(sim.calls)
    sim.run(fixture("export_pending_then_completed_b.csv"))
    patches = [c for c in _calls_after(sim, n) if c[0] == "PATCH" and c[1] == migros.id]
    assert patches == [("PATCH", migros.id, (("memo", "groceries"),))]
    assert sim.txs[migros.id].cleared == "reconciled"


def test_F33_user_memo_and_manual_clear_are_preserved() -> None:
    sim = Sim()
    sim.run(fixture("export_pending_then_completed_a.csv"))
    migros = _tx_by_import_id(sim, "YNAB:-45300:2026-10-11:1")
    sim.user_edit(migros.id, cleared="cleared", memo="dinner with Bob, split later")
    n = len(sim.calls)
    p = sim.run(fixture("export_pending_then_completed_b.csv"))
    assert all(c[1] != migros.id for c in _calls_after(sim, n))
    assert sim.txs[migros.id].memo == "dinner with Bob, split later"
    assert [a for a in p.ynab_actions if a.ynab_tx_id == migros.id] == []


def test_C02_still_pending_rows_keep_their_marker() -> None:
    sim = Sim()
    exp = fixture("export_pending_then_completed_a.csv")
    sim.run(exp)
    hotel = _tx_by_import_id(sim, "YNAB:-150000:2026-10-10:1")
    sim.user_edit(hotel.id, cleared="cleared")  # user marked the hold cleared by hand
    p = sim.run(exp)
    assert p.ynab_actions == []
    assert sim.txs[hotel.id].memo == "(pending)"


# ── F02: re-dating never creates a ghost that the sweep could delete ──────────


def test_F02_redate_then_reimport_never_deletes_the_live_transaction() -> None:
    sim = Sim()
    zurich = export(
        ("2026-08-09 10:00:00", "Pad", "-1.00"),
        ("2026-08-09 23:30:00", "Hotel Bellagio", "-880.00", "PENDING"),
    )
    shifted = export(
        ("2026-08-09 19:00:00", "Pad", "-1.00"),
        ("2026-08-10 08:30:00", "Hotel Bellagio", "-880.00", "PENDING"),
    )
    sim.run(zurich)
    hotel = _tx_by_import_id(sim, "YNAB:-880000:2026-08-09:1")
    n = len(sim.calls)
    sim.run(shifted)
    assert _calls_after(sim, n) == [("PATCH", hotel.id, (("date", "2026-08-10"),))]
    for _ in range(2):
        n = len(sim.calls)
        p = sim.run(shifted)
        assert p.actions == [] and _calls_after(sim, n) == []
    assert not sim.txs[hotel.id].deleted
    # One ledger entry, keyed by the original key, holds the transaction (no ghost).
    holders = [e for e in sim.entries() if e.ynab_tx_id == hotel.id]
    assert [(e.key, e.status) for e in holders] == [("YNAB:-880000:2026-08-09:1", "ACTIVE")]
    # The settled row later clears the same transaction.
    settled = export(
        ("2026-08-09 19:00:00", "Pad", "-1.00"),
        ("2026-08-10 08:30:00", "Hotel Bellagio", "-880.00"),
    )
    n = len(sim.calls)
    sim.run(settled)
    assert _calls_after(sim, n) == [
        ("PATCH", hotel.id, (("cleared", "cleared"), ("memo", ""))),
    ]


def test_F02_ghost_entries_from_v1_never_link_twice() -> None:
    sim = Sim()
    exp = export(
        ("2026-10-01 09:00:00", "Pad", "-1.00"),
        ("2026-10-05 12:00:00", "Shop", "-10.00", "PENDING"),
    )
    row = row_by(exp, started="2026-10-05 12:00:00")
    holder = sim.add_entry(
        entry(
            "YNAB:-10000:2026-10-04:1",
            identity="legacy",
            started_at=None,
            csv_state="PENDING",
            pushed_cleared="uncleared",
            ynab_import_id=row.key,
            ynab_tx_id="y-1",
        )
    )
    ghost = sim.add_entry(
        entry(row.key, status="UNLINKED", ynab_tx_id=None, identity="legacy", started_at=None)
    )
    sim.add_tx(ytx_for(holder, import_id=row.key))
    pad = row_by(exp, started="2026-10-01 09:00:00")
    sim.add_entry(entry(pad.key, started_at="2026-10-01 09:00:00", csv_payee="Pad"))
    sim.add_tx(ytx_for(sim.get(pad.key), cleared="cleared"))
    p = sim.run(exp)
    assert p.by_kind(ActionKind.LINK, ActionKind.DELETE, ActionKind.CREATE) == []
    assert not sim.txs["y-1"].deleted
    assert sim.get(ghost.key).status == "UNLINKED"
    assert any("already linked" in w for w in p.warnings)


# ── F08: removed rows reappearing; deleted rows never resurrected ─────────────


def test_F08_superseded_row_reappearing_is_recreated_with_a_fresh_import_id() -> None:
    sim = Sim()
    with_hold = export(
        ("2026-10-01 09:00:00", "Pad", "-1.00"),
        ("2026-10-02 14:00:00", "Hotel", "-100.00", "PENDING"),
        ("2026-10-03 09:00:00", "Pad", "-2.00"),
    )
    without = export(
        ("2026-10-01 09:00:00", "Pad", "-1.00"),
        ("2026-10-03 09:00:00", "Pad", "-2.00"),
    )
    sim.run(with_hold)
    hold = _tx_by_import_id(sim, "YNAB:-100000:2026-10-02:1")
    p = sim.run(without)
    assert ynab_kinds(p) == [("DELETE", "superseded")]
    assert sim.get("YNAB:-100000:2026-10-02:1").status == "SUPERSEDED"
    # It shows up again (newer export, completed): re-created under a fresh import_id
    # (YNAB still refuses the old one), never PATCHed by its deleted id.
    again = export(
        ("2026-10-01 09:00:00", "Pad", "-1.00"),
        ("2026-10-02 14:00:00", "Hotel", "-100.00"),
        ("2026-10-03 09:00:00", "Pad", "-2.00"),
    )
    n = len(sim.calls)
    p = sim.run(again)
    assert _calls_after(sim, n) == [("POST", "YNAB:-100000:2026-10-02:2", -100000, "cleared")]
    assert sim.get("YNAB:-100000:2026-10-02:1").status == "REKEYED"
    assert sim.get("YNAB:-100000:2026-10-02:1").rekeyed_to == "YNAB:-100000:2026-10-02:2"
    assert sim.get("YNAB:-100000:2026-10-02:2").status == "ACTIVE"
    assert sim.txs[hold.id].deleted
    assert sim.run(again).actions == []
    assert sim.balance() == _expected_total(again)


def test_F08_fx_requote_then_settlement_keeps_one_transaction() -> None:
    # v1: -100 pending, re-quoted to -101 (superseded+created), settles at -100 → PATCH of
    # a deleted transaction and an unbalanced YNAB. v2: settlement updates in place.
    sim = Sim()
    pad = ("2026-10-01 09:00:00", "Pad", "-1.00")
    sim.run(export(pad, ("2026-10-02 14:00:00", "Hotel", "-100.00", "PENDING")))
    (hotel,) = [t for t in sim.live() if t.payee_name == "Hotel"]
    sim.run(export(pad, ("2026-10-02 14:00:00", "Hotel", "-101.00", "PENDING")))
    final = export(pad, ("2026-10-02 14:00:00", "Hotel", "-100.00"))
    sim.run(final)
    assert [(t.id, t.amount, t.cleared) for t in sim.live() if t.payee_name == "Hotel"] == [
        (hotel.id, -100000, "cleared")
    ]
    assert not any(c[0] in ("DELETE", "POST-DUPLICATE") for c in sim.calls)
    assert sim.balance() == _expected_total(final)


def test_F08_deleted_in_ynab_is_never_recreated() -> None:
    sim = Sim()
    exp = fixture("export_basic.csv")
    sim.run(exp)
    coop = _tx_by_import_id(sim, "YNAB:-8400:2026-10-01:1")
    sim.user_delete(coop.id)
    n = len(sim.calls)
    p = sim.run(exp)
    assert p.ynab_actions == [] and _calls_after(sim, n) == []
    assert p.ignored_deleted == 1
    assert sim.get("YNAB:-8400:2026-10-01:1").status == "DELETED_IN_YNAB"
    # Also for a later export where it completes / changes.
    p = sim.run(fixture("export_tz_shift.csv"))
    assert all(a.ynab_tx_id != coop.id for a in p.ynab_actions)
    assert not [a for a in p.by_kind(ActionKind.CREATE) if a.row and a.row.payee == "Coop Pronto"]
    assert p.ignored_deleted == 1


# ── F09: --skip-pending never deletes pendings ───────────────────────────────


def test_F09_skip_pending_never_deletes_still_present_pendings() -> None:
    sim = Sim()
    a = fixture("export_pending_then_completed_a.csv")
    sim.run(a)
    n = len(sim.calls)
    p = sim.run(a, skip_pending=True)
    assert p.ynab_actions == [] and _calls_after(sim, n) == []
    assert len([t for t in sim.live() if t.cleared == "uncleared"]) == 3


def test_F09_skip_pending_does_not_skip_the_sweep_or_other_rows() -> None:
    sim = Sim()
    sim.run(fixture("export_pending_then_completed_a.csv"))
    # A parking pending imported earlier that vanished from statement b (superseded by a
    # differently timed charge) and is still uncleared in YNAB: the sweep must remove it
    # even with --skip-pending.
    parking = sim.add_entry(
        entry(
            "YNAB:-5000:2026-10-12:1",
            csv_payee="Parkhaus",
            csv_state="PENDING",
            pushed_cleared="uncleared",
            started_at="2026-10-12 10:00:13",
        )
    )
    sim.add_tx(ytx_for(parking, memo="(pending)"))
    n = len(sim.calls)
    p = sim.run(fixture("export_pending_then_completed_b.csv"), skip_pending=True)
    calls = _calls_after(sim, n)
    assert sorted(c[1:] for c in calls if c[0] == "DELETE") == sorted(
        [
            (parking.ynab_tx_id, "superseded"),
            (_tx_by_import_id(sim, "YNAB:-150000:2026-10-10:1").id, "reverted"),
        ]
    )
    assert ("UPDATE", "settlement") in ynab_kinds(p)
    assert [s.code for s in p.skipped_rows] == ["pending_skipped"]  # the new Coop pending
    assert not [c for c in calls if c[0] == "POST"]
    assert sim.get(parking.key).status == "SUPERSEDED"


# ── F11: payee renamed in YNAB ───────────────────────────────────────────────


def test_F11_payee_renamed_in_ynab_does_not_break_matching() -> None:
    sim = Sim()
    sim.run(fixture("export_pending_then_completed_a.csv"))
    for tx in sim.live():
        sim.user_edit(tx.id, payee_name=f"Renamed {tx.payee_name}")
    n = len(sim.calls)
    p = sim.run(fixture("export_pending_then_completed_b.csv"))
    posts = [c for c in _calls_after(sim, n) if c[0] == "POST"]
    assert posts == [("POST", "YNAB:-11200:2026-10-13:1", -11200, "uncleared")]  # only new row
    assert {m.method for m in p.matches} >= {MatchMethod.SETTLEMENT}
    # The ledger keeps Revolut's payee (never YNAB's).
    assert all(not (e.csv_payee or "").startswith("Renamed") for e in sim.entries())


def test_F11_legacy_date_shift_uses_the_csv_payee() -> None:
    sim = Sim()
    legacy = sim.add_entry(
        entry(
            "YNAB:-3500:2026-10-02:1",
            identity="legacy",
            started_at=None,
            csv_payee="Kiosk, Bahnhof Bern",
        )
    )
    sim.add_tx(ytx_for(legacy, payee_name="Kiosk (renamed)", cleared="reconciled"))
    exp = export(
        ("2026-10-01 10:00:00", "Pad", "-1.00"),
        ("2026-10-03 00:47:31", "Kiosk, Bahnhof Bern", "-3.5"),
    )
    p = sim.run(exp)
    assert [m.method for m in p.matches if m.entry_key == legacy.key] == [MatchMethod.DATE_SHIFT]
    assert not [a for a in p.by_kind(ActionKind.CREATE) if a.row.payee.startswith("Kiosk")]
    # Reconciled: never re-dated, only the ledger follows the CSV.
    assert sim.get(legacy.key).csv_date == "2026-10-03"
    assert sim.txs[legacy.ynab_tx_id].date == "2026-10-02"


# ── F12: per-account ledger ──────────────────────────────────────────────────


def test_F12_switching_accounts_imports_everything_and_leaves_the_old_account_alone() -> None:
    old = Sim(ACCT)
    exp = fixture("export_pending_then_completed_a.csv")
    old.run(exp)
    new = Sim(OTHER_ACCT)
    new.ledger, new.txs = old.ledger, old.txs  # one DB, one budget, two accounts
    n = len(old.calls)
    p = new.run(fixture("export_pending_then_completed_b.csv"))
    # Every row is new for the other account, with its natural import_ids.
    assert ynab_kinds(p) == [("CREATE", "new")] * 6
    assert [a.import_id for a in p.by_kind(ActionKind.CREATE)] == [
        r.key for r in fixture("export_pending_then_completed_b.csv").rows if r.state != "REVERTED"
    ]
    assert all(a.payload["account_id"] == OTHER_ACCT for a in p.ynab_actions)
    # The old account's pendings were not touched (no sweep, no PATCH).
    assert [c[0] for c in new.calls] == ["POST"] * 6
    assert len(old.calls) == n
    assert len([t for t in old.live() if t.cleared == "uncleared"]) == 3


# ── F34 / C01: renumbering ───────────────────────────────────────────────────


def test_F34_timezone_shift_renumbering_creates_no_duplicates() -> None:
    sim = Sim()
    basic, shifted = fixture("export_basic.csv"), fixture("export_tz_shift.csv")
    sim.run(basic)
    ids = {t.id for t in sim.live()}
    p = sim.run(shifted)
    assert {k for k, _ in ynab_kinds(p)} == {"UPDATE"}
    assert {a.reason for a in p.ynab_actions} == {"redated"}
    assert {t.id for t in sim.live()} == ids
    # The 10-08 23:30 coffee that took another coffee's key is matched by its timestamp.
    late = row_by(shifted, started="2026-10-09 08:30:00")
    assert late.key == "YNAB:-4500:2026-10-09:1"
    (match,) = [m for m in p.matches if m.row_key == late.key]
    assert (match.entry_key, match.offset_seconds) == ("YNAB:-4500:2026-10-08:2", 9 * 3600)
    assert sim.run(shifted).actions == []
    # And back again: re-dated to the original rendering, still no duplicates.
    p = sim.run(basic)
    assert {t.id for t in sim.live()} == ids
    assert {a.reason for a in p.ynab_actions} == {"redated"}
    assert sim.balance() == _expected_total(basic)


def test_F34_two_coffees_las_vegas_scenario() -> None:
    # The finding's exact scenario: the only key-equal pair is a collision (+6.5h).
    sim = Sim()
    zurich = export(
        ("2026-08-10 08:00:00", "Starbucks", "-4.50"),
        ("2026-08-10 23:30:00", "Starbucks", "-4.50"),
    )
    vegas = export(
        ("2026-08-09 23:00:00", "Starbucks", "-4.50"),
        ("2026-08-10 14:30:00", "Starbucks", "-4.50"),
    )
    sim.run(zurich)
    n = len(sim.calls)
    p = sim.run(vegas)
    assert not [c for c in _calls_after(sim, n) if c[0] == "POST"]
    assert len(sim.live()) == 2
    assert {m.offset_seconds for m in p.matches} == {-9 * 3600}
    assert sim.run(vegas).actions == []


def test_C01_renumbered_sibling_after_settlement() -> None:
    sim = Sim()
    a, b = (
        fixture("export_same_amount_same_day_a.csv"),
        fixture("export_same_amount_same_day_b.csv"),
    )
    sim.run(a)
    y1 = _tx_by_import_id(sim, "YNAB:-3600:2026-10-05:1")  # 08:10 pending
    y2 = _tx_by_import_id(sim, "YNAB:-3600:2026-10-05:2")  # 17:40 completed
    coffee = _tx_by_import_id(sim, "YNAB:-4500:2026-10-05:2")  # 12:30 pending
    # Precondition: the parser hands the pending's old key to the 17:40 ride.
    assert row_by(b, started="2026-10-05 17:40:00").key == "YNAB:-3600:2026-10-05:1"
    n = len(sim.calls)
    sim.run(b)
    assert _calls_after(sim, n) == [
        ("PATCH", y1.id, (("amount", -3800), ("cleared", "cleared"), ("memo", ""))),
        ("PATCH", coffee.id, (("cleared", "cleared"), ("memo", ""))),
    ]
    assert sim.txs[y2.id] == y2  # untouched
    assert sim.balance() == _expected_total(b) == -4500 * 3 - 3600 - 3800
    assert sim.run(b).actions == []


# ── F35: settlement at a different amount ────────────────────────────────────


def test_F35_settlement_updates_in_place_and_keeps_the_category() -> None:
    sim = Sim()
    sim.run(fixture("export_pending_then_completed_a.csv"))
    amazon = _tx_by_import_id(sim, "YNAB:-89990:2026-10-11:1")
    sim.user_edit(amazon.id, category_name="Shopping")
    n = len(sim.calls)
    p = sim.run(fixture("export_pending_then_completed_b.csv"))
    update = [a for a in p.ynab_actions if a.ynab_tx_id == amazon.id]
    assert [(a.kind.value, a.reason) for a in update] == [("UPDATE", "settlement")]
    assert update[0].changes["amount"] == (-89990, -90410)
    assert not [c for c in _calls_after(sim, n) if c[0] == "DELETE" and c[1] == amazon.id]
    assert sim.txs[amazon.id].category_name == "Shopping"
    assert sim.txs[amazon.id].amount == -90410
    assert sim.balance() == _expected_total(fixture("export_pending_then_completed_b.csv"))


def test_F35_ambiguous_settlement_falls_back_to_supersede_and_create() -> None:
    sim = Sim()
    pad = ("2026-10-01 09:00:00", "Pad", "-1.00")
    sim.run(
        export(
            pad,
            ("2026-10-05 10:00:00", "EV Charge", "-450.00", "PENDING"),
            ("2026-10-05 18:00:00", "EV Charge", "-300.00", "PENDING"),
        )
    )
    # Settled under a new timestamp: two pending candidates, no timestamp to decide.
    settled = export(pad, ("2026-10-06 03:00:00", "EV Charge", "-412.00"))
    p = sim.run(settled)
    # No guess: both pendings are removed and the settled row is created, so YNAB matches
    # Revolut.
    assert sorted(ynab_kinds(p)) == [
        ("CREATE", "new"),
        ("DELETE", "superseded"),
        ("DELETE", "superseded"),
    ]
    assert sim.balance() == _expected_total(settled)


def test_F35_settlement_at_the_same_instant_wins_over_other_rows_of_the_merchant() -> None:
    sim = Sim()
    pad = ("2026-10-01 09:00:00", "Pad", "-1.00")
    sim.run(export(pad, ("2026-10-11 22:15:00", "Amazon EU", "-89.99", "PENDING")))
    (pending,) = [t for t in sim.live() if t.payee_name == "Amazon EU"]
    later = export(
        pad,
        ("2026-10-11 22:15:00", "Amazon EU", "-90.41"),
        ("2026-10-12 10:00:00", "Amazon EU", "-12.00", "PENDING"),
    )
    p = sim.run(later)
    assert sorted(ynab_kinds(p)) == [("CREATE", "new"), ("UPDATE", "settlement")]
    assert only_update(p).ynab_tx_id == pending.id
    assert sim.balance() == _expected_total(later)


def only_update(p):
    (update,) = p.by_kind(ActionKind.UPDATE)
    return update


# ── F36: older exports ───────────────────────────────────────────────────────


def test_F36_older_export_never_downgrades_or_deletes() -> None:
    sim = Sim()
    a, b = (
        fixture("export_pending_then_completed_a.csv"),
        fixture("export_pending_then_completed_b.csv"),
    )
    sim.run(a)
    sim.run(b)
    before = dict(sim.txs)
    n = len(sim.calls)
    p = sim.run(a)  # re-sent by mistake
    assert _calls_after(sim, n) == []
    assert p.ynab_actions == []
    assert sim.txs == before
    # The settled Amazon (-90.41) is recognised in its pre-settlement form (-89.99).
    assert [m.method for m in p.matches if m.row_key == "YNAB:-89990:2026-10-11:1"] == [
        MatchMethod.PRE_SETTLEMENT
    ]


def test_F36_clearly_older_export_disables_the_sweep() -> None:
    sim = Sim()
    old = export(
        ("2026-09-01 09:00:00", "Pad", "-1.00"),
        ("2026-09-02 09:00:00", "Shop", "-5.00", "PENDING"),
    )
    new = export(
        ("2026-09-01 09:00:00", "Pad", "-1.00"),
        # Inside the old statement's window but started after it was generated: the old
        # statement cannot show it, and re-sending it must not sweep it.
        ("2026-09-02 18:30:00", "Kiosk", "-3.00", "PENDING"),
        ("2026-09-03 09:00:00", "Late pending", "-7.00", "PENDING"),
        ("2026-09-02 09:00:00", "Shop", "-5.00"),
        ("2026-09-10 09:00:00", "Pad", "-2.00"),
    )
    sim.run(old)
    sim.run(new)
    (kiosk,) = [t for t in sim.live() if t.payee_name == "Kiosk"]
    opts = sim.options(old)
    assert opts.allow_sweep is False
    n = len(sim.calls)
    p = sim.run(old)  # re-sent
    assert p.ynab_actions == [] and _calls_after(sim, n) == []
    assert p.allow_sweep is False and p.stale_export
    assert not sim.txs[kiosk.id].deleted
    # The Shop row is pending in the old statement but completed in the ledger: no downgrade.
    assert sim.txs[_tx_by_import_id(sim, "YNAB:-5000:2026-09-02:1").id].cleared == "cleared"
    # Only the service's allow_sweep keeps a re-sent statement that is *not* older by its
    # timestamps from sweeping (see test_sweep_conditions); here both signals agree.
    assert any("older than what was already imported" in w for w in p.warnings)


# ── F37 and reporting ────────────────────────────────────────────────────────


def test_F37_declined_and_failed_rows_are_reported_not_imported() -> None:
    sim = Sim()
    exp = fixture("export_declined.csv")
    p = sim.run(exp)
    result = ImportResult.from_plan_dry_run(p, exp)
    reasons = dict(result.skipped_rows)
    assert "DECLINED" in reasons[2] and "FAILED" in reasons[4]
    assert reasons[6] == "reverted by Revolut (never imported)"
    assert sim.balance() == 300000 - 250000 - 80000 - 20000
    assert all(t.amount != -250000 or t.cleared == "cleared" for t in sim.live())


def test_legacy_ledger_rows_are_matched_by_key_and_backfilled() -> None:
    sim = Sim()
    exp = fixture("export_basic.csv")
    for r in exp.rows:
        if r.state == "REVERTED":
            continue
        legacy = entry(
            r.key,
            identity="legacy",
            started_at=None,
            csv_payee=f"YNAB name of {r.payee}",  # v1 sync overwrote it (F11)
            csv_type=None,
            csv_state="COMPLETED" if r.state == "COMPLETED" else "PENDING",
            pushed_cleared=r.cleared,
            ynab_tx_id=f"y-{r.line_no}",
        )
        sim.add_entry(legacy)
        sim.add_tx(
            ytx_for(
                legacy, memo=r.memo, cleared="reconciled" if r.state == "COMPLETED" else "uncleared"
            )
        )
    p = sim.run(exp)
    assert p.ynab_actions == []
    assert {m.method for m in p.matches} == {MatchMethod.KEY}
    for e in sim.entries():
        assert e.identity == "csv" and e.started_at is not None
        assert not e.csv_payee.startswith("YNAB name of")
    # From now on identity is the timestamp: a tz-shifted export matches without creates.
    p = sim.run(fixture("export_tz_shift.csv"))
    assert not p.by_kind(ActionKind.CREATE, ActionKind.DELETE)


def test_reverted_pending_cleared_in_ynab_is_left_alone_with_a_warning() -> None:
    sim = Sim()
    sim.run(fixture("export_pending_then_completed_a.csv"))
    hotel = _tx_by_import_id(sim, "YNAB:-150000:2026-10-10:1")
    sim.user_edit(hotel.id, cleared="cleared")
    p = sim.run(fixture("export_pending_then_completed_b.csv"))
    assert not sim.txs[hotel.id].deleted
    assert any("Hotel Bellevue (hold)" in w and "reverted in Revolut" in w for w in p.warnings)
    assert sim.get("YNAB:-150000:2026-10-10:1").csv_state == "REVERTED"
    # Reported once, not on every import.
    p = sim.run(fixture("export_pending_then_completed_b.csv"))
    assert p.warnings == [] and p.actions == []


def test_lost_ledger_write_is_adopted_not_duplicated() -> None:
    # A previous run created the transactions but its ledger writes were lost (crash after
    # the POST, lost response, or rows imported through another DB such as the CLI's).
    sim = Sim()
    a = fixture("export_pending_then_completed_a.csv")
    sim.run(a)
    ids = {t.id for t in sim.live()}
    sim.ledger.clear()
    n = len(sim.calls)
    p = sim.run(a)
    assert _calls_after(sim, n) == []  # nothing created twice, nothing written
    assert {(a.kind.value, a.reason) for a in p.actions} == {("LINK", "adopted")}
    assert [m.method for m in p.matches] == [MatchMethod.ADOPTED] * 6
    assert {t.id for t in sim.live()} == ids
    assert _active_links(sim) == sorted(ids)
    # And the next export proceeds normally from the adopted entries.
    b = fixture("export_pending_then_completed_b.csv")
    p = sim.run(b)
    assert sorted(ynab_kinds(p)) == [
        ("CREATE", "new"),
        ("DELETE", "reverted"),
        ("UPDATE", "cleared"),
        ("UPDATE", "settlement"),
    ]
    assert sim.balance() == _expected_total(b)
    assert sim.run(b).actions == []


def test_adoption_applies_the_usual_rules_and_never_takes_a_known_id() -> None:
    sim = Sim()
    a = fixture("export_pending_then_completed_a.csv")
    sim.run(a)
    sim.ledger.clear()
    b = fixture("export_pending_then_completed_b.csv")
    p = sim.plan(b)
    by_reason = sorted(ynab_kinds(p))
    # Migros (pending in YNAB, completed now) is adopted and cleared; the reverted hotel hold
    # is adopted and deleted. The lost Amazon pending cannot be recognised without its
    # ledger entry (different key after settlement): it stays, the settled row is created.
    assert by_reason == [
        ("CREATE", "new"),
        ("CREATE", "new"),
        ("DELETE", "reverted"),
        ("UPDATE", "cleared"),
    ]
    # A transaction some ledger entry knows (here: a REKEYED tombstone's key) is not adopted.
    sim2 = Sim()
    sim2.run(a)
    topup = _tx_by_import_id(sim2, "YNAB:300000:2026-10-01:1")
    sim2.ledger = {
        k: (
            dataclasses.replace(e, status="REKEYED", ynab_tx_id=None, rekeyed_to="x")
            if e.ynab_tx_id == topup.id
            else e
        )
        for k, e in sim2.ledger.items()
    }
    p = sim2.plan(a)
    assert [m.method for m in p.matches if m.row_key == "YNAB:300000:2026-10-01:1"] == []
    assert [a.import_id for a in p.by_kind(ActionKind.CREATE)] == ["YNAB:300000:2026-10-01:2"]


def test_duplicate_answer_from_a_stale_mirror_is_resolved() -> None:
    # The mirror the plan was built from does not have the transactions yet, so the plan
    # POSTs natural keys; YNAB answers "duplicate" and the executor links them mid-run.
    sim = Sim()
    a = fixture("export_pending_then_completed_a.csv")
    sim.run(a)
    sim.ledger.clear()
    b = fixture("export_pending_then_completed_b.csv")
    opts = sim.options(b)
    p = plan_import(b, [], MirrorView(ACCT, []), opts)
    n = len(sim.calls)
    sim.apply(p, opts)
    calls = _calls_after(sim, n)
    dups = {c[1] for c in calls if c[0] == "POST-DUPLICATE"}
    assert dups == {
        "YNAB:300000:2026-10-01:1",
        "YNAB:-11200:2026-10-10:1",
        "YNAB:-6800:2026-10-12:1",
        "YNAB:-45300:2026-10-11:1",
    }
    migros = _tx_by_import_id(sim, "YNAB:-45300:2026-10-11:1")
    # The resolved duplicate gets the same treatment as any active match: cleared + marker.
    assert ("PATCH", migros.id, (("cleared", "cleared"), ("memo", ""))) in calls
    assert sim.get("YNAB:-45300:2026-10-11:1").status == "ACTIVE"
    assert len(_active_links(sim)) == len(set(_active_links(sim)))


def test_dry_run_result_reports_the_plan() -> None:
    sim = Sim()
    sim.run(fixture("export_pending_then_completed_a.csv"))
    exp = fixture("export_pending_then_completed_b.csv")
    p = sim.plan(exp)
    result = ImportResult.from_plan_dry_run(p, exp)
    assert result.dry_run and result.has_changes and result.ok
    assert (result.rows_total, result.rows_pending, result.rows_reverted) == (7, 1, 1)
    assert [t.payee for t in result.created] == ["Coop Pronto"]
    assert sorted(c.reason for c in result.updated) == ["cleared", "settlement"]
    assert [r.reason for r in result.removed] == ["reverted"]
    data = result.to_dict()
    assert data["window"] == ["2026-10-01", "2026-10-13"]
    assert data["updated"][1]["fields"]["amount"] == [-89990, -90410] or data["updated"][0][
        "fields"
    ].get("amount") == [-89990, -90410]


def test_entries_are_never_mutated_by_planning() -> None:
    sim = Sim()
    sim.run(fixture("export_pending_then_completed_a.csv"))
    snapshot = [dataclasses.asdict(e) for e in sim.entries()]
    plan(fixture("export_pending_then_completed_b.csv"), sim.entries(), sim.txs.values())
    assert [dataclasses.asdict(e) for e in sim.entries()] == snapshot
