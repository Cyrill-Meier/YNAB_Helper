"""Planner and executor-level regression tests for the final review's core findings (R01 R02
R06 R07 R11 R17 R28). The service-level scenarios are in tests/services/test_core_review_fixes.py.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from ynab_helper.importer import ActionKind, MatchMethod
from ynab_helper.importer.models import unconfirmed_write, unconfirmed_write_note

from .helpers import Sim, StoreSim, entry, export, plan, ytx, ytx_for

H = 3600
REPO = Path(__file__).resolve().parents[2]


def _deleted_pending(**kw):
    values = {
        "status": "DELETED_IN_YNAB",
        "csv_payee": "Amazon EU",
        "csv_state": "PENDING",
        "pushed_cleared": "uncleared",
        "started_at": "2026-10-10 12:00:00",
        "ynab_tx_id": "y-amazon",
    }
    values.update(kw)
    return entry("YNAB:-50000:2026-10-10:1", **values)


# ── R01 ──────────────────────────────────────────────────────────────────────


def test_core_R01_settlement_of_a_pending_deleted_in_ynab_is_matched_not_created() -> None:
    e = _deleted_pending()
    y = ytx_for(e, deleted=True)
    exp = export(
        ("2026-10-11 08:00:00", "Starbucks", "-4.50"),
        ("2026-10-10 12:00:00", "Amazon EU", "-50.65"),
    )
    p = plan(exp, [e], [y])
    assert [(m.entry_key, m.method) for m in p.matches] == [(e.key, MatchMethod.SETTLEMENT)]
    assert [a.row.payee for a in p.by_kind(ActionKind.CREATE)] == ["Starbucks"]
    assert p.ignored_deleted == 1
    (refresh,) = p.by_kind(ActionKind.LEDGER)
    assert refresh.ledger_updates["csv_amount"] == -50_650  # the next statement matches by time


def test_core_R01_a_new_purchase_near_a_deleted_pending_is_still_created() -> None:
    # The deleted pending vanished (cancelled order); a different purchase from the same
    # merchant hours later is the only candidate — but not at the pending's instant.
    e = _deleted_pending()
    exp = export(("2026-10-10 18:42:17", "Amazon EU", "-23.90"))
    p = plan(exp, [e], [ytx_for(e, deleted=True)])
    assert p.matches == []
    assert [a.row.amount for a in p.by_kind(ActionKind.CREATE)] == [-23_900]


def test_core_R01_live_pendings_are_settled_before_deleted_ones() -> None:
    # A re-timed settlement row is the only candidate of an ACTIVE pending and of a deleted
    # one: the live pending takes it (as before), the deleted one is not re-created either.
    active = entry(
        "YNAB:-20000:2026-10-10:1",
        csv_payee="Amazon EU",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-10-10 09:00:00",
    )
    deleted = _deleted_pending()
    exp = export(("2026-10-10 18:00:00", "Amazon EU", "-20.40"))
    p = plan(exp, [active, deleted], [ytx_for(active), ytx_for(deleted, deleted=True)])
    assert [(m.entry_key, m.method) for m in p.matches] == [(active.key, MatchMethod.SETTLEMENT)]
    assert [a.reason for a in p.ynab_actions] == ["settlement"]


def test_core_R01_an_unlinked_pending_that_settled_is_posted_under_its_own_import_id() -> None:
    # A create whose outcome was unknown (R07) and that never reached YNAB: the settled row is
    # POSTed under the pending's import_id, never under a second one.
    e = entry(
        "YNAB:-50000:2026-10-10:1",
        status="UNLINKED",
        ynab_tx_id=None,
        csv_payee="Hotel Zurich",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-10-10 12:00:00",
    )
    exp = export(("2026-10-10 12:00:00", "Hotel Zurich", "-52.30"))
    p = plan(exp, [e], [])
    (create,) = p.by_kind(ActionKind.CREATE)
    assert (create.reason, create.import_id, create.payload["amount"]) == (
        "unlinked_missing",
        e.key,
        -52_300,
    )


# ── R02 ──────────────────────────────────────────────────────────────────────


def test_core_R02_the_key_holder_is_adopted_and_its_edited_amount_is_never_overwritten() -> None:
    sim = Sim()
    sim.add_tx(
        ytx(
            "y-hotel",
            amount=-140_000,  # the user changed the hold in YNAB; the ledger was lost since
            date="2026-09-02",
            memo="(pending)",
            import_id="YNAB:-150000:2026-09-02:1",
            payee_name="Hotel",
        )
    )
    pad = ("2026-09-01 09:00:00", "Pad", "-1.00")
    p = sim.run(export(pad, ("2026-09-02 15:00:00", "Hotel", "-150.00", "PENDING")))
    assert [(a.kind.value, a.reason) for a in p.actions if a.row.payee == "Hotel"] == [
        ("LINK", "adopted")
    ]
    hotel = sim.get("YNAB:-150000:2026-09-02:1")
    assert (hotel.csv_state, hotel.pushed_amount) == ("PENDING", -150_000)  # what was POSTed
    # It settles at another amount: YNAB holds the user's value, not ours — cleared only.
    p = sim.run(export(pad, ("2026-09-02 15:00:00", "Hotel", "-162.40")))
    (update,) = p.by_kind(ActionKind.UPDATE)
    assert "amount" not in update.fields and update.fields["cleared"] == "cleared"
    assert any("edited in YNAB" in w for w in p.warnings)
    assert [t.amount for t in sim.live() if t.payee_name == "Hotel"] == [-140_000]


def test_core_R02_an_older_statement_through_a_fresh_ledger_then_the_current_one() -> None:
    # The hold was created as YNAB:-150000:… and settled (PATCHed) to -162.40 elsewhere; a
    # fresh ledger first sees an older statement (still pending), then the current one.
    sim = Sim()
    sim.add_tx(
        ytx(
            "y-hotel",
            amount=-162_400,
            date="2026-09-02",
            cleared="cleared",
            import_id="YNAB:-150000:2026-09-02:1",
            payee_name="Hotel",
        )
    )
    pad = ("2026-09-01 09:00:00", "Pad", "-1.00")
    older = export(pad, ("2026-09-02 15:00:00", "Hotel", "-150.00", "PENDING"))
    p = sim.run(older)
    assert [a.reason for a in p.ynab_actions] == ["new"]  # only the pad
    current = export(pad, ("2026-09-02 15:00:00", "Hotel", "-162.40"))
    p = sim.run(current)
    assert p.ynab_actions == [] and p.by_kind(ActionKind.CREATE) == []
    assert [t.amount for t in sim.live() if t.payee_name == "Hotel"] == [-162_400]


# ── R06 ──────────────────────────────────────────────────────────────────────


def test_core_R06_a_deleted_entry_teaches_the_export_offset() -> None:
    e = entry(
        "YNAB:-1500000:2026-09-01:1",
        status="DELETED_IN_YNAB",
        csv_payee="Landlord",
        started_at="2026-09-01 13:51:23",
        ynab_tx_id="y-landlord",
    )
    exp = export(
        ("2026-09-01 07:51:23", "Landlord", "-1500.00", "COMPLETED", "Transfer"),
        ("2026-09-03 07:34:12", "Cafe", "-4.50"),
    )
    p = plan(exp, [e], [ytx_for(e, deleted=True)])
    assert p.offsets == {-6 * H: 1}
    assert [(m.entry_key, m.offset_seconds) for m in p.matches] == [(e.key, -6 * H)]
    assert [a.row.payee for a in p.by_kind(ActionKind.CREATE)] == ["Cafe"]
    assert p.ignored_deleted == 1


def test_core_R06_a_reverted_entry_teaches_the_export_offset_too() -> None:
    e = entry(
        "YNAB:-80000:2026-09-01:1",
        status="REVERTED",
        csv_payee="Shell",
        csv_state="REVERTED",
        started_at="2026-09-01 10:15:07",
    )
    exp = export(
        ("2026-09-01 18:15:07", "Shell", "-80.00", "REVERTED"),
        ("2026-09-02 08:00:00", "Cafe", "-4.50"),
    )
    p = plan(exp, [e], [])
    assert p.offsets == {8 * H: 1}
    assert [(m.entry_key, m.offset_seconds) for m in p.matches] == [(e.key, 8 * H)]
    assert [a.row.payee for a in p.by_kind(ActionKind.CREATE)] == ["Cafe"]
    assert p.frame_offset == 8 * H  # the new row is stored in the ledger's frame


def test_core_R06_settlement_window_uses_the_entrys_rendered_date() -> None:
    # Imported from a +8 h statement: canonical instant 09-01 20:24:30, rendered on 09-02.
    pending = entry(
        "YNAB:-3600:2026-09-01:1",
        csv_date="2026-09-02",
        csv_payee="Night Market",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        pushed_date="2026-09-02",
        started_at="2026-09-01 20:24:30",
    )
    exp = export(
        ("2026-09-02 04:24:30", "Night Market", "-3.74"),
        ("2026-09-03 09:10:11", "Taxi", "-12.00"),
    )
    p = plan(exp, [pending], [ytx_for(pending)])
    assert [(m.entry_key, m.method) for m in p.matches] == [(pending.key, MatchMethod.SETTLEMENT)]
    assert [a.row.payee for a in p.by_kind(ActionKind.CREATE)] == ["Taxi"]


def _home_pending(started: str):
    return entry(
        "YNAB:-12300:2026-09-01:1",
        csv_payee="M001",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at=started,
    )


def test_core_R06_a_settled_pending_is_the_only_overlap_of_a_statement_from_abroad() -> None:
    # Imported at home on 09-01 19:49:43; the next statement, rendered +8 h, covers only
    # 09-02: the settled row (09-02 03:49:43) is its first row, the pending's own date lies
    # outside the window and no offset is learned. The distinctive +8 h instant decides.
    pending = _home_pending("2026-09-01 19:49:43")
    exp = export(
        ("2026-09-02 03:49:43", "M001", "-12.42"),
        ("2026-09-02 12:25:36", "M002", "-4.50", "PENDING"),
    )
    p = plan(exp, [pending], [ytx_for(pending)])
    assert [(m.entry_key, m.method, m.offset_seconds) for m in p.matches] == [
        (pending.key, MatchMethod.SETTLEMENT, 8 * H)
    ]
    assert [a.row.payee for a in p.by_kind(ActionKind.CREATE)] == ["M002"]
    assert p.frame_offset == 8 * H


def test_core_R06_round_times_outside_the_window_are_not_settlements() -> None:
    # The same shape at a round time (scheduled transfers, F10): not evidence of an offset.
    pending = _home_pending("2026-09-01 19:00:00")
    exp = export(("2026-09-02 03:00:00", "M001", "-12.42"))
    p = plan(exp, [pending], [ytx_for(pending)])
    assert p.matches == []


# ── R07: write-ahead placeholders in the executor (store-backed) ─────────────


def test_core_R07_placeholder_entries_link_by_import_id_whatever_the_amount(tmp_path) -> None:
    sim = StoreSim(tmp_path)
    try:
        # What the executor leaves after a POST whose answer was lost: an UNLINKED entry
        # carrying the import_id; YNAB has the transaction, settled since by the user.
        exp = export(("2026-10-10 12:00:00", "Hotel Zurich", "-50.00", "PENDING"))
        (create,) = sim.plan(exp)[0].by_kind(ActionKind.CREATE)
        sim.ledger.insert(create.entry)
        sim.txs["t1"] = ytx(
            "t1",
            amount=-50_000,
            date="2026-10-10",
            import_id=create.import_id,
            payee_name="Hotel Zurich",
        )
        settled = export(("2026-10-10 12:00:00", "Hotel Zurich", "-52.30"))
        p = sim.run(settled)
        assert [a.reason for a in p.ynab_actions] == ["settlement"]
        assert [(t.amount, t.cleared) for t in sim.live()] == [(-52_300, "cleared")]
    finally:
        sim.close()


# ── R11 ──────────────────────────────────────────────────────────────────────


def _bar(**kw):
    values = {
        "csv_payee": "Bar",
        "csv_date": "2026-09-02",
        "pushed_date": "2026-09-02",
        "started_at": "2026-09-02 23:30:12",
    }
    values.update(kw)
    return entry("YNAB:-30400:2026-09-02:1", **values)


_ABROAD = ("2026-09-03 00:30:12", "Bar", "-30.40")
_FILLER = ("2026-09-04 10:00:00", "Kiosk", "-2.10")


def test_core_R11_ynab_agreeing_with_the_statement_is_not_recorded_as_our_write() -> None:
    e = _bar()
    y = ytx_for(e, date="2026-09-03")  # the user moved it to the next day
    p = plan(export(_ABROAD, _FILLER), [e], [y], allow_sweep=False)
    (refresh,) = [a for a in p.actions if a.entry_key == e.key]
    assert refresh.kind is ActionKind.LEDGER
    assert refresh.ledger_updates["csv_date"] == "2026-09-03"
    assert "pushed_date" not in refresh.ledger_updates  # still what we last wrote


def test_core_R11_an_unconfirmed_write_found_in_ynab_is_recorded_as_ours() -> None:
    note = unconfirmed_write_note({"id": "y", "date": "2026-09-03", "cleared": "cleared"})
    assert note == 'unconfirmed write: {"date": "2026-09-03"}'
    e = _bar(last_error=note)
    assert unconfirmed_write(e) == {"date": "2026-09-03"}
    p = plan(export(_ABROAD, _FILLER), [e], [ytx_for(e, date="2026-09-03")], allow_sweep=False)
    (refresh,) = [a for a in p.actions if a.entry_key == e.key]
    assert refresh.ledger_updates["pushed_date"] == "2026-09-03"
    assert refresh.ledger_updates["last_error"] is None


def test_core_R11_an_unconfirmed_write_that_did_not_land_is_sent_again() -> None:
    e = _bar(last_error=unconfirmed_write_note({"date": "2026-09-03"}))
    p = plan(export(_ABROAD, _FILLER), [e], [ytx_for(e)], allow_sweep=False)
    (update,) = p.by_kind(ActionKind.UPDATE)
    assert dict(update.fields) == {"id": e.ynab_tx_id, "date": "2026-09-03"}
    assert update.ledger_updates["last_error"] is None


def test_core_R11_a_later_change_still_patches_over_our_unconfirmed_value() -> None:
    # The lost PATCH set -90.41; a later re-quote says -91.00: YNAB holds our value.
    e = entry(
        "YNAB:-89990:2026-10-11:1",
        csv_payee="Amazon EU",
        csv_state="PENDING",
        pushed_cleared="uncleared",
        started_at="2026-10-11 22:15:00",
        last_error=unconfirmed_write_note({"amount": -90_410}),
    )
    exp = export(
        ("2026-10-10 09:00:00", "Pad", "-1.00"),
        ("2026-10-11 22:15:00", "Amazon EU", "-91.00", "PENDING"),
    )
    p = plan(exp, [e], [ytx_for(e, amount=-90_410)])
    (update,) = p.by_kind(ActionKind.UPDATE)
    assert update.fields["amount"] == -91_000


# ── R17 ──────────────────────────────────────────────────────────────────────


def test_core_R17_a_deleted_entry_whose_row_is_reverted_becomes_reverted_quietly() -> None:
    e = _deleted_pending()
    exp = export(
        ("2026-10-09 09:00:00", "Pad", "-1.00"),
        ("2026-10-10 12:00:00", "Amazon EU", "-50.00", "REVERTED"),
    )
    p = plan(exp, [e], [ytx_for(e, deleted=True)])
    assert p.ignored_deleted == 0 and p.unchanged == 1
    assert not any("deleted in YNAB" in w for w in p.warnings)
    (ledger,) = [a for a in p.actions if a.entry_key == e.key]
    assert (ledger.kind, ledger.reason) == (ActionKind.LEDGER, "reverted")
    assert ledger.ledger_updates["status"] == "REVERTED"
    assert ledger.ledger_updates["csv_state"] == "REVERTED"


# ── R28 ──────────────────────────────────────────────────────────────────────


def test_core_R28_the_importer_package_does_not_load_the_http_client() -> None:
    code = (
        "import sys, ynab_helper.importer\n"
        "loaded = [m for m in ('ynab_helper.ynab.client', 'ynab_helper.net', "
        "'ynab_helper.importer.executor') if m in sys.modules]\n"
        "print(loaded)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], cwd=REPO, capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "[]"


def test_core_R28_the_planner_and_the_client_share_one_import_id_limit() -> None:
    from ynab_helper.importer import planner
    from ynab_helper.ynab import client, models

    assert planner.MAX_IMPORT_ID_LENGTH is models.MAX_IMPORT_ID_LENGTH
    assert client.MAX_IMPORT_ID_LENGTH is models.MAX_IMPORT_ID_LENGTH == 36
