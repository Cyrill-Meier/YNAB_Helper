"""The v2 planner continuing from a ledger the *frozen v1 importer* produced.

v1 (tests/legacy/v1_revolut_to_ynab.py) imports the first export into the stateful fake YNAB
(via ``FakeYnab.v1_request``) and its own SQLite DB; the storage migration turns that DB into
the v2 ledger, the mirror is filled from the fake through the real YnabClient, and the v2
planner plans the next export. This is the production upgrade path.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.fakes.fake_ynab import FAKE_TOKEN, FakeYnab
from tests.legacy import v1_revolut_to_ynab as v1
from ynab_helper.importer import (
    ActionKind,
    MatchMethod,
    PlanOptions,
    compute_allow_sweep,
    plan_import,
)
from ynab_helper.revolut import parse_export
from ynab_helper.storage import LedgerStore, MirrorStore, open_tx_db

from .helpers import FIXTURES, T, at, done, render_csv

BUDGET = "budget-1"
ACCT = "acct-main"


def _v1_import(fake: FakeYnab, db: Path, csv: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(v1, "ynab_request", fake.v1_request)
    conn = v1.init_db(str(db))
    try:
        txs = v1.parse_revolut_csv(str(csv))
        for tx in txs:
            tx["approved"] = True
        v1.import_and_track(conn, FAKE_TOKEN, BUDGET, ACCT, txs)
    finally:
        conn.close()


def _v1_import_statement(
    fake: FakeYnab, db: Path, txs: list[T], name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    csv = db.parent / name
    csv.write_bytes(render_csv(txs))
    _v1_import(fake, db, csv, monkeypatch)


def _live(fake: FakeYnab) -> list[tuple[int, str]]:
    return sorted(
        (t["amount"], "uncleared" if t["cleared"] == "uncleared" else "cleared")
        for t in fake.transactions(BUDGET)
        if not t["deleted"] and t["account_id"] == ACCT
    )


def _plan(exp, entries, view):
    opts = PlanOptions(
        account_id=ACCT,
        approved=True,
        allow_sweep=compute_allow_sweep(exp, entries, account_id=ACCT),
    )
    return plan_import(exp, entries, view, opts)


def _upgrade(fake: FakeYnab, db: Path, tmp_path: Path, *, drop_started_at: bool = False):
    if drop_started_at:  # most production rows predate v1's started_at column
        raw = sqlite3.connect(db)
        raw.execute("UPDATE transactions SET started_at = NULL")
        raw.commit()
        raw.close()
    conn = open_tx_db(db, default_account_id=ACCT, backup_dir=tmp_path / "backups")
    txs, knowledge = fake.client().get_transactions(BUDGET)
    mirror = MirrorStore(conn)
    mirror.upsert_transactions(BUDGET, [t.raw for t in txs], server_knowledge=knowledge)
    view = mirror.view_for_account(BUDGET, ACCT)
    ledger = LedgerStore(conn)
    ledger.refresh_from_mirror(ACCT, view)
    return conn, ledger.list_for_account(ACCT), view


@pytest.fixture
def fake() -> FakeYnab:
    f = FakeYnab()
    f.add_budget(BUDGET)
    f.add_account(BUDGET, ACCT, "Revolut")
    return f


@pytest.mark.parametrize("drop_started_at", [False, True])
def test_C01_after_a_v1_import(fake, tmp_path, monkeypatch, capsys, drop_started_at) -> None:
    db = tmp_path / "transactions_1.db"
    _v1_import(fake, db, FIXTURES / "export_same_amount_same_day_a.csv", monkeypatch)
    capsys.readouterr()  # v1 prints its summary
    conn, entries, view = _upgrade(fake, db, tmp_path, drop_started_at=drop_started_at)
    try:
        assert {e.identity for e in entries} == {"legacy"}
        y1 = view.by_import_id("YNAB:-3600:2026-10-05:1")
        y2 = view.by_import_id("YNAB:-3600:2026-10-05:2")
        coffee = view.by_import_id("YNAB:-4500:2026-10-05:2")
        b = parse_export(FIXTURES / "export_same_amount_same_day_b.csv")
        opts = PlanOptions(
            account_id=ACCT,
            approved=True,
            allow_sweep=compute_allow_sweep(b, entries, account_id=ACCT),
        )
        p = plan_import(b, entries, view, opts)
        writes = {(a.kind, a.ynab_tx_id): dict(a.fields) for a in p.ynab_actions}
        assert writes == {
            (ActionKind.UPDATE, y1.id): {
                "id": y1.id,
                "cleared": "cleared",
                "memo": "",
                "amount": -3800,
            },
            (ActionKind.UPDATE, coffee.id): {"id": coffee.id, "cleared": "cleared", "memo": ""},
        }
        assert all(a.ynab_tx_id != y2.id for a in p.ynab_actions)
        expected = {MatchMethod.KEY} if drop_started_at else {MatchMethod.TIMESTAMP}
        assert {m.method for m in p.matches} == expected | {MatchMethod.SETTLEMENT}
    finally:
        conn.close()


def test_tz_shift_after_a_v1_import_creates_nothing(fake, tmp_path, monkeypatch, capsys) -> None:
    db = tmp_path / "transactions_1.db"
    _v1_import(fake, db, FIXTURES / "export_basic.csv", monkeypatch)
    capsys.readouterr()
    conn, entries, view = _upgrade(fake, db, tmp_path)
    try:
        shifted = parse_export(FIXTURES / "export_tz_shift.csv")
        opts = PlanOptions(account_id=ACCT, approved=True)
        p = plan_import(shifted, entries, view, opts)
        assert p.by_kind(ActionKind.CREATE, ActionKind.DELETE) == []
        assert {a.reason for a in p.by_kind(ActionKind.UPDATE)} == {"redated"}
    finally:
        conn.close()


def test_v1_renumbered_group_sharing_a_started_at_upgrades_without_writes(
    fake, tmp_path, monkeypatch, capsys
) -> None:
    # Review blocker: v1 writes started_at (and payee) by key, and a renumbering moves keys
    # between rows of one (amount, date) group, so two migrated entries end up with the same
    # started_at. The first v2 import of the same statement must change nothing.
    db = tmp_path / "transactions_1.db"
    early = T(at("2026-09-03 08:30:04"), "SBB CFF FFS", "-3.60")
    late = done("2026-09-03 17:43:25", "SBB CFF FFS", "-3.60")
    _v1_import_statement(fake, db, [early, late], "a.csv", monkeypatch)  # early is :1
    early.complete(at("2026-09-04 06:00:00"))
    _v1_import_statement(fake, db, [early, late], "b.csv", monkeypatch)  # late is :1 now
    capsys.readouterr()
    assert _live(fake) == [(-3600, "cleared"), (-3600, "cleared")]  # v1 got it right
    raw = sqlite3.connect(db)
    started = raw.execute(
        "SELECT started_at FROM transactions WHERE import_id LIKE 'YNAB:-3600:%'"
    ).fetchall()
    raw.close()
    assert len({s for (s,) in started}) == 1, started  # the precondition of the finding
    conn, entries, view = _upgrade(fake, db, tmp_path)
    try:
        p = _plan(parse_export(render_csv([early, late])), entries, view)
        assert p.ynab_actions == [], [(a.kind.value, a.reason) for a in p.ynab_actions]
        assert len(p.matches) == 2
    finally:
        conn.close()


def test_F11_v1_synced_renamed_payee_then_shifted_statement_creates_nothing(
    fake, tmp_path, monkeypatch, capsys
) -> None:
    # v1's web sync copied YNAB's renamed payee into the row, which the migration turns into
    # csv_payee. A statement rendered one hour ahead moves the 23:15 kiosk row across
    # midnight: the legacy date shift must not need the (renamed) payee.
    txs = [
        done("2026-10-04 10:00:00", "Coop", "-12.00"),
        done("2026-10-05 23:15:00", "Kiosk Bahnhof Bern", "-3.50"),
        done("2026-10-07 10:00:00", "Migros", "-22.00"),
    ]
    db = tmp_path / "transactions_1.db"
    _v1_import_statement(fake, db, txs, "a.csv", monkeypatch)
    kiosk = fake.find_by_import_id(ACCT, "YNAB:-3500:2026-10-05:1")
    fake.user_edit_transaction(kiosk["id"], payee_name="Kiosk")
    conn = v1.init_db(str(db))
    v1.sync_budget_transactions(conn, FAKE_TOKEN, BUDGET, ACCT)
    conn.close()
    capsys.readouterr()
    conn, entries, view = _upgrade(fake, db, tmp_path, drop_started_at=True)
    try:
        (legacy,) = [e for e in entries if e.key == "YNAB:-3500:2026-10-05:1"]
        assert legacy.csv_payee == "Kiosk" and legacy.identity == "legacy"
        p = _plan(parse_export(render_csv(txs, tz_hours=1)), entries, view)
        assert p.by_kind(ActionKind.CREATE, ActionKind.DELETE) == []
        assert [m.method for m in p.matches if m.entry_key == legacy.key] == [
            MatchMethod.DATE_SHIFT
        ]
    finally:
        conn.close()
