"""The production upgrade path, end to end (docs/DESIGN.md §2.4, §5.3).

The frozen v1 importer (tests/legacy/v1_revolut_to_ynab.py, driven against the fake YNAB
through ``FakeYnab.v1_request``) imports two statements — the second rendered two hours ahead
(the phone abroad), so v1 re-keys and re-dates rows — and v1's web-UI syncs run in between,
copying YNAB's state (``reconciled``, user memos) into its table and leaving the ghost rows
of finding F02 behind. The user edits YNAB meanwhile. The DB is then migrated by
``ynab_helper.storage`` and v2's ``services.import_csv`` imports the next statements.

v2 must then: create nothing that already exists, never downgrade a reconciled transaction
(F01), never delete a live transaction (F02), keep the user's memo (F33), clear the pendings
that settled (amount and date as Revolut settled them) and remove the reverted ones.
"""

from __future__ import annotations

import contextlib
import io
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FAKE_TOKEN, FakeYnab
from tests.importer.helpers import T, at, done, render_csv, truth
from tests.legacy import v1_revolut_to_ynab as v1
from ynab_helper import services
from ynab_helper.storage import LedgerStore, connect, open_tx_db

from .helpers import ACCT, BUDGET, by_import_id, new_fake, user_ctx

TX_PATH = f"/budgets/{BUDGET}/transactions"

TOPUP = "YNAB:500000:2026-09-01:1"
COOP = "YNAB:-12500:2026-09-01:1"
HOTEL = "YNAB:-150000:2026-09-02:1"
MIGROS = "YNAB:-45300:2026-09-02:1"
AMAZON = "YNAB:-89990:2026-09-02:1"
KIOSK = "YNAB:-3500:2026-09-02:1"
SBB_1 = "YNAB:-3600:2026-09-03:1"


class World:
    """The truth (Revolut), the fake YNAB and the user's data directory."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.fake: FakeYnab = new_fake()
        self.dir = tmp_path
        self.data = tmp_path / "bot_data"
        self.data.mkdir()
        self.db = self.data / "transactions_42.db"
        monkeypatch.setattr(v1, "ynab_request", self.fake.v1_request)
        monkeypatch.setattr(services, "client_factory", lambda ctx: self.fake.client(max_retries=0))
        self.topup = done("2026-09-01 08:00:00", "Top-Up by *4242", "500.00", type="Topup")
        self.coop = done("2026-09-01 12:30:00", "Coop", "-12.50")
        self.tank = T(at("2026-09-02 10:00:00"), "Tamoil", "-60.00")
        self.hotel = T(at("2026-09-02 19:00:00"), "Hotel Bellevue", "-150.00")
        self.migros = T(at("2026-09-02 20:15:00"), "Migros", "-45.30")
        self.amazon = T(at("2026-09-02 22:30:00"), "Amazon EU", "-89.99")
        self.kiosk = done("2026-09-02 23:40:00", "Kiosk Bahnhof", "-3.50")
        self.parking = T(at("2026-09-03 08:00:00"), "Parking Bern", "-4.00")
        self.sbb_1 = done("2026-09-03 09:00:00", "SBB CFF FFS", "-3.60")
        self.sbb_2 = done("2026-09-03 17:00:00", "SBB CFF FFS", "-3.60")
        self.txs: list[T] = [
            self.topup,
            self.coop,
            self.tank,
            self.hotel,
            self.migros,
            self.amazon,
            self.kiosk,
            self.parking,
            self.sbb_1,
            self.sbb_2,
        ]

    # -- v1 --------------------------------------------------------------------

    def v1_import(self, name: str, *, tz_hours: float = 0) -> str:
        csv = self.dir / name
        csv.write_bytes(render_csv(self.txs, tz_hours=tz_hours))
        out = io.StringIO()
        conn = v1.init_db(str(self.db))
        try:
            rows = v1.parse_revolut_csv(str(csv))
            for row in rows:
                row["approved"] = True
            with contextlib.redirect_stdout(out):  # v1 reports through print() (F15)
                v1.import_and_track(conn, FAKE_TOKEN, BUDGET, ACCT, rows)
        finally:
            conn.close()
        return out.getvalue()

    def v1_web_sync(self) -> None:
        """What v1's web UI ran on page loads (all three syncs)."""
        conn = v1.init_db(str(self.db))
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                v1.sync_accounts_from_ynab(conn, FAKE_TOKEN, BUDGET)
                v1.sync_budget_transactions(conn, FAKE_TOKEN, BUDGET, ACCT)
                v1.sync_from_ynab(conn, FAKE_TOKEN, BUDGET, ACCT)
        finally:
            conn.close()

    # -- v2 --------------------------------------------------------------------

    def v2_import(self, name: str) -> Any:
        ctx = user_ctx(self.data, ACCT)
        return services.import_csv(
            ctx, render_csv(self.txs), original_name=name, source_kind="telegram"
        )

    # -- YNAB ------------------------------------------------------------------

    def tx(self, import_id: str) -> dict[str, Any]:
        return by_import_id(self.fake, ACCT, import_id)

    def edit(self, import_id: str, **fields: Any) -> None:
        self.fake.user_edit_transaction(self.tx(import_id)["id"], **fields)

    def live(self) -> list[tuple[int, str]]:
        return sorted(
            (t["amount"], "uncleared" if t["cleared"] == "uncleared" else "cleared")
            for t in self.fake.transactions(BUDGET, account_id=ACCT)
        )

    def writes(self) -> list[Any]:
        return [c for c in self.fake.calls if c.method != "GET"]


def _v1_history(w: World) -> None:
    """Two v1 imports with v1's web syncs and the user's edits in YNAB in between."""
    w.v1_import("s1.csv")
    w.v1_web_sync()
    w.edit(TOPUP, cleared="reconciled")
    w.edit(COOP, cleared="reconciled")
    w.edit(MIGROS, memo="(pending) | party")
    w.v1_web_sync()  # v1 stores 'reconciled' and the user's memo in its own rows (F01)
    w.tank.complete(at("2026-09-03 06:00:00"), "-58.75")  # settled at another amount
    w.parking.state = "REVERTED"
    w.txs.append(done("2026-09-04 10:00:00", "Cafe Central", "-4.80"))
    w.v1_import("s2.csv", tz_hours=2)  # abroad: v1 re-dates and re-keys rows
    w.v1_web_sync()  # leaves the F02 ghost rows (old keys of the re-keyed rows)
    assert w.live() == truth(w.txs)  # v1 itself got YNAB right up to here


@pytest.mark.parametrize("drop_started_at", [False, True], ids=["started_at", "no_started_at"])
def test_F01_F02_F33_v1_history_then_v2_imports_converge_without_damage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, drop_started_at: bool
) -> None:
    w = World(tmp_path, monkeypatch)
    _v1_history(w)
    raw = sqlite3.connect(w.db)
    # v1 deleted the superseded Tamoil pending and the reverted parking hold; its web sync
    # then flagged both rows deleted (the migration turns them into DELETED_IN_YNAB).
    removed = raw.execute(
        "SELECT import_id FROM transactions WHERE deleted = 1 ORDER BY import_id"
    ).fetchall()
    assert [r[0] for r in removed] == ["YNAB:-4000:2026-09-03:1", "YNAB:-60000:2026-09-02:1"]
    ghosts = raw.execute(
        "SELECT COUNT(*) FROM transactions t JOIN transactions u ON t.ynab_tx_id = u.ynab_tx_id "
        "AND t.import_id < u.import_id WHERE t.deleted = 0 AND u.deleted = 0"
    ).fetchone()[0]
    assert ghosts >= 1  # two live local rows for one YNAB transaction (F02 precondition)
    if drop_started_at:  # most production rows predate v1's started_at column
        raw.execute("UPDATE transactions SET started_at = NULL")
        raw.commit()
    raw.close()

    # The upgrade: migrate in place (with a backup), then the user reconciles in YNAB.
    open_tx_db(w.db, default_account_id=ACCT, backup_dir=w.data / "backups").close()
    assert list((w.data / "backups").glob("transactions_42.db.*.pre-v1.db"))
    for import_id in (TOPUP, COOP, SBB_1):
        w.edit(import_id, cleared="reconciled")
    before = {t["id"]: t for t in w.fake.transactions(BUDGET, account_id=ACCT)}
    amazon_id, kiosk_id, hotel_id = w.tx(AMAZON)["id"], w.tx(KIOSK)["id"], w.tx(HOTEL)["id"]
    w.fake.reset_log()

    # v2, statement 3 (home): pendings settle, the hold is reverted, a new pending appears.
    w.hotel.state = "REVERTED"
    w.migros.complete(at("2026-09-04 06:00:00"))
    w.amazon.complete(at("2026-09-04 06:00:01"), "-90.41")
    coop_2 = T(at("2026-09-05 11:00:00"), "Coop", "-11.20")
    w.txs.append(coop_2)
    third = w.v2_import("s3.csv")
    # v2, statement 4: the new pending completes, one more row.
    coop_2.complete(at("2026-09-06 06:00:00"))
    w.txs.append(done("2026-09-06 08:00:00", "Bakery", "-6.20"))
    fourth = w.v2_import("s4.csv")

    assert third.ok and fourth.ok, (third.to_dict(), fourth.to_dict())
    assert w.live() == truth(w.txs)  # nothing duplicated, nothing missing
    posted = [
        tx["import_id"] for c in w.writes() if c.method == "POST" for tx in c.body["transactions"]
    ]
    assert posted == ["YNAB:-11200:2026-09-05:1", "YNAB:-6200:2026-09-06:1"]  # only new rows
    deleted = [c.path.rsplit("/", 1)[1] for c in w.writes() if c.method == "DELETE"]
    assert deleted == [hotel_id]  # F02: no live transaction deleted, only the reverted hold
    after = {t["id"]: t for t in w.fake.transactions(BUDGET, account_id=ACCT)}
    assert set(before) - set(after) == {hotel_id}
    for import_id in (TOPUP, COOP, SBB_1):
        assert w.tx(import_id)["cleared"] == "reconciled", import_id  # F01
    migros = w.tx(MIGROS)
    assert (migros["cleared"], migros["memo"]) == ("cleared", "party")  # F33
    amazon = after[amazon_id]  # the settled pending is the same transaction, updated
    assert (amazon["amount"], amazon["cleared"], amazon["date"]) == (
        -90_410,
        "cleared",
        "2026-09-02",
    )
    assert after[kiosk_id]["date"] == "2026-09-02"  # back to the home rendering
    assert all(c.method in ("POST", "PATCH", "DELETE") for c in w.writes())

    conn = connect(w.db)
    try:
        counts = LedgerStore(conn).counts_by_status(ACCT)
    finally:
        conn.close()
    assert counts["UNLINKED"] == 0 and counts["ACTIVE"] == len(truth(w.txs))

    w.fake.reset_log()
    again = w.v2_import("s4-again.csv")
    assert again.ok and not again.has_changes and w.writes() == []
    w.fake.check_invariants()


def test_v2_after_v1_never_recreates_what_the_user_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    w = World(tmp_path, monkeypatch)
    _v1_history(w)
    w.fake.user_delete_transaction(w.tx(KIOSK)["id"])  # deleted in YNAB before the upgrade
    w.v1_web_sync()
    w.fake.reset_log()

    result = w.v2_import("s3.csv")

    assert result.ok and result.ignored_deleted >= 1
    assert not [c for c in w.writes() if c.method == "POST"]
    assert w.fake.find_by_import_id(ACCT, KIOSK, include_deleted=False) is None
