"""Import-flow regression tests: reverted rows, date-shifted rows, orphaned
pendings, and change reporting.

Run directly (no pytest needed):  python3 tests/test_import_flows.py
"""
import io
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import revolut_to_ynab as ynab
import revolut_ynab_bot as bot

calls = []


def fake_ynab_request(method, path, token, body=None):
    calls.append((method, path, body))
    if method == "POST":
        txs = body["transactions"]
        return {"data": {
            "transaction_ids": ["x"] * len(txs),
            "transactions": [
                {"import_id": t["import_id"], "id": f"created-{t['import_id']}"}
                for t in txs
            ],
            "duplicate_import_ids": [],
        }}
    return {"data": {}}


ynab.ynab_request = fake_ynab_request


def mk(date, amount, payee, state="COMPLETED", t="12:00:00", occ=1):
    return {
        "date": date, "amount": amount, "payee_name": payee,
        "memo": None,
        "cleared": "cleared" if state == "COMPLETED" else "uncleared",
        "approved": True, "import_id": f"YNAB:{amount}:{date}:{occ}",
        "_state": state, "_started_time": t, "_started_at": f"{date} {t}",
    }


def fresh_db(tmpdir, name):
    return ynab.init_db(os.path.join(tmpdir, name))


def run_import(conn, txs):
    calls.clear()
    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf
    try:
        ynab.import_and_track(conn, "t", "b", "a", [dict(t) for t in txs])
    finally:
        sys.stdout = old_stdout
    return buf.getvalue()


def test_reverted(tmpdir):
    conn = fresh_db(tmpdir, "reverted.db")
    # previously imported pending, now reported REVERTED -> delete in YNAB
    ynab.db_upsert(conn, mk("2026-08-09", -5000, "Shop", "PENDING"), ynab_tx_id="y1")
    conn.commit()
    run_import(conn, [
        mk("2026-08-09", -5000, "Shop", "REVERTED"),
        mk("2026-08-10", -300, "Kiosk", "REVERTED"),   # never imported -> skip
        mk("2026-08-10", -800, "Cafe"),                # normal create
    ])
    deletes = [p for m, p, _ in calls if m == "DELETE"]
    posts = [b for m, _, b in calls if m == "POST"]
    assert deletes == ["/budgets/b/transactions/y1"], deletes
    assert len(posts) == 1 and len(posts[0]["transactions"]) == 1
    row = conn.execute(
        "SELECT deleted, state FROM transactions WHERE import_id='YNAB:-5000:2026-08-09:1'"
    ).fetchone()
    assert (row[0], row[1]) == (1, "REVERTED")
    assert run_import(conn, [mk("2026-08-09", -5000, "Shop", "REVERTED")]) and calls == []
    conn.close()
    print("ok: reverted rows deleted once, never created, converge")


def test_date_shift(tmpdir):
    conn = fresh_db(tmpdir, "dateshift.db")
    ynab.db_upsert(conn, mk("2026-08-04", 2000000, "Top-up", t="02:32:04"), ynab_tx_id="y-dep")
    conn.commit()
    out = run_import(conn, [mk("2026-08-03", 2000000, "Top-up", t="22:32:04")])
    assert not [c for c in calls if c[0] == "POST"]
    patches = [c for c in calls if c[0] == "PATCH"]
    assert len(patches) == 1 and "y-dep" in patches[0][1]
    assert patches[0][2]["transaction"]["date"] == "2026-08-03"
    assert "date 2026-08-04 → 2026-08-03" in out
    rows = conn.execute("SELECT import_id FROM transactions WHERE amount=2000000").fetchall()
    assert [r[0] for r in rows] == ["YNAB:2000000:2026-08-03:1"]
    # guards: midday twin and unlinked rows still create
    ynab.db_upsert(conn, mk("2026-08-06", -2500, "Cafe Noon"), ynab_tx_id="y-noon")
    conn.commit()
    run_import(conn, [mk("2026-08-07", -2500, "Cafe Noon")])
    assert [c[0] for c in calls] == ["POST"]
    conn.close()
    print("ok: date-shifted row re-keyed via PATCH, guards intact")


def test_timezone_shift(tmpdir):
    """Export made in Las Vegas (UTC-7), re-exported from Zürich (UTC+2):
    every timestamp moves +9h, evening rows land on the next day."""
    conn = fresh_db(tmpdir, "tzshift.db")
    ynab.db_upsert(conn, mk("2026-08-22", -20000, "Lunch", t="12:15:10"), ynab_tx_id="y-lunch")
    ynab.db_upsert(conn, mk("2026-08-22", -30000, "Dinner", t="19:30:05"), ynab_tx_id="y-dinner")
    ynab.db_upsert(conn, mk("2026-08-22", -180, "Toll", t="22:10:00"), ynab_tx_id="y-toll")
    conn.commit()
    out = run_import(conn, [
        mk("2026-08-22", -20000, "Lunch", t="21:15:10"),          # same day, reveals +9h
        mk("2026-08-23", -30000, "Dinner", t="04:30:05"),         # crossed midnight, 04:30 is
                                                                  # outside the old 3h gate
        mk("2026-08-23", -180, "Toll", t="06:10:00", occ=1),      # genuinely new toll (+8h)
        mk("2026-08-23", -180, "Toll", t="07:10:00", occ=2),      # the old toll, shifted +9h
    ])
    posts = [b["transactions"] for m, _, b in calls if m == "POST"]
    assert len(posts) == 1 and len(posts[0]) == 1, posts
    assert posts[0][0]["import_id"] == "YNAB:-180:2026-08-23:1", posts[0]
    patches = {c[1].rsplit("/", 1)[1]: c[2]["transaction"] for c in calls if c[0] == "PATCH"}
    assert set(patches) == {"y-dinner", "y-toll"}, patches
    assert patches["y-dinner"]["date"] == "2026-08-23"
    assert "date 2026-08-22 → 2026-08-23" in out
    ids = sorted(r[0] for r in conn.execute(
        "SELECT import_id FROM transactions WHERE deleted = 0"))
    assert ids == [
        "YNAB:-180:2026-08-23:1", "YNAB:-180:2026-08-23:2",
        "YNAB:-20000:2026-08-22:1", "YNAB:-30000:2026-08-23:1",
    ], ids
    # Re-importing the same export converges
    run_import(conn, [
        mk("2026-08-22", -20000, "Lunch", t="21:15:10"),
        mk("2026-08-23", -30000, "Dinner", t="04:30:05"),
        mk("2026-08-23", -180, "Toll", t="06:10:00", occ=1),
        mk("2026-08-23", -180, "Toll", t="07:10:00", occ=2),
    ])
    assert calls == [], calls
    # Guard: a same-payee/amount neighbour whose time difference isn't a
    # timezone offset (here 3h17m) is a different transaction — create.
    ynab.db_upsert(conn, mk("2026-08-25", -4500, "Cafe", t="23:10:00"), ynab_tx_id="y-cafe")
    conn.commit()
    run_import(conn, [mk("2026-08-26", -4500, "Cafe", t="02:27:00")])
    assert [c[0] for c in calls] == ["POST"], calls
    # Legacy rows without started_at still use the near-midnight heuristic
    ynab.db_upsert(conn, mk("2026-08-27", -7000, "Bar", t="23:40:00"), ynab_tx_id="y-bar")
    ynab.db_upsert(conn, mk("2026-08-27", -8000, "Shop", t="14:00:00"), ynab_tx_id="y-shop")
    conn.execute("UPDATE transactions SET started_at = NULL WHERE payee_name IN ('Bar', 'Shop')")
    conn.commit()
    run_import(conn, [
        mk("2026-08-28", -7000, "Bar", t="00:40:00"),
        mk("2026-08-28", -8000, "Shop", t="15:00:00"),
    ])
    assert sorted(c[0] for c in calls) == ["PATCH", "POST"], calls
    conn.close()
    print("ok: 9h export offset learned from matched rows, evening rows re-keyed, guards intact")


def test_superseded_pending(tmpdir):
    conn = fresh_db(tmpdir, "superseded.db")
    # combined dinner+tip auth, later settled as two separate rows
    ynab.db_upsert(conn, mk("2026-08-18", -57480, "Tanta", "PENDING"), ynab_tx_id="y-tanta")
    # old pending outside CSV window and pending still present in CSV: untouched
    ynab.db_upsert(conn, mk("2026-07-01", -99000, "Old Hold", "PENDING"), ynab_tx_id="y-old")
    ynab.db_upsert(conn, mk("2026-08-19", -11000, "Hotel Hold", "PENDING"), ynab_tx_id="y-hotel")
    conn.commit()
    csv_txs = [
        mk("2026-08-18", -51060, "Tanta", t="10:59:18"),
        mk("2026-08-18", -5230, "Propina Tanta", t="13:59:18"),
        mk("2026-08-19", -11000, "Hotel Hold", "PENDING"),
    ]
    out = run_import(conn, csv_txs)
    deletes = [p for m, p, _ in calls if m == "DELETE"]
    assert deletes == ["/budgets/b/transactions/y-tanta"], deletes
    row = conn.execute(
        "SELECT deleted, state FROM transactions WHERE import_id='YNAB:-57480:2026-08-18:1'"
    ).fetchone()
    assert (row[0], row[1]) == (1, "SUPERSEDED")
    for iid in ("YNAB:-99000:2026-07-01:1", "YNAB:-11000:2026-08-19:1"):
        assert conn.execute(
            "SELECT deleted FROM transactions WHERE import_id=?", (iid,)
        ).fetchone()[0] == 0
    assert "pending superseded by settled rows" in out
    msg = bot.RevolutYNABBot._format_import_summary(None, out)
    assert "🗑 Removed from YNAB:" in msg and "Tanta" in msg
    run_import(conn, csv_txs)
    assert calls == [], calls
    conn.close()
    print("ok: superseded pending removed and reported, guards intact, converges")


def test_change_reporting(tmpdir):
    conn = fresh_db(tmpdir, "changes.db")
    seed = mk("2026-08-09", -45000, "EV Charge", "PENDING")
    seed["import_id"] = "YNAB:X:fixed:1"
    ynab.db_upsert(conn, seed, ynab_tx_id="y-ev")
    ynab.db_upsert(conn, mk("2026-08-10", -9990, "Cafe", "PENDING"), ynab_tx_id="y-cafe")
    conn.commit()
    changed = mk("2026-08-09", -41200, "EV Charge")
    changed["import_id"] = "YNAB:X:fixed:1"
    out = run_import(conn, [changed, mk("2026-08-10", -9990, "Cafe")])
    assert "Δ 2026-08-09  EV Charge — amount -45.00 → -41.20" in out
    assert sum(1 for l in out.splitlines() if l.strip().startswith("Δ")) == 1
    msg = bot.RevolutYNABBot._format_import_summary(None, out)
    assert "⚠ Amount/date changed:" in msg and "EV Charge" in msg
    conn.close()
    print("ok: amount changes reported, plain clearing silent")


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        test_reverted(tmpdir)
        test_date_shift(tmpdir)
        test_timezone_shift(tmpdir)
        test_superseded_pending(tmpdir)
        test_change_reporting(tmpdir)
    print("ALL PASSED")


if __name__ == "__main__":
    main()
