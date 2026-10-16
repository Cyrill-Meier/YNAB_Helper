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

calls = []        # mutating requests only: (method, path, body)
gets = []         # GET paths
ynab_state = {}   # stateful fake YNAB: ynab_tx_id -> transaction dict


class YnabNotFound(RuntimeError):
    pass


def fake_ynab_request(method, path, token, body=None):
    if method == "GET":
        gets.append(path)
        if path.startswith("/budgets/b/accounts/a/transactions") or path.startswith(
            "/budgets/b/transactions"
        ):
            return {"data": {"transactions": [dict(t) for t in ynab_state.values()],
                             "server_knowledge": 1}}
        return {"data": {}}
    calls.append((method, path, body))
    if method == "POST":
        txs = body["transactions"]
        created, dups = [], []
        existing_iids = {t.get("import_id") for t in ynab_state.values()}
        for t in txs:
            if t["import_id"] in existing_iids:
                dups.append(t["import_id"])
                continue
            tid = f"created-{t['import_id']}"
            ynab_state[tid] = {**t, "id": tid, "deleted": False}
            created.append(ynab_state[tid])
        return {"data": {
            "transaction_ids": [t["id"] for t in created],
            "transactions": [dict(t) for t in created],
            "duplicate_import_ids": dups,
        }}
    tx_id = path.rsplit("/", 1)[1]
    cur = ynab_state.get(tx_id)
    if cur is None or cur.get("deleted"):
        raise RuntimeError(f"YNAB API 404: transaction {tx_id} not found")
    if method == "PATCH":
        cur.update(body["transaction"])
        return {"data": {"transaction": dict(cur)}}
    if method == "DELETE":
        cur["deleted"] = True
        return {"data": {"transaction": dict(cur)}}
    raise AssertionError(f"unexpected {method} {path}")


def seed(conn, tx, ynab_tx_id, **ynab_overrides):
    """Record tx locally (linked to ynab_tx_id) and as it would sit in YNAB."""
    ynab.db_upsert(conn, tx, ynab_tx_id=ynab_tx_id)
    conn.commit()
    ynab_state[ynab_tx_id] = {
        "id": ynab_tx_id, "account_id": "a", "date": tx["date"], "amount": tx["amount"],
        "payee_name": tx["payee_name"], "import_id": tx["import_id"],
        "cleared": tx["cleared"], "memo": "(pending)" if tx["_state"] == "PENDING" else None,
        "deleted": False, **ynab_overrides,
    }


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
    ynab_state.clear()
    return ynab.init_db(os.path.join(str(tmpdir), name))


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
    seed(conn, mk("2026-08-09", -5000, "Shop", "PENDING"), "y1")
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
    seed(conn, mk("2026-08-04", 2000000, "Top-up", t="02:32:04"), "y-dep")
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
    seed(conn, mk("2026-08-06", -2500, "Cafe Noon"), "y-noon")
    conn.commit()
    run_import(conn, [mk("2026-08-07", -2500, "Cafe Noon")])
    assert [c[0] for c in calls] == ["POST"]
    conn.close()
    print("ok: date-shifted row re-keyed via PATCH, guards intact")


def test_timezone_shift(tmpdir):
    """Export made in Las Vegas (UTC-7), re-exported from Zürich (UTC+2):
    every timestamp moves +9h, evening rows land on the next day."""
    conn = fresh_db(tmpdir, "tzshift.db")
    seed(conn, mk("2026-08-22", -20000, "Lunch", t="12:15:10"), "y-lunch")
    seed(conn, mk("2026-08-22", -30000, "Dinner", t="19:30:05"), "y-dinner")
    seed(conn, mk("2026-08-22", -180, "Toll", t="22:10:00"), "y-toll")
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
    seed(conn, mk("2026-08-25", -4500, "Cafe", t="23:10:00"), "y-cafe")
    conn.commit()
    run_import(conn, [mk("2026-08-26", -4500, "Cafe", t="02:27:00")])
    assert [c[0] for c in calls] == ["POST"], calls
    # Legacy rows without started_at still use the near-midnight heuristic
    seed(conn, mk("2026-08-27", -7000, "Bar", t="23:40:00"), "y-bar")
    seed(conn, mk("2026-08-27", -8000, "Shop", t="14:00:00"), "y-shop")
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
    seed(conn, mk("2026-08-18", -57480, "Tanta", "PENDING"), "y-tanta")
    # old pending outside CSV window and pending still present in CSV: untouched
    seed(conn, mk("2026-07-01", -99000, "Old Hold", "PENDING"), "y-old")
    seed(conn, mk("2026-08-19", -11000, "Hotel Hold", "PENDING"), "y-hotel")
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
    ev = mk("2026-08-09", -45000, "EV Charge", "PENDING")
    ev["import_id"] = "YNAB:X:fixed:1"
    seed(conn, ev, "y-ev")
    seed(conn, mk("2026-08-10", -9990, "Cafe", "PENDING"), "y-cafe")
    changed = mk("2026-08-09", -41200, "EV Charge")
    changed["import_id"] = "YNAB:X:fixed:1"
    out = run_import(conn, [changed, mk("2026-08-10", -9990, "Cafe")])
    assert "Δ 2026-08-09  EV Charge — amount -45.00 → -41.20" in out
    assert sum(1 for l in out.splitlines() if l.strip().startswith("Δ")) == 1
    msg = bot.RevolutYNABBot._format_import_summary(None, out)
    assert "⚠ Amount/date changed:" in msg and "EV Charge" in msg
    conn.close()
    print("ok: amount changes reported, plain clearing silent")


def test_reconciled_never_downgraded(tmpdir):
    """The web UI's sync stores 'reconciled' locally; a later CSV import must
    not PATCH those rows back to 'cleared' or wipe the memo the user wrote."""
    conn = fresh_db(tmpdir, "reconciled.db")
    tx = mk("2026-09-01", -42000, "Dinner")
    seed(conn, tx, "y-dinner", cleared="reconciled", memo="dinner w/ Anna")
    # what sync_budget_transactions leaves behind locally
    conn.execute("UPDATE transactions SET cleared='reconciled', memo='dinner w/ Anna'")
    conn.commit()
    run_import(conn, [mk("2026-09-01", -42000, "Dinner")])
    assert calls == [], calls
    assert ynab_state["y-dinner"]["cleared"] == "reconciled"
    assert ynab_state["y-dinner"]["memo"] == "dinner w/ Anna"
    # An older export that still shows the row as PENDING never downgrades it
    run_import(conn, [mk("2026-09-01", -42000, "Dinner", "PENDING")])
    assert calls == [], calls
    conn.close()
    print("ok: reconciled rows never downgraded, memos kept")


def test_clearing_keeps_user_memo(tmpdir):
    conn = fresh_db(tmpdir, "memo.db")
    seed(conn, mk("2026-09-02", -51060, "Tanta", "PENDING"), "y-tanta",
         memo="(pending) | split with Bob")
    seed(conn, mk("2026-09-02", -1200, "Kiosk", "PENDING"), "y-kiosk",
         memo="newspaper")
    run_import(conn, [mk("2026-09-02", -51060, "Tanta"), mk("2026-09-02", -1200, "Kiosk")])
    bodies = {c[1].rsplit("/", 1)[1]: c[2]["transaction"] for c in calls if c[0] == "PATCH"}
    assert bodies["y-tanta"] == {"cleared": "cleared", "memo": "split with Bob"}, bodies
    assert bodies["y-kiosk"] == {"cleared": "cleared"}, bodies   # memo untouched
    assert ynab_state["y-kiosk"]["memo"] == "newspaper"
    # A user who already cleared it by hand gets no PATCH at all
    seed(conn, mk("2026-09-03", -800, "Cafe", "PENDING"), "y-cafe", cleared="cleared",
         memo="coffee")
    run_import(conn, [mk("2026-09-03", -800, "Cafe")])
    assert calls == [], calls
    conn.close()
    print("ok: clearing a pending keeps the user's memo")


def test_rekey_then_sync_keeps_live_transaction(tmpdir):
    """Re-key (timezone shift) + the web UI's sync must not leave a ghost row
    that the superseded sweep then deletes from YNAB."""
    conn = fresh_db(tmpdir, "ghost.db")
    seed(conn, mk("2026-08-09", -880000, "Hotel Bellagio", "PENDING", t="23:30:00"), "y1")
    run_import(conn, [mk("2026-08-10", -880000, "Hotel Bellagio", "PENDING", t="08:30:00")])
    assert [c[0] for c in calls] == ["PATCH"], calls   # re-dated, no duplicate
    # YNAB keeps the original import_id; the web UI's syncs run (both of them)
    buf, old = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        ynab.sync_budget_transactions(conn, "t", "b", primary_account_id="a")
        ynab.sync_from_ynab(conn, "t", "b", "a")
    finally:
        sys.stdout = old
    live = conn.execute(
        "SELECT import_id FROM transactions WHERE ynab_tx_id='y1' AND deleted=0"
    ).fetchall()
    assert [r[0] for r in live] == ["YNAB:-880000:2026-08-10:1"], live
    # Even a DB that already contains a ghost row never deletes the live one
    ghost = mk("2026-08-09", -880000, "Hotel Bellagio", "PENDING", t="23:30:00")
    ynab.db_upsert(conn, ghost, ynab_tx_id="y1")
    conn.commit()
    run_import(conn, [mk("2026-08-10", -880000, "Hotel Bellagio", "PENDING", t="08:30:00")])
    assert not [c for c in calls if c[0] == "DELETE"], calls
    assert not ynab_state["y1"]["deleted"]
    conn.close()
    print("ok: re-key + sync never deletes the live transaction")


def test_deleted_in_ynab_does_not_wedge(tmpdir):
    conn = fresh_db(tmpdir, "wedge.db")
    seed(conn, mk("2026-09-05", -1500, "Coffee", "PENDING"), "y-gone")
    seed(conn, mk("2026-09-05", -9000, "Shop", "PENDING"), "y-shop")
    del ynab_state["y-gone"]   # the user deleted it in YNAB
    out = run_import(conn, [
        mk("2026-09-05", -1500, "Coffee"),             # now completed
        mk("2026-09-05", -9000, "Shop", "REVERTED"),   # must still be removed
    ])
    assert [c for c in calls if c[0] == "PATCH"] == [], calls
    assert [p for m, p, _ in calls if m == "DELETE"] == ["/budgets/b/transactions/y-shop"]
    assert "no longer in YNAB" in out
    conn.close()
    print("ok: a transaction deleted in YNAB no longer aborts the import")


def test_superseded_but_cleared_in_ynab_is_kept(tmpdir):
    conn = fresh_db(tmpdir, "keep.db")
    seed(conn, mk("2026-09-06", -57480, "Tanta", "PENDING"), "y-tanta", cleared="cleared")
    out = run_import(conn, [mk("2026-09-06", -51060, "Tanta", t="10:00:00")])
    assert not [c for c in calls if c[0] == "DELETE"], calls
    assert "cleared in YNAB: left alone" in out
    conn.close()
    print("ok: pendings the user cleared in YNAB are never deleted as superseded")


def test_cleanup_pending_never_unreconciles(tmpdir):
    ynab_state.clear()
    calls.clear()
    ynab_state.update({
        "r": {"id": "r", "account_id": "a", "date": "2026-09-01", "amount": -1000,
              "import_id": "YNAB:-1000:2026-09-01:1", "cleared": "reconciled",
              "memo": "(pending) | taxi", "deleted": False},
        "u": {"id": "u", "account_id": "a", "date": "2026-09-02", "amount": -2000,
              "import_id": "YNAB:-2000:2026-09-02:1", "cleared": "uncleared",
              "memo": "(pending)", "deleted": False},
    })
    buf, old = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        ynab.cleanup_pending_memos("t", "b", "a")
    finally:
        sys.stdout = old
    bodies = {c[1].rsplit("/", 1)[1]: c[2]["transaction"] for c in calls if c[0] == "PATCH"}
    assert bodies == {"r": {"memo": "taxi"}}, bodies   # never touches cleared / still-pending
    assert ynab_state["r"]["cleared"] == "reconciled"
    print("ok: /cleanup_pending never un-reconciles or strips real pendings")


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        test_reverted(tmpdir)
        test_date_shift(tmpdir)
        test_timezone_shift(tmpdir)
        test_superseded_pending(tmpdir)
        test_change_reporting(tmpdir)
        test_reconciled_never_downgraded(tmpdir)
        test_clearing_keeps_user_memo(tmpdir)
        test_rekey_then_sync_keeps_live_transaction(tmpdir)
        test_deleted_in_ynab_does_not_wedge(tmpdir)
        test_superseded_but_cleared_in_ynab_is_kept(tmpdir)
        test_cleanup_pending_never_unreconciles(tmpdir)
    print("ALL PASSED")


if __name__ == "__main__":
    main()
