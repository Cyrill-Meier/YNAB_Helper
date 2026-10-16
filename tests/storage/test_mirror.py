"""mirror.py: MirrorStore upserts/deltas, sync_meta, MirrorView and the web read helpers."""

from __future__ import annotations

import dataclasses
import sqlite3
from typing import Any

import pytest

from ynab_helper.errors import YnabError
from ynab_helper.storage import LedgerEntry, LedgerStore, MirrorStore, MirrorView, classify_account

from .helpers import ynab_account, ynab_sub, ynab_tx

BUDGET = "budget-1"
MAIN = "acct-main"


@pytest.fixture
def mirror(tx_conn: sqlite3.Connection) -> MirrorStore:
    return MirrorStore(tx_conn)


def _rows(conn: sqlite3.Connection, sql: str, *params: Any) -> list[tuple]:
    return [tuple(r) for r in conn.execute(sql, params)]


# ── classification ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("account_type", "on_budget", "expected"),
    [
        ("checking", True, "cash"),
        ("savings", True, "cash"),
        ("cash", True, "cash"),
        ("creditCard", True, "credit"),
        ("lineOfCredit", True, "credit"),
        ("mortgage", False, "credit"),
        ("autoLoan", False, "credit"),
        ("studentLoan", False, "credit"),
        ("personalLoan", False, "credit"),
        ("medicalDebt", False, "credit"),
        ("otherDebt", False, "credit"),
        ("someFutureDebt", False, "credit"),
        ("otherAsset", False, "tracking"),
        ("otherLiability", False, "tracking"),
        ("brandNewType", True, "cash"),
        ("brandNewType", False, "tracking"),
        (None, False, "tracking"),
    ],
)
def test_classify_account(account_type: str | None, on_budget: bool, expected: str) -> None:
    assert classify_account(account_type, on_budget) == expected


# ── accounts ──────────────────────────────────────────────────────────────────


def test_upsert_accounts_and_reads(mirror: MirrorStore) -> None:
    written = mirror.upsert_accounts(
        BUDGET,
        [
            ynab_account(MAIN, name="Revolut"),
            ynab_account("acct-cc", name="Visa", type="creditCard", balance=-5000),
            ynab_account("acct-closed", name="Old", closed=True),
            ynab_account("acct-del", name="Gone", deleted=True),
            ynab_account("acct-house", name="House", type="otherAsset", on_budget=False),
        ],
        server_knowledge=11,
    )
    assert written == 5
    assert mirror.get_accounts_knowledge(BUDGET) == 11
    assert [a.id for a in mirror.accounts(BUDGET)] == [MAIN, "acct-cc", "acct-house"]
    assert {a.id for a in mirror.accounts(BUDGET, include_closed=True)} == {
        MAIN,
        "acct-cc",
        "acct-house",
        "acct-closed",
    }
    main = mirror.account(MAIN)
    assert main is not None
    assert (main.budget_id, main.classification, main.on_budget) == (BUDGET, "cash", True)
    assert (main.balance, main.cleared_balance, main.uncleared_balance) == (100000, 90000, 10000)
    assert mirror.account("acct-del").deleted  # type: ignore[union-attr]
    assert mirror.account("acct-house").classification == "tracking"  # type: ignore[union-attr]
    assert mirror.accounts("other-budget") == []
    assert mirror.account("nope") is None


def test_account_update_keeps_first_seen_and_overwrites_balances(
    mirror: MirrorStore, tx_conn: sqlite3.Connection
) -> None:
    mirror.upsert_accounts(BUDGET, [ynab_account(MAIN)])
    first_seen = tx_conn.execute(
        "SELECT first_seen_at, created_at FROM accounts WHERE account_id = ?", (MAIN,)
    ).fetchone()
    tx_conn.execute("UPDATE accounts SET first_seen_at = 'early', created_at = 'early'")
    mirror.upsert_accounts(BUDGET, [ynab_account(MAIN, name="Renamed", balance=None)])
    row = tx_conn.execute(
        "SELECT name, balance, first_seen_at, created_at FROM accounts WHERE account_id = ?",
        (MAIN,),
    ).fetchone()
    assert tuple(row) == ("Renamed", None, "early", "early")
    assert first_seen  # was set on insert


def test_full_account_list_marks_missing_accounts_deleted(mirror: MirrorStore) -> None:
    mirror.upsert_accounts(BUDGET, [ynab_account(MAIN), ynab_account("acct-x")])
    mirror.upsert_accounts("budget-2", [ynab_account("acct-y")])
    mirror.upsert_accounts(BUDGET, [ynab_account(MAIN)], full=True)
    assert mirror.account("acct-x").deleted  # type: ignore[union-attr]
    assert not mirror.account("acct-y").deleted  # type: ignore[union-attr]  # other budget
    mirror.upsert_accounts(BUDGET, [ynab_account(MAIN)])  # a delta never deletes
    assert not mirror.account(MAIN).deleted  # type: ignore[union-attr]


def test_malformed_account_raises_and_writes_nothing(mirror: MirrorStore) -> None:
    with pytest.raises(YnabError):
        mirror.upsert_accounts(BUDGET, [ynab_account(MAIN), {"name": "no id"}])
    with pytest.raises(YnabError):
        mirror.upsert_accounts(BUDGET, [ynab_account(MAIN, balance=1.5)])
    with pytest.raises(ValueError):
        mirror.upsert_accounts("", [ynab_account(MAIN)])
    assert mirror.account(MAIN) is None


# ── transactions ──────────────────────────────────────────────────────────────


def test_upsert_transactions_stores_every_field(
    mirror: MirrorStore, tx_conn: sqlite3.Connection
) -> None:
    tx = ynab_tx(
        "t1",
        date="2026-08-03",
        amount=-12340,
        memo="lunch",
        cleared="reconciled",
        approved=False,
        flag_color="red",
        payee_id="p1",
        payee_name="Cafe",
        category_id="c1",
        category_name="Dining",
        transfer_account_id=None,
        matched_transaction_id="m1",
        import_id="YNAB:-12340:2026-08-03:1",
    )
    assert mirror.upsert_transactions(BUDGET, [tx], server_knowledge=7) == 1
    row = dict(tx_conn.execute("SELECT * FROM ynab_transactions WHERE id = 't1'").fetchone())
    assert row["budget_id"] == BUDGET and row["account_id"] == MAIN
    assert (row["date"], row["amount"], row["cleared"], row["approved"]) == (
        "2026-08-03",
        -12340,
        "reconciled",
        0,
    )
    assert (row["memo"], row["flag_color"], row["category_name"]) == ("lunch", "red", "Dining")
    assert row["matched_transaction_id"] == "m1" and row["deleted"] == 0 and row["synced_at"]
    assert mirror.get_server_knowledge(BUDGET) == 7
    got = mirror.get_transaction("t1")
    assert got is not None and got.cleared == "reconciled" and not got.approved


def test_F39_moved_transaction_stays_one_row(mirror: MirrorStore) -> None:
    """F39: a transaction moved between accounts must not become two live rows."""
    mirror.upsert_transactions(BUDGET, [ynab_tx("t1", account_id="acct-savings", amount=-500000)])
    mirror.upsert_transactions(BUDGET, [ynab_tx("t1", account_id="acct-checking", amount=-500000)])
    assert mirror.transactions_for_account(BUDGET, "acct-savings") == []
    assert [t.id for t in mirror.transactions_for_account(BUDGET, "acct-checking")] == ["t1"]
    rows = mirror.analytics_rows(BUDGET)
    assert len(rows) == 1 and rows[0]["amount"] == -500000


def test_F76_resync_updates_date(mirror: MirrorStore) -> None:
    """F76: a date changed in YNAB must reach the local copy."""
    mirror.upsert_transactions(BUDGET, [ynab_tx("t1", date="2026-08-31")])
    mirror.upsert_transactions(BUDGET, [ynab_tx("t1", date="2026-09-01", memo="moved")])
    tx = mirror.get_transaction("t1")
    assert tx is not None and tx.date == "2026-09-01" and tx.memo == "moved"


def test_delta_with_deleted_rows_keeps_them_flagged(mirror: MirrorStore) -> None:
    mirror.upsert_transactions(BUDGET, [ynab_tx("t1"), ynab_tx("t2")], server_knowledge=10)
    mirror.upsert_transactions(BUDGET, [ynab_tx("t1", deleted=True)], server_knowledge=12)
    deleted = mirror.get_transaction("t1")
    assert deleted is not None and deleted.deleted
    assert [t.id for t in mirror.transactions_for_account(BUDGET, MAIN)] == ["t2"]
    assert {t.id for t in mirror.transactions_for_account(BUDGET, MAIN, include_deleted=True)} == {
        "t1",
        "t2",
    }
    assert mirror.get_server_knowledge(BUDGET) == 12


def test_full_transaction_list_marks_missing_rows_deleted(mirror: MirrorStore) -> None:
    mirror.upsert_transactions(BUDGET, [ynab_tx("t1"), ynab_tx("t2")])
    mirror.upsert_transactions("budget-2", [ynab_tx("t9")])
    mirror.upsert_transactions(BUDGET, [ynab_tx("t2")], full=True)
    assert mirror.get_transaction("t1").deleted  # type: ignore[union-attr]
    assert not mirror.get_transaction("t2").deleted  # type: ignore[union-attr]
    assert not mirror.get_transaction("t9").deleted  # type: ignore[union-attr]


def test_subtransactions_are_replaced_per_parent(
    mirror: MirrorStore, tx_conn: sqlite3.Connection
) -> None:
    split = ynab_tx(
        "p1",
        amount=-10000,
        category_name="Split",
        subtransactions=[ynab_sub("s1", "p1", amount=-6000), ynab_sub("s2", "p1", amount=-4000)],
    )
    other = ynab_tx("p2", subtransactions=[ynab_sub("s9", "p2", amount=-10000)])
    mirror.upsert_transactions(BUDGET, [split, other])
    assert mirror.get_transaction("p1").has_subtransactions  # type: ignore[union-attr]

    # YNAB re-split p1: s1 changed, s2 removed (sent as deleted), s3 added.
    resplit = dict(split)
    resplit["subtransactions"] = [
        ynab_sub("s1", "p1", amount=-7000),
        ynab_sub("s2", "p1", amount=-4000, deleted=True),
        ynab_sub("s3", "p1", amount=-3000),
    ]
    mirror.upsert_transactions(BUDGET, [resplit])
    subs = _rows(
        tx_conn,
        "SELECT id, amount, deleted FROM ynab_subtransactions WHERE transaction_id = ? ORDER BY id",
        "p1",
    )
    assert subs == [("s1", -7000, 0), ("s2", -4000, 1), ("s3", -3000, 0)]
    assert _rows(tx_conn, "SELECT id FROM ynab_subtransactions WHERE transaction_id = 'p2'") == [
        ("s9",)
    ]

    # Split undone in YNAB: empty list → no live subtransactions left.
    unsplit = dict(split, subtransactions=[])
    mirror.upsert_transactions(BUDGET, [unsplit])
    assert not mirror.get_transaction("p1").has_subtransactions  # type: ignore[union-attr]
    assert _rows(tx_conn, "SELECT id FROM ynab_subtransactions WHERE transaction_id = 'p1'") == []

    # A payload without the key leaves stored subtransactions alone.
    mirror.upsert_transactions(BUDGET, [split])
    no_key = {k: v for k, v in split.items() if k != "subtransactions"}
    mirror.upsert_transactions(BUDGET, [no_key])
    assert mirror.get_transaction("p1").has_subtransactions  # type: ignore[union-attr]


def test_all_deleted_subtransactions_mean_not_split(mirror: MirrorStore) -> None:
    tx = ynab_tx("p1", subtransactions=[ynab_sub("s1", "p1", deleted=True)])
    mirror.upsert_transactions(BUDGET, [tx])
    assert not mirror.get_transaction("p1").has_subtransactions  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "bad",
    [
        {"id": None},
        {"account_id": ""},
        {"date": "03.08.2026"},
        {"amount": -1.5},
        {"amount": True},
        {"cleared": None},
        {"approved": "yes"},
        {"memo": 5},
        {"subtransactions": "nope"},
        {"subtransactions": [{"id": "s1"}]},
    ],
)
def test_malformed_transaction_raises_and_writes_nothing(
    mirror: MirrorStore, bad: dict[str, Any]
) -> None:
    with pytest.raises(YnabError):
        mirror.upsert_transactions(
            BUDGET, [ynab_tx("ok"), ynab_tx("bad", **bad)], server_knowledge=99
        )
    assert mirror.get_transaction("ok") is None
    assert mirror.get_server_knowledge(BUDGET) is None  # cursor never passes unsaved data


def test_accepts_model_objects_with_raw_dict(mirror: MirrorStore) -> None:
    @dataclasses.dataclass
    class Model:  # shape of ynab_helper.ynab.models objects
        id: str
        raw: dict[str, Any]

    mirror.upsert_transactions(BUDGET, [Model("t1", ynab_tx("t1", amount=-42))])
    mirror.upsert_accounts(BUDGET, [Model(MAIN, ynab_account(MAIN))])
    assert mirror.get_transaction("t1").amount == -42  # type: ignore[union-attr]
    assert mirror.account(MAIN) is not None
    with pytest.raises(YnabError):
        mirror.upsert_transactions(BUDGET, [object()])


def test_F01_mirror_writes_never_touch_the_ledger(
    mirror: MirrorStore, tx_conn: sqlite3.Connection
) -> None:
    """F01/F11: YNAB's reconciled state, memo and renamed payee live only in the mirror; the
    importer's CSV identity is untouched by a sync."""
    ledger = LedgerStore(tx_conn)
    entry = ledger.insert(
        LedgerEntry(
            account_id=MAIN,
            key="YNAB:-42000:2026-08-01:1",
            ynab_tx_id="t1",
            status="ACTIVE",
            csv_date="2026-08-01",
            csv_amount=-42000,
            csv_payee="AMZN Mktp CH*1X2Y3",
            csv_state="COMPLETED",
            pushed_cleared="cleared",
        )
    )
    mirror.upsert_transactions(
        BUDGET,
        [
            ynab_tx(
                "t1",
                amount=-42000,
                cleared="reconciled",
                memo="dinner w/ Anna",
                payee_name="Amazon",
                import_id="YNAB:-42000:2026-08-01:1",
            )
        ],
    )
    assert ledger.get(MAIN, entry.key) == entry
    view = mirror.view_for_account(BUDGET, MAIN)
    y = view.by_id("t1")
    assert y is not None and (y.cleared, y.memo, y.payee_name) == (
        "reconciled",
        "dinner w/ Anna",
        "Amazon",
    )


# ── sync_meta ─────────────────────────────────────────────────────────────────


def test_sync_meta_keys(mirror: MirrorStore, tx_conn: sqlite3.Connection) -> None:
    mirror.set_server_knowledge(BUDGET, 5)
    mirror.set_accounts_knowledge(BUDGET, 6)
    mirror.set_budget_currency(BUDGET, "CHF", checked_at="2026-10-16T00:00:00Z")
    mirror.set_last_sync_at(BUDGET, "2026-10-16T01:00:00Z")
    keys = dict(_rows(tx_conn, "SELECT key, value FROM sync_meta"))
    assert keys == {
        f"mirror_knowledge:{BUDGET}": "5",
        f"accounts_knowledge:{BUDGET}": "6",
        f"budget_currency:{BUDGET}": "CHF",
        f"budget_currency_checked_at:{BUDGET}": "2026-10-16T00:00:00Z",
        f"last_sync_at:{BUDGET}": "2026-10-16T01:00:00Z",
    }
    assert mirror.get_budget_currency(BUDGET) == "CHF"
    assert mirror.budget_currency_checked_at(BUDGET) == "2026-10-16T00:00:00Z"
    assert mirror.get_last_sync_at(BUDGET) == "2026-10-16T01:00:00Z"
    mirror.reset_knowledge(BUDGET)
    assert mirror.get_server_knowledge(BUDGET) is None
    assert mirror.get_accounts_knowledge(BUDGET) is None
    with pytest.raises(ValueError):
        mirror.set_server_knowledge(BUDGET, -1)
    with pytest.raises(ValueError):
        mirror.set_meta("x", 5)  # type: ignore[arg-type]
    mirror.set_meta(f"mirror_knowledge:{BUDGET}", "garbage")
    assert mirror.get_server_knowledge(BUDGET) is None  # unreadable → full sync


# ── MirrorView ────────────────────────────────────────────────────────────────


def test_view_for_account_contents(mirror: MirrorStore, tx_conn: sqlite3.Connection) -> None:
    mirror.upsert_transactions(
        BUDGET,
        [
            ynab_tx("t1", import_id="YNAB:-1:2026-08-01:1"),
            ynab_tx("t2", import_id="YNAB:-2:2026-08-01:1", deleted=True),
            ynab_tx("t3", import_id=None),
            ynab_tx("moved", account_id="acct-other", import_id="YNAB:-3:2026-08-01:1"),
            ynab_tx("unrelated", account_id="acct-other", import_id="YNAB:-4:2026-08-01:1"),
        ],
    )
    mirror.upsert_transactions("budget-2", [ynab_tx("foreign", import_id="YNAB:-5:2026-08-01:1")])
    LedgerStore(tx_conn).insert(
        LedgerEntry(
            account_id=MAIN,
            key="YNAB:-3:2026-08-01:1",
            ynab_tx_id="moved",
            status="ACTIVE",
            csv_date="2026-08-01",
            csv_amount=-3,
            csv_state="COMPLETED",
        )
    )
    view = mirror.view_for_account(BUDGET, MAIN)
    assert view.account_id == MAIN
    assert view.by_id("t2") is not None and view.by_id("t2").deleted  # type: ignore[union-attr]
    moved = view.by_id("moved")  # linked from the ledger although it left the account
    assert moved is not None and moved.account_id == "acct-other"
    assert view.by_id("unrelated") is None
    assert view.by_id("foreign") is None
    assert view.by_id(None) is None
    assert view.by_import_id("YNAB:-1:2026-08-01:1").id == "t1"  # type: ignore[union-attr]
    assert view.by_import_id("YNAB:-3:2026-08-01:1") is None  # other account
    assert view.used_import_ids == frozenset({"YNAB:-1:2026-08-01:1", "YNAB:-2:2026-08-01:1"})
    assert [t.id for t in view.transactions()] == ["t1", "t3"]
    assert len(view.transactions(include_deleted=True)) == 3
    assert MirrorView(MAIN, [])  # an empty view is still truthy (no __len__ footgun)


def test_view_prefers_live_transaction_for_shared_import_id() -> None:
    def tx(tx_id: str, deleted: bool, date: str = "2026-08-01") -> Any:
        from ynab_helper.storage import YnabTx

        return YnabTx(
            id=tx_id,
            account_id=MAIN,
            date=date,
            amount=-1,
            payee_name=None,
            memo=None,
            cleared="uncleared",
            approved=True,
            deleted=deleted,
            import_id="same",
            category_name=None,
            transfer_account_id=None,
            has_subtransactions=False,
        )

    view = MirrorView(MAIN, [tx("live", False), tx("dead", True, "2026-08-09")])
    assert view.by_import_id("same").id == "live"  # type: ignore[union-attr]
    only_dead = MirrorView(MAIN, [tx("d1", True), tx("d2", True, "2026-08-02")])
    assert only_dead.by_import_id("same").id == "d2"  # type: ignore[union-attr]
    assert only_dead.used_import_ids == {"same"}


def test_ynab_tx_is_frozen(mirror: MirrorStore) -> None:
    mirror.upsert_transactions(BUDGET, [ynab_tx("t1")])
    tx = mirror.get_transaction("t1")
    assert tx is not None
    with pytest.raises(dataclasses.FrozenInstanceError):
        tx.amount = 5  # type: ignore[misc]
    assert [f.name for f in dataclasses.fields(tx)] == [
        "id",
        "account_id",
        "date",
        "amount",
        "payee_name",
        "memo",
        "cleared",
        "approved",
        "deleted",
        "import_id",
        "category_name",
        "transfer_account_id",
        "has_subtransactions",
    ]


# ── reconcile helper ──────────────────────────────────────────────────────────


def test_cleared_sum_after(mirror: MirrorStore) -> None:
    mirror.upsert_transactions(
        BUDGET,
        [
            ynab_tx("before", date="2026-08-01", amount=-100, cleared="cleared"),
            ynab_tx("on", date="2026-08-02", amount=-200, cleared="cleared"),
            ynab_tx("after-c", date="2026-08-03", amount=-300, cleared="cleared"),
            ynab_tx("after-r", date="2026-08-04", amount=-400, cleared="reconciled"),
            ynab_tx("after-u", date="2026-08-04", amount=-500, cleared="uncleared"),
            ynab_tx("after-del", date="2026-08-04", amount=-600, cleared="cleared", deleted=True),
            ynab_tx("other-acct", date="2026-08-04", amount=-700, account_id="acct-x"),
        ],
    )
    assert mirror.cleared_sum_after(MAIN, "2026-08-02") == -700
    assert mirror.cleared_sum_after(MAIN, "2026-08-04") == 0
    assert mirror.cleared_sum_after("acct-none", "2026-01-01") == 0


# ── web read helpers ──────────────────────────────────────────────────────────


@pytest.fixture
def populated(mirror: MirrorStore) -> MirrorStore:
    mirror.upsert_accounts(
        BUDGET,
        [
            ynab_account(MAIN, name="Revolut"),
            ynab_account("acct-house", name="House", type="otherAsset", on_budget=False),
        ],
    )
    mirror.upsert_transactions(
        BUDGET,
        [
            ynab_tx(
                "t1",
                date="2026-08-01",
                amount=-1000,
                payee_name="Cafe 100%",
                category_name="Dining",
            ),
            ynab_tx(
                "t2", date="2026-08-02", amount=-2000, payee_name="Cafe_Bar", cleared="reconciled"
            ),
            ynab_tx("t3", date="2026-08-03", amount=5000, payee_name="Salary", category_name=None),
            ynab_tx("t4", date="2026-08-04", amount=-9000, payee_name="Gone", deleted=True),
            ynab_tx(
                "t5",
                date="2026-08-05",
                amount=-10000,
                payee_name="Supermarket",
                category_name="Split",
                subtransactions=[
                    ynab_sub("s1", "t5", amount=-6000, category_name="Groceries"),
                    ynab_sub("s2", "t5", amount=-4000, category_name="Household", memo="soap"),
                ],
            ),
            ynab_tx("t6", date="2026-08-06", amount=-300, account_id="acct-house", memo="m"),
        ],
    )
    return mirror


def test_search_transactions_filters_and_escapes(populated: MirrorStore) -> None:
    total, rows = populated.search_transactions(BUDGET)
    assert total == 5 and [r["id"] for r in rows] == ["t6", "t5", "t3", "t2", "t1"]
    assert rows[1]["account_name"] == "Revolut" and rows[1]["account_classification"] == "cash"

    # LIKE wildcards are literal: '%' and '_' must not match everything.
    assert [r["id"] for r in populated.search_transactions(BUDGET, text="100%")[1]] == ["t1"]
    assert [r["id"] for r in populated.search_transactions(BUDGET, text="_")[1]] == ["t2"]
    assert populated.search_transactions(BUDGET, text="%")[0] == 1
    assert [r["id"] for r in populated.search_transactions(BUDGET, cleared="reconciled")[1]] == [
        "t2"
    ]
    assert [r["id"] for r in populated.search_transactions(BUDGET, uncategorized=True)[1]] == ["t3"]
    assert [r["id"] for r in populated.search_transactions(BUDGET, category="Dining")[1]] == ["t1"]
    assert [
        r["id"] for r in populated.search_transactions(BUDGET, account_ids=["acct-house"])[1]
    ] == ["t6"]
    ranged = populated.search_transactions(BUDGET, date_from="2026-08-02", date_to="2026-08-03")
    assert [r["id"] for r in ranged[1]] == ["t3", "t2"]
    total, page = populated.search_transactions(BUDGET, sort="amount", limit=2, offset=1)
    assert total == 5 and [r["id"] for r in page] == ["t2", "t1"]
    _, by_bad_sort = populated.search_transactions(BUDGET, sort="amount; DROP TABLE x")
    assert by_bad_sort  # unknown sort column falls back to date
    with pytest.raises(ValueError):
        populated.search_transactions(BUDGET, cleared="everything")


def test_categories_expand_splits(populated: MirrorStore) -> None:
    cats = {c["name"]: (c["count"], c["spent"]) for c in populated.categories(BUDGET)}
    # The split parent ("Split") is replaced by its lines; the deleted t4 is ignored.
    assert cats == {
        "": (1, 0),  # the uncategorised salary inflow
        "Dining": (1, -1000),
        "Groceries": (3, -2000 - 300 - 6000),
        "Household": (1, -4000),
    }
    only_house = populated.categories(BUDGET, account_ids=["acct-house"])
    assert [(c["name"], c["spent"]) for c in only_house] == [("Groceries", -300)]


def test_analytics_rows_replace_split_parents(populated: MirrorStore) -> None:
    rows = populated.analytics_rows(BUDGET, date_from="2026-08-02")
    by_id = {r["id"]: r for r in rows}
    assert set(by_id) == {"t2", "t3", "s1", "s2", "t6"}
    assert by_id["s1"]["parent_id"] == "t5" and by_id["s1"]["payee_name"] == "Supermarket"
    assert by_id["s2"]["memo"] == "soap" and by_id["s2"]["category_name"] == "Household"
    assert by_id["s1"]["on_budget"] == 1 and by_id["t6"]["on_budget"] == 0
    assert by_id["t6"]["classification"] == "tracking"
    assert [r["date"] for r in rows] == sorted(r["date"] for r in rows)
    parents = populated.analytics_rows(BUDGET, expand_splits=False, date_to="2026-08-05")
    assert {r["id"] for r in parents} == {"t1", "t2", "t3", "t5"}


def test_account_activity(populated: MirrorStore) -> None:
    assert populated.account_activity(BUDGET) == {
        MAIN: (4, "2026-08-05"),
        "acct-house": (1, "2026-08-06"),
    }


def test_full_listing_with_no_rows_marks_nothing(mirror: MirrorStore) -> None:
    mirror.upsert_transactions(BUDGET, [ynab_tx("t1")])
    mirror.upsert_accounts(BUDGET, [ynab_account(MAIN)])
    mirror.upsert_transactions(BUDGET, [], full=True)
    mirror.upsert_accounts(BUDGET, [], full=True)
    assert not mirror.get_transaction("t1").deleted  # type: ignore[union-attr]
    assert not mirror.account(MAIN).deleted  # type: ignore[union-attr]


def test_full_listing_beyond_sqlite_parameter_limit(mirror: MirrorStore) -> None:
    many = [ynab_tx(f"t{i:05d}", subtransactions=[]) for i in range(33_000)]
    assert mirror.upsert_transactions(BUDGET, [*many, ynab_tx("stale")]) == 33_001
    mirror.upsert_transactions(BUDGET, many, full=True)
    assert mirror.get_transaction("stale").deleted  # type: ignore[union-attr]
    assert not mirror.get_transaction("t32999").deleted  # type: ignore[union-attr]


def test_F11_payee_renamed_in_ynab_keeps_csv_payee(
    mirror: MirrorStore, tx_conn: sqlite3.Connection
) -> None:
    """F11: YNAB's payee rename rules must not overwrite the Revolut Description the importer
    matches on; the renamed payee is visible only through the mirror."""
    ledger = LedgerStore(tx_conn)
    key = "YNAB:-2500:2026-08-09:1"
    ledger.insert(
        LedgerEntry(
            account_id=MAIN,
            key=key,
            ynab_tx_id="t-amzn",
            status="ACTIVE",
            csv_date="2026-08-09",
            csv_amount=-2500,
            csv_payee="AMZN Mktp CH*1X2Y3",
            csv_state="PENDING",
            started_at="2026-08-09 23:30:00",
        )
    )
    for _ in range(3):  # repeated web-UI syncs
        mirror.upsert_transactions(
            BUDGET, [ynab_tx("t-amzn", amount=-2500, payee_name="Amazon", import_id=key)]
        )
    entry = ledger.get(MAIN, key)
    assert entry is not None and entry.csv_payee == "AMZN Mktp CH*1X2Y3"
    assert entry.started_at == "2026-08-09 23:30:00" and entry.csv_date == "2026-08-09"
    assert mirror.get_transaction("t-amzn").payee_name == "Amazon"  # type: ignore[union-attr]
