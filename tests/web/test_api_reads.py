"""JSON reads from the mirror (DESIGN §9.2; F64, C06, F100, PENTEST M-1, F30/F65 wiring)."""

from __future__ import annotations

import threading
import time
from datetime import datetime
from pathlib import Path

import pytest

from tests.fakes.fake_ynab import FakeYnab
from ynab_helper import services
from ynab_helper.storage import connect
from ynab_helper.web import queries

from .conftest import (
    ACCT,
    BUDGET,
    OTHER,
    TestClient,
    ctx_for,
    fixture_bytes,
    wait_background,
)

TODAY_TS = datetime(2026, 10, 17, 12, 0).timestamp()


@pytest.fixture(autouse=True)
def _today(clock) -> None:
    clock.now = TODAY_TS


def _sync(data_dir: Path) -> None:
    result = services.sync(ctx_for(data_dir), force=True)
    assert result is not None and result.ok


@pytest.fixture
def seeded(fake: FakeYnab, data_dir: Path) -> dict[str, str]:
    ids = {
        "coffee_cleared": fake.add_transaction(
            BUDGET,
            ACCT,
            amount=-4_500,
            date="2026-10-01",
            payee_name="Coffee Bar",
            category_name="Dining Out",
            cleared="cleared",
        ),
        "coffee_pending": fake.add_transaction(
            BUDGET,
            ACCT,
            amount=-5_000,
            date="2026-10-02",
            payee_name="Coffee Bar",
            category_name="Dining Out",
            cleared="uncleared",
        ),
        "percent": fake.add_transaction(
            BUDGET,
            ACCT,
            amount=-9_900,
            date="2026-10-03",
            payee_name="100% Natural",
            category_name="Groceries",
            cleared="reconciled",
        ),
        "underscore": fake.add_transaction(
            BUDGET,
            ACCT,
            amount=-1_000,
            date="2026-10-04",
            payee_name="snake_case shop",
            memo="a_b",
            cleared="cleared",
        ),
        "split": fake.add_transaction(
            BUDGET,
            OTHER,
            amount=-60_000,
            date="2026-10-05",
            payee_name="Migros",
            subtransactions=[
                {"amount": -45_000, "category_name": "Groceries"},
                {"amount": -15_000, "category_name": None, "memo": "misc"},
            ],
        ),
        "transfer": fake.add_transaction(
            BUDGET,
            ACCT,
            amount=-200_000,
            date="2026-10-06",
            payee_name="Transfer : Visa",
            transfer_account_id=OTHER,
        ),
        "deleted": fake.add_transaction(
            BUDGET,
            ACCT,
            amount=-7_000,
            date="2026-10-07",
            payee_name="Coffee Bar",
            deleted=True,
        ),
        "income": fake.add_transaction(
            BUDGET,
            ACCT,
            amount=1_000_000,
            date="2026-09-25",
            payee_name="Employer",
            category_name="Inflow: Ready to Assign",
        ),
    }
    _sync(data_dir)
    return ids


def _ids(response) -> list[str]:
    assert response.status_code == 200, response.text
    return [item["id"] for item in response.json()["items"]]


# ── dashboard (F64, C06, F100) ──────────────────────────────────────────────


def test_F64_C06_dashboard_balances_and_currency_come_from_the_mirror(
    client: TestClient, csrf, fake: FakeYnab, data_dir
):
    fake.add_transaction(BUDGET, ACCT, amount=1_000_000, date="2026-10-01", cleared="cleared")
    fake.add_transaction(BUDGET, ACCT, amount=-150_000, date="2026-10-10", cleared="uncleared")
    fake.add_transaction(BUDGET, OTHER, amount=-99_000, date="2026-10-10", cleared="uncleared")
    _sync(data_dir)
    data = client.get("/api/dashboard").json()
    assert data["balances"] == {"working": 850_000, "cleared": 1_000_000, "uncleared": -150_000}
    assert data["currency"] == "CHF"  # v1 always said '?'
    assert data["account"]["id"] == ACCT and data["account"]["found"]
    accounts = {a["id"]: a for a in client.get("/api/accounts").json()["items"]}
    assert accounts[ACCT]["balance"] == data["balances"]["working"]  # one source (F64)


def test_F100_dashboard_counts_are_scoped_to_the_import_account(
    client: TestClient, csrf, fake: FakeYnab, data_dir
):
    fake.add_transaction(BUDGET, ACCT, amount=-1_000, date="2026-10-01", cleared="cleared")
    fake.add_transaction(BUDGET, ACCT, amount=-1_000, date="2026-10-02", cleared="reconciled")
    for day in ("2026-10-03", "2026-10-04", "2026-10-05"):
        fake.add_transaction(BUDGET, OTHER, amount=-1_000, date=day, cleared="uncleared")
    _sync(data_dir)
    data = client.get("/api/dashboard").json()
    assert data["transactions"] == {"cleared": 1, "reconciled": 1, "uncleared": 0, "total": 2}


def test_dashboard_reports_the_last_statement(client: TestClient, csrf, data_dir):
    result = services.import_csv(
        ctx_for(data_dir),
        fixture_bytes("export_basic.csv"),
        original_name="account-statement_basic.csv",
        source_kind="telegram",
    )
    assert result.ok
    data = client.get("/api/dashboard").json()
    statement = data["last_statement"]
    assert statement["name"] == "account-statement_basic.csv"
    assert statement["status"] == "imported" and statement["source"] == "telegram"
    assert statement["summary"]["created"] == len(result.created)
    assert statement["summary"]["ok"] is True
    assert data["ledger"]["total"] == sum(data["ledger"]["counts"].values())
    assert data["ledger"]["counts"]["ACTIVE"] > 0


def test_dashboard_before_the_first_sync(client: TestClient, csrf, app, monkeypatch):
    release = threading.Event()
    real_sync = services.sync

    def gated_sync(ctx, **kwargs):
        release.wait(10)
        return real_sync(ctx, **kwargs)

    monkeypatch.setattr(services, "sync", gated_sync)
    data = client.get("/api/dashboard").json()
    assert data["account"]["found"] is False
    assert data["balances"] == {"working": None, "cleared": None, "uncleared": None}
    assert data["syncing"] is True and data["last_sync_at"] is None
    release.set()
    wait_background(app)
    after = client.get("/api/dashboard").json()
    assert after["account"]["found"] is True and after["last_sync_at"]


def test_reads_never_wait_for_ynab(client: TestClient, csrf, app, monkeypatch):
    release = threading.Event()
    started = threading.Event()

    def slow_sync(ctx, *, force=False, **kwargs):
        started.set()
        release.wait(10)

    monkeypatch.setattr(services, "sync", slow_sync)
    t0 = time.monotonic()
    response = client.get("/api/transactions")
    elapsed = time.monotonic() - t0
    assert response.status_code == 200 and response.json()["syncing"] is True
    assert started.wait(5) and elapsed < 2.0
    # While it runs, further reads do not start another sync.
    assert client.get("/api/accounts").json()["syncing"] is True
    release.set()
    wait_background(app)
    assert app.state.web.syncer.started_count == 1


def test_background_sync_is_throttled(client: TestClient, csrf, app, monkeypatch):
    calls = []
    monkeypatch.setattr(services, "sync", lambda ctx, **kw: calls.append(kw))
    for _ in range(5):
        client.get("/api/me")  # /api/me does not sync at all
        client.get("/api/accounts")
        wait_background(app)
    assert calls == [{"force": False}]


# ── transactions (mirror; PENTEST M-1; LIKE escaping) ───────────────────────


def test_transactions_list_reads_the_mirror(client: TestClient, csrf, seeded):
    data = client.get("/api/transactions").json()
    ids = [item["id"] for item in data["items"]]
    assert seeded["deleted"] not in ids
    assert data["total"] == len(ids) == 8  # 7 live + the transfer's other side
    assert data["currency"] == "CHF"
    split = next(i for i in data["items"] if i["id"] == seeded["split"])
    assert [s["amount"] for s in split["subtransactions"]] == [-45_000, -15_000]
    first = data["items"][0]
    assert set(first) >= {
        "id",
        "date",
        "amount",
        "payee_name",
        "memo",
        "cleared",
        "category_name",
        "account_id",
        "account_name",
        "account_classification",
        "subtransactions",
    }
    assert [i["date"] for i in data["items"]] == sorted(
        (i["date"] for i in data["items"]), reverse=True
    )


def test_like_wildcards_in_the_search_are_literal(client: TestClient, csrf, seeded):
    assert _ids(client.get("/api/transactions", params={"q": "%"})) == [seeded["percent"]]
    assert _ids(client.get("/api/transactions", params={"q": "_"})) == [seeded["underscore"]]
    assert _ids(client.get("/api/transactions", params={"q": "100%"})) == [seeded["percent"]]
    assert _ids(client.get("/api/transactions", params={"q": "\\"})) == []


def test_M1_text_and_state_filters_both_apply(client: TestClient, csrf, seeded):
    response = client.get("/api/transactions", params={"q": "coffee", "state": "uncleared"})
    assert _ids(response) == [seeded["coffee_pending"]]
    response = client.get("/api/transactions", params={"q": "coffee", "state": "cleared"})
    assert _ids(response) == [seeded["coffee_cleared"]]


@pytest.mark.parametrize("category", ["Groceries", "__none__"])
def test_M1_every_where_fragment_is_parenthesised(category):
    filters = queries.TransactionFilters(
        text="a",
        state="cleared",
        category=category,
        account_ids=("x", "y"),
        date_from="2026-01-01",
        date_to="2026-12-31",
    )
    fragments, params = queries.where_fragments("b1", filters)
    assert any(" OR " in fragment for fragment in fragments)
    where, same_params = queries.build_where("b1", filters)
    assert where == " AND ".join(f"({fragment})" for fragment in fragments)
    assert same_params == params and params[0] == "b1"
    assert where.count("?") == len(params)


def test_M1_category_and_other_filters_both_apply(client: TestClient, csrf, seeded):
    response = client.get(
        "/api/transactions", params={"category": "Dining Out", "state": "uncleared"}
    )
    assert _ids(response) == [seeded["coffee_pending"]]
    response = client.get("/api/transactions", params={"category": "__none__", "accounts": OTHER})
    assert _ids(response) == [seeded["split"]]
    response = client.get("/api/transactions", params={"q": "migros", "category": "Groceries"})
    assert _ids(response) == [seeded["split"]]


def test_state_buckets_are_exact(client: TestClient, csrf, seeded):
    reconciled = _ids(client.get("/api/transactions", params={"state": "reconciled"}))
    assert reconciled == [seeded["percent"]]
    cleared = _ids(client.get("/api/transactions", params={"state": "cleared"}))
    assert seeded["percent"] not in cleared and seeded["coffee_cleared"] in cleared


def test_category_filter_includes_split_lines(client: TestClient, csrf, seeded):
    ids = _ids(client.get("/api/transactions", params={"category": "Groceries"}))
    assert sorted(ids) == sorted([seeded["percent"], seeded["split"]])


def test_uncategorized_filter_excludes_transfers(client: TestClient, csrf, seeded):
    ids = _ids(client.get("/api/transactions", params={"category": "__none__"}))
    assert seeded["underscore"] in ids
    assert seeded["split"] in ids  # one of its lines has no category
    assert seeded["transfer"] not in ids


def test_account_date_paging_and_sort_filters(client: TestClient, csrf, seeded):
    ids = _ids(client.get("/api/transactions", params={"accounts": OTHER}))
    assert seeded["split"] in ids and seeded["coffee_cleared"] not in ids
    ids = _ids(
        client.get("/api/transactions", params={"date_from": "2026-10-02", "date_to": "2026-10-03"})
    )
    assert sorted(ids) == sorted([seeded["coffee_pending"], seeded["percent"]])
    page = client.get("/api/transactions", params={"page": 2, "page_size": 3, "sort": "amount"})
    data = page.json()
    assert (data["page"], data["page_size"], data["total"]) == (2, 3, 8)
    amounts = [i["amount"] for i in data["items"]]
    assert amounts == sorted(amounts) and len(amounts) == 3
    huge = client.get("/api/transactions", params={"page_size": 10_000}).json()
    assert huge["page_size"] == 200


def test_transactions_of_another_budget_are_not_shown(client: TestClient, csrf, seeded, data_dir):
    conn = connect(ctx_for(data_dir).db_path)
    try:
        conn.execute(
            "INSERT INTO ynab_transactions (id, budget_id, account_id, date, amount, cleared, "
            "approved, deleted, synced_at, payee_name) VALUES "
            "('foreign', 'b2', 'acct-x', '2026-10-01', -1, 'cleared', 1, 0, 'x', 'Coffee Bar')"
        )
    finally:
        conn.close()
    assert "foreign" not in _ids(client.get("/api/transactions", params={"q": "coffee"}))


@pytest.mark.parametrize(
    "params",
    [
        {"state": "pending"},
        {"q": "x" * 201},
        {"date_from": "2026-13-01"},
        {"date_to": "yesterday"},
        {"accounts": "a" * 65},
        {"accounts": ",".join(f"a{i}" for i in range(101))},
        {"page_size": "lots"},
    ],
)
def test_invalid_transaction_filters_are_a_400(client: TestClient, csrf, params):
    response = client.get("/api/transactions", params=params)
    assert response.status_code == 400
    assert response.json()["error"] == "bad_request"


# ── accounts, categories, spending, subscriptions ───────────────────────────


def test_accounts_list(client: TestClient, csrf, seeded, fake: FakeYnab, data_dir):
    fake.add_account(BUDGET, "acct-closed", "Old", closed=True)
    _sync(data_dir)
    data = client.get("/api/accounts").json()
    items = {a["id"]: a for a in data["items"]}
    assert set(items) == {ACCT, OTHER, "acct-closed"}
    assert items[ACCT]["is_primary"] and not items[OTHER]["is_primary"]
    assert items[OTHER]["classification"] == "credit"
    assert items["acct-closed"]["closed"] is True
    assert items[ACCT]["tx_count"] == 6 and items[ACCT]["last_activity"] == "2026-10-06"
    assert data["primary_account_id"] == ACCT


def test_categories_endpoint(client: TestClient, csrf, seeded):
    data = client.get("/api/categories").json()
    names = {c["name"]: c for c in data["items"]}
    assert names["Groceries"]["count"] == 2
    assert names["Groceries"]["spent"] == 9_900 + 45_000
    assert data["uncategorized"] == 2  # snake_case shop + the split's misc line
    only_card = client.get("/api/categories", params={"accounts": OTHER}).json()
    assert {c["name"] for c in only_card["items"]} == {"Groceries"}


def test_spending_endpoint_applies_the_rules(client: TestClient, csrf, seeded):
    data = client.get("/api/spending", params={"months": 1}).json()
    assert data["months"] == ["2026-09"]
    assert data["month_to_date"]["month"] == "2026-10"
    by_cat = {c["category"]: c for c in data["categories"]}
    # October so far: coffee x2, 100% Natural, snake_case shop and the split's lines.
    assert by_cat["Dining Out"]["month_to_date"] == 9_500
    assert by_cat["Groceries"]["month_to_date"] == 9_900 + 45_000
    assert by_cat[None]["month_to_date"] == 1_000 + 15_000
    assert "Inflow: Ready to Assign" not in by_cat  # income is not spending
    assert data["total"] == 0  # September had only the salary
    assert data["currency"] == "CHF"


def test_spending_months_parameter(client: TestClient, csrf, seeded):
    assert len(client.get("/api/spending", params={"months": 12}).json()["months"]) == 12
    assert client.get("/api/spending", params={"months": "x"}).status_code == 400


def test_subscriptions_endpoint(client: TestClient, csrf, fake: FakeYnab, data_dir):
    for month in range(3, 10):
        payee = "NETFLIX.COM" if month % 2 else "Netflix.com"
        fake.add_transaction(
            BUDGET,
            OTHER,
            amount=-17_900 if month > 6 else -15_900,
            date=f"2026-{month:02d}-12",
            payee_name=payee,
            category_name="Subscriptions",
        )
    # F98: the newest yearly charge is two months old (a 13-month window alone misses it).
    fake.add_transaction(BUDGET, ACCT, amount=-40_000, date="2025-08-04", payee_name="Vignette")
    fake.add_transaction(BUDGET, ACCT, amount=-40_000, date="2026-08-04", payee_name="Vignette")
    _sync(data_dir)
    data = client.get("/api/subscriptions").json()
    assert data["lookback_months"] == 13 and data["yearly_detectable"]
    assert (data["since"], data["history_since"]) == ("2025-09-01", "2024-08-01")
    found = {(s["payee_name"].lower(), s["cadence"]) for s in data["items"]}
    assert found == {("netflix.com", "monthly"), ("vignette", "yearly")}
    netflix = next(s for s in data["items"] if s["cadence"] == "monthly")
    assert netflix["first"] < netflix["last"] and netflix["amount_latest"] == 17_900
    vignette = next(s for s in data["items"] if s["cadence"] == "yearly")
    assert [h["date"] for h in vignette["history"]] == ["2025-08-04", "2026-08-04"]
    # A short look-back lists what was charged inside it: the yearly charge of August,
    # but not Netflix (only two charges since August, three needed).
    short = client.get("/api/subscriptions", params={"lookback_months": 2}).json()
    assert {s["cadence"] for s in short["items"]} == {"yearly"}
    assert short["yearly_detectable"] is False


def test_uncategorized_count_matches_the_uncategorized_list(
    client: TestClient, csrf, fake: FakeYnab, data_dir
):
    """/api/categories counts what /api/transactions?category= lists: transactions (a
    split once), the app's adjustments included, transfers and tracking accounts not."""
    fake.add_account(BUDGET, "acct-crypto", "Crypto", type="otherAsset", on_budget=False)
    fake.add_transaction(BUDGET, ACCT, amount=-5_000, date="2026-10-01", payee_name="Shop")
    fake.add_transaction(
        BUDGET,
        ACCT,
        amount=-2_150,
        date="2026-10-02",
        payee_name="CSV Reconciliation",
        import_id="RECON:-2150:2026-10-02:1",
    )
    fake.add_transaction(
        BUDGET,
        OTHER,
        amount=-60_000,
        date="2026-10-05",
        payee_name="Migros",
        subtransactions=[
            {"amount": -45_000, "category_name": None},
            {"amount": -15_000, "category_name": None, "memo": "misc"},
        ],
    )
    fake.add_transaction(
        BUDGET,
        OTHER,
        amount=-30_000,
        date="2026-10-06",
        payee_name="Coop",
        subtransactions=[
            {"amount": -20_000, "category_name": "Groceries"},
            {"amount": -5_000, "category_name": "Groceries", "memo": "more"},
            {"amount": -5_000, "category_name": "Household"},
        ],
    )
    fake.add_transaction(BUDGET, ACCT, amount=-9_000, date="2026-10-07", category_name="Groceries")
    fake.add_transaction(
        BUDGET, ACCT, amount=-1_000, date="2026-10-20", payee_name="Future", category_name=None
    )
    fake.add_transaction(
        BUDGET, ACCT, amount=-200_000, date="2026-10-08", transfer_account_id=OTHER
    )
    fake.add_transaction(BUDGET, "acct-crypto", amount=-3_000, date="2026-10-09")
    _sync(data_dir)

    def listed(category: str, **params: str) -> int:
        response = client.get("/api/transactions", params={"category": category, **params})
        return response.json()["total"]

    for params in ({}, {"accounts": ACCT}, {"accounts": OTHER}):
        cats = client.get("/api/categories", params=params).json()
        assert cats["uncategorized"] == listed("__none__", **params), params
        for item in cats["items"]:
            assert item["count"] == listed(item["name"], **params), (params, item)
    cats = client.get("/api/categories").json()
    # Shop, the RECON adjustment, the uncategorized split (once) and the future entry.
    assert cats["uncategorized"] == 4
    counts = {c["name"]: c["count"] for c in cats["items"]}
    assert counts == {"Groceries": 2, "Household": 1}


def test_me_has_no_secrets(client: TestClient, csrf):
    data = client.get("/api/me").json()
    assert data["telegram_id"] == 42 and data["first_name"] == "Ann"
    text = str(data)
    assert "test-token" not in text and "ynab_token" not in text


def test_F97_forced_sync_works_before_any_import(client: TestClient, csrf, fake, data_dir):
    assert not ctx_for(data_dir).db_path.exists()  # nothing was ever imported
    fake.add_transaction(BUDGET, ACCT, amount=-1_000, date="2026-10-01")
    response = client.post("/api/sync", headers={"X-CSRF-Token": csrf})
    assert response.json()["synced"] is True
    assert client.get("/api/transactions").json()["total"] == 1
