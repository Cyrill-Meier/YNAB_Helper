"""The fake YNAB must be trustworthy: every later test builds on these semantics."""

from __future__ import annotations

import json
import threading
import urllib.error
from datetime import date
from typing import Any

import pytest

from tests.fakes.fake_ynab import FAKE_TOKEN, Call, FakeYnab
from tests.ynab.conftest import ACCOUNT, BUDGET, OTHER_ACCOUNT, new_tx
from ynab_helper.errors import (
    YnabError,
    YnabNotFound,
    YnabRateLimited,
    YnabServerError,
    YnabValidationError,
)
from ynab_helper.ynab.client import YnabClient

IID = "YNAB:-1000:2026-10-01:1"


def api(
    fake: FakeYnab,
    method: str,
    path: str,
    body: Any = None,
    *,
    token: str | None = FAKE_TOKEN,
) -> tuple[int, dict[str, str], Any]:
    """Call the fake's transport directly (no client semantics in between)."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    raw = None if body is None else json.dumps(body).encode()
    status, resp_headers, payload = fake.transport(method, fake.base_url + path, headers, raw, 5.0)
    return status, resp_headers, json.loads(payload)


def tx_path(tx_id: str = "") -> str:
    return f"/budgets/{BUDGET}/transactions" + (f"/{tx_id}" if tx_id else "")


# ── responses & routing ─────────────────────────────────────────────────────


def test_error_bodies_are_ynab_shaped(fake: FakeYnab) -> None:
    status, _, body = api(fake, "GET", "/budgets/nope/accounts")
    assert status == 404
    assert body == {
        "error": {"id": "404.2", "name": "resource_not_found", "detail": "Resource not found"}
    }
    status, _, body = api(fake, "GET", "/nothing/here")
    assert (status, body["error"]["id"]) == (404, "404.1")
    status, _, body = api(fake, "POST", "/budgets")
    assert (status, body["error"]["name"]) == (405, "method_not_allowed")
    status, _, body = api(fake, "POST", tx_path(), {"transaction": {"account_id": ACCOUNT}})
    assert status == 400
    assert set(body["error"]) == {"id", "name", "detail"}


def test_auth_required(fake: FakeYnab) -> None:
    assert api(fake, "GET", "/user", token="wrong")[0] == 401
    assert api(fake, "GET", "/user", token=None)[0] == 401
    assert api(fake, "GET", "/user")[0] == 200


def test_token_none_accepts_any_token() -> None:
    fake = FakeYnab(token=None)
    assert api(fake, "GET", "/user", token="whatever")[0] == 200


def test_foreign_host_not_served(fake: FakeYnab) -> None:
    status, _, _ = fake.transport(
        "GET", "https://evil.example/v1/user", {"Authorization": f"Bearer {FAKE_TOKEN}"}, None, 1
    )
    assert status == 404


def test_invalid_json_body_is_400(fake: FakeYnab) -> None:
    status, _, payload = fake.transport(
        "POST", fake.base_url + tx_path(), {"Authorization": f"Bearer {FAKE_TOKEN}"}, b"{nope", 1
    )
    assert status == 400
    assert b"not valid JSON" in payload


def test_budgets_and_settings(fake: FakeYnab) -> None:
    fake.add_budget("usd", "USD", name="US")
    status, _, body = api(fake, "GET", "/budgets?include_accounts=true")
    assert status == 200
    budgets = {b["id"]: b for b in body["data"]["budgets"]}
    assert budgets[BUDGET]["currency_format"]["iso_code"] == "CHF"
    assert {a["id"] for a in budgets[BUDGET]["accounts"]} == {ACCOUNT, OTHER_ACCOUNT}
    assert budgets["usd"]["accounts"] == []
    status, _, body = api(fake, "GET", "/budgets/usd/settings")
    assert body["data"]["settings"]["currency_format"]["currency_symbol"] == "$"
    # last-used resolves to the most recently added budget
    assert api(fake, "GET", "/budgets/last-used/settings")[2] == body


def test_get_single_transaction_includes_deleted(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", deleted=True)
    status, _, body = api(fake, "GET", tx_path(tx_id))
    assert status == 200
    assert body["data"]["transaction"]["deleted"] is True
    assert api(fake, "GET", tx_path("missing"))[0] == 404


def test_rendered_transaction_has_full_api_shape(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", import_id=IID)
    expected_keys = {
        "id", "date", "amount", "memo", "cleared", "approved", "flag_color", "flag_name",
        "account_id", "account_name", "payee_id", "payee_name", "category_id", "category_name",
        "transfer_account_id", "transfer_transaction_id", "matched_transaction_id", "import_id",
        "import_payee_name", "import_payee_name_original", "debt_transaction_type", "deleted",
        "subtransactions",
    }  # fmt: skip
    assert set(fake.tx(tx_id)) == expected_keys
    assert fake.tx(tx_id)["account_name"] == "Revolut"
    assert len(tx_id) == 36


# ── create & import_id dedupe ───────────────────────────────────────────────


def test_single_create_response_shape(fake: FakeYnab) -> None:
    status, _, body = api(fake, "POST", tx_path(), {"transaction": new_tx(import_id=IID)})
    assert status == 201
    data = body["data"]
    assert data["transaction"]["import_id"] == IID
    assert data["transaction_ids"] == [data["transaction"]["id"]]
    assert data["duplicate_import_ids"] == []
    assert data["server_knowledge"] == fake.server_knowledge(BUDGET)
    assert "transactions" not in data


def test_single_create_duplicate_is_201_without_transaction(fake: FakeYnab) -> None:
    api(fake, "POST", tx_path(), {"transaction": new_tx(import_id=IID)})
    status, _, body = api(fake, "POST", tx_path(), {"transaction": new_tx(import_id=IID)})
    assert status == 201
    assert body["data"]["duplicate_import_ids"] == [IID]
    assert body["data"]["transaction_ids"] == []
    assert "transaction" not in body["data"]


def test_single_create_duplicate_conflict_mode(fake: FakeYnab) -> None:
    fake.single_duplicate_conflict = True
    api(fake, "POST", tx_path(), {"transaction": new_tx(import_id=IID)})
    status, _, body = api(fake, "POST", tx_path(), {"transaction": new_tx(import_id=IID)})
    assert (status, body["error"]["id"]) == (409, "409")


def test_bulk_create_defaults_and_payees(fake: FakeYnab) -> None:
    payload = {"account_id": ACCOUNT, "date": "2026-10-01", "amount": -5, "payee_name": "Migros"}
    status, _, body = api(fake, "POST", tx_path(), {"transactions": [payload, dict(payload)]})
    assert status == 201
    created = body["data"]["transactions"]
    assert len(created) == 2
    assert created[0]["cleared"] == "uncleared"  # YNAB defaults
    assert created[0]["approved"] is False
    assert created[0]["import_id"] is None
    assert created[0]["payee_id"] == created[1]["payee_id"]  # payee reused by name


def test_dedupe_is_per_account(fake: FakeYnab) -> None:
    client = fake.client()
    first = client.create_transactions(BUDGET, [new_tx(import_id=IID)])
    other = client.create_transactions(BUDGET, [new_tx(import_id=IID, account_id=OTHER_ACCOUNT)])
    same = client.create_transactions(BUDGET, [new_tx(import_id=IID)])
    assert len(first.created) == 1
    assert len(other.created) == 1
    assert same.created == [] and same.duplicate_import_ids == [IID]


def test_dedupe_within_one_batch(fake: FakeYnab) -> None:
    result = fake.client().create_transactions(BUDGET, [new_tx(import_id=IID)] * 3)
    assert len(result.created) == 1
    assert result.duplicate_import_ids == [IID, IID]


def test_dedupe_includes_deleted_by_default(fake: FakeYnab) -> None:
    client = fake.client()
    created = client.create_transactions(BUDGET, [new_tx(import_id=IID)]).created[0]
    client.delete_transaction(BUDGET, created.id)
    again = client.create_transactions(BUDGET, [new_tx(import_id=IID)])
    assert again.created == []
    assert again.duplicate_import_ids == [IID]


def test_dedupe_excluding_deleted_when_configured(fake: FakeYnab) -> None:
    fake.dedupe_includes_deleted = False
    client = fake.client()
    created = client.create_transactions(BUDGET, [new_tx(import_id=IID)]).created[0]
    client.delete_transaction(BUDGET, created.id)
    again = client.create_transactions(BUDGET, [new_tx(import_id=IID)])
    assert len(again.created) == 1
    assert again.created[0].id != created.id
    assert fake.find_by_import_id(ACCOUNT, IID)["id"] == again.created[0].id  # live one preferred


def test_transactions_without_import_id_are_never_deduped(fake: FakeYnab) -> None:
    result = fake.client().create_transactions(BUDGET, [new_tx(), new_tx()])
    assert len(result.created) == 2


def test_duplicate_does_not_bump_knowledge(fake: FakeYnab) -> None:
    client = fake.client()
    client.create_transactions(BUDGET, [new_tx(import_id=IID)])
    before = fake.server_knowledge(BUDGET)
    client.create_transactions(BUDGET, [new_tx(import_id=IID)])
    assert fake.server_knowledge(BUDGET) == before


@pytest.mark.parametrize(
    ("bad", "detail"),
    [
        ({"import_id": "Y" * 37}, "import_id is too long"),
        ({"amount": 1.5}, "amount must be an integer"),
        ({"amount": True}, "amount must be an integer"),
        ({"date": "2026-02-30"}, "date must be a valid"),
        ({"date": "01.10.2026"}, "date must be a valid"),
        ({"cleared": "pending"}, "cleared must be one of"),
        ({"approved": "yes"}, "approved must be a boolean"),
        ({"flag_color": "pink"}, "flag_color is invalid"),
        ({"memo": "m" * 501}, "memo is too long"),
        ({"payee_name": "p" * 201}, "payee_name is too long"),
        ({"account_id": "nope"}, "account_id does not exist"),
        ({"category_id": "nope"}, "category_id does not exist"),
        ({"payee_id": "nope"}, "payee_id does not exist"),
        ({"clered": "cleared"}, "unknown field"),
    ],
)
def test_create_validation_is_atomic(fake: FakeYnab, bad: dict[str, Any], detail: str) -> None:
    good = new_tx(import_id="YNAB:1:2026-10-01:1")
    broken = new_tx(import_id=IID)
    broken.update(bad)
    before = fake.server_knowledge(BUDGET)
    status, _, body = api(fake, "POST", tx_path(), {"transactions": [good, broken]})
    assert status == 400
    assert detail in body["error"]["detail"]
    assert fake.transactions(BUDGET, include_deleted=True) == []
    assert fake.server_knowledge(BUDGET) == before


def test_create_requires_date_and_amount(fake: FakeYnab) -> None:
    status, _, _ = api(fake, "POST", tx_path(), {"transaction": {"account_id": ACCOUNT}})
    assert status == 400


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"transaction": new_tx(), "transactions": [new_tx()]},
        {"transactions": []},
        {"transactions": "x"},
        {"transactions": ["x"]},
        [new_tx()],
    ],
)
def test_create_body_shape_validated(fake: FakeYnab, body: Any) -> None:
    assert api(fake, "POST", tx_path(), body)[0] == 400


def test_future_dates_rejected_only_when_today_set(fake: FakeYnab) -> None:
    assert api(fake, "POST", tx_path(), {"transaction": new_tx(date="2099-01-01")})[0] == 201
    fake.today = date(2026, 10, 16)
    status, _, body = api(fake, "POST", tx_path(), {"transaction": new_tx(date="2026-10-17")})
    assert status == 400
    assert "future" in body["error"]["detail"]
    assert api(fake, "POST", tx_path(), {"transaction": new_tx(date="2026-10-16")})[0] == 201


def test_today_accepts_iso_string() -> None:
    fake = FakeYnab(today="2026-10-16")
    assert str(fake.today) == "2026-10-16"


def test_categories(fake: FakeYnab) -> None:
    fake.add_category(BUDGET, "cat-food", "Groceries")
    created = fake.client().create_transactions(BUDGET, [new_tx(category_id="cat-food")])
    assert created.created[0].category_name == "Groceries"
    inflow = fake.inflow_category_id(BUDGET)
    created = fake.client().create_transactions(BUDGET, [new_tx(amount=5, category_id=inflow)])
    assert created.created[0].category_name == "Inflow: Ready to Assign"


# ── updates & import_id immutability ────────────────────────────────────────


@pytest.mark.parametrize("method", ["PUT", "PATCH"])
def test_single_update_partial_semantics(fake: FakeYnab, method: str) -> None:
    tx_id = fake.add_transaction(
        BUDGET, ACCOUNT, amount=-1, date="2026-10-01", memo="keep", cleared="uncleared"
    )
    status, _, body = api(fake, method, tx_path(tx_id), {"transaction": {"cleared": "cleared"}})
    assert status == 200
    assert body["data"]["transaction"]["cleared"] == "cleared"
    assert body["data"]["transaction"]["memo"] == "keep"  # untouched fields stay
    assert body["data"]["server_knowledge"] == fake.server_knowledge(BUDGET)


@pytest.mark.parametrize("method", ["PUT", "PATCH"])
def test_import_id_immutable_on_single_update(fake: FakeYnab, method: str) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", import_id=IID)
    status, _, body = api(
        fake, method, tx_path(tx_id), {"transaction": {"import_id": "OTHER:1", "memo": "m"}}
    )
    assert status == 200
    assert body["data"]["transaction"]["import_id"] == IID
    assert body["data"]["transaction"]["memo"] == "m"
    assert fake.import_id_change_attempts == [(tx_id, IID, "OTHER:1")]


def test_import_id_immutable_on_bulk_update_and_user_tx_stays_user_entered(fake: FakeYnab) -> None:
    manual = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01")
    fake.client().update_transactions(BUDGET, [{"id": manual, "import_id": IID}])
    assert fake.tx(manual)["import_id"] is None
    assert len(fake.import_id_change_attempts) == 1


def test_bulk_update_by_id_and_by_import_id(fake: FakeYnab) -> None:
    a = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", cleared="uncleared")
    b = fake.add_transaction(
        BUDGET, ACCOUNT, amount=-2, date="2026-10-01", import_id=IID, cleared="uncleared"
    )
    status, _, body = api(
        fake,
        "PATCH",
        tx_path(),
        {"transactions": [{"id": a, "memo": "by id"}, {"import_id": IID, "cleared": "cleared"}]},
    )
    assert status == 209
    assert body["data"]["transaction_ids"] == [a, b]
    assert fake.tx(a)["memo"] == "by id"
    assert fake.tx(b)["cleared"] == "cleared"


def test_bulk_update_by_import_id_scoped_by_account(fake: FakeYnab) -> None:
    fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", import_id=IID)
    other = fake.add_transaction(BUDGET, OTHER_ACCOUNT, amount=-1, date="2026-10-01", import_id=IID)
    status, _, body = api(fake, "PATCH", tx_path(), {"transactions": [{"import_id": IID}]})
    assert status == 400 and "ambiguous" in body["error"]["detail"]
    status, _, _ = api(
        fake,
        "PATCH",
        tx_path(),
        {"transactions": [{"import_id": IID, "account_id": OTHER_ACCOUNT, "memo": "x"}]},
    )
    assert status == 209
    assert fake.tx(other)["memo"] == "x"


@pytest.mark.parametrize(
    "items",
    [
        [{"id": "missing", "memo": "x"}],
        [{"import_id": "nope", "memo": "x"}],
    ],
)
def test_bulk_update_unknown_target_is_404_and_atomic(fake: FakeYnab, items: list[Any]) -> None:
    good = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", memo="old")
    before = fake.server_knowledge(BUDGET)
    status, _, _ = api(
        fake, "PATCH", tx_path(), {"transactions": [{"id": good, "memo": "new"}, *items]}
    )
    assert status == 404
    assert fake.tx(good)["memo"] == "old"
    assert fake.server_knowledge(BUDGET) == before


def test_bulk_update_invalid_row_is_400_and_atomic(fake: FakeYnab) -> None:
    a = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", memo="old")
    b = fake.add_transaction(BUDGET, ACCOUNT, amount=-2, date="2026-10-01")
    status, _, _ = api(
        fake,
        "PATCH",
        tx_path(),
        {"transactions": [{"id": a, "memo": "new"}, {"id": b, "cleared": "x"}]},
    )
    assert status == 400
    assert fake.tx(a)["memo"] == "old"


@pytest.mark.parametrize(
    "body",
    [
        {"transactions": []},
        {"transaction": {}},
        {"transactions": [{"memo": "no target"}]},
        {"transactions": ["x"]},
    ],
)
def test_bulk_update_body_validated(fake: FakeYnab, body: Any) -> None:
    assert api(fake, "PATCH", tx_path(), body)[0] == 400


def test_bulk_update_same_id_twice_rejected(fake: FakeYnab) -> None:
    a = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01")
    body = {"transactions": [{"id": a, "memo": "1"}, {"id": a, "memo": "2"}]}
    assert api(fake, "PATCH", tx_path(), body)[0] == 400


def test_update_deleted_transaction_is_404(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01")
    fake.user_delete_transaction(tx_id)
    for method, path, body in [
        ("PUT", tx_path(tx_id), {"transaction": {"memo": "x"}}),
        ("PATCH", tx_path(tx_id), {"transaction": {"memo": "x"}}),
        ("PATCH", tx_path(), {"transactions": [{"id": tx_id, "memo": "x"}]}),
    ]:
        assert api(fake, method, path, body)[0] == 404


def test_update_can_move_account(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-100, date="2026-10-01")
    fake.client().update_transaction(BUDGET, tx_id, {"account_id": OTHER_ACCOUNT})
    assert fake.tx(tx_id)["account_id"] == OTHER_ACCOUNT
    assert fake.balances(BUDGET, ACCOUNT) == (0, 0, 0)
    assert fake.balances(BUDGET, OTHER_ACCOUNT) == (-100, -100, 0)


def test_update_payee_name(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", payee_name="A")
    updated = fake.client().update_transaction(BUDGET, tx_id, {"payee_name": "B"})
    assert updated.payee_name == "B"
    assert updated.payee_id is not None


def test_noop_update_does_not_appear_in_delta(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", memo="same")
    before = fake.server_knowledge(BUDGET)
    fake.client().update_transaction(BUDGET, tx_id, {"memo": "same"})
    assert fake.server_knowledge(BUDGET) == before


# ── deletes ─────────────────────────────────────────────────────────────────


def test_delete_is_soft_and_twice_is_404(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", import_id=IID)
    status, _, body = api(fake, "DELETE", tx_path(tx_id))
    assert status == 200
    assert body["data"]["transaction"]["deleted"] is True
    assert body["data"]["transaction"]["import_id"] == IID
    assert fake.tx(tx_id)["deleted"] is True
    assert api(fake, "DELETE", tx_path(tx_id))[0] == 404
    assert api(fake, "DELETE", tx_path("nope"))[0] == 404


# ── server knowledge & deltas ───────────────────────────────────────────────


def test_delta_returns_exactly_changed_rows_including_deleted(fake: FakeYnab) -> None:
    client = fake.client()
    a = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", cleared="uncleared")
    b = fake.add_transaction(BUDGET, ACCOUNT, amount=-2, date="2026-10-02")
    c = fake.add_transaction(BUDGET, OTHER_ACCOUNT, amount=-3, date="2026-10-03")
    full, k0 = client.get_transactions(BUDGET)
    assert {t.id for t in full} == {a, b, c}

    assert client.get_transactions(BUDGET, last_knowledge=k0) == ([], k0)

    client.update_transactions(BUDGET, [{"id": a, "cleared": "cleared"}])
    client.delete_transaction(BUDGET, b)
    delta, k1 = client.get_transactions(BUDGET, last_knowledge=k0)
    assert {(t.id, t.cleared, t.deleted) for t in delta} == {
        (a, "cleared", False),
        (b, "cleared", True),
    }
    assert k1 > k0

    # Full list excludes deleted; the account endpoint scopes the delta.
    assert {t.id for t in client.get_transactions(BUDGET)[0]} == {a, c}
    acct_delta, _ = client.get_transactions(BUDGET, account_id=OTHER_ACCOUNT, last_knowledge=k0)
    assert acct_delta == []

    fake.user_edit_transaction(c, cleared="reconciled", memo="checked")
    delta2, k2 = client.get_transactions(BUDGET, last_knowledge=k1)
    assert [(t.id, t.cleared, t.memo) for t in delta2] == [(c, "reconciled", "checked")]
    assert k2 == k1 + 1


def test_each_mutating_request_bumps_knowledge_once(fake: FakeYnab) -> None:
    before = fake.server_knowledge(BUDGET)
    fake.client().create_transactions(BUDGET, [new_tx(amount=-i) for i in range(1, 6)])
    assert fake.server_knowledge(BUDGET) == before + 1


def test_since_date_and_type_filters(fake: FakeYnab) -> None:
    fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-09-30", approved=False)
    fake.add_transaction(BUDGET, ACCOUNT, amount=-2, date="2026-10-01", category_name="Food")
    _, _, body = api(fake, "GET", tx_path() + "?since_date=2026-10-01")
    assert [t["amount"] for t in body["data"]["transactions"]] == [-2]
    _, _, body = api(fake, "GET", tx_path() + "?type=unapproved")
    assert [t["amount"] for t in body["data"]["transactions"]] == [-1]
    _, _, body = api(fake, "GET", tx_path() + "?type=uncategorized")
    assert [t["amount"] for t in body["data"]["transactions"]] == [-1]


@pytest.mark.parametrize(
    "query",
    ["since_date=2026-13-01", "since_date=yesterday", "last_knowledge_of_server=x",
     "last_knowledge_of_server=-1", "type=all"],
)  # fmt: skip
def test_invalid_query_params_are_400(fake: FakeYnab, query: str) -> None:
    assert api(fake, "GET", tx_path() + "?" + query)[0] == 400


def test_transactions_sorted_by_date_then_creation(fake: FakeYnab) -> None:
    first = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-02")
    second = fake.add_transaction(BUDGET, ACCOUNT, amount=-2, date="2026-10-01")
    third = fake.add_transaction(BUDGET, ACCOUNT, amount=-3, date="2026-10-02")
    ids = [t.id for t in fake.client().get_transactions(BUDGET)[0]]
    assert ids == [second, first, third]


def test_account_endpoint_unknown_account_is_404(fake: FakeYnab) -> None:
    assert api(fake, "GET", f"/budgets/{BUDGET}/accounts/nope/transactions")[0] == 404


# ── accounts & balances ─────────────────────────────────────────────────────


def test_balances_computed_from_non_deleted_transactions(fake: FakeYnab) -> None:
    fake.add_transaction(BUDGET, ACCOUNT, amount=10_000, date="2026-10-01", cleared="reconciled")
    fake.add_transaction(BUDGET, ACCOUNT, amount=-2_000, date="2026-10-02", cleared="cleared")
    pending = fake.add_transaction(
        BUDGET, ACCOUNT, amount=-500, date="2026-10-03", cleared="uncleared"
    )
    fake.add_transaction(BUDGET, ACCOUNT, amount=-99_999, date="2026-10-03", deleted=True)
    assert fake.balances(BUDGET, ACCOUNT) == (7_500, 8_000, -500)
    account = fake.client().get_account(BUDGET, ACCOUNT)
    assert (account.balance, account.cleared_balance, account.uncleared_balance) == (
        7_500,
        8_000,
        -500,
    )
    fake.client().delete_transaction(BUDGET, pending)
    assert fake.balances(BUDGET, ACCOUNT) == (8_000, 8_000, 0)


def test_accounts_delta_contains_accounts_whose_balance_changed(fake: FakeYnab) -> None:
    client = fake.client()
    _, k0 = client.get_accounts(BUDGET)
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-5, date="2026-10-01", memo="a")
    delta, k1 = client.get_accounts(BUDGET, last_knowledge=k0)
    assert [a.id for a in delta] == [ACCOUNT]
    # A memo-only edit changes no balance: the account is not in the delta.
    fake.user_edit_transaction(tx_id, memo="b")
    assert client.get_accounts(BUDGET, last_knowledge=k1)[0] == []
    # Clearing moves money between cleared/uncleared balances.
    _, k2 = client.get_accounts(BUDGET)
    fake.user_edit_transaction(tx_id, cleared="uncleared")
    assert [a.id for a in client.get_accounts(BUDGET, last_knowledge=k2)[0]] == [ACCOUNT]


def test_deleted_and_closed_accounts(fake: FakeYnab) -> None:
    client = fake.client()
    _, k0 = client.get_accounts(BUDGET)
    fake.edit_account(BUDGET, OTHER_ACCOUNT, deleted=True)
    fake.edit_account(BUDGET, ACCOUNT, closed=True, name="Revolut CHF")
    full, _ = client.get_accounts(BUDGET)
    assert [(a.id, a.closed, a.name) for a in full] == [(ACCOUNT, True, "Revolut CHF")]
    delta, _ = client.get_accounts(BUDGET, last_knowledge=k0)
    assert {(a.id, a.deleted) for a in delta} == {(ACCOUNT, False), (OTHER_ACCOUNT, True)}
    # Creating into a deleted account is rejected.
    status, _, _ = api(fake, "POST", tx_path(), {"transaction": new_tx(account_id=OTHER_ACCOUNT)})
    assert status == 400


def test_account_seed_validation(fake: FakeYnab) -> None:
    with pytest.raises(ValueError):
        fake.add_account(BUDGET, ACCOUNT, "dup")
    with pytest.raises(ValueError):
        fake.add_account("missing", "x", "x")
    with pytest.raises(ValueError):
        fake.add_budget(BUDGET)
    with pytest.raises(ValueError):
        fake.edit_account(BUDGET, ACCOUNT, balance=5)


# ── splits & transfers ──────────────────────────────────────────────────────


def test_split_create_and_rules(fake: FakeYnab) -> None:
    fake.add_category(BUDGET, "c1", "Food")
    fake.add_category(BUDGET, "c2", "Home")
    body = {
        "transaction": new_tx(
            amount=-3000,
            subtransactions=[
                {"amount": -1000, "category_id": "c1", "memo": "a"},
                {"amount": -2000, "category_id": "c2", "payee_name": "IKEA"},
            ],
        )
    }
    status, _, resp = api(fake, "POST", tx_path(), body)
    assert status == 201
    tx = resp["data"]["transaction"]
    assert tx["category_name"] == "Split (Multiple Categories)..."
    assert [(s["amount"], s["category_name"]) for s in tx["subtransactions"]] == [
        (-1000, "Food"),
        (-2000, "Home"),
    ]
    assert all(s["transaction_id"] == tx["id"] for s in tx["subtransactions"])
    client = fake.client()
    with pytest.raises(YnabValidationError):
        client.update_transaction(BUDGET, tx["id"], {"amount": -2500})
    with pytest.raises(YnabValidationError):
        client.update_transaction(
            BUDGET, tx["id"], {"subtransactions": [{"amount": -3000, "category_id": "c1"}]}
        )
    # Date changes on split transactions are ignored by YNAB.
    assert client.update_transaction(BUDGET, tx["id"], {"date": "2026-10-05"}).date == "2026-10-01"
    # Same amount is fine.
    assert client.update_transaction(BUDGET, tx["id"], {"amount": -3000, "memo": "m"}).memo == "m"
    deleted = client.delete_transaction(BUDGET, tx["id"])
    assert all(s.deleted for s in deleted.subtransactions)


def test_split_sum_mismatch_rejected(fake: FakeYnab) -> None:
    body = {"transaction": new_tx(amount=-3000, subtransactions=[{"amount": -1000}])}
    status, _, resp = api(fake, "POST", tx_path(), body)
    assert status == 400 and "add up" in resp["error"]["detail"]
    with pytest.raises(ValueError):
        fake.add_transaction(
            BUDGET, ACCOUNT, amount=-5, date="2026-10-01", subtransactions=[{"amount": -4}]
        )


def test_convert_to_split_via_update(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-10, date="2026-10-01")
    updated = fake.client().update_transaction(
        BUDGET, tx_id, {"subtransactions": [{"amount": -4}, {"amount": -6}]}
    )
    assert updated.is_split


def test_seeded_split_with_category_names(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(
        BUDGET,
        ACCOUNT,
        amount=-30,
        date="2026-10-01",
        subtransactions=[
            {"amount": -10, "category_name": "Food"},
            {"amount": -20, "category_name": "Fun", "payee_name": "Cinema"},
        ],
    )
    subs = fake.tx(tx_id)["subtransactions"]
    assert [(s["category_name"], s["payee_name"]) for s in subs] == [
        ("Food", None),
        ("Fun", "Cinema"),
    ]


def test_seeded_transfer_creates_linked_pair(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(
        BUDGET, ACCOUNT, amount=-50_000, date="2026-10-01", transfer_account_id=OTHER_ACCOUNT
    )
    tx = fake.tx(tx_id)
    partner = fake.tx(tx["transfer_transaction_id"])
    assert tx["transfer_account_id"] == OTHER_ACCOUNT
    assert tx["payee_name"] == "Transfer : Savings"
    assert partner["amount"] == 50_000
    assert partner["account_id"] == OTHER_ACCOUNT
    assert partner["transfer_account_id"] == ACCOUNT
    assert partner["transfer_transaction_id"] == tx_id
    assert partner["payee_name"] == "Transfer : Revolut"
    assert fake.balances(BUDGET, OTHER_ACCOUNT)[0] == 50_000


def test_api_transfer_via_transfer_payee(fake: FakeYnab) -> None:
    payee = fake.account(BUDGET, OTHER_ACCOUNT)["transfer_payee_id"]
    created = fake.client().create_transactions(BUDGET, [new_tx(amount=-7, payee_id=payee)])
    tx = created.created[0]
    assert tx.transfer_account_id == OTHER_ACCOUNT
    partner = fake.tx(tx.transfer_transaction_id)
    assert partner["amount"] == 7
    # Updating the amount keeps both sides in sync; deleting removes both.
    fake.client().update_transaction(BUDGET, tx.id, {"amount": -9, "date": "2026-10-02"})
    partner = fake.tx(tx.transfer_transaction_id)
    assert (partner["amount"], partner["date"]) == (9, "2026-10-02")
    fake.client().delete_transaction(BUDGET, tx.id)
    assert fake.tx(tx.transfer_transaction_id)["deleted"] is True
    # Transfer to own account or to a deleted account is invalid.
    own = fake.account(BUDGET, ACCOUNT)["transfer_payee_id"]
    assert api(fake, "POST", tx_path(), {"transaction": new_tx(payee_id=own)})[0] == 400
    fake.edit_account(BUDGET, OTHER_ACCOUNT, deleted=True)
    assert api(fake, "POST", tx_path(), {"transaction": new_tx(payee_id=payee)})[0] == 400


def test_unmodelled_transfer_operations_raise(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(
        BUDGET, ACCOUNT, amount=-5, date="2026-10-01", transfer_account_id=OTHER_ACCOUNT
    )
    with pytest.raises(NotImplementedError):
        api(fake, "PUT", tx_path(tx_id), {"transaction": {"payee_name": "x"}})


# ── seeds & simulated user actions ──────────────────────────────────────────


def test_seed_refuses_second_live_import_id(fake: FakeYnab) -> None:
    fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", import_id=IID)
    with pytest.raises(ValueError):
        fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", import_id=IID)
    # allowed in another account, or as a deleted row
    fake.add_transaction(BUDGET, OTHER_ACCOUNT, amount=-1, date="2026-10-01", import_id=IID)
    fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", import_id=IID, deleted=True)


def test_seed_with_explicit_id_and_validation(fake: FakeYnab) -> None:
    assert fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", id="t-1") == "t-1"
    with pytest.raises(ValueError):
        fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", id="t-1")
    with pytest.raises(ValueError):
        fake.add_transaction(BUDGET, ACCOUNT, amount=1.5, date="2026-10-01")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        fake.add_transaction(BUDGET, ACCOUNT, amount=1, date="2026/10/01")
    with pytest.raises(ValueError):
        fake.add_transaction(BUDGET, ACCOUNT, amount=1, date="2026-10-01", cleared="x")
    with pytest.raises(ValueError):
        fake.add_transaction(BUDGET, "nope", amount=1, date="2026-10-01")
    for bad_target in ("nope", ACCOUNT):
        with pytest.raises(ValueError):
            fake.add_transaction(
                BUDGET, ACCOUNT, amount=1, date="2026-10-01", transfer_account_id=bad_target
            )
    assert len(fake.transactions(BUDGET)) == 1  # nothing half-created


def test_user_edit_and_delete(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(
        BUDGET, ACCOUNT, amount=-1, date="2026-10-01", memo="Card | (pending)", cleared="uncleared"
    )
    edited = fake.user_edit_transaction(
        tx_id, memo="my note", cleared="reconciled", category_name="Food", payee_name="Shop"
    )
    assert (edited["memo"], edited["cleared"], edited["category_name"]) == (
        "my note",
        "reconciled",
        "Food",
    )
    assert edited["payee_name"] == "Shop"
    with pytest.raises(ValueError):
        fake.user_edit_transaction(tx_id, import_id="x")
    with pytest.raises(ValueError):
        fake.user_edit_transaction(tx_id, cleared="bogus")
    fake.user_delete_transaction(tx_id)
    assert fake.tx(tx_id)["deleted"] is True
    with pytest.raises(KeyError):
        fake.tx("unknown")


def test_user_cannot_edit_or_redelete_a_deleted_transaction(fake: FakeYnab) -> None:
    """The app cannot touch a deleted row; the fake must not invent that state."""
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", cleared="uncleared")
    client = fake.client()
    fake.user_delete_transaction(tx_id)
    _, knowledge = client.get_transactions(BUDGET)
    before = fake.tx(tx_id)
    with pytest.raises(ValueError, match="deleted"):
        fake.user_edit_transaction(tx_id, cleared="reconciled", memo="edited after delete")
    with pytest.raises(ValueError, match="deleted"):
        fake.user_delete_transaction(tx_id)
    assert fake.tx(tx_id) == before
    assert fake.server_knowledge(BUDGET) == knowledge
    assert client.get_transactions(BUDGET, last_knowledge=knowledge)[0] == []


def test_user_cannot_edit_the_partner_of_a_deleted_transfer(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(
        BUDGET, ACCOUNT, amount=-100, date="2026-10-01", transfer_account_id=OTHER_ACCOUNT
    )
    partner = fake.tx(tx_id)["transfer_transaction_id"]
    fake.user_delete_transaction(tx_id)
    with pytest.raises(ValueError):
        fake.user_edit_transaction(partner, memo="x")


def test_user_edit_is_all_or_nothing(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(
        BUDGET, ACCOUNT, amount=-30, date="2026-10-01",
        subtransactions=[{"amount": -10}, {"amount": -20}],
    )  # fmt: skip
    before = fake.server_knowledge(BUDGET)
    with pytest.raises(ValueError):
        fake.user_edit_transaction(tx_id, account_id=OTHER_ACCOUNT, amount=-31)
    assert fake.tx(tx_id)["account_id"] == ACCOUNT
    assert fake.server_knowledge(BUDGET) == before
    with pytest.raises(ValueError):
        fake.user_edit_transaction(tx_id, memo="x", date="2026-13-01")
    assert fake.tx(tx_id)["memo"] is None
    # date edits on splits are allowed in the app
    assert fake.user_edit_transaction(tx_id, date="2026-10-05")["date"] == "2026-10-05"


def test_user_edit_transfer_amount_propagates(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(
        BUDGET, ACCOUNT, amount=-5, date="2026-10-01", transfer_account_id=OTHER_ACCOUNT
    )
    fake.user_edit_transaction(tx_id, amount=-8)
    assert fake.tx(fake.tx(tx_id)["transfer_transaction_id"])["amount"] == 8


def test_inspection_helpers(fake: FakeYnab) -> None:
    a = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-02", import_id=IID)
    fake.add_transaction(BUDGET, OTHER_ACCOUNT, amount=-2, date="2026-10-01")
    fake.add_transaction(BUDGET, ACCOUNT, amount=-3, date="2026-10-03", deleted=True)
    assert [t["amount"] for t in fake.transactions()] == [-2, -1]
    assert [t["amount"] for t in fake.transactions(BUDGET, account_id=ACCOUNT)] == [-1]
    assert len(fake.transactions(BUDGET, include_deleted=True)) == 3
    assert fake.find_by_import_id(ACCOUNT, IID)["id"] == a
    assert fake.find_by_import_id(OTHER_ACCOUNT, IID) is None
    assert fake.find_by_import_id("unknown-account", IID) is None


# ── snapshot ────────────────────────────────────────────────────────────────


def _build(order: list[int], fake: FakeYnab) -> FakeYnab:
    client = fake.client()
    rows = {
        1: new_tx(amount=-1, import_id="YNAB:-1:2026-10-01:1"),
        2: new_tx(amount=-2, import_id="YNAB:-2:2026-10-01:1", memo="x"),
        3: new_tx(amount=-3, date="2026-09-01"),
    }
    for i in order:
        client.create_transactions(BUDGET, [rows[i]])
    return fake


def test_snapshot_is_id_free_and_order_independent() -> None:
    def fresh() -> FakeYnab:
        f = FakeYnab()
        f.add_budget(BUDGET)
        f.add_account(BUDGET, ACCOUNT, "Revolut")
        return f

    a = _build([1, 2, 3], fresh())
    b = _build([3, 2, 1], fresh())
    assert a.snapshot() == b.snapshot()
    assert "id" not in a.snapshot()[0]
    b.user_edit_transaction(b.transactions()[0]["id"], cleared="reconciled")
    assert a.snapshot() != b.snapshot()


def test_snapshot_deleted_handling(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01")
    fake.user_delete_transaction(tx_id)
    assert fake.snapshot() == []
    snap = fake.snapshot(include_deleted=True)
    assert len(snap) == 1 and snap[0]["deleted"] is True


# ── fault injection & request log ───────────────────────────────────────────


def test_request_log_records_everything(fake: FakeYnab) -> None:
    client = fake.client()
    client.get_transactions(BUDGET, since_date="2026-10-01")
    client.create_transactions(BUDGET, [new_tx(import_id=IID)])
    assert fake.calls[0] == (
        "GET",
        f"/budgets/{BUDGET}/transactions",
        {"since_date": "2026-10-01"},
        None,
    )
    assert isinstance(fake.calls[0], Call)
    method, path, query, body = fake.calls[1]
    assert (method, path, query) == ("POST", tx_path(), {})
    assert body["transactions"][0]["import_id"] == IID
    assert fake.requests[1].status == 201
    assert fake.requests[1].timeout == client.timeout
    assert fake.mutating_calls == 1
    assert fake.request_count == 2


def test_fail_next_matching_and_times(fake: FakeYnab) -> None:
    client = fake.client(max_retries=0)
    fake.fail_next("GET", r"/budgets/[^/]+/accounts", 500, times=2)
    client.get_transactions(BUDGET)  # different path: unaffected
    client.get_user()
    for _ in range(2):
        with pytest.raises(YnabServerError):
            client.get_accounts(BUDGET)
    client.get_accounts(BUDGET)
    assert fake.pending_faults == 0
    # fullmatch: a prefix pattern does not match longer paths
    fake.fail_next(None, r"/budgets/[^/]+/transactions", 500)
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01")
    client.update_transaction(BUDGET, tx_id, {"memo": "x"})
    assert fake.pending_faults == 1
    fake.clear_faults()
    assert fake.pending_faults == 0


def test_fail_next_custom_body_and_retry_after(fake: FakeYnab) -> None:
    fake.fail_next("*", None, 503, retry_after=7, body=b"<html>down</html>")
    status, headers, payload = fake.transport(
        "GET", fake.base_url + "/user", {"Authorization": f"Bearer {FAKE_TOKEN}"}, None, 1
    )
    assert (status, headers["Retry-After"], payload) == (503, "7", b"<html>down</html>")
    assert fake.requests[-1].status == 503
    assert fake.requests[-1].fault == "status 503"


def test_fail_before_apply_changes_nothing(fake: FakeYnab) -> None:
    fake.fail_next("POST", None, 500)
    before = fake.server_knowledge(BUDGET)
    with pytest.raises(YnabServerError):
        fake.client(max_retries=0).create_transactions(BUDGET, [new_tx(import_id=IID)])
    assert fake.transactions(BUDGET) == []
    assert fake.server_knowledge(BUDGET) == before


def test_fail_after_apply_changes_state_but_loses_response(fake: FakeYnab) -> None:
    fake.fail_next("POST", None, 502, after_apply=True)
    with pytest.raises(YnabServerError):
        fake.client(max_retries=0).create_transactions(BUDGET, [new_tx(import_id=IID)])
    assert [t["import_id"] for t in fake.transactions(BUDGET)] == [IID]


def test_fail_at_call_counts_only_mutating_calls_from_registration(fake: FakeYnab) -> None:
    client = fake.client(max_retries=0)
    client.create_transactions(BUDGET, [new_tx(import_id="YNAB:1:2026-10-01:1")])  # call 1
    fake.fail_at_call(2, 400)
    client.get_transactions(BUDGET)  # GETs never count
    client.create_transactions(BUDGET, [new_tx(import_id="YNAB:2:2026-10-01:1")])  # 1st from now
    with pytest.raises(YnabValidationError):
        client.create_transactions(BUDGET, [new_tx(import_id="YNAB:3:2026-10-01:1")])
    client.create_transactions(BUDGET, [new_tx(import_id="YNAB:4:2026-10-01:1")])
    assert sorted(t["import_id"] for t in fake.transactions(BUDGET)) == [
        "YNAB:1:2026-10-01:1",
        "YNAB:2:2026-10-01:1",
        "YNAB:4:2026-10-01:1",
    ]
    assert fake.mutating_calls == 4


def test_fail_at_call_with_retry_counts_the_retry(fake: FakeYnab) -> None:
    fake.fail_at_call(1, 500)
    client = fake.client()  # retries 5xx
    client.create_transactions(BUDGET, [new_tx(import_id=IID)])
    assert fake.mutating_calls == 2
    assert len(fake.transactions(BUDGET)) == 1


def test_fail_at_call_survives_reset_log(fake: FakeYnab) -> None:
    client = fake.client(max_retries=0)
    client.create_transactions(BUDGET, [new_tx(import_id="YNAB:1:2026-10-01:1")])
    fake.fail_at_call(2, 400)
    fake.reset_log()
    assert fake.calls == [] and fake.mutating_calls == 0
    client.create_transactions(BUDGET, [new_tx(import_id="YNAB:2:2026-10-01:1")])
    with pytest.raises(YnabValidationError):
        client.create_transactions(BUDGET, [new_tx(import_id="YNAB:3:2026-10-01:1")])


def test_fail_at_call_validation(fake: FakeYnab) -> None:
    with pytest.raises(ValueError):
        fake.fail_at_call(0)
    with pytest.raises(ValueError):
        fake.raise_network_error_at_call(0)


def test_two_faults_for_the_same_call_are_refused(fake: FakeYnab) -> None:
    """A second fault for one call would never fire and stay pending forever."""
    fake.fail_at_call(1, 500)
    with pytest.raises(ValueError, match="already registered"):
        fake.fail_at_call(1, 400)
    with pytest.raises(ValueError, match="already registered"):
        fake.raise_network_error_at_call(1)
    fake.fail_at_call(2, 400)  # a different call is fine
    assert fake.pending_faults == 2
    client = fake.client(max_retries=0)
    with pytest.raises(YnabServerError):
        client.create_transactions(BUDGET, [new_tx(import_id="YNAB:1:2026-10-01:1")])
    with pytest.raises(YnabValidationError):
        client.create_transactions(BUDGET, [new_tx(import_id="YNAB:2:2026-10-01:1")])
    assert fake.pending_faults == 0


def test_raise_network_error_next(fake: FakeYnab) -> None:
    fake.raise_network_error_next("GET", r"/user")
    with pytest.raises(TimeoutError):
        fake.transport("GET", fake.base_url + "/user", {}, None, 1)
    assert fake.requests[-1].fault == "network"
    assert fake.requests[-1].status is None
    fake.raise_network_error_next(exc=ConnectionResetError("boom"))
    with pytest.raises(ConnectionResetError):
        fake.transport("GET", fake.base_url + "/user", {}, None, 1)


def test_raise_network_error_at_call_after_apply(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", cleared="uncleared")
    fake.raise_network_error_at_call(1, after_apply=True)
    client = fake.client()
    # Applied, response lost, retried (idempotent PATCH) → same final state, two calls.
    out = client.update_transactions(BUDGET, [{"id": tx_id, "cleared": "cleared"}])
    assert out[0].cleared == "cleared"
    assert fake.mutating_calls == 2


def test_quota_and_rate_limit_header(fake: FakeYnab) -> None:
    fake.quota = 3
    client = fake.client()
    for _ in range(3):
        client.get_user()
    assert client.rate_limit_used == 3
    with pytest.raises(YnabRateLimited):
        client.get_user()
    assert fake.requests[-1].status == 429
    fake.reset_log()
    client.get_user()


def test_client_defaults_do_not_sleep(fake: FakeYnab) -> None:
    fake.fail_next("GET", None, 500, times=2)
    fake.client().get_user()
    assert fake.slept == [1.0, 2.0]


def test_unknown_budget_via_client_is_not_found(fake: FakeYnab) -> None:
    with pytest.raises(YnabNotFound):
        fake.client().get_transactions("missing")


def test_concurrent_creates_never_duplicate(fake: FakeYnab) -> None:
    client = fake.client()
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            client.create_transactions(
                BUDGET, [new_tx(import_id=f"YNAB:-{i}:2026-10-01:1") for i in range(20)]
            )
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert len(fake.transactions(BUDGET)) == 20


# ── v1 adapter ──────────────────────────────────────────────────────────────


def test_v1_request_contract(fake: FakeYnab, capsys: pytest.CaptureFixture[str]) -> None:
    result = fake.v1_request("GET", "/budgets", FAKE_TOKEN)
    assert result["data"]["budgets"][0]["id"] == BUDGET
    with pytest.raises(RuntimeError, match=r"^YNAB API 404: "):
        fake.v1_request("DELETE", f"/budgets/{BUDGET}/transactions/nope", FAKE_TOKEN)
    assert capsys.readouterr().out.startswith("  ✗ YNAB API error (404): {")
    with pytest.raises(RuntimeError, match=r"^YNAB API 401"):
        fake.v1_request("GET", "/user", "wrong")
    fake.raise_network_error_next(exc=urllib.error.URLError("Name or service not known"))
    with pytest.raises(RuntimeError, match=r"^YNAB network error: <urlopen error"):
        fake.v1_request("GET", "/user", FAKE_TOKEN)
    assert "  ✗ YNAB network error: <urlopen error" in capsys.readouterr().out
    path = f"/budgets/{BUDGET}/accounts/{ACCOUNT}/transactions?since_date=2026-01-01"
    assert fake.v1_request("GET", path, FAKE_TOKEN)["data"]["transactions"] == []
    assert fake.calls[-1].query == {"since_date": "2026-01-01"}


@pytest.mark.parametrize("exc", [TimeoutError("timed out"), ConnectionResetError("reset")])
def test_v1_request_lets_non_urlerror_escape_like_v1(fake: FakeYnab, exc: OSError) -> None:
    """v1 only caught URLError; a read timeout escaped raw. The adapter must not hide that."""
    fake.raise_network_error_next(exc=exc)
    with pytest.raises(type(exc)):
        fake.v1_request("GET", "/user", FAKE_TOKEN)
    fake.raise_network_error_next()  # the default injected error is a TimeoutError too
    with pytest.raises(TimeoutError):
        fake.v1_request("GET", "/user", FAKE_TOKEN)


def test_v1_request_invalid_json_success_escapes_raw(fake: FakeYnab) -> None:
    fake.fail_next("GET", r"/user", 200, body=b"<html>gateway</html>")
    with pytest.raises(json.JSONDecodeError):
        fake.v1_request("GET", "/user", FAKE_TOKEN)


def test_v1_importer_runs_against_fake(
    fake: FakeYnab, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The frozen v1 importer works on the fake (used by migration/oracle tests)."""
    from tests.legacy import v1_revolut_to_ynab as v1

    monkeypatch.setattr(v1, "ynab_request", fake.v1_request)

    def row(state: str) -> dict[str, Any]:
        return {
            "date": "2026-08-09",
            "amount": -5000,
            "payee_name": "Shop",
            "memo": "(pending)" if state == "PENDING" else None,
            "cleared": "cleared" if state == "COMPLETED" else "uncleared",
            "approved": True,
            "import_id": "YNAB:-5000:2026-08-09:1",
            "_state": state,
            "_started_time": "12:00:00",
            "_started_at": "2026-08-09 12:00:00",
        }

    conn = v1.init_db(str(tmp_path / "v1.db"))
    v1.import_and_track(conn, FAKE_TOKEN, BUDGET, ACCOUNT, [row("PENDING")])
    assert fake.snapshot()[0]["cleared"] == "uncleared"
    assert fake.snapshot()[0]["memo"] == "(pending)"
    v1.import_and_track(conn, FAKE_TOKEN, BUDGET, ACCOUNT, [row("COMPLETED")])
    snap = fake.snapshot()
    assert (snap[0]["cleared"], snap[0]["memo"]) == ("cleared", None)
    fake.reset_log()
    v1.import_and_track(conn, FAKE_TOKEN, BUDGET, ACCOUNT, [row("COMPLETED")])
    assert fake.calls == []  # re-import makes zero calls (v1 contract)
    conn.close()


# ── convergence harness ─────────────────────────────────────────────────────


def _mini_import(client: YnabClient, rows: list[dict[str, Any]]) -> None:
    """A tiny idempotent importer: create by import_id, then clear completed rows."""
    client.create_transactions(BUDGET, rows)
    txs, _ = client.get_transactions(BUDGET, account_id=ACCOUNT)
    wanted = {r["import_id"] for r in rows if r["cleared"] == "cleared"}
    updates = [
        {"id": t.id, "cleared": "cleared"}
        for t in txs
        if t.import_id in wanted and t.cleared == "uncleared"
    ]
    if updates:
        client.update_transactions(BUDGET, updates)


def _seeded() -> FakeYnab:
    f = FakeYnab()
    f.add_budget(BUDGET)
    f.add_account(BUDGET, ACCOUNT, "Revolut")
    for i in range(3):  # pendings from an earlier import that have now completed
        f.add_transaction(
            BUDGET,
            ACCOUNT,
            amount=-(i + 1),
            date="2026-10-01",
            cleared="uncleared",
            import_id=f"YNAB:-{i + 1}:2026-10-01:1",
        )
    return f


_ROWS = [
    new_tx(amount=-(i + 1), import_id=f"YNAB:-{i + 1}:2026-10-01:1", cleared="cleared")
    for i in range(5)
]


@pytest.mark.parametrize("after_apply", [False, True])
@pytest.mark.parametrize("network", [False, True])
def test_fail_at_every_mutating_call_then_rerun_converges(after_apply: bool, network: bool) -> None:
    baseline = _seeded()
    _mini_import(baseline.client(), _ROWS)
    total = baseline.mutating_calls
    assert total == 2
    for k in range(1, total + 1):
        fake = _seeded()
        if network:
            fake.raise_network_error_at_call(k, after_apply=after_apply)
        else:
            fake.fail_at_call(k, 400, after_apply=after_apply)
        client = fake.client(max_retries=0)
        with pytest.raises(YnabError):
            _mini_import(client, _ROWS)
        _mini_import(client, _ROWS)  # the rerun finishes the job
        assert fake.snapshot(include_deleted=True) == baseline.snapshot(include_deleted=True), k


# ── randomized consistency ──────────────────────────────────────────────────


@pytest.mark.parametrize("seed", range(5))
def test_random_operations_keep_balances_and_deltas_consistent(seed: int) -> None:
    """Balances, deltas and indexes stay consistent under a random mix of operations."""
    import random

    rng = random.Random(seed)
    fake = FakeYnab()
    fake.add_budget(BUDGET)
    accounts = [ACCOUNT, OTHER_ACCOUNT, "acc-cash"]
    fake.add_account(BUDGET, ACCOUNT, "Revolut")
    fake.add_account(BUDGET, OTHER_ACCOUNT, "Savings", "savings")
    fake.add_account(BUDGET, "acc-cash", "Cash", "cash")
    client = fake.client(max_retries=0)
    mirror: dict[str, Any] = {}
    knowledge: int | None = None
    for step in range(150):
        live = [t["id"] for t in fake.transactions(BUDGET)]
        op = rng.choice(["create", "create", "update", "bulk", "delete", "user", "transfer"])
        if op == "create" or not live:
            n = rng.randint(1, 3)
            client.create_transactions(
                BUDGET,
                [
                    new_tx(
                        amount=rng.randint(-5000, 5000),
                        account_id=rng.choice(accounts),
                        cleared=rng.choice(["cleared", "uncleared", "reconciled"]),
                        import_id=f"YNAB:{rng.randint(1, 40)}:2026-10-01:1",
                    )
                    for _ in range(n)
                ],
            )
        elif op == "update":
            tx_id = rng.choice(live)
            fields = rng.choice(
                [
                    {"amount": rng.randint(-5000, 5000)},
                    {"cleared": rng.choice(["cleared", "uncleared", "reconciled"])},
                    {"memo": f"m{step}"},
                    {"date": f"2026-10-{rng.randint(1, 28):02d}"},
                ]
            )
            if fake.tx(tx_id)["transfer_account_id"] is None and rng.random() < 0.2:
                fields = {"account_id": rng.choice(accounts)}
            client.update_transaction(BUDGET, tx_id, fields)
        elif op == "bulk":
            ids = rng.sample(live, k=min(len(live), rng.randint(1, 4)))
            client.update_transactions(
                BUDGET, [{"id": i, "cleared": rng.choice(["cleared", "uncleared"])} for i in ids]
            )
        elif op == "delete":
            client.delete_transaction(BUDGET, rng.choice(live))
        elif op == "user":
            fake.user_edit_transaction(rng.choice(live), amount=rng.randint(-100, 100))
        else:
            src, dst = rng.sample(accounts, k=2)
            fake.add_transaction(
                BUDGET,
                src,
                amount=rng.randint(-900, 900),
                date="2026-10-02",
                transfer_account_id=dst,
            )
        # A delta-synced mirror must always equal a full fetch.
        delta, knowledge = client.get_transactions(BUDGET, last_knowledge=knowledge)
        for tx in delta:
            mirror[tx.id] = tx
        live_mirror = {i: t for i, t in mirror.items() if not t.deleted}
        full, _ = client.get_transactions(BUDGET)
        assert {t.id: t for t in full} == live_mirror, f"step {step} ({op})"
    fake.check_invariants()
    for account_id in accounts:
        acc = client.get_account(BUDGET, account_id)
        live_rows = [t for t in live_mirror.values() if t.account_id == account_id]
        assert acc.balance == sum(t.amount for t in live_rows)
        assert acc.cleared_balance == sum(t.amount for t in live_rows if t.is_cleared)


# ── C04: the v1 fake never returned duplicates, 404s for deleted rows or 429/5xx ──


def test_C04_fake_reports_duplicate_import_ids(fake: FakeYnab) -> None:
    client = fake.client()
    client.create_transactions(BUDGET, [new_tx(import_id=IID)])
    again = client.create_transactions(BUDGET, [new_tx(import_id=IID)])
    assert again.duplicate_import_ids == [IID]
    assert len(fake.transactions(BUDGET)) == 1


def test_C04_fake_returns_404_for_deleted_transactions(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(BUDGET, ACCOUNT, amount=-1, date="2026-10-01", import_id=IID)
    fake.user_delete_transaction(tx_id)
    client = fake.client()
    with pytest.raises(YnabNotFound):
        client.update_transaction(BUDGET, tx_id, {"cleared": "cleared"})
    with pytest.raises(YnabNotFound):
        client.delete_transaction(BUDGET, tx_id)


def test_C04_fake_returns_reconciled_rows_and_injects_429_and_5xx(fake: FakeYnab) -> None:
    tx_id = fake.add_transaction(
        BUDGET, ACCOUNT, amount=-1, date="2026-10-01", cleared="reconciled"
    )
    client = fake.client(max_retries=0)
    assert client.get_transactions(BUDGET)[0][0].cleared == "reconciled"
    fake.fail_next("PATCH", None, 429)
    with pytest.raises(YnabRateLimited):
        client.update_transactions(BUDGET, [{"id": tx_id, "memo": "x"}])
    fake.fail_next("PATCH", None, 503)
    with pytest.raises(YnabServerError):
        client.update_transactions(BUDGET, [{"id": tx_id, "memo": "x"}])
    assert fake.tx(tx_id)["memo"] is None
