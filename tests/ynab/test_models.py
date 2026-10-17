"""Parsing of YNAB API objects into typed models."""

from __future__ import annotations

import pytest

from ynab_helper.errors import YnabError
from ynab_helper.ynab.models import YnabAccount, YnabSubtransaction, YnabTransaction


def _tx(**overrides: object) -> dict[str, object]:
    data: dict[str, object] = {
        "id": "t1",
        "date": "2026-10-01",
        "amount": -12340,
        "memo": "Card | (pending)",
        "cleared": "uncleared",
        "approved": True,
        "flag_color": None,
        "account_id": "a1",
        "account_name": "Revolut",
        "payee_id": "p1",
        "payee_name": "Coop",
        "category_id": None,
        "category_name": None,
        "transfer_account_id": None,
        "transfer_transaction_id": None,
        "matched_transaction_id": None,
        "import_id": "YNAB:-12340:2026-10-01:1",
        "import_payee_name": "Coop",
        "import_payee_name_original": "COOP-1234",
        "debt_transaction_type": None,
        "deleted": False,
        "subtransactions": [],
        "some_future_field": {"x": 1},
    }
    data.update(overrides)
    return data


def test_transaction_from_api_keeps_fields_and_raw() -> None:
    raw = _tx()
    tx = YnabTransaction.from_api(raw)
    assert tx.id == "t1"
    assert tx.amount == -12340
    assert tx.cleared == "uncleared"
    assert tx.import_id == "YNAB:-12340:2026-10-01:1"
    assert tx.import_payee_name_original == "COOP-1234"
    assert tx.raw is raw
    assert tx.raw["some_future_field"] == {"x": 1}
    assert not tx.is_split
    assert not tx.is_transfer
    assert not tx.is_cleared


@pytest.mark.parametrize(("cleared", "expected"), [("cleared", True), ("reconciled", True)])
def test_transaction_is_cleared(cleared: str, expected: bool) -> None:
    assert YnabTransaction.from_api(_tx(cleared=cleared)).is_cleared is expected


def test_transaction_with_subtransactions() -> None:
    tx = YnabTransaction.from_api(
        _tx(
            amount=-3000,
            subtransactions=[
                {"id": "s1", "transaction_id": "t1", "amount": -1000, "deleted": False},
                {"id": "s2", "amount": -2000, "category_name": "Food", "deleted": False},
            ],
        )
    )
    assert tx.is_split
    assert [s.amount for s in tx.subtransactions] == [-1000, -2000]
    # transaction_id falls back to the parent id when YNAB omits it
    assert tx.subtransactions[1].transaction_id == "t1"
    assert tx.subtransactions[1].category_name == "Food"


def test_split_with_only_deleted_subtransactions_is_not_split() -> None:
    tx = YnabTransaction.from_api(
        _tx(subtransactions=[{"id": "s1", "transaction_id": "t1", "amount": -1, "deleted": True}])
    )
    assert not tx.is_split


def test_models_are_frozen_and_compare_without_raw() -> None:
    a = YnabTransaction.from_api(_tx())
    b = YnabTransaction.from_api(_tx(some_future_field="different"))
    assert a == b
    with pytest.raises(AttributeError):
        a.amount = 1  # type: ignore[misc]


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": 12.5},  # float would be a unit mix-up
        {"amount": True},
        {"amount": None},
        {"amount": "100"},
        {"cleared": None},  # never default cleared: could cause a downgrade
        {"id": ""},
        {"account_id": None},
        {"date": "01.10.2026"},
        {"date": None},
        {"approved": "yes"},
        {"deleted": "false"},
        {"memo": 5},
        {"subtransactions": "nope"},
        {"subtransactions": [{"id": "s1", "amount": 1.0}]},
    ],
)
def test_malformed_transaction_raises_typed_error(overrides: dict[str, object]) -> None:
    with pytest.raises(YnabError) as info:
        YnabTransaction.from_api(_tx(**overrides))
    assert "malformed" in str(info.value)
    assert "unexpected format" in info.value.user_message


def test_missing_cleared_key_is_rejected() -> None:
    data = _tx()
    del data["cleared"]
    with pytest.raises(YnabError):
        YnabTransaction.from_api(data)


@pytest.mark.parametrize("payload", [None, [], "x", 3])
def test_non_object_payload_rejected(payload: object) -> None:
    with pytest.raises(YnabError):
        YnabTransaction.from_api(payload)
    with pytest.raises(YnabError):
        YnabAccount.from_api(payload)


def test_optional_booleans_default_when_absent() -> None:
    data = _tx()
    del data["deleted"]
    del data["approved"]
    tx = YnabTransaction.from_api(data)
    assert tx.deleted is False
    assert tx.approved is False


def test_account_from_api() -> None:
    raw = {
        "id": "a1",
        "name": "Revolut",
        "type": "checking",
        "on_budget": True,
        "closed": False,
        "note": None,
        "balance": 1000,
        "cleared_balance": 800,
        "uncleared_balance": 200,
        "transfer_payee_id": "tp1",
        "last_reconciled_at": "2026-10-01T00:00:00Z",
        "deleted": False,
    }
    acc = YnabAccount.from_api(raw)
    assert (acc.balance, acc.cleared_balance, acc.uncleared_balance) == (1000, 800, 200)
    assert acc.raw is raw
    assert acc.transfer_payee_id == "tp1"


def test_account_missing_balance_is_none_not_zero() -> None:
    # Valuation syncs must notice a missing balance instead of treating it as 0.
    acc = YnabAccount.from_api({"id": "a1", "name": "X"})
    assert acc.balance is None
    assert acc.cleared_balance is None


@pytest.mark.parametrize("bad", [{"balance": 1.5}, {"cleared_balance": "1"}, {"id": None}])
def test_account_malformed(bad: dict[str, object]) -> None:
    data = {"id": "a1", "name": "X", "balance": 1}
    data.update(bad)
    with pytest.raises(YnabError):
        YnabAccount.from_api(data)


def test_subtransaction_requires_parent_id() -> None:
    with pytest.raises(YnabError):
        YnabSubtransaction.from_api({"id": "s1", "amount": 1})
    sub = YnabSubtransaction.from_api({"id": "s1", "amount": 1}, parent_id="t1")
    assert sub.transaction_id == "t1"
