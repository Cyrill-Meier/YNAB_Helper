"""Fixtures shared by the YNAB client / fake tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from tests.fakes.fake_ynab import FakeYnab

BUDGET = "b1"
ACCOUNT = "acc-revolut"
OTHER_ACCOUNT = "acc-savings"


@pytest.fixture
def fake() -> Iterator[FakeYnab]:
    """A fake with one CHF budget, a checking and a savings account.

    The fake's internal invariants are verified after every test that uses it.
    """
    f = FakeYnab()
    f.add_budget(BUDGET, currency="CHF", name="Main")
    f.add_account(BUDGET, ACCOUNT, "Revolut")
    f.add_account(BUDGET, OTHER_ACCOUNT, "Savings", "savings")
    yield f
    f.check_invariants()


def new_tx(
    amount: int = -1000,
    date: str = "2026-10-01",
    import_id: str | None = None,
    account_id: str = ACCOUNT,
    **extra: object,
) -> dict[str, object]:
    """A create payload like the importer sends."""
    tx: dict[str, object] = {
        "account_id": account_id,
        "date": date,
        "amount": amount,
        "payee_name": "Coop",
        "cleared": "uncleared",
        "approved": True,
    }
    if import_id is not None:
        tx["import_id"] = import_id
    tx.update(extra)
    return tx
