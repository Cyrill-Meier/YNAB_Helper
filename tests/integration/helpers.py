"""Shared builders for the end-to-end tests (services + executor + planner + storage + fake)."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from tests.fakes.fake_ynab import FAKE_TOKEN, FakeYnab
from ynab_helper.services import UserContext

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
BUDGET = "b1"
ACCT = "acct-main"
OTHER = "acct-other"


def new_fake(**kwargs: Any) -> FakeYnab:
    fake = FakeYnab(**kwargs)
    fake.add_budget(BUDGET, currency="CHF")
    fake.add_account(BUDGET, ACCT, "Revolut")
    fake.add_account(BUDGET, OTHER, "Revolut Business")
    return fake


def user_ctx(data_dir: Path, account_id: str = ACCT, telegram_id: int = 42) -> UserContext:
    return UserContext.from_user_row(
        {
            "telegram_id": telegram_id,
            "ynab_token": FAKE_TOKEN,
            "budget_id": BUDGET,
            "account_id": account_id,
            "auto_approve": True,
        },
        data_dir,
    )


def combined_csv(*names: str) -> bytes:
    """One statement made of the rows of several fixture exports (header once).

    The fixtures use disjoint (amount, date) groups, so concatenating them keeps every
    row's occurrence number (and therefore its import_id).
    """
    lines: list[bytes] = []
    for index, name in enumerate(names):
        content = (FIXTURES / name).read_bytes().splitlines(keepends=True)
        lines.extend(content if index == 0 else content[1:])
    return b"".join(lines)


def live_rows(fake: FakeYnab, account_id: str) -> Counter[tuple[str, int, str | None]]:
    """(date, amount, payee) of the account's live transactions, with multiplicity."""
    return Counter(
        (t["date"], t["amount"], t["payee_name"])
        for t in fake.transactions(BUDGET, account_id=account_id)
    )


def mutating_methods(fake: FakeYnab) -> list[str]:
    return [c.method for c in fake.calls if c.method in ("POST", "PUT", "PATCH", "DELETE")]


def by_import_id(fake: FakeYnab, account_id: str, import_id: str) -> dict[str, Any]:
    tx = fake.find_by_import_id(account_id, import_id, include_deleted=False)
    assert tx is not None, import_id
    return tx


def cleared_of(fake: FakeYnab, account_id: str, import_ids: Iterable[str]) -> dict[str, str]:
    return {i: by_import_id(fake, account_id, i)["cleared"] for i in import_ids}
