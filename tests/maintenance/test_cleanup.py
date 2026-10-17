"""Tests for ynab_helper.maintenance.cleanup (DESIGN §6.7; findings F01, C02)."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.fakes.fake_ynab import FakeYnab
from tests.maintenance.helpers import (
    ACCOUNT,
    BUDGET,
    OTHER,
    make_fake,
    mutating_calls,
    open_db,
    sync_mirror,
)
from ynab_helper.errors import YnabAuthError
from ynab_helper.maintenance.cleanup import CleanupResult, cleanup_pending_memos

PATCH_PATH = r"/budgets/[^/]+/transactions"


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    c = open_db(tmp_path)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
def fake() -> FakeYnab:
    f = make_fake()
    f.add_transaction(
        BUDGET,
        ACCOUNT,
        id="r1",
        amount=-12_000,
        date="2026-10-01",
        cleared="reconciled",
        memo="(pending)",
        import_id="YNAB:-12000:2026-10-01:1",
    )
    f.add_transaction(
        BUDGET,
        ACCOUNT,
        id="c1",
        amount=-5_000,
        date="2026-10-02",
        cleared="cleared",
        memo="Transfer | (pending) | Fee: 0.5",
        import_id="YNAB:-5000:2026-10-02:1",
    )
    f.add_transaction(
        BUDGET,
        ACCOUNT,
        id="c2",
        amount=-7_000,
        date="2026-10-03",
        cleared="cleared",
        memo="dinner w/ Anna (pending)",
        import_id="YNAB:-7000:2026-10-03:1",
    )
    # Still pending at Revolut: must keep its marker (C02).
    f.add_transaction(
        BUDGET,
        ACCOUNT,
        id="p1",
        amount=-50_000,
        date="2026-10-15",
        cleared="uncleared",
        memo="(pending)",
        import_id="YNAB:-50000:2026-10-15:1",
    )
    f.add_transaction(BUDGET, ACCOUNT, id="n1", amount=-1_000, date="2026-10-04", memo="no marker")
    f.add_transaction(
        BUDGET, ACCOUNT, id="d1", amount=-2_000, date="2026-10-04", memo="(pending)", deleted=True
    )
    f.add_transaction(BUDGET, OTHER, id="o1", amount=-3_000, date="2026-10-04", memo="(pending)")
    return f


def run(conn: sqlite3.Connection, fake: FakeYnab, **kwargs) -> CleanupResult:
    sync_mirror(conn, fake)
    return cleanup_pending_memos(
        conn, fake.client(), budget_id=BUDGET, account_id=ACCOUNT, **kwargs
    )


def test_C02_strips_markers_only_on_cleared_rows_and_never_unreconciles(conn, fake):
    result = run(conn, fake)
    assert result.ok
    assert [f.tx_id for f in result.updated] == ["r1", "c1", "c2"]
    assert result.skipped_uncleared == 1
    r1, c1, c2, p1 = (fake.tx(i) for i in ("r1", "c1", "c2", "p1"))
    assert (r1["cleared"], r1["memo"]) == ("reconciled", None)
    assert (c1["cleared"], c1["memo"]) == ("cleared", "Transfer | Fee: 0.5")
    assert c2["memo"] == "dinner w/ Anna"
    assert (p1["cleared"], p1["memo"]) == ("uncleared", "(pending)")
    assert fake.tx("n1")["memo"] == "no marker"
    assert fake.tx("o1")["memo"] == "(pending)"


def test_F01_only_memo_is_ever_sent(conn, fake):
    run(conn, fake)
    calls = mutating_calls(fake)
    assert [c.method for c in calls] == ["PATCH"]
    for update in calls[0].body["transactions"]:
        assert set(update) == {"id", "memo"}


def test_dry_run_lists_candidates_without_writing(conn, fake):
    result = run(conn, fake, dry_run=True)
    assert [f.tx_id for f in result.candidates] == ["r1", "c1", "c2"]
    assert result.updated == []
    assert result.candidates[0].new_memo is None
    assert mutating_calls(fake) == []


def test_nothing_to_do(conn):
    fake = make_fake()
    fake.add_transaction(
        BUDGET, ACCOUNT, amount=-1, date="2026-10-01", memo="(pending)", cleared="uncleared"
    )
    result = run(conn, fake)
    assert result.candidates == [] and result.skipped_uncleared == 1
    assert fake.calls == []


@pytest.mark.parametrize("bulk_status", [400, None])
def test_rejected_bulk_patch_falls_back_to_one_by_one(conn, fake, bulk_status):
    sync_mirror(conn, fake)
    fake.user_delete_transaction("c1")  # gone in YNAB, the mirror still lists it
    if bulk_status is not None:
        fake.fail_next("PATCH", PATCH_PATH, bulk_status)
    # Without an injected fault the bulk PATCH itself is a 404 (it names a deleted row).
    result = cleanup_pending_memos(conn, fake.client(), budget_id=BUDGET, account_id=ACCOUNT)
    assert [f.tx_id for f in result.updated] == ["r1", "c2"]
    assert [(f.fix.tx_id, f.error) for f in result.failures] == [
        ("c1", "The transaction no longer exists in YNAB.")
    ]
    assert not result.ok
    assert fake.tx("r1")["memo"] is None and fake.tx("r1")["cleared"] == "reconciled"
    puts = [c for c in mutating_calls(fake) if c.method == "PUT"]
    assert len(puts) == 3
    assert all(set(c.body["transaction"]) == {"memo"} for c in puts)


def test_rate_limit_stops_the_run(conn, fake):
    fake.fail_next("PATCH", PATCH_PATH, 429)
    result = run(conn, fake)
    assert result.rate_limited
    assert result.not_applied == 3
    assert result.updated == []
    assert fake.tx("r1")["memo"] == "(pending)"


def test_rate_limit_in_a_later_chunk(conn):
    fake = make_fake()
    for i in range(60):
        fake.add_transaction(
            BUDGET, ACCOUNT, id=f"t{i:02d}", amount=-1000 - i, date="2026-10-01", memo="(pending)"
        )
    sync_mirror(conn, fake)
    fake.fail_at_call(2, 429)
    result = cleanup_pending_memos(conn, fake.client(), budget_id=BUDGET, account_id=ACCOUNT)
    assert len(result.updated) == 50
    assert result.not_applied == 10
    assert result.rate_limited and not result.ok


def test_server_error_on_a_chunk_is_reported_per_row(conn, fake):
    fake.fail_next("PATCH", PATCH_PATH, 500, times=3)  # initial try + 2 client retries
    result = run(conn, fake)
    assert len(result.failures) == 3
    assert result.updated == []


def test_auth_error_is_raised(conn, fake):
    fake.fail_next("PATCH", PATCH_PATH, 401)
    with pytest.raises(YnabAuthError):
        run(conn, fake)


def test_result_to_dict(conn, fake):
    data = run(conn, fake).to_dict()
    assert data["ok"] is True
    assert len(data["updated"]) == 3
