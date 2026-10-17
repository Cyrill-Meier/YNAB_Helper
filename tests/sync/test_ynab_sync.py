"""YNAB → mirror sync (docs/DESIGN.md §6.5) against the stateful fake YNAB."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.errors import YnabAuthError, YnabError, YnabServerError
from ynab_helper.storage import LedgerStore, MirrorStore, open_tx_db
from ynab_helper.storage.ledger import LedgerEntry, LedgerStatus
from ynab_helper.sync import (
    CURRENCY_MAX_AGE,
    DEFAULT_RATE_LIMIT_BACKOFF,
    MAX_RATE_LIMIT_BACKOFF,
    SyncResult,
    format_sync_time,
    note_rate_limited,
    parse_sync_time,
    rate_limited_until,
    sync_and_refresh,
    sync_budget,
)

BUDGET = "b1"
ACCT = "acct-main"
OTHER = "acct-other"
T0 = datetime(2026, 10, 16, 12, 0, 0, tzinfo=UTC)
TX_PATH = rf"/budgets/{BUDGET}/transactions"
ACCOUNTS_PATH = rf"/budgets/{BUDGET}/accounts"


@pytest.fixture
def fake() -> FakeYnab:
    f = FakeYnab()
    f.add_budget(BUDGET, currency="CHF")
    f.add_account(BUDGET, ACCT, "Revolut")
    f.add_account(BUDGET, OTHER, "Savings", type="savings")
    return f


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    c = open_tx_db(tmp_path / "tx.db", default_account_id=ACCT, backup_dir=tmp_path / "bk")
    try:
        yield c
    finally:
        c.close()


def _gets(fake: FakeYnab, path: str) -> list[dict[str, str]]:
    return [c.query for c in fake.calls_matching("GET", path)]


def _row(conn: sqlite3.Connection, tx_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM ynab_transactions WHERE id = ?", (tx_id,)).fetchone()
    assert row is not None, tx_id
    return row


def test_first_sync_asks_for_knowledge_zero_and_stores_deleted_rows_and_splits(fake, conn):
    live = fake.add_transaction(
        BUDGET, ACCT, amount=-12_500, date="2026-10-01", payee_name="Coop", import_id="YNAB:x:1"
    )
    gone = fake.add_transaction(
        BUDGET,
        ACCT,
        amount=-3_000,
        date="2026-10-02",
        payee_name="Kiosk",
        import_id="YNAB:-3000:2026-10-02:1",
    )
    fake.user_delete_transaction(gone)
    split = fake.add_transaction(
        BUDGET,
        OTHER,
        amount=-50_000,
        date="2026-10-03",
        payee_name="Migros",
        subtransactions=[
            {"amount": -30_000, "category_name": "Groceries"},
            {"amount": -20_000, "category_name": "Household"},
        ],
    )
    mirror = MirrorStore(conn)

    result = sync_budget(fake.client(), mirror, BUDGET, now=T0)

    # Both deltas start from 0 so YNAB includes deleted transactions (a plain listing
    # would omit them and a user-deleted row could never become DELETED_IN_YNAB).
    assert _gets(fake, ACCOUNTS_PATH) == [{"last_knowledge_of_server": "0"}]
    assert _gets(fake, TX_PATH) == [{"last_knowledge_of_server": "0"}]
    assert result.full and result.ok
    assert result.tx_changed == 3 and result.accounts_changed == 2
    assert result.server_knowledge == fake.server_knowledge(BUDGET)
    assert mirror.get_server_knowledge(BUDGET) == result.server_knowledge
    assert mirror.get_accounts_knowledge(BUDGET) == result.accounts_knowledge
    assert mirror.get_last_sync_at(BUDGET) == "2026-10-16T12:00:00Z"
    assert _row(conn, gone)["deleted"] == 1
    assert _row(conn, live)["deleted"] == 0
    subs = conn.execute(
        "SELECT amount, category_name FROM ynab_subtransactions WHERE transaction_id = ? "
        "ORDER BY amount",
        (split,),
    ).fetchall()
    assert [tuple(s) for s in subs] == [(-30_000, "Groceries"), (-20_000, "Household")]
    view = mirror.view_for_account(BUDGET, ACCT)
    assert view.by_import_id("YNAB:-3000:2026-10-02:1").deleted
    assert "YNAB:-3000:2026-10-02:1" in view.used_import_ids
    savings = mirror.account(OTHER)
    assert savings is not None and savings.classification == "cash"
    assert savings.budget_id == BUDGET and savings.balance == -50_000
    assert result.currency == "CHF" and result.currency_refreshed


def test_F39_F76_delta_sync_updates_moved_and_redated_rows_in_place(fake, conn):
    tx = fake.add_transaction(BUDGET, ACCT, amount=-8_400, date="2026-08-31", payee_name="Coop")
    mirror = MirrorStore(conn)
    first = sync_budget(fake.client(), mirror, BUDGET, now=T0)
    # F76: a re-date in YNAB reaches the mirror; F39: a move keeps one row (keyed by id).
    fake.user_edit_transaction(tx, date="2026-09-01", account_id=OTHER, cleared="reconciled")
    fake.reset_log()

    second = sync_budget(fake.client(), mirror, BUDGET, now=T0 + timedelta(minutes=5))

    assert _gets(fake, TX_PATH) == [{"last_knowledge_of_server": str(first.server_knowledge)}]
    assert not second.full and second.tx_changed == 1
    rows = conn.execute("SELECT id, account_id, date, cleared FROM ynab_transactions").fetchall()
    assert [tuple(r) for r in rows] == [(tx, OTHER, "2026-09-01", "reconciled")]
    assert mirror.account(ACCT).balance == 0
    assert mirror.account(OTHER).balance == -8_400


def test_split_lines_are_replaced_per_parent_on_a_delta(fake, conn):
    tx = fake.add_transaction(
        BUDGET,
        ACCT,
        amount=-10_000,
        date="2026-10-01",
        subtransactions=[{"amount": -6_000}, {"amount": -4_000}],
    )
    mirror = MirrorStore(conn)
    sync_budget(fake.client(), mirror, BUDGET, now=T0)
    fake.user_delete_transaction(tx)
    sync_budget(fake.client(), mirror, BUDGET, now=T0)
    assert _row(conn, tx)["deleted"] == 1
    assert mirror.get_transaction(tx).has_subtransactions is False


def test_full_sync_refetches_from_zero_and_marks_rows_ynab_no_longer_lists(fake, conn):
    fake.add_transaction(BUDGET, ACCT, amount=-1_000, date="2026-10-01")
    mirror = MirrorStore(conn)
    sync_budget(fake.client(), mirror, BUDGET, now=T0)
    # A row only the mirror has (e.g. from a budget restored in YNAB): a complete listing
    # proves it is gone.
    conn.execute(
        "INSERT INTO ynab_transactions (id, budget_id, account_id, date, amount, cleared, "
        "approved, deleted, synced_at) VALUES ('stray', ?, ?, '2026-09-01', -5, 'cleared', 1, "
        "0, 'x')",
        (BUDGET, ACCT),
    )
    fake.reset_log()

    result = sync_budget(fake.client(), mirror, BUDGET, full=True, now=T0)

    assert result.full
    assert _gets(fake, TX_PATH) == [{"last_knowledge_of_server": "0"}]
    assert _gets(fake, ACCOUNTS_PATH) == [{"last_knowledge_of_server": "0"}]
    assert _row(conn, "stray")["deleted"] == 1
    assert result.currency_refreshed  # a full sync re-checks the currency too


def test_budget_currency_is_refreshed_at_most_once_a_day(fake, conn):
    mirror = MirrorStore(conn)
    settings = rf"/budgets/{BUDGET}/settings"
    first = sync_budget(fake.client(), mirror, BUDGET, now=T0)
    later = sync_budget(fake.client(), mirror, BUDGET, now=T0 + timedelta(hours=23))
    assert first.currency_refreshed and not later.currency_refreshed
    assert later.currency == "CHF"
    assert len(fake.calls_matching("GET", settings)) == 1
    next_day = sync_budget(fake.client(), mirror, BUDGET, now=T0 + CURRENCY_MAX_AGE)
    assert next_day.currency_refreshed
    assert len(fake.calls_matching("GET", settings)) == 2
    assert mirror.budget_currency_checked_at(BUDGET) == format_sync_time(T0 + CURRENCY_MAX_AGE)
    # A clock that went backwards is no reason to trust the old value forever.
    sync_budget(fake.client(), mirror, BUDGET, now=T0 - timedelta(days=3))
    assert len(fake.calls_matching("GET", settings)) == 3


def test_a_failing_currency_refresh_does_not_fail_the_sync(fake, conn):
    mirror = MirrorStore(conn)
    sync_budget(fake.client(), mirror, BUDGET, now=T0)
    fake.fail_next("GET", rf"/budgets/{BUDGET}/settings", 500, times=3)
    result = sync_budget(fake.client(), mirror, BUDGET, now=T0 + timedelta(days=2))
    assert result.ok and result.currency == "CHF" and not result.currency_refreshed
    assert mirror.get_last_sync_at(BUDGET) == format_sync_time(T0 + timedelta(days=2))


def test_a_rejected_token_on_the_currency_refresh_propagates(fake, conn):
    fake.fail_next("GET", rf"/budgets/{BUDGET}/settings", 401)
    with pytest.raises(YnabAuthError):
        sync_budget(fake.client(), MirrorStore(conn), BUDGET, now=T0)


def test_malformed_data_raises_and_the_cursor_does_not_move(fake, conn):
    mirror = MirrorStore(conn)
    sync_budget(fake.client(), mirror, BUDGET, now=T0)
    knowledge = mirror.get_server_knowledge(BUDGET)
    fake.add_transaction(BUDGET, ACCT, amount=-1_000, date="2026-10-05")
    bad = {"data": {"transactions": [{"id": "t", "amount": 1.5}], "server_knowledge": 99}}
    fake.fail_next("GET", TX_PATH, 200, body=bad)
    with pytest.raises(YnabError):
        sync_budget(fake.client(), mirror, BUDGET, now=T0 + timedelta(minutes=1))
    assert mirror.get_server_knowledge(BUDGET) == knowledge
    assert mirror.get_last_sync_at(BUDGET) == format_sync_time(T0)


def test_server_error_propagates_and_last_sync_at_stays(fake, conn):
    mirror = MirrorStore(conn)
    fake.fail_next("GET", TX_PATH, 503, times=3)
    with pytest.raises(YnabServerError):
        sync_budget(fake.client(), mirror, BUDGET, now=T0)
    assert mirror.get_last_sync_at(BUDGET) is None
    assert mirror.get_server_knowledge(BUDGET) is None
    # The accounts part was stored with its own cursor and is not fetched from 0 again.
    assert mirror.get_accounts_knowledge(BUDGET) is not None


# ── sync_and_refresh ────────────────────────────────────────────────────────


def test_F17_non_forced_syncs_are_throttled(fake, conn):
    client = fake.client()
    first = sync_and_refresh(client, conn, BUDGET, ACCT, now=T0)
    assert first.ok
    n = fake.request_count

    skipped = sync_and_refresh(client, conn, BUDGET, ACCT, now=T0 + timedelta(seconds=29))
    assert skipped.skipped and not skipped.ok and skipped.refresh is None
    assert skipped.synced_at == format_sync_time(T0) and skipped.currency == "CHF"
    assert fake.request_count == n  # nothing was requested

    forced = sync_and_refresh(
        client, conn, BUDGET, ACCT, force=True, now=T0 + timedelta(seconds=29)
    )
    assert forced.ok and fake.request_count > n
    n = fake.request_count

    due = sync_and_refresh(client, conn, BUDGET, ACCT, now=T0 + timedelta(seconds=60))
    assert due.ok and fake.request_count > n
    n = fake.request_count

    # The clock moved backwards: the stored time is in the future, so sync anyway.
    back = sync_and_refresh(client, conn, BUDGET, ACCT, now=T0 - timedelta(hours=1))
    assert back.ok and fake.request_count > n


def test_F17_a_rate_limited_sync_returns_a_flag_instead_of_raising(fake, conn):
    client = fake.client()
    sync_and_refresh(client, conn, BUDGET, ACCT, now=T0)
    mirror = MirrorStore(conn)
    knowledge = mirror.get_server_knowledge(BUDGET)
    fake.add_transaction(BUDGET, ACCT, amount=-1_000, date="2026-10-05")
    fake.fail_next("GET", TX_PATH, 429, retry_after=120)

    result = sync_and_refresh(client, conn, BUDGET, ACCT, force=True, now=T0 + timedelta(minutes=1))

    assert result.rate_limited and not result.ok
    assert result.retry_after == 120
    assert result.refresh is None and result.currency == "CHF"
    assert result.synced_at == format_sync_time(T0)  # the previous successful sync
    assert mirror.get_server_knowledge(BUDGET) == knowledge
    assert mirror.get_last_sync_at(BUDGET) == format_sync_time(T0)


def test_F17_a_429_starts_a_backoff_that_only_non_forced_syncs_respect(fake, conn):
    client = fake.client()
    mirror = MirrorStore(conn)
    sync_and_refresh(client, conn, BUDGET, ACCT, now=T0)
    fake.reset_log()
    fake.fail_next("GET", ACCOUNTS_PATH, 429, retry_after=600)

    limited = sync_and_refresh(client, conn, BUDGET, ACCT, now=T0 + timedelta(minutes=1))

    assert limited.rate_limited and fake.request_count == 1
    assert rate_limited_until(mirror, BUDGET, now=T0) == T0 + timedelta(minutes=11)
    # While it runs, /status and dashboard refreshes send nothing (they used to send one
    # request each, keeping the token's quota exhausted).
    for minute in (2, 5, 10):
        quiet = sync_and_refresh(client, conn, BUDGET, ACCT, now=T0 + timedelta(minutes=minute))
        assert quiet.rate_limited and not quiet.skipped and quiet.refresh is None
        assert quiet.retry_after == (11 - minute) * 60
        assert quiet.synced_at == format_sync_time(T0) and quiet.currency == "CHF"
    assert fake.request_count == 1
    # A forced sync (an import the user started) still tries; a success ends the backoff.
    forced = sync_and_refresh(client, conn, BUDGET, ACCT, force=True, now=T0 + timedelta(minutes=6))
    assert forced.ok and fake.request_count > 1
    assert rate_limited_until(mirror, BUDGET, now=T0 + timedelta(minutes=6)) is None
    n = fake.request_count
    later = sync_and_refresh(client, conn, BUDGET, ACCT, now=T0 + timedelta(minutes=7))
    assert later.ok and fake.request_count > n


def test_F17_the_rate_limit_backoff_has_a_default_and_a_cap(conn):
    mirror = MirrorStore(conn)
    default = T0 + timedelta(seconds=DEFAULT_RATE_LIMIT_BACKOFF)
    assert note_rate_limited(mirror, BUDGET, None, now=T0) == default
    assert note_rate_limited(mirror, BUDGET, 0, now=T0) == default
    assert note_rate_limited(mirror, BUDGET, 1.2, now=T0) == T0 + timedelta(seconds=2)
    capped = note_rate_limited(mirror, BUDGET, 10**9, now=T0)
    assert capped == T0 + timedelta(seconds=MAX_RATE_LIMIT_BACKOFF)
    assert rate_limited_until(mirror, BUDGET, now=T0) == capped
    assert rate_limited_until(mirror, BUDGET, now=capped) is None  # over
    # A clock that went backwards cannot silence syncs for longer than the cap.
    assert rate_limited_until(mirror, BUDGET, now=T0 - timedelta(days=1)) is None
    assert rate_limited_until(mirror, "other-budget", now=T0) is None


def test_sync_and_refresh_lets_a_rejected_token_through(fake, conn):
    fake.fail_next("GET", ACCOUNTS_PATH, 401)
    with pytest.raises(YnabAuthError):
        sync_and_refresh(fake.client(), conn, BUDGET, ACCT, now=T0)


def test_sync_and_refresh_reconciles_the_ledger_with_the_mirror(fake, conn):
    key = "YNAB:-12500:2026-10-01:1"
    tx = fake.add_transaction(
        BUDGET, ACCT, amount=-12_500, date="2026-10-01", import_id=key, cleared="uncleared"
    )
    ledger = LedgerStore(conn)
    ledger.insert(
        LedgerEntry(
            account_id=ACCT,
            key=key,
            ynab_import_id=key,
            status=LedgerStatus.UNLINKED.value,
            csv_date="2026-10-01",
            csv_amount=-12_500,
            csv_state="PENDING",
            identity="legacy",
        )
    )

    result = sync_and_refresh(fake.client(), conn, BUDGET, ACCT, now=T0)

    assert result.refresh is not None and result.refresh.linked == 1
    linked = ledger.get(ACCT, key)
    assert linked.status == LedgerStatus.ACTIVE and linked.ynab_tx_id == tx
    assert linked.pushed_cleared == "uncleared"


def test_sync_and_refresh_without_an_account_only_syncs(fake, conn):
    result = sync_and_refresh(fake.client(), conn, BUDGET, None, now=T0)
    assert result.ok and result.refresh is None


def test_sync_result_to_dict_is_json_safe(fake, conn):
    result = sync_and_refresh(fake.client(), conn, BUDGET, ACCT, now=T0)
    data = json.loads(json.dumps(result.to_dict()))
    assert data["ok"] is True and data["full"] is True
    assert data["refresh"] == {
        "import_ids_corrected": 0,
        "linked": 0,
        "marked_deleted": 0,
        "conflicts": [],
    }
    assert SyncResult(BUDGET, skipped=True).to_dict()["ok"] is False


def test_sync_times_round_trip_and_bad_values_are_ignored():
    assert parse_sync_time(format_sync_time(T0)) == T0
    assert parse_sync_time("garbage") is None
    assert parse_sync_time(None) is None
    with pytest.raises(ValueError):
        format_sync_time(datetime(2026, 1, 1))  # naive: ambiguous
