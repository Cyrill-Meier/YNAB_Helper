"""Tests for ynab_helper.balance.reconcile (DESIGN §6.6; findings F04, F27)."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FakeYnab
from tests.maintenance.helpers import (
    ACCOUNT,
    BUDGET,
    add_entry,
    make_fake,
    mutating_calls,
    open_db,
    record_statement,
    sync_mirror,
)
from tests.revolut.csvbuild import make_csv, row
from ynab_helper.balance import reconcile
from ynab_helper.balance.reconcile import (
    ReconcilePreview,
    ReconcileTokens,
    ReconcileUnavailable,
    StatementUnavailable,
    reconcile_apply,
    reconcile_preview,
)
from ynab_helper.errors import StaleStateError, YnabHelperError

TODAY = date(2026, 10, 5)

# Completed Date order, a pending hotel hold interleaved (Revolut's real layout).
ROWS = [
    row(
        type="Topup",
        started="2026-10-01 07:45:10",
        completed="2026-10-01 07:46:10",
        desc="Top-Up",
        amount="500.00",
        balance="3000.00",
    ),
    row(
        started="2026-10-01 12:03:44",
        completed="2026-10-02 06:12:09",
        desc="Coop",
        amount="-8.40",
        balance="2991.60",
    ),
    row(
        type="Transfer",
        started="2026-10-02 09:00:00",
        completed="2026-10-02 09:00:02",
        desc="To Anna",
        amount="-120.00",
        balance="2871.60",
    ),
    row(
        started="2026-10-02 18:00:00",
        completed="",
        desc="Hotel",
        amount="-150.00",
        state="PENDING",
        balance="",
    ),
]
CSV = make_csv(*ROWS)
AS_OF = "2026-10-02"
CSV_BALANCE = 2_871_600


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@dataclass
class Env:
    fake: FakeYnab
    conn: sqlite3.Connection
    tmp: Path
    clock: Clock
    tokens: ReconcileTokens

    @property
    def client(self):
        return self.fake.client()

    def sync(self) -> None:
        sync_mirror(self.conn, self.fake)

    def statement(self, content: bytes = CSV, **kwargs: Any) -> int:
        file_id, _ = record_statement(self.conn, self.tmp / "csv", content, **kwargs)
        return file_id

    def preview(self, **kwargs: Any) -> ReconcilePreview:
        kwargs.setdefault("budget_currency", "CHF")
        kwargs.setdefault("now", TODAY)
        kwargs.setdefault("tokens", self.tokens)
        client = kwargs.pop("client", self.client)
        return reconcile_preview(self.conn, client, budget_id=BUDGET, account_id=ACCOUNT, **kwargs)

    def apply(self, token: str | None, **kwargs: Any):
        kwargs.setdefault("budget_currency", "CHF")
        kwargs.setdefault("tokens", self.tokens)
        kwargs.setdefault("account_id", ACCOUNT)
        return reconcile_apply(self.conn, self.client, budget_id=BUDGET, token=token, **kwargs)


def seed_imported(fake: FakeYnab, conn: sqlite3.Connection, *, skip_transfer: bool = False):
    """YNAB state after importing CSV on top of a reconciled 2,500.00 opening balance."""
    fake.add_transaction(
        BUDGET, ACCOUNT, id="t-open", amount=2_500_000, date="2026-09-01", cleared="reconciled"
    )
    rows = [
        ("t-topup", "YNAB:500000:2026-10-01:1", 500_000, "2026-10-01", "cleared", "COMPLETED"),
        ("t-coop", "YNAB:-8400:2026-10-01:1", -8_400, "2026-10-01", "cleared", "COMPLETED"),
        ("t-anna", "YNAB:-120000:2026-10-02:1", -120_000, "2026-10-02", "cleared", "COMPLETED"),
        ("t-hotel", "YNAB:-150000:2026-10-02:1", -150_000, "2026-10-02", "uncleared", "PENDING"),
    ]
    for tx_id, key, amount, day, cleared, state in rows:
        if skip_transfer and tx_id == "t-anna":
            continue
        fake.add_transaction(
            BUDGET, ACCOUNT, id=tx_id, amount=amount, date=day, cleared=cleared, import_id=key
        )
        add_entry(
            conn,
            key,
            ynab_tx_id=tx_id,
            csv_date=day,
            csv_amount=amount,
            csv_state=state,
            pushed_cleared="cleared" if cleared == "cleared" else "uncleared",
        )


@pytest.fixture
def env(tmp_path: Path) -> Iterator[Env]:
    conn = open_db(tmp_path)
    clock = Clock()
    try:
        yield Env(make_fake(), conn, tmp_path, clock, ReconcileTokens(clock=clock))
    finally:
        conn.close()


@pytest.fixture
def ready(env: Env) -> Env:
    """Imported statement; YNAB lacks the -120.00 transfer, so delta = -120.00."""
    seed_imported(env.fake, env.conn, skip_transfer=True)
    env.statement()
    env.sync()
    return env


def all_user_text(*items: Any) -> str:
    texts = []
    for item in items:
        if isinstance(item, YnabHelperError):
            texts.append(item.user_message)
        elif isinstance(item, ReconcilePreview):
            texts.extend(item.problems)
        elif isinstance(item, str):
            texts.append(item)
    return "\n".join(texts)


# ── happy paths ────────────────────────────────────────────────────────────


def test_in_sync_preview_has_no_token(env):
    seed_imported(env.fake, env.conn)
    env.statement()
    env.sync()
    preview = env.preview()
    assert preview.csv_balance == CSV_BALANCE
    assert preview.as_of == AS_OF
    assert preview.ynab_cleared_now == CSV_BALANCE
    assert preview.ynab_cleared_as_of == CSV_BALANCE
    assert preview.delta == 0
    assert preview.in_sync
    assert preview.token is None
    assert preview.problems == []
    assert len(env.tokens) == 0


def test_preview_then_apply_posts_one_reconciled_adjustment(ready):
    preview = ready.preview()
    assert preview.delta == -120_000
    assert preview.ynab_cleared_as_of == CSV_BALANCE + 120_000
    assert preview.currency == "CHF"
    assert preview.csv_name.startswith("account-statement_")
    assert preview.token and preview.token_ttl_seconds == reconcile.RECONCILE_TOKEN_TTL
    assert mutating_calls(ready.fake) == []

    result = ready.apply(preview.token)
    assert result.outcome == "created"
    adj = result.adjustment
    assert adj.import_id == "RECON:-120000:2026-10-02:1"
    tx = ready.fake.tx(adj.ynab_tx_id)
    assert tx["amount"] == -120_000
    assert tx["date"] == AS_OF
    assert tx["payee_name"] == "CSV Reconciliation"
    assert tx["cleared"] == "reconciled"
    assert tx["memo"] == "Reconciled to CSV running balance 2,871.60 CHF as of 2026-10-02"
    assert len(mutating_calls(ready.fake)) == 1
    assert "lock" not in all_user_text(preview, tx["memo"]).lower()


def test_apply_reports_duplicate_when_ynab_already_holds_the_import_id(ready):
    deleted = ready.fake.add_transaction(
        BUDGET,
        ACCOUNT,
        amount=-120_000,
        date=AS_OF,
        cleared="reconciled",
        import_id="RECON:-120000:2026-10-02:1",
    )
    ready.fake.user_delete_transaction(deleted)
    ready.sync()
    result = ready.apply(ready.preview().token)
    assert result.outcome == "duplicate"


# ── F04: stale / unimported statements, as-of computation ───────────────────


def test_F04_cleared_rows_after_as_of_are_excluded(env):
    seed_imported(env.fake, env.conn)
    # Newer activity, already cleared in YNAB, that the statement cannot know about.
    env.fake.add_transaction(BUDGET, ACCOUNT, amount=-50_000, date="2026-10-04", cleared="cleared")
    env.statement()
    env.sync()
    preview = env.preview()
    assert preview.ynab_cleared_now == CSV_BALANCE - 50_000
    assert preview.cleared_after_as_of == -50_000
    assert preview.ynab_cleared_as_of == CSV_BALANCE
    assert preview.delta == 0  # v1 compared with the current balance and posted +50.00
    assert preview.token is None
    assert any("after 2026-10-02" in p for p in preview.problems)


def test_F04_older_backfill_statement_reconciles_as_of_its_own_date(env):
    """v1 posted -4,950.00 'reconciled' when the newest upload was an old statement."""
    old_csv = make_csv(
        row(
            type="Topup",
            started="2026-03-30 10:00:00",
            completed="2026-03-31 10:00:00",
            desc="Top-Up",
            amount="100.00",
            balance="4200.00",
        )
    )
    env.fake.add_transaction(
        BUDGET, ACCOUNT, amount=4_100_000, date="2026-03-01", cleared="reconciled"
    )
    env.fake.add_transaction(BUDGET, ACCOUNT, amount=100_000, date="2026-03-30", cleared="cleared")
    env.fake.add_transaction(
        BUDGET, ACCOUNT, amount=4_950_000, date="2026-08-15", cleared="cleared"
    )
    env.statement(old_csv, name="account-statement_2026-01-01_2026-03-31_en_x.csv")
    env.sync()
    preview = env.preview()
    assert preview.ynab_cleared_now == 9_150_000
    assert preview.as_of == "2026-03-31"
    assert preview.ynab_cleared_as_of == 4_200_000
    assert preview.delta == 0
    assert any("days ago" in p for p in preview.problems)


@pytest.mark.parametrize("status", ["failed", "partial", "stored", "legacy"])
def test_F04_newest_statement_must_be_imported(ready, status):
    ready.statement(status=status, uploaded_at="2026-10-16T12:00:00Z")
    with pytest.raises(StatementUnavailable) as info:
        ready.preview()
    assert info.value.reason == "not_imported"
    assert "account-statement_" in info.value.user_message
    assert mutating_calls(ready.fake) == []


def test_F04_older_imported_statement_is_not_used_as_fallback(ready):
    ready.statement(status="failed", uploaded_at="2026-10-17T12:00:00Z")
    with pytest.raises(StatementUnavailable):
        ready.preview()


def test_no_statement(env):
    env.sync()
    with pytest.raises(StatementUnavailable) as info:
        env.preview()
    assert info.value.reason == "no_statement"


def test_statement_of_another_account_is_not_used(env):
    env.statement(account_id="acct-other")
    env.sync()
    with pytest.raises(StatementUnavailable) as info:
        env.preview()
    assert info.value.reason == "no_statement"


def test_missing_statement_file(env):
    _file_id, path = record_statement(env.conn, env.tmp / "csv", CSV)
    path.unlink()
    env.sync()
    with pytest.raises(StatementUnavailable) as info:
        env.preview()
    assert info.value.reason == "missing_file"


def test_statement_file_changed_on_disk(env):
    _, path = record_statement(env.conn, env.tmp / "csv", CSV)
    path.write_bytes(CSV.replace(b"2871.60", b"9871.60"))
    env.sync()
    with pytest.raises(StatementUnavailable) as info:
        env.preview()
    assert info.value.reason == "file_changed"


def test_currency_must_match_budget(env):
    env.statement(make_csv(*([*r[:7], "EUR", *r[8:]] for r in ROWS)))
    env.sync()
    with pytest.raises(ReconcileUnavailable) as info:
        env.preview()
    assert info.value.reason == "currency_mismatch"
    assert "EUR" in info.value.user_message and "CHF" in info.value.user_message


def test_unknown_budget_currency_is_refused(ready):
    with pytest.raises(ReconcileUnavailable) as info:
        ready.preview(budget_currency=None)
    assert info.value.reason == "currency_unknown"


def test_latest_completed_row_without_balance_is_refused(env):
    rows = [
        *ROWS[:2],
        row(started="2026-10-02 09:00:00", completed="2026-10-03 09:00:00", balance=""),
    ]
    env.statement(make_csv(*rows))
    env.sync()
    with pytest.raises(StatementUnavailable) as info:
        env.preview()
    assert info.value.reason == "no_balance"
    assert "data row 3" in info.value.user_message


def test_statement_without_completed_rows(env):
    env.statement(make_csv(ROWS[3]))
    env.sync()
    with pytest.raises(StatementUnavailable) as info:
        env.preview()
    assert info.value.reason == "no_completed_rows"


def test_completed_row_with_unreadable_completed_date_is_refused(env):
    rows = [*ROWS[:2], row(started="2026-10-02 09:00:00", completed="garbage", balance="1.00")]
    env.statement(make_csv(*rows))
    env.sync()
    with pytest.raises(StatementUnavailable) as info:
        env.preview()
    assert info.value.reason == "undated_completed_row"


def test_account_missing_from_mirror(env):
    env.statement()
    with pytest.raises(ReconcileUnavailable) as info:
        env.preview(client=None)
    assert info.value.reason == "account_unknown"


# ── F04: statements older than what YNAB already reflects (review) ─────────

OCTOBER = make_csv(
    row(
        started="2026-10-05 10:00:00",
        completed="2026-10-10 10:00:00",
        desc="Rent",
        amount="-100.00",
        balance="900.00",
    )
)
SEPTEMBER = make_csv(
    row(
        started="2026-09-29 10:00:00",
        completed="2026-09-30 10:00:00",
        desc="Coffee",
        amount="-1.00",
        balance="1000.00",
    )
)
SEPTEMBER_NAME = "account-statement_2026-09-01_2026-09-30_en_x.csv"


def seed_with_stray_credit(env: Env) -> None:
    """True balance 1,000.00 at 09-30 and 900.00 at 10-10; YNAB has a stray +20.00 cleared."""
    env.fake.add_transaction(
        BUDGET, ACCOUNT, amount=1_001_000, date="2026-09-01", cleared="reconciled"
    )
    env.fake.add_transaction(BUDGET, ACCOUNT, amount=20_000, date="2026-09-20", cleared="cleared")
    env.fake.add_transaction(BUDGET, ACCOUNT, amount=-1_000, date="2026-09-29", cleared="cleared")
    env.fake.add_transaction(BUDGET, ACCOUNT, amount=-100_000, date="2026-10-05", cleared="cleared")


def cleared_balance(env: Env) -> int:
    return env.fake.balances(BUDGET, ACCOUNT)[1]


def test_F04_backfill_after_a_newer_reconcile_is_refused(env):
    """Reconciling October then a September backfill posted the same -20.00 twice (880.00)."""
    seed_with_stray_credit(env)
    env.statement(OCTOBER, uploaded_at="2026-10-11T10:00:00Z")
    env.sync()
    first = env.preview(now=date(2026, 10, 11))
    assert first.delta == -20_000
    assert env.apply(first.token).outcome == "created"
    assert cleared_balance(env) == 900_000

    env.statement(SEPTEMBER, name=SEPTEMBER_NAME, uploaded_at="2026-10-12T10:00:00Z")
    env.sync()
    with pytest.raises(ReconcileUnavailable) as info:
        env.preview(now=date(2026, 10, 12))
    assert info.value.reason == "reconciled_after_as_of"
    assert "2026-10-10" in info.value.user_message
    assert "lock" not in info.value.user_message.lower()
    assert len(mutating_calls(env.fake)) == 1
    assert cleared_balance(env) == 900_000
    assert len(env.tokens) == 0


def test_F04_backfill_older_than_imported_rows_is_refused(env):
    """A statement ending before rows the ledger already holds is a backfill: refused."""
    seed_with_stray_credit(env)
    add_entry(
        env.conn,
        "YNAB:-100000:2026-10-05:1",
        ynab_tx_id="t-rent",
        csv_date="2026-10-05",
        csv_amount=-100_000,
    )
    env.statement(SEPTEMBER, name=SEPTEMBER_NAME, uploaded_at="2026-10-12T10:00:00Z")
    env.sync()
    with pytest.raises(ReconcileUnavailable) as info:
        env.preview(now=date(2026, 10, 12))
    assert info.value.reason == "older_statement"
    assert "2026-10-05" in info.value.user_message
    assert "lock" not in info.value.user_message.lower()
    assert len(env.tokens) == 0


@pytest.mark.parametrize(
    ("newest_imported", "refused"), [("2026-10-03", False), ("2026-10-04", True)]
)
def test_F04_older_statement_rule_allows_one_day_of_timezone_slack(env, newest_imported, refused):
    seed_imported(env.fake, env.conn)  # statement rows end 2026-10-02
    add_entry(env.conn, "YNAB:-1000:later:1", ynab_tx_id="t-later", csv_date=newest_imported)
    env.statement()
    env.sync()
    if refused:
        with pytest.raises(ReconcileUnavailable) as info:
            env.preview()
        assert info.value.reason == "older_statement"
    else:
        assert env.preview().in_sync


def test_F04_in_app_ynab_reconcile_after_as_of_is_refused(env):
    """The YNAB app's adjustment (dated after as_of) was excluded and corrected again (1,050)."""
    env.fake.add_transaction(BUDGET, ACCOUNT, amount=950_000, date="2026-10-01", cleared="cleared")
    env.statement(
        make_csv(
            row(
                started="2026-10-14 10:00:00",
                completed="2026-10-14 10:00:01",
                amount="-0.00",
                balance="1000.00",
            )
        ),
        uploaded_at="2026-10-14T12:00:00Z",
    )
    env.fake.add_transaction(
        BUDGET,
        ACCOUNT,
        amount=50_000,
        date="2026-10-16",
        cleared="reconciled",
        payee_name="Reconciliation Balance Adjustment",
    )
    env.sync()
    with pytest.raises(ReconcileUnavailable) as info:
        env.preview(now=date(2026, 10, 16))
    assert info.value.reason == "reconciled_after_as_of"
    assert "2026-10-16" in info.value.user_message
    assert mutating_calls(env.fake) == []
    assert cleared_balance(env) == 1_000_000


@pytest.mark.parametrize(
    "fields",
    [
        {"cleared": "reconciled", "payee_name": "Migros"},
        {"cleared": "cleared", "import_id": "RECON:-20000:2026-10-04:1"},
        {"cleared": "cleared", "payee_name": "Reconciliation Balance Adjustment"},
        {"cleared": "cleared", "payee_name": "CSV Reconciliation"},
    ],
)
def test_F04_correction_after_as_of_blocks_reconcile(env, fields):
    seed_imported(env.fake, env.conn)
    env.fake.add_transaction(BUDGET, ACCOUNT, amount=-20_000, date="2026-10-04", **fields)
    env.statement()
    env.sync()
    with pytest.raises(ReconcileUnavailable) as info:
        env.preview()
    assert info.value.reason == "reconciled_after_as_of"


def test_F04_correction_after_the_preview_blocks_apply(ready):
    preview = ready.preview()
    ready.fake.add_transaction(
        BUDGET,
        ACCOUNT,
        amount=-120_000,
        date="2026-10-04",
        cleared="reconciled",
        payee_name="Reconciliation Balance Adjustment",
    )
    ready.sync()
    with pytest.raises(ReconcileUnavailable) as info:
        ready.apply(preview.token)
    assert info.value.reason == "reconciled_after_as_of"
    assert mutating_calls(ready.fake) == []


def test_F04_deleted_correction_after_as_of_does_not_block(env):
    seed_imported(env.fake, env.conn)
    tx = env.fake.add_transaction(
        BUDGET, ACCOUNT, amount=-20_000, date="2026-10-04", cleared="reconciled"
    )
    env.fake.user_delete_transaction(tx)
    env.statement()
    env.sync()
    assert env.preview().in_sync


def test_F04_cleared_rows_after_as_of_warn_about_double_correction(env):
    seed_imported(env.fake, env.conn)
    env.fake.add_transaction(BUDGET, ACCOUNT, amount=-50_000, date="2026-10-04", cleared="cleared")
    env.statement()
    env.sync()
    (problem,) = [p for p in env.preview().problems if "after 2026-10-02" in p]
    assert "second time" in problem


def test_reconciling_the_same_statement_again_is_in_sync(ready):
    """The bot's own adjustment is dated as_of (not after), so it never blocks a re-run."""
    ready.apply(ready.preview().token)
    ready.sync()
    again = ready.preview()
    assert again.in_sync and again.token is None
    assert len(mutating_calls(ready.fake)) == 1


def test_newer_statement_after_a_reconcile_is_allowed(env):
    seed_with_stray_credit(env)
    env.statement(SEPTEMBER, name=SEPTEMBER_NAME, uploaded_at="2026-10-01T10:00:00Z")
    env.sync()
    first = env.preview(now=date(2026, 10, 1))
    assert first.delta == -20_000 and first.as_of == "2026-09-30"
    env.apply(first.token)
    env.sync()
    env.statement(OCTOBER, uploaded_at="2026-10-11T10:00:00Z")
    env.sync()
    second = env.preview(now=date(2026, 10, 11))
    assert second.in_sync
    assert cleared_balance(env) == 900_000


# ── F27: which row carries the balance ──────────────────────────────────────


def test_F27_latest_completed_row_wins_over_file_order(env):
    out_of_order = make_csv(
        row(
            started="2026-10-10 10:00:00",
            completed="2026-10-13 08:00:00",
            desc="Card hold",
            amount="-200.00",
            balance="750.00",
        ),
        row(
            type="Transfer",
            started="2026-10-11 09:00:00",
            completed="2026-10-12 09:00:00",
            desc="Transfer",
            amount="-50.00",
            balance="950.00",
        ),
    )
    env.fake.add_transaction(BUDGET, ACCOUNT, amount=750_000, date="2026-10-01", cleared="cleared")
    env.statement(out_of_order)
    env.sync()
    preview = env.preview(now=date(2026, 10, 14))
    assert preview.csv_balance == 750_000  # v1 took the last row: 950.00, and posted +200
    assert preview.as_of == "2026-10-13"
    assert preview.delta == 0


def test_completed_date_ties_go_to_the_last_row_in_file(env):
    ties = make_csv(
        row(started="2026-10-02 08:00:00", completed="2026-10-02 09:00:00", balance="100.00"),
        row(started="2026-10-02 08:30:00", completed="2026-10-02 09:00:00", balance="99.00"),
    )
    env.statement(ties)
    env.sync()
    assert env.preview().csv_balance == 99_000


def test_other_products_never_provide_the_balance(env):
    rows = [
        *ROWS,
        row(
            product="Savings",
            started="2026-10-03 10:00:00",
            completed="2026-10-03 10:00:01",
            balance="0.00",
        ),
    ]
    seed_imported(env.fake, env.conn)
    env.statement(make_csv(*rows))
    env.sync()
    preview = env.preview()
    assert preview.csv_balance == CSV_BALANCE
    assert preview.in_sync


# ── tokens ─────────────────────────────────────────────────────────────────


def test_token_expires_after_ten_minutes(ready):
    preview = ready.preview()
    ready.clock.now += reconcile.RECONCILE_TOKEN_TTL + 1
    with pytest.raises(StaleStateError):
        ready.apply(preview.token)
    assert mutating_calls(ready.fake) == []


def test_token_is_single_use(ready):
    preview = ready.preview()
    ready.apply(preview.token)
    with pytest.raises(StaleStateError):
        ready.apply(preview.token)
    assert len(mutating_calls(ready.fake)) == 1


def test_unknown_token(ready):
    with pytest.raises(StaleStateError):
        ready.apply("not-a-token")
    with pytest.raises(StaleStateError):
        ready.apply(None)


def test_token_is_bound_to_the_account(ready):
    preview = ready.preview()
    with pytest.raises(StaleStateError):
        ready.apply(preview.token, account_id="acct-other")
    assert mutating_calls(ready.fake) == []


def test_new_preview_revokes_the_previous_token(ready):
    first = ready.preview()
    second = ready.preview()
    with pytest.raises(StaleStateError):
        ready.apply(first.token)
    assert ready.apply(second.token).outcome == "created"


def test_apply_refuses_when_ynab_changed_after_the_preview(ready):
    preview = ready.preview()
    ready.fake.add_transaction(BUDGET, ACCOUNT, amount=-7_000, date="2026-10-01", cleared="cleared")
    ready.sync()
    with pytest.raises(StaleStateError) as info:
        ready.apply(preview.token)
    assert "nothing was posted" in info.value.user_message
    assert mutating_calls(ready.fake) == []


def test_apply_refuses_when_a_newer_statement_arrived(ready):
    preview = ready.preview()
    ready.statement(uploaded_at="2026-10-16T12:00:00Z")  # same content, new csv_files row
    with pytest.raises(StaleStateError):
        ready.apply(preview.token)
    assert mutating_calls(ready.fake) == []


def test_apply_refuses_when_the_mirror_is_behind_ynab(ready):
    preview = ready.preview()
    # Changed in YNAB but not synced: the mirror still matches the token, YNAB does not.
    ready.fake.add_transaction(BUDGET, ACCOUNT, amount=-7_000, date="2026-10-01", cleared="cleared")
    with pytest.raises(StaleStateError):
        ready.apply(preview.token)
    assert mutating_calls(ready.fake) == []


def test_preview_with_client_checks_the_mirror(ready):
    ready.fake.add_transaction(BUDGET, ACCOUNT, amount=-7_000, date="2026-10-01", cleared="cleared")
    with pytest.raises(StaleStateError):
        ready.preview()
    assert ready.preview(client=None).delta == -120_000  # mirror-only preview


def test_token_registry_is_bounded():
    clock = Clock()
    tokens = ReconcileTokens(clock=clock, max_entries=2)
    base = dict(
        budget_id="b",
        csv_file_id=1,
        csv_sha256="0" * 64,
        as_of="2026-10-01",
        csv_balance=1,
        ynab_cleared_as_of=0,
        delta=1,
        currency="CHF",
    )
    issued = [tokens.issue(reconcile._Binding(account_id=f"a{i}", **base)) for i in range(3)]
    assert len(tokens) == 2
    with pytest.raises(StaleStateError):
        tokens.consume(issued[0])
    assert tokens.consume(issued[2]).account_id == "a2"


# ── problems shown with the preview ─────────────────────────────────────────


def test_uncleared_manual_transaction_before_as_of_is_flagged(ready):
    ready.fake.add_transaction(
        BUDGET, ACCOUNT, amount=-120_000, date="2026-10-02", cleared="uncleared", payee_name="Anna"
    )
    ready.sync()
    preview = ready.preview()
    assert any("uncleared" in p and "twice" in p for p in preview.problems)


def test_known_revolut_pending_is_not_flagged(env):
    seed_imported(env.fake, env.conn)  # the hotel hold is uncleared and PENDING in the ledger
    env.statement()
    env.sync()
    assert env.preview().problems == []


def test_pending_at_revolut_but_cleared_in_ynab_is_flagged(env):
    seed_imported(env.fake, env.conn)
    env.fake.user_edit_transaction("t-hotel", cleared="cleared")
    env.statement()
    env.sync()
    preview = env.preview()
    assert preview.delta == 150_000
    assert any("still pending at Revolut" in p for p in preview.problems)


def test_closed_account_is_flagged(ready):
    ready.fake.edit_account(BUDGET, ACCOUNT, closed=True)
    ready.sync()
    assert "The YNAB account is closed." in ready.preview().problems


def test_no_message_claims_other_transactions_get_locked(ready, tmp_path):
    texts: list[Any] = [ready.preview()]
    for status in ("failed", "partial", "stored", "legacy"):
        conn = open_db(tmp_path / status)
        try:
            record_statement(conn, tmp_path / status / "csv", CSV, status=status)
            with pytest.raises(StatementUnavailable) as info:
                reconcile.load_imported_statement(conn, ACCOUNT)
            texts.append(info.value)
        finally:
            conn.close()
    assert "lock" not in all_user_text(*texts).lower()


def test_preview_to_dict(ready):
    data = ready.preview().to_dict()
    assert data["delta"] == -120_000
    assert data["in_sync"] is False
    assert data["token"]
