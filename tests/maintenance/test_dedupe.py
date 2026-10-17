"""Tests for ynab_helper.maintenance.dedupe (DESIGN §6.7; findings F13, F20, F44, F46)."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FakeYnab
from tests.maintenance.helpers import (
    ACCOUNT,
    BUDGET,
    OTHER,
    add_entry,
    make_fake,
    mutating_calls,
    open_db,
    record_statement,
    sync_mirror,
)
from tests.revolut.csvbuild import make_csv, row
from ynab_helper.balance.reconcile import StatementUnavailable
from ynab_helper.errors import StaleStateError, YnabAuthError
from ynab_helper.maintenance import dedupe
from ynab_helper.maintenance.dedupe import (
    DedupeDeleteResult,
    DedupeScan,
    DedupeScans,
    DedupeSelectionError,
    dedupe_delete,
    dedupe_scan,
)
from ynab_helper.storage import LedgerStore

# The file name claims 2020..2026; the rows only cover 2026-10-01..2026-10-05 (F46).
WIDE_NAME = "account-statement_2020-01-01_2026-12-31_en_abc.csv"
CSV = make_csv(
    row(started="2026-10-01 09:00:00", completed="2026-10-01 09:00:01", amount="-8.40"),
    row(started="2026-10-03 09:00:00", completed="2026-10-03 09:00:01", amount="-300.00"),
    row(started="2026-10-05 09:00:00", completed="2026-10-05 09:00:01", amount="-1.00"),
)
USER = "42"


class Clock:
    def __init__(self) -> None:
        self.now = 500.0

    def __call__(self) -> float:
        return self.now


@dataclass
class Env:
    fake: FakeYnab
    conn: sqlite3.Connection
    tmp: Path
    clock: Clock
    scans: DedupeScans

    def sync(self) -> None:
        sync_mirror(self.conn, self.fake)

    def scan(self, user: str = USER, account_id: str = ACCOUNT) -> DedupeScan:
        return dedupe_scan(
            self.conn, budget_id=BUDGET, account_id=account_id, user_key=user, registry=self.scans
        )

    def delete(self, scan_id: str, ids: Any, **kwargs: Any) -> DedupeDeleteResult:
        kwargs.setdefault("user_key", USER)
        kwargs.setdefault("account_id", ACCOUNT)
        return dedupe_delete(
            self.conn,
            self.fake.client(),
            budget_id=BUDGET,
            scan_id=scan_id,
            ids=ids,
            registry=self.scans,
            **kwargs,
        )

    def deletes(self) -> list[str]:
        return [c.path.rsplit("/", 1)[1] for c in mutating_calls(self.fake) if c.method == "DELETE"]


def seed(fake: FakeYnab, conn: sqlite3.Connection) -> None:
    def tx(tx_id: str, day: str, import_id: str | None, account: str = ACCOUNT, **kw: Any) -> None:
        fake.add_transaction(
            BUDGET, account, id=tx_id, amount=-1000, date=day, import_id=import_id, **kw
        )

    tx("linked", "2026-10-01", "YNAB:-8400:2026-10-01:1")
    add_entry(conn, "YNAB:-8400:2026-10-01:1", ynab_tx_id="linked")
    # F13: re-dated by a later export; YNAB keeps the old import_id, the ledger links it.
    tx("rekeyed", "2026-10-03", "YNAB:-300000:2026-10-02:1")
    add_entry(
        conn,
        "YNAB:-300000:2026-10-03:1",
        ynab_import_id="YNAB:-300000:2026-10-02:1",
        ynab_tx_id="rekeyed",
        csv_date="2026-10-03",
    )
    add_entry(
        conn,
        "YNAB:-300000:2026-10-02:1",
        status="REKEYED",
        rekeyed_to="YNAB:-300000:2026-10-03:1",
        ynab_tx_id="rekeyed",
    )
    tx("orphan1", "2026-10-02", "YNAB:-4500:2026-10-02:2", payee_name="Starbucks")
    tx("orphan2", "2026-10-04", "YNAB:-9900:2026-10-04:1", payee_name="Amazon")
    tx("manual", "2026-10-02", None)
    tx("crypto", "2026-10-02", "CRYPTO:5000:2026-10-02:1")
    tx("bankimport", "2026-10-02", "ynab_manual:abc")
    tx("old", "2025-06-01", "YNAB:-1000:2025-06-01:1")  # inside the filename window only
    tx("gone", "2026-10-02", "YNAB:-2000:2026-10-02:1", deleted=True)
    tx("elsewhere", "2026-10-02", "YNAB:-3000:2026-10-02:1", account=OTHER)
    # Moved here from another account whose ledger still links it.
    tx("moved", "2026-10-02", "YNAB:-3100:2026-10-02:1")
    add_entry(conn, "YNAB:-3100:2026-10-02:1", account_id=OTHER, ynab_tx_id="moved")
    # UNLINKED entry still being resolved: its import_id is ours.
    tx("pending_link", "2026-10-02", "YNAB:-3200:2026-10-02:1")
    add_entry(conn, "YNAB:-3200:2026-10-02:1", status="UNLINKED")


@pytest.fixture
def env(tmp_path: Path) -> Iterator[Env]:
    conn = open_db(tmp_path)
    clock = Clock()
    fake = make_fake()
    seed(fake, conn)
    e = Env(fake, conn, tmp_path, clock, DedupeScans(clock=clock))
    record_statement(conn, tmp_path / "csv", CSV, name=WIDE_NAME)
    e.sync()
    try:
        yield e
    finally:
        conn.close()


# ── scan ───────────────────────────────────────────────────────────────────


def test_F13_scan_lists_only_unaccounted_importer_rows(env):
    scan = env.scan()
    assert [c.id for c in scan.candidates] == ["orphan1", "orphan2"]
    assert scan.candidates[0].import_id == "YNAB:-4500:2026-10-02:2"
    assert scan.candidates[0].payee_name == "Starbucks"
    assert scan.csv_name == WIDE_NAME
    assert scan.statement_rows == 3
    assert scan.scan_id and scan.ttl_seconds == dedupe.DEDUPE_SCAN_TTL


def test_F46_window_comes_from_the_rows_not_the_file_name(env):
    scan = env.scan()
    assert scan.window == ("2026-10-01", "2026-10-05")
    assert "old" not in {c.id for c in scan.candidates}


def test_scan_requires_a_successfully_imported_newest_statement(env):
    record_statement(
        env.conn, env.tmp / "csv", CSV, status="partial", uploaded_at="2026-10-17T00:00:00Z"
    )
    with pytest.raises(StatementUnavailable) as info:
        env.scan()
    assert info.value.reason == "not_imported"
    assert "looking for duplicates" in info.value.user_message


def test_new_scan_replaces_the_old_one_even_if_it_fails(env):
    first = env.scan()
    record_statement(
        env.conn, env.tmp / "csv", CSV, status="failed", uploaded_at="2026-10-17T00:00:00Z"
    )
    with pytest.raises(StatementUnavailable):
        env.scan()
    with pytest.raises(StaleStateError):
        env.delete(first.scan_id, ["orphan1"])
    assert env.deletes() == []


def test_scan_to_dict(env):
    data = env.scan().to_dict()
    assert data["window"] == ["2026-10-01", "2026-10-05"]
    assert [c["id"] for c in data["candidates"]] == ["orphan1", "orphan2"]


# ── delete: validation (F20 / F44) ──────────────────────────────────────────


def test_F20_replaced_scan_id_is_rejected(env):
    first = env.scan()
    second = env.scan()
    assert first.scan_id != second.scan_id
    with pytest.raises(StaleStateError):
        env.delete(first.scan_id, ["orphan1"])
    assert env.deletes() == []


def test_F20_ids_outside_the_scan_reject_the_whole_request(env):
    scan = env.scan()
    with pytest.raises(StaleStateError):
        env.delete(scan.scan_id, ["orphan1", "linked"])
    assert env.deletes() == []
    assert env.fake.tx("orphan1")["deleted"] is False


def test_F20_scan_of_another_user_is_rejected(env):
    scan = env.scan(user="7")
    with pytest.raises(StaleStateError):
        env.delete(scan.scan_id, ["orphan1"])  # user 42
    assert env.deletes() == []


def test_scan_for_another_account_is_rejected(env):
    scan = env.scan()
    with pytest.raises(StaleStateError):
        env.delete(scan.scan_id, ["orphan1"], account_id=OTHER)
    assert env.deletes() == []


def test_scan_expires(env):
    scan = env.scan()
    env.clock.now += dedupe.DEDUPE_SCAN_TTL + 1
    with pytest.raises(StaleStateError):
        env.delete(scan.scan_id, ["orphan1"])
    assert env.deletes() == []


def test_F44_more_than_200_ids_are_refused_before_anything_else(env):
    with pytest.raises(DedupeSelectionError) as info:
        env.delete("no-such-scan", [f"id{i}" for i in range(201)])
    assert "200" in info.value.user_message
    assert env.fake.calls == []


@pytest.mark.parametrize("ids", [[], "orphan1", ["orphan1", 3], [""], None])
def test_malformed_selection(env, ids):
    scan = env.scan()
    with pytest.raises(DedupeSelectionError):
        env.delete(scan.scan_id, ids)
    assert env.deletes() == []


# ── delete: execution ──────────────────────────────────────────────────────


def test_delete_one_by_one_and_drop_processed_ids_from_the_scan(env):
    scan = env.scan()
    result = env.delete(scan.scan_id, ["orphan2", "orphan1", "orphan2"])
    assert [c.id for c in result.deleted] == ["orphan2", "orphan1"]
    assert result.ok and result.already_deleted == []
    assert env.deletes() == ["orphan2", "orphan1"]
    assert env.fake.tx("orphan1")["deleted"] is True
    assert env.fake.tx("linked")["deleted"] is False
    with pytest.raises(StaleStateError):  # already processed
        env.delete(scan.scan_id, ["orphan1"])


def test_scan_can_be_used_for_several_chunks(env):
    scan = env.scan()
    env.delete(scan.scan_id, ["orphan1"])
    assert [c.id for c in env.delete(scan.scan_id, ["orphan2"]).deleted] == ["orphan2"]


def test_404_counts_as_deleted(env):
    scan = env.scan()
    env.fake.user_delete_transaction("orphan1")
    result = env.delete(scan.scan_id, ["orphan1"])
    assert [c.id for c in result.deleted] == ["orphan1"]
    assert result.already_deleted == ["orphan1"]
    assert result.ok


def test_deleted_per_mirror_needs_no_request(env):
    scan = env.scan()
    env.fake.user_delete_transaction("orphan1")
    env.sync()
    result = env.delete(scan.scan_id, ["orphan1"])
    assert result.already_deleted == ["orphan1"]
    assert env.deletes() == []


def test_F13_revalidation_skips_a_row_linked_after_the_scan(env):
    scan = env.scan()
    add_entry(env.conn, "YNAB:-4500:2026-10-02:2", ynab_tx_id="orphan1")  # an import ran
    result = env.delete(scan.scan_id, ["orphan1", "orphan2"])
    assert [(c.id, reason) for c, reason in result.skipped] == [
        ("orphan1", "belongs to a statement row")
    ]
    assert [c.id for c in result.deleted] == ["orphan2"]
    assert env.deletes() == ["orphan2"]
    assert env.fake.tx("orphan1")["deleted"] is False


def test_linked_ledger_entries_become_deleted_in_ynab(env):
    ledger = LedgerStore(env.conn)
    add_entry(env.conn, "YNAB:-9900:2026-10-04:7", status="UNLINKED", ynab_tx_id="orphan2")
    add_entry(env.conn, "YNAB:-9900:2026-10-04:8", status="REVERTED", ynab_tx_id="orphan2")
    scan = env.scan()
    assert "orphan2" in {c.id for c in scan.candidates}
    result = env.delete(scan.scan_id, ["orphan2"])
    assert result.ledger_marked == 1
    assert ledger.get(ACCOUNT, "YNAB:-9900:2026-10-04:7").status == "DELETED_IN_YNAB"
    assert ledger.get(ACCOUNT, "YNAB:-9900:2026-10-04:8").status == "REVERTED"


def test_per_id_failure_is_reported_and_the_rest_continues(env):
    scan = env.scan()
    env.fake.fail_next("DELETE", r"/budgets/[^/]+/transactions/orphan1", 500, times=3)
    result = env.delete(scan.scan_id, ["orphan1", "orphan2"])
    assert [c.id for c, _ in result.failures] == ["orphan1"]
    assert [c.id for c in result.deleted] == ["orphan2"]
    assert not result.ok
    # A failed id stays in the scan and can be retried.
    assert [c.id for c in env.delete(scan.scan_id, ["orphan1"]).deleted] == ["orphan1"]


def test_rate_limit_stops_and_keeps_the_rest_for_later(env):
    scan = env.scan()
    env.fake.fail_next("DELETE", None, 429)
    result = env.delete(scan.scan_id, ["orphan1", "orphan2"])
    assert result.rate_limited
    assert [c.id for c in result.not_attempted] == ["orphan1", "orphan2"]
    assert result.deleted == []
    assert env.fake.tx("orphan2")["deleted"] is False
    assert len(env.delete(scan.scan_id, ["orphan1", "orphan2"]).deleted) == 2


def test_auth_error_is_raised(env):
    scan = env.scan()
    env.fake.fail_next("DELETE", None, 401)
    with pytest.raises(YnabAuthError):
        env.delete(scan.scan_id, ["orphan1"])


def test_audit_line_per_deletion(env, caplog):
    scan = env.scan()
    with caplog.at_level("INFO", logger="ynab_helper.audit"):
        env.delete(scan.scan_id, ["orphan1"])
    lines = [r.getMessage() for r in caplog.records if r.name == "ynab_helper.audit"]
    assert len(lines) == 1 and "orphan1" in lines[0]


def test_result_to_dict(env):
    scan = env.scan()
    data = env.delete(scan.scan_id, ["orphan1"]).to_dict()
    assert data["ok"] is True
    assert [c["id"] for c in data["deleted"]] == ["orphan1"]


def test_registry_discard():
    scans = DedupeScans()
    scans.discard("nobody")  # no error
    with pytest.raises(ValueError):
        DedupeScans(ttl=0)
