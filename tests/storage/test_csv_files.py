"""csv_files.py: CsvFileStore."""

from __future__ import annotations

import hashlib
import json
import sqlite3

import pytest

from ynab_helper.storage import CsvFileStore, RecordNotFound

SHA = hashlib.sha256(b"csv").hexdigest()


@pytest.fixture
def files(tx_conn: sqlite3.Connection) -> CsvFileStore:
    return CsvFileStore(tx_conn)


def test_add_get_and_defaults(files: CsvFileStore) -> None:
    file_id = files.add(
        stored_path="/data/csv_cache/1/20261016T180000Z_abcd1234.csv",
        sha256=SHA,
        source="telegram",
        account_id="acct-main",
        original_name="account-statement.csv",
        currency="CHF",
        row_count=12,
        window=("2026-08-01", "2026-08-31"),
    )
    record = files.get(file_id)
    assert record is not None
    assert record.status == "stored" and record.source == "telegram"
    assert record.window == ("2026-08-01", "2026-08-31")
    assert record.uploaded_at.endswith("Z") and record.imported_at is None
    assert record.result is None
    assert files.get(file_id + 100) is None


@pytest.mark.parametrize(
    "bad",
    [
        {"source": "email"},
        {"status": "done"},
        {"sha256": "XYZ"},
        {"sha256": SHA.upper()},
        {"stored_path": ""},
    ],
)
def test_add_validates(files: CsvFileStore, bad: dict) -> None:
    kwargs = {"stored_path": "/x.csv", "sha256": SHA, "source": "web"}
    kwargs.update(bad)
    with pytest.raises(ValueError):
        files.add(**kwargs)


def test_set_status_with_result_and_auto_timestamp(files: CsvFileStore) -> None:
    file_id = files.add(stored_path="/x.csv", sha256=SHA, source="web")
    result = {"created": [], "failures": [{"action": "UPDATE", "error": "YNAB 500"}], "ok": False}
    partial = files.set_status(file_id, "partial", result_json=result)
    assert partial.status == "partial" and partial.imported_at
    assert partial.result == result
    assert json.loads(partial.result_json or "") == result

    # A later failure keeps the stored result unless a new one is given.
    failed = files.set_status(file_id, "failed")
    assert failed.status == "failed" and failed.result == result
    assert failed.imported_at == partial.imported_at

    done = files.set_status(file_id, "imported", '{"ok": true}', imported_at="2026-10-16T18:00:00Z")
    assert done.result == {"ok": True} and done.imported_at == "2026-10-16T18:00:00Z"

    with pytest.raises(ValueError):
        files.set_status(file_id, "bogus")
    with pytest.raises(RecordNotFound):
        files.set_status(9999, "failed")


def test_update_meta(files: CsvFileStore) -> None:
    file_id = files.add(stored_path="/x.csv", sha256=SHA, source="cli")
    updated = files.update_meta(
        file_id,
        account_id="acct-main",
        currency="EUR",
        row_count=3,
        window=("2026-01-01", "2026-01-31"),
    )
    assert (updated.account_id, updated.currency, updated.row_count) == ("acct-main", "EUR", 3)
    assert updated.window == ("2026-01-01", "2026-01-31")
    assert files.update_meta(file_id) == updated
    with pytest.raises(RecordNotFound):
        files.update_meta(9999, currency="CHF")


def test_latest_and_recent_per_account(files: CsvFileStore) -> None:
    assert files.latest() is None
    a1 = files.add(
        stored_path="/a1",
        sha256=SHA,
        source="web",
        account_id="a",
        uploaded_at="2026-08-01T00:00:00Z",
    )
    b1 = files.add(
        stored_path="/b1",
        sha256=SHA,
        source="web",
        account_id="b",
        uploaded_at="2026-08-03T00:00:00Z",
    )
    a2 = files.add(
        stored_path="/a2",
        sha256=SHA,
        source="web",
        account_id="a",
        uploaded_at="2026-08-02T00:00:00Z",
    )
    same_time = files.add(
        stored_path="/a3",
        sha256=SHA,
        source="web",
        account_id="a",
        uploaded_at="2026-08-02T00:00:00Z",
    )
    assert files.latest().id == b1  # type: ignore[union-attr]
    assert files.latest("a").id == same_time  # type: ignore[union-attr]  # tie → newest id
    assert files.latest("nobody") is None
    assert [f.id for f in files.recent(account_id="a")] == [same_time, a2, a1]
    assert [f.id for f in files.recent(limit=2)] == [b1, same_time]


def test_result_property_tolerates_garbage(
    files: CsvFileStore, tx_conn: sqlite3.Connection
) -> None:
    file_id = files.add(stored_path="/x.csv", sha256=SHA, source="web")
    tx_conn.execute("UPDATE csv_files SET result_json = 'not json' WHERE id = ?", (file_id,))
    assert files.get(file_id).result is None  # type: ignore[union-attr]
    tx_conn.execute("UPDATE csv_files SET result_json = '[1, 2]' WHERE id = ?", (file_id,))
    assert files.get(file_id).result is None  # type: ignore[union-attr]
