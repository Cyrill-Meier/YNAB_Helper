"""Uploaded statements and their import status (``csv_files``).

One row per stored CSV (Telegram, web, CLI, or the ``legacy`` file found in v1's csv_cache).
Reconcile uses :meth:`CsvFileStore.latest` to insist on the newest statement having been fully
imported (findings F04/F27); the import result is kept as JSON for the web UI and /status.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from ynab_helper.storage.db import RecordNotFound
from ynab_helper.timeutil import utc_now_iso

CSV_SOURCES: Final = frozenset({"telegram", "web", "cli", "legacy"})
CSV_STATUSES: Final = frozenset({"stored", "imported", "partial", "failed", "legacy"})
#: Statuses for which ``imported_at`` is stamped automatically if not given.
_IMPORT_FINISHED = frozenset({"imported", "partial"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

_COLUMNS = (
    "id",
    "account_id",
    "stored_path",
    "original_name",
    "sha256",
    "uploaded_at",
    "source",
    "currency",
    "row_count",
    "window_start",
    "window_end",
    "status",
    "imported_at",
    "result_json",
)
_SELECT = f"SELECT {', '.join(_COLUMNS)} FROM csv_files"


@dataclass(frozen=True, slots=True)
class CsvFile:
    """A ``csv_files`` row."""

    id: int
    account_id: str | None
    stored_path: str
    original_name: str | None
    sha256: str
    uploaded_at: str
    source: str
    currency: str | None
    row_count: int | None
    window_start: str | None
    window_end: str | None
    status: str
    imported_at: str | None
    result_json: str | None

    @property
    def window(self) -> tuple[str, str] | None:
        """``(window_start, window_end)`` if both are known."""
        if self.window_start and self.window_end:
            return (self.window_start, self.window_end)
        return None

    @property
    def result(self) -> dict[str, Any] | None:
        """``result_json`` decoded (None if absent or unreadable)."""
        if not self.result_json:
            return None
        try:
            decoded = json.loads(self.result_json)
        except ValueError:
            return None
        return decoded if isinstance(decoded, dict) else None


def _row(row: sqlite3.Row) -> CsvFile:
    return CsvFile(**{name: row[name] for name in _COLUMNS})


def _check_status(status: str) -> str:
    if status not in CSV_STATUSES:
        raise ValueError(f"unknown csv_files status {status!r}")
    return status


def _encode_result(result: str | Mapping[str, Any] | None) -> str | None:
    if result is None or isinstance(result, str):
        return result
    return json.dumps(dict(result), sort_keys=True, separators=(",", ":"))


class CsvFileStore:
    """Access to the ``csv_files`` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def add(
        self,
        *,
        stored_path: str,
        sha256: str,
        source: str,
        account_id: str | None = None,
        original_name: str | None = None,
        uploaded_at: str | None = None,
        currency: str | None = None,
        row_count: int | None = None,
        window: tuple[str, str] | None = None,
        status: str = "stored",
    ) -> int:
        """Record a stored statement; returns its id. ``uploaded_at`` defaults to now (UTC)."""
        if not stored_path:
            raise ValueError("stored_path is required")
        if not _SHA256_RE.match(sha256 or ""):
            raise ValueError("sha256 must be 64 lowercase hex characters")
        if source not in CSV_SOURCES:
            raise ValueError(f"unknown csv_files source {source!r}")
        start, end = window if window is not None else (None, None)
        cursor = self._conn.execute(
            "INSERT INTO csv_files (account_id, stored_path, original_name, sha256, uploaded_at, "
            "source, currency, row_count, window_start, window_end, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                account_id,
                stored_path,
                original_name,
                sha256,
                uploaded_at or utc_now_iso(),
                source,
                currency,
                row_count,
                start,
                end,
                _check_status(status),
            ),
        )
        return int(cursor.lastrowid or 0)

    def update_meta(
        self,
        file_id: int,
        *,
        account_id: str | None = None,
        currency: str | None = None,
        row_count: int | None = None,
        window: tuple[str, str] | None = None,
    ) -> CsvFile:
        """Fill in parse results (only the given values change)."""
        changes: dict[str, Any] = {}
        if account_id is not None:
            changes["account_id"] = account_id
        if currency is not None:
            changes["currency"] = currency
        if row_count is not None:
            changes["row_count"] = row_count
        if window is not None:
            changes["window_start"], changes["window_end"] = window
        if changes:
            assignments = ", ".join(f"{name} = ?" for name in changes)
            self._require_changed(
                self._conn.execute(
                    f"UPDATE csv_files SET {assignments} WHERE id = ?", (*changes.values(), file_id)
                ).rowcount,
                file_id,
            )
        return self._require(file_id)

    def set_status(
        self,
        file_id: int,
        status: str,
        result_json: str | Mapping[str, Any] | None = None,
        imported_at: str | None = None,
    ) -> CsvFile:
        """Set the import status; ``result_json``/``imported_at`` change only when given.

        ``imported_at`` is stamped with now for ``imported``/``partial`` if not provided.
        ``result_json`` may be a JSON string or a mapping (``ImportResult.to_dict()``).
        """
        _check_status(status)
        if imported_at is None and status in _IMPORT_FINISHED:
            imported_at = utc_now_iso()
        self._require_changed(
            self._conn.execute(
                "UPDATE csv_files SET status = ?, result_json = COALESCE(?, result_json), "
                "imported_at = COALESCE(?, imported_at) WHERE id = ?",
                (status, _encode_result(result_json), imported_at, file_id),
            ).rowcount,
            file_id,
        )
        return self._require(file_id)

    def get(self, file_id: int) -> CsvFile | None:
        row = self._conn.execute(f"{_SELECT} WHERE id = ?", (file_id,)).fetchone()
        return _row(row) if row else None

    def latest(self, account_id: str | None = None) -> CsvFile | None:
        """Newest statement (by upload time, then id), optionally for one account only."""
        if account_id is None:
            row = self._conn.execute(
                f"{_SELECT} ORDER BY uploaded_at DESC, id DESC LIMIT 1"
            ).fetchone()
        else:
            row = self._conn.execute(
                f"{_SELECT} WHERE account_id = ? ORDER BY uploaded_at DESC, id DESC LIMIT 1",
                (account_id,),
            ).fetchone()
        return _row(row) if row else None

    def recent(self, *, account_id: str | None = None, limit: int = 10) -> list[CsvFile]:
        """Newest statements first."""
        if account_id is None:
            rows = self._conn.execute(
                f"{_SELECT} ORDER BY uploaded_at DESC, id DESC LIMIT ?", (max(0, limit),)
            ).fetchall()
        else:
            rows = self._conn.execute(
                f"{_SELECT} WHERE account_id = ? ORDER BY uploaded_at DESC, id DESC LIMIT ?",
                (account_id, max(0, limit)),
            ).fetchall()
        return [_row(r) for r in rows]

    def _require(self, file_id: int) -> CsvFile:
        record = self.get(file_id)
        if record is None:
            raise RecordNotFound(f"no csv_files row {file_id}")
        return record

    @staticmethod
    def _require_changed(rowcount: int, file_id: int) -> None:
        if rowcount != 1:
            raise RecordNotFound(f"no csv_files row {file_id}")
