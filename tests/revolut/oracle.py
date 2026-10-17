"""v1 parser oracle: run the frozen v1 ``parse_revolut_csv`` and compare with v2.

The frozen copy lives in ``tests/legacy/v1_revolut_to_ynab.py`` (never imported by
production code). v1 prints a line for each row it skips; callers that care can use
pytest's ``capsys``.
"""

from __future__ import annotations

import contextlib
import io
from pathlib import Path
from typing import Any

from tests.legacy import v1_revolut_to_ynab as v1
from ynab_helper.revolut.parser import STATE_ALLOWLIST, ParsedExport


def v1_parse(path: Path) -> list[dict[str, Any]]:
    """Parse ``path`` with the frozen v1 parser (its skip prints are swallowed)."""
    with contextlib.redirect_stdout(io.StringIO()):
        return v1.parse_revolut_csv(str(path))


def assert_matches_v1(export: ParsedExport, v1_rows: list[dict[str, Any]]) -> None:
    """Assert v2 rows are byte-identical to v1 for every allowlisted-state row.

    v1 also returned rows with other states (DECLINED, FAILED, …); v2 skips those, but
    they must still have consumed the same occurrence numbers, so their keys are
    compared against the keys v2 reports in ``skipped``.
    """
    kept = [t for t in v1_rows if t["_state"] in STATE_ALLOWLIST]
    dropped = [t for t in v1_rows if t["_state"] not in STATE_ALLOWLIST]
    assert len(export.rows) == len(kept), (len(export.rows), len(kept))
    for row, legacy in zip(export.rows, kept, strict=True):
        actual = {
            "import_id": row.key,
            "amount": row.amount,
            "date": row.date,
            "memo": row.memo,
            "payee_name": row.payee,
            "cleared": row.cleared,
            "_state": row.state,
            "_started_at": row.started_at_text,
            "_started_time": row.started_at.strftime("%H:%M:%S"),
        }
        expected = {key: legacy[key] for key in actual}
        assert actual == expected, f"line {row.line_no}"
        # Byte-identical, not merely equal (e.g. no str subclass, same code points).
        assert type(row.key) is str
        assert row.key.encode() == legacy["import_id"].encode()
        if row.memo is not None:
            assert row.memo.encode() == legacy["memo"].encode()
        assert row.payee.encode() == legacy["payee_name"].encode()
    state_skips = [s.key for s in export.skipped if s.code == "unsupported_state"]
    assert state_skips == [t["import_id"] for t in dropped]
