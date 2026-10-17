"""Page-level behaviour of the browser modules: the real page modules run in Node.js against
the templates as the server renders them (tests/web_frontend/node_page_checks.mjs, with the
small fake DOM of fake_dom.mjs and a fake fetch). Node is optional: without it these tests
are skipped.

Covers F104 (select all → confirm → chunks of 200; stop at a stale scan) and the review
findings: a failed filter change clears the old rows and offers Retry, the account picker
opens inside the viewport on every page, and the reconcile dialog repeats the warnings."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from .conftest import JS_DIR, node_path

HARNESS = Path(__file__).with_name("node_page_checks.mjs")
PAGES = ("transactions", "spending", "subscriptions", "dedupe", "reconcile")
CHECKS = (
    "failed_filter_change_clears_the_rows_and_offers_retry",
    "account_picker_anchors_match_each_toolbar",
    "F104_dedupe_select_all_deletes_in_confirmed_chunks_of_200",
    "F104_dedupe_stops_at_a_stale_scan_and_says_what_stayed",
    "reconcile_dialog_repeats_the_warnings_before_posting",
)


@pytest.fixture
def results(signed_in, tmp_path: Path) -> dict[str, str]:
    node = node_path()
    if node is None:
        pytest.skip("node is not installed (optional)")
    pages = tmp_path / "pages"
    pages.mkdir()
    for page in PAGES:
        response = signed_in.get(f"/app/{page}")
        assert response.status_code == 200
        (pages / f"{page}.html").write_text(response.text, encoding="utf-8")
    env = {**os.environ, "LANG": "en_US.UTF-8", "LC_ALL": "en_US.UTF-8", "TZ": "UTC"}
    env.pop("NODE_OPTIONS", None)
    completed = subprocess.run(
        [node, str(HARNESS), str(JS_DIR), str(pages)],
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
    assert lines, completed.stdout + completed.stderr
    return json.loads(lines[-1])


def _ok(results: dict[str, str], name: str) -> None:
    assert results.get(name) == "ok", results.get(name)


def test_harness_ran_every_page_check(results):
    assert set(results) == set(CHECKS)


def test_failed_filter_change_clears_the_rows_and_offers_retry(results):
    _ok(results, "failed_filter_change_clears_the_rows_and_offers_retry")


def test_F99_account_picker_anchors_match_each_toolbar(results):
    _ok(results, "account_picker_anchors_match_each_toolbar")


def test_F104_dedupe_select_all_deletes_in_confirmed_chunks_of_200(results):
    _ok(results, "F104_dedupe_select_all_deletes_in_confirmed_chunks_of_200")


def test_F104_dedupe_stops_at_a_stale_scan_and_says_what_stayed(results):
    _ok(results, "F104_dedupe_stops_at_a_stale_scan_and_says_what_stayed")


def test_reconcile_dialog_repeats_the_warnings_before_posting(results):
    _ok(results, "reconcile_dialog_repeats_the_warnings_before_posting")
