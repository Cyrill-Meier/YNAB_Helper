"""Behaviour of the browser modules, exercised in Node.js with a small fake DOM
(tests/web_frontend/node_checks.mjs, fake_dom.mjs). Node is optional: without it these tests
are skipped (the Python-side scans in test_js_static.py still run).

Findings: F62 (error normalisation, CSRF reload prompt), F24 (401 → login page), F63 (stale
responses dropped; failed loads never leave rows of other filters), F99 (account picker stays
open while picking, opens inside the viewport), F101 (state in the URL), F102 (keyboard-
friendly compact navigation), F103 (upload toast counts changes, never overclaims), F104
(chunks of 200), F105 (sparkline per row, fixed slots), F106 (dates never shifted)."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from .conftest import JS_DIR, node_path

HARNESS = Path(__file__).with_name("node_checks.mjs")
CHECKS = (
    "html_escapes_every_interpolation",
    "html_nests_fragments_and_arrays",
    "html_url_attributes_only_take_same_site_urls",
    "html_refuses_handler_style_and_srcdoc_attributes",
    "setHTML_accepts_only_safe_fragments",
    "F106_dates_and_timestamps_are_never_shifted_by_parsing",
    "money_from_milliunits_is_exact",
    "F104_select_all_over_200_is_sent_in_chunks_of_200",
    "F105_sparkline_is_scaled_per_row",
    "sparkline_rows_share_a_fixed_number_of_slots",
    "F101_filters_round_trip_through_the_address_bar",
    "csrf_header_is_sent_on_every_request",
    "F62_error_messages_are_normalised",
    "F62_rate_limits_say_when_to_retry",
    "F24_session_expiry_goes_to_the_login_page",
    "F62_stale_csrf_token_is_handled_with_a_reload_prompt",
    "F63_a_slow_stale_response_never_overwrites_a_newer_one",
    "F63_loader_refreshes_while_the_server_syncs",
    "failed_load_tells_the_page_whether_its_rows_still_match",
    "load_error_shows_the_reason_and_retries",
    "F99_account_picker_stays_open_while_picking",
    "account_picker_panel_stays_inside_the_viewport",
    "nav_menu_is_keyboard_friendly",
    "F103_upload_reports_changes_not_parsed_rows",
    "reconcile_confirmation_repeats_the_warnings",
    "cleared_states_read_the_same_everywhere",
)


@pytest.fixture(scope="module")
def results() -> dict[str, str]:
    node = node_path()
    if node is None:
        pytest.skip("node is not installed (optional)")
    env = {**os.environ, "LANG": "en_US.UTF-8", "LC_ALL": "en_US.UTF-8", "TZ": "UTC"}
    env.pop("NODE_OPTIONS", None)
    completed = subprocess.run(
        [node, str(HARNESS), str(JS_DIR)],
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


def test_harness_ran_every_check(results):
    assert set(results) == set(CHECKS)


GENERIC = [name for name in CHECKS if not name.startswith("F")]


@pytest.mark.parametrize("name", GENERIC)
def test_js_behaviour(results, name):
    assert results.get(name) == "ok", results.get(name)


def _ok(results: dict[str, str], name: str) -> None:
    assert results.get(name) == "ok", results.get(name)


def test_F106_dates_and_timestamps_are_never_shifted_by_parsing(results):
    _ok(results, "F106_dates_and_timestamps_are_never_shifted_by_parsing")


def test_F104_select_all_over_200_is_sent_in_chunks_of_200(results):
    _ok(results, "F104_select_all_over_200_is_sent_in_chunks_of_200")


def test_F105_sparkline_is_scaled_per_row(results):
    _ok(results, "F105_sparkline_is_scaled_per_row")


def test_F101_filters_round_trip_through_the_address_bar(results):
    _ok(results, "F101_filters_round_trip_through_the_address_bar")


def test_F62_error_messages_are_normalised(results):
    _ok(results, "F62_error_messages_are_normalised")


def test_F62_rate_limits_say_when_to_retry(results):
    _ok(results, "F62_rate_limits_say_when_to_retry")


def test_F62_stale_csrf_token_is_handled_with_a_reload_prompt(results):
    _ok(results, "F62_stale_csrf_token_is_handled_with_a_reload_prompt")


def test_F24_session_expiry_goes_to_the_login_page(results):
    _ok(results, "F24_session_expiry_goes_to_the_login_page")


def test_F63_a_slow_stale_response_never_overwrites_a_newer_one(results):
    _ok(results, "F63_a_slow_stale_response_never_overwrites_a_newer_one")


def test_F63_loader_refreshes_while_the_server_syncs(results):
    _ok(results, "F63_loader_refreshes_while_the_server_syncs")


def test_F103_upload_reports_changes_not_parsed_rows(results):
    _ok(results, "F103_upload_reports_changes_not_parsed_rows")


def test_F99_account_picker_stays_open_while_picking(results):
    _ok(results, "F99_account_picker_stays_open_while_picking")


def test_every_finding_check_has_a_named_test():
    names = {name for name in globals() if name.startswith("test_F")}
    for check in CHECKS:
        if check.startswith("F"):
            assert f"test_{check}" in names, check
