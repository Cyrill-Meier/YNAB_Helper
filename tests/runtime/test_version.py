"""version.py: version line from __version__ + BUILD_SHA/BUILD_DATE (F112)."""

from __future__ import annotations

import re

import pytest

import ynab_helper
from ynab_helper import version


def test_package_version_is_2_0_0() -> None:
    assert ynab_helper.__version__ == "2.0.0"


def test_F112_version_comes_from_build_args() -> None:
    env = {"BUILD_SHA": "abc1234def5678", "BUILD_DATE": "2026-10-16T20:00:00Z"}
    assert version.version_info(env) == {
        "version": "2.0.0",
        "sha": "abc1234",
        "date": "2026-10-16 20:00 UTC",
    }
    assert version.version_line(env) == "v2.0.0 (abc1234, 2026-10-16 20:00 UTC)"


def test_version_line_defaults() -> None:
    assert version.version_line({}) == "v2.0.0 (dev, local)"


def test_version_line_reads_os_environ(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BUILD_SHA", "feedbeef")
    monkeypatch.setenv("BUILD_DATE", "2026-10-16 20:00 UTC")
    assert version.version_line() == "v2.0.0 (feedbee, 2026-10-16 20:00 UTC)"


def test_cache_bust_token_release_build() -> None:
    assert version.cache_bust_token({"BUILD_SHA": "abc1234"}) == "2.0.0-abc1234"


@pytest.mark.parametrize("sha", [None, "", "dev", "local"])
def test_cache_bust_token_dev_build_is_stable_per_process(sha: str | None) -> None:
    env = {} if sha is None else {"BUILD_SHA": sha}
    token = version.cache_bust_token(env)
    assert re.fullmatch(r"2\.0\.0-dev-\d+", token)
    assert version.cache_bust_token(env) == token


@pytest.mark.parametrize("sha", ["../../x", "a b", "<x>", "ab%2F"])
def test_cache_bust_token_is_url_path_safe(sha: str) -> None:
    token = version.cache_bust_token({"BUILD_SHA": sha})
    assert re.fullmatch(r"[0-9A-Za-z._-]+", token)
    assert ".." not in token.replace("2.0.0", "")
