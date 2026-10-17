"""revolut_to_ynab.py: a thin, side-effect-free shim over ynab_helper.cli (DESIGN §2.3)."""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import REPO_ROOT
from ynab_helper import cli, container
from ynab_helper.errors import YnabHelperError

SHIM = REPO_ROOT / "revolut_to_ynab.py"


def _clean_env(home: Path) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("YNAB_", "CRYPTO_", "IBKR_", "LOG_", "CSV_", "BOT_"))
    }
    env.update({"HOME": str(home), "PYTHONDONTWRITEBYTECODE": "1"})
    return env


def test_the_shim_is_small_and_has_a_main_guard():
    text = SHIM.read_text(encoding="utf-8")
    assert len(text.splitlines()) < 60
    assert 'if __name__ == "__main__":\n    raise SystemExit(main())' in text


def test_importing_the_shim_runs_nothing(tmp_path: Path):
    result = subprocess.run(
        [sys.executable, "-c", "import revolut_to_ynab; print(callable(revolut_to_ynab.main))"],
        cwd=REPO_ROOT,
        env=_clean_env(tmp_path),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "True\n" and result.stderr == ""
    assert list(tmp_path.iterdir()) == []  # no log file, no database


def test_help_runs_from_any_directory_and_creates_nothing(tmp_path: Path):
    result = subprocess.run(
        [sys.executable, str(SHIM), "--help"],
        cwd=tmp_path,
        env=_clean_env(tmp_path),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("usage: revolut_to_ynab.py")
    for option in ("--sync", "--watch", "--reconcile", "--crypto-sync", "--db-path", "--yes"):
        assert option in result.stdout
    assert list(tmp_path.iterdir()) == []


def test_a_usage_error_exits_one(tmp_path: Path):
    result = subprocess.run(
        [sys.executable, str(SHIM), "--bogus"],
        cwd=tmp_path,
        env=_clean_env(tmp_path),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 1 and "unrecognized arguments" in result.stderr


@pytest.fixture
def shim(monkeypatch: pytest.MonkeyPatch):
    module = importlib.import_module("revolut_to_ynab")
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(container, "prepare_runtime", lambda: calls.append(("prepare", None)))
    monkeypatch.setattr(cli, "main", lambda argv=None: calls.append(("cli", argv)) or 7)
    return module, calls


def test_as_root_the_shim_drops_privileges_before_the_cli_runs(shim, monkeypatch):
    module, calls = shim
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    assert module.main(["--sync"]) == 7
    assert calls == [("prepare", None), ("cli", ["--sync"])]


def test_as_a_normal_user_the_shim_only_runs_the_cli(shim, monkeypatch):
    module, calls = shim
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    assert module.main(["--db-stats"]) == 7
    assert calls == [("cli", ["--db-stats"])]


def test_without_geteuid_windows_the_shim_runs_the_cli(shim, monkeypatch):
    module, calls = shim
    monkeypatch.delattr(os, "geteuid", raising=False)
    assert module.main(["--help"]) == 7  # v1 ran wherever Python ran; no AttributeError
    assert calls == [("cli", ["--help"])]


@pytest.mark.parametrize("error", [OSError("read-only file system"), YnabHelperError("drop")])
def test_as_root_a_failed_preparation_refuses_to_run(monkeypatch, capsys, error):
    module = importlib.import_module("revolut_to_ynab")
    ran: list[object] = []

    def failing() -> None:
        raise error

    monkeypatch.setattr(os, "geteuid", lambda: 0)
    monkeypatch.setattr(container, "prepare_runtime", failing)
    monkeypatch.setattr(cli, "main", lambda argv=None: ran.append(argv) or 0)
    assert module.main([]) == 1
    assert ran == [] and "refusing to run as root" in capsys.readouterr().err
