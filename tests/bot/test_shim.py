"""revolut_ynab_bot.py: a thin, side-effect-free shim over ynab_helper.bot.main (DESIGN §2.3)."""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import REPO_ROOT
from ynab_helper import container
from ynab_helper.bot import main as bot_main
from ynab_helper.errors import YnabHelperError

SHIM = REPO_ROOT / "revolut_ynab_bot.py"


def clean_env(home: Path) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("YNAB_", "CRYPTO_", "TELEGRAM_", "WEB_UI_", "LOG_", "BOT_"))
    }
    env.update({"HOME": str(home), "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": str(REPO_ROOT)})
    return env


def test_the_shim_is_small_and_guarded():
    text = SHIM.read_text(encoding="utf-8")
    assert len(text.splitlines()) < 60
    assert 'if __name__ == "__main__":\n    raise SystemExit(main())' in text
    assert "import web" not in text and "from web" not in text  # never the legacy package


def test_importing_the_shim_runs_nothing(tmp_path: Path):
    result = subprocess.run(
        [sys.executable, "-c", "import revolut_ynab_bot; print(callable(revolut_ynab_bot.main))"],
        cwd=REPO_ROOT,
        env=clean_env(tmp_path),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "True\n" and result.stderr == ""
    assert list(tmp_path.iterdir()) == []  # no log file, no database, no data dir


def test_running_without_telegram_settings_exits_1(tmp_path: Path):
    env = clean_env(tmp_path)
    env["BOT_DATA_DIR"] = str(tmp_path / "data")
    result = subprocess.run(
        [sys.executable, str(SHIM)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 1
    assert "TELEGRAM_BOT_TOKEN" in result.stderr
    assert not (tmp_path / "data").exists()


def test_as_root_the_shim_prepares_the_runtime_first(monkeypatch: pytest.MonkeyPatch):
    shim = importlib.import_module("revolut_ynab_bot")
    calls: list[str] = []
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    monkeypatch.setattr(container, "prepare_runtime", lambda: calls.append("prepare") or True)
    monkeypatch.setattr(bot_main, "main", lambda: calls.append("bot") or 0)
    assert shim.main() == 0
    assert calls == ["prepare", "bot"]


def test_a_failed_privilege_drop_never_runs_the_bot(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    shim = importlib.import_module("revolut_ynab_bot")
    ran: list[str] = []

    def fail() -> bool:
        raise YnabHelperError("setuid failed")

    monkeypatch.setattr(os, "geteuid", lambda: 0)
    monkeypatch.setattr(container, "prepare_runtime", fail)
    monkeypatch.setattr(bot_main, "main", lambda: ran.append("bot") or 0)
    assert shim.main() == 1
    assert ran == []
    assert "refusing to run as root" in capsys.readouterr().err


def test_unprivileged_runs_the_bot_directly(monkeypatch: pytest.MonkeyPatch):
    shim = importlib.import_module("revolut_ynab_bot")
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    monkeypatch.setattr(container, "prepare_runtime", lambda: pytest.fail("must not prepare"))
    monkeypatch.setattr(bot_main, "main", lambda: 3)
    assert shim.main() == 3


def test_the_bot_loads_no_web_framework_and_no_legacy_package(tmp_path: Path):
    code = (
        "import sys, ynab_helper.bot.main, ynab_helper.bot.commands as c; c.build_registry(); "
        "heavy = [m for m in sys.modules if m.split('.')[0] in "
        "('fastapi', 'starlette', 'uvicorn', 'jinja2', 'web', 'revolut_to_ynab')]; print(heavy)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        env=clean_env(tmp_path),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[]"
