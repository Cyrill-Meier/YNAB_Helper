"""R20: the documented entry points say which Python they need on an older ``python3``.

v1 was a single stdlib script that ran on the system ``python3`` (3.9 on macOS). The v2
package needs 3.12, so ``python3 revolut_to_ynab.py``, ``python3 revolut_ynab_bot.py`` and
``python3 -m ynab_helper`` check the interpreter before importing the package and print one
line instead of an ``ImportError: cannot import name 'UTC'`` traceback.
"""

from __future__ import annotations

import ast
import builtins
import importlib
import io
import os
import re
import shutil
import subprocess
import sys
import tokenize
import tomllib
from pathlib import Path
from types import ModuleType

import pytest

from tests.conftest import REPO_ROOT

ENTRY_POINTS = {
    "revolut_to_ynab": REPO_ROOT / "revolut_to_ynab.py",
    "revolut_ynab_bot": REPO_ROOT / "revolut_ynab_bot.py",
    "ynab_helper.__main__": REPO_ROOT / "ynab_helper" / "__main__.py",
}
_GATE_RE = re.compile(r"if sys\.version_info < \((\d+), (\d+)\):")


def _required_python() -> tuple[int, int]:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    match = re.fullmatch(r">=(\d+)\.(\d+)", pyproject["project"]["requires-python"])
    assert match, "requires-python must stay a plain '>=X.Y' lower bound"
    return int(match[1]), int(match[2])


@pytest.mark.parametrize("module_name", sorted(ENTRY_POINTS))
def test_runtime_infra_R20_old_python_gets_one_line_before_any_package_import(
    module_name: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    module: ModuleType = importlib.import_module(module_name)
    real_import = builtins.__import__

    def python39_import(name: str, *args: object, **kwargs: object) -> object:
        # What Python 3.9 does on the first package import (config.py: datetime.UTC).
        if name == "ynab_helper" or name.startswith("ynab_helper."):
            raise ImportError("cannot import name 'UTC' from 'datetime'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(sys, "version_info", (3, 9, 6, "final", 0))
    monkeypatch.setattr(sys, "version", "3.9.6 (default, Mar 10 2023, 20:16:38)")
    monkeypatch.setattr(builtins, "__import__", python39_import)
    assert module.main() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.count("\n") == 1  # exactly one line
    assert captured.err.startswith("YNAB Helper v2 needs Python 3.12 or newer, not 3.9.6:")
    assert ".venv/bin/python" in captured.err


def test_runtime_infra_R20_gate_matches_requires_python_and_avoids_newer_syntax() -> None:
    required = _required_python()
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    ruff_targets = pyproject["tool"]["ruff"]["per-file-target-version"]
    for path in ENTRY_POINTS.values():
        source = path.read_text(encoding="utf-8")
        relative = path.relative_to(REPO_ROOT).as_posix()
        gates = [(int(major), int(minor)) for major, minor in _GATE_RE.findall(source)]
        assert gates == [required], relative
        assert f"Python {required[0]}.{required[1]} or newer" in source, relative
        # The file is compiled before the gate can run, so it must not use syntax an old
        # python3 rejects: f-strings (3.6) and `from __future__ import annotations` (3.7).
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
        assert not [t for t in tokens if t.type == tokenize.FSTRING_START], relative
        futures = [
            node
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.ImportFrom) and node.module == "__future__"
        ]
        assert futures == [], relative
        assert ruff_targets.get(relative) == "py37", relative  # ruff flags 3.8+ syntax


def _older_interpreters() -> list[str]:
    candidates = ["/usr/bin/python3", "python3", *(f"python3.{minor}" for minor in range(6, 12))]
    found: dict[str, str] = {}
    for candidate in candidates:
        path = shutil.which(candidate)
        if not path or os.path.realpath(path) in found:
            continue
        probe = subprocess.run(
            [path, "-E", "-s", "-c", "import sys; print(sys.version_info >= (3, 12))"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if probe.returncode == 0 and probe.stdout.strip() == "False":
            found[os.path.realpath(path)] = path
    return sorted(found.values())


_OLD_PYTHONS = _older_interpreters()


@pytest.mark.skipif(not _OLD_PYTHONS, reason="no python3 older than 3.12 on this machine")
@pytest.mark.parametrize("python", _OLD_PYTHONS)
@pytest.mark.parametrize(
    "args",
    [
        ["revolut_to_ynab.py", "--help"],
        ["revolut_to_ynab.py", "--brokerage-sync", "--dry-run"],
        ["revolut_ynab_bot.py"],
        ["-m", "ynab_helper", "cli", "--help"],
    ],
    ids=["cli-help", "cli-brokerage", "bot", "dash-m"],
)
def test_runtime_infra_R20_real_old_python_prints_the_requirement(
    python: str, args: list[str], tmp_path: Path
) -> None:
    env = {"PATH": os.environ.get("PATH", ""), "HOME": str(tmp_path)}
    # -E/-s: no PYTHON* variables or user site; -B: no .pyc files for 3.x in the checkout.
    result = subprocess.run(
        [python, "-E", "-s", "-B", *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 1, result.stderr
    assert result.stdout == ""
    assert "Traceback" not in result.stderr
    assert result.stderr.count("\n") == 1
    assert "needs Python 3.12 or newer" in result.stderr
    assert list(tmp_path.iterdir()) == []  # no log file, no data directory
