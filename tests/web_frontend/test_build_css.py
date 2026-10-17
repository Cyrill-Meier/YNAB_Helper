"""The ahead-of-time Tailwind build (DESIGN §9.4, F23): pinned, checksum-verified standalone
CLI; the committed app.css is what the build produces (--check, CI's drift check); every
class the templates and JS modules use exists in it; v1's palette and dark-mode strategy."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from .conftest import JS_DIR, REPO_ROOT, STATIC_DIR, TEMPLATES_DIR

SCRIPT = REPO_ROOT / "scripts" / "build_css.sh"
CONFIG = REPO_ROOT / "tailwind.config.js"
INPUT = REPO_ROOT / "ynab_helper" / "web" / "styles" / "input.css"
APP_CSS = STATIC_DIR / "app.css"
VERSION = "3.4.17"
CACHE = REPO_ROOT / ".cache" / f"tailwindcss-v{VERSION}"


def _cached_binary() -> Path | None:
    if not CACHE.is_dir() or shutil.which("bash") is None:
        return None
    binaries = [p for p in CACHE.iterdir() if p.name.startswith("tailwindcss-") and p.is_file()]
    return binaries[0] if binaries else None


def _run(script: Path, *args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "TAILWIND_OFFLINE": "1"}  # never download in tests
    return subprocess.run(
        ["bash", str(script), *args], cwd=cwd, capture_output=True, text=True, timeout=180, env=env
    )


# ── the script ───────────────────────────────────────────────────────────────


def test_build_script_pins_the_cli_and_its_checksums():
    source = SCRIPT.read_text(encoding="utf-8")
    assert os.access(SCRIPT, os.X_OK)
    assert f'TAILWIND_VERSION="{VERSION}"' in source
    assert (
        'RELEASE_URL="https://github.com/tailwindlabs/tailwindcss/releases/download/'
        'v${TAILWIND_VERSION}"' in source
    )
    pins = dict(re.findall(r'asset="(tailwindcss-[\w.-]+)"\s*\n\s*sha256="([0-9a-f]+)"', source))
    assert set(pins) == {
        "tailwindcss-macos-arm64",
        "tailwindcss-macos-x64",
        "tailwindcss-linux-x64",
        "tailwindcss-linux-arm64",
        "tailwindcss-linux-armv7",
        "tailwindcss-windows-x64.exe",
        "tailwindcss-windows-arm64.exe",
    }
    assert all(len(digest) == 64 for digest in pins.values())
    assert len(set(pins.values())) == len(pins)
    # The digest is verified before the binary is used, and a bad cached copy is fatal.
    assert 'if [[ "${actual}" != "${sha256}" ]]; then' in source
    assert "--minify" in source and "--check" in source and "--proto '=https'" in source


def test_gitignore_excludes_the_tool_cache():
    lines = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert ".cache/" in lines


def test_tailwind_config_matches_v1():
    config = CONFIG.read_text(encoding="utf-8")
    assert 'darkMode: "class"' in config
    assert '"./ynab_helper/web/templates/**/*.html"' in config
    assert '"./ynab_helper/web/static/js/**/*.js"' in config
    assert "relative: true" in config
    for colour in ("#ECFFFB", "#C7FFF6", "#0EE3C8", "#0BB6A0", "#0A9081", "#063D38"):
        assert colour in config, f"v1 brand colour {colour}"
    for colour in ("#0B0F14", "#11171F", "#1A222D", "#283443", "#475569"):
        assert colour in config, f"v1 ink colour {colour}"
    assert "0 1px 2px rgba(0,0,0,0.04), 0 8px 24px rgba(0,0,0,0.06)" in config


# ── the built stylesheet ─────────────────────────────────────────────────────


def test_app_css_is_the_minified_tailwind_build():
    css = APP_CSS.read_text(encoding="utf-8")
    assert "--tw-border-spacing-x:0" in css, "Tailwind's preflight variables"
    assert css.count("\n") <= 2, "minified"
    assert "@import" not in css and "@tailwind" not in css and "@apply" not in css
    assert not re.search(r"url\(\s*['\"]?(?:https?:)?//", css), "no external resources"
    assert ".dark\\:bg-ink-800:is(.dark *)" in css, "dark mode by class"


def _defined_classes(css: str) -> set[str]:
    names = set()
    for match in re.finditer(r"\.((?:\\.|[A-Za-z0-9_-])+)", css):
        names.add(re.sub(r"\\(.)", r"\1", match.group(1)))
    return names


def _template_classes() -> set[str]:
    tokens = set()
    for path in TEMPLATES_DIR.glob("*.html"):
        for value in re.findall(r'\sclass="([^"]*)"', path.read_text(encoding="utf-8")):
            tokens.update(t for t in value.split() if "{" not in t and "}" not in t)
    return tokens


def _attribute_values(code: str, start: str) -> list[str]:
    """The values of ``class="…"`` in JS template literals, with ``${…}`` parts (which may
    contain quotes and braces) blanked out; quoted strings inside them are kept separately."""
    values = []
    pos = 0
    while (begin := code.find(start, pos)) != -1:
        i = begin + len(start)
        out: list[str] = []
        while i < len(code) and code[i] != '"':
            if code.startswith("${", i):
                depth, j = 1, i + 2
                while j < len(code) and depth:
                    depth += {"{": 1, "}": -1}.get(code[j], 0)
                    j += 1
                inner = code[i + 2 : j - 1]
                values.extend(re.findall(r'"([^"$]*)"', inner))  # e.g. cond ? "a b" : ""
                out.append(" ")
                i = j
            else:
                out.append(code[i])
                i += 1
        values.append("".join(out))
        pos = i + 1
    return values


def _js_classes() -> set[str]:
    tokens = set()
    for path in JS_DIR.rglob("*.js"):
        code = path.read_text(encoding="utf-8")
        values = _attribute_values(code, ' class="')
        values += re.findall(r'className\s*=\s*"([^"]*)"', code)
        values += re.findall(r'classList\.(?:add|remove|toggle)\(\s*"([^"]*)"', code)
        values += re.findall(r'\b\w*[cC]lass\s*:\s*"([^"]*)"', code)  # e.g. latestClass: "…"
        for block in re.findall(r"const \w*(?:CLASSES|BADGES)\s*=\s*[{\[](.*?)[}\]];", code, re.S):
            values += re.findall(r'"([^"]*)"', block)
        for value in values:
            tokens.update(t for t in value.split() if "$" not in t and "{" not in t)
    return tokens


def test_every_used_class_exists_in_app_css():
    """A class name that is misspelt, or composed from pieces the build cannot see, would
    silently do nothing; every literal class token must be defined in the built CSS."""
    defined = _defined_classes(APP_CSS.read_text(encoding="utf-8"))
    used = _template_classes() | _js_classes()
    assert len(used) > 150, "the scan found the class attributes"
    missing = sorted(token for token in used if token not in defined)
    assert not missing, f"classes missing from app.css (rebuild or fix the name): {missing}"


# ── the build itself (needs the cached CLI; never downloads) ─────────────────


def test_build_css_check_passes():
    """CI's drift check: rebuilding produces exactly the committed app.css."""
    if _cached_binary() is None:
        pytest.skip("Tailwind CLI not cached; run scripts/build_css.sh once (CI runs --check)")
    result = _run(SCRIPT, "--check", cwd=REPO_ROOT)
    assert result.returncode == 0, result.stderr
    assert "up to date" in result.stderr


def test_build_css_check_detects_drift(tmp_path: Path):
    """--check fails for an outdated app.css and passes after a rebuild (in a copy of the
    tree, so the committed file is never touched)."""
    binary = _cached_binary()
    if binary is None:
        pytest.skip("Tailwind CLI not cached; run scripts/build_css.sh once")
    root = tmp_path / "repo"
    (root / "scripts").mkdir(parents=True)
    shutil.copy2(SCRIPT, root / "scripts" / "build_css.sh")
    shutil.copy2(CONFIG, root / "tailwind.config.js")
    web = root / "ynab_helper" / "web"
    (web / "styles").mkdir(parents=True)
    shutil.copy2(INPUT, web / "styles" / "input.css")
    shutil.copytree(TEMPLATES_DIR, web / "templates")
    shutil.copytree(JS_DIR, web / "static" / "js")
    cache = root / ".cache" / CACHE.name
    cache.mkdir(parents=True)
    (cache / binary.name).symlink_to(binary)
    script = root / "scripts" / "build_css.sh"

    (web / "static" / "app.css").write_text("/* stale */\n", encoding="utf-8")
    stale = _run(script, "--check", cwd=root)
    assert stale.returncode == 1 and "out of date" in stale.stderr, stale.stderr

    built = _run(script, cwd=root)
    assert built.returncode == 0, built.stderr
    assert (web / "static" / "app.css").read_bytes() == APP_CSS.read_bytes(), "deterministic"
    assert _run(script, "--check", cwd=root).returncode == 0


#: `uname -s`, `uname -m` → the release asset build_css.sh pins for that machine.
_ASSETS = {
    ("Darwin", "arm64"): "tailwindcss-macos-arm64",
    ("Darwin", "x86_64"): "tailwindcss-macos-x64",
    ("Linux", "x86_64"): "tailwindcss-linux-x64",
    ("Linux", "amd64"): "tailwindcss-linux-x64",
    ("Linux", "aarch64"): "tailwindcss-linux-arm64",
    ("Linux", "arm64"): "tailwindcss-linux-arm64",
    ("Linux", "armv7l"): "tailwindcss-linux-armv7",
}


def test_build_css_refuses_a_tampered_cached_cli(tmp_path: Path):
    """A cached CLI whose sha256 does not match the pin is refused and never executed — in
    build and --check mode — and is not silently replaced (no network involved)."""
    if shutil.which("bash") is None or not hasattr(os, "uname"):
        pytest.skip("needs bash on a POSIX system")
    uname = os.uname()
    asset = _ASSETS.get((uname.sysname, uname.machine))
    if asset is None:
        pytest.skip(f"no pinned asset for {uname.sysname}/{uname.machine}")
    root = tmp_path / "repo"
    (root / "scripts").mkdir(parents=True)
    shutil.copy2(SCRIPT, root / "scripts" / "build_css.sh")
    shutil.copy2(CONFIG, root / "tailwind.config.js")  # so it would get as far as the CLI
    (root / "ynab_helper" / "web" / "styles").mkdir(parents=True)
    (root / "ynab_helper" / "web" / "static").mkdir(parents=True)
    shutil.copy2(INPUT, root / "ynab_helper" / "web" / "styles" / "input.css")
    cache = root / ".cache" / CACHE.name
    cache.mkdir(parents=True)
    marker = tmp_path / "the-stub-ran"
    stub = cache / asset
    stub_source = f"#!/bin/sh\ntouch '{marker}'\necho 'body{{}}' > \"$6\"\n"
    stub.write_text(stub_source, encoding="utf-8")
    stub.chmod(0o755)

    for args in ((), ("--check",)):
        result = _run(root / "scripts" / "build_css.sh", *args, cwd=root)
        assert result.returncode == 1, (args, result.stderr)
        assert "sha256" in result.stderr and "expected" in result.stderr, result.stderr
        assert not marker.exists(), f"a binary with the wrong checksum was executed ({args})"
    assert stub.read_text(encoding="utf-8") == stub_source, "the bad copy is reported, not replaced"
    assert not (root / "ynab_helper" / "web" / "static" / "app.css").exists()


def test_build_css_refuses_to_download_offline(tmp_path: Path):
    root = tmp_path / "repo"
    (root / "scripts").mkdir(parents=True)
    shutil.copy2(SCRIPT, root / "scripts" / "build_css.sh")
    result = _run(root / "scripts" / "build_css.sh", "--check", cwd=root)
    assert result.returncode != 0
    assert "not cached" in result.stderr or "no pinned Tailwind CLI" in result.stderr
