"""Static regression tests for the container, CI and reference deployment files.

These files have no runtime of their own in the test suite, so their safety properties
(pinned supply chain, publish only from main, non-root runtime, health check, log
rotation, …) are asserted on the text. The checks are deliberately structural rather
than byte-for-byte so harmless edits (comments, ordering of unrelated keys) keep passing.

The image itself is exercised by ``scripts/ci_smoke.sh`` in the CI image job.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = REPO_ROOT / "Dockerfile"
DOCKERIGNORE = REPO_ROOT / ".dockerignore"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
OLD_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "docker-publish.yml"
DEPENDABOT = REPO_ROOT / ".github" / "dependabot.yml"
COMPOSE = REPO_ROOT / "docker-compose.yml"
CADDYFILE = REPO_ROOT / "Caddyfile"
SMOKE_SCRIPT = REPO_ROOT / "scripts" / "ci_smoke.sh"
REQUIREMENTS = REPO_ROOT / "requirements.txt"

PUSH_TO_MAIN = "github.event_name == 'push' && github.ref == 'refs/heads/main'"
APP_ID = "10001"


# ─── helpers ─────────────────────────────────────────────────────────────────


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _active_lines(text: str, comment: str = "#") -> list[str]:
    """Lines with full-line comments and blank lines removed (inline text kept)."""
    return [
        line for line in text.splitlines() if line.strip() and not line.lstrip().startswith(comment)
    ]


@dataclass(frozen=True, slots=True)
class Instruction:
    keyword: str
    args: str


def _dockerfile_instructions() -> list[Instruction]:
    """Dockerfile instructions with ``\\`` continuations joined and comments dropped."""
    instructions: list[Instruction] = []
    pending = ""
    for line in _text(DOCKERFILE).splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.endswith("\\"):
            pending += stripped[:-1] + " "
            continue
        full = (pending + stripped).strip()
        pending = ""
        keyword, _, args = full.partition(" ")
        instructions.append(Instruction(keyword.upper(), " ".join(args.split())))
    assert not pending, "dangling line continuation at the end of the Dockerfile"
    return instructions


def _instructions(keyword: str) -> list[str]:
    return [i.args for i in _dockerfile_instructions() if i.keyword == keyword]


def _index_of(keyword: str, predicate: str = "") -> int:
    for index, instruction in enumerate(_dockerfile_instructions()):
        if instruction.keyword == keyword and predicate in instruction.args:
            return index
    raise AssertionError(f"no {keyword} instruction containing {predicate!r}")


def _yaml_block(text: str, header: str) -> str:
    """The lines below the first line equal to ``header`` (indentation included) that are
    indented deeper than it, i.e. the value of that mapping key.

    Good enough for the hand-written YAML files in this repository; it avoids a YAML
    dependency in the test environment.
    """
    lines = text.splitlines()
    for start, line in enumerate(lines):
        if line.rstrip() == header:
            indent = len(line) - len(line.lstrip())
            block: list[str] = []
            for following in lines[start + 1 :]:
                if following.strip() and len(following) - len(following.lstrip()) <= indent:
                    break
                block.append(following)
            return "\n".join(block)
    raise AssertionError(f"YAML key {header!r} not found")


def _workflow_jobs() -> dict[str, str]:
    """``job id -> job block`` of the CI workflow."""
    jobs = _yaml_block(_text(CI_WORKFLOW), "jobs:")
    names = re.findall(r"^  ([\w-]+):$", jobs, re.MULTILINE)
    return {name: _yaml_block(jobs, f"  {name}:") for name in names}


def _job_key(job: str, key: str) -> str | None:
    """The scalar value of a top-level key of a job (``needs``, ``if``, …), if present."""
    match = re.search(rf"^    {re.escape(key)}: (.+)$", _workflow_jobs()[job], re.MULTILINE)
    return match.group(1).strip() if match else None


def _workflow_steps(job: str) -> list[str]:
    """The text of each step (``- name: …`` item) of a job in the CI workflow."""
    steps_text = _yaml_block(_workflow_jobs()[job], "    steps:")
    steps = re.split(r"\n(?=      - )", "\n" + steps_text)
    return [step for step in steps if step.strip()]


def _step(job: str, marker: str) -> str:
    """The single step of ``job`` whose text contains ``marker``."""
    (step,) = [s for s in _workflow_steps(job) if marker in s]
    return step


def _github_glob(pattern: str) -> re.Pattern[str]:
    """GitHub Actions path filter glob → regex (``**`` crosses directories, ``*`` does not)."""
    out = ""
    i = 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out += "(?:.*/)?"
            i += 3
        elif pattern.startswith("**", i):
            out += ".*"
            i += 2
        elif pattern[i] == "*":
            out += "[^/]*"
            i += 1
        elif pattern[i] == "?":
            out += "[^/]"
            i += 1
        else:
            out += re.escape(pattern[i])
            i += 1
    return re.compile(out + r"\Z")


def _release(version: str) -> tuple[int, ...]:
    """Numeric release segments of a pinned version (``"0.49.1"`` -> ``(0, 49, 1)``).

    Suffixes (``rc1``, ``.post1``) are ignored: the lock file pins final releases (uv
    does not select pre-releases unless asked to), so they do not occur in practice.
    """
    match = re.match(r"\d+(?:\.\d+)*", version)
    assert match, f"unparseable version {version!r}"
    return tuple(int(part) for part in match.group(0).split("."))


def _requirement_pins() -> dict[str, tuple[tuple[int, ...], list[str]]]:
    """``name -> (release, hashes)`` from the hash-locked requirements.txt."""
    pins: dict[str, tuple[tuple[int, ...], list[str]]] = {}
    current: str | None = None
    for line in _text(REQUIREMENTS).splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = re.match(r"([A-Za-z0-9_.\-]+)==([^\s;\\]+)", stripped)
        if match:
            current = match.group(1).lower().replace("_", "-")
            pins[current] = (_release(match.group(2)), [])
        hashes = re.findall(r"--hash=sha256:([0-9a-f]{64})", stripped)
        if hashes:
            assert current is not None, f"hash without a requirement: {stripped}"
            pins[current][1].extend(hashes)
    return pins


# ─── Dockerfile ──────────────────────────────────────────────────────────────


def test_F73_base_image_pinned_by_digest() -> None:
    (base,) = _instructions("FROM")
    assert re.fullmatch(r"python:3\.12-slim-bookworm@sha256:[0-9a-f]{64}", base), base


def test_F73_dependencies_installed_from_hash_locked_requirements() -> None:
    (install,) = [args for args in _instructions("RUN") if "pip install" in args]
    assert "--require-hashes" in install
    assert "--no-cache-dir" in install
    assert "-r requirements.txt" in install
    # Cache-friendly layering: the lock file and the install come before the code.
    assert _index_of("COPY", "requirements.txt") < _index_of("RUN", "pip install")
    assert _index_of("RUN", "pip install") < _index_of("COPY", "ynab_helper/")


def test_F73_runtime_environment_flags() -> None:
    env = " ".join(_instructions("ENV"))
    assert "PYTHONDONTWRITEBYTECODE=1" in env
    assert "PYTHONUNBUFFERED=1" in env


def test_F70_no_user_instruction_entrypoint_drops_privileges() -> None:
    # The entrypoint must start as root to chown the root-owned production bind mount
    # and read the root-only bot.env; a USER line would crash-loop production.
    assert _instructions("USER") == []
    assert _instructions("ENTRYPOINT") == ['["python3", "-m", "ynab_helper.container"]']
    assert _instructions("CMD") == ['["bot"]']


def test_F70_app_user_and_data_dir_prepared_before_volume() -> None:
    (setup,) = [args for args in _instructions("RUN") if "useradd" in args]
    assert f"--gid {APP_ID}" in setup and f"--uid {APP_ID}" in setup
    assert "/usr/sbin/nologin" in setup
    assert re.search(rf"install -d -o {APP_ID} -g {APP_ID} -m 0?700 /app/bot_data", setup)
    assert _instructions("VOLUME") == ['["/app/bot_data"]']
    # Changes to a path after it is declared a VOLUME are discarded by the builder.
    assert _index_of("RUN", "useradd") < _index_of("VOLUME")


def test_F69_healthcheck_runs_the_healthcheck_module() -> None:
    assert _instructions("HEALTHCHECK") == [
        "--interval=60s --timeout=10s --start-period=60s --retries=3 "
        'CMD ["python3", "-m", "ynab_helper.healthcheck"]'
    ]


def test_F40_stop_signal_and_port() -> None:
    assert _instructions("STOPSIGNAL") == ["SIGTERM"]
    assert _instructions("EXPOSE") == ["8080"]


def test_F112_build_metadata_baked_into_the_image() -> None:
    args = _instructions("ARG")
    assert "BUILD_SHA=dev" in args
    assert "BUILD_DATE=local" in args
    env = " ".join(_instructions("ENV"))
    assert "BUILD_SHA=${BUILD_SHA}" in env
    assert "BUILD_DATE=${BUILD_DATE}" in env
    labels = " ".join(_instructions("LABEL"))
    for key in ("source", "revision", "created", "title", "description"):
        assert f"org.opencontainers.image.{key}=" in labels
    # Declared after the dependency layer so a new commit does not reinstall packages.
    assert _index_of("ARG", "BUILD_SHA") > _index_of("RUN", "pip install")


def test_image_copies_only_package_and_shims() -> None:
    copies = _instructions("COPY")
    assert copies == [
        "requirements.txt ./",
        "ynab_helper/ ./ynab_helper/",
        "revolut_to_ynab.py revolut_ynab_bot.py ./",
    ]


def test_dockerignore_is_an_allowlist() -> None:
    patterns = _active_lines(_text(DOCKERIGNORE))
    assert patterns[0] == "*", "the first pattern must exclude everything"
    allowed = {p[1:] for p in patterns if p.startswith("!")}
    assert allowed == {
        "requirements.txt",
        "ynab_helper/**",
        "revolut_to_ynab.py",
        "revolut_ynab_bot.py",
    }
    excluded_after_allowlist = patterns[patterns.index("!revolut_ynab_bot.py") + 1 :]
    for pattern in ("**/__pycache__", "**/*.pyc", "**/.env", "**/*.db", "**/*.csv"):
        assert pattern in excluded_after_allowlist


# ─── CI workflow ─────────────────────────────────────────────────────────────


def test_F68_old_publish_workflow_removed() -> None:
    assert not OLD_WORKFLOW.exists()


def test_F115_every_action_pinned_to_a_full_commit_sha() -> None:
    uses = re.findall(r"^\s*(?:-\s+)?uses:\s*(\S+)(.*)$", _text(CI_WORKFLOW), re.MULTILINE)
    assert uses, "no actions found"
    for ref, trailer in uses:
        assert re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", ref), f"not SHA-pinned: {ref}"
        assert re.fullmatch(r"\s*#\s*v\d+(\.\d+)*", trailer), f"missing version comment: {ref}"


def test_F115_only_the_main_push_publish_job_can_write_packages() -> None:
    # Jobs that run for pull requests (Dependabot's action bumps included) must not hold
    # a token that can move :latest, or unreviewed action code could deploy to production
    # through the pin-and-bump flow without any merge.
    workflow = _text(CI_WORKFLOW)
    assert _yaml_block(workflow, "permissions:").split() == ["contents:", "read"]
    jobs = _workflow_jobs()
    assert set(jobs) == {"test", "image", "publish"}
    writers = [name for name, block in jobs.items() if "packages: write" in block]
    assert writers == ["publish"]
    assert workflow.count("packages: write") == 1
    publish_permissions = _yaml_block(jobs["publish"], "    permissions:").split()
    assert publish_permissions == ["contents:", "read", "packages:", "write"]
    assert _job_key("publish", "if") == PUSH_TO_MAIN
    image_permissions = _yaml_block(jobs["image"], "    permissions:")
    assert image_permissions.split() == ["contents:", "read"]
    # The write-token job runs no repository code: nothing to check out or build.
    publish_steps = "\n".join(_workflow_steps("publish"))
    for forbidden in ("actions/checkout@", "docker/build-push-action@", "docker/setup-buildx"):
        assert forbidden not in publish_steps, forbidden
    for name in jobs:
        for step in _workflow_steps(name):
            if "actions/checkout@" in step:
                assert "persist-credentials: false" in step


def test_F31_image_is_gated_on_tests_and_self_check() -> None:
    assert _job_key("image", "needs") == "test"
    assert _job_key("publish", "needs") == "image"
    test_steps = "\n".join(_workflow_steps("test"))
    for command in (
        "ruff check",
        "ruff format --check",
        "python -m pytest",
        "--require-hashes -r requirements-dev.txt",
        "pip-audit --requirement requirements.txt --require-hashes",
    ):
        assert command in test_steps, command
    image_steps = "\n".join(_workflow_steps("image"))
    selfcheck = image_steps.index('docker run --rm "$IMAGE" selfcheck')
    smoke = image_steps.index("scripts/ci_smoke.sh")
    export = image_steps.index("docker save")
    upload = image_steps.index("actions/upload-artifact@")
    assert selfcheck < smoke < export < upload


def test_F31_publish_pushes_exactly_the_checked_image() -> None:
    # The image job exports the checked image with its checksum; the publish job verifies
    # the checksum and the commit it was built from before loading and pushing it.
    outputs = _yaml_block(_workflow_jobs()["image"], "    outputs:")
    assert "archive_sha256: ${{ steps.save.outputs.sha256 }}" in outputs
    export = _step("image", "docker save")
    assert "sha256sum" in export
    assert '"${IMAGE}:${SHORT_SHA}" "${IMAGE}:latest"' in export
    upload = _step("image", "actions/upload-artifact@")
    assert "if-no-files-found: error" in upload
    download = _step("publish", "actions/download-artifact@")
    assert "digest-mismatch: error" in download
    load = _step("publish", "docker load")
    assert "ARCHIVE_SHA256: ${{ needs.image.outputs.archive_sha256 }}" in load
    checksum = load.index("sha256sum --check --strict")
    loading = load.index("docker load")
    revision = load.index('"org.opencontainers.image.revision"')
    assert checksum < loading < revision
    assert '"${revision}" != "${GITHUB_SHA}"' in load
    steps = "\n".join(_workflow_steps("publish"))
    assert steps.index("docker load") < steps.index("docker/login-action@")
    assert steps.index("docker/login-action@") < steps.index("docker push")


@pytest.mark.parametrize("trigger", ["pull_request:", "workflow_dispatch:", "push:"])
def test_F31_workflow_triggers(trigger: str) -> None:
    on_block = _yaml_block(_text(CI_WORKFLOW), "on:")
    assert re.search(rf"^  {trigger}", on_block, re.MULTILINE)


def test_F71_publishing_only_on_push_to_main() -> None:
    # Registry login and pushes exist only in the publish job, which is skipped for pull
    # requests and manual runs as a whole.
    assert _job_key("publish", "if") == PUSH_TO_MAIN
    for name in ("test", "image"):
        steps = "\n".join(_workflow_steps(name))
        assert "docker/login-action@" not in steps, name
        assert "docker push" not in steps, name
        assert "secrets.GITHUB_TOKEN" not in steps, name
    build = _step("image", "docker/build-push-action@")
    assert "push: false" in build
    assert "load: true" in build
    # Exporting the image is only needed (and only paid for) when it will be published.
    for marker in ("docker save", "actions/upload-artifact@"):
        assert f"if: {PUSH_TO_MAIN}" in _step("image", marker), marker


def test_F109_main_pushes_share_a_queue_that_never_cancels_a_running_publish() -> None:
    # A running main build is never cancelled, so :latest moves in commit order. Pending
    # main runs can still be superseded by a newer one (GitHub keeps one pending run per
    # group), so not every commit gets a :<sha> tag; the workflow comment says so.
    workflow = _text(CI_WORKFLOW)
    concurrency = _yaml_block(workflow, "concurrency:")
    group = re.search(r"group:\s*(.+)", concurrency)
    cancel = re.search(r"cancel-in-progress:\s*(.+)", concurrency)
    assert group and cancel
    assert f"({PUSH_TO_MAIN}) && 'deploy-main'" in group.group(1)
    assert cancel.group(1).strip() == f"${{{{ !({PUSH_TO_MAIN}) }}}}"
    # GitHub rejects `queue: max` together with cancel-in-progress: true (used for PRs).
    assert "queue:" not in concurrency
    assert "are never cancelled" not in workflow


@pytest.mark.parametrize(
    "path",
    [
        "ynab_helper/bot/app.py",
        "ynab_helper/web/templates/base.html",
        "ynab_helper/web/static/app.css",
        "revolut_ynab_bot.py",
        "requirements.txt",
        "Dockerfile",
        ".dockerignore",
        "tests/test_something.py",
        "scripts/ci_smoke.sh",
        ".github/workflows/ci.yml",
    ],
)
def test_F68_code_changes_always_build_and_deploy(path: str) -> None:
    push = _yaml_block(_text(CI_WORKFLOW), "  push:")
    assert not re.search(r"^\s+paths:", push, re.MULTILINE), "no path allowlist on push"
    ignored = re.findall(r'^\s+- "([^"]+)"', _yaml_block(push, "    paths-ignore:"), re.MULTILINE)
    assert ignored
    assert not any(_github_glob(pattern).match(path) for pattern in ignored), path


@pytest.mark.parametrize("path", ["README.md", "docs/OPERATIONS.md", "Caddyfile"])
def test_F68_docs_only_pushes_do_not_restart_production(path: str) -> None:
    push = _yaml_block(_text(CI_WORKFLOW), "  push:")
    ignored = re.findall(r'^\s+- "([^"]+)"', _yaml_block(push, "    paths-ignore:"), re.MULTILINE)
    assert any(_github_glob(pattern).match(path) for pattern in ignored), path


def test_F112_ci_passes_build_metadata() -> None:
    build = _step("image", "docker/build-push-action@")
    assert "BUILD_SHA=${{ steps.meta.outputs.short_sha }}" in build
    assert "BUILD_DATE=${{ steps.meta.outputs.build_date }}" in build
    assert "org.opencontainers.image.revision=${{ github.sha }}" in build
    assert "type=gha" in build


def test_image_tags_lowercased_repository_with_latest_and_short_sha() -> None:
    meta = _step("image", "id: meta")
    assert 'image="ghcr.io/${GITHUB_REPOSITORY,,}"' in meta
    assert 'short_sha="${GITHUB_SHA::7}"' in meta
    outputs = _yaml_block(_workflow_jobs()["image"], "    outputs:")
    assert "image: ${{ steps.meta.outputs.image }}" in outputs
    assert "short_sha: ${{ steps.meta.outputs.short_sha }}" in outputs
    push = _step("publish", "docker push")
    assert "IMAGE: ${{ needs.image.outputs.image }}" in push
    assert "SHORT_SHA: ${{ needs.image.outputs.short_sha }}" in push
    sha_push = push.index('docker push "${IMAGE}:${SHORT_SHA}"')
    latest_push = push.index('docker push "${IMAGE}:latest"')
    assert sha_push < latest_push, "the rollback tag must exist before :latest moves"


def test_ci_smoke_script_is_strict_and_executable() -> None:
    text = _text(SMOKE_SCRIPT)
    assert text.startswith("#!/usr/bin/env bash\n")
    assert "\nset -euo pipefail\n" in text
    assert "--network none" in text, "the smoke test must never reach the internet"
    if os.name == "posix":
        assert os.access(SMOKE_SCRIPT, os.X_OK)


def _smoke_constant(name: str) -> int:
    match = re.search(rf"^readonly {name}=(\d+)$", _text(SMOKE_SCRIPT), re.MULTILINE)
    assert match, name
    return int(match.group(1))


def _smoke_section(number: int) -> str:
    text = _text(SMOKE_SCRIPT)
    start = text.index(f'echo "== {number}. ')
    end = text.find(f'echo "== {number + 1}. ', start)
    return text[start:] if end == -1 else text[start:end]


def test_F40_smoke_stop_budget_matches_production_stop_timeout() -> None:
    # Production stops the bot with a plain `docker stop` or Watchtower: SIGKILL 10 s
    # after SIGTERM (no stop_grace_period on the server). A looser CI budget would pass a
    # bot that production SIGKILLs on every deploy.
    stop_timeout = _smoke_constant("STOP_TIMEOUT")
    budget_ms = _smoke_constant("STOP_BUDGET_MS")
    assert stop_timeout == 10
    assert 8000 <= budget_ms < stop_timeout * 1000  # the bot's documented 8 s must fit
    section = _smoke_section(5)
    assert 'docker stop --time "${STOP_TIMEOUT}"' in section
    assert "elapsed_ms=$(($(now_ms) - start_ms))" in section
    assert '"${exit_code}" != "137" && "${elapsed_ms}" -le "${STOP_BUDGET_MS}"' in section


def test_compat_2_3_smoke_runs_the_documented_entry_points() -> None:
    # selfcheck only imports what exists under ynab_helper/, so a missing CLI or a broken
    # shim would otherwise reach :latest unnoticed.
    section = _smoke_section(4)
    assert "via_entrypoint cli --help" in section
    runs = re.findall(r"python3 revolut_to_ynab\.py --help$", section, re.MULTILINE)
    assert len(runs) == 2  # through the entrypoint and the way `docker exec` runs it
    assert '--entrypoint timeout "${IMAGE}"' in section  # root, entrypoint bypassed
    assert "import revolut_to_ynab, revolut_ynab_bot" in section
    assert 'inspect_sh --user "${APP_ID}:${APP_ID}"' in section
    # The root run is checked for root-owned leftovers before any entrypoint run, which
    # would chown them and hide the problem.
    exec_style = section.index('--entrypoint timeout "${IMAGE}"')
    ownership = section.index("docker exec style left no root-owned files in bot_data")
    assert exec_style < ownership < section.index("via_entrypoint")


# ─── smoke script helpers (executed) ─────────────────────────────────────────

BASH = shutil.which("bash")
needs_bash = pytest.mark.skipif(BASH is None, reason="bash is not installed")


def _smoke_function(name: str) -> str:
    match = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", _text(SMOKE_SCRIPT), re.MULTILINE | re.DOTALL)
    assert match, f"function {name} not found"
    return match.group(0)


def _run_now_ms(extra_env: dict[str, str] | None = None) -> int:
    assert BASH is not None
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), **(extra_env or {})}
    result = subprocess.run(
        [BASH, "-c", _smoke_function("now_ms") + "now_ms"],
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    return int(result.stdout.strip())


def _bash_has_native_epochrealtime() -> bool:
    assert BASH is not None
    # bash >= 5 ignores assignments to its dynamic EPOCHREALTIME variable.
    result = subprocess.run(
        [BASH, "-c", 'EPOCHREALTIME=x; printf %s "${EPOCHREALTIME}"'],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    return result.stdout != "x"


@needs_bash
def test_F40_now_ms_reports_wall_clock_milliseconds() -> None:
    before = time.time() * 1000
    value = _run_now_ms()
    after = time.time() * 1000
    # The whole-second fallback (no EPOCHREALTIME, no perl) may lag by up to 1 s.
    assert before - 1000 <= value <= after + 1000


@needs_bash
@pytest.mark.parametrize("separator", [".", ","])
def test_F40_now_ms_parses_epochrealtime_in_any_locale(separator: str) -> None:
    if _bash_has_native_epochrealtime():
        pytest.skip("this bash computes EPOCHREALTIME itself")
    value = _run_now_ms({"EPOCHREALTIME": f"1760000000{separator}012345"})
    assert value == 1760000000012


# ─── dependencies ────────────────────────────────────────────────────────────


def test_F107_runtime_dependencies_hash_locked_and_patched() -> None:
    pins = _requirement_pins()
    for name, (_, hashes) in pins.items():
        assert hashes, f"{name} has no --hash"
    assert pins["starlette"][0] >= (0, 49, 1)  # CVE-2025-54121, CVE-2025-62727
    assert pins["jinja2"][0] >= (3, 1, 6)
    # Uploads are raw bodies now; the multipart parser and unused signer are gone.
    assert "python-multipart" not in pins
    assert "itsdangerous" not in pins


def test_dependabot_covers_every_pinned_ecosystem() -> None:
    text = _text(DEPENDABOT)
    ecosystems = re.findall(r"package-ecosystem:\s*(\S+)", text)
    assert sorted(ecosystems) == ["docker", "docker-compose", "github-actions", "uv"]
    assert text.count("interval: weekly") == len(ecosystems)
    assert text.count("groups:") == len(ecosystems)


# ─── reference compose file and Caddyfile ────────────────────────────────────


def _compose_services() -> dict[str, str]:
    services = _yaml_block(_text(COMPOSE), "services:")
    names = re.findall(r"^  ([\w-]+):$", services, re.MULTILINE)
    return {name: _yaml_block(services, f"  {name}:") for name in names}


def test_compose_reference_keeps_the_production_contract() -> None:
    services = _compose_services()
    assert set(services) == {"revolut-ynab-bot", "caddy", "watchtower"}
    bot = services["revolut-ynab-bot"]
    assert "image: ghcr.io/cyrill-meier/ynab_helper:latest" in bot
    assert "container_name: revolut-ynab-bot" in bot
    assert "- ./bot.env:/app/.env:ro" in bot
    assert "- ./bot_data:/app/bot_data" in bot
    assert "- TZ=Europe/Zurich" in bot
    assert "PYTHONUNBUFFERED" not in bot, "set in the image"
    assert not re.search(r"^\s+(command|user|entrypoint):", bot, re.MULTILINE)
    assert "stop_grace_period: 20s" in bot
    for service in services.values():
        assert '"com.centurylinklabs.watchtower.scope=ynab"' in service


def test_F110_every_compose_service_rotates_its_logs() -> None:
    anchor = _yaml_block(_text(COMPOSE), "x-logging: &default-logging")
    assert re.search(r"driver: local", anchor)
    assert re.search(r'max-size: "10m"', anchor)
    assert re.search(r'max-file: "3"', anchor)
    for name, service in _compose_services().items():
        assert "logging: *default-logging" in service, name


def test_compose_images_pinned_to_versions() -> None:
    services = _compose_services()
    assert re.search(r"image: caddy:2\.\d+-alpine$", services["caddy"], re.MULTILINE)
    assert re.search(
        r"image: nickfedor/watchtower:\d+\.\d+(\.\d+)?$", services["watchtower"], re.MULTILINE
    )


def _caddy_active() -> str:
    return "\n".join(_active_lines(_text(CADDYFILE)))


def test_F21_caddy_limits_request_bodies_at_the_edge() -> None:
    active = _caddy_active()
    # At least the app's own 26 MB upload cap (MiB >= MB), so Caddy never rejects a body
    # the app would accept, while multi-GB junk never reaches the container.
    assert re.search(r"request_body\s*\{\s*max_size 26MiB\s*\}", active)
    assert "bot.example.com {" in active
    assert "reverse_proxy revolut-ynab-bot:8080" in active


def test_F23_caddy_needs_no_cdn_and_never_overrides_the_app_csp() -> None:
    active = _caddy_active()
    assert "cdn.tailwindcss.com" not in active
    assert "unsafe-inline" not in active
    # `?` = only when the response has none; a plain set in a deferred header block would
    # replace the app's strict policy.
    assert re.search(r"^\s*header \?Content-Security-Policy ", active, re.MULTILINE)
    assert not re.search(r"^\s*Content-Security-Policy ", active, re.MULTILINE)
