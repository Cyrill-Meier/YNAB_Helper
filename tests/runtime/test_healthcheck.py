"""healthcheck.py: heartbeat freshness + local web /health probe (F69)."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from ynab_helper import config, healthcheck

REPO_ROOT = Path(__file__).resolve().parents[2]


class _Handler(BaseHTTPRequestHandler):
    status = 200
    delay = 0.0
    location = ""

    def do_GET(self) -> None:
        if self.delay:
            time.sleep(self.delay)
        status = 200 if self.path == "/health" else 404
        status = self.status if status == 200 else status
        self.send_response(status)
        if self.location:
            self.send_header("Location", self.location)
        body = b'{"ok": true}'
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: object) -> None:
        pass


def _serve(status: int = 200, delay: float = 0.0, location: str = "") -> ThreadingHTTPServer:
    handler = type("H", (_Handler,), {"status": status, "delay": delay, "location": location})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.daemon_threads = True
    threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    ).start()
    return server


@pytest.fixture
def web_server() -> Iterator[ThreadingHTTPServer]:
    server = _serve()
    yield server
    server.shutdown()
    server.server_close()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _beat(data_dir: Path, age: float) -> None:
    path = data_dir / config.HEARTBEAT_FILENAME
    path.touch()
    stamp = time.time() - age
    os.utime(path, (stamp, stamp))


# ─── heartbeat ───────────────────────────────────────────────────────────────


def test_fresh_heartbeat_is_healthy(tmp_path: Path) -> None:
    _beat(tmp_path, age=5)
    report = healthcheck.run_checks({"BOT_DATA_DIR": str(tmp_path)})
    assert report.ok
    assert report.web is None
    assert report.line.startswith("OK: heartbeat 5s old; web UI disabled")


def test_F69_stale_heartbeat_is_unhealthy(tmp_path: Path) -> None:
    """A hung poll loop used to look healthy forever."""
    _beat(tmp_path, age=300)
    report = healthcheck.run_checks({"BOT_DATA_DIR": str(tmp_path)})
    assert not report.ok
    assert report.line.startswith("UNHEALTHY: heartbeat 300s old (max 180s)")


def test_missing_heartbeat_is_unhealthy(tmp_path: Path) -> None:
    report = healthcheck.run_checks({"BOT_DATA_DIR": str(tmp_path)})
    assert not report.ok
    assert "missing" in report.heartbeat.detail


def test_heartbeat_from_the_future_counts_as_fresh(tmp_path: Path) -> None:
    _beat(tmp_path, age=-3600)
    assert healthcheck.run_checks({"BOT_DATA_DIR": str(tmp_path)}).ok


def test_custom_max_age(tmp_path: Path) -> None:
    _beat(tmp_path, age=100)
    env = {"BOT_DATA_DIR": str(tmp_path), "HEALTHCHECK_MAX_AGE": "60"}
    assert not healthcheck.run_checks(env).ok
    env["HEALTHCHECK_MAX_AGE"] = "600.5"
    assert healthcheck.run_checks(env).ok


@pytest.mark.parametrize("raw", ["abc", "-1", "0", "nan", "inf"])
def test_invalid_max_age_uses_default_and_says_so(tmp_path: Path, raw: str) -> None:
    _beat(tmp_path, age=100)
    report = healthcheck.run_checks({"BOT_DATA_DIR": str(tmp_path), "HEALTHCHECK_MAX_AGE": raw})
    assert report.ok
    assert "invalid HEALTHCHECK_MAX_AGE" in report.line


def test_check_heartbeat_uses_given_clock(tmp_path: Path) -> None:
    path = tmp_path / "hb"
    path.touch()
    mtime = path.stat().st_mtime
    assert healthcheck.check_heartbeat(path, max_age=10, now=mtime + 9).ok
    assert not healthcheck.check_heartbeat(path, max_age=10, now=mtime + 11).ok


# ─── web probe ───────────────────────────────────────────────────────────────


def _web_env(data_dir: Path, port: int, **extra: str) -> dict[str, str]:
    return {
        "BOT_DATA_DIR": str(data_dir),
        "WEB_UI_ENABLED": "1",
        "WEB_UI_PORT": str(port),
        **extra,
    }


def test_web_enabled_and_up(tmp_path: Path, web_server: ThreadingHTTPServer) -> None:
    _beat(tmp_path, age=1)
    report = healthcheck.run_checks(_web_env(tmp_path, web_server.server_address[1]))
    assert report.ok
    assert report.line == "OK: heartbeat 1s old; web /health 200"


def test_web_wildcard_host_probes_loopback(tmp_path: Path, web_server: ThreadingHTTPServer) -> None:
    _beat(tmp_path, age=1)
    env = _web_env(tmp_path, web_server.server_address[1], WEB_UI_HOST="0.0.0.0")
    assert healthcheck.run_checks(env).ok


@pytest.mark.parametrize("status", [500, 503, 401, 403])
def test_web_error_status_is_unhealthy(tmp_path: Path, status: int) -> None:
    server = _serve(status=status)
    try:
        _beat(tmp_path, age=1)
        report = healthcheck.run_checks(_web_env(tmp_path, server.server_address[1]))
    finally:
        server.shutdown()
        server.server_close()
    assert not report.ok
    assert f"returned {status}" in report.line


def test_web_redirect_is_not_followed(tmp_path: Path) -> None:
    server = _serve(status=302, location="/?expired=1")
    try:
        result = healthcheck.check_web("127.0.0.1", server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()
    assert not result.ok
    assert "302" in result.detail


def test_web_down_is_unhealthy_even_with_fresh_heartbeat(tmp_path: Path) -> None:
    _beat(tmp_path, age=1)
    report = healthcheck.run_checks(_web_env(tmp_path, _free_port()))
    assert not report.ok
    assert report.heartbeat.ok
    assert "web /health failed" in report.line


def test_web_timeout(tmp_path: Path) -> None:
    server = _serve(delay=1.0)
    try:
        started = time.monotonic()
        result = healthcheck.check_web("127.0.0.1", server.server_address[1], timeout=0.2)
        elapsed = time.monotonic() - started
    finally:
        server.shutdown()
        server.server_close()
    assert not result.ok
    assert elapsed < 0.9


def test_web_disabled_values_skip_probe(tmp_path: Path) -> None:
    _beat(tmp_path, age=1)
    for raw in ("0", "", "false", "nonsense"):
        env = _web_env(tmp_path, _free_port(), WEB_UI_ENABLED=raw)
        assert healthcheck.run_checks(env).web is None


@pytest.mark.parametrize(
    ("host", "reason"),
    [
        ("bad host", "invalid WEB_UI_HOST"),  # http.client.InvalidURL in the constructor
        ("a\nb", "invalid WEB_UI_HOST"),
        ("x" * 300, "web /health failed"),  # UnicodeError (a ValueError) from IDNA encoding
    ],
    ids=["space", "newline", "label-too-long"],
)
def test_malformed_web_host_is_unhealthy_not_a_crash(
    tmp_path: Path, host: str, reason: str
) -> None:
    """Review: WEB_UI_HOST='bad host' crashed the health check with a traceback."""
    _beat(tmp_path, age=1)
    report = healthcheck.run_checks(_web_env(tmp_path, _free_port(), WEB_UI_HOST=host))
    assert not report.ok
    assert report.heartbeat.ok
    assert reason in report.line
    assert "\n" not in report.line


def test_check_web_never_raises_on_malformed_host() -> None:
    result = healthcheck.check_web("bad host", 8080, timeout=0.2)
    assert result == healthcheck.CheckResult(False, "web /health failed (invalid WEB_UI_HOST)")


def test_heartbeat_path_with_nul_is_unhealthy() -> None:
    result = healthcheck.check_heartbeat(Path("/x\x00y"), max_age=180, now=time.time())
    assert result == healthcheck.CheckResult(False, "heartbeat path invalid")


def test_invalid_web_port_probes_default_and_notes_it(tmp_path: Path) -> None:
    _beat(tmp_path, age=1)
    env = {"BOT_DATA_DIR": str(tmp_path), "WEB_UI_ENABLED": "1", "WEB_UI_PORT": "80a"}
    report = healthcheck.run_checks(env)
    assert "invalid WEB_UI_PORT" in report.line


# ─── main() ──────────────────────────────────────────────────────────────────


def test_main_reads_dotenv_and_prints_one_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Docker's health check does not see what the entrypoint loaded from /app/.env."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    env_file = tmp_path / ".env"
    env_file.write_text(f"BOT_DATA_DIR={data_dir}\nWEB_UI_ENABLED=0\n")
    monkeypatch.setattr(config, "DEFAULT_ENV_FILE", env_file)
    for name in ("BOT_DATA_DIR", "WEB_UI_ENABLED", "HEALTHCHECK_MAX_AGE"):
        monkeypatch.delenv(name, raising=False)

    assert healthcheck.main() == 1
    out = capsys.readouterr().out
    assert out.count("\n") == 1 and out.startswith("UNHEALTHY: heartbeat")

    _beat(data_dir, age=0)
    assert healthcheck.main() == 0
    assert capsys.readouterr().out.startswith("OK: heartbeat")


def test_main_reports_unexpected_errors_on_one_line(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def explode(_env: object, **_kwargs: object) -> healthcheck.HealthReport:
        raise RuntimeError("boom\nwith a second line")

    monkeypatch.setattr(healthcheck, "run_checks", explode)
    assert healthcheck.main() == 1
    assert capsys.readouterr().out == "UNHEALTHY: health check error (RuntimeError)\n"


def _subprocess_env(**extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("WEB_UI_", "BOT_DATA"))}
    env["PYTHONPATH"] = str(REPO_ROOT)
    env.update(extra)
    return env


def test_module_entrypoint_exit_codes(tmp_path: Path) -> None:
    env = _subprocess_env(BOT_DATA_DIR=str(tmp_path), WEB_UI_ENABLED="0")
    cmd = [sys.executable, "-m", "ynab_helper.healthcheck"]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=60)
    assert proc.returncode == 1, proc.stderr
    assert proc.stdout.startswith("UNHEALTHY")
    _beat(tmp_path, age=0)
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.startswith("OK")


def test_module_entrypoint_with_bad_web_host_prints_one_line(tmp_path: Path) -> None:
    """The reviewer's reproduction: exit 1 with one status line, no traceback."""
    _beat(tmp_path, age=0)
    env = _subprocess_env(BOT_DATA_DIR=str(tmp_path), WEB_UI_ENABLED="1", WEB_UI_HOST="bad host")
    cmd = [sys.executable, "-m", "ynab_helper.healthcheck"]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=60)
    assert proc.returncode == 1
    assert proc.stdout.count("\n") == 1
    assert proc.stdout.startswith("UNHEALTHY: heartbeat ")
    assert "web /health failed (invalid WEB_UI_HOST)" in proc.stdout
    assert "Traceback" not in proc.stderr


def test_healthcheck_does_not_import_fastapi() -> None:
    code = (
        "import sys, ynab_helper.healthcheck; "
        "bad = [m for m in ('fastapi', 'starlette', 'uvicorn', 'jinja2') if m in sys.modules]; "
        "sys.exit(1 if bad else 0)"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], env=_subprocess_env(), capture_output=True, timeout=60
    )
    assert proc.returncode == 0, proc.stderr
