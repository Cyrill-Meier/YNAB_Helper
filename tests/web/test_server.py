"""serve_in_thread lifecycle (F94), event loop never blocked (F41), proxy headers + allowlist
(F59) on a real uvicorn server bound to loopback."""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from pathlib import Path

import pytest

from ynab_helper import services
from ynab_helper.errors import ConfigError
from ynab_helper.web.config import WebConfig, parse_allowlist
from ynab_helper.web.server import WebServerHandle, serve_in_thread

from .conftest import SECRET, issue_token


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _config(port: int, **kw) -> WebConfig:
    return WebConfig(
        enabled=True,
        host="127.0.0.1",
        port=port,
        public_url=f"http://127.0.0.1:{port}",
        secret_key=SECRET,
        **kw,
    )


def _get(port: int, path: str, headers: dict[str, str] | None = None, *, method="GET", body=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        response = conn.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        conn.close()


@pytest.fixture
def serve(users_db: Path, data_dir: Path):
    handles: list[WebServerHandle] = []

    def start(config: WebConfig, **kw) -> WebServerHandle:
        handle = serve_in_thread(
            config,
            users_db_path=users_db,
            data_dir=data_dir,
            version_line="v2.0.0 (test)",
            cache_bust="2.0.0-test",
            **kw,
        )
        handles.append(handle)
        return handle

    yield start
    for handle in handles:
        handle.stop(5.0)


def test_serve_in_thread_starts_serves_and_stops(serve):
    port = _free_port()
    handle = serve(_config(port))
    assert handle.wait_started(10)
    assert handle.started.is_set() and handle.failed is None and handle.running
    assert handle.port == port and handle.url == f"http://127.0.0.1:{port}"
    status, headers, body = _get(port, "/health")
    assert (status, json.loads(body)) == (200, {"ok": True})
    assert "server" not in {k.lower() for k in headers}
    handle.stop(5.0)
    assert not handle.running and handle.stopped.is_set() and handle.failed is None
    with pytest.raises(OSError):
        _get(port, "/health")


def test_F94_port_in_use_is_reported_not_swallowed(serve):
    blocker = socket.socket()
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    port = blocker.getsockname()[1]
    try:
        handle = serve(_config(port))
        assert handle.wait_started(5) is False
        assert isinstance(handle.failed, OSError)
        assert not handle.running
    finally:
        blocker.close()


def test_F94_unusable_config_is_reported(serve):
    config = WebConfig(enabled=True, public_url="", secret_key="short")
    handle = serve(config)
    assert handle.wait_started(1) is False
    assert isinstance(handle.failed, ConfigError)
    assert "WEB_UI_SECRET_KEY" in handle.failed.user_message


def test_F59_broken_allowlist_keeps_the_web_ui_off(serve):
    networks, problems = parse_allowlist("203.0.113.700")
    config = _config(
        _free_port(),
        allowed_networks=networks,
        parse_problems=tuple(("WEB_UI_ALLOWED_IPS", p) for p in problems),
    )
    handle = serve(config)
    assert handle.wait_started(1) is False
    assert isinstance(handle.failed, ConfigError)


def test_F94_a_failing_app_startup_is_recorded(serve, monkeypatch):
    from ynab_helper.web.middleware import SessionPurger

    def broken_start(self):
        raise RuntimeError("cannot start the purge thread")

    monkeypatch.setattr(SessionPurger, "start", broken_start)
    handle = serve(_config(_free_port()))
    assert handle.wait_started(10) is False
    assert isinstance(handle.failed, SystemExit)  # uvicorn's exit after a lifespan failure
    assert handle.stopped.is_set() and not handle.running


def test_F41_a_slow_import_does_not_block_other_requests(serve, users_db, monkeypatch):
    entered = threading.Event()
    release = threading.Event()

    def slow_import(ctx, body, **kwargs):
        entered.set()
        release.wait(10)
        raise RuntimeError("never mind")

    monkeypatch.setattr(services, "import_csv", slow_import)
    port = _free_port()
    config = _config(port)
    handle = serve(config)
    assert handle.wait_started(10)

    token = issue_token(users_db, config, now=time.time())
    status, headers, _ = _get(
        port,
        "/auth",
        {"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
        body=f"t={token}",
    )
    assert status == 303
    cookie = headers["set-cookie"].split(";", 1)[0]
    csrf = config.token_hasher().csrf_for(cookie.split("=", 1)[1])
    upload_done = threading.Event()

    def upload() -> None:
        _get(
            port,
            "/api/upload",
            {"Cookie": cookie, "X-CSRF-Token": csrf, "Content-Type": "text/csv"},
            method="POST",
            body=Path(__file__).parent.parent.joinpath("fixtures", "export_basic.csv").read_bytes(),
        )
        upload_done.set()

    threading.Thread(target=upload, daemon=True).start()
    assert entered.wait(10)
    t0 = time.monotonic()
    assert _get(port, "/health")[0] == 200
    assert _get(port, "/")[0] == 200
    assert time.monotonic() - t0 < 1.0  # the event loop is free while the import runs
    release.set()
    assert upload_done.wait(10)


def test_F59_allowlist_uses_the_client_ip_from_proxy_headers(serve):
    networks, problems = parse_allowlist("203.0.113.0/24")
    assert problems == []
    port = _free_port()
    handle = serve(_config(port, allowed_networks=networks, trusted_proxies="127.0.0.1"))
    assert handle.wait_started(10)
    # Direct loopback (the Docker health check): always allowed.
    assert _get(port, "/health")[0] == 200
    # Through the trusted proxy: the forwarded client address decides.
    assert _get(port, "/health", {"X-Forwarded-For": "203.0.113.9"})[0] == 200
    status, headers, body = _get(port, "/health", {"X-Forwarded-For": "198.51.100.1"})
    assert status == 403
    assert json.loads(body)["error"] == "forbidden"
    assert "content-security-policy" in {k.lower() for k in headers}


def test_F59_untrusted_proxy_headers_are_ignored(serve):
    networks, _ = parse_allowlist("203.0.113.0/24")
    port = _free_port()
    handle = serve(_config(port, allowed_networks=networks, trusted_proxies="10.9.9.9"))
    assert handle.wait_started(10)
    # The peer (127.0.0.1) is not a trusted proxy: uvicorn ignores X-Forwarded-For, so an
    # allowed address in it does not get in (it would if the header were honoured) ...
    assert _get(port, "/health", {"X-Forwarded-For": "203.0.113.9"})[0] == 403
    # ... and the loopback peer relaying forwarded traffic is not exempt either.
    assert _get(port, "/health", {"X-Forwarded-For": "198.51.100.1"})[0] == 403
    # Only the direct, header-less request (the Docker health check) is.
    assert _get(port, "/health")[0] == 200


@pytest.mark.parametrize("trusted", ["*", "127.0.0.1"])
@pytest.mark.parametrize(
    "forwarded", ["127.0.0.1", "::1", "127.0.0.1, 198.51.100.1", "::ffff:127.0.0.1"]
)
def test_F59_a_forwarded_loopback_address_is_not_exempt(serve, trusted, forwarded):
    networks, _ = parse_allowlist("203.0.113.0/24")
    port = _free_port()
    handle = serve(_config(port, allowed_networks=networks, trusted_proxies=trusted))
    assert handle.wait_started(10)
    status, _, body = _get(port, "/health", {"X-Forwarded-For": forwarded})
    assert status == 403, (trusted, forwarded)
    assert json.loads(body)["error"] == "forbidden"


def test_F59_allowlist_with_trust_everything_logs_a_warning(users_db, data_dir, caplog):
    from ynab_helper.web.app import make_app

    networks, _ = parse_allowlist("203.0.113.0/24")
    with caplog.at_level("WARNING", logger="ynab_helper.web.app"):
        make_app(
            _config(8080, allowed_networks=networks),  # WEB_UI_TRUSTED_PROXIES default '*'
            users_db_path=users_db,
            data_dir=data_dir,
            version_line="v",
            cache_bust="c",
        )
    assert any("WEB_UI_TRUSTED_PROXIES" in r.getMessage() for r in caplog.records)
    caplog.clear()
    with caplog.at_level("WARNING", logger="ynab_helper.web.app"):
        make_app(
            _config(8080, allowed_networks=networks, trusted_proxies="172.18.0.2"),
            users_db_path=users_db,
            data_dir=data_dir,
            version_line="v",
            cache_bust="c",
        )
    assert not any("WEB_UI_TRUSTED_PROXIES" in r.getMessage() for r in caplog.records)


def test_session_purge_thread_runs_with_the_server(serve):
    port = _free_port()
    handle = serve(_config(port))
    assert handle.wait_started(10)
    purger = handle._server.config.app.state.web.purger
    thread = purger._thread
    assert thread is not None and thread.is_alive() and thread.daemon
    handle.stop(5.0)
    assert not thread.is_alive()


def test_stop_is_idempotent_and_safe_before_start(serve):
    handle = serve(WebConfig(enabled=False))
    handle.stop()
    handle.stop()
    assert handle.failed is not None


def test_importing_the_server_module_does_not_load_fastapi():
    import subprocess
    import sys

    code = (
        "import sys; import ynab_helper.web.server; "
        "bad = [m for m in ('fastapi', 'starlette', 'uvicorn', 'jinja2') if m in sys.modules]; "
        "print(bad); raise SystemExit(1 if bad else 0)"
    )
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=root, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
