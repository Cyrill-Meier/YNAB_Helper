"""Unit tests: rate limiter (F87), allowlist (F59), template context, background helpers,
devserver seeding."""

from __future__ import annotations

import threading
from datetime import date
from pathlib import Path

import pytest

from ynab_helper.web.background import BackgroundSyncer, DetachedRunner
from ynab_helper.web.config import WebConfig, parse_allowlist
from ynab_helper.web.deps import NAV, PAGES
from ynab_helper.web.ratelimit import SlidingWindowLimiter

from .conftest import USER_ID, Clock, TestClient, ctx_for, issue_token, login, make_ready

# ── rate limiter (F87) ──────────────────────────────────────────────────────


def test_F87_limiter_allows_limit_per_window():
    clock = Clock(0.0)
    limiter = SlidingWindowLimiter(3, 60.0, clock=clock)
    assert [limiter.allow("a") for _ in range(4)] == [True, True, True, False]
    assert limiter.allow("b")
    assert limiter.retry_after("a") == pytest.approx(60.0)
    clock.advance(60.1)
    assert limiter.allow("a")


def test_F87_limiter_evicts_stale_keys():
    clock = Clock(0.0)
    limiter = SlidingWindowLimiter(10, 60.0, clock=clock)
    for i in range(5000):
        limiter.allow(f"10.0.{i // 256}.{i % 256}")
    assert len(limiter) == 4096  # bounded even while all are fresh
    clock.advance(61)
    limiter.allow("1.1.1.1")
    assert len(limiter) == 1  # every stale bucket was dropped


def test_F87_limiter_has_no_global_cap_that_a_flood_could_fill():
    clock = Clock(0.0)
    limiter = SlidingWindowLimiter(10, 60.0, clock=clock)
    # 400 rotating addresses x 10 attempts each: nobody else is locked out by them.
    for i in range(400):
        assert all(limiter.allow(f"198.51.{i // 256}.{i % 256}") for _ in range(10))
    assert limiter.allow("203.0.113.7")
    with pytest.raises(TypeError):
        SlidingWindowLimiter(10, 60.0, global_limit=5)  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ("ip", "key"),
    [
        ("203.0.113.7", "203.0.113.7"),
        ("2001:db8:1:2:aaaa::1", "2001:db8:1:2::/64"),
        ("2001:db8:1:2:ffff:ffff:ffff:ffff", "2001:db8:1:2::/64"),
        ("::ffff:203.0.113.7", "203.0.113.7"),
        (None, "unknown"),
        ("testclient", "testclient"),
    ],
)
def test_F87_ipv6_clients_are_limited_per_64(ip, key):
    from ynab_helper.web.ratelimit import client_key

    assert client_key(ip) == key


def test_limiter_release_gives_the_slot_back():
    clock = Clock(0.0)
    limiter = SlidingWindowLimiter(1, 10.0, clock=clock)
    assert limiter.allow("u")
    assert not limiter.allow("u")
    limiter.release("u")
    assert limiter.allow("u")
    limiter.release("never-seen")  # harmless
    assert len(limiter) == 1


def test_limiter_rejects_bad_arguments():
    with pytest.raises(ValueError):
        SlidingWindowLimiter(0)
    with pytest.raises(ValueError):
        SlidingWindowLimiter(1, 0)


def test_limiter_is_thread_safe():
    limiter = SlidingWindowLimiter(1000, 60.0)
    allowed = []

    def hammer() -> None:
        allowed.extend(limiter.allow("same") for _ in range(400))

    threads = [threading.Thread(target=hammer) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert allowed.count(True) == 1000


# ── allowlist (F59) ─────────────────────────────────────────────────────────


def _app_with(config: WebConfig, users_db: Path, data_dir: Path):
    from ynab_helper.web.app import make_app

    return make_app(
        config, users_db_path=users_db, data_dir=data_dir, version_line="v", cache_bust="c"
    )


def test_F59_allowlist_admits_listed_networks_and_loopback(users_db, data_dir):
    networks, problems = parse_allowlist("203.0.113.0/24, 2001:db8::/32")
    assert problems == []
    config = WebConfig(
        enabled=True,
        public_url="http://testserver",
        secret_key="s" * 40,
        allowed_networks=networks,
    )
    app = _app_with(config, users_db, data_dir)
    for ip, status in [
        ("203.0.113.77", 200),
        ("2001:db8::1", 200),
        ("127.0.0.1", 200),
        ("::1", 200),
        ("198.51.100.1", 403),
        ("testclient", 403),  # not an address: refused when an allowlist exists
    ]:
        with TestClient(app, client=(ip, 1234)) as c:
            response = c.get("/health")
            assert response.status_code == status, ip
            if status == 403:
                assert response.json()["error"] == "forbidden"


def test_F59_allowlist_runs_before_auth_and_before_the_handler_reads_the_body(users_db, data_dir):
    networks, _ = parse_allowlist("203.0.113.0/24")
    config = WebConfig(
        enabled=True, public_url="http://t", secret_key="s" * 40, allowed_networks=networks
    )
    app = _app_with(config, users_db, data_dir)
    with TestClient(app, client=("198.51.100.1", 1)) as c:
        assert c.post("/auth", data={"t": "x" * 43}).status_code == 403


def test_F59_middleware_order_for_chunked_bodies_from_a_refused_client(users_db, data_dir):
    """The documented order (DESIGN §9.1): the size limit is outermost. A chunked body on a
    small-body path is read by it (at most that path's limit, 16 KiB here) before the
    allowlist refuses the client; an oversized one is a 413 first. Nothing reaches a
    handler either way."""
    networks, _ = parse_allowlist("203.0.113.0/24")
    config = WebConfig(
        enabled=True, public_url="http://t", secret_key="s" * 40, allowed_networks=networks
    )
    app = _app_with(config, users_db, data_dir)
    with TestClient(app, client=("198.51.100.1", 1)) as c:
        small = c.post("/auth", content=iter([b"t=", b"x" * 43]))
        assert small.status_code == 403
        big = c.post("/auth", content=iter([b"x" * 8192] * 3))
        assert big.status_code == 413


def _asgi_status(app, ip: str, headers: list[tuple[bytes, bytes]] | None = None) -> int:
    import asyncio

    sent = []

    async def send(message):
        sent.append(message)

    async def receive():
        return {"type": "http.request", "body": b""}

    scope = {"type": "http", "path": "/", "client": (ip, 1), "headers": headers or []}
    asyncio.run(app(scope, receive, send))
    return sent[0]["status"]


async def _ok(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b""})


@pytest.mark.parametrize(
    "header", [b"x-forwarded-for", b"forwarded", b"x-real-ip", b"X-Forwarded-For"]
)
def test_F59_loopback_exemption_only_without_proxy_headers(header):
    from ynab_helper.web.middleware import IPAllowlistMiddleware

    networks, _ = parse_allowlist("203.0.113.0/24")
    app = IPAllowlistMiddleware(_ok, config=WebConfig(allowed_networks=networks))
    assert _asgi_status(app, "127.0.0.1") == 200  # the health check
    assert _asgi_status(app, "::1") == 200
    forwarded = [(header.lower(), b"198.51.100.1")]
    # A loopback client address next to proxy headers is a spoof or a relaying proxy.
    assert _asgi_status(app, "127.0.0.1", forwarded) == 403
    assert _asgi_status(app, "::ffff:127.0.0.1", forwarded) == 403
    assert _asgi_status(app, "203.0.113.5", forwarded) == 200
    # Loopback listed explicitly still works for forwarded requests.
    networks, _ = parse_allowlist("203.0.113.0/24, 127.0.0.0/8")
    listed = IPAllowlistMiddleware(_ok, config=WebConfig(allowed_networks=networks))
    assert _asgi_status(listed, "127.0.0.1", forwarded) == 200


def test_F59_broken_allowlist_is_refused_by_make_app(users_db, data_dir):
    from ynab_helper.errors import ConfigError

    networks, problems = parse_allowlist("10.0.0.0/8; 192.168.0.0/16")
    assert problems
    config = WebConfig(
        enabled=True,
        public_url="http://t",
        secret_key="s" * 40,
        allowed_networks=networks,
        parse_problems=tuple(("WEB_UI_ALLOWED_IPS", p) for p in problems),
    )
    with pytest.raises(ConfigError):
        _app_with(config, users_db, data_dir)


def test_F59_middleware_with_a_failed_allowlist_admits_only_loopback():
    import asyncio

    from ynab_helper.web.middleware import IPAllowlistMiddleware

    config = WebConfig(parse_problems=(("WEB_UI_ALLOWED_IPS", "bad"),))
    assert config.allowlist_failed

    async def inner(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    app = IPAllowlistMiddleware(inner, config=config)

    def status_for(ip: str) -> int:
        sent = []

        async def send(message):
            sent.append(message)

        async def receive():
            return {"type": "http.request", "body": b""}

        scope = {"type": "http", "path": "/", "client": (ip, 1), "headers": []}
        asyncio.run(app(scope, receive, send))
        return sent[0]["status"]

    assert status_for("127.0.0.1") == 200
    assert status_for("203.0.113.1") == 403
    # With proxy headers even loopback is refused while the allowlist is broken.
    assert _asgi_status(app, "127.0.0.1", [(b"x-forwarded-for", b"127.0.0.1")]) == 403


# ── template context contract ───────────────────────────────────────────────


@pytest.mark.parametrize("page", list(PAGES))
def test_every_page_renders_with_the_context_contract(
    client: TestClient, users_db, config, clock, page
):
    csrf = login(client, users_db, config, clock)
    response = client.get(f"/app/{page}")
    assert response.status_code == 200
    html = response.text
    assert f'data-page="{page}"' in html
    assert f'<meta name="csrf-token" content="{csrf}">' in html
    assert "/static/2.0.0-test/" in html
    assert "v2.0.0 (test)" in html
    assert 'aria-current="page"' in html
    assert "test-token" not in html  # the YNAB token never reaches a page


def test_template_context_keys(app, users_db, config, clock):
    from ynab_helper.web.deps import AuthedUser, resolve_session, template_context

    with TestClient(app) as c:
        login(c, users_db, config, clock)
        token = c.cookies.get("rynab_session")
    state = app.state.web
    authed = resolve_session(state, token)
    assert isinstance(authed, AuthedUser)
    context = template_context(state, page="dashboard", authed=authed)
    assert set(context) == {
        "page",
        "csrf_token",
        "user",
        "version_line",
        "cache_bust",
        "static_base",
        "nav",
        "expired",
        "notice",
    }
    assert context["notice"] is None
    assert context["nav"] == NAV and context["nav"][0] == (
        "dashboard",
        "Dashboard",
        "/app/dashboard",
    )
    summary = context["user"].to_dict()
    assert set(summary) == {
        "telegram_id",
        "first_name",
        "username",
        "budget_name",
        "account_name",
        "auto_approve",
        "crypto_configured",
        "session_expires_at",
    }


def test_user_supplied_names_are_escaped(client: TestClient, users_db, config, clock):
    from ynab_helper.storage import connect

    conn = connect(users_db)
    try:
        make_ready(conn, USER_ID, first_name="<script>alert(1)</script>")
    finally:
        conn.close()
    login(client, users_db, config, clock)
    html = client.get("/app/dashboard").text
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html


def test_unsafe_cache_bust_token_falls_back(users_db, data_dir):
    from ynab_helper.web.app import make_app

    config = WebConfig(enabled=True, public_url="http://t", secret_key="s" * 40)
    app = make_app(
        config,
        users_db_path=users_db,
        data_dir=data_dir,
        version_line="v",
        cache_bust="../../etc",
    )
    assert app.state.web.cache_bust == "dev"


# ── background helpers ──────────────────────────────────────────────────────


def test_background_syncer_is_single_flight_and_throttled(data_dir):
    clock = Clock(0.0)
    release = threading.Event()
    runs = []

    def run(ctx):
        runs.append(ctx.key)
        release.wait(5)

    syncer = BackgroundSyncer(run, min_interval=30, clock=clock)
    ctx = ctx_for(data_dir)
    assert syncer.trigger(ctx) is True
    assert syncer.trigger(ctx) is True  # running: no second thread
    release.set()
    assert syncer.wait_idle(ctx.key, 5)
    assert syncer.trigger(ctx) is False  # throttled
    clock.advance(31)
    assert syncer.trigger(ctx) is True
    assert syncer.wait_idle(ctx.key, 5)
    assert runs == [ctx.key, ctx.key]
    syncer.close()
    clock.advance(31)
    assert syncer.trigger(ctx) is False


def test_background_syncer_survives_errors(data_dir):
    from ynab_helper.errors import Busy, YnabAuthError

    errors = iter([Busy("import"), YnabAuthError("401"), RuntimeError("bug")])

    def run(ctx):
        raise next(errors)

    clock = Clock(0.0)
    syncer = BackgroundSyncer(run, min_interval=0, clock=clock)
    ctx = ctx_for(data_dir)
    for _ in range(3):
        syncer.trigger(ctx)
        assert syncer.wait_idle(ctx.key, 5)
    assert not syncer.running(ctx.key)


def test_detached_runner_bounds_concurrency_and_swallows_errors():
    gate = threading.Event()
    runner = DetachedRunner(max_concurrent=1)
    first = runner.submit(gate.wait, 5)
    assert first is not None
    assert runner.submit(print, "dropped") is None  # over the bound: dropped, not queued
    gate.set()
    first.join(5)
    failing = runner.submit(lambda: 1 / 0)
    assert failing is not None
    failing.join(5)
    assert runner.submit(lambda: None) is not None


# ── devserver ───────────────────────────────────────────────────────────────


def test_devserver_seed_is_offline_and_complete(tmp_path, monkeypatch):
    from ynab_helper import services
    from ynab_helper.web import devserver

    # seed() repoints the service hooks at its fake; restore them afterwards.
    monkeypatch.setattr(services, "client_factory", services.client_factory)
    monkeypatch.setattr(services, "token_client_factory", services.token_client_factory)
    data = devserver.seed(tmp_path / "dev", today=date(2026, 10, 17))
    assert data.users_db_path.exists() and len(data.stray_ids) == 2
    config = WebConfig(
        enabled=True, public_url="http://testserver", secret_key=devserver.DEV_SECRET
    )
    from ynab_helper.web.app import make_app

    app = make_app(
        config,
        users_db_path=data.users_db_path,
        data_dir=data.data_dir,
        version_line="dev",
        cache_bust="dev",
        clock=Clock(1_792_238_400.0),  # 2026-10-17
    )
    clock_now = app.state.web.clock()
    with TestClient(app) as c:
        url = devserver._login_url(data, config.token_hasher(), "http://testserver", 600)
        assert url.startswith("http://testserver/auth?t=")
        token = issue_token(data.users_db_path, config, devserver.DEV_USER_ID, now=clock_now)
        assert c.post("/auth", data={"t": token}, follow_redirects=False).status_code == 303
        csrf = {"X-CSRF-Token": config.token_hasher().csrf_for(c.cookies.get("rynab_session"))}
        dashboard = c.get("/api/dashboard").json()
        assert dashboard["last_statement"]["status"] == "imported"
        assert dashboard["balances"]["working"] > 0
        accounts = {a["name"] for a in c.get("/api/accounts").json()["items"]}
        assert {"Revolut", "Visa Card", "Savings", "Crypto Portfolio"} <= accounts
        preview = c.get("/api/reconcile/preview", headers=csrf).json()
        assert preview["delta"] == devserver.STATEMENT_GAP and preview["token"]
        scan = c.get("/api/dedupe/scan", headers=csrf).json()
        assert sorted(x["id"] for x in scan["candidates"]) == sorted(data.stray_ids)
        items = c.get("/api/subscriptions").json()["items"]
        subs = {s["payee_name"].lower() for s in items}
        assert {"spotify", "swisscom (schweiz) ag"} <= subs
        assert any("netflix" in s for s in subs)
        # F98: the yearly charge five months ago is found.
        assert ("sbb halbtax", "yearly") in {(s["payee_name"].lower(), s["cadence"]) for s in items}
        # Groceries and train tickets at varying amounts are not subscriptions.
        assert not subs & {"denner", "coop", "migros", "sbb cff ffs"}
        cats = c.get("/api/categories").json()
        none = c.get("/api/transactions", params={"category": "__none__"}).json()
        assert cats["uncategorized"] == none["total"]
        spending = c.get("/api/spending").json()
        assert spending["total"] > 0
        app.state.web.syncer.wait_idle(str(devserver.DEV_USER_ID), 5)
    assert clock_now == 1_792_238_400.0
