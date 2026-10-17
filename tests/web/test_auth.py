"""Sign-in, sessions and sign-out (DESIGN §9.1; F18, F24, F61, F86, F87, F88)."""

from __future__ import annotations

import threading
from http.cookies import SimpleCookie
from pathlib import Path

import pytest

from ynab_helper import sessions
from ynab_helper.sessions import SESSION_COOKIE
from ynab_helper.storage import UserStore, connect
from ynab_helper.web.config import WebConfig

from .conftest import (
    OTHER_ID,
    USER_ID,
    Deleted,
    TestClient,
    issue_token,
    login,
    make_ready,
    wait_for,
)


def _session_rows(users_db: Path, kind: str = "session") -> int:
    conn = connect(users_db)
    try:
        return conn.execute("SELECT COUNT(*) FROM web_sessions WHERE kind = ?", (kind,)).fetchone()[
            0
        ]
    finally:
        conn.close()


def _set_cookie(response) -> SimpleCookie:
    cookie = SimpleCookie()
    for header in response.headers.get_list("set-cookie"):
        cookie.load(header)
    return cookie


# ── GET /auth: an interstitial, never a sign-in ─────────────────────────────


def test_F88_get_auth_renders_a_post_form_and_does_not_consume_the_token(
    client: TestClient, users_db, config, clock
):
    token = issue_token(users_db, config, now=clock())
    page = client.get("/auth", params={"t": token})
    assert page.status_code == 200
    assert 'method="post" action="/auth"' in page.text
    assert f'name="t" value="{token}"' in page.text
    assert "Continue" in page.text and "/js/auth.js" in page.text
    assert SESSION_COOKIE not in client.cookies
    assert _session_rows(users_db) == 0
    # The token still works: a link unfurler's GET did not burn it.
    assert client.post("/auth", data={"t": token}, follow_redirects=False).status_code == 303


def test_get_auth_without_token_goes_to_the_login_page(client: TestClient):
    response = client.get("/auth", follow_redirects=False)
    assert (response.status_code, response.headers["location"]) == (303, "/")


@pytest.mark.parametrize("bad", ["short", "x" * 300, "abc<script>alert(1)</script>defghijklm"])
def test_get_auth_with_a_malformed_token_is_not_echoed(client: TestClient, bad):
    response = client.get("/auth", params={"t": bad})
    assert response.status_code == 400
    assert "<script>alert" not in response.text and bad not in response.text


# ── POST /auth ──────────────────────────────────────────────────────────────


def test_post_auth_signs_in_with_the_documented_cookie(client: TestClient, users_db, config, clock):
    token = issue_token(users_db, config, now=clock())
    response = client.post("/auth", data={"t": token}, follow_redirects=False)
    assert (response.status_code, response.headers["location"]) == (303, "/app/dashboard")
    morsel = _set_cookie(response)[SESSION_COOKIE]
    assert morsel["httponly"] is True
    assert morsel["samesite"].lower() == "lax"
    assert morsel["max-age"] == str(config.session_absolute_ttl)
    assert morsel["path"] == "/"
    assert not morsel["secure"]  # public URL is http in this test
    assert client.get("/api/me").json()["telegram_id"] == USER_ID


def test_cookie_is_secure_when_the_public_url_is_https(users_db, data_dir, clock):
    from ynab_helper.web.app import make_app

    config = WebConfig(enabled=True, public_url="https://bot.example.com", secret_key="s" * 40)
    app = make_app(
        config,
        users_db_path=users_db,
        data_dir=data_dir,
        version_line="v",
        cache_bust="c",
        clock=clock,
    )
    token = issue_token(users_db, config, now=clock())
    with TestClient(app, base_url="https://bot.example.com") as c:
        response = c.post("/auth", data={"t": token}, follow_redirects=False)
    assert response.status_code == 303
    assert _set_cookie(response)[SESSION_COOKIE]["secure"] is True


def test_post_auth_with_an_expired_token_is_refused(client: TestClient, users_db, config, clock):
    token = issue_token(users_db, config, now=clock())
    clock.advance(config.login_ttl + 1)
    response = client.post("/auth", data={"t": token}, follow_redirects=False)
    assert response.status_code == 400
    assert "expired or was already used" in response.text
    assert SESSION_COOKIE not in client.cookies


def test_post_auth_token_is_single_use(client: TestClient, users_db, config, clock):
    token = issue_token(users_db, config, now=clock())
    assert client.post("/auth", data={"t": token}, follow_redirects=False).status_code == 303
    client.cookies.clear()
    again = client.post("/auth", data={"t": token}, follow_redirects=False)
    assert again.status_code == 400
    assert _session_rows(users_db) == 1


def test_F86_concurrent_use_of_one_token_mints_one_session(app, users_db, config, clock):
    token = issue_token(users_db, config, now=clock())
    barrier = threading.Barrier(4)
    statuses: list[int] = []

    def attempt() -> None:
        with TestClient(app) as c:
            barrier.wait()
            statuses.append(c.post("/auth", data={"t": token}, follow_redirects=False).status_code)

    threads = [threading.Thread(target=attempt) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert sorted(statuses) == [303, 400, 400, 400]
    assert _session_rows(users_db) == 1


def test_F87_auth_is_rate_limited_to_10_per_minute_per_ip(client: TestClient, clock):
    for _ in range(10):
        assert client.post("/auth", data={"t": "x" * 43}).status_code == 400
    limited = client.post("/auth", data={"t": "x" * 43})
    assert limited.status_code == 429
    assert int(limited.headers["retry-after"]) >= 1
    assert "Too many sign-in attempts" in limited.text
    clock.advance(61)
    assert client.post("/auth", data={"t": "x" * 43}).status_code == 400


def test_F87_rate_limit_is_per_ip(app, clock):
    with TestClient(app, client=("198.51.100.1", 1000)) as a:
        for _ in range(10):
            a.post("/auth", data={"t": "x" * 43})
        assert a.post("/auth", data={"t": "x" * 43}).status_code == 429
    with TestClient(app, client=("198.51.100.2", 1000)) as b:
        assert b.post("/auth", data={"t": "x" * 43}).status_code == 400


def test_post_auth_from_another_site_is_refused(client: TestClient, users_db, config, clock):
    token = issue_token(users_db, config, now=clock())
    response = client.post("/auth", data={"t": token}, headers={"Origin": "https://evil.example"})
    assert response.status_code == 403
    assert SESSION_COOKIE not in client.cookies
    # The token was not touched: a same-origin POST still works.
    ok = client.post(
        "/auth",
        data={"t": token},
        headers={"Origin": "http://testserver"},
        follow_redirects=False,
    )
    assert ok.status_code == 303


def test_post_auth_marked_cross_site_by_fetch_metadata_is_refused(
    client: TestClient, users_db, config, clock
):
    token = issue_token(users_db, config, now=clock())
    response = client.post("/auth", data={"t": token}, headers={"Sec-Fetch-Site": "cross-site"})
    assert response.status_code == 403


def test_F88_login_message_is_deleted_in_the_background(
    client: TestClient, users_db, config, clock, deleted: Deleted
):
    login(client, users_db, config, clock, chat_id=555, message_id=777)
    assert wait_for(lambda: deleted.calls == [(555, 777)])


def test_F88_a_failing_message_deletion_does_not_break_the_sign_in(users_db, data_dir, clock):
    from ynab_helper.web.app import make_app

    def boom(chat_id: int, message_id: int) -> None:
        raise RuntimeError("telegram down")

    config = WebConfig(enabled=True, public_url="http://testserver", secret_key="s" * 40)
    app = make_app(
        config,
        users_db_path=users_db,
        data_dir=data_dir,
        version_line="v",
        cache_bust="c",
        delete_telegram_message=boom,
        clock=clock,
    )
    with TestClient(app) as c:
        login(c, users_db, config, clock, chat_id=1, message_id=2)
        assert c.get("/api/me").status_code == 200


def test_F18_a_user_who_is_not_ready_cannot_sign_in(client: TestClient, users_db, config, clock):
    conn = connect(users_db)
    try:
        UserStore(conn).upsert(OTHER_ID, state="awaiting_token")
    finally:
        conn.close()
    token = issue_token(users_db, config, OTHER_ID, now=clock())
    assert client.post("/auth", data={"t": token}, follow_redirects=False).status_code == 400
    assert _session_rows(users_db) == 0


def test_F18_a_denied_user_loses_the_session_at_once(client: TestClient, users_db, config, clock):
    login(client, users_db, config, clock)
    assert client.get("/api/me").status_code == 200
    conn = connect(users_db)
    try:
        UserStore(conn).set_state(USER_ID, "denied")
    finally:
        conn.close()
    response = client.get("/api/me")
    assert response.status_code == 401
    assert response.json()["error"] == "session_expired"
    assert _session_rows(users_db) == 0  # the session row was deleted


def test_a_new_sign_in_replaces_the_previous_session(client: TestClient, users_db, config, clock):
    login(client, users_db, config, clock)
    login(client, users_db, config, clock)
    assert _session_rows(users_db) == 1


# ── expired sessions (F24) ──────────────────────────────────────────────────


def test_F24_root_with_a_valid_session_goes_to_the_dashboard(
    client: TestClient, users_db, config, clock
):
    login(client, users_db, config, clock)
    response = client.get("/", follow_redirects=False)
    assert (response.status_code, response.headers["location"]) == (303, "/app/dashboard")


def test_F24_root_with_a_stale_cookie_shows_the_login_page_and_clears_it(
    client: TestClient, users_db, config, clock
):
    login(client, users_db, config, clock)
    clock.advance(config.session_ttl + 1)
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 200
    assert "session has expired" in response.text
    assert _set_cookie(response)[SESSION_COOKIE]["max-age"] == "0"


def test_F24_expired_session_redirects_html_pages(client: TestClient, users_db, config, clock):
    login(client, users_db, config, clock)
    clock.advance(config.session_ttl + 1)
    response = client.get("/app/transactions", follow_redirects=False)
    assert (response.status_code, response.headers["location"]) == (303, "/?expired=1")
    assert _set_cookie(response)[SESSION_COOKIE]["max-age"] == "0"
    page = client.get("/?expired=1")
    assert page.status_code == 200 and "session has expired" in page.text


def test_F24_expired_session_on_json_routes_is_a_401(client: TestClient, users_db, config, clock):
    login(client, users_db, config, clock)
    clock.advance(config.session_ttl + 1)
    response = client.get("/api/dashboard")
    assert response.status_code == 401
    assert response.json() == {
        "error": "session_expired",
        "message": response.json()["message"],
    }
    assert "/login" in response.json()["message"]


def test_no_cookie_on_an_html_page_redirects_to_the_plain_login_page(client: TestClient):
    response = client.get("/app/dashboard", follow_redirects=False)
    assert (response.status_code, response.headers["location"]) == (303, "/")


def test_sliding_session_cannot_outlive_the_absolute_ttl(
    client: TestClient, users_db, config, clock
):
    login(client, users_db, config, clock)
    step = config.session_ttl - 60
    elapsed = 0
    while elapsed + step < config.session_absolute_ttl:
        clock.advance(step)
        elapsed += step
        assert client.get("/api/me").status_code == 200
    clock.advance(config.session_absolute_ttl - elapsed + 1)
    assert client.get("/api/me").status_code == 401


# ── logout (F61) ────────────────────────────────────────────────────────────


def test_F61_logout_without_csrf_is_refused(client: TestClient, users_db, config, clock):
    login(client, users_db, config, clock)
    # A form post without the token (a forged cross-site form): nothing happens, the
    # browser lands on a fresh page (never a raw JSON page, F62).
    response = client.post("/logout", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/app/dashboard?notice=logout_stale"
    assert "set-cookie" not in response.headers
    assert client.get("/api/me").status_code == 200  # still signed in
    # A fetch call with a wrong header gets the JSON 403.
    response = client.post("/logout", headers={"X-CSRF-Token": "0" * 32}, follow_redirects=False)
    assert response.status_code == 403
    assert response.json()["error"] == "csrf_failed"
    assert client.get("/api/me").status_code == 200


def test_F62_logout_from_an_outdated_tab_is_not_a_json_dead_end(
    client: TestClient, users_db, config, clock
):
    old_csrf = login(client, users_db, config, clock)  # tab 1 was rendered with this
    new_csrf = login(client, users_db, config, clock)  # signed in again in another tab
    response = client.post("/logout", data={"csrf_token": old_csrf}, follow_redirects=False)
    assert (response.status_code, response.headers["location"]) == (
        303,
        "/app/dashboard?notice=logout_stale",
    )
    page = client.get(response.headers["location"])
    assert page.status_code == 200 and "text/html" in page.headers["content-type"]
    assert "still signed in" in page.text
    assert f'name="csrf_token" value="{new_csrf}"' in page.text  # a working logout form
    assert client.post("/logout", data={"csrf_token": new_csrf}).status_code == 200
    assert _session_rows(users_db) == 0


def test_logout_form_with_an_expired_session_just_clears_the_cookie(
    client: TestClient, users_db, config, clock
):
    login(client, users_db, config, clock)
    clock.advance(config.session_ttl + 1)
    response = client.post("/logout", data={"csrf_token": "0" * 32}, follow_redirects=False)
    assert (response.status_code, response.headers["location"]) == (303, "/")
    assert _set_cookie(response)[SESSION_COOKIE]["max-age"] == "0"


def test_unknown_notice_keys_are_ignored(client: TestClient, users_db, config, clock):
    login(client, users_db, config, clock)
    page = client.get("/app/dashboard", params={"notice": "<b>injected</b>"})
    assert page.status_code == 200
    assert "injected" not in page.text and 'class="notice"' not in page.text


def test_F61_logout_with_the_csrf_header(client: TestClient, users_db, config, clock):
    csrf = login(client, users_db, config, clock)
    response = client.post("/logout", headers={"X-CSRF-Token": csrf}, follow_redirects=False)
    assert (response.status_code, response.headers["location"]) == (303, "/")
    assert _set_cookie(response)[SESSION_COOKIE]["max-age"] == "0"
    assert _session_rows(users_db) == 0


def test_F61_logout_with_the_csrf_form_field(client: TestClient, users_db, config, clock):
    csrf = login(client, users_db, config, clock)
    page = client.get("/app/settings")
    assert f'name="csrf_token" value="{csrf}"' in page.text  # the layout's logout form
    response = client.post("/logout", data={"csrf_token": csrf}, follow_redirects=False)
    assert response.status_code == 303
    assert _session_rows(users_db) == 0


def test_logout_without_a_session_just_returns_to_the_login_page(client: TestClient):
    response = client.post("/logout", follow_redirects=False)
    assert (response.status_code, response.headers["location"]) == (303, "/")


def test_F61_logout_all_revokes_every_session_and_login_link(
    app, client: TestClient, users_db, config, clock
):
    csrf = login(client, users_db, config, clock)
    with TestClient(app) as other_browser:
        login(other_browser, users_db, config, clock)
        pending_link = issue_token(users_db, config, now=clock())
        response = client.post("/api/logout-all", headers={"X-CSRF-Token": csrf})
        assert response.status_code == 200
        assert response.json() == {"ok": True, "revoked": 3}
        assert other_browser.get("/api/me").status_code == 401
    assert client.get("/api/me").status_code == 401
    assert client.post("/auth", data={"t": pending_link}).status_code == 400


def test_sessions_of_other_users_are_untouched_by_logout_all(
    app, client: TestClient, users_db, config, clock
):
    conn = connect(users_db)
    try:
        make_ready(conn, OTHER_ID)
    finally:
        conn.close()
    csrf = login(client, users_db, config, clock)
    other_token = issue_token(users_db, config, OTHER_ID, now=clock())
    with TestClient(app) as other:
        assert other.post("/auth", data={"t": other_token}).status_code == 200
        client.post("/api/logout-all", headers={"X-CSRF-Token": csrf})
        assert other.get("/api/me").json()["telegram_id"] == OTHER_ID


def test_session_purge_removes_expired_rows(app, users_db, config, clock):
    conn = connect(users_db)
    try:
        sessions.issue_login_token(
            conn, config.token_hasher(), USER_ID, ttl_seconds=10, now=clock()
        )
    finally:
        conn.close()
    purger = app.state.web.purger
    assert purger.purge_once() == 0
    clock.advance(11)
    assert purger.purge_once() == 1


def test_post_auth_with_origin_null_needs_same_origin_fetch_metadata(
    client: TestClient, users_db, config, clock
):
    # A page with a no-referrer policy makes the browser send "Origin: null" on its own
    # same-origin form POST; a sandboxed attacker frame sends the same Origin.
    token = issue_token(users_db, config, now=clock())
    attack = client.post("/auth", data={"t": token}, headers={"Origin": "null"})
    assert attack.status_code == 403
    attack = client.post(
        "/auth", data={"t": token}, headers={"Origin": "null", "Sec-Fetch-Site": "cross-site"}
    )
    assert attack.status_code == 403
    ok = client.post(
        "/auth",
        data={"t": token},
        headers={"Origin": "null", "Sec-Fetch-Site": "same-origin"},
        follow_redirects=False,
    )
    assert ok.status_code == 303


def test_post_auth_from_the_real_browser_flow(client: TestClient, users_db, config, clock):
    """What a browser sends for the auto-submitted form of GET /auth."""
    token = issue_token(users_db, config, now=clock())
    page = client.get("/auth", params={"t": token})
    assert 'name="referrer"' not in page.text  # the form POST must keep its Origin
    response = client.post(
        "/auth",
        data={"t": token},
        headers={
            "Origin": "http://testserver",
            "Sec-Fetch-Site": "same-origin",
            "Referer": f"http://testserver/auth?t={token}",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303


def test_F89_rotating_the_secret_key_signs_everyone_out(
    client: TestClient, users_db, data_dir, config, clock
):
    from ynab_helper.web.app import make_app

    login(client, users_db, config, clock)
    cookie = client.cookies.get(SESSION_COOKIE)
    rotated = WebConfig(enabled=True, public_url="http://testserver", secret_key="r" * 40)
    app = make_app(
        rotated,
        users_db_path=users_db,
        data_dir=data_dir,
        version_line="v",
        cache_bust="c",
        clock=clock,
    )
    with TestClient(app, cookies={SESSION_COOKIE: cookie}) as other:
        assert other.get("/api/me").status_code == 401
    conn = connect(users_db)
    try:
        stored = [r[0] for r in conn.execute("SELECT token_hash FROM web_sessions")]
    finally:
        conn.close()
    assert cookie not in stored  # only the keyed hash is stored


# ── review fixes: no global sign-in lockout, switch-account confirmation ────


def test_F87_a_rotating_address_flood_does_not_lock_out_a_legit_user(app, users_db, config, clock):
    # 40 addresses x 10 attempts = 400 attempts in one minute (more than any global cap
    # would allow): the legitimate user's sign-in still works.
    for n in range(40):
        with TestClient(app, client=(f"198.51.100.{n}", 1000)) as flood:
            for _ in range(10):
                assert flood.post("/auth", data={"t": "x" * 43}).status_code == 400
    with TestClient(app, client=("203.0.113.9", 1000)) as legit:
        token = issue_token(users_db, config, now=clock())
        response = legit.post("/auth", data={"t": token}, follow_redirects=False)
        assert response.status_code == 303


def test_F87_ipv6_addresses_of_one_64_share_one_budget(app, clock):
    for n in range(10):
        with TestClient(app, client=(f"2001:db8:1:2::{n:x}", 1000)) as c:
            assert c.post("/auth", data={"t": "x" * 43}).status_code == 400
    with TestClient(app, client=("2001:db8:1:2:ffff::1", 1000)) as c:
        assert c.post("/auth", data={"t": "x" * 43}).status_code == 429
    with TestClient(app, client=("2001:db8:1:3::1", 1000)) as c:  # another /64
        assert c.post("/auth", data={"t": "x" * 43}).status_code == 400


def test_get_auth_while_signed_in_asks_before_switching_accounts(
    client: TestClient, users_db, config, clock
):
    """A link someone forwarded must not silently swap a signed-in user into the
    sender's budget: the interstitial is not auto-submitted then."""
    login(client, users_db, config, clock)
    token = issue_token(users_db, config, OTHER_ID, now=clock())
    page = client.get("/auth", params={"t": token})
    assert page.status_code == 200
    assert "Switch account?" in page.text and "Ann" in page.text
    assert 'data-autosubmit="true"' not in page.text
    assert f'name="t" value="{token}"' in page.text  # pressing Continue still works
    assert 'href="/app/dashboard"' in page.text
    assert _session_rows(users_db, "login") == 1  # nothing consumed


def test_get_auth_without_a_session_is_auto_submitted(client: TestClient, users_db, config, clock):
    token = issue_token(users_db, config, now=clock())
    page = client.get("/auth", params={"t": token})
    assert 'data-autosubmit="true"' in page.text
    assert "Switch account?" not in page.text
    # A stale cookie is not a session either.
    login(client, users_db, config, clock)
    clock.advance(config.session_ttl + 1)
    token = issue_token(users_db, config, now=clock())
    assert 'data-autosubmit="true"' in client.get("/auth", params={"t": token}).text


def test_auth_js_only_submits_forms_marked_for_auto_submit():
    from ynab_helper.web.app import STATIC_DIR

    script = (STATIC_DIR / "js" / "auth.js").read_text(encoding="utf-8")
    assert 'dataset.autosubmit === "true"' in script
