"""CSRF on every state-changing route, body limits before auth (F21, F22), security headers
(F90) and the error-shape conventions."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from fastapi.routing import iter_route_contexts

from ynab_helper.web import middleware
from ynab_helper.web.deps import api_user_csrf
from ynab_helper.web.middleware import (
    API_BODY_LIMIT,
    CONTENT_SECURITY_POLICY,
    DEFAULT_BODY_LIMIT,
    UPLOAD_BODY_LIMIT,
    BodySizeLimitMiddleware,
    body_limit_for,
)
from ynab_helper.web.routes import (
    CSRF_PROTECTED_ROUTES,
    PUBLIC_POST_ROUTES,
    SIDE_EFFECT_GET_ROUTES,
)

from .conftest import TestClient

JSON_BODIES: dict[str, Any] = {
    "/api/reconcile": {"token": "t"},
    "/api/dedupe/delete": {"scan_id": "s", "ids": ["x"]},
    "/api/settings": {"auto_approve": False},
}


def _request(client: TestClient, path: str, headers: dict[str, str] | None = None):
    headers = dict(headers or {})
    if path == "/api/upload":
        headers["Content-Type"] = "text/csv"
        return client.post(path, content=b"a,b\n", headers=headers, follow_redirects=False)
    if path in JSON_BODIES:
        return client.post(path, json=JSON_BODIES[path], headers=headers, follow_redirects=False)
    return client.post(path, headers=headers, follow_redirects=False)


# ── CSRF ────────────────────────────────────────────────────────────────────


def _all_routes(app) -> set[tuple[str, str]]:
    found = set()
    for route in iter_route_contexts(app.routes):
        for method in route.methods or ():
            found.add((method, route.path))
    return found


def test_every_mutating_route_is_in_the_csrf_table(app):
    mutating = {(m, p) for m, p in _all_routes(app) if m not in ("GET", "HEAD")}
    assert mutating == set(CSRF_PROTECTED_ROUTES) | set(PUBLIC_POST_ROUTES)
    # Sanity: the enumeration sees the read routes as well.
    assert ("GET", "/api/transactions") in _all_routes(app)


def _depends_on(dependant, target) -> bool:
    return any(
        d.call is target or _depends_on(d, target) for d in getattr(dependant, "dependencies", ())
    )


def test_exactly_the_side_effect_gets_require_csrf(app):
    """GETs that sync YNAB and replace a pending token/scan must carry the CSRF header
    (a cross-site navigation with the SameSite=Lax cookie cannot); plain reads do not."""
    guarded = set()
    for route in iter_route_contexts(app.routes):
        dependant = getattr(route, "dependant", None)
        if dependant is None or not _depends_on(dependant, api_user_csrf):
            continue
        guarded |= {(m, route.path) for m in route.methods or () if m in ("GET", "HEAD")}
    assert {(m, p) for m, p in guarded if m == "GET"} == set(SIDE_EFFECT_GET_ROUTES)
    # The POSTs with a session depend on it too (``/logout`` checks the form field itself).
    for _method, path in CSRF_PROTECTED_ROUTES:
        if path.startswith("/api/"):
            route = next(r for r in iter_route_contexts(app.routes) if r.path == path)
            assert _depends_on(route.dependant, api_user_csrf), path


@pytest.mark.parametrize(("method", "path"), SIDE_EFFECT_GET_ROUTES)
def test_side_effect_get_without_csrf_is_refused_without_a_ynab_request(
    client: TestClient, csrf, fake, method, path
):
    fake.reset_log()
    navigation = {"Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "document"}
    for headers in ({}, navigation, {"X-CSRF-Token": "0" * len(csrf)}):
        response = client.get(path, headers=headers)
        assert response.status_code == 403, (headers, response.text)
        assert response.json()["error"] == "csrf_failed"
    assert fake.requests == []


@pytest.mark.parametrize("site", ["cross-site", "same-site"])
@pytest.mark.parametrize(
    "path", ["/api/me", "/api/transactions", *(p for _, p in SIDE_EFFECT_GET_ROUTES)]
)
def test_api_refuses_requests_the_browser_marks_as_cross_site(
    client: TestClient, csrf, fake, site, path
):
    """Defence in depth: a cross-site navigation (or a sibling subdomain) carries the Lax
    cookie, but the page's own fetch calls are always same-origin."""
    fake.reset_log()
    response = client.get(path, headers={"Sec-Fetch-Site": site, "X-CSRF-Token": csrf})
    assert response.status_code == 403
    assert response.json()["error"] == "cross_site"
    assert fake.requests == []


@pytest.mark.parametrize("site", ["same-origin", "none", None])
def test_api_accepts_same_origin_fetch_metadata(client: TestClient, csrf, site):
    headers = {"Sec-Fetch-Site": site} if site else {}
    assert client.get("/api/me", headers=headers).status_code == 200


def test_cross_site_post_is_refused_before_the_csrf_check(client: TestClient, csrf):
    response = client.post(
        "/api/settings",
        json={"auto_approve": False},
        headers={"X-CSRF-Token": csrf, "Sec-Fetch-Site": "cross-site"},
    )
    assert (response.status_code, response.json()["error"]) == (403, "cross_site")


@pytest.mark.parametrize(("method", "path"), CSRF_PROTECTED_ROUTES)
def test_mutating_route_without_csrf_token_is_refused(client: TestClient, csrf, method, path):
    response = _request(client, path)
    if path == "/logout":
        # A plain form post: refused by redirecting to a fresh page (F62), still signed in.
        assert response.status_code == 303
        assert response.headers["location"] == "/app/dashboard?notice=logout_stale"
        assert client.get("/api/me").status_code == 200
        return
    assert response.status_code == 403, response.text
    assert response.json()["error"] == "csrf_failed"


@pytest.mark.parametrize(("method", "path"), CSRF_PROTECTED_ROUTES)
def test_mutating_route_with_a_wrong_csrf_token_is_refused(client: TestClient, csrf, method, path):
    wrong = "0" * len(csrf)
    response = _request(client, path, {"X-CSRF-Token": wrong})
    assert response.status_code == 403
    assert response.json()["error"] == "csrf_failed"


@pytest.mark.parametrize(
    ("method", "path"), [r for r in CSRF_PROTECTED_ROUTES if r[1].startswith("/api/")]
)
def test_mutating_api_route_without_session_is_a_401(client: TestClient, method, path):
    response = _request(client, path, {"X-CSRF-Token": "0" * 32})
    assert response.status_code == 401
    assert response.json()["error"] == "session_expired"


def test_csrf_of_another_session_is_refused(app, client: TestClient, csrf, users_db, config, clock):
    from .conftest import login

    with TestClient(app) as other:
        other_csrf = login(other, users_db, config, clock)
    assert other_csrf != csrf
    response = client.post(
        "/api/settings", json={"auto_approve": False}, headers={"X-CSRF-Token": other_csrf}
    )
    assert response.status_code == 403


# ── body limits (F21) ───────────────────────────────────────────────────────


def test_body_limits_per_path():
    assert body_limit_for("/api/upload") == UPLOAD_BODY_LIMIT == 26 * 1024 * 1024
    assert body_limit_for("/api/settings") == API_BODY_LIMIT == 64 * 1024
    assert body_limit_for("/api/anything/else") == API_BODY_LIMIT
    assert body_limit_for("/auth") == DEFAULT_BODY_LIMIT == 16 * 1024
    assert body_limit_for("/logout") == DEFAULT_BODY_LIMIT
    assert body_limit_for("/apix") == DEFAULT_BODY_LIMIT


ALL_POST_PATHS = [path for _, path in CSRF_PROTECTED_ROUTES + PUBLIC_POST_ROUTES]
ANY_PATHS = [*ALL_POST_PATHS, "/", "/app/dashboard", "/api/me", "/no/such/route", "/static/x"]


@pytest.mark.parametrize("path", ANY_PATHS)
def test_F21_declared_oversized_body_is_refused_before_auth(client: TestClient, path):
    limit = body_limit_for(path)
    # A declared length above the limit is refused without reading a byte, and before the
    # (missing) session is even looked at.
    response = client.post(
        path,
        content=b"x" * 10,
        headers={"Content-Length": str(limit + 1), "Content-Type": "application/json"},
    )
    assert response.status_code == 413, (path, response.status_code)
    assert response.json()["error"] == "too_large"
    assert response.headers["content-security-policy"] == CONTENT_SECURITY_POLICY


def _chunks(total: int, size: int = 4096):
    sent = 0
    while sent < total:
        n = min(size, total - sent)
        sent += n
        yield b"x" * n


@pytest.mark.parametrize("path", [p for p in ANY_PATHS if p != "/api/upload"])
def test_F21_chunked_oversized_body_is_refused_before_auth(client: TestClient, path):
    limit = body_limit_for(path)
    response = client.post(path, content=_chunks(limit + 1))
    assert response.status_code == 413, (path, response.status_code, response.text[:200])


def test_F21_chunked_upload_without_length_is_refused(client: TestClient, csrf):
    response = client.post(
        "/api/upload",
        content=_chunks(1000),
        headers={"Content-Type": "text/csv", "X-CSRF-Token": csrf},
    )
    assert response.status_code == 411
    assert response.json()["error"] == "length_required"


def test_F21_chunked_body_within_the_limit_still_reaches_the_handler(client: TestClient, csrf):
    body = json.dumps({"auto_approve": False}).encode()
    response = client.post(
        "/api/settings",
        content=iter([body[:5], body[5:]]),
        headers={"Content-Type": "application/json", "X-CSRF-Token": csrf},
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"ok": True, "auto_approve": False}


def test_F21_unauthenticated_json_is_never_parsed(client: TestClient, monkeypatch):
    calls = []
    monkeypatch.setattr(json, "loads", lambda *a, **k: calls.append(a) or {})
    response = client.post("/api/settings", json={"auto_approve": True})
    assert response.status_code == 401
    assert calls == []


def test_F21_invalid_content_length_is_rejected(client: TestClient):
    response = client.post("/api/settings", content=b"{}", headers={"Content-Length": "abc"})
    assert response.status_code == 400


class _Inner:
    """A stand-in app that records whether it ran and reads the whole body."""

    def __init__(self) -> None:
        self.called = False
        self.body = b""

    async def __call__(self, scope, receive, send) -> None:
        self.called = True
        while True:
            message = await receive()
            self.body += message.get("body", b"")
            if not message.get("more_body"):
                break
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})


def _run(app, path: str, headers: list[tuple[bytes, bytes]], chunks: list[bytes]):
    sent: list[dict] = []
    queue = [
        {"type": "http.request", "body": c, "more_body": i < len(chunks) - 1}
        for i, c in enumerate(chunks)
    ]

    async def receive():
        return queue.pop(0) if queue else {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "method": "POST", "path": path, "headers": headers}
    asyncio.run(app(scope, receive, send))
    return sent


def test_F21_a_lying_content_length_is_caught_by_the_byte_count():
    inner = _Inner()
    wrapped = BodySizeLimitMiddleware(inner, limit_for=lambda path: 100)
    sent = _run(wrapped, "/api/settings", [(b"content-length", b"50")], [b"x" * 60, b"x" * 60])
    assert sent[0]["status"] == 413
    assert inner.called  # it started reading, but never got more than the limit
    assert len(inner.body) <= 100


def test_F21_oversized_chunked_body_never_reaches_the_app():
    inner = _Inner()
    wrapped = BodySizeLimitMiddleware(inner, limit_for=lambda path: 100)
    sent = _run(wrapped, "/x", [(b"transfer-encoding", b"chunked")], [b"x" * 80, b"x" * 80])
    assert sent[0]["status"] == 413 and not inner.called


def test_F21_declared_oversized_body_never_reaches_the_app():
    inner = _Inner()
    wrapped = BodySizeLimitMiddleware(inner, limit_for=lambda path: 100)
    sent = _run(wrapped, "/x", [(b"content-length", b"101")], [b"x"])
    assert sent[0]["status"] == 413 and not inner.called


def test_F22_multipart_upload_is_not_accepted(client: TestClient, csrf):
    response = client.post(
        "/api/upload",
        files={"file": ("statement.csv", b"Type,Product\n", "text/csv")},
        headers={"X-CSRF-Token": csrf},
    )
    assert response.status_code == 415
    with pytest.raises(ImportError):
        __import__("python_multipart")


# ── security headers (F90) ──────────────────────────────────────────────────

BASE_HEADERS = {
    "content-security-policy": CONTENT_SECURITY_POLICY,
    "x-content-type-options": "nosniff",
    "referrer-policy": "same-origin",
    "x-frame-options": "DENY",
}


def _assert_base_headers(response) -> None:
    for name, value in BASE_HEADERS.items():
        assert response.headers.get(name) == value, (name, response.request.url)


def test_F23_csp_is_the_strict_self_only_policy():
    assert CONTENT_SECURITY_POLICY == (
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
        "connect-src 'self'; font-src 'self'; frame-ancestors 'none'; base-uri 'self'; "
        "form-action 'self'; object-src 'none'"
    )
    assert "unsafe-inline" not in CONTENT_SECURITY_POLICY


@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/health",
        "/auth?t=" + "a" * 43,
        "/app/dashboard",
        "/app/transactions",
        "/api/me",
        "/api/transactions",
        "/no/such/page",
    ],
)
def test_F90_security_headers_and_no_store(client: TestClient, csrf, path):
    response = client.get(path, follow_redirects=False)
    _assert_base_headers(response)
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["pragma"] == "no-cache"


def test_F90_versioned_static_files_are_immutable(client: TestClient):
    response = client.get("/static/2.0.0-test/js/auth.js")
    assert response.status_code == 200
    _assert_base_headers(response)
    assert response.headers["cache-control"] == "public, max-age=31536000, immutable"
    plain = client.get("/static/js/auth.js")
    assert plain.status_code == 200 and plain.headers["cache-control"] == "no-cache"
    missing = client.get("/static/2.0.0-test/js/nope.js")
    assert missing.status_code == 404 and missing.headers["cache-control"] == "no-store"


def test_F90_redirects_and_errors_carry_the_headers(client: TestClient):
    for response in (
        client.get("/app/dashboard", follow_redirects=False),  # 303
        client.get("/api/me"),  # 401
        client.post("/auth", content=b"x", headers={"Content-Length": "999999"}),  # 413
    ):
        _assert_base_headers(response)
        assert response.headers["cache-control"] == "no-store"


def test_unexpected_errors_become_a_500_with_the_error_shape(client: TestClient, csrf, monkeypatch):
    from ynab_helper.web.routes import api

    def boom(*args, **kwargs):
        raise RuntimeError("secret detail")

    monkeypatch.setattr(api.queries, "search_transactions", boom)
    response = client.get("/api/transactions")
    assert response.status_code == 500
    assert response.json()["error"] == "internal"
    assert "secret detail" not in response.text
    _assert_base_headers(response)


def test_docs_and_openapi_are_disabled(client: TestClient):
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(path).status_code == 404


def test_unknown_app_page_is_a_404(client: TestClient, csrf):
    response = client.get("/app/admin")
    assert response.status_code == 404
    assert response.json() == {"error": "not_found", "message": "Not found."}


def test_invalid_query_parameter_is_a_400(client: TestClient, csrf):
    response = client.get("/api/transactions", params={"page": "abc"})
    assert response.status_code == 400
    assert response.json() == {"error": "bad_request", "message": "Invalid value for page."}


def test_middleware_module_exports_plain_error():
    assert callable(middleware.plain_error)


def test_F62_every_error_response_has_error_and_message(client: TestClient, csrf):
    responses = [
        client.get("/api/transactions", params={"page": "x"}),  # 400 validation
        client.post("/api/settings", json={}, headers={"X-CSRF-Token": csrf}),  # 400
        client.post("/api/settings", json={"auto_approve": True}),  # 403 csrf
        client.get("/api/nope"),  # 404
        client.put("/api/settings"),  # 405
        client.get("/api/reconcile/preview", headers={"X-CSRF-Token": csrf}),  # 409 unavailable
        client.post("/api/settings", content=b"x" * 70_000, headers={"X-CSRF-Token": csrf}),
        client.post("/api/upload", content=b"a", headers={"X-CSRF-Token": csrf}),  # 415
    ]
    client.cookies.clear()
    responses.append(client.get("/api/me"))  # 401
    statuses = [r.status_code for r in responses]
    assert statuses == [400, 400, 403, 404, 405, 409, 413, 415, 401]
    for response in responses:
        body = response.json()
        assert isinstance(body["error"], str) and body["error"]
        assert isinstance(body["message"], str) and body["message"]
        assert "detail" not in body
