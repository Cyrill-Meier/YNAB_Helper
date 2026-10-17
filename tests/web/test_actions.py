"""Operations through the service layer (DESIGN §9.2; F04, F13, F20, F41 wiring, Busy)."""

from __future__ import annotations

import threading
from pathlib import Path
from urllib.parse import quote

import pytest

from tests.fakes.fake_ynab import FakeYnab
from ynab_helper import services
from ynab_helper.locks import LOCKS
from ynab_helper.storage import CsvFileStore, UserStore, connect
from ynab_helper.web.deps import api_error_for

from .conftest import ACCT, BUDGET, USER_ID, TestClient, ctx_for, fixture_bytes

PENDING_A = "export_pending_then_completed_a.csv"


def _upload(
    client: TestClient,
    csrf: str,
    content: bytes,
    name: str | None = "statement.csv",
    content_type: str = "text/csv",
):
    headers = {"X-CSRF-Token": csrf, "Content-Type": content_type}
    if name is not None:
        headers["X-Filename"] = quote(name)
    return client.post("/api/upload", content=content, headers=headers)


def _mutations(fake: FakeYnab) -> list[tuple[str, str]]:
    return [(c.method, c.path) for c in fake.calls if c.method != "GET"]


# ── upload ──────────────────────────────────────────────────────────────────


def test_upload_imports_a_raw_csv(client: TestClient, csrf, fake: FakeYnab, data_dir: Path):
    name = "account-statement_2026-10-01_2026-10-12_en-gb_e7a1.csv"
    response = _upload(client, csrf, fixture_bytes(PENDING_A), name)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["ok"] is True and result["dry_run"] is False
    assert result["account_id"] == ACCT and result["currency"] == "CHF"
    assert len(result["created"]) > 0
    assert len(fake.transactions(BUDGET, account_id=ACCT)) == len(result["created"])
    conn = connect(ctx_for(data_dir).db_path)
    try:
        record = CsvFileStore(conn).latest(ACCT)
    finally:
        conn.close()
    assert record is not None
    assert (record.source, record.status, record.original_name) == ("web", "imported", name)
    assert Path(record.stored_path).parent == data_dir / "csv_cache" / str(USER_ID)


def test_upload_respects_auto_approve(client: TestClient, csrf, fake: FakeYnab):
    client.post("/api/settings", json={"auto_approve": False}, headers={"X-CSRF-Token": csrf})
    assert _upload(client, csrf, fixture_bytes(PENDING_A)).json()["ok"]
    assert {tx["approved"] for tx in fake.transactions(BUDGET, account_id=ACCT)} == {False}


def test_upload_rejects_a_file_that_is_not_a_revolut_csv(client: TestClient, csrf, fake):
    response = _upload(client, csrf, b"date,amount\n2026-01-01,5\n")
    assert response.status_code == 400
    assert response.json()["error"] == "not_revolut"
    assert _mutations(fake) == []


def test_upload_rejects_binary_garbage(client: TestClient, csrf):
    response = _upload(client, csrf, b"\x89PNG\r\n\x1a\n" + bytes(range(256)))
    assert response.status_code == 400


def test_upload_wrong_currency_is_refused_before_any_write(client: TestClient, csrf, fake):
    response = _upload(client, csrf, fixture_bytes("export_eur.csv"))
    assert response.status_code == 400
    body = response.json()
    assert body["error"] == "invalid_csv" and "EUR" in body["message"]
    assert _mutations(fake) == []


def test_upload_too_large(client: TestClient, csrf, monkeypatch):
    from ynab_helper.web.routes import actions

    monkeypatch.setattr(actions, "MAX_CSV_BYTES", 1000)
    response = _upload(client, csrf, fixture_bytes(PENDING_A) + b"x" * 1000)
    assert response.status_code == 413
    assert response.json() == {
        "error": "too_large",
        "message": "The statement is larger than 25 MB.",
    }


def test_upload_over_the_request_limit_is_refused_before_auth(client: TestClient):
    response = client.post(
        "/api/upload",
        content=b"x",
        headers={"Content-Type": "text/csv", "Content-Length": str(26 * 1024 * 1024 + 1)},
    )
    assert response.status_code == 413


@pytest.mark.parametrize(
    "content_type", ["application/json", "multipart/form-data; boundary=x", ""]
)
def test_upload_needs_text_csv(client: TestClient, csrf, content_type):
    response = _upload(client, csrf, fixture_bytes(PENDING_A), content_type=content_type)
    assert response.status_code == 415


def test_upload_needs_a_csv_file_name(client: TestClient, csrf):
    response = _upload(client, csrf, fixture_bytes(PENDING_A), "statement.xlsx")
    assert (response.status_code, response.json()["error"]) == (400, "bad_extension")


def test_upload_file_name_is_display_metadata_only(client: TestClient, csrf, data_dir: Path):
    response = _upload(client, csrf, fixture_bytes(PENDING_A), "../../etc/evil‮.csv")
    assert response.status_code == 200
    conn = connect(ctx_for(data_dir).db_path)
    try:
        record = CsvFileStore(conn).latest(ACCT)
    finally:
        conn.close()
    assert record.original_name == "evil.csv"
    assert Path(record.stored_path).parent == data_dir / "csv_cache" / str(USER_ID)


def test_upload_empty_body(client: TestClient, csrf):
    response = _upload(client, csrf, b"")
    assert (response.status_code, response.json()["error"]) == (400, "empty")


def test_F16_upload_while_another_operation_of_the_user_runs_is_busy(
    client: TestClient, csrf, fake
):
    with LOCKS.operation(str(USER_ID), "import"):
        response = _upload(client, csrf, fixture_bytes(PENDING_A))
    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "busy" and body["operation"] == "import"
    assert "still running" in body["message"]
    assert _mutations(fake) == []


def test_upload_waits_for_a_background_sync_instead_of_being_busy(
    client: TestClient, csrf, app, monkeypatch
):
    release = threading.Event()
    real_sync = services.sync

    def slow_sync(ctx, **kwargs):
        release.wait(10)
        return real_sync(ctx, **kwargs)

    monkeypatch.setattr(services, "sync", slow_sync)
    client.get("/api/accounts")  # starts the background sync
    assert app.state.web.syncer.running(str(USER_ID))
    threading.Timer(0.2, release.set).start()
    response = _upload(client, csrf, fixture_bytes(PENDING_A))
    assert response.status_code == 200, response.text


def test_upload_partial_failure_is_reported(client: TestClient, csrf, fake: FakeYnab):
    fake.fail_next("POST", rf"/budgets/{BUDGET}/transactions", 500)
    response = _upload(client, csrf, fixture_bytes(PENDING_A))
    assert response.status_code == 200
    result = response.json()
    assert result["ok"] is False and result["failures"]


# ── settings ────────────────────────────────────────────────────────────────


def test_settings_toggle_auto_approve(client: TestClient, csrf, users_db: Path):
    response = client.post(
        "/api/settings", json={"auto_approve": False}, headers={"X-CSRF-Token": csrf}
    )
    assert response.json() == {"ok": True, "auto_approve": False}
    conn = connect(users_db)
    try:
        assert UserStore(conn).get(USER_ID).auto_approve is False
    finally:
        conn.close()
    assert client.get("/api/me").json()["auto_approve"] is False


@pytest.mark.parametrize(
    "body", [{"auto_approve": "yes"}, {"auto_approve": 1}, {}, {"auto_approve": True, "x": 1}]
)
def test_settings_reject_anything_but_a_bool(client: TestClient, csrf, body):
    response = client.post("/api/settings", json=body, headers={"X-CSRF-Token": csrf})
    assert response.status_code == 400


@pytest.mark.parametrize(
    ("content", "content_type", "status"),
    [
        (b"auto_approve=false", "application/x-www-form-urlencoded", 415),
        (b"[true]", "application/json", 400),
        (b"{not json", "application/json", 400),
        (b'{"auto_approve": NaN}', "application/json", 400),
        (b"", "application/json", 400),
        (b"[" * 5000 + b"]" * 5000, "application/json", 400),
    ],
)
def test_settings_body_validation(client: TestClient, csrf, content, content_type, status):
    response = client.post(
        "/api/settings",
        content=content,
        headers={"Content-Type": content_type, "X-CSRF-Token": csrf},
    )
    assert response.status_code == status
    assert set(response.json()) >= {"error", "message"}


# ── sync ────────────────────────────────────────────────────────────────────


def test_forced_sync_then_throttled(client: TestClient, csrf, fake: FakeYnab, clock):
    fake.add_transaction(BUDGET, ACCT, amount=-1_000, date="2026-10-01")
    response = client.post("/api/sync", headers={"X-CSRF-Token": csrf})
    data = response.json()
    assert data["synced"] is True and data["sync"]["tx_changed"] == 1
    again = client.post("/api/sync", headers={"X-CSRF-Token": csrf}).json()
    assert again["synced"] is False and again["reason"] == "throttled"
    clock.advance(11)
    assert client.post("/api/sync", headers={"X-CSRF-Token": csrf}).json()["synced"] is True


def test_forced_sync_rate_limited_by_ynab(client: TestClient, csrf, fake: FakeYnab):
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 429, retry_after=30)
    response = client.post("/api/sync", headers={"X-CSRF-Token": csrf})
    assert response.status_code == 429
    assert response.json()["error"] == "rate_limited"
    assert response.headers["retry-after"] == "30"


def test_sync_ynab_token_rejected_is_not_a_session_error(client: TestClient, csrf, fake):
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 401)
    response = client.post("/api/sync", headers={"X-CSRF-Token": csrf})
    assert response.status_code == 502  # never 401: that would sign the browser out
    assert response.json()["error"] == "ynab_auth"


# ── reconcile (F04) ─────────────────────────────────────────────────────────


@pytest.fixture
def imported(client: TestClient, csrf, fake: FakeYnab) -> None:
    fake.add_transaction(
        BUDGET,
        ACCT,
        amount=995_000,
        date="2026-09-30",
        payee_name="Opening",
        cleared="reconciled",
    )
    assert _upload(client, csrf, fixture_bytes(PENDING_A)).json()["ok"]


def test_reconcile_preview_then_confirm(client: TestClient, csrf, imported, fake: FakeYnab):
    fake.reset_log()
    preview = client.get("/api/reconcile/preview", headers={"X-CSRF-Token": csrf}).json()
    assert (preview["as_of"], preview["csv_balance"], preview["delta"]) == (
        "2026-10-12",
        1_282_000,
        5_000,
    )
    assert preview["token"] and preview["currency"] == "CHF"
    assert _mutations(fake) == []  # the preview never writes
    result = client.post(
        "/api/reconcile", json={"token": preview["token"]}, headers={"X-CSRF-Token": csrf}
    )
    assert result.status_code == 200, result.text
    assert result.json()["adjustment"]["outcome"] == "created"
    assert [m for m, _ in _mutations(fake)] == ["POST"]


def test_F04_a_used_or_stale_reconcile_token_is_refused(
    client: TestClient, csrf, imported, fake: FakeYnab
):
    preview = client.get("/api/reconcile/preview", headers={"X-CSRF-Token": csrf}).json()
    fake.add_transaction(BUDGET, ACCT, amount=-1_000, date="2026-10-05", cleared="cleared")
    fake.reset_log()
    stale = client.post(
        "/api/reconcile", json={"token": preview["token"]}, headers={"X-CSRF-Token": csrf}
    )
    assert stale.status_code == 409 and stale.json()["error"] == "stale"
    assert _mutations(fake) == []
    unknown = client.post("/api/reconcile", json={"token": "nope"}, headers={"X-CSRF-Token": csrf})
    assert unknown.status_code == 409


def test_reconcile_without_an_imported_statement(client: TestClient, csrf):
    response = client.get("/api/reconcile/preview", headers={"X-CSRF-Token": csrf})
    assert response.status_code == 409
    body = response.json()
    assert (body["error"], body["reason"]) == ("unavailable", "no_statement")


def test_reconcile_requires_a_token(client: TestClient, csrf):
    response = client.post("/api/reconcile", json={}, headers={"X-CSRF-Token": csrf})
    assert response.status_code == 400


# ── dedupe (F13, F20) ───────────────────────────────────────────────────────


def test_F13_dedupe_offers_only_strays_and_deletes_them(client: TestClient, csrf, fake: FakeYnab):
    assert _upload(client, csrf, fixture_bytes(PENDING_A)).json()["ok"]
    stray = fake.add_transaction(
        BUDGET,
        ACCT,
        amount=-11_200,
        date="2026-10-10",
        payee_name="Coop Pronto",
        import_id="YNAB:-11200:2026-10-10:2",
    )
    scan = client.get("/api/dedupe/scan", headers={"X-CSRF-Token": csrf}).json()
    # F13: the statement's own (linked) transactions are never offered.
    assert [c["id"] for c in scan["candidates"]] == [stray]
    assert scan["candidates"][0]["amount"] == -11_200
    response = client.post(
        "/api/dedupe/delete",
        json={"scan_id": scan["scan_id"], "ids": [stray]},
        headers={"X-CSRF-Token": csrf},
    )
    assert response.status_code == 200, response.text
    assert [c["id"] for c in response.json()["deleted"]] == [stray]
    assert fake.tx(stray)["deleted"] is True


def test_F20_delete_with_a_replaced_scan_is_refused(client: TestClient, csrf, fake: FakeYnab):
    assert _upload(client, csrf, fixture_bytes(PENDING_A)).json()["ok"]
    stray = fake.add_transaction(
        BUDGET, ACCT, amount=-11_200, date="2026-10-10", import_id="YNAB:-11200:2026-10-10:2"
    )
    old = client.get("/api/dedupe/scan", headers={"X-CSRF-Token": csrf}).json()
    # A new scan replaces the old one.
    client.get("/api/dedupe/scan", headers={"X-CSRF-Token": csrf})
    fake.reset_log()
    response = client.post(
        "/api/dedupe/delete",
        json={"scan_id": old["scan_id"], "ids": [stray]},
        headers={"X-CSRF-Token": csrf},
    )
    assert response.status_code == 409 and response.json()["error"] == "stale"
    assert _mutations(fake) == []
    assert fake.tx(stray)["deleted"] is False


def test_dedupe_delete_refuses_ids_outside_the_scan(client: TestClient, csrf, fake: FakeYnab):
    assert _upload(client, csrf, fixture_bytes(PENDING_A)).json()["ok"]
    scan = client.get("/api/dedupe/scan", headers={"X-CSRF-Token": csrf}).json()
    linked = fake.transactions(BUDGET, account_id=ACCT)[0]["id"]
    fake.reset_log()
    response = client.post(
        "/api/dedupe/delete",
        json={"scan_id": scan["scan_id"], "ids": [linked]},
        headers={"X-CSRF-Token": csrf},
    )
    assert response.status_code == 409
    assert _mutations(fake) == []


@pytest.mark.parametrize(
    ("body", "error"),
    [
        ({"scan_id": "s", "ids": []}, "bad_selection"),
        ({"scan_id": "s", "ids": "abc"}, "bad_selection"),
        ({"scan_id": "s", "ids": [1]}, "bad_selection"),
        ({"scan_id": "s", "ids": ["x" * 65]}, "bad_selection"),
        ({"scan_id": "s", "ids": [f"id{i}" for i in range(201)]}, "too_many_ids"),
        ({"ids": ["a"]}, "bad_request"),
    ],
)
def test_dedupe_delete_validates_before_any_work(client: TestClient, csrf, fake, body, error):
    fake.reset_log()
    response = client.post("/api/dedupe/delete", json=body, headers={"X-CSRF-Token": csrf})
    assert (response.status_code, response.json()["error"]) == (400, error)
    assert fake.calls == []


def test_dedupe_scan_without_statement(client: TestClient, csrf):
    response = client.get("/api/dedupe/scan", headers={"X-CSRF-Token": csrf})
    assert response.status_code == 409 and response.json()["error"] == "unavailable"


# ── error mapping ───────────────────────────────────────────────────────────


def test_error_mapping_uses_only_user_messages():
    from ynab_helper.errors import Busy, ConfigError, YnabNetworkError, YnabRateLimited

    assert api_error_for(Busy("import")).status == 409
    limited = api_error_for(YnabRateLimited("429 token=abc", retry_after=12.2))
    assert (limited.status, limited.headers["Retry-After"]) == (429, "13")
    network = api_error_for(YnabNetworkError("raw detail", user_message="YNAB is unreachable."))
    assert (network.status, network.message) == (502, "YNAB is unreachable.")
    config = api_error_for(ConfigError("x", user_message="Run /setup."))
    assert (config.status, config.error) == (409, "setup_required")
    from ynab_helper.storage.db import MigrationError

    storage = api_error_for(MigrationError("disk full", user_message="Database upgrade failed."))
    assert (storage.status, storage.error, storage.message) == (
        500,
        "storage",
        "Database upgrade failed.",
    )


def test_settings_write_holds_the_bots_users_lock(users_db, data_dir, config, clock):
    from ynab_helper.web.app import make_app

    from .conftest import login

    class RecordingLock:
        def __init__(self) -> None:
            self._lock = threading.RLock()
            self.entered = 0

        def __enter__(self):
            self._lock.acquire()
            self.entered += 1
            return self

        def __exit__(self, *exc):
            self._lock.release()

    lock = RecordingLock()
    app = make_app(
        config,
        users_db_path=users_db,
        data_dir=data_dir,
        version_line="v",
        cache_bust="c",
        users_lock=lock,  # type: ignore[arg-type]
        clock=clock,
    )
    with TestClient(app) as c:
        csrf = login(c, users_db, config, clock)
        response = c.post(
            "/api/settings", json={"auto_approve": False}, headers={"X-CSRF-Token": csrf}
        )
    assert response.json() == {"ok": True, "auto_approve": False}
    assert lock.entered >= 1


# ── syncing GETs: throttle and YNAB backoff (review findings) ───────────────

SYNCING_GETS = ["/api/reconcile/preview", "/api/dedupe/scan"]


def _ynab_requests(fake: FakeYnab) -> int:
    return len(fake.requests)


@pytest.mark.parametrize("path", SYNCING_GETS)
def test_syncing_gets_are_throttled_per_user(
    client: TestClient, csrf, imported, fake: FakeYnab, clock, path
):
    from ynab_helper.web.routes.actions import FRESH_READ_LIMIT, FRESH_READ_WINDOW

    headers = {"X-CSRF-Token": csrf}
    for _ in range(FRESH_READ_LIMIT):
        assert client.get(path, headers=headers).status_code == 200
    before = _ynab_requests(fake)
    limited = client.get(path, headers=headers)
    assert limited.status_code == 429
    body = limited.json()
    assert body["error"] == "throttled" and body["retry_after"] > 0 and body["message"]
    assert int(limited.headers["retry-after"]) >= 1
    assert _ynab_requests(fake) == before  # no forced sync for the throttled call
    clock.advance(FRESH_READ_WINDOW + 1)
    assert client.get(path, headers=headers).status_code == 200


def test_syncing_gets_are_throttled_separately(client: TestClient, csrf, imported, fake):
    from ynab_helper.web.routes.actions import FRESH_READ_LIMIT

    headers = {"X-CSRF-Token": csrf}
    for _ in range(FRESH_READ_LIMIT):
        client.get("/api/reconcile/preview", headers=headers)
    assert client.get("/api/reconcile/preview", headers=headers).status_code == 429
    assert client.get("/api/dedupe/scan", headers=headers).status_code == 200


@pytest.mark.parametrize("path", SYNCING_GETS)
def test_a_busy_syncing_get_does_not_use_up_the_throttle(client: TestClient, csrf, imported, path):
    from ynab_helper.web.routes.actions import FRESH_READ_LIMIT

    headers = {"X-CSRF-Token": csrf}
    with LOCKS.operation(str(USER_ID), "import"):
        for _ in range(FRESH_READ_LIMIT + 2):
            assert client.get(path, headers=headers).status_code == 409
    assert client.get(path, headers=headers).status_code == 200


@pytest.mark.parametrize("path", SYNCING_GETS)
def test_syncing_gets_honour_the_ynab_backoff(
    client: TestClient, csrf, imported, fake: FakeYnab, path
):
    headers = {"X-CSRF-Token": csrf}
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 429, retry_after=600)
    assert client.post("/api/sync", headers=headers).status_code == 429  # backoff starts
    before = _ynab_requests(fake)
    for _ in range(5):
        response = client.get(path, headers=headers)
        assert response.status_code == 429
        body = response.json()
        assert body["error"] == "rate_limited" and 500 < body["retry_after"] <= 600
        assert 500 < int(response.headers["retry-after"]) <= 600
    assert _ynab_requests(fake) == before  # YNAB said 429: nothing more was sent


def test_forced_sync_honours_the_ynab_backoff(client: TestClient, csrf, fake: FakeYnab):
    headers = {"X-CSRF-Token": csrf}
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 429, retry_after=600)
    assert client.post("/api/sync", headers=headers).status_code == 429
    before = _ynab_requests(fake)
    again = client.post("/api/sync", headers=headers)
    assert again.status_code == 429 and again.json()["error"] == "rate_limited"
    assert _ynab_requests(fake) == before


def test_forced_sync_after_busy_is_not_reported_as_throttled(client: TestClient, csrf, fake):
    headers = {"X-CSRF-Token": csrf}
    with LOCKS.operation(str(USER_ID), "import"):
        first = client.post("/api/sync", headers=headers)
    assert (first.status_code, first.json()["error"]) == (409, "busy")
    retry = client.post("/api/sync", headers=headers).json()
    assert retry["synced"] is True  # not "Synced a moment ago" (nothing had synced)


def test_forced_sync_after_a_ynab_error_is_not_reported_as_throttled(
    client: TestClient, csrf, fake: FakeYnab
):
    headers = {"X-CSRF-Token": csrf}
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 500)
    failed = client.post("/api/sync", headers=headers)
    assert failed.status_code == 502
    retry = client.post("/api/sync", headers=headers).json()
    assert retry["synced"] is True
    # A sync that ran does use the slot.
    assert client.post("/api/sync", headers=headers).json()["reason"] == "throttled"
