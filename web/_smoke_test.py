"""End-to-end smoke test for the web UI — runs against a temp DB.

Not part of the runtime. Run with:

    python3 -m web._smoke_test

The test exercises the auth flow, every page, every JSON endpoint that
doesn't actually hit YNAB, and the CSRF guard. Endpoints that do hit YNAB
(``/api/dashboard`` balance lookup, ``/api/reconcile``, ``/api/dedupe/scan``,
``/api/upload``'s import step) are stubbed out via monkey-patches.

Failures raise — the script exits non-zero on any assertion error.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

# Make the project root importable when running as `python3 -m web._smoke_test`
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

import logging  # noqa: E402

import revolut_to_ynab as ynab  # noqa: E402
from web import auth as web_auth  # noqa: E402
from web.config import WebConfig  # noqa: E402
from web.server import SESSION_COOKIE, make_app  # noqa: E402

# ── Stub YNAB-touching helpers so we don't need a real token ──────────
ynab.get_ynab_account_balance = lambda *a, **kw: 12345_000  # 12,345.00
ynab.find_orphaned_imports = lambda *a, **kw: {
    "start_date": "2026-04-01",
    "end_date": "2026-04-24",
    "csv_count": 100,
    "ynab_count_in_range": 102,
    "orphans": [
        {"id": "yt-1", "date": "2026-04-15", "amount": -86150,
         "payee_name": "Amazon", "memo": "", "import_id": "YNAB:1",
         "cleared": "cleared"},
    ],
}
ynab.delete_ynab_transaction = lambda *a, **kw: None
ynab.reconcile_from_csv = lambda *a, **kw: None
ynab.parse_revolut_csv = lambda path: [{
    "date": "2026-04-01", "amount": -1000, "payee_name": "Test",
    "memo": "", "cleared": "cleared", "import_id": "YNAB:-1000:2026-04-01:1",
    "_state": "COMPLETED",
}]
ynab.is_revolut_csv = lambda path: True
ynab.import_and_track = lambda *a, **kw: None


def _setup(tmpdir: Path):
    """Build a working bot user DB with one ready user."""
    import revolut_ynab_bot as bot
    db_path = tmpdir / "bot_users.db"
    conn = bot.init_user_db(db_path)
    bot.upsert_user(conn, 12345,
                    chat_id=12345, username="alice", first_name="Alice",
                    ynab_token="fake-token", budget_id="bud-1",
                    budget_name="My Budget", account_id="acc-1",
                    account_name="Revolut", auto_approve=1, state="ready")
    return db_path, conn


def _config(tmpdir):
    return WebConfig(
        enabled=True,
        host="127.0.0.1",
        port=0,  # not used — TestClient
        public_url="http://localhost:8080",
        secret_key="x" * 48,
        login_ttl=300,
        session_ttl=1800,
        session_absolute_ttl=12 * 3600,
        data_dir=tmpdir,
    )


def _csrf(client):
    """Pull the session cookie and derive the CSRF token from it."""
    cookie = client.cookies.get(SESSION_COOKIE)
    assert cookie, "no session cookie set"
    return web_auth.csrf_token_for(cookie)


def main():
    log = logging.getLogger("smoke")
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    with tempfile.TemporaryDirectory() as td:
        tmpdir = Path(td)
        db_path, conn = _setup(tmpdir)
        cfg = _config(tmpdir)

        from starlette.testclient import TestClient
        app = make_app(cfg, db_path, log)
        client = TestClient(app, follow_redirects=False)

        # 1. /health — public.
        r = client.get("/health")
        assert r.status_code == 200, r.text
        assert r.json()["ok"] is True

        # 2. / when unauthenticated → login page.
        r = client.get("/")
        assert r.status_code == 200
        assert "Sign in" in r.text or "/login" in r.text
        log.info("✓ login page rendered")

        # 3. Mint a login token via the auth helper, exchange at /auth.
        token = web_auth.issue_login_token(conn, 12345, ttl_seconds=300)
        r = client.get(f"/auth?t={token}")
        assert r.status_code == 302, r.text
        assert r.headers["location"] == "/app"
        assert SESSION_COOKIE in client.cookies, "no session cookie issued"
        log.info("✓ /auth issued session cookie")

        # 4. The same token cannot be reused.
        r2 = client.get(f"/auth?t={token}")
        assert r2.status_code in (200, 400), r2.status_code
        assert "expired" in r2.text or "/login" in r2.text
        log.info("✓ login token is one-shot")

        # 5. /app/dashboard renders.
        r = client.get("/app/dashboard")
        assert r.status_code == 200, r.text
        assert "Dashboard" in r.text
        log.info("✓ dashboard page renders")

        # 6. /api/dashboard returns expected JSON shape.
        r = client.get("/api/dashboard")
        assert r.status_code == 200, r.text
        d = r.json()
        assert d["balance"] == 12345.0
        assert d["budget_name"] == "My Budget"
        assert d["account_name"] == "Revolut"
        log.info("✓ /api/dashboard ok")

        # 7. /api/transactions with no DB → empty list.
        r = client.get("/api/transactions")
        assert r.status_code == 200, r.text
        assert r.json()["total"] == 0
        log.info("✓ /api/transactions ok (empty)")

        # 8. CSRF: state-changing call WITHOUT header → 403.
        r = client.post("/api/settings", json={"auto_approve": False})
        assert r.status_code == 403, f"expected 403, got {r.status_code}"
        log.info("✓ CSRF guard blocks header-less POST")

        # 9. With CSRF header: settings update succeeds.
        csrf = _csrf(client)
        r = client.post("/api/settings",
                        headers={"X-CSRF-Token": csrf},
                        json={"auto_approve": False})
        assert r.status_code == 200, r.text
        # Verify the row actually changed in the DB.
        row = conn.execute(
            "SELECT auto_approve FROM users WHERE telegram_id = 12345"
        ).fetchone()
        assert row["auto_approve"] == 0
        log.info("✓ /api/settings persists update")

        # 10. /api/dedupe/scan needs a CSV — without one, 400.
        r = client.get("/api/dedupe/scan")
        assert r.status_code == 400
        assert r.json()["error"] == "no_csv"

        # 11. Drop a CSV in csv_cache to satisfy the no_csv check.
        cache = tmpdir / "csv_cache" / "12345"
        cache.mkdir(parents=True)
        (cache / "account-statement_2026-04-01_2026-04-24_en-us_test.csv").write_text(
            "Type,Product,Started Date,Completed Date,Description,Amount,"
            "Fee,Currency,State,Balance\n"
        )
        r = client.get("/api/dedupe/scan")
        assert r.status_code == 200, r.text
        scan = r.json()
        assert scan["start_date"] == "2026-04-01"
        assert len(scan["orphans"]) == 1
        log.info("✓ /api/dedupe/scan ok")

        # 12. Dedupe delete — needs CSRF.
        r = client.post("/api/dedupe/delete",
                        headers={"X-CSRF-Token": _csrf(client)},
                        json={"ids": ["yt-1"]})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["deleted"] == 1
        log.info("✓ /api/dedupe/delete ok")

        # 13. Reconcile.
        r = client.post("/api/reconcile",
                        headers={"X-CSRF-Token": _csrf(client)})
        assert r.status_code == 200, r.text
        log.info("✓ /api/reconcile ok")

        # 14. Upload a CSV.
        from io import BytesIO
        r = client.post(
            "/api/upload",
            headers={"X-CSRF-Token": _csrf(client)},
            files={"file": (
                "account-statement_2026-04-01_2026-04-24_en-us_aaa.csv",
                BytesIO(b"a,b,c\n1,2,3\n"),
                "text/csv",
            )},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["transaction_count"] == 1
        log.info("✓ /api/upload ok")

        # 15. /logout clears the cookie.
        r = client.post("/logout")
        assert r.status_code in (302, 303), r.status_code
        # After logout, /api/me should 401.
        # (TestClient retains cookies across requests by default — but the
        # delete_cookie response headers should clear them.)
        r2 = client.get("/api/me")
        assert r2.status_code == 401
        log.info("✓ logout clears cookie")

        log.info("ALL OK")


if __name__ == "__main__":
    main()
