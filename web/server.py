"""FastAPI server — all routes, page renders, and JSON endpoints.

Entry points:
* ``make_app(config, bot_db_path, log)`` — pure factory, easy to test
  with ``starlette.testclient.TestClient``.
* ``serve_in_thread(config, bot_db_path, log, on_ready=None)`` — spawns
  uvicorn on a daemon thread and returns it. Used by the bot's
  ``main()`` when ``WEB_UI_ENABLED=1``.
"""

from __future__ import annotations

import io
import json
import logging
import sqlite3
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Optional

from fastapi import (
    Cookie, Depends, FastAPI, File, Form, Header, HTTPException, Request,
    UploadFile, status,
)
from fastapi.responses import (
    HTMLResponse, JSONResponse, RedirectResponse, Response,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# These come from the same package that already has the YNAB CLI logic.
# We import inside functions where needed to avoid a heavy module-level
# import dependency on the bot module.
from . import auth as auth_mod
from .config import WebConfig

# ──────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────

SESSION_COOKIE = "rynab_session"
CSRF_HEADER = "X-CSRF-Token"

_THIS_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = _THIS_DIR / "templates"
STATIC_DIR = _THIS_DIR / "static"


# ──────────────────────────────────────────────────────────────────────
# Lightweight in-process rate limiter — token-bucket per IP
# ──────────────────────────────────────────────────────────────────────

class _RateLimiter:
    """Best-effort fixed-window limiter. Loses state on restart (fine)."""

    def __init__(self, max_per_minute=10):
        self._lock = threading.Lock()
        self._buckets = {}  # ip -> deque[timestamps]
        self.max = int(max_per_minute)

    def allow(self, ip):
        now = time.time()
        with self._lock:
            dq = self._buckets.get(ip)
            if dq is None:
                dq = deque()
                self._buckets[ip] = dq
            # Drop entries older than 60 s
            while dq and dq[0] < now - 60:
                dq.popleft()
            if len(dq) >= self.max:
                return False
            dq.append(now)
            # Bound memory: forget IPs that haven't been seen recently
            if len(self._buckets) > 5000:
                stale = [k for k, v in self._buckets.items() if not v]
                for k in stale:
                    self._buckets.pop(k, None)
            return True


# ──────────────────────────────────────────────────────────────────────
# DB helpers — opened per-request because connections aren't thread-safe
# ──────────────────────────────────────────────────────────────────────

def _open_user_db(path):
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
    except sqlite3.Error:
        pass
    return conn


def _user_tx_db_path(data_dir, telegram_id):
    return Path(data_dir) / f"transactions_{int(telegram_id)}.db"


# ──────────────────────────────────────────────────────────────────────
# App factory
# ──────────────────────────────────────────────────────────────────────

def make_app(config: WebConfig, bot_db_path: Path, log: logging.Logger):
    """Build a FastAPI app bound to the given config + bot user DB."""
    app = FastAPI(
        title="Revolut → YNAB",
        docs_url=None, redoc_url=None, openapi_url=None,  # not a public API
    )
    app.state.config = config
    app.state.bot_db_path = Path(bot_db_path)
    app.state.log = log
    app.state.auth_limiter = _RateLimiter(max_per_minute=10)
    app.state.last_purge = 0.0

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    app.state.templates = templates

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)),
                  name="static")

    # ── Middleware: IP allowlist ─────────────────────────────────────
    @app.middleware("http")
    async def _allowlist_mw(request: Request, call_next):
        cfg: WebConfig = request.app.state.config
        if cfg.allowed_ips:
            ip = request.client.host if request.client else ""
            if not cfg.ip_allowed(ip):
                log.warning("web: blocked request from %s (not in allowlist)", ip)
                return JSONResponse({"error": "forbidden"}, status_code=403)
        return await call_next(request)

    # ── Dependencies ────────────────────────────────────────────────
    def _current_user(
        request: Request,
        rynab_session: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE),
    ):
        """Resolve the cookie to a (telegram_id, user_row) pair or 401.

        Refreshes the session sliding TTL on every successful call.
        """
        cfg: WebConfig = request.app.state.config
        if not rynab_session:
            raise HTTPException(status_code=401, detail="not_authenticated")
        conn = _open_user_db(request.app.state.bot_db_path)
        try:
            res = auth_mod.lookup_session(
                conn, rynab_session,
                sliding_ttl=cfg.session_ttl,
                absolute_ttl=cfg.session_absolute_ttl,
            )
            if res is None:
                raise HTTPException(status_code=401, detail="session_expired")
            telegram_id, expires_at = res
            user = conn.execute(
                "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,),
            ).fetchone()
            if not user:
                raise HTTPException(status_code=401, detail="unknown_user")
            user_dict = dict(user)
            user_dict["_session_expires_at"] = expires_at
            return user_dict
        finally:
            conn.close()

    def _csrf_check(
        request: Request,
        rynab_session: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE),
        x_csrf_token: Optional[str] = Header(default=None, alias=CSRF_HEADER),
    ):
        """Reject state-changing requests without a matching CSRF token."""
        if not rynab_session:
            raise HTTPException(status_code=401, detail="not_authenticated")
        expected = auth_mod.csrf_token_for(rynab_session)
        if not x_csrf_token or x_csrf_token != expected:
            raise HTTPException(status_code=403, detail="csrf_failed")

    # Convenience: a single dependency that does both auth + csrf for
    # POST/PATCH/DELETE.
    def _authed_csrf(
        user=Depends(_current_user),
        _=Depends(_csrf_check),
    ):
        return user

    # ── Periodic cleanup hook ───────────────────────────────────────
    @app.middleware("http")
    async def _periodic_purge(request: Request, call_next):
        now = time.time()
        if now - request.app.state.last_purge > 600:  # every 10 min
            request.app.state.last_purge = now
            try:
                conn = _open_user_db(request.app.state.bot_db_path)
                try:
                    auth_mod.purge_expired(conn)
                finally:
                    conn.close()
            except Exception as e:
                log.warning("web: purge_expired failed: %s", e)
        return await call_next(request)

    # ── Routes ──────────────────────────────────────────────────────

    @app.get("/health")
    def _health():
        return {"ok": True, "ts": time.time()}

    @app.get("/", response_class=HTMLResponse)
    def _root(request: Request,
              rynab_session: Optional[str] = Cookie(default=None,
                                                    alias=SESSION_COOKIE)):
        if rynab_session:
            return RedirectResponse(url="/app", status_code=302)
        return templates.TemplateResponse("login.html", {
            "request": request,
            "message": "Open Telegram and send /login to your bot to get a "
                       "one-time URL.",
        })

    @app.get("/auth")
    def _auth(request: Request, t: str = ""):
        """Exchange a one-shot URL token for a session cookie."""
        cfg: WebConfig = request.app.state.config
        ip = request.client.host if request.client else ""
        ua = request.headers.get("user-agent", "")[:200]

        if not request.app.state.auth_limiter.allow(ip):
            log.warning("web: auth rate-limit hit for %s", ip)
            return templates.TemplateResponse(
                "login.html",
                {"request": request,
                 "message": "Too many attempts. Wait a minute and try again."},
                status_code=429,
            )

        if not t:
            return RedirectResponse(url="/", status_code=302)

        conn = _open_user_db(request.app.state.bot_db_path)
        try:
            res = auth_mod.consume_login_token(
                conn, t, ip=ip, user_agent=ua,
                session_ttl=cfg.session_ttl,
                absolute_ttl=cfg.session_absolute_ttl,
            )
        finally:
            conn.close()

        if res is None:
            log.info("web: bad/expired login token from %s", ip)
            return templates.TemplateResponse(
                "login.html",
                {"request": request,
                 "message": "That login URL has expired or already been "
                            "used. Open Telegram and run /login again."},
                status_code=400,
            )

        telegram_id, session_token, expires_at = res
        log.info("web: session issued user=%s ip=%s", telegram_id, ip)

        resp = RedirectResponse(url="/app", status_code=302)
        # HttpOnly so JS can't read it; SameSite=Lax so the cross-site
        # redirect from Telegram still keeps the cookie. We still rely
        # on CSRF tokens for state-changing endpoints.
        resp.set_cookie(
            SESSION_COOKIE, session_token,
            max_age=cfg.session_absolute_ttl,
            httponly=True,
            samesite="lax",
            secure=cfg.public_url.startswith("https://"),
            path="/",
        )
        return resp

    @app.post("/logout")
    def _logout(request: Request,
                rynab_session: Optional[str] = Cookie(default=None,
                                                      alias=SESSION_COOKIE)):
        if rynab_session:
            conn = _open_user_db(request.app.state.bot_db_path)
            try:
                auth_mod.delete_session(conn, rynab_session)
            finally:
                conn.close()
        resp = RedirectResponse(url="/", status_code=303)
        resp.delete_cookie(SESSION_COOKIE, path="/")
        return resp

    # ── Pages (HTML) ────────────────────────────────────────────────

    def _page(request: Request, name: str, ctx: dict, user: dict):
        cfg: WebConfig = request.app.state.config
        # CSRF: derived from the session cookie, exposed in template so
        # client JS can echo it in headers.
        cookie = request.cookies.get(SESSION_COOKIE, "")
        csrf = auth_mod.csrf_token_for(cookie) if cookie else ""
        full = {
            "request": request,
            "user": user,
            "csrf_token": csrf,
            "version": _bot_version_line(),
            "now_ts": int(time.time()),
            **ctx,
        }
        return templates.TemplateResponse(name, full)

    @app.get("/app", response_class=HTMLResponse)
    def _app_root(request: Request, user=Depends(_current_user)):
        return RedirectResponse(url="/app/dashboard", status_code=302)

    @app.get("/app/dashboard", response_class=HTMLResponse)
    def _dashboard(request: Request, user=Depends(_current_user)):
        return _page(request, "dashboard.html", {}, user)

    @app.get("/app/transactions", response_class=HTMLResponse)
    def _transactions(request: Request, user=Depends(_current_user)):
        return _page(request, "transactions.html", {}, user)

    @app.get("/app/reconcile", response_class=HTMLResponse)
    def _reconcile_page(request: Request, user=Depends(_current_user)):
        return _page(request, "reconcile.html", {}, user)

    @app.get("/app/dedupe", response_class=HTMLResponse)
    def _dedupe_page(request: Request, user=Depends(_current_user)):
        return _page(request, "dedupe.html", {}, user)

    @app.get("/app/settings", response_class=HTMLResponse)
    def _settings_page(request: Request, user=Depends(_current_user)):
        return _page(request, "settings.html", {}, user)

    @app.get("/app/upload", response_class=HTMLResponse)
    def _upload_page(request: Request, user=Depends(_current_user)):
        return _page(request, "upload.html", {}, user)

    # ── JSON API (auth required for everything below) ───────────────

    @app.get("/api/me")
    def _api_me(user=Depends(_current_user)):
        return _user_summary(user)

    @app.get("/api/dashboard")
    def _api_dashboard(request: Request, user=Depends(_current_user)):
        cfg: WebConfig = request.app.state.config
        tg_id = user["telegram_id"]
        # Pull a balance from YNAB (cheap, single GET) — degrade gracefully.
        ynab_balance = None
        currency = "?"
        try:
            import revolut_to_ynab as ynab
            balance_milli = ynab.get_ynab_account_balance(
                user["ynab_token"], user["budget_id"], user["account_id"],
            )
            ynab_balance = balance_milli / 1000
        except Exception as e:
            log.warning("web: balance fetch failed for %s: %s", tg_id, e)
        # Local DB stats
        path = _user_tx_db_path(cfg.data_dir, tg_id)
        stats = {"total": 0, "cleared": 0, "uncleared": 0,
                 "last_import": None, "first_date": None, "last_date": None}
        if path.exists():
            conn = _open_user_db(path)
            try:
                row = conn.execute(
                    "SELECT count(*) c, "
                    " sum(case when cleared='cleared' then 1 else 0 end) cc, "
                    " sum(case when cleared!='cleared' then 1 else 0 end) cu, "
                    " min(date) mi, max(date) ma, "
                    " max(imported_at) li FROM transactions"
                ).fetchone()
                if row:
                    stats["total"] = row["c"] or 0
                    stats["cleared"] = row["cc"] or 0
                    stats["uncleared"] = row["cu"] or 0
                    stats["last_import"] = row["li"]
                    stats["first_date"] = row["mi"]
                    stats["last_date"] = row["ma"]
            finally:
                conn.close()
        # Latest cached CSV (if any)
        csv_dir = cfg.data_dir / "csv_cache" / str(tg_id)
        last_csv = None
        if csv_dir.exists():
            csvs = sorted(
                (p for p in csv_dir.iterdir()
                 if p.is_file() and p.suffix.lower() == ".csv"),
                key=lambda p: p.stat().st_mtime, reverse=True,
            )
            if csvs:
                top = csvs[0]
                last_csv = {
                    "name": top.name,
                    "size": top.stat().st_size,
                    "mtime": top.stat().st_mtime,
                }
        return {
            "balance": ynab_balance,
            "currency": currency,
            "stats": stats,
            "last_csv": last_csv,
            "auto_approve": bool(user.get("auto_approve", 1)),
            "budget_name": user.get("budget_name"),
            "account_name": user.get("account_name"),
            "version": _bot_version_line(),
        }

    @app.get("/api/transactions")
    def _api_transactions(
        request: Request,
        q: str = "", state: str = "all",
        page: int = 1, page_size: int = 50,
        sort: str = "-date",
        user=Depends(_current_user),
    ):
        """Paginated, searchable transaction list — reads the user's DB."""
        cfg: WebConfig = request.app.state.config
        page = max(1, int(page))
        page_size = min(200, max(1, int(page_size)))
        sort_field = sort.lstrip("-")
        sort_dir = "DESC" if sort.startswith("-") else "ASC"
        if sort_field not in ("date", "amount", "payee_name", "imported_at"):
            sort_field = "date"
            sort_dir = "DESC"

        path = _user_tx_db_path(cfg.data_dir, user["telegram_id"])
        if not path.exists():
            return {"items": [], "total": 0, "page": page,
                    "page_size": page_size}

        where = ["deleted = 0 OR deleted IS NULL"]
        params = []
        if q:
            like = f"%{q}%"
            where.append("(payee_name LIKE ? OR memo LIKE ?)")
            params.extend([like, like])
        if state == "cleared":
            where.append("cleared = 'cleared'")
        elif state == "uncleared":
            where.append("(cleared != 'cleared' OR cleared IS NULL)")
        where_sql = " AND ".join(where)

        conn = _open_user_db(path)
        try:
            total = conn.execute(
                f"SELECT count(*) FROM transactions WHERE {where_sql}",
                params,
            ).fetchone()[0]
            offset = (page - 1) * page_size
            rows = conn.execute(
                f"SELECT date, amount, payee_name, memo, cleared, "
                f"       ynab_tx_id, imported_at "
                f"FROM transactions WHERE {where_sql} "
                f"ORDER BY {sort_field} {sort_dir}, imported_at DESC "
                f"LIMIT ? OFFSET ?",
                [*params, page_size, offset],
            ).fetchall()
        finally:
            conn.close()
        return {
            "items": [{
                "date": r["date"],
                "amount": r["amount"],
                "amount_display": (r["amount"] or 0) / 1000,
                "payee_name": r["payee_name"] or "",
                "memo": r["memo"] or "",
                "cleared": r["cleared"] or "",
                "ynab_tx_id": r["ynab_tx_id"],
                "imported_at": r["imported_at"],
            } for r in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    @app.post("/api/reconcile")
    def _api_reconcile(request: Request, user=Depends(_authed_csrf)):
        """Run reconcile against the user's most recent cached CSV."""
        cfg: WebConfig = request.app.state.config
        tg_id = user["telegram_id"]
        csv_path = _latest_csv(cfg.data_dir, tg_id)
        if not csv_path:
            return JSONResponse(
                {"error": "no_csv",
                 "message": "Upload a CSV first (Telegram or the /app/upload "
                            "page)."},
                status_code=400,
            )
        import revolut_to_ynab as ynab
        # Capture stdout from the existing helper so we can show it.
        buf = io.StringIO()
        old = sys.stdout
        sys.stdout = buf
        try:
            try:
                ynab.reconcile_from_csv(
                    user["ynab_token"], user["budget_id"],
                    user["account_id"], str(csv_path),
                )
            except Exception as e:
                log.error("web: reconcile failed user=%s: %s", tg_id, e)
                return JSONResponse(
                    {"error": "reconcile_failed", "message": str(e),
                     "stdout": buf.getvalue()},
                    status_code=500,
                )
        finally:
            sys.stdout = old
        return {"ok": True, "stdout": buf.getvalue(),
                "csv_name": csv_path.name}

    @app.get("/api/dedupe/scan")
    def _api_dedupe_scan(user=Depends(_current_user), request: Request = None):
        cfg: WebConfig = request.app.state.config
        tg_id = user["telegram_id"]
        csv_path = _latest_csv(cfg.data_dir, tg_id)
        if not csv_path:
            return JSONResponse(
                {"error": "no_csv",
                 "message": "Upload a CSV first."},
                status_code=400,
            )
        import revolut_to_ynab as ynab
        try:
            report = ynab.find_orphaned_imports(
                user["ynab_token"], user["budget_id"],
                user["account_id"], str(csv_path),
            )
        except Exception as e:
            log.error("web: dedupe scan failed user=%s: %s", tg_id, e)
            return JSONResponse(
                {"error": "scan_failed", "message": str(e)},
                status_code=500,
            )
        # Strip ints to floats for display + a stable id for selection.
        return {
            "csv_name": csv_path.name,
            "start_date": report["start_date"],
            "end_date": report["end_date"],
            "csv_count": report["csv_count"],
            "ynab_count_in_range": report["ynab_count_in_range"],
            "orphans": [{
                "id": o["id"],
                "date": o["date"],
                "amount": (o["amount"] or 0) / 1000,
                "payee_name": o["payee_name"],
                "memo": o["memo"],
                "import_id": o["import_id"],
                "cleared": o["cleared"],
            } for o in report["orphans"]],
        }

    @app.post("/api/dedupe/delete")
    def _api_dedupe_delete(payload: dict, request: Request,
                           user=Depends(_authed_csrf)):
        ids = payload.get("ids") or []
        if not isinstance(ids, list) or not ids:
            return JSONResponse(
                {"error": "no_ids", "message": "No transaction IDs given."},
                status_code=400,
            )
        cfg: WebConfig = request.app.state.config
        tg_id = user["telegram_id"]
        import revolut_to_ynab as ynab
        local_db_path = _user_tx_db_path(cfg.data_dir, tg_id)
        local_conn = _open_user_db(local_db_path) if local_db_path.exists() else None
        deleted = 0
        failures = []
        try:
            for tx_id in ids:
                try:
                    ynab.delete_ynab_transaction(
                        local_conn, user["ynab_token"], user["budget_id"], tx_id,
                    )
                    deleted += 1
                    log.info(
                        "web: dedupe deleted ynab_id=%s user=%s", tx_id, tg_id,
                    )
                except Exception as e:
                    log.error("web: dedupe delete failed id=%s: %s", tx_id, e)
                    failures.append({"id": tx_id, "error": str(e)})
        finally:
            if local_conn is not None:
                local_conn.close()
        return {"deleted": deleted, "failures": failures,
                "requested": len(ids)}

    @app.post("/api/settings")
    def _api_set_settings(payload: dict, request: Request,
                          user=Depends(_authed_csrf)):
        """Patch user settings (currently just auto_approve)."""
        tg_id = user["telegram_id"]
        updates = {}
        if "auto_approve" in payload:
            updates["auto_approve"] = 1 if payload["auto_approve"] else 0
        if not updates:
            return JSONResponse(
                {"error": "nothing_to_update"}, status_code=400,
            )
        conn = _open_user_db(request.app.state.bot_db_path)
        try:
            sets = ", ".join(f"{k} = ?" for k in updates)
            params = list(updates.values()) + [tg_id]
            conn.execute(
                f"UPDATE users SET {sets}, updated_at = ? WHERE telegram_id = ?",
                params[:-1] + [_iso_now(), tg_id],
            )
            conn.commit()
        finally:
            conn.close()
        log.info("web: settings updated user=%s %s", tg_id, list(updates))
        return {"ok": True, "updated": updates}

    @app.post("/api/upload")
    async def _api_upload(request: Request,
                          file: UploadFile = File(...),
                          user=Depends(_authed_csrf)):
        """Upload a Revolut CSV via the web (mirrors Telegram's behavior)."""
        cfg: WebConfig = request.app.state.config
        tg_id = user["telegram_id"]
        if not (file.filename and file.filename.lower().endswith(".csv")):
            return JSONResponse(
                {"error": "bad_extension",
                 "message": "File must end in .csv"},
                status_code=400,
            )
        # Cap upload to 25 MB — defensive.
        body = await file.read()
        if len(body) > 25 * 1024 * 1024:
            return JSONResponse(
                {"error": "too_large", "message": "Max 25 MB."},
                status_code=413,
            )
        # Persist to the same csv_cache dir the bot uses.
        target_dir = cfg.data_dir / "csv_cache" / str(tg_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / Path(file.filename).name
        target.write_bytes(body)

        # Validate
        import revolut_to_ynab as ynab
        if not ynab.is_revolut_csv(str(target)):
            target.unlink(missing_ok=True)
            return JSONResponse(
                {"error": "not_revolut",
                 "message": "Doesn't look like a Revolut account-statement CSV."},
                status_code=400,
            )

        # Drop older files so the cache stays at one CSV per user.
        for p in target_dir.iterdir():
            try:
                if p.is_file() and p.resolve() != target.resolve():
                    p.unlink()
            except OSError:
                pass

        # Run the same import pipeline.
        try:
            transactions = ynab.parse_revolut_csv(str(target))
            for tx in transactions:
                tx["approved"] = bool(user.get("auto_approve", 1))
            local_db = _user_tx_db_path(cfg.data_dir, tg_id)
            conn = ynab.init_db(str(local_db))
            buf = io.StringIO()
            old = sys.stdout
            sys.stdout = buf
            try:
                ynab.import_and_track(
                    conn, user["ynab_token"], user["budget_id"],
                    user["account_id"], transactions,
                )
            finally:
                sys.stdout = old
                conn.close()
            log.info(
                "web: upload+import user=%s file=%s txns=%d",
                tg_id, target.name, len(transactions),
            )
            return {"ok": True, "filename": target.name,
                    "transaction_count": len(transactions),
                    "stdout": buf.getvalue()}
        except Exception as e:
            log.error("web: upload import failed user=%s: %s", tg_id, e)
            return JSONResponse(
                {"error": "import_failed", "message": str(e)},
                status_code=500,
            )

    return app


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def _iso_now():
    from datetime import datetime
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _user_summary(user):
    """Trim a user row to fields safe for the browser."""
    return {
        "telegram_id": user["telegram_id"],
        "first_name": user.get("first_name"),
        "username": user.get("username"),
        "budget_name": user.get("budget_name"),
        "account_name": user.get("account_name"),
        "auto_approve": bool(user.get("auto_approve", 1)),
        "session_expires_at": user.get("_session_expires_at"),
    }


def _bot_version_line():
    try:
        import revolut_ynab_bot as bot
        return bot.format_version_line()
    except Exception:
        return "v? (?, ?)"


def _latest_csv(data_dir, telegram_id):
    d = Path(data_dir) / "csv_cache" / str(int(telegram_id))
    if not d.exists():
        return None
    csvs = [p for p in d.iterdir()
            if p.is_file() and p.suffix.lower() == ".csv"]
    if not csvs:
        return None
    return max(csvs, key=lambda p: p.stat().st_mtime)


# ──────────────────────────────────────────────────────────────────────
# Threaded server
# ──────────────────────────────────────────────────────────────────────

def serve_in_thread(config: WebConfig, bot_db_path, log: logging.Logger,
                    on_ready=None):
    """Spawn uvicorn on a daemon thread; return the thread.

    The thread blocks inside ``uvicorn.Server.run``; we don't expose a
    graceful shutdown path because the parent process exit kills the
    daemon thread anyway. ``on_ready`` (if supplied) is invoked once the
    server reports that it's serving.
    """
    import uvicorn
    app = make_app(config, bot_db_path, log)
    cfg = uvicorn.Config(
        app=app,
        host=config.host,
        port=config.port,
        log_level="warning",
        access_log=False,
        # Caddy (and any future reverse proxy) sits in front of us and
        # adds X-Forwarded-For / X-Forwarded-Proto. Tell uvicorn to honor
        # those headers regardless of the immediate peer IP — without
        # this, request.client.host returns Caddy's docker IP, breaking
        # both audit logs and the per-IP rate limiter (everyone shares
        # one bucket).
        proxy_headers=True,
        forwarded_allow_ips="*",
        # Keep the loop simple — no reload.
    )
    server = uvicorn.Server(cfg)

    def _runner():
        try:
            server.run()
        except Exception as e:
            log.error("web: uvicorn crashed: %s", e)

    t = threading.Thread(target=_runner, name="web-ui", daemon=True)
    t.start()

    if on_ready is not None:
        # Fire-and-forget readiness probe — uvicorn flips `started` ~instantly.
        def _wait():
            for _ in range(50):  # ~5 s
                if getattr(server, "started", False):
                    on_ready()
                    return
                time.sleep(0.1)
        threading.Thread(target=_wait, daemon=True).start()

    return t
