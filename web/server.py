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
from datetime import datetime, timedelta
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


# Tracks the last YNAB→local sync per (telegram_id) inside this process.
# It's belt-and-suspenders alongside server_knowledge in the DB; the
# DB-side mechanism handles correctness across restarts, this just
# stops us hammering YNAB when the user clicks around fast.
_SYNC_THROTTLE_SECONDS = 30
_last_sync_at = {}
_sync_locks = {}
_sync_locks_lock = threading.Lock()


def _maybe_sync_categories(cfg, user, log, force=False):
    """Pull recent YNAB data (accounts + budget-wide transactions).

    Despite the historical name, this now syncs:
      1. Accounts — via sync_accounts_from_ynab (cheap, ~1 GET)
      2. Transactions across every account — via sync_budget_transactions
         (single GET, delta-paginated through server_knowledge)

    YNAB's delta protocol means subsequent calls only return what
    changed, so this is cheap to call on most page loads. We additionally
    throttle in-process to ~once per 30 s per user so a flurry of clicks
    doesn't fan out into N requests.

    Errors are logged and swallowed; the caller still gets to render a
    degraded view rather than a 500.
    """
    tg_id = user["telegram_id"]
    now = time.time()
    if not force and (now - _last_sync_at.get(tg_id, 0)) < _SYNC_THROTTLE_SECONDS:
        return False
    # Single in-flight sync per user — avoid two concurrent calls
    # racing on the same DB / server_knowledge row.
    with _sync_locks_lock:
        lock = _sync_locks.setdefault(tg_id, threading.Lock())
    if not lock.acquire(blocking=False):
        return False
    try:
        if not force and (now - _last_sync_at.get(tg_id, 0)) < _SYNC_THROTTLE_SECONDS:
            return False  # someone else just did it
        db_path = _user_tx_db_path(cfg.data_dir, tg_id)
        if not db_path.exists():
            return False
        import revolut_to_ynab as ynab
        conn = ynab.init_db(str(db_path))
        try:
            buf = io.StringIO()
            old = sys.stdout
            sys.stdout = buf
            try:
                # 1. Accounts — discovers new ones, marks deletions/closures
                try:
                    ynab.sync_accounts_from_ynab(
                        conn, user["ynab_token"], user["budget_id"],
                    )
                except Exception as e:
                    log.warning("web: sync_accounts_from_ynab failed user=%s: %s",
                                tg_id, e)
                # 2. Budget-wide transactions (all accounts, one call)
                try:
                    ynab.sync_budget_transactions(
                        conn, user["ynab_token"], user["budget_id"],
                        primary_account_id=user.get("account_id") or "",
                    )
                except Exception as e:
                    log.warning("web: sync_budget_transactions failed user=%s: %s",
                                tg_id, e)
                # 3. Legacy single-account sync — still useful for the
                #    primary account because it shares a server_knowledge
                #    slot with the dedupe path.
                try:
                    ynab.sync_from_ynab(
                        conn, user["ynab_token"], user["budget_id"],
                        user["account_id"],
                    )
                except Exception as e:
                    log.warning("web: sync_from_ynab (primary) failed user=%s: %s",
                                tg_id, e)
            finally:
                sys.stdout = old
        finally:
            conn.close()
        _last_sync_at[tg_id] = time.time()
        return True
    finally:
        lock.release()


def _parse_account_filter(raw):
    """Parse a comma-separated ``accounts=`` query param.

    Returns a list of account_id strings (deduped, stripped of empty
    items) or ``[]`` for the default "all accounts" behaviour.
    """
    if not raw:
        return []
    seen = []
    for piece in str(raw).split(","):
        p = piece.strip()
        if p and p not in seen:
            seen.append(p)
    return seen


def _account_where_clause(account_filter):
    """Render an SQL fragment + params that filter rows to the given accounts.

    Returns ``("", [])`` for the default-all case so callers can tack
    it onto an existing WHERE. Otherwise an ``account_id IN (?, ?, …)``
    clause with the right number of placeholders.
    """
    if not account_filter:
        return "", []
    placeholders = ",".join("?" for _ in account_filter)
    return f"account_id IN ({placeholders})", list(account_filter)


# Spending charts ignore amounts close to zero (e.g. transfers, the
# "Closing transaction" rows after the Product=Current filter still
# allows mirror-leg pocket transfers to remain). We also exclude
# positive amounts from spending breakdowns — those are deposits /
# refunds and would skew per-category totals.
_SPENDING_MIN_ABS_MILLI = 1   # any non-zero spend


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
            "cache_bust": _cache_bust_token(),
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
                {"request": request, "cache_bust": _cache_bust_token(),
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
                {"request": request, "cache_bust": _cache_bust_token(),
                 "message": "That login URL has expired or already been "
                            "used. Open Telegram and run /login again."},
                status_code=400,
            )

        telegram_id, session_token, expires_at, tg_chat_id, tg_msg_id = res
        log.info("web: session issued user=%s ip=%s", telegram_id, ip)

        # Fire-and-forget: ask Telegram to delete the original /login DM
        # so the URL doesn't sit forever in the user's chat history.
        # Runs on a daemon thread so the redirect to /app isn't blocked
        # by Telegram's response time.
        if cfg.bot_token and tg_chat_id and tg_msg_id:
            threading.Thread(
                target=_delete_login_message,
                args=(cfg.bot_token, tg_chat_id, tg_msg_id, log),
                daemon=True,
            ).start()

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
            # Used as ?v=… on /static/* URLs to bust browser caches on
            # every deploy. Combines version + commit SHA so even a
            # rebuild without a version bump invalidates clients.
            "cache_bust": _cache_bust_token(),
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

    @app.get("/app/accounts", response_class=HTMLResponse)
    def _accounts_page(request: Request, user=Depends(_current_user)):
        return _page(request, "accounts.html", {}, user)

    @app.get("/app/spending", response_class=HTMLResponse)
    def _spending_page(request: Request, user=Depends(_current_user)):
        return _page(request, "spending.html", {}, user)

    @app.get("/app/subscriptions", response_class=HTMLResponse)
    def _subscriptions_page(request: Request, user=Depends(_current_user)):
        return _page(request, "subscriptions.html", {}, user)

    # ── JSON API (auth required for everything below) ───────────────

    @app.get("/api/me")
    def _api_me(user=Depends(_current_user)):
        return _user_summary(user)

    @app.get("/api/dashboard")
    def _api_dashboard(request: Request, accounts: str = "",
                       user=Depends(_current_user)):
        cfg: WebConfig = request.app.state.config
        _maybe_sync_categories(cfg, user, log)
        tg_id = user["telegram_id"]
        account_filter = _parse_account_filter(accounts)
        # Pull a balance from YNAB (cheap, single GET) — degrade gracefully.
        # Note: this is the user's *primary* account balance. The account-
        # filter applies to local-DB stats; the headline balance card on
        # the dashboard always shows the primary, since that's the one
        # the bot writes to via CSV import.
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
        # Local DB stats. Reconciled rows count as cleared — see the
        # similar comment in /api/transactions about YNAB's three-value
        # `cleared` enum (cleared / reconciled / uncleared).
        path = _user_tx_db_path(cfg.data_dir, tg_id)
        stats = {"total": 0, "cleared": 0, "reconciled": 0, "uncleared": 0,
                 "last_import": None, "first_date": None, "last_date": None}
        if path.exists():
            conn = _open_user_db(path)
            try:
                acct_clause, acct_params = _account_where_clause(account_filter)
                where_extra = f" AND {acct_clause}" if acct_clause else ""
                row = conn.execute(
                    "SELECT count(*) c, "
                    " sum(case when cleared='cleared' then 1 else 0 end) cc, "
                    " sum(case when cleared='reconciled' then 1 else 0 end) cr, "
                    " sum(case when cleared='uncleared' OR cleared IS NULL "
                    "          then 1 else 0 end) cu, "
                    " min(date) mi, max(date) ma, "
                    " max(imported_at) li FROM transactions "
                    "WHERE (deleted = 0 OR deleted IS NULL)" + where_extra,
                    acct_params,
                ).fetchone()
                if row:
                    stats["total"] = row["c"] or 0
                    stats["cleared"] = row["cc"] or 0
                    stats["reconciled"] = row["cr"] or 0
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
        q: str = "", state: str = "all", category: str = "",
        accounts: str = "",
        page: int = 1, page_size: int = 50,
        sort: str = "-date",
        user=Depends(_current_user),
    ):
        """Paginated, searchable transaction list — reads the user's DB.

        Filters: ``q`` (payee/memo substring), ``state`` (all/cleared/
        uncleared), ``category`` (exact match — pass the empty sentinel
        ``"__none__"`` to filter for uncategorized rows), ``accounts``
        (comma-separated YNAB account ids; default = all).
        """
        cfg: WebConfig = request.app.state.config
        # Pull fresh categories from YNAB (delta-synced, throttled).
        _maybe_sync_categories(cfg, user, log)
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

        # NOTE: parens are load-bearing — without them, joining this
        # clause with " AND " plus a second OR clause hits SQL operator
        # precedence (AND > OR) and the second filter becomes effectively
        # ignored. See PENTEST_REPORT.md M-1.
        #
        # All column refs are prefixed `t.` because the SELECT below
        # joins `transactions t` with `accounts a` and several columns
        # (deleted, account_id) exist on both tables.
        where = ["(t.deleted = 0 OR t.deleted IS NULL)"]
        params = []
        if q:
            like = f"%{q}%"
            where.append("(t.payee_name LIKE ? OR t.memo LIKE ?)")
            params.extend([like, like])
        # YNAB's `cleared` column is a three-value enum:
        # cleared, reconciled, uncleared. The UI exposes all three as
        # distinct filter options; each one matches exactly its bucket.
        # Reconciled means "cleared and locked by a reconcile pass" —
        # it's MORE cleared than just cleared, but users still want to
        # be able to slice it on its own.
        #   state=cleared    → only cleared (NOT reconciled)
        #   state=reconciled → only reconciled
        #   state=uncleared  → uncleared OR NULL
        if state == "cleared":
            where.append("t.cleared = 'cleared'")
        elif state == "reconciled":
            where.append("t.cleared = 'reconciled'")
        elif state == "uncleared":
            where.append("(t.cleared = 'uncleared' OR t.cleared IS NULL)")
        if category:
            if category == "__none__":
                where.append("(t.category_name IS NULL OR t.category_name = '')")
            else:
                where.append("t.category_name = ?")
                params.append(category)
        acct_clause, acct_params = _account_where_clause(_parse_account_filter(accounts))
        if acct_clause:
            where.append(acct_clause.replace("account_id", "t.account_id"))
            params.extend(acct_params)
        where_sql = " AND ".join(where)

        conn = _open_user_db(path)
        try:
            total = conn.execute(
                f"SELECT count(*) FROM transactions t WHERE {where_sql}",
                params,
            ).fetchone()[0]
            offset = (page - 1) * page_size
            # Same WHERE clause used for total + page query. account_id
            # is the only column that exists on both joined tables, and
            # the where-builder above already prefixes it as t.account_id.
            rows = conn.execute(
                f"SELECT t.date, t.amount, t.payee_name, t.memo, t.cleared, "
                f"       t.category_name, t.ynab_tx_id, t.imported_at, "
                f"       t.account_id, a.name AS account_name, "
                f"       a.classification AS account_classification "
                f"FROM transactions t "
                f"LEFT JOIN accounts a ON a.account_id = t.account_id "
                f"WHERE {where_sql} "
                f"ORDER BY t.{sort_field} {sort_dir}, t.imported_at DESC "
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
                "category_name": r["category_name"] or "",
                "ynab_tx_id": r["ynab_tx_id"],
                "imported_at": r["imported_at"],
                "account_id": r["account_id"] or "",
                "account_name": r["account_name"] or "",
                "account_classification": r["account_classification"] or "",
            } for r in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    @app.get("/api/accounts")
    def _api_accounts(request: Request, user=Depends(_current_user)):
        """Return every YNAB account known to this user, with status.

        Schema returned per item:
            id, name, type, classification ('cash'|'credit'|'tracking'),
            on_budget, closed, deleted, balance, cleared_balance,
            uncleared_balance, last_seen_at, last_activity (last
            transaction date), tx_count (over all stored transactions),
            is_primary (whether this is the user's CSV-import account)
        """
        cfg: WebConfig = request.app.state.config
        _maybe_sync_categories(cfg, user, log)
        path = _user_tx_db_path(cfg.data_dir, user["telegram_id"])
        if not path.exists():
            return {"items": []}
        primary = user.get("account_id") or ""
        conn = _open_user_db(path)
        try:
            # Single LEFT JOIN to surface aggregate stats per account
            rows = conn.execute("""
                SELECT
                    a.account_id, a.name, a.type, a.classification,
                    a.on_budget, a.closed, a.deleted, a.balance,
                    a.cleared_balance, a.uncleared_balance,
                    a.last_seen_at, a.note,
                    (SELECT COUNT(*) FROM transactions t
                       WHERE t.account_id = a.account_id
                         AND (t.deleted = 0 OR t.deleted IS NULL))
                      AS tx_count,
                    (SELECT MAX(t.date) FROM transactions t
                       WHERE t.account_id = a.account_id
                         AND (t.deleted = 0 OR t.deleted IS NULL))
                      AS last_activity
                FROM accounts a
                ORDER BY
                    a.deleted ASC,
                    a.closed ASC,
                    CASE a.classification
                         WHEN 'cash' THEN 0
                         WHEN 'credit' THEN 1
                         WHEN 'tracking' THEN 2
                         ELSE 3 END,
                    a.name COLLATE NOCASE
            """).fetchall()
        finally:
            conn.close()
        items = [{
            "id": r["account_id"],
            "name": r["name"] or "(unnamed)",
            "type": r["type"] or "",
            "classification": r["classification"] or "tracking",
            "on_budget": bool(r["on_budget"]),
            "closed": bool(r["closed"]),
            "deleted": bool(r["deleted"]),
            "balance": (r["balance"] or 0) / 1000,
            "cleared_balance": (r["cleared_balance"] or 0) / 1000,
            "uncleared_balance": (r["uncleared_balance"] or 0) / 1000,
            "tx_count": r["tx_count"] or 0,
            "last_activity": r["last_activity"],
            "last_seen_at": r["last_seen_at"],
            "is_primary": r["account_id"] == primary,
            "note": r["note"] or "",
        } for r in rows]
        return {"items": items, "primary_account_id": primary}

    @app.get("/api/categories")
    def _api_categories(request: Request, accounts: str = "",
                        user=Depends(_current_user)):
        """Distinct categories present in the user's local DB.

        Returns each category with row count and total spend (negative
        amounts only) over all time. Used to populate the Transactions
        page filter dropdown and as input to the Spending page legend.
        """
        cfg: WebConfig = request.app.state.config
        _maybe_sync_categories(cfg, user, log)
        path = _user_tx_db_path(cfg.data_dir, user["telegram_id"])
        if not path.exists():
            return {"items": [], "uncategorized": 0}
        conn = _open_user_db(path)
        try:
            acct_clause, acct_params = _account_where_clause(
                _parse_account_filter(accounts)
            )
            extra = f" AND {acct_clause}" if acct_clause else ""
            rows = conn.execute(
                "SELECT "
                "  COALESCE(NULLIF(category_name, ''), '') AS name, "
                "  COUNT(*) AS n, "
                "  SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS spent "
                "FROM transactions "
                "WHERE (deleted = 0 OR deleted IS NULL)" + extra +
                " GROUP BY name "
                "ORDER BY name COLLATE NOCASE",
                acct_params,
            ).fetchall()
        finally:
            conn.close()
        items = []
        uncat = 0
        for r in rows:
            if not r["name"]:
                uncat = r["n"]
                continue
            items.append({
                "name": r["name"],
                "count": r["n"],
                "spent": (r["spent"] or 0) / 1000,
            })
        return {"items": items, "uncategorized": uncat}

    @app.get("/api/spending")
    def _api_spending(request: Request, months: int = 6,
                      accounts: str = "",
                      user=Depends(_current_user)):
        """Per-category spending totals for the last N months.

        Returns a row per (year, month, category_name) plus a summary
        per-category and a flat list of all months in scope. Frontend
        uses this for both the per-category month-over-month bars and
        the top-N list. Negative amounts only — positive rows
        (deposits, refunds) would skew the breakdown.
        """
        cfg: WebConfig = request.app.state.config
        _maybe_sync_categories(cfg, user, log)
        months = max(1, min(24, int(months)))
        # Floor-clamp the start to the 1st of (today - months) so we
        # always cover full months on the boundary.
        from datetime import date
        today = date.today()
        # Months back, clamped to month-start
        start_month = (today.month - months) % 12 or 12
        start_year = today.year - ((months - today.month) // 12 if today.month <= months else 0)
        # Simpler: just go back by N months naively
        start = date(today.year, today.month, 1)
        for _ in range(months):
            start = (start.replace(day=1) - timedelta(days=1)).replace(day=1)
        path = _user_tx_db_path(cfg.data_dir, user["telegram_id"])
        if not path.exists():
            return {"months": [], "by_category": {}, "totals": {}}
        conn = _open_user_db(path)
        try:
            acct_clause, acct_params = _account_where_clause(
                _parse_account_filter(accounts)
            )
            extra = f" AND {acct_clause}" if acct_clause else ""
            rows = conn.execute(
                "SELECT "
                "  substr(date, 1, 7) AS ym, "
                "  COALESCE(NULLIF(category_name, ''), '(uncategorized)') AS category, "
                "  SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END) AS spent_milli, "
                "  COUNT(CASE WHEN amount < 0 THEN 1 END) AS n "
                "FROM transactions "
                "WHERE date >= ? "
                "  AND (deleted = 0 OR deleted IS NULL)" + extra +
                " GROUP BY ym, category "
                "ORDER BY ym DESC, spent_milli DESC",
                [start.isoformat(), *acct_params],
            ).fetchall()
        finally:
            conn.close()

        # Distinct months in window, oldest first
        all_months = []
        seen_m = set()
        cur = start
        while cur <= today:
            ym = f"{cur.year:04d}-{cur.month:02d}"
            if ym not in seen_m:
                seen_m.add(ym); all_months.append(ym)
            # advance one month
            ny, nm = (cur.year + 1, 1) if cur.month == 12 else (cur.year, cur.month + 1)
            cur = date(ny, nm, 1)

        by_cat = {}        # category -> {ym: spent}
        per_month = {ym: 0.0 for ym in all_months}
        for r in rows:
            cat = r["category"]
            ym = r["ym"]
            spent = (r["spent_milli"] or 0) / 1000
            if spent <= 0:
                continue
            by_cat.setdefault(cat, {ym2: 0.0 for ym2 in all_months})
            by_cat[cat][ym] = by_cat[cat].get(ym, 0.0) + spent
            per_month[ym] = per_month.get(ym, 0.0) + spent

        # Aggregate totals per category over the whole window
        totals = []
        for cat, by_ym in by_cat.items():
            total = sum(by_ym.values())
            totals.append({
                "category": cat,
                "total": total,
                "by_month": [by_ym.get(m, 0.0) for m in all_months],
            })
        totals.sort(key=lambda x: x["total"], reverse=True)

        return {
            "months": all_months,
            "per_month_total": [per_month[m] for m in all_months],
            "categories": totals,
        }

    @app.get("/api/subscriptions")
    def _api_subscriptions(request: Request, lookback_months: int = 6,
                           min_occurrences: int = 3,
                           accounts: str = "",
                           user=Depends(_current_user)):
        """Detect recurring transactions from the user's history.

        Heuristic: group rows by ``payee_name`` (case-insensitive,
        whitespace-stripped). Within each group, sort by date and
        compute deltas between consecutive transactions. If the
        median delta sits in [25, 35] days and there are at least
        ``min_occurrences`` rows, the group is flagged as a likely
        monthly subscription. Yearly cadence ([350, 380]) is also
        flagged. Amount changes between consecutive occurrences are
        surfaced so price-creep is visible.
        """
        cfg: WebConfig = request.app.state.config
        _maybe_sync_categories(cfg, user, log)
        from datetime import date
        from statistics import median
        lookback_months = max(1, min(24, int(lookback_months)))
        min_occurrences = max(2, min(12, int(min_occurrences)))

        # Window
        start = date.today()
        for _ in range(lookback_months):
            start = (start.replace(day=1) - timedelta(days=1)).replace(day=1)

        path = _user_tx_db_path(cfg.data_dir, user["telegram_id"])
        if not path.exists():
            return {"items": [], "lookback_months": lookback_months}
        conn = _open_user_db(path)
        try:
            acct_clause, acct_params = _account_where_clause(
                _parse_account_filter(accounts)
            )
            extra = f" AND {acct_clause}" if acct_clause else ""
            rows = conn.execute(
                "SELECT date, amount, payee_name, category_name, memo, account_id "
                "FROM transactions "
                "WHERE date >= ? "
                "  AND amount < 0 "
                "  AND (deleted = 0 OR deleted IS NULL) "
                "  AND payee_name IS NOT NULL "
                "  AND payee_name != ''" + extra +
                " ORDER BY payee_name, date",
                [start.isoformat(), *acct_params],
            ).fetchall()
        finally:
            conn.close()

        # Group by normalized payee
        groups = {}
        for r in rows:
            key = (r["payee_name"] or "").strip().lower()
            if not key:
                continue
            groups.setdefault(key, []).append(r)

        out = []
        for key, txs in groups.items():
            if len(txs) < min_occurrences:
                continue
            # Compute date deltas
            dates = [datetime.strptime(t["date"], "%Y-%m-%d").date() for t in txs]
            deltas = [(dates[i+1] - dates[i]).days for i in range(len(dates) - 1)]
            if not deltas:
                continue
            med = median(deltas)
            cadence = None
            if 25 <= med <= 35:
                cadence = "monthly"
            elif 12 <= med <= 16:
                cadence = "biweekly"
            elif 6 <= med <= 8:
                cadence = "weekly"
            elif 350 <= med <= 380:
                cadence = "yearly"
            if cadence is None:
                continue
            # Amount stability: extract the unsigned amount sequence (most recent last)
            amounts = [(-t["amount"]) / 1000 for t in txs]
            mn, mx = min(amounts), max(amounts)
            # Allow ±5 cents as identical (rounding) and call anything else
            # a price change — track the latest two distinct values
            distinct = []
            for a in amounts:
                if not distinct or abs(distinct[-1] - a) > 0.05:
                    distinct.append(a)
            price_changed = len(distinct) > 1
            # Use the canonical (display) payee name from the most
            # recent transaction
            display_name = txs[-1]["payee_name"]
            categories = sorted({(t["category_name"] or "(uncategorized)") for t in txs})
            out.append({
                "payee_name": display_name,
                "cadence": cadence,
                "median_days": med,
                "occurrences": len(txs),
                "first": dates[0].isoformat(),
                "last": dates[-1].isoformat(),
                "amount_min": mn,
                "amount_max": mx,
                "amount_latest": amounts[-1],
                "amount_history": amounts,
                "price_changed": price_changed,
                "categories": [c for c in categories if c],
            })
        # Sort: monthly first, then by latest amount descending
        cadence_order = {"weekly": 0, "biweekly": 1, "monthly": 2, "yearly": 3}
        out.sort(key=lambda x: (cadence_order.get(x["cadence"], 9),
                                -x["amount_latest"]))
        return {"items": out, "lookback_months": lookback_months}

    @app.post("/api/sync")
    def _api_sync(user=Depends(_authed_csrf), request: Request = None):
        """Force a fresh YNAB → local sync (bypasses the throttle).

        Used by the 'Refresh from YNAB' button. Returns whether the
        sync actually ran (it can still skip if a concurrent sync
        was already in flight).
        """
        cfg: WebConfig = request.app.state.config
        ran = _maybe_sync_categories(cfg, user, log, force=True)
        return {"ok": True, "synced": bool(ran)}

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

    # Each delete fans out to a YNAB DELETE call; YNAB rate-limits at
    # 200/hour. Capping the array length here protects the user's quota
    # from a runaway client (or a compromised session) hammering it. See
    # PENTEST_REPORT.md L-1.
    DEDUPE_DELETE_MAX = 200

    @app.post("/api/dedupe/delete")
    def _api_dedupe_delete(payload: dict, request: Request,
                           user=Depends(_authed_csrf)):
        raw_ids = payload.get("ids") or []
        if not isinstance(raw_ids, list) or not raw_ids:
            return JSONResponse(
                {"error": "no_ids", "message": "No transaction IDs given."},
                status_code=400,
            )
        # Validate + dedupe + cap. YNAB tx IDs are UUIDs (~36 chars);
        # anything dramatically longer is junk and we reject the whole
        # request rather than silently dropping items.
        ids = []
        seen = set()
        for x in raw_ids:
            if not isinstance(x, str):
                return JSONResponse(
                    {"error": "bad_ids",
                     "message": "Every id must be a string."},
                    status_code=400,
                )
            if len(x) > 64 or not x.strip():
                return JSONResponse(
                    {"error": "bad_ids",
                     "message": "An id is empty or implausibly long."},
                    status_code=400,
                )
            if x not in seen:
                seen.add(x)
                ids.append(x)
        if len(ids) > DEDUPE_DELETE_MAX:
            return JSONResponse(
                {"error": "too_many_ids",
                 "message": f"At most {DEDUPE_DELETE_MAX} transactions per "
                            f"request (YNAB rate limit). Got {len(ids)}."},
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


def _delete_login_message(bot_token, chat_id, message_id, log):
    """Best-effort: delete the bot's /login DM after the user signs in.

    Runs on a daemon thread; we never raise out of here. Telegram bots
    can delete their own messages within 48 hours, so this should
    succeed unless the user has already deleted the chat or revoked the
    bot. Failures are logged at INFO and otherwise ignored.
    """
    try:
        url = f"https://api.telegram.org/bot{bot_token}/deleteMessage"
        body = json.dumps(
            {"chat_id": int(chat_id), "message_id": int(message_id)}
        ).encode("utf-8")
        from urllib.request import Request
        from urllib.error import HTTPError, URLError
        from urllib.request import urlopen
        req = Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        try:
            with urlopen(req, timeout=8) as resp:
                _ = resp.read()
            log.info(
                "web: deleted /login DM chat=%s msg=%s after sign-in",
                chat_id, message_id,
            )
        except HTTPError as e:
            err = e.read().decode("utf-8", "replace")
            log.info(
                "web: deleteMessage chat=%s msg=%s failed (%s): %s",
                chat_id, message_id, e.code, err[:200],
            )
        except URLError as e:
            log.info("web: deleteMessage network error: %s", e)
    except Exception as e:
        log.warning("web: _delete_login_message crashed: %s", e)


def _bot_version_line():
    try:
        import revolut_ynab_bot as bot
        return bot.format_version_line()
    except Exception:
        return "v? (?, ?)"


def _cache_bust_token():
    """Short opaque token that changes on every deploy.

    Format: ``<version>-<sha>``. Used as ``?v=…`` on every reference to
    a /static/ asset so browsers don't serve stale CSS/JS after a
    deploy. Falls back to a constant if the bot module can't be
    imported (which would only happen during very early boot).
    """
    try:
        import revolut_ynab_bot as bot
        sha = (bot.get_version_info() or {}).get("sha", "dev")
        return f"{bot.__version__}-{sha}"
    except Exception:
        return "dev"


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
