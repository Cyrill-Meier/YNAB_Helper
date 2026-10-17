"""HTTP routes of the web UI (docs/DESIGN.md §9.1, §9.2).

* :mod:`.auth` — ``/health``, ``/`` (login page), ``/auth`` (GET interstitial, POST
  sign-in), ``/logout``.
* :mod:`.pages` — ``/app`` and the allowlisted ``/app/<page>`` HTML pages.
* :mod:`.api` — JSON reads from the mirror (``/api/me``, ``/api/dashboard``,
  ``/api/transactions``, ``/api/accounts``, ``/api/categories``, ``/api/spending``,
  ``/api/subscriptions``).
* :mod:`.actions` — operations through the service layer (``/api/sync``,
  ``/api/reconcile/preview``, ``/api/reconcile``, ``/api/dedupe/scan``,
  ``/api/dedupe/delete``, ``/api/settings``, ``/api/upload``, ``/api/logout-all``).

:data:`CSRF_PROTECTED_ROUTES` lists every state-changing route; each requires the session's
CSRF token (header ``X-CSRF-Token``; ``/logout`` also accepts a ``csrf_token`` form
field). ``POST /auth`` is the only other POST: it has no session yet and is protected by
the one-time login token, a same-origin check and the rate limiter. A test asserts that
the app has no other POST/PUT/PATCH/DELETE route.

:data:`SIDE_EFFECT_GET_ROUTES` are GETs that are not read-only: they run a forced YNAB
sync and replace the user's pending reconcile token or dedupe scan. They require the CSRF
header too and are throttled per user. A test asserts that exactly these GETs depend on
the CSRF check.
"""

from __future__ import annotations

from typing import Final

from fastapi import FastAPI

from ynab_helper.web.routes import actions, api, auth, pages

__all__ = [
    "CSRF_PROTECTED_ROUTES",
    "PUBLIC_POST_ROUTES",
    "SIDE_EFFECT_GET_ROUTES",
    "include_routes",
]

#: Every state-changing route that requires the CSRF token, as ``(method, path)``.
CSRF_PROTECTED_ROUTES: Final = (
    ("POST", "/logout"),
    ("POST", "/api/logout-all"),
    ("POST", "/api/sync"),
    ("POST", "/api/reconcile"),
    ("POST", "/api/dedupe/delete"),
    ("POST", "/api/settings"),
    ("POST", "/api/upload"),
)
#: POST routes without a session (protected otherwise, see the module docstring).
PUBLIC_POST_ROUTES: Final = (("POST", "/auth"),)
#: GETs with side effects (forced YNAB sync, replaced token/scan): CSRF header + throttle.
SIDE_EFFECT_GET_ROUTES: Final = (
    ("GET", "/api/reconcile/preview"),
    ("GET", "/api/dedupe/scan"),
)


def include_routes(app: FastAPI) -> None:
    """Register every router on ``app``."""
    for module in (auth, pages, api, actions):
        app.include_router(module.router)
