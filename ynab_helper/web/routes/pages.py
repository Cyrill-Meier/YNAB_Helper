"""The app's HTML pages (docs/DESIGN.md §2.8, §9.1).

``/app`` redirects to the dashboard; ``/app/<page>`` renders ``<page>.html`` for the pages
in :data:`ynab_helper.web.deps.PAGES` only (anything else is a 404). A missing or expired
session redirects (303) to the login page and clears the cookie (F24). ``?notice=<key>``
selects one of the fixed :data:`ynab_helper.web.deps.NOTICES` (unknown keys are ignored;
no text from the URL is ever shown).
"""

from __future__ import annotations

from fastapi import APIRouter
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from ynab_helper.web.deps import PAGES, get_state, page_user, render

__all__ = ["router"]

router = APIRouter()


@router.get("/app", include_in_schema=False)
def app_root() -> Response:
    return RedirectResponse("/app/dashboard", status_code=303)


@router.get("/app/{page}", include_in_schema=False)
def app_page(request: Request, page: str, notice: str | None = None) -> Response:
    """One of the allowlisted app pages (the session is checked after the page name)."""
    if page not in PAGES:
        raise HTTPException(status_code=404)
    authed = page_user(request)
    return render(
        get_state(request), request, f"{page}.html", page=page, authed=authed, notice=notice
    )
