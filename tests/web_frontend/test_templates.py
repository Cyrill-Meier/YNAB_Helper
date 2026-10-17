"""Every template renders with the backend's context and is CSP-clean (DESIGN §9.4; F23,
F24, F62, F66, F102): no inline scripts or styles, no event-handler attributes, no external
URLs, every page boots its JS module, controls are labelled, live regions and the native
confirmation dialog exist, and user-controlled names are escaped."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from ynab_helper.web.deps import NAV, NOTICES, PAGES

from .conftest import (
    CLASSIC_SCRIPTS,
    JS_DIR,
    STATIC_BASE,
    TEMPLATES_DIR,
    PageParser,
    issue_token,
    login,
    make_ready,
    parse,
)

URL_ATTRIBUTES = ("href", "src", "action", "formaction", "poster", "srcset", "xlink:href", "data")
_EXTERNAL = re.compile(r"^\s*(?:[a-z][a-z0-9+.-]*:|//|\\\\)", re.IGNORECASE)
_EMOJI = re.compile("[\U0001f300-\U0001faff☀-➿]")


def assert_csp_clean(page: PageParser, html: str) -> None:
    """No inline script/style, no on* or style attributes, no external URLs."""
    for script in page.find_all("script"):
        assert script.attrs.get("src"), f"inline <script> found: {script.attrs}"
        assert not script.text.strip(), "a <script src> must not carry a body"
        assert script.attrs["src"].startswith(f"{STATIC_BASE}/js/"), script.attrs["src"]
    assert not page.find_all("style"), "inline <style> element"
    for element in page.elements:
        for name, value in element.attrs.items():
            assert not name.startswith("on"), f"event-handler attribute {name} on <{element.tag}>"
            assert name != "style", f"style attribute on <{element.tag}>"
            if name in URL_ATTRIBUTES:
                assert not _EXTERNAL.match(value), (
                    f"external/scheme URL {value!r} in <{element.tag}>"
                )
    for link in page.find_all("link"):
        assert link.attrs["href"].startswith(f"{STATIC_BASE}/"), link.attrs
    assert "cdn.tailwindcss.com" not in html and "https://" not in html, "external origin"


def accessible_name_ok(page: PageParser, element) -> bool:
    if element.attrs.get("aria-label") or element.attrs.get("aria-labelledby"):
        return True
    element_id = element.attrs.get("id")
    if element_id and any(lbl.attrs.get("for") == element_id for lbl in page.find_all("label")):
        return True
    return any(a.tag == "label" for a in element.ancestors())


def assert_controls_labelled(page: PageParser) -> None:
    for element in page.elements:
        if element.tag in ("select", "textarea") or (
            element.tag == "input" and element.attrs.get("type") not in ("hidden", "submit")
        ):
            assert accessible_name_ok(page, element), f"unlabelled control: {element.attrs}"
        if element.tag == "button":
            name = element.text.strip() or element.attrs.get("aria-label")
            assert name or element.attrs.get("aria-labelledby"), (
                f"button without a name: {element.attrs}"
            )


def assert_emoji_hidden(page: PageParser) -> None:
    """Decorative emoji are aria-hidden (F102)."""
    for text, node in page.texts:
        if _EMOJI.search(text):
            chain = [node, *node.ancestors()]
            assert any(e.attrs.get("aria-hidden") == "true" for e in chain), (
                f"emoji {text.strip()!r} is not aria-hidden"
            )


# ── app pages ────────────────────────────────────────────────────────────────


def test_every_template_is_covered():
    names = {p.name for p in TEMPLATES_DIR.glob("*.html")}
    expected = {f"{page}.html" for page in PAGES} | {
        "base.html",
        "_public.html",
        "login.html",
        "auth.html",
    }
    assert names == expected


@pytest.mark.parametrize("page_name", list(PAGES))
def test_app_page_renders_csp_clean(signed_in, page_name):
    response = signed_in.get(f"/app/{page_name}")
    assert response.status_code == 200
    html = response.text
    page = parse(html)
    assert_csp_clean(page, html)
    body = page.find_all("body")[0]
    assert body.attrs["data-page"] == page_name
    assert re.search(r'<meta name="csrf-token" content="[0-9a-f]{32}">', html)
    scripts = [s.attrs["src"] for s in page.find_all("script")]
    assert scripts == [f"{STATIC_BASE}/js/theme.js", f"{STATIC_BASE}/js/main.js"]
    (main,) = [s for s in page.find_all("script") if s.attrs["src"].endswith("/main.js")]
    assert main.attrs.get("type") == "module"
    (theme,) = [s for s in page.find_all("script") if s.attrs["src"].endswith("/theme.js")]
    assert theme.parent.tag == "head" and "defer" not in theme.attrs, "theme before first paint"
    assert [lnk.attrs["href"] for lnk in page.find_all("link", rel="stylesheet")] == [
        f"{STATIC_BASE}/app.css"
    ]


@pytest.mark.parametrize("page_name", list(PAGES))
def test_F102_app_page_accessibility_basics(signed_in, page_name):
    page = parse(signed_in.get(f"/app/{page_name}").text)
    assert_controls_labelled(page)
    assert_emoji_hidden(page)
    current = page.find_all("a", **{"aria-current": "page"})
    assert [a.attrs["href"] for a in current] == [f"/app/{page_name}"]
    nav = page.by_id("site-nav")
    assert nav is not None and nav.tag == "nav" and nav.attrs.get("aria-label")
    assert [a.attrs["href"] for li in nav.children[0].children for a in li.children] == [
        href for _key, _label, href in NAV
    ]
    toggle = page.by_id("nav-toggle")
    assert toggle.attrs["aria-controls"] == "site-nav" and toggle.attrs["aria-expanded"] == "false"
    toasts = page.by_id("toasts")
    assert toasts.attrs.get("aria-live") == "polite", "toasts are announced"
    dialog = page.by_id("confirm-dialog")
    assert dialog is not None and dialog.tag == "dialog", "native <dialog> for confirmations"
    assert dialog.attrs["aria-labelledby"] == "confirm-title"
    main = page.by_id("main")
    assert main.tag == "main"
    skip = page.find_all("a", href="#main")
    assert skip, "skip link"
    assert page.find_all("html")[0].attrs.get("lang") == "en"


_FOCUSABLE = ("a", "button", "input", "select", "textarea")


def test_F102_menu_toggle_is_followed_by_the_navigation(signed_in):
    """Tab from the compact-menu toggle goes straight to the first page link: the toggle is
    the header's last control and nothing focusable sits between it and the panel."""
    page = parse(signed_in.get("/app/dashboard").text)
    focusable = [
        e for e in page.elements if e.tag in _FOCUSABLE and e.attrs.get("type") != "hidden"
    ]
    toggle = page.by_id("nav-toggle")
    nav = page.by_id("site-nav")
    (header,) = page.find_all("header")
    links = [e for e in focusable if nav in e.ancestors()]
    controls = [e for e in focusable if header in e.ancestors() and nav not in e.ancestors()]
    assert controls[-1] is toggle, [c.attrs for c in controls[-2:]]
    assert focusable[focusable.index(toggle) + 1] is links[0]
    logout = [c for c in controls if c.tag == "button" and c.text.strip() == "Logout"]
    assert logout and controls.index(logout[0]) < controls.index(toggle)


def test_header_shows_user_and_budget_and_escapes_them(client, users_db, config):
    from ynab_helper.storage import connect

    conn = connect(users_db)
    try:
        make_ready(conn, first_name='<script>alert("x")</script>', budget_name="Budget <b>&</b>")
    finally:
        conn.close()
    login(client, users_db, config)
    html = client.get("/app/dashboard").text
    assert "<script>alert" not in html and "<b>&</b>" not in html
    chip = parse(html).by_id("user-chip")
    assert chip is not None
    assert '<script>alert("x")</script>' in chip.text and "Budget <b>&</b>" in chip.text


def test_notice_is_rendered_as_status_only_from_the_fixed_table(signed_in):
    html = signed_in.get("/app/dashboard", params={"notice": "logout_stale"}).text
    page = parse(html)
    status = [p for p in page.find_all("p") if p.attrs.get("role") == "status"]
    assert [p.text.strip() for p in status] == [NOTICES["logout_stale"]]
    html = signed_in.get("/app/dashboard", params={"notice": "<i>x</i>"}).text
    assert "<i>x</i>" not in html and "&lt;i&gt;" not in html


def test_logout_forms_post_the_csrf_token(client, users_db, config):
    csrf = login(client, users_db, config)
    html = client.get("/app/settings").text
    forms = parse(html).find_all("form", action="/logout")
    assert len(forms) == 2  # header + Settings card
    for form in forms:
        assert form.attrs["method"] == "post"
        fields = [c for c in form.children if c.tag == "input"]
        assert [(f.attrs["name"], f.attrs["value"]) for f in fields] == [("csrf_token", csrf)]


def page_module_ids(page_name: str) -> set[str]:
    source = (JS_DIR / "pages" / f"{page_name}.js").read_text(encoding="utf-8")
    return set(re.findall(r'byId\("([A-Za-z0-9_-]+)"\)', source))


@pytest.mark.parametrize("page_name", list(PAGES))
def test_page_module_finds_every_element_it_needs(signed_in, page_name):
    """The ids a page module looks up with byId("…") exist in its template (a renamed id
    would otherwise only fail in the browser)."""
    page = parse(signed_in.get(f"/app/{page_name}").text)
    present = {e.attrs["id"] for e in page.elements if "id" in e.attrs}
    created_by_js = {"rec-apply", "rec-expiry"}  # rendered by the module itself
    missing = page_module_ids(page_name) - present - created_by_js
    assert not missing, f"{page_name}.js expects missing ids: {sorted(missing)}"


def test_F66_upload_input_is_keyboard_reachable(signed_in):
    page = parse(signed_in.get("/app/upload").text)
    file_input = page.by_id("up-input")
    assert file_input.attrs["type"] == "file"
    classes = file_input.attrs.get("class", "").split()
    assert "sr-only" in classes, "visually hidden but focusable"
    assert "hidden" not in classes and "hidden" not in file_input.attrs
    assert "disabled" not in file_input.attrs and "tabindex" not in file_input.attrs
    assert file_input.attrs.get("aria-describedby") == "up-hint"
    drop = page.by_id("up-drop")
    assert drop.tag == "label" and drop.attrs.get("for") == "up-input"
    assert "focus-within:ring-2" in drop.attrs["class"], "the drop zone shows keyboard focus"
    source = (JS_DIR / "pages" / "upload.js").read_text(encoding="utf-8")
    assert ".click()" not in source, "no extra programmatic click (one file chooser)"


def test_F102_settings_switch_is_named(signed_in):
    page = parse(signed_in.get("/app/settings").text)
    switch = page.by_id("set-auto")
    assert switch.attrs["role"] == "switch"
    assert switch.attrs["aria-checked"] in ("true", "false")
    assert page.by_id(switch.attrs["aria-labelledby"]) is not None
    assert page.by_id(switch.attrs["aria-describedby"]) is not None


# ── login and sign-in pages ──────────────────────────────────────────────────


def test_login_page_renders_csp_clean(client):
    response = client.get("/")
    assert response.status_code == 200
    page = parse(response.text)
    assert_csp_clean(page, response.text)
    assert page.find_all("body")[0].attrs["data-page"] == "login"
    assert [s.attrs["src"] for s in page.find_all("script")] == [f"{STATIC_BASE}/js/theme.js"]
    assert 'name="csrf-token"' not in response.text
    assert_emoji_hidden(page)


def test_F24_login_page_explains_an_expired_session(client):
    page = parse(client.get("/?expired=1").text)
    statuses = [p.text for p in page.find_all("p") if p.attrs.get("role") == "status"]
    assert any("session has expired" in text for text in statuses)


def test_login_errors_are_alerts(client):
    response = client.get("/auth", params={"t": "bad token!"})
    assert response.status_code == 400
    page = parse(response.text)
    assert_csp_clean(page, response.text)
    alerts = [p for p in page.find_all("p") if p.attrs.get("role") == "alert"]
    assert alerts and "expired or was already used" in alerts[0].text


def test_auth_interstitial_auto_submits_without_a_session(client, users_db, config):
    token = issue_token(users_db, config)
    response = client.get("/auth", params={"t": token})
    page = parse(response.text)
    assert_csp_clean(page, response.text)
    (form,) = page.find_all("form")
    assert form.attrs["id"] == "auth-form" and form.attrs["data-autosubmit"] == "true"
    assert (form.attrs["method"], form.attrs["action"]) == ("post", "/auth")
    assert [s.attrs["src"] for s in page.find_all("script")] == [
        f"{STATIC_BASE}/js/theme.js",
        f"{STATIC_BASE}/js/auth.js",
    ]
    assert any(b.text.strip() == "Continue" for b in page.find_all("button"))
    assert "no-referrer" not in response.text


def test_auth_interstitial_asks_before_switching_accounts(client, users_db, config):
    login(client, users_db, config)
    token = issue_token(users_db, config)
    response = client.get("/auth", params={"t": token})
    page = parse(response.text)
    assert_csp_clean(page, response.text)
    (form,) = page.find_all("form")
    assert "data-autosubmit" not in form.attrs
    assert "Switch account?" in response.text and "Ann" in response.text
    assert "Household" in response.text
    stay = page.find_all("a", href="/app/dashboard")
    assert stay and "Stay signed in" in stay[0].text


def test_auth_js_submits_only_marked_forms():
    source = (JS_DIR / "auth.js").read_text(encoding="utf-8")
    assert 'dataset.autosubmit === "true"' in source
    assert "auth.js" in CLASSIC_SCRIPTS


# ── static files ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "path", ["app.css", "favicon.svg", "js/main.js", "js/theme.js", "js/auth.js"]
)
def test_referenced_static_files_are_served(client, path):
    response = client.get(f"{STATIC_BASE}/{path}")
    assert response.status_code == 200
    assert "immutable" in response.headers.get("cache-control", "")
    if path.endswith(".js"):
        assert "javascript" in response.headers["content-type"]


def test_every_page_module_exists_and_is_registered_in_main():
    main = (JS_DIR / "main.js").read_text(encoding="utf-8")
    table = dict(re.findall(r'(\w+): \(\) => import\("\./pages/(\w+)\.js"\)', main))
    assert set(table) == set(PAGES), "main.js boots exactly the app pages"
    for key, module in table.items():
        assert key == module
        path = JS_DIR / "pages" / f"{module}.js"
        assert path.is_file(), path
        assert re.search(r"^export async function init\(\)", path.read_text("utf-8"), re.M)


def test_templates_contain_no_inline_code_in_source():
    """Template sources too (blocks a template might only render in some states)."""
    for path in TEMPLATES_DIR.glob("*.html"):
        source = path.read_text(encoding="utf-8")
        assert not re.search(r"<script(?![^>]*\bsrc=)[^>]*>", source), path.name
        assert "<style" not in source, path.name
        assert not re.search(r"\sstyle\s*=", source), path.name
        assert not re.search(r"\son[a-z]+\s*=", source), path.name
        assert not re.search(r"(?:src|href|action)=\"(?:https?:)?//", source), path.name


def test_template_files_are_utf8_and_end_with_newline():
    for path in Path(TEMPLATES_DIR).glob("*.html"):
        data = path.read_bytes()
        data.decode("utf-8")
        assert data.endswith(b"\n"), path.name
