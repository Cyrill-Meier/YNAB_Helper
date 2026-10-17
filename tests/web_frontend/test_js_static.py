"""Source-level guarantees of the browser JavaScript (DESIGN §9.4; F23, F62, F91/F96 for the
web, F106): markup only through the escaping html`` helper, every request through the CSRF
fetch wrapper, no inline styles or handlers, no Date parsing of server strings, no external
origins, imports that resolve, and (when Node.js is installed) valid syntax."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from .conftest import CLASSIC_SCRIPTS, JS_DIR, js_files, node_path

HTML_HELPER = JS_DIR / "core" / "html.js"
API_WRAPPER = JS_DIR / "core" / "api.js"


def _rel(path: Path) -> str:
    return path.relative_to(JS_DIR).as_posix()


def _code(path: Path) -> str:
    """The source without // line comments and /* block */ comments."""
    text = path.read_text(encoding="utf-8")
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return re.sub(r"(?m)(^|[^:\"'`\\])//.*$", r"\1", text)


def test_there_are_js_files():
    names = {_rel(p) for p in js_files()}
    assert {
        "main.js",
        "theme.js",
        "auth.js",
        "core/api.js",
        "core/html.js",
        "core/format.js",
    } <= names


#: Markup, script and URL sinks. Matched as tokens, whatever the syntax around them
#: (``el.innerHTML += x``, ``el["innerHTML"] = x``, ``Object.assign(el, {innerHTML: x})``).
_SINKS = re.compile(
    r"\b(?:innerHTML|outerHTML|insertAdjacentHTML|srcdoc|setHTMLUnsafe|parseHTMLUnsafe|"
    r"createContextualFragment|DOMParser)\b|"
    r"\bdocument\.write(?:ln)?\s*\(|\beval\s*\(|\bnew\s+Function\s*\(|\bwindow\.open\s*\(|"
    r"\bset(?:Timeout|Interval)\s*\(\s*[\"'`]|"
    r"setAttribute\(\s*[\"'`](?:on\w+|style|href|src|srcset|srcdoc|action|formaction|data)[\"'`]|"
    r"\.(?:href|src|srcset|action|formAction|poster)\s*\+?=(?!=)|"
    r"\blocation\s*=(?!=)"
)
#: location.assign/replace may only take these constant targets.
_NAVIGATION = re.compile(r"\blocation\.(?:assign|replace)\(\s*([^)]*)\)")
_ALLOWED_NAVIGATION = frozenset({'"/"', '"/?expired=1"'})


def dom_sinks(code: str) -> list[str]:
    """Every place in `code` that could turn data into markup, script or a navigation."""
    found = [m.group(0) for m in _SINKS.finditer(code)]
    found += [
        m.group(0)
        for m in _NAVIGATION.finditer(code)
        if m.group(1).strip() not in _ALLOWED_NAVIGATION
    ]
    return found


@pytest.mark.parametrize("path", js_files(), ids=_rel)
def test_markup_only_through_the_html_helper(path):
    """innerHTML (and every other markup/URL sink) appears only in core/html.js's setHTML(),
    which accepts nothing but fragments built by the escaping html`` tagged template."""
    code = _code(path)
    sinks = dom_sinks(code)
    if path == HTML_HELPER:
        # setHTML's one assignment, and srcdoc in the regex that refuses it.
        assert sorted(sinks) == ["innerHTML", "srcdoc"], sinks
        assert "element.innerHTML = fragment.toString();" in code
        assert "if (!(fragment instanceof SafeHtml))" in code
        assert re.search(r"FORBIDDEN_ATTR_RE = .*srcdoc", code)
    else:
        assert not sinks, f"{_rel(path)} writes markup or URLs outside setHTML(): {sinks}"


UNSAFE_SNIPPETS = [
    "el.innerHTML = x;",
    "el.innerHTML += x;",
    'el["innerHTML"] = x;',
    "Object.assign(el, { innerHTML: x });",
    "el.outerHTML = x;",
    'el.insertAdjacentHTML("beforeend", x);',
    "el.setHTMLUnsafe(x);",
    "frame.srcdoc = x;",
    "new DOMParser().parseFromString(x, 'text/html');",
    "document.write(x);",
    "eval(x);",
    "new Function(x)();",
    'setTimeout("go()", 10);',
    "window.open(x);",
    'el.setAttribute("href", x);',
    "el.setAttribute('onclick', x);",
    "a.href = x;",
    "img.src += x;",
    "form.action = x;",
    "window.location.href = x;",
    "location = x;",
    "window.location.assign(x);",
    "location.replace(next);",
    'window.location.assign("https://evil.example/");',
]
SAFE_SNIPPETS = [
    "if (a.href === b) return;",
    "el.textContent = x;",
    "el.title = x;",
    'window.location.assign("/");',
    'window.location.assign("/?expired=1");',
    "window.location.reload();",
    "this.data = data;",
    "setTimeout(() => go(), 10);",
]


@pytest.mark.parametrize("snippet", UNSAFE_SNIPPETS)
def test_sink_scan_catches_unsafe_code(snippet):
    """The scan above is not vacuous: each of these would fail it."""
    assert dom_sinks(snippet), snippet


@pytest.mark.parametrize("snippet", SAFE_SNIPPETS)
def test_sink_scan_accepts_safe_code(snippet):
    assert not dom_sinks(snippet), snippet


@pytest.mark.parametrize("path", js_files(), ids=_rel)
def test_raw_escape_hatch_is_not_used_with_data(path):
    """raw() is defined in html.js and not called anywhere else."""
    calls = re.findall(r"\braw\(", _code(path))
    if path == HTML_HELPER:
        return
    assert not calls, f"{_rel(path)} uses raw(); render data with html`` instead"


@pytest.mark.parametrize("path", js_files(), ids=_rel)
def test_set_html_is_never_given_a_string(path):
    """setHTML(target, …) gets an html`` fragment (or an expression producing one) — never a
    plain string or untagged template literal (setHTML would also throw at runtime)."""
    if path == HTML_HELPER:
        return
    code = _code(path)
    for match in re.finditer(r"setHTML\(\s*[^,]+,\s*", code):
        rest = code[match.end() : match.end() + 40]
        assert not rest.startswith(("'", '"', "`")), f"{_rel(path)}: setHTML({rest!r}"
        assert not re.match(r"String\(|`|\w+\.textContent", rest), f"{_rel(path)}: {rest!r}"


@pytest.mark.parametrize("path", js_files(), ids=_rel)
def test_no_inline_styles_or_handlers_in_generated_markup(path):
    code = _code(path)
    assert not re.search(r"\sstyle\s*=\s*[\"'$]", code), "style attribute in generated markup"
    assert not re.search(r"<[^>]*\son[a-z]+\s*=", code), "event-handler attribute in markup"
    assert ".style." not in code and ".cssText" not in code, "use classes, not inline styles"


@pytest.mark.parametrize("path", js_files(), ids=_rel)
def test_F106_server_dates_are_never_parsed(path):
    code = _code(path)
    assert "new Date(" not in code and "Date.parse(" not in code, _rel(path)
    assert "toLocaleString(" not in code and "toLocaleDateString(" not in code


@pytest.mark.parametrize("path", js_files(), ids=_rel)
def test_F23_no_external_origins(path):
    code = _code(path)
    assert not re.search(r"https?://", code), f"{_rel(path)} references an external URL"
    assert "cdn." not in code


@pytest.mark.parametrize("path", js_files(), ids=_rel)
def test_every_request_goes_through_the_csrf_wrapper(path):
    code = _code(path)
    if path == API_WRAPPER:
        assert re.search(r'"X-CSRF-Token":\s*CSRF_TOKEN', code), "the header is always set"
        assert 'credentials: "same-origin"' in code
        assert "/?expired=1" in code and '"csrf_failed"' in code
        return
    assert not re.search(r"\bfetch\(|XMLHttpRequest|sendBeacon|EventSource|WebSocket", code), (
        f"{_rel(path)} talks to the server without core/api.js"
    )


@pytest.mark.parametrize("path", js_files(), ids=_rel)
def test_imports_resolve_to_served_files(path):
    code = _code(path)
    specifiers = re.findall(
        r"""(?<![\w"'-])from\s+["']([^"']+)["']|\bimport\s*\(\s*["']([^"']+)["']\s*\)|"""
        r"""^\s*import\s+["']([^"']+)["']""",
        code,
        flags=re.M,
    )
    specifiers = [next(part for part in found if part) for found in specifiers]
    if path.name == "main.js":
        assert len(specifiers) >= 11, specifiers  # api, nav and the nine page modules
    if path.name in CLASSIC_SCRIPTS:
        assert not specifiers, "classic scripts cannot import"
        return
    for spec in specifiers:
        assert spec.startswith(("./", "../")) and spec.endswith(".js"), (
            f"{_rel(path)}: import {spec!r} must be a relative .js path (no bare/remote modules)"
        )
        target = (path.parent / spec).resolve()
        assert target.is_file(), f"{_rel(path)}: {spec} does not exist"
        assert JS_DIR.resolve() in target.parents


def test_theme_follows_v1_storage_and_class():
    code = _code(JS_DIR / "theme.js")
    assert 'var KEY = "theme"' in code and "localStorage" in code
    assert 'classList.toggle("dark"' in code
    assert "prefers-color-scheme: dark" in code


def test_F99_account_picker_source_contract():
    """Without Node.js the behaviour checks skip; the source still has to keep the shape
    (test_js_behaviour.py and test_js_pages.py exercise the behaviour itself)."""
    code = _code(JS_DIR / "components" / "account_picker.js")
    # Clicks inside the picker never close it; changes are debounced and flushed on close.
    assert "!this.root.contains(event.target)" in code
    assert "this.notify = debounce(" in code and "this.notify.flush()" in code
    assert 'event.key === "Escape"' in code and "aria-expanded" in code
    assert "render()" in code and code.count("setHTML(") == 1, "rendered once, updated in place"
    assert "export function panelSide(" in code and "if (open) this.place();" in code


def test_F104_dedupe_deletes_in_chunks_of_the_server_cap():
    from ynab_helper.maintenance.dedupe import MAX_DELETE_IDS

    chunk_source = _code(JS_DIR / "core" / "chunk.js")
    assert f"DEDUPE_CHUNK_SIZE = {MAX_DELETE_IDS};" in chunk_source
    page = _code(JS_DIR / "pages" / "dedupe.js")
    assert "for (const batch of batches)" in page and "chunk(" in page
    assert '"/api/dedupe/delete", { scan_id: state.scan.scan_id, ids: batch }' in page


def test_F62_pages_report_errors_through_the_wrapper():
    """Pages never build error text from statusText; they use reportError/describeError."""
    for path in (JS_DIR / "pages").glob("*.js"):
        code = _code(path)
        assert "statusText" not in code, path.name
        assert "console.log" not in code, path.name


def _node_check(node: str, path: Path) -> subprocess.CompletedProcess[str]:
    if path.name in CLASSIC_SCRIPTS:
        return subprocess.run(
            [node, "--check", str(path)], capture_output=True, text=True, timeout=60
        )
    # `node --check file.js` does not reliably check ES-module syntax; stdin with an
    # explicit module input type does.
    return subprocess.run(
        [node, "--input-type=module", "--check"],
        input=path.read_text(encoding="utf-8"),
        capture_output=True,
        text=True,
        timeout=60,
    )


@pytest.mark.parametrize("path", js_files(), ids=_rel)
def test_js_syntax_with_node(path):
    node = node_path()
    if node is None:
        pytest.skip("node is not installed (optional)")
    result = _node_check(node, path)
    assert result.returncode == 0, result.stderr


def test_node_syntax_check_catches_errors(tmp_path):
    """The check above is not vacuous: a module with a syntax error fails it."""
    node = node_path()
    if node is None:
        pytest.skip("node is not installed (optional)")
    broken = tmp_path / "broken.js"
    broken.write_text('import { a } from "./a.js";\nconst x = ;\n', encoding="utf-8")
    assert _node_check(node, broken).returncode != 0
