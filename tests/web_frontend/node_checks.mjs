// Behavioural checks of the browser modules, run by test_js_behaviour.py when Node.js is
// available (it is optional). Usage: node node_checks.mjs <path to ynab_helper/web/static/js>
// Prints one JSON object {check name: "ok" | failure message}. A small fake DOM
// (fake_dom.mjs) stands in for the browser; it is installed before the modules are imported.

import { pathToFileURL } from "node:url";
import { join } from "node:path";
import { FakeDocument, FakeEvent, install, settle } from "./fake_dom.mjs";

const jsDir = process.argv[2];
const mod = (rel) => import(pathToFileURL(join(jsDir, rel)).href);
const results = {};

function assert(condition, message) {
  if (!condition) throw new Error(message);
}
function eq(actual, expected, label = "value") {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) throw new Error(`${label}: expected ${e}, got ${a}`);
}
/** The rejection of `promise`; fails the check when it resolves (no vacuous catch blocks). */
async function rejection(promise, label) {
  try {
    await promise;
  } catch (err) {
    return err;
  }
  throw new Error(`${label}: expected a rejection, but it resolved`);
}
async function check(name, fn) {
  try {
    await fn();
    results[name] = "ok";
  } catch (err) {
    results[name] = String(err && err.stack ? err.stack : err);
  }
}

// ── the page the modules see ─────────────────────────────────────────────────
const PAGE = `<!doctype html><html lang="en"><head><meta name="csrf-token" content="csrf-123"></head>
<body data-page="test"><main id="main"></main>
<div id="toasts" aria-live="polite"></div>
<dialog id="confirm-dialog"><h2 id="confirm-title"></h2><div id="confirm-body"></div>
<button type="button" id="confirm-cancel">Cancel</button><button type="button" id="confirm-ok">Confirm</button></dialog>
</body></html>`;
const doc = new FakeDocument(PAGE);
const win = install(doc, { path: "/app/transactions" });
const main = doc.getElementById("main");
const toasts = doc.getElementById("toasts");

let fetchImpl = null;
globalThis.fetch = (url, init) => fetchImpl(url, init);
function jsonResponse(status, body, headers = {}) {
  const all = { "content-type": "application/json", ...headers };
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (name) => all[name.toLowerCase()] ?? null },
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}
function textResponse(status, text) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (name) => (name.toLowerCase() === "content-type" ? "text/html" : null) },
    json: async () => {
      throw new Error("not json");
    },
    text: async () => text,
  };
}
function clearToasts() {
  toasts.replaceChildren();
}

// ── html.js ───────────────────────────────────────────────────────────────────
const { html, raw, setHTML, isSafeHtml, isSafeUrl, escapeHtml } = await mod("core/html.js");

await check("html_escapes_every_interpolation", () => {
  const evil = `<img src=x onerror="alert(1)">'&\``;
  eq(String(html`<td>${evil}</td>`), "<td>&lt;img src=x onerror=&quot;alert(1)&quot;&gt;&#39;&amp;&#96;</td>");
  eq(String(html`<a title="${'" onclick="x'}">t</a>`), '<a title="&quot; onclick=&quot;x">t</a>');
  eq(escapeHtml(5), "5");
});

await check("html_nests_fragments_and_arrays", () => {
  const items = ["<b>", "c"].map((x) => html`<li>${x}</li>`);
  eq(String(html`<ul>${items}</ul>`), "<ul><li>&lt;b&gt;</li><li>c</li></ul>");
  eq(String(html`${null}${undefined}${false}${0}`), "0");
  assert(isSafeHtml(html``), "html`` returns a SafeHtml");
  eq(String(html`${raw("<svg></svg>")}`), "<svg></svg>");
});

await check("html_url_attributes_only_take_same_site_urls", () => {
  for (const bad of [
    "javascript:alert(1)",
    "https://evil.example/",
    "//evil.example/",
    "/\\evil.example/",
    "/\t/evil.example/x", // browsers strip tabs and newlines: "//evil.example/x"
    "/\n/evil.example/x",
    "/\r/evil.example/x",
    " //evil.example/",
    "/a\\..\\..\\/evil",
    "",
  ]) {
    eq(String(html`<a href="${bad}">x</a>`), '<a href="#">x</a>', `href for ${JSON.stringify(bad)}`);
    assert(!isSafeUrl(bad), `isSafeUrl(${JSON.stringify(bad)})`);
  }
  eq(String(html`<object data="${"https://evil.example/x.svg"}"></object>`), '<object data="#"></object>');
  eq(String(html`<a href="${"/app/transactions?category=A%26B"}">x</a>`), '<a href="/app/transactions?category=A%26B">x</a>');
  eq(String(html`<a data-id="${"https://x"}">x</a>`), '<a data-id="https://x">x</a>', "data-* is not a URL");
});

await check("html_refuses_handler_style_and_srcdoc_attributes", () => {
  let threw = 0;
  for (const make of [
    () => html`<a onclick="${"x"}">`,
    () => html`<a style="${"x"}">`,
    () => html`<iframe srcdoc="${"<script>x</script>"}">`,
  ]) {
    try {
      make();
    } catch {
      threw += 1;
    }
  }
  eq(threw, 3, "throws");
});

await check("setHTML_accepts_only_safe_fragments", () => {
  const element = { innerHTML: "" };
  let threw = false;
  try {
    setHTML(element, "<b>plain string</b>");
  } catch (err) {
    threw = err instanceof TypeError;
  }
  assert(threw, "a plain string is refused with TypeError");
  eq(element.innerHTML, "", "untouched");
  setHTML(element, html`<b>${"<i>"}</b>`);
  eq(element.innerHTML, "<b>&lt;i&gt;</b>");
});

// ── format.js (F106: dates verbatim, instants in local time; exact money) ───
const format = await mod("core/format.js");

await check("F106_dates_and_timestamps_are_never_shifted_by_parsing", () => {
  eq(format.date("2026-10-31"), "2026-10-31", "calendar dates verbatim");
  eq(format.date(""), "—");
  const zurich = { timeZone: "Europe/Zurich" };
  eq(format.timestamp("2026-10-16T18:30:00Z", zurich), "2026-10-16 20:30", "UTC instant → local (CEST)");
  eq(format.timestamp("2026-10-31T23:30:00Z", zurich), "2026-11-01 00:30", "CET after the DST change");
  eq(format.timestamp("2026-10-16T23:59:59+00:00", { timeZone: "UTC" }), "2026-10-16 23:59");
  eq(format.timestamp("2026-10-16T08:15:00+02:00", zurich), "2026-10-16 08:15", "explicit offset");
  eq(format.timestamp("2026-10-16T08:15:00", zurich), "2026-10-16 08:15", "naive stays as written");
  eq(format.timestamp("2026-02-31T10:00:00Z", zurich), "2026-02-31T10:00:00Z", "impossible day: verbatim");
  eq(format.timestamp("not a date"), "not a date");
  eq(format.instantOf("2026-10-16T18:30:00Z"), Date.UTC(2026, 9, 16, 18, 30));
  eq(format.instantOf("2026-10-16T18:30:00"), null, "naive: no instant");
  eq(String(format.dateRange("2026-09-05", "2026-10-17")),
    '<span class="whitespace-nowrap">2026-09-05</span> → <span class="whitespace-nowrap">2026-10-17</span>');
});

await check("money_from_milliunits_is_exact", () => {
  const sp = (text) => text.replace(/ /g, " "); // Intl puts a no-break space after "CHF"
  eq(format.milliToDecimal(-1234560), "-1234.560");
  eq(format.milliToDecimal(5), "0.005");
  eq(format.milliToDecimal(0), "0.000");
  eq(format.amount(-1234560, "CHF"), "-1,234.56");
  eq(format.amount(1005, "CHF"), "1.01", "decimal half-up from the exact string");
  eq(sp(format.money(7227770, "CHF")), "CHF 7,227.77");
  eq(sp(format.money(12350, "CHF", { signed: true })), "+CHF 12.35");
  eq(format.money(1500, "JPY"), "¥2", "the currency's own decimals");
  eq(format.money(null, "CHF"), "—");
  eq(format.money(1000, "not-a-code"), "1.00");
});

// ── chunk.js (F104) ──────────────────────────────────────────────────────────
const { chunk, DEDUPE_CHUNK_SIZE } = await mod("core/chunk.js");

await check("F104_select_all_over_200_is_sent_in_chunks_of_200", () => {
  eq(DEDUPE_CHUNK_SIZE, 200);
  const ids = Array.from({ length: 450 }, (_, i) => `id-${i}`);
  const parts = chunk(ids);
  eq(parts.map((p) => p.length), [200, 200, 50]);
  eq(parts.flat(), ids, "order kept, nothing lost");
  eq(chunk([]), []);
});

// ── chart.js (F105: per-row scale, fixed slots) ──────────────────────────────
const { vbars } = await mod("core/chart.js");

function bars(svg) {
  return [...String(svg).matchAll(/x="([\d.]+)" y="[\d.]+" width="[\d.]+" height="([\d.]+)" rx/g)].map((m) => ({
    x: Number(m[1]),
    h: Number(m[2]),
  }));
}

await check("F105_sparkline_is_scaled_per_row", () => {
  const svg = String(vbars([12900, 12900, 15900], { height: 40 }));
  const heights = bars(svg).map((b) => b.h);
  eq(heights.length, 3, "bars");
  eq(heights[2], 40, "the row's own maximum fills the bar");
  assert(Math.abs(heights[0] - (40 * 12900) / 15900) < 0.01, `12.90 vs 15.90 is visible: ${heights}`);
  assert(!svg.includes("style="), "no inline style");
});

await check("sparkline_rows_share_a_fixed_number_of_slots", () => {
  const yearly = String(vbars([9900, 9900], { slots: 8 }));
  const monthly = String(vbars(Array(8).fill(1290), { slots: 8 }));
  eq(yearly.match(/viewBox="([^"]+)"/)[1], monthly.match(/viewBox="([^"]+)"/)[1], "same scale");
  eq(bars(yearly).map((b) => b.x), [61, 71], "right-aligned: the newest charge last");
  eq(bars(monthly).length, 8);
});

// ── urlstate.js (F101) ───────────────────────────────────────────────────────
const urlstate = await mod("core/urlstate.js");

await check("F101_filters_round_trip_through_the_address_bar", () => {
  urlstate.writeParams(
    { q: "Migros & Co", state: "all", accounts: ["a", "b"], page: 3, date_from: "" },
    { q: "", state: "all", accounts: [], page: 1, date_from: "" },
  );
  eq(win.location.pathname, "/app/transactions");
  const params = new URLSearchParams(win.location.search);
  eq(params.get("q"), "Migros & Co");
  eq(params.get("state"), null, "defaults are left out");
  eq(urlstate.readList(urlstate.readParams(), "accounts"), ["a", "b"], "read back from the address bar");
  eq(urlstate.readInt(params, "page", 1, 1, 100), 3);
  eq(urlstate.readInt(new URLSearchParams("page=-4"), "page", 1, 1, 100), 1);
  eq(urlstate.readChoice(new URLSearchParams("state=evil"), "state", ["all"], "all"), "all");
  eq(urlstate.readDate(new URLSearchParams("date_from=2026-13-99x"), "date_from"), "");
  eq(urlstate.pageUrl("/app/transactions", { category: "A&B", accounts: [], x: "" }), "/app/transactions?category=A%26B");
});

// ── api.js (F62, F24) ────────────────────────────────────────────────────────
const api = await mod("core/api.js");

await check("csrf_header_is_sent_on_every_request", async () => {
  const seen = [];
  fetchImpl = async (url, init) => {
    seen.push([init.method, init.headers["X-CSRF-Token"], init.credentials]);
    return jsonResponse(200, { ok: true });
  };
  await api.get("/api/dedupe/scan");
  await api.post("/api/sync");
  eq(seen, [
    ["GET", "csrf-123", "same-origin"],
    ["POST", "csrf-123", "same-origin"],
  ]);
});

await check("F62_error_messages_are_normalised", async () => {
  const cases = [
    [jsonResponse(409, { error: "busy", message: "Another operation is running." }), "Another operation is running."],
    [jsonResponse(422, { detail: "plain detail" }), "plain detail"],
    [jsonResponse(422, { detail: [{ msg: "field a" }, { msg: "field b" }] }), "field a; field b"],
    [textResponse(502, "<html>Bad gateway</html>"), "YNAB could not be reached — try again later."],
    [jsonResponse(500, {}), "Something went wrong on the server — try again later."],
  ];
  for (const [response, expected] of cases) {
    fetchImpl = async () => response;
    const err = await rejection(api.get("/api/x"), `HTTP ${response.status}`);
    assert(err instanceof api.ApiError, `ApiError for ${response.status}`);
    eq(err.message, expected, `message for ${response.status}`);
    assert(!err.handled, "not handled");
  }
  fetchImpl = async () => {
    throw new TypeError("Failed to fetch");
  };
  const err = await rejection(api.get("/api/x"), "network failure");
  eq([err.status, err.error], [0, "network"]);
});

await check("F62_rate_limits_say_when_to_retry", async () => {
  fetchImpl = async () => jsonResponse(429, { error: "throttled", message: "Wait.", retry_after: 12.3 });
  const err = await rejection(api.get("/api/reconcile/preview"), "429");
  eq([err.status, err.error, err.retryAfter], [429, "throttled", 12.3]);
  eq(api.describeError(err), "Wait. (try again in 13 s)");
  fetchImpl = async () => jsonResponse(429, { error: "throttled", message: "Wait." }, { "retry-after": "120" });
  eq(api.describeError(await rejection(api.get("/api/x"), "429 header")), "Wait. (try again in 2 min)");
});

await check("F24_session_expiry_goes_to_the_login_page", async () => {
  fetchImpl = async () => jsonResponse(401, { error: "session_expired", message: "Expired." });
  win.navigations.length = 0;
  for (let i = 0; i < 2; i += 1) {
    const err = await rejection(api.get("/api/dashboard"), `401 #${i + 1}`);
    eq([err.status, err.handled], [401, true], "marked handled (no second toast)");
  }
  eq(win.navigations, ["/?expired=1"], "one redirect");
});

await check("F62_stale_csrf_token_is_handled_with_a_reload_prompt", async () => {
  clearToasts();
  win.navigations.length = 0;
  fetchImpl = async () => jsonResponse(403, { error: "csrf_failed", message: "Reload." });
  const err = await rejection(api.post("/api/sync"), "403 csrf");
  eq([err.status, err.error, err.handled], [403, "csrf_failed", true]);
  eq(toasts.children.length, 1, "one reload toast");
  const shown = toasts.children[0];
  eq(shown.getAttribute("role"), "alert");
  const reload = shown.querySelectorAll("button").find((b) => b.textContent === "Reload");
  assert(reload, "a Reload button");
  reload.click();
  eq(win.navigations, ["reload"], "the button reloads the page");

  clearToasts();
  fetchImpl = async () => jsonResponse(403, { error: "cross_site", message: "No." });
  const other = await rejection(api.get("/api/me"), "403 other");
  eq([other.error, other.handled], ["cross_site", false]);
  eq(toasts.children.length, 0, "no reload prompt for other 403s");
});

// ── loader.js (F63) and the inline load error ───────────────────────────────
const { createLoader } = await mod("core/loader.js");
const loadError = await mod("components/load_error.js");

await check("F63_a_slow_stale_response_never_overwrites_a_newer_one", async () => {
  const rendered = [];
  const pending = [];
  const loader = createLoader({
    fetch: (signal) =>
      new Promise((resolve) => {
        pending.push({ resolve, signal }); // a server that ignores the abort
      }),
    render: (data) => rendered.push(data.id),
    followSync: false,
  });
  const first = loader.load();
  const second = loader.load();
  assert(pending[0].signal.aborted, "the older request is aborted");
  pending[1].resolve({ id: "new" });
  await second;
  pending[0].resolve({ id: "old" });
  await first;
  eq(rendered, ["new"]);
});

await check("F63_loader_refreshes_while_the_server_syncs", async () => {
  const rendered = [];
  let calls = 0;
  const realSetTimeout = globalThis.setTimeout;
  globalThis.setTimeout = (fn) => realSetTimeout(fn, 0);
  try {
    const loader = createLoader({
      fetch: async () => {
        calls += 1;
        return { syncing: calls < 3, n: calls };
      },
      render: (data) => rendered.push(data.n),
    });
    await loader.load();
    await new Promise((r) => realSetTimeout(r, 50));
  } finally {
    globalThis.setTimeout = realSetTimeout;
  }
  eq(rendered, [1, 2, 3], "refreshed until syncing is false");
});

await check("failed_load_tells_the_page_whether_its_rows_still_match", async () => {
  clearToasts();
  const failures = [];
  let next = null;
  const loader = createLoader({
    fetch: async () => {
      if (next instanceof Error) throw next;
      return next;
    },
    render: () => {},
    renderError: (err, info) => failures.push([err.status, info.stale, typeof info.retry]),
    followSync: false,
  });
  const boom = () => Object.assign(new api.ApiError({ status: 500, error: "internal", message: "Boom." }));
  next = { ok: 1 };
  await loader.load();
  next = boom();
  await loader.load(); // new filters failed: the rows on screen are for the old filters
  await loader.load({ quiet: true }); // a refresh after that: nothing current is shown
  next = { ok: 2 };
  await loader.load();
  next = boom();
  await loader.load({ quiet: true }); // a background refresh of the same query failed
  await loader.load({ quiet: true }); // …and failed again: still the same (current) rows
  eq(failures, [
    [500, false, "function"],
    [500, false, "function"],
    [500, true, "function"],
    [500, true, "function"],
  ]);
  eq(toasts.children.length, 0, "the inline error replaces the toast");
});

await check("load_error_shows_the_reason_and_retries", async () => {
  setHTML(main, html`<div id="box" hidden></div>`);
  const box = doc.getElementById("box");
  let retried = 0;
  const err = new api.ApiError({ status: 500, error: "internal", message: "Boom <b>." });
  loadError.loadErrorHandler(box, "transactions", () => {
    throw new Error("must not clear rows that still match");
  })(err, { stale: true, retry: () => (retried += 1) });
  assert(!box.hidden, "shown");
  const alert = box.querySelector('[role="alert"]');
  assert(alert && alert.textContent.includes("Could not refresh transactions: Boom <b>."), alert?.textContent);
  assert(alert.textContent.includes("may be out of date"), "says the rows may be outdated");
  let cleared = 0;
  loadError.loadErrorHandler(box, "transactions", () => (cleared += 1))(err, { stale: false, retry: () => (retried += 1) });
  eq(cleared, 1, "rows of other filters are cleared");
  assert(box.textContent.includes("Could not load transactions: Boom <b>."), box.textContent);
  box.querySelector('[data-act="retry"]').click();
  eq(retried, 1, "Retry reloads");
  assert(box.hidden && box.textContent === "", "the message goes away on retry");
});

// ── account picker (F99) ─────────────────────────────────────────────────────
const picker = await mod("components/account_picker.js");
const ACCOUNTS = [
  { id: "a1", name: "Revolut", classification: "cash", closed: false },
  { id: "a2", name: "Visa <Card>", classification: "credit", closed: false },
  { id: "a3", name: "Savings", classification: "cash", closed: true },
];

function mountPicker(options = {}) {
  setHTML(main, html`<div id="picker-root"></div><button type="button" id="outside">Elsewhere</button>`);
  const changes = [];
  const instance = new picker.AccountPicker(doc.getElementById("picker-root"), {
    accounts: ACCOUNTS,
    onChange: (ids) => changes.push(ids),
    ...options,
  });
  return { instance, changes, root: doc.getElementById("picker-root"), outside: doc.getElementById("outside") };
}

await check("F99_account_picker_stays_open_while_picking", async () => {
  const { instance, changes, root, outside } = mountPicker({ selected: ["a1", "gone"] });
  const button = root.querySelector("button[aria-expanded]");
  const panel = doc.getElementById(button.getAttribute("aria-controls"));
  eq(instance.value(), ["a1"], "unknown ids are dropped");
  assert(panel.hidden, "starts closed");
  button.click();
  assert(!panel.hidden && button.getAttribute("aria-expanded") === "true", "opens");
  const boxes = panel.querySelectorAll("input[type=checkbox]");
  eq(boxes.map((b) => b.checked), [true, false, false]);
  const markupBefore = root.innerHTML.length;
  boxes[1].click(); // tick Visa — the click bubbles to the document like in a browser
  boxes[2].click(); // tick Savings
  panel.querySelector("label").dispatchEvent(new FakeEvent("click"));
  assert(!panel.hidden, "clicks inside the panel never close it");
  eq(changes, [], "debounced: nothing applied while picking");
  assert(root.textContent.includes("3 accounts"), "the button label updates in place");
  assert(boxes.every((b) => b.isConnected), "not re-rendered (focus stays put)");
  assert(Math.abs(root.innerHTML.length - markupBefore) < 40, "updated in place");
  outside.click();
  assert(panel.hidden && button.getAttribute("aria-expanded") === "false", "a click outside closes it");
  eq(changes, [["a1", "a2", "a3"]], "one change, applied at once on close");

  button.click();
  boxes[0].click();
  boxes[0].dispatchEvent(new FakeEvent("keydown", { key: "Escape" }));
  assert(panel.hidden, "Escape closes it");
  assert(doc.activeElement === button, "focus returns to the button");
  eq(changes.length, 2, "and applies the pending change");
  eq(changes[1], ["a2", "a3"]);

  button.click();
  boxes[1].focus();
  outside.focus();
  assert(panel.hidden, "focus leaving the picker closes it");
  eq(changes.length, 2, "closing without a change applies nothing");
  assert(!root.innerHTML.includes("<Card>") && root.textContent.includes("Visa <Card>"), "names are escaped");
});

await check("account_picker_panel_stays_inside_the_viewport", async () => {
  const side = picker.panelSide;
  const rect = (left, width) => ({ left, right: left + width });
  eq(side(rect(16, 224), 288, 375, "left"), "left", "start of a toolbar on a phone");
  eq(side(rect(135, 224), 288, 375, "right"), "right", "end of a right-aligned row");
  eq(side(rect(16, 224), 288, 375, "right"), "left", "flips when the left edge would be cut off");
  eq(side(rect(790, 224), 288, 1024, "left"), "right", "flips when the right edge would be cut off");
  eq(side(rect(40, 224), 343, 375, "right"), "left", "neither fits: the side with more room");

  doc.documentElement.clientWidth = 375;
  for (const [align, left, expected] of [
    ["left", 16, "left-0"], // Spending / Subscriptions toolbar on a phone
    ["right", 135, "right-0"], // Transactions (justify-end)
    ["right", 16, "left-0"], // would stick out to the left: flipped
  ]) {
    const { root } = mountPicker({ align });
    const button = root.querySelector("button[aria-expanded]");
    const panel = doc.getElementById(button.getAttribute("aria-controls"));
    button.rect = { left, top: 100, width: 224, height: 38 };
    panel.rect = { left: 0, top: 140, width: 288, height: 200 };
    button.click();
    assert(panel.classList.contains(expected), `${align} at ${left}: ${panel.className}`);
    const other = expected === "left-0" ? "right-0" : "left-0";
    assert(!panel.classList.contains(other), `only one anchor: ${panel.className}`);
  }
  delete doc.documentElement.clientWidth;
});

// ── compact navigation (F102) ────────────────────────────────────────────────
const { initNav } = await mod("components/nav.js");

await check("nav_menu_is_keyboard_friendly", async () => {
  setHTML(
    main,
    html`<button type="button" id="before">Logout</button>
      <button type="button" id="nav-toggle" aria-expanded="false" aria-controls="site-nav">Menu</button>
      <nav id="site-nav"><ul><li><a href="/app/dashboard" id="first">Dashboard</a></li>
      <li><a href="/app/accounts" id="second">Accounts</a></li></ul></nav>
      <button type="button" id="after">Elsewhere</button>`,
  );
  initNav();
  const toggle = doc.getElementById("nav-toggle");
  const nav = doc.getElementById("site-nav");
  const open = () => nav.dataset.open === "true";
  toggle.focus();
  toggle.click();
  assert(open() && toggle.getAttribute("aria-expanded") === "true", "opens");
  doc.getElementById("first").focus();
  doc.getElementById("second").focus();
  assert(open(), "moving through the links keeps it open");
  doc.getElementById("after").focus();
  assert(!open() && toggle.getAttribute("aria-expanded") === "false", "focus leaving closes it");
  toggle.click();
  doc.getElementById("first").focus();
  doc.getElementById("first").dispatchEvent(new FakeEvent("keydown", { key: "Escape" }));
  assert(!open(), "Escape closes it");
  assert(doc.activeElement === toggle, "and returns focus to the toggle");
  toggle.click();
  doc.getElementById("before").focus();
  assert(!open(), "Shift+Tab out of the toggle closes it");
});

// ── upload.js (F103) ─────────────────────────────────────────────────────────
const upload = await mod("pages/upload.js");

await check("F103_upload_reports_changes_not_parsed_rows", () => {
  const changed = { ok: true, has_changes: true, rows_total: 342, created: [{}, {}, {}], updated: [{}], removed: [], linked: [] };
  eq(upload.changeSummary(changed), "3 created, 1 updated");
  eq(upload.changeSummary({ rows_total: 342, created: [], updated: [], removed: [], linked: [] }), "");
  const done = upload.uploadOutcome("s.csv", changed);
  eq([done.kind, done.toast, done.status], ["ok", "Imported s.csv: 3 created, 1 updated.", "Imported s.csv: 3 created, 1 updated."]);
  assert(!done.toast.includes("342"), "parsed rows are not reported as imported");
  // 40 rows, 2 reverted and 1 kept deleted in YNAB: YNAB does NOT have "all 40 rows".
  const none = upload.uploadOutcome("s.csv", {
    ok: true, has_changes: false, rows_total: 40, rows_reverted: 2, ignored_deleted: 1,
    created: [], updated: [], removed: [], linked: [],
  });
  eq(none.kind, "info");
  eq(none.toast, "s.csv: no changes — YNAB already matches this statement (40 rows checked).");
  assert(!/already had all/.test(none.toast), "no overclaim");
  const broken = upload.uploadOutcome("s.csv", { ok: false, has_changes: true, created: [{}], failures: [{}] });
  eq(broken.kind, "warn");
  assert(broken.toast.includes("problems"), broken.toast);
});

// ── reconcile.js: the confirmation repeats the preview's warnings ───────────
const reconcile = await mod("pages/reconcile.js");

await check("reconcile_confirmation_repeats_the_warnings", () => {
  const preview = { delta: 12350, currency: "CHF", as_of: "2026-10-16", problems: [] };
  const clean = reconcile.confirmContent(preview, "Revolut");
  eq([clean.confirmLabel, clean.danger], ["Post adjustment", false]);
  assert(String(clean.body).includes("matches the statement"), String(clean.body));
  const warning = "2 uncleared transaction(s) dated on or before 2026-10-16 (<b>-64.40</b>) are not in YNAB's cleared balance";
  const risky = reconcile.confirmContent({ ...preview, problems: [warning] }, "Revolut <main>");
  eq([risky.confirmLabel, risky.danger], ["Post anyway", true]);
  const body = String(risky.body);
  assert(body.includes("2 uncleared transaction(s) dated on or before 2026-10-16 (&lt;b&gt;-64.40&lt;/b&gt;)"), body);
  assert(body.includes("Revolut &lt;main&gt;"), "account name escaped");
  assert(!body.includes("matches the statement"), "no all-clear next to a warning");
});

// ── badges ───────────────────────────────────────────────────────────────────
const badges = await mod("components/badges.js");

await check("cleared_states_read_the_same_everywhere", () => {
  assert(String(badges.clearedBadge("uncleared")).includes(">pending<"), "uncleared → pending");
  assert(String(badges.clearedBadge("cleared")).includes(">cleared<"));
  assert(String(badges.clearedBadge("reconciled")).includes("reconciled<"));
});

process.stdout.write(`${JSON.stringify(results)}\n`, () => process.exit(0));
