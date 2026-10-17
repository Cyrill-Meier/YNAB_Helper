// Page-level behaviour: the real page modules run against the real rendered templates (a
// fake DOM, a fake fetch). Run by test_js_pages.py when Node.js is available.
// Usage: node node_page_checks.mjs <ynab_helper/web/static/js> <dir with <page>.html files>
// Prints one JSON object {check name: "ok" | failure message}.

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { FakeDocument, FakeEvent, install, settle } from "./fake_dom.mjs";

const [jsDir, pagesDir] = process.argv.slice(2);
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
async function check(name, fn) {
  try {
    await fn();
    results[name] = "ok";
  } catch (err) {
    results[name] = String(err && err.stack ? err.stack : err);
  } finally {
    // A check that failed half-way must not leave the (module-wide) dialog open for the next.
    const dialog = globalThis.document && globalThis.document.getElementById("confirm-dialog");
    if (dialog && dialog.open) dialog.close("cancel");
    await settle();
  }
}

function json(status, body) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (name) => (name.toLowerCase() === "content-type" ? "application/json" : null) },
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}

/** Load templates/<page>.html into a fresh fake document and boot pages/<page>.js. */
async function openPage(page, { search = "", routes, width = 375 }) {
  const doc = new FakeDocument(readFileSync(join(pagesDir, `${page}.html`), "utf8"));
  const win = install(doc, { path: `/app/${page}`, search, innerWidth: width });
  doc.documentElement.clientWidth = width;
  const calls = [];
  globalThis.fetch = async (url, init = {}) => {
    const parsed = new URL(url, "http://testserver");
    const body = typeof init.body === "string" ? JSON.parse(init.body) : null;
    calls.push({ method: init.method || "GET", path: parsed.pathname, params: parsed.searchParams, body });
    const handler = routes[`${init.method || "GET"} ${parsed.pathname}`];
    if (!handler) return json(404, { error: "not_found", message: `no fake for ${parsed.pathname}` });
    return handler({ params: parsed.searchParams, body });
  };
  const module = await mod(`pages/${page}.js`);
  await module.init();
  await settle();
  return { doc, win, calls, byId: (id) => doc.getElementById(id) };
}

const META = { currency: "CHF", last_sync_at: "2026-10-17T09:17:00Z", syncing: false };
const ACCOUNTS = {
  ...META,
  items: [
    { id: "a1", name: "Revolut", classification: "cash", closed: false, deleted: false },
    { id: "a2", name: "Visa", classification: "credit", closed: false, deleted: false },
  ],
};

function pickerPanel(doc, rootId) {
  const button = doc.getElementById(rootId).querySelector("button[aria-expanded]");
  return { button, panel: doc.getElementById(button.getAttribute("aria-controls")) };
}

// ── Transactions: a failed reload never leaves the old filter's rows (review) ─
await check("failed_filter_change_clears_the_rows_and_offers_retry", async () => {
  const TX = [
    { id: "t1", date: "2026-10-15", payee_name: "Migros", amount: -23450, cleared: "uncleared", approved: true },
    { id: "t2", date: "2026-10-01", payee_name: "Rent", amount: -1800000, cleared: "reconciled", approved: true },
  ];
  let failing = false;
  const page = await openPage("transactions", {
    routes: {
      "GET /api/transactions": ({ params }) => {
        if (failing) return json(500, { error: "internal", message: "The server broke." });
        const state = params.get("state");
        const items = TX.filter((t) => !state || state === "all" || t.cleared === state);
        return json(200, { ...META, items, total: items.length, page: 1, page_size: 50 });
      },
      "GET /api/categories": () => json(200, { ...META, items: [], uncategorized: 0 }),
      "GET /api/accounts": () => json(200, ACCOUNTS),
    },
  });
  const { byId, doc, win } = page;
  const rows = () => byId("tx-tbody").querySelectorAll("tr");
  eq(rows().length, 2, "first load");
  assert(byId("tx-error").hidden, "no error at first");

  failing = true;
  byId("tx-state").value = "reconciled";
  byId("tx-state").dispatchEvent(new FakeEvent("change"));
  await settle();
  eq(win.location.search, "?state=reconciled", "the filter is in the address bar");
  eq(rows().length, 0, "the rows of the previous filter are gone");
  assert(!byId("tx-error").hidden, "an inline error is shown");
  const alert = byId("tx-error").querySelector('[role="alert"]');
  assert(alert.textContent.includes("Could not load transactions: The server broke."), alert.textContent);
  eq(byId("tx-summary").textContent, "", "no '1–2 of 2' for rows that are not shown");
  eq(byId("tx-sync-status").textContent, "", "no 'synced …' line for data that is not shown");
  assert(byId("tx-prev").disabled && byId("tx-next").disabled, "paging disabled");
  assert(byId("tx-loading").hidden, "spinner stopped");
  eq(doc.getElementById("toasts").children.length, 0, "inline error instead of a toast");

  failing = false;
  byId("tx-error").querySelector('[data-act="retry"]').click();
  await settle();
  eq(rows().length, 1, "Retry loads the current filter");
  assert(rows()[0].textContent.includes("Rent") && rows()[0].textContent.includes("reconciled"), rows()[0].textContent);
  assert(byId("tx-error").hidden, "the error is gone");
  eq(byId("tx-summary").textContent, "1–1 of 1");
});

// ── the account picker opens inside the viewport on every page (review) ─────
await check("account_picker_anchors_match_each_toolbar", async () => {
  const SPENDING = {
    ...META,
    months: ["2026-08", "2026-09"],
    per_month_total: [100000, 120000],
    month_to_date: { total: 5000, month: "2026-10", from: "2026-10-01", through: "2026-10-17" },
    categories: [],
    average: 110000,
    today: "2026-10-17",
  };
  const SUBS = { ...META, items: [], since: "2025-09-17", monthly_total: 0, yearly_detectable: true, lookback_months: 13 };
  const TXS = { ...META, items: [], total: 0, page: 1, page_size: 50 };
  const cases = [
    ["spending", "sp-accounts", { "GET /api/spending": () => json(200, SPENDING) }, "left-0"],
    ["subscriptions", "sub-accounts", { "GET /api/subscriptions": () => json(200, SUBS) }, "left-0"],
    [
      "transactions",
      "tx-accounts",
      {
        "GET /api/transactions": () => json(200, TXS),
        "GET /api/categories": () => json(200, { ...META, items: [], uncategorized: 0 }),
      },
      "right-0",
    ],
  ];
  for (const [name, rootId, routes, anchor] of cases) {
    const { doc } = await openPage(name, { routes: { ...routes, "GET /api/accounts": () => json(200, ACCOUNTS) } });
    const { button, panel } = pickerPanel(doc, rootId);
    assert(panel.classList.contains(anchor), `${name}: rendered with ${anchor}: ${panel.className}`);
    // A phone: the picker starts at the 16 px gutter (Spending/Subscriptions) or ends at it.
    button.rect = anchor === "left-0" ? { left: 16, top: 0, width: 224, height: 38 } : { left: 135, top: 0, width: 224, height: 38 };
    panel.rect = { left: 0, top: 40, width: 288, height: 200 };
    button.click();
    assert(!panel.hidden && panel.classList.contains(anchor), `${name}: opens with ${anchor}: ${panel.className}`);
  }
});

// ── Dedupe: select all → confirm → chunks of 200 (F104) ─────────────────────
async function dedupePage({ candidates, onDelete }) {
  return openPage("dedupe", {
    routes: {
      "GET /api/dedupe/scan": () =>
        json(200, {
          scan_id: "scan-1",
          ttl_seconds: 600,
          csv_name: "account-statement_2026-09-05_2026-10-17.csv",
          window: ["2026-09-05", "2026-10-17"],
          statement_rows: 40,
          ynab_count_in_window: 490,
          candidates,
        }),
      "GET /api/dashboard": () => json(200, { currency: "CHF" }),
      "POST /api/dedupe/delete": ({ body }) => onDelete(body),
    },
  });
}

function makeCandidates(n) {
  return Array.from({ length: n }, (_, i) => ({
    id: `tx-${i}`,
    date: "2026-10-01",
    payee_name: `Payee ${i}`,
    category_name: null,
    amount: -1000 - i,
    memo: "",
    cleared: "uncleared",
  }));
}

async function scan(page) {
  page.byId("dd-scan").click();
  await settle();
}

async function selectAllAndDelete(page) {
  const { byId, doc } = page;
  const all = byId("dd-select-all");
  all.click();
  await settle();
  assert(!byId("dd-delete").disabled, "delete enabled once something is selected");
  byId("dd-delete").click();
  await settle();
  const dialog = byId("confirm-dialog");
  assert(dialog.open, "a confirmation dialog first");
  assert(doc.activeElement === byId("confirm-cancel"), "a destructive dialog starts on Cancel");
  return dialog;
}

await check("F104_dedupe_select_all_deletes_in_confirmed_chunks_of_200", async () => {
  const candidates = makeCandidates(450);
  const byIdMap = new Map(candidates.map((c) => [c.id, c]));
  const page = await dedupePage({
    candidates,
    onDelete: (body) => json(200, { scan_id: body.scan_id, deleted: body.ids.map((id) => byIdMap.get(id)), already_deleted: [], skipped: [], failures: [], not_attempted: [], rate_limited: false, ok: true }),
  });
  const { byId, calls } = page;
  await scan(page);
  // The scan is shown with dates kept on one line, the full name on hover, YNAB states as words.
  eq(byId("dd-csv").title, "account-statement_2026-09-05_2026-10-17.csv");
  eq(byId("dd-range").querySelectorAll("span.whitespace-nowrap").map((s) => s.textContent), ["2026-09-05", "2026-10-17"]);
  const firstRow = byId("dd-tbody").querySelector("tr");
  assert(firstRow.textContent.includes("pending") && !firstRow.textContent.includes("uncleared"), firstRow.textContent);

  const dialog = await selectAllAndDelete(page);
  assert(byId("confirm-body").textContent.includes("batches of 200"), byId("confirm-body").textContent);
  eq(calls.filter((c) => c.method === "POST").length, 0, "nothing is deleted before the confirmation");
  byId("confirm-ok").click();
  await settle(60);
  assert(!dialog.open, "dialog closed");
  const posts = calls.filter((c) => c.method === "POST" && c.path === "/api/dedupe/delete");
  eq(posts.map((c) => c.body.ids.length), [200, 200, 50], "chunks of the server's cap");
  assert(posts.every((c) => c.body.scan_id === "scan-1"), "every chunk names the scan");
  eq(posts.flatMap((c) => c.body.ids), candidates.map((c) => c.id), "every selected id once, in order");
  eq(byId("dd-tbody").querySelectorAll("tr").length, 0, "deleted rows are gone");
  assert(byId("dd-report").textContent.includes("Deleted 450 transactions."), byId("dd-report").textContent);
});

await check("F104_dedupe_stops_at_a_stale_scan_and_says_what_stayed", async () => {
  const candidates = makeCandidates(450);
  const byIdMap = new Map(candidates.map((c) => [c.id, c]));
  let n = 0;
  const page = await dedupePage({
    candidates,
    onDelete: (body) => {
      n += 1;
      if (n === 2) return json(409, { error: "stale", message: "The scan expired." });
      return json(200, { scan_id: body.scan_id, deleted: body.ids.map((id) => byIdMap.get(id)), already_deleted: [], skipped: [], failures: [], not_attempted: [], rate_limited: false, ok: true });
    },
  });
  await scan(page);
  await selectAllAndDelete(page);
  page.byId("confirm-ok").click();
  await settle(60);
  const posts = page.calls.filter((c) => c.method === "POST");
  eq(posts.length, 2, "no further chunk after the stale answer");
  eq(page.byId("dd-tbody").querySelectorAll("tr").length, 250, "the 250 not deleted stay listed");
  const report = page.byId("dd-report").textContent;
  assert(report.includes("Deleted 200 transactions."), report);
  assert(report.includes("The scan expired.") && report.includes("Press Scan again") && report.includes("250 transactions stayed."), report);
});

// ── Reconcile: the dialog repeats the preview's warnings (review) ───────────
await check("reconcile_dialog_repeats_the_warnings_before_posting", async () => {
  const problem = "2 uncleared transaction(s) dated on or before 2026-10-16 (-64.40 CHF) are not in YNAB's cleared balance.";
  const page = await openPage("reconcile", {
    routes: {
      "GET /api/dashboard": () => json(200, { currency: "CHF", account: { name: "Revolut" }, last_statement: { name: "stmt.csv" } }),
      "GET /api/reconcile/preview": () =>
        json(200, {
          in_sync: false,
          token: "tok-1",
          token_ttl_seconds: 600,
          delta: 12350,
          currency: "CHF",
          as_of: "2026-10-16",
          csv_name: "stmt.csv",
          csv_balance: 100000,
          ynab_cleared_as_of: 87650,
          ynab_cleared_now: 90000,
          cleared_after_as_of: 2350,
          problems: [problem],
        }),
      "POST /api/reconcile": () =>
        json(200, { adjustment: { outcome: "created", delta_milli: 12350, date: "2026-10-16", payee: "CSV Reconciliation", cleared: "reconciled" } }),
    },
  });
  const { byId, calls, doc } = page;
  eq(byId("rec-csv").title, "stmt.csv");
  byId("rec-preview").click();
  await settle();
  const labels = byId("rec-preview-result").querySelectorAll("dt").map((dt) => dt.textContent.trim());
  assert(labels[0] === "Statement balance on 2026-10-16", labels[0]);
  assert(byId("rec-preview-result").querySelector("dt").querySelector("span.whitespace-nowrap"), "the date stays on one line");
  doc.getElementById("rec-apply").click();
  await settle();
  const dialog = byId("confirm-dialog");
  assert(dialog.open, "dialog open");
  eq(byId("confirm-title").textContent, "Post the adjustment despite the warnings?");
  eq(byId("confirm-ok").textContent, "Post anyway");
  assert(byId("confirm-body").textContent.includes(problem), byId("confirm-body").textContent);
  assert(!byId("confirm-body").textContent.includes("matches the statement"), "no all-clear");
  assert(doc.activeElement === byId("confirm-cancel"), "focus starts on Cancel");
  byId("confirm-cancel").click();
  await settle();
  eq(calls.filter((c) => c.method === "POST").length, 0, "cancel posts nothing");

  doc.getElementById("rec-apply").click();
  await settle();
  byId("confirm-ok").click();
  await settle();
  const posts = calls.filter((c) => c.method === "POST");
  eq(posts.map((c) => [c.path, c.body]), [["/api/reconcile", { token: "tok-1" }]]);
  assert(byId("rec-outcome-body").textContent.includes("Posted an adjustment"), byId("rec-outcome-body").textContent);
});

process.stdout.write(`${JSON.stringify(results)}\n`, () => process.exit(0));
