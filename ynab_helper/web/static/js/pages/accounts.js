// Accounts: every account of the budget from the YNAB mirror, grouped like v1 (cash,
// credit, tracking, closed/deleted). Each row links to its transactions.

import { get } from "../core/api.js";
import { byId, setText, show } from "../core/dom.js";
import { amount, date, money, nowrap, plural } from "../core/format.js";
import { html, setHTML } from "../core/html.js";
import { createLoader } from "../core/loader.js";
import { pageUrl } from "../core/urlstate.js";
import { hideLoadError, loadErrorHandler } from "../components/load_error.js";
import { bindSyncButton, showSyncStatus } from "../components/sync.js";

const GROUPS = ["cash", "credit", "tracking", "closed"];

function statusBadge(a) {
  if (a.deleted) return html`<span class="badge badge-neutral">deleted</span>`;
  if (a.closed) return html`<span class="badge badge-warn">closed</span>`;
  return html`<span class="badge badge-ok">active</span>`;
}

function row(a, currency) {
  const link = pageUrl("/app/transactions", { accounts: a.id });
  const lastLine = a.last_activity
    ? html`last transaction ${nowrap(date(a.last_activity))}`
    : "no transactions yet";
  return html`<li>
    <a href="${link}" class="block px-5 py-3 hover:bg-slate-50 dark:hover:bg-ink-700/40">
      <div class="flex flex-wrap items-center justify-between gap-x-4 gap-y-1">
        <div class="flex min-w-0 flex-wrap items-center gap-2">
          <span class="truncate font-medium">${a.name}</span>
          ${a.is_primary ? html`<span class="badge badge-brand">primary</span>` : ""}
          ${statusBadge(a)}
          ${a.on_budget ? "" : html`<span class="badge badge-neutral">off budget</span>`}
          ${a.type ? html`<span class="text-xs text-ink-500 dark:text-slate-400">${a.type}</span>` : ""}
        </div>
        <div class="text-sm font-medium tabular-nums ${a.balance < 0 ? "amount-out" : ""}">
          ${money(a.balance, currency)}
        </div>
      </div>
      <div class="mt-1 flex flex-wrap items-center justify-between gap-x-4 text-xs text-ink-500 dark:text-slate-400">
        <span>${plural(a.tx_count || 0, "transaction")} · ${lastLine}</span>
        <span>cleared ${amount(a.cleared_balance, currency)}${a.uncleared_balance ? ` · pending ${amount(a.uncleared_balance, currency)}` : ""}</span>
      </div>
    </a>
  </li>`;
}

function render(d) {
  hideLoadError(byId("acc-error"));
  showSyncStatus(byId("acc-sync-status"), d);
  const items = d.items || [];
  const currency = d.currency;
  show(byId("acc-empty"), items.length === 0);

  const buckets = { cash: [], credit: [], tracking: [], closed: [] };
  for (const a of items) {
    if (a.deleted || a.closed) buckets.closed.push(a);
    else (buckets[a.classification] || buckets.tracking).push(a);
  }
  for (const group of ["cash", "credit", "tracking"]) {
    const total = buckets[group].reduce((sum, a) => sum + (a.balance || 0), 0);
    setText(byId(`acc-${group}-total`), items.length ? money(total, currency) : "—");
    setText(byId(`acc-${group}-meta`), plural(buckets[group].length, "account"));
  }
  for (const group of GROUPS) {
    const accounts = buckets[group];
    show(byId(`acc-${group}-section`), accounts.length > 0);
    setHTML(byId(`acc-${group}-rows`), html`${accounts.map((a) => row(a, currency))}`);
  }
}

/** Empty every figure and list (shown again by the next successful load). */
function clear() {
  setText(byId("acc-sync-status"), ""); // no refresh follows a failure
  for (const group of ["cash", "credit", "tracking"]) {
    setText(byId(`acc-${group}-total`), "—");
    setText(byId(`acc-${group}-meta`), "");
  }
  for (const group of GROUPS) {
    show(byId(`acc-${group}-section`), false);
    setHTML(byId(`acc-${group}-rows`), html``);
  }
  show(byId("acc-empty"), false);
}

export async function init() {
  const loader = createLoader({
    fetch: (signal) => get("/api/accounts", { signal }),
    render,
    renderError: loadErrorHandler(byId("acc-error"), "the accounts", clear),
    onLoading: (busy) => show(byId("acc-loading"), busy),
  });
  bindSyncButton(byId("acc-sync"), () => loader.load({ quiet: true }));
  await loader.load();
}
