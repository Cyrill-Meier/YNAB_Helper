// Transactions: the budget's transactions from the YNAB mirror with filters, sorting and
// pagination. Every filter lives in the address bar (F101, replaceState) and is restored
// on load; list requests are sequenced so a slow old response never wins (F63). When a
// request for new filters fails, the rows of the previous filters are cleared and an inline
// error with Retry is shown — rows never sit under filters they do not match.

import { get, reportError } from "../core/api.js";
import { byId, debounce, setText, show } from "../core/dom.js";
import { amount, amountClass, count, date } from "../core/format.js";
import { html, setHTML } from "../core/html.js";
import { createLoader } from "../core/loader.js";
import {
  pageUrl,
  readChoice,
  readDate,
  readInt,
  readList,
  readParams,
  writeParams,
} from "../core/urlstate.js";
import { AccountPicker, pickerAccounts } from "../components/account_picker.js";
import { clearedBadge } from "../components/badges.js";
import { hideLoadError, loadErrorHandler } from "../components/load_error.js";
import { showSyncStatus } from "../components/sync.js";

const PAGE_SIZE = 50;
const STATES = ["all", "cleared", "reconciled", "uncleared"];
const SORTS = ["-date", "date", "-amount", "amount", "payee_name", "-payee_name"];
const UNCATEGORIZED = "__none__";
const DEFAULTS = {
  q: "",
  state: "all",
  category: "",
  accounts: [],
  date_from: "",
  date_to: "",
  sort: "-date",
  page: 1,
};

function readState() {
  const params = readParams();
  return {
    q: (params.get("q") || "").slice(0, 200),
    state: readChoice(params, "state", STATES, "all"),
    category: (params.get("category") || "").slice(0, 200),
    accounts: readList(params, "accounts").slice(0, 100),
    date_from: readDate(params, "date_from"),
    date_to: readDate(params, "date_to"),
    sort: readChoice(params, "sort", SORTS, "-date"),
    page: readInt(params, "page", 1, 1, 100000),
  };
}

function categoryCell(t, currency) {
  const subs = t.subtransactions || [];
  if (subs.length) {
    return html`<span class="badge badge-neutral">Split</span>
      <ul class="mt-1 space-y-0.5 text-xs text-ink-500 dark:text-slate-400">
        ${subs.map(
          (s) => html`<li class="flex justify-between gap-3">
            <span class="truncate">${s.category_name || (s.transfer_account_id ? "Transfer" : "Uncategorized")}</span>
            <span class="tabular-nums">${amount(s.amount, currency)}</span>
          </li>`,
        )}
      </ul>`;
  }
  if (t.category_name) return html`<span class="badge badge-neutral">${t.category_name}</span>`;
  if (t.transfer_account_id) return html`<span class="text-xs text-ink-500 dark:text-slate-400">Transfer</span>`;
  return html`<span class="text-xs italic text-ink-500 dark:text-slate-400">—<span class="sr-only">uncategorized</span></span>`;
}

function rowHtml(t, currency) {
  return html`<tr>
    <td class="whitespace-nowrap px-4 py-2 text-ink-500 dark:text-slate-400">${date(t.date)}</td>
    <td class="px-4 py-2">
      <div>${t.payee_name || "—"}${t.approved ? "" : html` <span class="badge badge-warn" title="Not approved in YNAB yet">unapproved</span>`}</div>
      ${t.account_name ? html`<div class="text-xs text-ink-500 dark:text-slate-400">${t.account_name}</div>` : ""}
    </td>
    <td class="whitespace-nowrap px-4 py-2 text-right tabular-nums ${amountClass(t.amount)}">${amount(t.amount, currency)}</td>
    <td class="px-4 py-2">${categoryCell(t, currency)}</td>
    <td class="max-w-[24ch] truncate px-4 py-2 text-ink-500 dark:text-slate-400" title="${t.memo || ""}">${t.memo || ""}</td>
    <td class="px-4 py-2">${clearedBadge(t.cleared)}</td>
  </tr>`;
}

export async function init() {
  const state = readState();
  const search = byId("tx-search");
  const stateSelect = byId("tx-state");
  const categorySelect = byId("tx-category");
  const sortSelect = byId("tx-sort");
  const fromInput = byId("tx-from");
  const toInput = byId("tx-to");
  const prev = byId("tx-prev");
  const next = byId("tx-next");
  const errorBox = byId("tx-error");
  let total = 0;
  let currency = null;

  search.value = state.q;
  stateSelect.value = state.state;
  sortSelect.value = state.sort;
  fromInput.value = state.date_from;
  toInput.value = state.date_to;

  function query() {
    return {
      q: state.q,
      state: state.state,
      category: state.category,
      accounts: state.accounts,
      date_from: state.date_from,
      date_to: state.date_to,
      sort: state.sort,
      page: state.page,
      page_size: PAGE_SIZE,
    };
  }

  const list = createLoader({
    fetch: (signal) => get(pageUrl("/api/transactions", query()), { signal }),
    onLoading: (busy) => {
      show(byId("tx-loading"), busy);
      byId("tx-tbody").closest("table").setAttribute("aria-busy", busy ? "true" : "false");
    },
    renderError: loadErrorHandler(errorBox, "transactions", () => {
      setText(byId("tx-sync-status"), ""); // no refresh follows a failure
      setHTML(byId("tx-tbody"), html``);
      show(byId("tx-empty"), false);
      setText(byId("tx-summary"), "");
      total = 0;
      prev.disabled = true;
      next.disabled = true;
    }),
    render(d) {
      hideLoadError(errorBox);
      currency = d.currency;
      total = d.total;
      showSyncStatus(byId("tx-sync-status"), d);
      const lastPage = Math.max(1, Math.ceil(d.total / d.page_size));
      if (!d.items.length && d.total > 0 && state.page > lastPage) {
        state.page = lastPage; // e.g. ?page=99 from an old bookmark
        writeParams(state, DEFAULTS);
        list.load();
        return;
      }
      setHTML(byId("tx-tbody"), html`${d.items.map((t) => rowHtml(t, currency))}`);
      show(byId("tx-empty"), d.items.length === 0);
      const start = (d.page - 1) * d.page_size + 1;
      const end = Math.min(d.total, d.page * d.page_size);
      setText(
        byId("tx-summary"),
        d.total ? `${count(start)}–${count(end)} of ${count(d.total)}` : "No transactions",
      );
      prev.disabled = d.page <= 1;
      next.disabled = d.page * d.page_size >= d.total;
    },
  });

  function fillCategories(report) {
    const keep = state.category;
    const options = [
      html`<option value="">All categories</option>`,
      html`<option value="${UNCATEGORIZED}">— uncategorized — (${count(report?.uncategorized ?? 0)})</option>`,
    ];
    const names = new Set();
    for (const c of report?.items || []) {
      names.add(c.name);
      options.push(html`<option value="${c.name}">${c.name} (${count(c.count)})</option>`);
    }
    if (keep && keep !== UNCATEGORIZED && !names.has(keep)) {
      // A category from the URL that has no transactions here: keep it selectable.
      options.push(html`<option value="${keep}">${keep} (0)</option>`);
    }
    setHTML(categorySelect, html`${options}`);
    categorySelect.value = keep;
  }

  const categories = createLoader({
    fetch: (signal) => get(pageUrl("/api/categories", { accounts: state.accounts }), { signal }),
    render: fillCategories,
    followSync: false,
    onError: (err) => {
      fillCategories(null);
      reportError(err, "Categories could not be loaded");
    },
  });
  fillCategories(null);

  function changed({ resetPage = true } = {}) {
    if (resetPage) state.page = 1;
    writeParams(state, DEFAULTS);
    list.load();
  }

  const onSearch = debounce(() => {
    state.q = search.value.trim();
    changed();
  }, 300);
  search.addEventListener("input", onSearch);
  byId("tx-filters").addEventListener("submit", (event) => {
    event.preventDefault();
    onSearch.flush();
  });
  stateSelect.addEventListener("change", () => {
    state.state = stateSelect.value;
    changed();
  });
  categorySelect.addEventListener("change", () => {
    state.category = categorySelect.value;
    changed();
  });
  sortSelect.addEventListener("change", () => {
    state.sort = sortSelect.value;
    changed();
  });
  for (const [input, key] of [
    [fromInput, "date_from"],
    [toInput, "date_to"],
  ]) {
    input.addEventListener("change", () => {
      const value = /^\d{4}-\d{2}-\d{2}$/.test(input.value) ? input.value : "";
      if (value === state[key]) return;
      state[key] = value;
      changed();
    });
  }
  byId("tx-clear").addEventListener("click", () => {
    Object.assign(state, { q: "", state: "all", category: "", date_from: "", date_to: "" });
    search.value = "";
    stateSelect.value = "all";
    categorySelect.value = "";
    fromInput.value = "";
    toInput.value = "";
    changed();
  });
  prev.addEventListener("click", () => {
    if (state.page <= 1) return;
    state.page -= 1;
    changed({ resetPage: false });
  });
  next.addEventListener("click", () => {
    if (state.page * PAGE_SIZE >= total) return;
    state.page += 1;
    changed({ resetPage: false });
  });

  // The list does not wait for the account names: start it right away.
  const firstLoad = list.load();
  categories.load();
  try {
    const accounts = await get("/api/accounts");
    const picker = new AccountPicker(byId("tx-accounts"), {
      accounts: pickerAccounts(accounts.items),
      selected: state.accounts,
      align: "right", // the picker sits at the end of a right-aligned row
      onChange: (ids) => {
        state.accounts = ids;
        changed();
        categories.load();
      },
    });
    const known = picker.value();
    if (known.length !== state.accounts.length) {
      state.accounts = known; // ids from the URL that no longer exist
      changed({ resetPage: false });
      categories.load();
    }
  } catch (err) {
    reportError(err, "Accounts could not be loaded");
  }
  await firstLoad;
}
