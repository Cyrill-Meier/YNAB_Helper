// Spending: net spending per category over N complete months plus the month-to-date shown
// separately (F30, F65). Window and accounts are kept in the address bar (F101); category
// rows link to the matching transactions with the same accounts and dates.

import { get, reportError } from "../core/api.js";
import { byId, setText, show } from "../core/dom.js";
import { hbar, vbars } from "../core/chart.js";
import { amount, dateRange, money, nowrap, percent, plural } from "../core/format.js";
import { html, setHTML } from "../core/html.js";
import { createLoader } from "../core/loader.js";
import { pageUrl, readChoice, readList, readParams, writeParams } from "../core/urlstate.js";
import { AccountPicker, pickerAccounts } from "../components/account_picker.js";
import { hideLoadError, loadErrorHandler } from "../components/load_error.js";
import { bindSyncButton, showSyncStatus } from "../components/sync.js";

const MONTH_CHOICES = ["3", "6", "12", "24"];
const DEFAULTS = { months: "6", accounts: [] };

function trend(current, previous, currency, previousLabel) {
  if (typeof previous !== "number" || previous <= 0 || typeof current !== "number") {
    return html`no earlier month to compare`;
  }
  const delta = current - previous;
  const cls = delta > 0 ? "amount-out" : "amount-in";
  return html`<span class="${cls}">${money(delta, currency, { signed: true })} (${percent(delta / previous)})</span> vs ${nowrap(previousLabel)}`;
}

function render(d, state) {
  showSyncStatus(byId("sp-sync-status"), d);
  const currency = d.currency;
  const months = d.months || [];
  const perMonth = d.per_month_total || [];
  const mtd = d.month_to_date || { total: 0, month: "", from: "", through: "" };
  const cats = d.categories || [];
  const last = months.length - 1;

  setText(byId("sp-mtd"), money(mtd.total, currency));
  setHTML(
    byId("sp-mtd-meta"),
    mtd.month ? html`${dateRange(mtd.from, mtd.through)} (not in the average)` : html``,
  );
  setText(byId("sp-last"), last >= 0 ? money(perMonth[last], currency) : "—");
  setHTML(
    byId("sp-last-meta"),
    last >= 0
      ? html`${nowrap(months[last])} · ${trend(perMonth[last], perMonth[last - 1], currency, months[last - 1] || "")}`
      : html``,
  );
  setText(byId("sp-avg"), money(d.average, currency));
  setHTML(
    byId("sp-avg-meta"),
    months.length
      ? html`${dateRange(months[0], months[last])} · ${plural(months.length, "complete month")}`
      : html``,
  );
  setText(
    byId("sp-window-label"),
    months.length ? `${plural(months.length, "complete month")} + month to date` : "",
  );

  // Total per month: complete months, then the month-to-date (lighter bar).
  const monthPeak = Math.max(0, ...perMonth, mtd.total || 0);
  const monthRows = months.map(
    (m, i) => html`<li class="flex items-center gap-3">
      <span class="w-28 shrink-0 text-xs tabular-nums text-ink-500 dark:text-slate-400">${m}</span>
      <span class="flex-1">${hbar(perMonth[i], monthPeak)}</span>
      <span class="w-24 shrink-0 text-right text-sm tabular-nums">${amount(perMonth[i], currency)}</span>
    </li>`,
  );
  if (mtd.month) {
    monthRows.push(html`<li class="flex items-center gap-3">
      <span class="w-28 shrink-0 whitespace-nowrap text-xs tabular-nums text-ink-500 dark:text-slate-400">${mtd.month} (to date)</span>
      <span class="flex-1">${hbar(mtd.total, monthPeak, { muted: true })}</span>
      <span class="w-24 shrink-0 text-right text-sm tabular-nums text-ink-500 dark:text-slate-400">${amount(mtd.total, currency)}</span>
    </li>`);
  }
  setHTML(byId("sp-month-bars"), html`${monthRows}`);

  // Categories: same scale for every row, so categories compare at a glance.
  show(byId("sp-empty"), cats.length === 0);
  const peak = Math.max(0, ...cats.flatMap((c) => [...(c.by_month || []), c.month_to_date || 0]));
  const labels = [...months, mtd.month ? `${mtd.month} (to date)` : ""];
  const range = {
    accounts: state.accounts,
    date_from: months.length ? `${months[0]}-01` : mtd.from,
    date_to: d.today || mtd.through,
  };
  setHTML(
    byId("sp-cats"),
    html`${cats.map((c) => {
      const named = typeof c.category === "string" && c.category !== "";
      const link = pageUrl("/app/transactions", { category: named ? c.category : "__none__", ...range });
      const values = [...(c.by_month || []), c.month_to_date || 0];
      return html`<li>
        <a href="${link}" class="block px-5 py-3 hover:bg-slate-50 dark:hover:bg-ink-700/40">
          <div class="flex flex-wrap items-baseline justify-between gap-x-4">
            <span class="font-medium ${named ? "" : "italic text-ink-500 dark:text-slate-400"}">${named ? c.category : "(uncategorized)"}</span>
            <span class="text-sm tabular-nums">${amount(c.total, currency)}</span>
          </div>
          <div class="mt-0.5 flex flex-wrap justify-between gap-x-4 text-xs text-ink-500 dark:text-slate-400">
            <span>${plural(c.count || 0, "transaction")}</span>
            <span>avg ${amount(c.average, currency)} / month · ${amount(c.month_to_date, currency)} this month</span>
          </div>
          <div class="mt-2 h-10">${vbars(values, {
            peak,
            labels: values.map((v, i) => `${labels[i]}: ${money(v, currency)}`),
            mutedFrom: months.length,
          })}</div>
        </a>
      </li>`;
    })}`,
  );
}

/** Empty every figure (a failed load must not leave the previous window's numbers). */
function clear() {
  setText(byId("sp-sync-status"), ""); // no refresh follows a failure
  for (const id of ["sp-mtd", "sp-last", "sp-avg"]) setText(byId(id), "—");
  for (const id of ["sp-mtd-meta", "sp-last-meta", "sp-avg-meta", "sp-window-label"]) {
    setText(byId(id), "");
  }
  setHTML(byId("sp-month-bars"), html``);
  setHTML(byId("sp-cats"), html``);
  show(byId("sp-empty"), false);
}

export async function init() {
  const params = readParams();
  const state = {
    months: readChoice(params, "months", MONTH_CHOICES, "6"),
    accounts: readList(params, "accounts").slice(0, 100),
  };
  const select = byId("sp-months");
  select.value = state.months;

  const loader = createLoader({
    fetch: (signal) =>
      get(pageUrl("/api/spending", { months: state.months, accounts: state.accounts }), { signal }),
    render: (d) => {
      hideLoadError(byId("sp-error"));
      render(d, state);
    },
    renderError: loadErrorHandler(byId("sp-error"), "spending", clear),
    onLoading: (busy) => show(byId("sp-loading"), busy),
  });
  const reload = () => {
    writeParams(state, DEFAULTS);
    return loader.load();
  };
  select.addEventListener("change", () => {
    state.months = MONTH_CHOICES.includes(select.value) ? select.value : "6";
    reload();
  });
  bindSyncButton(byId("sp-sync"), () => loader.load({ quiet: true }));

  const first = loader.load();
  try {
    const accounts = await get("/api/accounts");
    const picker = new AccountPicker(byId("sp-accounts"), {
      accounts: pickerAccounts(accounts.items),
      selected: state.accounts,
      align: "left", // the first control of a left-aligned toolbar
      onChange: (ids) => {
        state.accounts = ids;
        reload();
      },
    });
    if (picker.value().length !== state.accounts.length) {
      state.accounts = picker.value();
      reload();
    }
  } catch (err) {
    reportError(err, "Accounts could not be loaded");
  }
  await first;
}
