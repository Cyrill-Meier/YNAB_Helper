// Subscriptions: recurring outflows detected by the server (C03, F98). Each row's history
// bars are scaled to that row's own highest charge, so a 12.90 → 15.90 price rise is
// visible next to the rent (F105); every row has the same number of bar slots (newest on
// the right), so a yearly subscription's two charges look like two charges, not two blocks.
// The run-rate is the server's monthly equivalent.

import { get, reportError } from "../core/api.js";
import { byId, setText, show } from "../core/dom.js";
import { vbars } from "../core/chart.js";
import { amount, count, date, money, nowrap } from "../core/format.js";
import { html, setHTML } from "../core/html.js";
import { createLoader } from "../core/loader.js";
import { pageUrl, readChoice, readList, readParams, writeParams } from "../core/urlstate.js";
import { AccountPicker, pickerAccounts } from "../components/account_picker.js";
import { hideLoadError, loadErrorHandler } from "../components/load_error.js";
import { bindSyncButton, showSyncStatus } from "../components/sync.js";

const WINDOWS = ["6", "13", "24", "36"];
const DEFAULTS = { lookback: "13", accounts: [] };
const SPARK_CHARGES = 8;
const CADENCE_BADGES = {
  weekly: "badge bg-fuchsia-100 text-fuchsia-700 dark:bg-fuchsia-500/15 dark:text-fuchsia-300",
  biweekly: "badge bg-violet-100 text-violet-700 dark:bg-violet-500/15 dark:text-violet-300",
  monthly: "badge badge-brand",
  yearly: "badge bg-amber-100 text-amber-800 dark:bg-amber-500/15 dark:text-amber-300",
};

function row(s, currency) {
  const history = (s.history || []).slice(-SPARK_CHARGES);
  const values = history.map((h) => h.amount);
  const categories = (s.categories || []).filter(Boolean);
  return html`<tr class="${s.active ? "" : "opacity-60"}">
    <td class="min-w-[10rem] px-4 py-2">
      <span class="font-medium">${s.payee_name}</span>
      ${s.price_changed
        ? html`<span class="badge badge-danger ml-1" title="The amount changed over time"><span aria-hidden="true">±</span><span class="sr-only">price changed</span></span>`
        : ""}
      ${s.active ? "" : html`<span class="badge badge-neutral ml-1" title="No charge for longer than the cadence">ended?</span>`}
      ${(s.accounts || []).length ? html`<div class="text-xs text-ink-500 dark:text-slate-400">${s.accounts.join(", ")}</div>` : ""}
    </td>
    <td class="whitespace-nowrap px-4 py-2">
      <span class="${CADENCE_BADGES[s.cadence] || "badge badge-neutral"}">${s.cadence} (~${Math.round(s.median_days)} d)</span>
    </td>
    <td class="px-4 py-2">
      ${categories.length
        ? categories.map((c) => html`<span class="badge badge-neutral mb-0.5 mr-1">${c}</span>`)
        : html`<span class="text-xs italic text-ink-500 dark:text-slate-400">—</span>`}
    </td>
    <td class="whitespace-nowrap px-4 py-2 text-right tabular-nums">${amount(s.amount_latest, currency)}</td>
    <td class="whitespace-nowrap px-4 py-2 text-right tabular-nums text-ink-500 dark:text-slate-400">${amount(s.monthly_equivalent, currency)}</td>
    <td class="hidden whitespace-nowrap px-4 py-2 text-right text-xs tabular-nums text-ink-500 dark:text-slate-400 lg:table-cell">
      ${s.amount_min === s.amount_max ? "—" : `${amount(s.amount_min, currency)} – ${amount(s.amount_max, currency)}`}
    </td>
    <td class="px-4 py-2">
      <div class="h-6 w-20">${vbars(values, {
        labels: history.map((h) => `${h.date}: ${amount(h.amount, currency)}`),
        latestClass: "spark-bar-latest",
        slots: SPARK_CHARGES,
      })}</div>
      <span class="sr-only">${history.map((h) => `${h.date} ${amount(h.amount, currency)}`).join(", ")}</span>
    </td>
    <td class="whitespace-nowrap px-4 py-2 text-xs text-ink-500 dark:text-slate-400">
      ${date(s.last)} (${s.occurrences}×)
      ${s.active && s.next_expected ? html`<div>next ~${date(s.next_expected)}</div>` : ""}
    </td>
  </tr>`;
}

function render(d) {
  hideLoadError(byId("sub-error"));
  showSyncStatus(byId("sub-sync-status"), d);
  const items = d.items || [];
  const active = items.filter((s) => s.active).length;
  setText(byId("sub-count"), count(items.length));
  setHTML(
    byId("sub-count-meta"),
    items.length
      ? html`${count(active)} active · charged since ${nowrap(date(d.since))}`
      : html`since ${nowrap(date(d.since))}`,
  );
  setText(byId("sub-monthly"), money(d.monthly_total, d.currency));
  setText(byId("sub-changes"), count(items.filter((s) => s.price_changed).length));

  const note = byId("sub-yearly-note");
  if (d.yearly_detectable) {
    show(note, false);
  } else {
    setText(
      note,
      `Yearly subscriptions are listed only when they were charged within the last ${d.lookback_months} months. ` +
        "Choose 13 months or more to see every active yearly subscription.",
    );
    show(note, true);
  }

  show(byId("sub-empty"), items.length === 0);
  setHTML(byId("sub-tbody"), html`${items.map((s) => row(s, d.currency))}`);
}

/** Empty every figure (a failed load must not leave the previous window's rows). */
function clear() {
  setText(byId("sub-sync-status"), ""); // no refresh follows a failure
  for (const id of ["sub-count", "sub-monthly", "sub-changes"]) setText(byId(id), "—");
  setText(byId("sub-count-meta"), "");
  show(byId("sub-yearly-note"), false);
  show(byId("sub-empty"), false);
  setHTML(byId("sub-tbody"), html``);
}

export async function init() {
  const params = readParams();
  const state = {
    lookback: readChoice(params, "lookback", WINDOWS, "13"),
    accounts: readList(params, "accounts").slice(0, 100),
  };
  const select = byId("sub-window");
  select.value = state.lookback;

  const loader = createLoader({
    fetch: (signal) =>
      get(pageUrl("/api/subscriptions", { lookback_months: state.lookback, accounts: state.accounts }), {
        signal,
      }),
    render,
    renderError: loadErrorHandler(byId("sub-error"), "subscriptions", clear),
    onLoading: (busy) => show(byId("sub-loading"), busy),
  });
  const reload = () => {
    writeParams(state, DEFAULTS);
    return loader.load();
  };
  select.addEventListener("change", () => {
    state.lookback = WINDOWS.includes(select.value) ? select.value : "13";
    reload();
  });
  bindSyncButton(byId("sub-sync"), () => loader.load({ quiet: true }));

  const first = loader.load();
  try {
    const accounts = await get("/api/accounts");
    const picker = new AccountPicker(byId("sub-accounts"), {
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
