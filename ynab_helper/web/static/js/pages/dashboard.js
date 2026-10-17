// Dashboard: the import account's balances and counts from the YNAB mirror (F64, F100),
// the newest statement with its import outcome, and the active setup.

import { get } from "../core/api.js";
import { byId, setText } from "../core/dom.js";
import { count, dateRange, money, plural, timestamp } from "../core/format.js";
import { html, setHTML } from "../core/html.js";
import { createLoader } from "../core/loader.js";
import { hideLoadError, loadErrorHandler } from "../components/load_error.js";
import { bindSyncButton } from "../components/sync.js";

const STATUS_LABELS = {
  imported: "imported",
  partial: "imported with problems",
  failed: "import failed",
  stored: "not imported yet",
  legacy: "imported by v1",
};

function statementMeta(statement) {
  if (!statement) return html`Upload your first statement`;
  const status = STATUS_LABELS[statement.status] || statement.status || "uploaded";
  if (!statement.window) return html`${status}`;
  return html`${status} · ${dateRange(statement.window[0], statement.window[1])}`;
}

function importSummary(statement) {
  if (!statement || !statement.summary) return null;
  const s = statement.summary;
  const parts = [];
  if (s.created) parts.push(`${s.created} created`);
  if (s.updated) parts.push(`${s.updated} updated`);
  if (s.linked) parts.push(`${s.linked} linked`);
  if (s.removed) parts.push(`${s.removed} removed`);
  if (s.failures) parts.push(`${s.failures} failed`);
  if (s.not_applied) parts.push(`${s.not_applied} not applied`);
  return parts.length ? parts.join(", ") : "no changes needed";
}

function alerts(d) {
  const items = [];
  if (!d.account.found) {
    items.push(
      html`<p class="alert alert-warn" role="status">The import account was not found in YNAB's data
        yet. If it was deleted in YNAB, choose another one with <code>/setup</code> in Telegram.</p>`,
    );
  } else if (d.account.deleted || d.account.closed) {
    items.push(
      html`<p class="alert alert-warn" role="status">The import account
        <strong>${d.account.name || ""}</strong> is ${d.account.deleted ? "deleted" : "closed"} in YNAB:
        imports are refused. Choose another one with <code>/setup</code> in Telegram.</p>`,
    );
  }
  const statement = d.last_statement;
  if (statement && (statement.status === "partial" || statement.status === "failed")) {
    items.push(
      html`<p class="alert alert-error" role="status">The last import of <strong>${statement.name || "the statement"}</strong>
        did not finish (${STATUS_LABELS[statement.status]}). Upload the statement again to complete it —
        the import picks up where it stopped.</p>`,
    );
  }
  return items;
}

function render(d) {
  hideLoadError(byId("dash-error"));
  const currency = d.currency;
  const accountName = d.account.name || "the import account";
  setText(
    byId("dash-subtitle"),
    d.syncing
      ? `${accountName} · syncing with YNAB…`
      : `${accountName}${d.last_sync_at ? ` · synced ${timestamp(d.last_sync_at)}` : ""}`,
  );
  setHTML(byId("dash-alerts"), html`${alerts(d)}`);

  const b = d.balances;
  setText(byId("dash-balance"), b.working === null ? "—" : money(b.working, currency));
  setText(
    byId("dash-balance-meta"),
    b.working === null
      ? "Not synced yet"
      : `Cleared ${money(b.cleared, currency)} · pending ${money(b.uncleared, currency)}`,
  );

  const tx = d.transactions || {};
  setText(byId("dash-tx"), count(tx.total ?? 0));
  setText(
    byId("dash-tx-meta"),
    `In ${accountName}: ${count(tx.cleared ?? 0)} cleared, ${count(tx.reconciled ?? 0)} reconciled`,
  );
  setText(byId("dash-pending"), count(tx.uncleared ?? 0));

  const statement = d.last_statement;
  setText(byId("dash-csv"), statement ? statement.name || "(unnamed statement)" : "—");
  byId("dash-csv").title = statement?.name || "";
  setHTML(byId("dash-csv-meta"), statementMeta(statement));

  setText(byId("dash-budget"), d.budget_name || "—");
  setText(byId("dash-account"), d.account.name || "—");
  setText(byId("dash-auto"), d.auto_approve ? "On" : "Off");
  const ledger = d.ledger || { total: 0 };
  setText(byId("dash-ledger"), plural(ledger.total || 0, "statement row"));
  const summary = importSummary(statement);
  setText(
    byId("dash-imported"),
    statement && statement.imported_at
      ? `${timestamp(statement.imported_at)}${summary ? ` · ${summary}` : ""}`
      : "—",
  );
  setText(byId("dash-synced"), d.syncing ? "syncing…" : timestamp(d.last_sync_at));
}

/** Replace every figure with "—" (a failed load must not leave stale numbers looking current). */
function clear() {
  setText(byId("dash-subtitle"), "Could not load the dashboard.");
  setHTML(byId("dash-alerts"), html``);
  for (const id of ["dash-balance", "dash-tx", "dash-pending", "dash-csv"]) setText(byId(id), "—");
  byId("dash-csv").title = "";
  for (const id of ["dash-balance-meta", "dash-tx-meta", "dash-csv-meta"]) setText(byId(id), "");
  for (const id of ["dash-budget", "dash-account", "dash-auto", "dash-ledger", "dash-imported", "dash-synced"]) {
    setText(byId(id), "—");
  }
}

export async function init() {
  const loader = createLoader({
    fetch: (signal) => get("/api/dashboard", { signal }),
    render,
    renderError: loadErrorHandler(byId("dash-error"), "the dashboard", clear),
  });
  bindSyncButton(byId("dash-sync"), () => loader.load({ quiet: true }));
  await loader.load();
}
