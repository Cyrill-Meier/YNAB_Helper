// Upload: a Revolut statement is sent as the raw request body (text/csv, file name in
// X-Filename — no multipart, F22). The file input is keyboard-accessible and the label
// opens the chooser once (F66); drag & drop works on the zone. One upload at a time (F103
// in-flight guard). The result is rendered from the ImportResult the server returns, and
// the toast reports what actually changed in YNAB — not the number of parsed rows (F103).

import { describeError, isAbort, request } from "../core/api.js";
import { byId, setText, show } from "../core/dom.js";
import { amount, count, date, dateRange, plural } from "../core/format.js";
import { html, setHTML } from "../core/html.js";
import { toast } from "../components/toast.js";

const MAX_BYTES = 25 * 1024 * 1024;
const DRAG_CLASSES = ["border-brand-500", "bg-brand-500/5"];
const FIELD_NAMES = {
  amount: "amount",
  cleared: "cleared",
  date: "date",
  memo: "memo",
  payee_name: "payee",
  approved: "approved",
  flag_color: "flag",
};

function fieldValue(name, value, currency) {
  if (value === null || value === undefined || value === "") return "—";
  if (name === "amount" && typeof value === "number") return amount(value, currency);
  return String(value);
}

function txLine(tx, currency) {
  if (!tx) return html`<span class="text-ink-500 dark:text-slate-400">—</span>`;
  return html`<span class="tabular-nums text-ink-500 dark:text-slate-400">${date(tx.date)}</span>
    <span class="font-medium">${tx.payee || "—"}</span>
    <span class="tabular-nums ${tx.amount < 0 ? "amount-out" : "amount-in"}">${amount(tx.amount, currency)}</span>`;
}

function section(title, items, renderItem, { open = false } = {}) {
  if (!items || !items.length) return "";
  return html`<details class="rounded-md border border-slate-200 dark:border-ink-700"${open ? html` open` : ""}>
    <summary class="cursor-pointer select-none px-3 py-2 text-sm font-medium">${title} (${count(items.length)})</summary>
    <ul class="divide-y divide-slate-100 border-t border-slate-200 text-sm dark:divide-ink-700 dark:border-ink-700">
      ${items.map((item) => html`<li class="flex flex-wrap items-baseline gap-x-3 gap-y-1 px-3 py-2">${renderItem(item)}</li>`)}
    </ul>
  </details>`;
}

/** "3 created, 1 updated" — what the import changed in YNAB. */
export function changeSummary(r) {
  const parts = [];
  if (r.created?.length) parts.push(`${r.created.length} created`);
  if (r.updated?.length) parts.push(`${r.updated.length} updated`);
  if (r.removed?.length) parts.push(`${r.removed.length} removed`);
  if (r.linked?.length) parts.push(`${r.linked.length} linked`);
  return parts.join(", ");
}

/**
 * The status line and toast for an upload's ImportResult `r` (F103): what changed in YNAB,
 * "no changes" with the number of rows checked (not "YNAB has all N rows": reverted, skipped
 * and kept-deleted rows are deliberately not in YNAB), or that there were problems.
 */
export function uploadOutcome(name, r) {
  const changes = changeSummary(r);
  if (!r.ok) {
    return {
      status: `Imported ${name} with problems.`,
      kind: "warn",
      toast: `${name}: the import finished with problems — see the result below.`,
    };
  }
  if (!r.has_changes) {
    return {
      status: `${name}: nothing to change.`,
      kind: "info",
      toast: `${name}: no changes — YNAB already matches this statement (${plural(r.rows_total || 0, "row")} checked).`,
    };
  }
  return { status: `Imported ${name}: ${changes}.`, kind: "ok", toast: `Imported ${name}: ${changes}.` };
}

function headline(r) {
  const changes = changeSummary(r);
  if (r.rate_limited) {
    return html`<p class="alert alert-error" role="alert">YNAB's rate limit stopped the import${
      r.not_applied ? html` — ${plural(r.not_applied, "change")} not applied` : ""
    }. Upload the same statement again in a while to finish it.</p>`;
  }
  if (!r.ok) {
    return html`<p class="alert alert-error" role="alert">The import finished with problems:
      ${plural(r.failures?.length || 0, "change")} failed${r.not_applied ? html`, ${plural(r.not_applied, "change")} not applied` : ""}.
      ${changes ? html`Done: ${changes}.` : ""} Upload the statement again to retry.</p>`;
  }
  if (!r.has_changes) {
    return html`<p class="alert alert-info">Nothing to change — YNAB already matches this statement
      (${plural(r.rows_total || 0, "row")} checked).</p>`;
  }
  return html`<p class="alert alert-ok">Imported: ${changes}.</p>`;
}

function renderResult(r) {
  const currency = r.currency;
  const facts = [
    ["Statement period", r.window ? dateRange(r.window[0], r.window[1]) : "—"],
    ["Currency", r.currency || "—"],
    [
      "Rows",
      `${count(r.rows_total || 0)}${r.rows_pending ? ` (${count(r.rows_pending)} pending)` : ""}${
        r.rows_reverted ? `, ${count(r.rows_reverted)} reverted` : ""
      }`,
    ],
    ["Already up to date", count(r.unchanged || 0)],
    ["Skipped rows", count((r.skipped_rows || []).length)],
    ["Deleted in YNAB (kept deleted)", count(r.ignored_deleted || 0)],
  ];
  return html`${headline(r)}
    <dl class="grid grid-cols-1 gap-x-6 gap-y-1 text-sm sm:grid-cols-2">
      ${facts.map(
        ([label, value]) => html`<div class="flex justify-between gap-4 border-b border-slate-100 py-1.5 dark:border-ink-700">
          <dt class="text-ink-500 dark:text-slate-400">${label}</dt><dd class="font-medium tabular-nums">${value}</dd>
        </div>`,
      )}
    </dl>
    ${(r.warnings || []).length
      ? html`<ul class="space-y-1">${r.warnings.map((w) => html`<li class="alert alert-warn">${w}</li>`)}</ul>`
      : ""}
    <div class="space-y-2">
      ${section("Failed", r.failures, (f) => html`${txLine(f.tx, currency)} <span class="w-full text-rose-700 dark:text-rose-300">${f.action}: ${f.error}</span>`, { open: true })}
      ${section("Created in YNAB", r.created, (t) => txLine(t, currency))}
      ${section("Updated in YNAB", r.updated, (c) =>
        html`${txLine(c.tx, currency)}
          <span class="w-full text-xs text-ink-500 dark:text-slate-400">${Object.entries(c.fields || {})
            .map(([name, [before, after]]) => `${FIELD_NAMES[name] || name}: ${fieldValue(name, before, currency)} → ${fieldValue(name, after, currency)}`)
            .join("; ")}${c.reason ? ` (${c.reason})` : ""}</span>`,
      )}
      ${section("Removed from YNAB", r.removed, (x) => html`${txLine(x.tx, currency)} <span class="text-xs text-ink-500 dark:text-slate-400">${x.reason}</span>`)}
      ${section("Linked to existing YNAB transactions", r.linked, (t) => txLine(t, currency))}
      ${section("Skipped rows", r.skipped_rows, ([line, reason]) => html`<span class="tabular-nums text-ink-500 dark:text-slate-400">row ${line}</span> <span>${reason}</span>`)}
    </div>`;
}

export async function init() {
  const zone = byId("up-drop");
  const input = byId("up-input");
  const status = byId("up-status");
  const result = byId("up-result");
  let uploading = false;

  function setBusy(busy, name = "") {
    uploading = busy;
    input.disabled = busy;
    if (busy) zone.setAttribute("aria-busy", "true");
    else zone.removeAttribute("aria-busy");
    if (busy) {
      setHTML(status, html`<span class="spinner" aria-hidden="true"></span> Uploading and importing ${name}… this can take a minute.`);
    }
  }

  async function upload(file) {
    if (uploading) {
      toast("info", "An upload is already running.");
      return;
    }
    if (!file) return;
    if (!/\.csv$/i.test(file.name)) {
      toast("error", `${file.name} is not a .csv file. Choose the Revolut statement export.`);
      return;
    }
    if (file.size === 0) {
      toast("error", `${file.name} is empty.`);
      return;
    }
    if (file.size > MAX_BYTES) {
      toast("error", `${file.name} is larger than 25 MB.`);
      return;
    }
    setBusy(true, file.name);
    show(result, false);
    try {
      const r = await request("POST", "/api/upload", {
        body: file,
        headers: { "Content-Type": "text/csv", "X-Filename": encodeURIComponent(file.name) },
      });
      setText(byId("up-result-name"), file.name);
      setHTML(byId("up-result-body"), renderResult(r));
      show(result, true);
      const outcome = uploadOutcome(file.name, r);
      setText(status, outcome.status);
      toast(outcome.kind, outcome.toast);
      result.focus();
    } catch (err) {
      if (isAbort(err) || err.handled) {
        setText(status, "");
        return;
      }
      const message = describeError(err);
      setText(status, `Upload failed: ${message}`);
      toast("error", `Upload failed: ${message}`);
    } finally {
      setBusy(false);
      input.value = "";
    }
  }

  input.addEventListener("change", () => upload(input.files && input.files[0]));

  const highlight = (on) => {
    for (const cls of DRAG_CLASSES) zone.classList.toggle(cls, on);
  };
  for (const type of ["dragenter", "dragover"]) {
    zone.addEventListener(type, (event) => {
      event.preventDefault();
      if (event.dataTransfer) event.dataTransfer.dropEffect = uploading ? "none" : "copy";
      highlight(!uploading);
    });
  }
  zone.addEventListener("dragleave", (event) => {
    if (!zone.contains(event.relatedTarget)) highlight(false);
  });
  zone.addEventListener("drop", (event) => {
    event.preventDefault();
    highlight(false);
    const files = event.dataTransfer ? event.dataTransfer.files : null;
    if (files && files.length > 1) {
      toast("error", "Drop one statement at a time.");
      return;
    }
    upload(files && files[0]);
  });
  // A file dropped next to the zone must not make the browser navigate away to it.
  for (const type of ["dragover", "drop"]) {
    window.addEventListener(type, (event) => {
      if (!zone.contains(event.target)) event.preventDefault();
    });
  }
}
