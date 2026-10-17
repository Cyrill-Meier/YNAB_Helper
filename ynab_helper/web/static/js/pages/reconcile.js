// Reconcile, in two steps (F04): "Check balance" asks for a preview (numbers plus a
// one-time token; no YNAB write), and only a confirmed dialog posts the adjustment with
// that token. The dialog repeats the preview's warnings (e.g. uncleared transactions the
// adjustment would count twice) and then asks to "Post anyway". A token that expired or no
// longer matches YNAB is refused by the server (409 stale): the page asks to check again.

import { describeError, get, isAbort, post, reportError } from "../core/api.js";
import { withBusy } from "../components/busy.js";
import { confirmDialog } from "../components/confirm.js";
import { toast } from "../components/toast.js";
import { byId, setText, show } from "../core/dom.js";
import { date, duration, money, nowrap } from "../core/format.js";
import { html, setHTML } from "../core/html.js";

const OUTCOMES = {
  created: (a, c) => html`<p class="alert alert-ok">Posted an adjustment of <strong>${money(a.delta_milli, c, { signed: true })}</strong>
    dated ${date(a.date)} (payee “${a.payee}”, ${a.cleared}). YNAB now matches the statement.</p>`,
  duplicate: () => html`<p class="alert alert-info">YNAB already has this adjustment — nothing new was posted.</p>`,
  in_sync: () => html`<p class="alert alert-info">YNAB already matches the statement — nothing was posted.</p>`,
};

function numbers(p) {
  const c = p.currency;
  const asOf = nowrap(date(p.as_of));
  const rows = [
    [html`Statement balance on ${asOf}`, money(p.csv_balance, c), ""],
    [html`YNAB cleared balance on ${asOf}`, money(p.ynab_cleared_as_of, c), ""],
    ["Difference", money(p.delta, c, { signed: true }), p.in_sync ? "amount-in" : "amount-out"],
    ["YNAB cleared balance today", money(p.ynab_cleared_now, c), ""],
    [html`Cleared after ${asOf}`, money(p.cleared_after_as_of, c), ""],
  ];
  return html`<dl class="grid grid-cols-1 gap-x-6 gap-y-1 text-sm sm:grid-cols-2">
    ${rows.map(
      ([label, value, cls]) => html`<div class="flex justify-between gap-4 border-b border-slate-100 py-1.5 dark:border-ink-700">
        <dt class="text-ink-500 dark:text-slate-400">${label}</dt>
        <dd class="font-medium tabular-nums ${cls}">${value}</dd>
      </div>`,
    )}
  </dl>`;
}

/**
 * The confirmation dialog for posting the adjustment of preview `p`. With warnings from the
 * check they are repeated here — the last step before a YNAB write must not read as an
 * all-clear — and the dialog becomes a destructive one ("Post anyway", focus on Cancel).
 */
export function confirmContent(p, accountName) {
  const problems = (p.problems || []).filter((text) => typeof text === "string" && text);
  const what = html`This adds <strong>${money(p.delta, p.currency, { signed: true })}</strong> to
    <strong>${accountName}</strong>, dated ${nowrap(date(p.as_of))}, payee “CSV Reconciliation”,
    marked reconciled.`;
  if (!problems.length) {
    return {
      title: "Post a reconciliation adjustment?",
      body: html`<p>${what} Afterwards YNAB's cleared balance matches the statement.</p>`,
      confirmLabel: "Post adjustment",
      danger: false,
    };
  }
  return {
    title: "Post the adjustment despite the warnings?",
    body: html`<p>${what}</p>
      <p class="mt-3 font-medium text-ink-900 dark:text-slate-100">The check found:</p>
      <ul class="mt-1 space-y-1">${problems.map((text) => html`<li class="alert alert-warn">${text}</li>`)}</ul>
      <p class="mt-3">The adjustment may then be wrong. Fix these in YNAB and press Check balance
        again, unless you are sure the difference is right.</p>`,
    confirmLabel: "Post anyway",
    danger: true,
  };
}

export async function init() {
  const previewButton = byId("rec-preview");
  const area = byId("rec-preview-result");
  const outcome = byId("rec-outcome");
  let expiryTimer = null;
  let accountName = "the import account";

  try {
    const d = await get("/api/dashboard");
    accountName = d.account?.name || accountName;
    const s = d.last_statement;
    setText(byId("rec-csv"), s ? s.name || "(unnamed statement)" : "(no statement uploaded yet)");
    byId("rec-csv").title = s?.name || "";
  } catch (err) {
    reportError(err);
  }

  function showMessage(fragment) {
    clearTimeout(expiryTimer);
    setHTML(area, fragment);
    show(area, true);
    show(byId("rec-initial"), false);
  }

  function renderPreview(p) {
    clearTimeout(expiryTimer);
    const problems = (p.problems || []).map((text) => html`<li class="alert alert-warn">${text}</li>`);
    const action = p.in_sync
      ? html`<p class="alert alert-ok">YNAB matches the statement — nothing to do.</p>`
      : html`<div class="flex flex-wrap items-center gap-3">
          <button type="button" id="rec-apply" class="btn-primary">Post adjustment of ${money(p.delta, p.currency, { signed: true })}</button>
          <span class="text-xs text-ink-500 dark:text-slate-400" id="rec-expiry">${
            p.token_ttl_seconds ? `This check is valid for ${duration(p.token_ttl_seconds)}.` : ""
          }</span>
        </div>`;
    showMessage(html`<p class="text-sm text-ink-500 dark:text-slate-400">Statement <strong class="text-ink-900 dark:text-slate-100">${p.csv_name}</strong></p>
      ${numbers(p)}
      ${problems.length ? html`<ul class="space-y-1">${problems}</ul>` : ""}
      ${action}`);
    if (p.in_sync || !p.token) return;

    const apply = byId("rec-apply");
    if (p.token_ttl_seconds) {
      expiryTimer = setTimeout(() => {
        apply.disabled = true;
        setText(byId("rec-expiry"), "This check expired — press Check balance again.");
      }, p.token_ttl_seconds * 1000);
    }
    apply.addEventListener("click", async () => {
      const ok = await confirmDialog(confirmContent(p, accountName));
      if (!ok) return;
      await withBusy(apply, "Posting…", async () => {
        try {
          const r = await post("/api/reconcile", { token: p.token });
          clearTimeout(expiryTimer);
          const a = r.adjustment;
          const view = OUTCOMES[a.outcome] || OUTCOMES.in_sync;
          setHTML(byId("rec-outcome-body"), view(a, r.preview?.currency || p.currency));
          show(outcome, true);
          show(area, false);
          show(byId("rec-initial"), true);
          outcome.focus();
          toast(a.outcome === "created" ? "ok" : "info", a.outcome === "created" ? "Adjustment posted." : "Nothing was posted.");
        } catch (err) {
          if (isAbort(err) || err.handled) return;
          if (err.status === 409 && err.error === "stale") {
            showMessage(html`<p class="alert alert-warn" role="alert">${describeError(err)} Press Check balance to see the current numbers.</p>`);
            return;
          }
          reportError(err, "Reconcile failed");
        }
      });
    });
  }

  previewButton.addEventListener("click", () =>
    withBusy(previewButton, "Checking…", async () => {
      show(outcome, false);
      try {
        renderPreview(await get("/api/reconcile/preview"));
      } catch (err) {
        if (isAbort(err) || err.handled) return;
        if (err.status === 409 || err.status === 429) {
          showMessage(html`<p class="alert alert-warn" role="alert">${describeError(err)}</p>`);
          return;
        }
        reportError(err, "Check failed");
      }
    }),
  );
}
