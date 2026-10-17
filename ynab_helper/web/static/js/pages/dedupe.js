// Dedupe: scan → select → confirm (F13, F20, F46). The server keeps the scan (id + 10 min
// lifetime) and only deletes ids of that scan. Deletion is sent in chunks of at most 200
// ids (the server's cap, F104) with progress; it stops at a YNAB rate limit or a stale
// scan, and reports per-row outcomes inline.

import { describeError, get, isAbort, post, reportError } from "../core/api.js";
import { clearedBadge } from "../components/badges.js";
import { withBusy } from "../components/busy.js";
import { confirmDialog } from "../components/confirm.js";
import { toast } from "../components/toast.js";
import { chunk, DEDUPE_CHUNK_SIZE as CHUNK_SIZE } from "../core/chunk.js";
import { byId, setText, show } from "../core/dom.js";
import { amount, amountClass, count, date, dateRange, money, plural } from "../core/format.js";
import { html, setHTML } from "../core/html.js";

export async function init() {
  const scanButton = byId("dd-scan");
  const deleteButton = byId("dd-delete");
  const selectAll = byId("dd-select-all");
  const tbody = byId("dd-tbody");
  const progress = byId("dd-progress");
  const report = byId("dd-report");
  let currency = null;

  const state = {
    scan: null,
    candidates: [],
    selected: new Set(),
    notes: new Map(), // id → {kind, text}: per-row outcome of the last delete
    expiresAt: 0,
    deleting: false,
  };

  function expired() {
    return state.scan !== null && performance.now() > state.expiresAt;
  }

  function updateSelection() {
    const n = state.selected.size;
    setText(byId("dd-selected-count"), `${count(n)} selected`);
    deleteButton.disabled = n === 0 || state.deleting;
    selectAll.checked = n > 0 && n === state.candidates.length;
    selectAll.indeterminate = n > 0 && n < state.candidates.length;
    setText(byId("dd-candidates"), count(state.candidates.length));
  }

  function renderRows() {
    setHTML(
      tbody,
      html`${state.candidates.map((c) => {
        const note = state.notes.get(c.id);
        const label = `Select ${c.payee_name || "transaction"}, ${c.date}, ${amount(c.amount, currency)}`;
        return html`<tr>
          <td class="px-4 py-2">
            <input type="checkbox" class="checkbox" value="${c.id}" aria-label="${label}"${state.selected.has(c.id) ? html` checked` : ""}${state.deleting ? html` disabled` : ""}>
          </td>
          <td class="whitespace-nowrap px-4 py-2 text-ink-500 dark:text-slate-400">${date(c.date)}</td>
          <td class="px-4 py-2">
            <div>${c.payee_name || "—"}</div>
            ${c.category_name ? html`<div class="text-xs text-ink-500 dark:text-slate-400">${c.category_name}</div>` : ""}
            ${note ? html`<div class="text-xs ${note.kind === "error" ? "text-rose-700 dark:text-rose-300" : "text-amber-800 dark:text-amber-300"}">${note.text}</div>` : ""}
          </td>
          <td class="whitespace-nowrap px-4 py-2 text-right tabular-nums ${amountClass(c.amount)}">${amount(c.amount, currency)}</td>
          <td class="max-w-[24ch] truncate px-4 py-2 text-ink-500 dark:text-slate-400" title="${c.memo || ""}">${c.memo || ""}</td>
          <td class="px-4 py-2">${clearedBadge(c.cleared)}</td>
        </tr>`;
      })}`,
    );
    show(byId("dd-results"), state.candidates.length > 0);
    show(byId("dd-empty"), state.scan !== null && state.candidates.length === 0);
    updateSelection();
  }

  tbody.addEventListener("change", (event) => {
    const box = event.target;
    if (!(box instanceof HTMLInputElement) || box.type !== "checkbox") return;
    if (box.checked) state.selected.add(box.value);
    else state.selected.delete(box.value);
    updateSelection();
  });
  selectAll.addEventListener("change", () => {
    state.selected = selectAll.checked ? new Set(state.candidates.map((c) => c.id)) : new Set();
    for (const box of tbody.querySelectorAll("input[type=checkbox]")) box.checked = selectAll.checked;
    updateSelection();
  });
  byId("dd-clear").addEventListener("click", () => {
    state.selected.clear();
    for (const box of tbody.querySelectorAll("input[type=checkbox]")) box.checked = false;
    updateSelection();
  });

  scanButton.addEventListener("click", () =>
    withBusy(scanButton, "Scanning…", async () => {
      if (state.deleting) return;
      setText(progress, "");
      setHTML(report, html``);
      try {
        const scan = await get("/api/dedupe/scan");
        state.scan = scan;
        state.candidates = scan.candidates || [];
        state.selected = new Set();
        state.notes = new Map();
        state.expiresAt = performance.now() + Math.max(0, (scan.ttl_seconds || 600) - 5) * 1000;
        const dash = await get("/api/dashboard").catch(() => null);
        currency = dash ? dash.currency : null;
        setText(byId("dd-csv"), scan.csv_name || "—");
        byId("dd-csv").title = scan.csv_name || "";
        setHTML(byId("dd-range"), scan.window ? dateRange(scan.window[0], scan.window[1]) : html`—`);
        setText(
          byId("dd-counts"),
          `${count(scan.statement_rows || 0)} · ${count(scan.ynab_count_in_window || 0)}`,
        );
        show(byId("dd-meta"), true);
        show(byId("dd-initial"), false);
        renderRows();
      } catch (err) {
        if (isAbort(err) || err.handled) return;
        if (err.status === 409 || err.status === 429) {
          setHTML(report, html`<p class="alert alert-warn" role="alert">${describeError(err)}</p>`);
          return;
        }
        reportError(err, "Scan failed");
      }
    }),
  );

  /** Delete `ids` (already confirmed) in chunks; report per row and in total. */
  async function deleteIds(ids) {
    state.deleting = true;
    renderRows();
    const outcome = { deleted: new Set(), failed: 0, skipped: 0, notAttempted: 0, rateLimited: false, stopped: null };
    const batches = chunk(ids);
    let done = 0;
    try {
      for (const batch of batches) {
        setText(progress, `Deleting… ${count(done)} of ${count(ids.length)} done.`);
        let r;
        try {
          r = await post("/api/dedupe/delete", { scan_id: state.scan.scan_id, ids: batch });
        } catch (err) {
          if (isAbort(err)) return;
          outcome.stopped = err;
          outcome.notAttempted += ids.length - done;
          break;
        }
        for (const c of r.deleted || []) outcome.deleted.add(c.id);
        for (const id of r.already_deleted || []) outcome.deleted.add(id);
        for (const { candidate, reason } of r.skipped || []) {
          outcome.skipped += 1;
          state.notes.set(candidate.id, { kind: "warn", text: `Not deleted: ${reason}` });
        }
        for (const { candidate, error } of r.failures || []) {
          outcome.failed += 1;
          state.notes.set(candidate.id, { kind: "error", text: `Delete failed: ${error}` });
        }
        for (const c of r.not_attempted || []) {
          outcome.notAttempted += 1;
          state.notes.set(c.id, { kind: "warn", text: "Not attempted (YNAB rate limit)" });
        }
        done += batch.length;
        if (r.rate_limited) {
          outcome.rateLimited = true;
          outcome.notAttempted += ids.length - done;
          break;
        }
      }
    } finally {
      state.deleting = false;
      state.candidates = state.candidates.filter((c) => !outcome.deleted.has(c.id));
      state.selected = new Set([...state.selected].filter((id) => !outcome.deleted.has(id)));
      setText(progress, "");
      renderRows();
    }

    const messages = [];
    if (outcome.deleted.size) messages.push(html`<p class="alert alert-ok">Deleted ${plural(outcome.deleted.size, "transaction")}.</p>`);
    if (outcome.failed || outcome.skipped) {
      messages.push(html`<p class="alert alert-warn">${outcome.failed ? `${plural(outcome.failed, "delete")} failed. ` : ""}${
        outcome.skipped ? `${plural(outcome.skipped, "transaction")} no longer qualified and stayed. ` : ""
      }The reasons are shown in the list.</p>`);
    }
    if (outcome.rateLimited) {
      messages.push(html`<p class="alert alert-error" role="alert">YNAB's rate limit stopped the deletion; ${plural(outcome.notAttempted, "transaction")} stayed. Try again later.</p>`);
    }
    if (outcome.stopped) {
      const stale = outcome.stopped.status === 409;
      messages.push(html`<p class="alert alert-error" role="alert">${describeError(outcome.stopped)}${stale ? " Press Scan again." : ""}
        ${outcome.notAttempted ? `${plural(outcome.notAttempted, "transaction")} stayed.` : ""}</p>`);
    }
    setHTML(report, html`${messages}`);
    if (outcome.deleted.size && !outcome.failed && !outcome.stopped && !outcome.rateLimited) {
      toast("ok", `Deleted ${plural(outcome.deleted.size, "transaction")} from YNAB.`);
    } else if (outcome.stopped && outcome.stopped.handled) {
      // the API wrapper already told the user (session expired / page out of date)
    } else {
      toast("warn", "The deletion did not finish completely — see the details above the list.");
    }
  }

  deleteButton.addEventListener("click", async () => {
    if (state.deleting) return;
    const chosen = state.candidates.filter((c) => state.selected.has(c.id));
    if (!chosen.length) return;
    if (expired()) {
      setHTML(report, html`<p class="alert alert-warn" role="alert">The scan expired. Press Scan again before deleting.</p>`);
      return;
    }
    const total = chosen.reduce((sum, c) => sum + c.amount, 0);
    const ok = await confirmDialog({
      title: `Delete ${plural(chosen.length, "transaction")}?`,
      body: html`<p>This permanently removes ${plural(chosen.length, "transaction")} (together ${money(total, currency)})
        from YNAB. There is no undo.</p>${chosen.length > CHUNK_SIZE
          ? html`<p class="mt-2">They are deleted in batches of ${CHUNK_SIZE}.</p>`
          : ""}`,
      confirmLabel: "Yes, delete",
      danger: true,
    });
    if (!ok || state.deleting) return;
    await withBusy(deleteButton, "Deleting…", () => deleteIds(chosen.map((c) => c.id)));
    updateSelection(); // withBusy restored the button; the selection decides
  });
}
