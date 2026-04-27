// Revolut → YNAB web UI — single-file vanilla JS app
// =========================================================================
// Layout: every page renders skeletal HTML in its template, then calls
//   RYNAB.boot('<page-name>')
// from a deferred <script> at the bottom. boot() dispatches to the
// per-page initializer which fetches JSON from /api/* and populates
// the DOM. Shared concerns (CSRF, fetch wrapper, toasts, theme
// toggle) live up here.
"use strict";

(function () {
  const csrf = (window.RYNAB && window.RYNAB.csrfToken) || "";

  // ── Fetch wrapper ────────────────────────────────────────────────
  async function api(method, url, body, opts = {}) {
    const init = {
      method,
      credentials: "same-origin",
      headers: { Accept: "application/json", "X-CSRF-Token": csrf },
    };
    if (body !== undefined && body !== null) {
      if (body instanceof FormData) {
        init.body = body;
      } else {
        init.headers["Content-Type"] = "application/json";
        init.body = JSON.stringify(body);
      }
    }
    if (opts.signal) init.signal = opts.signal;
    const r = await fetch(url, init);
    let data;
    const ct = r.headers.get("content-type") || "";
    if (ct.includes("application/json")) {
      data = await r.json().catch(() => ({}));
    } else {
      data = await r.text();
    }
    if (!r.ok) {
      const message = (data && data.message) || r.statusText || "Request failed";
      const err = new Error(message);
      err.status = r.status;
      err.data = data;
      throw err;
    }
    return data;
  }

  // ── Toasts ──────────────────────────────────────────────────────
  function toast(kind, msg, ttl = 4000) {
    const wrap = document.getElementById("toasts");
    if (!wrap) return console.log(`[${kind}] ${msg}`);
    const el = document.createElement("div");
    el.className = `toast ${kind}`;
    el.textContent = msg;
    wrap.appendChild(el);
    setTimeout(() => {
      el.style.transition = "opacity 0.2s, transform 0.2s";
      el.style.opacity = "0";
      el.style.transform = "translateY(-4px)";
      setTimeout(() => el.remove(), 220);
    }, ttl);
  }

  // ── Helpers ─────────────────────────────────────────────────────
  function fmtMoney(n, currency) {
    if (n === null || n === undefined || Number.isNaN(n)) return "—";
    const opts = { minimumFractionDigits: 2, maximumFractionDigits: 2 };
    let s;
    try {
      s = new Intl.NumberFormat(undefined,
        currency && currency !== "?"
          ? { ...opts, style: "currency", currency }
          : opts).format(n);
    } catch {
      s = n.toLocaleString(undefined, opts);
    }
    return s;
  }
  function fmtTimestamp(s) {
    if (!s) return "—";
    try { return new Date(s).toLocaleString(); } catch { return s; }
  }
  function fmtBytes(n) {
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    return `${(n / 1024 / 1024).toFixed(2)} MB`;
  }
  function escHTML(s) {
    if (s == null) return "";
    return String(s).replace(/[&<>"']/g, c =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  }

  // ── Theme toggle ────────────────────────────────────────────────
  document.addEventListener("click", (e) => {
    const t = e.target.closest("#theme-toggle");
    if (!t) return;
    const dark = document.documentElement.classList.toggle("dark");
    localStorage.setItem("theme", dark ? "dark" : "light");
  });

  // ──────────────────────────────────────────────────────────────────
  // Page initializers
  // ──────────────────────────────────────────────────────────────────
  const pages = {};

  // Dashboard ───────────────────────────────────────────────────────
  pages.dashboard = async () => {
    const $ = (id) => document.getElementById(id);
    const refresh = $("dash-refresh");
    refresh.addEventListener("click", load);
    await load();

    async function load() {
      $("dash-subtitle").textContent = "Loading account…";
      try {
        const d = await api("GET", "/api/dashboard");
        $("dash-subtitle").textContent =
          `Last updated ${new Date().toLocaleTimeString()}`;
        $("card-balance").textContent = fmtMoney(d.balance, d.currency);
        $("card-balance-meta").textContent = d.balance == null
          ? "Could not reach YNAB"
          : (d.account_name || "");
        $("card-tracked").textContent = d.stats.total.toLocaleString();
        $("card-tracked-meta").textContent =
          d.stats.last_import
            ? `Last import: ${fmtTimestamp(d.stats.last_import)}`
            : "No imports yet";
        $("card-pending").textContent = d.stats.uncleared.toLocaleString();
        if (d.last_csv) {
          $("card-csv").textContent = d.last_csv.name;
          $("card-csv-meta").textContent =
            `${fmtBytes(d.last_csv.size)} · ${fmtTimestamp(d.last_csv.mtime * 1000)}`;
        } else {
          $("card-csv").textContent = "—";
          $("card-csv-meta").textContent = "Upload your first CSV";
        }
        $("meta-budget").textContent = d.budget_name || "—";
        $("meta-account").textContent = d.account_name || "—";
        $("meta-auto").textContent = d.auto_approve ? "On" : "Off";
        $("meta-range").textContent =
          (d.stats.first_date && d.stats.last_date)
            ? `${d.stats.first_date} → ${d.stats.last_date}`
            : "—";
      } catch (e) {
        toast("error", `Couldn't load dashboard: ${e.message}`);
      }
    }
  };

  // Transactions ────────────────────────────────────────────────────
  pages.transactions = async () => {
    const $ = (id) => document.getElementById(id);
    const state = { q: "", state: "all", sort: "-date", page: 1, total: 0 };
    let timer = null;

    function debounce(fn, ms = 250) {
      return (...args) => {
        clearTimeout(timer);
        timer = setTimeout(() => fn(...args), ms);
      };
    }
    $("tx-search").addEventListener("input", debounce(() => {
      state.q = $("tx-search").value.trim();
      state.page = 1;
      load();
    }));
    $("tx-state").addEventListener("change", () => {
      state.state = $("tx-state").value;
      state.page = 1; load();
    });
    $("tx-sort").addEventListener("change", () => {
      state.sort = $("tx-sort").value; load();
    });
    $("tx-prev").addEventListener("click", () => {
      if (state.page > 1) { state.page -= 1; load(); }
    });
    $("tx-next").addEventListener("click", () => {
      const last = Math.ceil(state.total / 50);
      if (state.page < last) { state.page += 1; load(); }
    });

    await load();

    async function load() {
      $("tx-loading").classList.remove("hidden");
      $("tx-empty").classList.add("hidden");
      $("tx-tbody").innerHTML = "";
      try {
        const params = new URLSearchParams({
          q: state.q, state: state.state, sort: state.sort,
          page: String(state.page), page_size: "50",
        });
        const d = await api("GET", `/api/transactions?${params}`);
        state.total = d.total;
        const rows = d.items.map(t => {
          const cls = t.amount < 0 ? "text-rose-600 dark:text-rose-400"
                                   : "text-emerald-600 dark:text-emerald-400";
          const stateBadge = t.cleared === "cleared"
            ? `<span class="inline-flex items-center px-2 py-0.5 text-xs rounded-full
                          bg-emerald-100 text-emerald-700 dark:bg-emerald-500/15 dark:text-emerald-300">cleared</span>`
            : `<span class="inline-flex items-center px-2 py-0.5 text-xs rounded-full
                          bg-amber-100 text-amber-800 dark:bg-amber-500/15 dark:text-amber-300">pending</span>`;
          return `<tr>
            <td class="px-4 py-2 whitespace-nowrap text-ink-500 dark:text-slate-400">${escHTML(t.date)}</td>
            <td class="px-4 py-2">${escHTML(t.payee_name)}</td>
            <td class="px-4 py-2 text-right tabular-nums ${cls}">${fmtMoney(t.amount_display)}</td>
            <td class="px-4 py-2 text-ink-500 dark:text-slate-400 truncate max-w-[24ch]">${escHTML(t.memo)}</td>
            <td class="px-4 py-2">${stateBadge}</td>
          </tr>`;
        }).join("");
        $("tx-tbody").innerHTML = rows;
        if (!d.items.length) $("tx-empty").classList.remove("hidden");
        const start = (d.page - 1) * d.page_size + 1;
        const end = Math.min(d.total, d.page * d.page_size);
        $("tx-summary").textContent = d.total
          ? `${start.toLocaleString()}–${end.toLocaleString()} of ${d.total.toLocaleString()}`
          : "No transactions";
        $("tx-prev").disabled = d.page <= 1;
        $("tx-next").disabled = d.page * d.page_size >= d.total;
      } catch (e) {
        toast("error", `Couldn't load transactions: ${e.message}`);
      } finally {
        $("tx-loading").classList.add("hidden");
      }
    }
  };

  // Reconcile ───────────────────────────────────────────────────────
  pages.reconcile = async () => {
    const $ = (id) => document.getElementById(id);
    try {
      const d = await api("GET", "/api/dashboard");
      $("rec-csv").textContent = d.last_csv ? d.last_csv.name : "(no CSV uploaded yet)";
    } catch { /* not fatal */ }

    $("rec-run").addEventListener("click", async () => {
      const btn = $("rec-run");
      btn.disabled = true;
      btn.innerHTML = `<span class="spinner inline-block"></span> <span class="ml-2">Reconciling…</span>`;
      $("rec-result").classList.add("hidden");
      try {
        const r = await api("POST", "/api/reconcile");
        $("rec-output").textContent = r.stdout || "(no output)";
        $("rec-result").classList.remove("hidden");
        $("rec-empty").classList.add("hidden");
        toast("ok", `Reconcile finished against ${r.csv_name}`);
      } catch (e) {
        $("rec-output").textContent =
          (e.data && e.data.stdout ? e.data.stdout + "\n\n" : "") +
          `Error: ${e.message}`;
        $("rec-result").classList.remove("hidden");
        toast("error", e.message);
      } finally {
        btn.disabled = false;
        btn.textContent = "Run reconcile";
      }
    });
  };

  // Dedupe ──────────────────────────────────────────────────────────
  pages.dedupe = async () => {
    const $ = (id) => document.getElementById(id);
    const state = { items: [], selected: new Set() };

    $("dd-scan").addEventListener("click", scan);
    $("dd-select-all").addEventListener("change", e => {
      if (e.target.checked) state.selected = new Set(state.items.map(o => o.id));
      else state.selected.clear();
      render();
    });
    $("dd-deselect").addEventListener("click", () => {
      state.selected.clear(); render();
    });
    $("dd-delete").addEventListener("click", () => {
      if (!state.selected.size) return;
      $("dd-modal-count").textContent = state.selected.size;
      $("dd-modal").classList.remove("hidden");
    });
    $("dd-modal-cancel").addEventListener("click", () => {
      $("dd-modal").classList.add("hidden");
    });
    $("dd-modal-confirm").addEventListener("click", deleteSelected);

    async function scan() {
      const btn = $("dd-scan");
      btn.disabled = true;
      btn.innerHTML = `<span class="spinner inline-block"></span> <span class="ml-2">Scanning…</span>`;
      $("dd-empty").classList.add("hidden");
      $("dd-results").classList.add("hidden");
      $("dd-meta").classList.add("hidden");
      $("dd-initial").classList.add("hidden");
      try {
        const r = await api("GET", "/api/dedupe/scan");
        state.items = r.orphans || [];
        state.selected.clear();
        $("dd-range").textContent = `${r.start_date} → ${r.end_date}`;
        $("dd-csv-count").textContent = (r.csv_count || 0).toLocaleString();
        $("dd-ynab-count").textContent = (r.ynab_count_in_range || 0).toLocaleString();
        $("dd-orphan-count").textContent = state.items.length.toLocaleString();
        $("dd-meta").classList.remove("hidden");
        if (!state.items.length) {
          $("dd-empty").classList.remove("hidden");
        } else {
          render();
          $("dd-results").classList.remove("hidden");
        }
      } catch (e) {
        toast("error", e.message);
        $("dd-initial").classList.remove("hidden");
      } finally {
        btn.disabled = false;
        btn.textContent = "Scan";
      }
    }

    function render() {
      const tb = $("dd-tbody");
      const rows = state.items.map(o => {
        const cls = o.amount < 0 ? "text-rose-600 dark:text-rose-400"
                                 : "text-emerald-600 dark:text-emerald-400";
        const checked = state.selected.has(o.id) ? "checked" : "";
        return `<tr>
          <td class="px-4 py-2">
            <input type="checkbox" data-id="${escHTML(o.id)}" ${checked}
                   class="rounded border-slate-300 dark:border-ink-600 text-brand-500 focus:ring-brand-500">
          </td>
          <td class="px-4 py-2 whitespace-nowrap text-ink-500 dark:text-slate-400">${escHTML(o.date)}</td>
          <td class="px-4 py-2">${escHTML(o.payee_name)}</td>
          <td class="px-4 py-2 text-right tabular-nums ${cls}">${fmtMoney(o.amount)}</td>
          <td class="px-4 py-2 text-ink-500 dark:text-slate-400 truncate max-w-[24ch]">${escHTML(o.memo)}</td>
          <td class="px-4 py-2 text-ink-500 dark:text-slate-400">${escHTML(o.cleared || "")}</td>
        </tr>`;
      }).join("");
      tb.innerHTML = rows;
      tb.querySelectorAll("input[type=checkbox]").forEach(cb => {
        cb.addEventListener("change", () => {
          if (cb.checked) state.selected.add(cb.dataset.id);
          else state.selected.delete(cb.dataset.id);
          $("dd-selected-count").textContent = `${state.selected.size} selected`;
          $("dd-delete").disabled = state.selected.size === 0;
          $("dd-select-all").checked =
            state.selected.size > 0 && state.selected.size === state.items.length;
        });
      });
      $("dd-selected-count").textContent = `${state.selected.size} selected`;
      $("dd-delete").disabled = state.selected.size === 0;
      $("dd-select-all").checked =
        state.selected.size > 0 && state.selected.size === state.items.length;
    }

    async function deleteSelected() {
      const ids = [...state.selected];
      const btn = $("dd-modal-confirm");
      btn.disabled = true;
      btn.innerHTML = `<span class="spinner inline-block"></span> <span class="ml-2">Deleting…</span>`;
      try {
        const r = await api("POST", "/api/dedupe/delete", { ids });
        toast("ok", `Deleted ${r.deleted} of ${r.requested} transaction(s).`);
        state.items = state.items.filter(o => !state.selected.has(o.id) || (r.failures || []).some(f => f.id === o.id));
        state.selected.clear();
        $("dd-orphan-count").textContent = state.items.length.toLocaleString();
        if (!state.items.length) {
          $("dd-results").classList.add("hidden");
          $("dd-empty").classList.remove("hidden");
        } else {
          render();
        }
        if (r.failures && r.failures.length) {
          toast("warn", `${r.failures.length} delete(s) failed — see logs.`);
        }
      } catch (e) {
        toast("error", e.message);
      } finally {
        btn.disabled = false;
        btn.textContent = "Yes, delete";
        $("dd-modal").classList.add("hidden");
      }
    }
  };

  // Settings ────────────────────────────────────────────────────────
  pages.settings = async () => {
    const $ = (id) => document.getElementById(id);
    const toggle = $("set-auto");
    function setToggle(on) {
      toggle.setAttribute("aria-checked", on ? "true" : "false");
    }
    try {
      const d = await api("GET", "/api/dashboard");
      setToggle(!!d.auto_approve);
      $("set-budget").textContent = d.budget_name || "—";
      $("set-account").textContent = d.account_name || "—";
    } catch (e) {
      toast("error", e.message);
    }
    toggle.addEventListener("click", async () => {
      const desired = toggle.getAttribute("aria-checked") !== "true";
      setToggle(desired);
      try {
        await api("POST", "/api/settings", { auto_approve: desired });
        toast("ok", `Auto-approve ${desired ? "enabled" : "disabled"}.`);
      } catch (e) {
        setToggle(!desired);  // revert on failure
        toast("error", e.message);
      }
    });
  };

  // Upload ──────────────────────────────────────────────────────────
  pages.upload = async () => {
    const $ = (id) => document.getElementById(id);
    const drop = $("up-drop");
    const input = $("up-input");

    drop.addEventListener("click", () => input.click());
    ["dragenter", "dragover"].forEach(ev =>
      drop.addEventListener(ev, e => {
        e.preventDefault(); e.stopPropagation();
        drop.classList.add("ring-2", "ring-brand-500");
      })
    );
    ["dragleave", "drop"].forEach(ev =>
      drop.addEventListener(ev, e => {
        e.preventDefault(); e.stopPropagation();
        drop.classList.remove("ring-2", "ring-brand-500");
      })
    );
    drop.addEventListener("drop", e => {
      const f = e.dataTransfer.files[0];
      if (f) upload(f);
    });
    input.addEventListener("change", () => {
      const f = input.files[0];
      if (f) upload(f);
    });

    async function upload(file) {
      const fd = new FormData();
      fd.append("file", file);
      drop.classList.add("opacity-60");
      $("up-result").classList.add("hidden");
      try {
        const r = await api("POST", "/api/upload", fd);
        toast("ok", `Imported ${r.transaction_count} txns from ${r.filename}.`);
        $("up-result-name").textContent = r.filename;
        $("up-output").textContent = (r.stdout || "").trim() || "(no output)";
        $("up-result").classList.remove("hidden");
      } catch (e) {
        toast("error", e.message);
      } finally {
        drop.classList.remove("opacity-60");
        input.value = "";
      }
    }
  };

  // ── Public API ──────────────────────────────────────────────────
  window.RYNAB = Object.assign(window.RYNAB || {}, {
    api, toast, boot,
  });

  function boot(name) {
    const fn = pages[name];
    if (!fn) return console.warn(`No initializer for page "${name}"`);
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", () => fn().catch(err => {
        console.error(err); toast("error", err.message);
      }));
    } else {
      fn().catch(err => { console.error(err); toast("error", err.message); });
    }
  }
})();
