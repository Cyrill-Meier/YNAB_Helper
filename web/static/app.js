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
    // category may be set via ?category=… on the URL (e.g. clicking
    // through from the Spending page) — pick that up on load.
    const initialCat = new URLSearchParams(location.search).get("category") || "";
    const state = { q: "", state: "all", sort: "-date",
                    category: initialCat, page: 1, total: 0 };
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
    $("tx-category").addEventListener("change", () => {
      state.category = $("tx-category").value;
      state.page = 1; load();
    });
    $("tx-prev").addEventListener("click", () => {
      if (state.page > 1) { state.page -= 1; load(); }
    });
    $("tx-next").addEventListener("click", () => {
      const last = Math.ceil(state.total / 50);
      if (state.page < last) { state.page += 1; load(); }
    });

    // Populate the category dropdown from /api/categories
    try {
      const cats = await api("GET", "/api/categories");
      const sel = $("tx-category");
      for (const c of cats.items) {
        const opt = document.createElement("option");
        opt.value = c.name;
        opt.textContent = `${c.name} (${c.count})`;
        sel.appendChild(opt);
      }
      if (cats.uncategorized) {
        // Update existing __none__ row label
        const none = sel.querySelector('option[value="__none__"]');
        if (none) none.textContent = `— uncategorized — (${cats.uncategorized})`;
      }
      if (initialCat) sel.value = initialCat;
    } catch (e) {
      // Non-fatal — keep working without the dropdown
      console.warn("categories load failed:", e);
    }

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
        if (state.category) params.set("category", state.category);
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
          const catCell = t.category_name
            ? `<span class="inline-flex items-center px-2 py-0.5 text-xs rounded-full
                            bg-slate-100 text-slate-700 dark:bg-ink-700 dark:text-slate-300">${escHTML(t.category_name)}</span>`
            : `<span class="text-xs text-ink-500 dark:text-slate-500 italic">—</span>`;
          return `<tr>
            <td class="px-4 py-2 whitespace-nowrap text-ink-500 dark:text-slate-400">${escHTML(t.date)}</td>
            <td class="px-4 py-2">${escHTML(t.payee_name)}</td>
            <td class="px-4 py-2 text-right tabular-nums ${cls}">${fmtMoney(t.amount_display)}</td>
            <td class="px-4 py-2">${catCell}</td>
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
  // Spending ───────────────────────────────────────────────────────
  pages.spending = async () => {
    const $ = (id) => document.getElementById(id);
    const state = { months: 6 };
    $("sp-months").value = state.months;
    $("sp-months").addEventListener("change", () => {
      state.months = parseInt($("sp-months").value, 10) || 6;
      load();
    });
    $("sp-refresh").addEventListener("click", async () => {
      const btn = $("sp-refresh");
      btn.disabled = true;
      btn.textContent = "Syncing…";
      try {
        const r = await api("POST", "/api/sync");
        toast(r.synced ? "ok" : "info",
              r.synced ? "Synced from YNAB." : "Already up to date.");
        await load();
      } catch (e) {
        toast("error", e.message);
      } finally {
        btn.disabled = false;
        btn.textContent = "⟳ Sync";
      }
    });
    await load();

    async function load() {
      $("sp-loading").classList.remove("hidden");
      $("sp-empty").classList.add("hidden");
      $("sp-cats").innerHTML = "";
      $("sp-month-bars").innerHTML = "";
      try {
        const d = await api("GET", `/api/spending?months=${state.months}`);
        const months = d.months || [];
        const perMonth = d.per_month_total || [];
        const cats = d.categories || [];

        // Summary cards
        const thisIdx = months.length - 1;
        const prevIdx = months.length - 2;
        const thisTotal = thisIdx >= 0 ? perMonth[thisIdx] : 0;
        const prevTotal = prevIdx >= 0 ? perMonth[prevIdx] : 0;
        const avg = months.length
          ? perMonth.reduce((a, b) => a + b, 0) / months.length : 0;
        $("sp-this-month").textContent = fmtMoney(thisTotal);
        if (prevTotal > 0) {
          const delta = thisTotal - prevTotal;
          const pct = (delta / prevTotal) * 100;
          const cls = delta > 0 ? "text-rose-600 dark:text-rose-400"
                                : "text-emerald-600 dark:text-emerald-400";
          $("sp-this-month-meta").innerHTML =
            `<span class="${cls}">${delta >= 0 ? "+" : ""}${fmtMoney(delta)} (${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%)</span> vs prev`;
        } else {
          $("sp-this-month-meta").textContent = "no prior month";
        }
        $("sp-prev-month").textContent = fmtMoney(prevTotal);
        $("sp-avg").textContent = fmtMoney(avg);
        $("sp-window-meta").textContent = months.length
          ? `${months[0]} → ${months[months.length - 1]}`
          : "";
        $("sp-window-label").textContent = months.length
          ? `${months.length} months`
          : "";

        // Per-month bars (stacked total)
        const peakMonth = perMonth.length ? Math.max(...perMonth) : 0;
        $("sp-month-bars").innerHTML = months.map((m, i) => {
          const v = perMonth[i] || 0;
          const w = peakMonth > 0 ? (v / peakMonth) * 100 : 0;
          return `<div class="flex items-center gap-3">
            <div class="w-20 text-xs text-ink-500 dark:text-slate-400 tabular-nums">${escHTML(m)}</div>
            <div class="flex-1 h-5 rounded bg-slate-100 dark:bg-ink-700 overflow-hidden">
              <div class="h-full bg-brand-500/80" style="width: ${w.toFixed(2)}%"></div>
            </div>
            <div class="w-28 text-right text-sm tabular-nums">${fmtMoney(v)}</div>
          </div>`;
        }).join("");

        // Per-category breakdown
        if (!cats.length) {
          $("sp-empty").classList.remove("hidden");
        } else {
          const peak = Math.max(...cats.map(c => Math.max(...c.by_month)));
          $("sp-cats").innerHTML = cats.map(c => {
            const isUncat = c.category === "(uncategorized)";
            const link = isUncat
              ? `/app/transactions?category=__none__`
              : `/app/transactions?category=${encodeURIComponent(c.category)}`;
            const bars = c.by_month.map((v, i) => {
              const w = peak > 0 ? (v / peak) * 100 : 0;
              return `<div class="flex-1 flex flex-col justify-end" title="${escHTML(months[i])}: ${fmtMoney(v)}">
                <div class="bg-brand-500/70 hover:bg-brand-500 transition-colors rounded-sm" style="height: ${w.toFixed(1)}%"></div>
              </div>`;
            }).join("");
            return `<a href="${link}" class="block px-5 py-3 hover:bg-slate-50 dark:hover:bg-ink-700/40">
              <div class="flex items-center justify-between gap-4">
                <div class="font-medium ${isUncat ? 'italic text-ink-500 dark:text-slate-400' : ''}">${escHTML(c.category)}</div>
                <div class="text-sm tabular-nums">${fmtMoney(c.total)}</div>
              </div>
              <div class="mt-2 flex items-end gap-1 h-10">${bars}</div>
            </a>`;
          }).join("");
        }
      } catch (e) {
        toast("error", e.message);
      } finally {
        $("sp-loading").classList.add("hidden");
      }
    }
  };

  // Subscriptions ───────────────────────────────────────────────────
  pages.subscriptions = async () => {
    const $ = (id) => document.getElementById(id);
    const state = { window: 12 };
    $("sub-window").value = state.window;
    $("sub-window").addEventListener("change", () => {
      state.window = parseInt($("sub-window").value, 10) || 12;
      load();
    });
    $("sub-refresh").addEventListener("click", async () => {
      const btn = $("sub-refresh");
      btn.disabled = true;
      btn.textContent = "Syncing…";
      try {
        const r = await api("POST", "/api/sync");
        toast(r.synced ? "ok" : "info",
              r.synced ? "Synced from YNAB." : "Already up to date.");
        await load();
      } catch (e) {
        toast("error", e.message);
      } finally {
        btn.disabled = false;
        btn.textContent = "⟳ Sync";
      }
    });
    await load();

    async function load() {
      $("sub-loading").classList.remove("hidden");
      $("sub-empty").classList.add("hidden");
      $("sub-tbody").innerHTML = "";
      try {
        const d = await api(
          "GET", `/api/subscriptions?lookback_months=${state.window}`,
        );
        const items = d.items || [];

        // Summary cards
        $("sub-count").textContent = items.length.toLocaleString();
        $("sub-count-meta").textContent =
          items.length ? `over the last ${state.window} months` : "";
        const monthlyMultipliers = {
          weekly: 4.33, biweekly: 2.17, monthly: 1, yearly: 1/12,
        };
        const monthlyRunRate = items.reduce(
          (sum, x) => sum + x.amount_latest * (monthlyMultipliers[x.cadence] || 0),
          0,
        );
        $("sub-monthly").textContent = fmtMoney(monthlyRunRate);
        const changes = items.filter(x => x.price_changed).length;
        $("sub-changes").textContent = changes.toLocaleString();

        if (!items.length) {
          $("sub-empty").classList.remove("hidden");
          return;
        }

        // Spark history — fixed-size cell, area on the right
        const peakHist = Math.max(...items.flatMap(x => x.amount_history));
        $("sub-tbody").innerHTML = items.map(x => {
          const cadenceColors = {
            weekly:   "bg-fuchsia-100 text-fuchsia-700 dark:bg-fuchsia-500/15 dark:text-fuchsia-300",
            biweekly: "bg-violet-100  text-violet-700  dark:bg-violet-500/15  dark:text-violet-300",
            monthly:  "bg-brand-50    text-brand-700   dark:bg-brand-500/15   dark:text-brand-500",
            yearly:   "bg-amber-100   text-amber-700   dark:bg-amber-500/15   dark:text-amber-300",
          };
          const cadenceCls = cadenceColors[x.cadence] || "bg-slate-100 text-slate-700";
          const cats = (x.categories || []).filter(c => c && c !== "(uncategorized)");
          const catCell = cats.length
            ? cats.map(c => `<span class="inline-flex items-center px-2 py-0.5 text-xs rounded-full
                  bg-slate-100 text-slate-700 dark:bg-ink-700 dark:text-slate-300 mr-1">${escHTML(c)}</span>`).join("")
            : `<span class="text-xs text-ink-500 italic">—</span>`;
          // 6 latest amounts as tiny bars
          const last6 = x.amount_history.slice(-6);
          const sparkBars = last6.map(v => {
            const h = peakHist > 0 ? (v / peakHist) * 100 : 0;
            return `<div class="w-1.5 bg-brand-500/70 rounded-sm" style="height: ${h.toFixed(1)}%" title="${fmtMoney(v)}"></div>`;
          }).join("");
          const priceFlag = x.price_changed
            ? `<span class="ml-2 inline-flex items-center px-1.5 py-0.5 text-xs rounded-full
                          bg-rose-100 text-rose-700 dark:bg-rose-500/15 dark:text-rose-300"
                    title="Latest amount differs from earlier charges">±</span>`
            : "";
          return `<tr>
            <td class="px-4 py-2">
              ${escHTML(x.payee_name)}
              ${priceFlag}
            </td>
            <td class="px-4 py-2">
              <span class="inline-flex items-center px-2 py-0.5 text-xs rounded-full ${cadenceCls}">
                ${x.cadence} (~${x.median_days}d)
              </span>
            </td>
            <td class="px-4 py-2">${catCell}</td>
            <td class="px-4 py-2 text-right tabular-nums">${fmtMoney(x.amount_latest)}</td>
            <td class="px-4 py-2 text-right text-xs text-ink-500 dark:text-slate-400 tabular-nums">
              ${x.amount_min === x.amount_max
                ? "—"
                : `${fmtMoney(x.amount_min)} – ${fmtMoney(x.amount_max)}`}
            </td>
            <td class="px-4 py-2">
              <div class="flex items-end gap-0.5 h-6">${sparkBars}</div>
            </td>
            <td class="px-4 py-2 text-xs text-ink-500 dark:text-slate-400 whitespace-nowrap">
              ${escHTML(x.last)} (${x.occurrences}×)
            </td>
          </tr>`;
        }).join("");
      } catch (e) {
        toast("error", e.message);
      } finally {
        $("sub-loading").classList.add("hidden");
      }
    }
  };

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
