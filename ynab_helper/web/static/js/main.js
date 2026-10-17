// Entry module of every app page (loaded by base.html as type="module").
// Wires the shared chrome (navigation) and boots the page module named by
// <body data-page="…">. Page modules are loaded from this fixed table only.

import { reportError } from "./core/api.js";
import { initNav } from "./components/nav.js";

const PAGES = {
  dashboard: () => import("./pages/dashboard.js"),
  accounts: () => import("./pages/accounts.js"),
  transactions: () => import("./pages/transactions.js"),
  spending: () => import("./pages/spending.js"),
  subscriptions: () => import("./pages/subscriptions.js"),
  upload: () => import("./pages/upload.js"),
  reconcile: () => import("./pages/reconcile.js"),
  dedupe: () => import("./pages/dedupe.js"),
  settings: () => import("./pages/settings.js"),
};

window.addEventListener("unhandledrejection", (event) => {
  reportError(event.reason);
});

async function boot() {
  initNav();
  const page = document.body.dataset.page || "";
  const load = Object.hasOwn(PAGES, page) ? PAGES[page] : null;
  if (!load) return;
  const module = await load();
  await module.init();
}

boot().catch((err) => reportError(err, "The page could not start"));
