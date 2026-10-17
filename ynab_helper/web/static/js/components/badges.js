// Badges shared by several pages, so a state reads the same everywhere.

import { html } from "../core/html.js";

/** YNAB's cleared state as a badge: reconciled, cleared, or pending (YNAB's "uncleared"). */
export function clearedBadge(cleared) {
  if (cleared === "reconciled") {
    return html`<span class="badge badge-info" title="Cleared and locked by reconciliation"><span aria-hidden="true">🔒</span> reconciled</span>`;
  }
  if (cleared === "cleared") return html`<span class="badge badge-ok">cleared</span>`;
  return html`<span class="badge badge-warn">pending</span>`;
}
