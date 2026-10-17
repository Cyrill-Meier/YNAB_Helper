// "Sync" buttons (POST /api/sync, a forced mirror sync) and the "synced …" status line that
// every list page shows from the read responses' `last_sync_at` / `syncing`.

import { post, reportError } from "../core/api.js";
import { timestamp } from "../core/format.js";
import { withBusy } from "./busy.js";
import { toast } from "./toast.js";

function describe(sync) {
  if (!sync) return "Synced with YNAB.";
  const changed = (sync.tx_changed || 0) + (sync.accounts_changed || 0);
  if (changed === 0) return "Synced with YNAB — no changes.";
  return `Synced with YNAB — ${changed} ${changed === 1 ? "change" : "changes"}.`;
}

/**
 * Wire a sync button: forced sync, a toast, then `onSynced()` (usually: reload the view).
 * A throttled sync (one every 10 s) is reported as info, a YNAB rate limit as an error.
 */
export function bindSyncButton(button, onSynced) {
  button.addEventListener("click", () =>
    withBusy(button, "Syncing…", async () => {
      try {
        const result = await post("/api/sync");
        if (result.synced) toast("ok", describe(result.sync));
        else toast("info", result.message || "Synced a moment ago — try again in a few seconds.");
        if (onSynced) await onSynced();
      } catch (err) {
        reportError(err, "Sync failed");
      }
    }),
  );
}

/** Fill a status line: "Syncing with YNAB…" / "Synced 2026-10-16 18:30 UTC" / nothing yet. */
export function showSyncStatus(element, data) {
  if (!element || !data) return;
  if (data.syncing) element.textContent = "Syncing with YNAB… the page refreshes when done.";
  else if (data.last_sync_at) element.textContent = `Synced with YNAB ${timestamp(data.last_sync_at)}`;
  else element.textContent = "Not synced with YNAB yet.";
}
