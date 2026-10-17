// One loader per list or view (F63): every load aborts the previous request and carries a
// request id; a response that is not the newest is dropped, so a slow earlier request can
// never overwrite the results for the current filters.
//
// Reads come from the server's mirror of YNAB and may report `syncing: true` (a background
// sync started). The loader then refreshes quietly a few times until the sync is done, so
// fresh data shows up without a click.
//
// A failed load never leaves the previous query's results on screen as if they were current:
// with `renderError` the page is told whether what it shows still belongs to the visible
// filters (`stale: true` — a quiet refresh of the same query failed) or not (`stale: false` —
// the page must clear it), and gets a `retry` callback for an inline "Retry" button.

import { ApiError, isAbort, reportError } from "./api.js";

const FOLLOW_DELAY_MS = 2500;
const MAX_FOLLOWS = 8;

/**
 * @param {object} options
 * @param {(signal: AbortSignal) => Promise<any>} options.fetch  performs the request
 * @param {(data: any) => void} options.render                   renders the newest response
 * @param {(busy: boolean) => void} [options.onLoading]          shows/hides a spinner
 * @param {(err: Error, info: {stale: boolean, retry: () => Promise<void>}) => void} [options.renderError]
 *        shows the failure in the page (replaces the default toast)
 * @param {(err: Error) => void} [options.onError]               default: toast (without renderError)
 * @param {boolean} [options.followSync=true]                    refresh while `syncing`
 */
export function createLoader({
  fetch: fetcher,
  render,
  onLoading,
  renderError,
  onError,
  followSync = true,
}) {
  let controller = null;
  let sequence = 0;
  let follows = 0;
  let followTimer = null;
  let spinnerShown = false;
  let showingCurrent = false; // the page shows a successful render of the current query

  function setSpinner(on) {
    if (on === spinnerShown) return;
    spinnerShown = on;
    if (onLoading) onLoading(on);
  }

  async function load({ quiet = false, follow = false } = {}) {
    if (controller) controller.abort();
    clearTimeout(followTimer);
    if (!follow) follows = 0;
    const id = ++sequence;
    const current = new AbortController();
    controller = current;
    if (!quiet) setSpinner(true);
    try {
      const data = await fetcher(current.signal);
      if (id !== sequence) return; // superseded by a newer load
      render(data);
      showingCurrent = true;
      if (followSync && data && data.syncing === true && follows < MAX_FOLLOWS) {
        follows += 1;
        followTimer = setTimeout(() => load({ quiet: true, follow: true }), FOLLOW_DELAY_MS);
      }
    } catch (err) {
      if (isAbort(err) || id !== sequence) return;
      // Only a quiet refresh (same query) leaves results that still match the filters.
      const stale = quiet && showingCurrent;
      showingCurrent = stale;
      if (renderError) {
        if (!(err instanceof ApiError)) console.error(err);
        renderError(err, { stale, retry: () => load() });
        if (onError) onError(err);
      } else if (onError) {
        onError(err);
      } else {
        reportError(err);
      }
    } finally {
      if (id === sequence) {
        controller = null;
        setSpinner(false);
      }
    }
  }

  function abort() {
    clearTimeout(followTimer);
    sequence += 1;
    if (controller) controller.abort();
    controller = null;
    setSpinner(false);
  }

  return { load, abort };
}
