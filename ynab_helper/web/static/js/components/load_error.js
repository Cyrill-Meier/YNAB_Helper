// Inline load failures (js/core/loader.js renderError): "Could not load <what>: <reason>"
// with a Retry button, in a page's error box (role="alert", so it is announced once). When
// the failed request was for new filters the page's `clear()` empties what it showed, so
// rows of the previous query never sit under the new filters; when a quiet refresh of the
// same query failed the content stays and the message says it may be out of date.

import { describeError } from "../core/api.js";
import { show } from "../core/dom.js";
import { html, setHTML } from "../core/html.js";

/** Hide and empty an error box (call it when a load succeeds). */
export function hideLoadError(box) {
  if (box.hidden) return;
  setHTML(box, html``);
  show(box, false);
}

/** Show a load failure with a Retry button in `box`. */
export function showLoadError(box, err, { what, stale = false, retry }) {
  const reason = describeError(err);
  setHTML(
    box,
    html`<div class="alert alert-error flex flex-wrap items-center gap-x-3 gap-y-2" role="alert">
      <p class="min-w-0 flex-1">${stale ? `Could not refresh ${what}: ${reason} What is shown may be out of date.` : `Could not load ${what}: ${reason}`}</p>
      <button type="button" class="btn" data-act="retry">Retry</button>
    </div>`,
  );
  show(box, true);
  box.querySelector('[data-act="retry"]').addEventListener("click", () => {
    hideLoadError(box);
    retry();
  });
}

/**
 * A loader `renderError` for a page: clears the page's content unless it still matches the
 * filters, then shows the failure in `box`.
 */
export function loadErrorHandler(box, what, clear) {
  return (err, { stale, retry }) => {
    if (!stale && clear) clear();
    showLoadError(box, err, { what, stale, retry });
  };
}
