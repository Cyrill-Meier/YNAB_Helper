// Tiny SVG bar charts. Sizes are SVG attributes (not inline styles: the CSP forbids them);
// colours come from CSS classes in input.css. Charts are decorative (aria-hidden): the
// numbers are always shown as text next to them.

import { html } from "./html.js";

function share(value, peak) {
  if (typeof value !== "number" || !(peak > 0) || value <= 0) return 0;
  return Math.min(1, value / peak);
}

/** A horizontal bar: `value` relative to `peak` (track + fill). */
export function hbar(value, peak, { muted = false } = {}) {
  const width = (share(value, peak) * 100).toFixed(2);
  return html`<svg viewBox="0 0 100 10" preserveAspectRatio="none" class="block h-5 w-full overflow-hidden rounded" aria-hidden="true" focusable="false">
    <rect class="bar-track" x="0" y="0" width="100" height="10"/>
    <rect class="${muted ? "bar-fill-muted" : "bar-fill"}" x="0" y="0" width="${width}" height="10"/>
  </svg>`;
}

/**
 * Vertical bars, one per value, scaled to `peak` (default: the largest value). `labels`
 * become hover titles. `mutedFrom` marks the bars from that index on (e.g. month-to-date).
 * `slots` fixes the number of bar positions (bars are right-aligned, the newest last), so
 * rows with fewer values keep the same bar width as full rows.
 */
export function vbars(
  values,
  { peak, labels = [], mutedFrom = Infinity, latestClass = null, height = 40, slots = 0 } = {},
) {
  const top = peak ?? Math.max(0, ...values.filter((v) => typeof v === "number"));
  const step = 10;
  const total = Math.max(slots, values.length, 1);
  const offset = total - values.length;
  const bars = values.map((value, i) => {
    const h = Math.max(share(value, top) * height, value > 0 ? 1 : 0);
    let cls = i >= mutedFrom ? "bar-fill-muted" : "bar-fill-hover";
    if (latestClass && i === values.length - 1) cls = latestClass;
    return html`<rect class="${cls}" x="${(offset + i) * step + 1}" y="${(height - h).toFixed(2)}" width="${step - 2}" height="${h.toFixed(2)}" rx="1"><title>${labels[i] ?? ""}</title></rect>`;
  });
  return html`<svg viewBox="0 0 ${total * step} ${height}" preserveAspectRatio="none" class="block h-full w-full" aria-hidden="true" focusable="false">${bars}</svg>`;
}
