// Page state in the address bar (F101): filters and pagination survive reload, Back and
// bookmarks. Updates use history.replaceState, so filtering does not flood the history.

/** The current query parameters. */
export function readParams() {
  return new URLSearchParams(window.location.search);
}

/** A comma-separated list parameter as unique, non-empty strings. */
export function readList(params, name) {
  const seen = new Set();
  for (const piece of (params.get(name) || "").split(",")) {
    const item = piece.trim();
    if (item) seen.add(item);
  }
  return [...seen];
}

/** An integer parameter within [min, max], or `fallback`. */
export function readInt(params, name, fallback, min, max) {
  const value = params.get(name);
  if (value === null || !/^\d+$/.test(value)) return fallback;
  const number = Number(value);
  return number >= min && number <= max ? number : fallback;
}

/** One of `allowed`, or `fallback`. */
export function readChoice(params, name, allowed, fallback) {
  const value = params.get(name);
  return value !== null && allowed.includes(value) ? value : fallback;
}

/** A YYYY-MM-DD parameter, or "". */
export function readDate(params, name) {
  const value = params.get(name) || "";
  return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : "";
}

/**
 * Write `state` into the address bar. Values equal to `defaults[key]` (or empty) are left
 * out; arrays are joined with commas.
 */
export function writeParams(state, defaults = {}) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(state)) {
    const text = Array.isArray(value) ? value.join(",") : String(value ?? "");
    const fallback = defaults[key];
    const defaultText = Array.isArray(fallback) ? fallback.join(",") : String(fallback ?? "");
    if (text !== "" && text !== defaultText) params.set(key, text);
  }
  const query = params.toString();
  const url = `${window.location.pathname}${query ? `?${query}` : ""}${window.location.hash}`;
  if (url !== `${window.location.pathname}${window.location.search}${window.location.hash}`) {
    window.history.replaceState(window.history.state, "", url);
  }
}

/** "/app/transactions?category=…" from a path and parameters (empty values dropped). */
export function pageUrl(path, params = {}) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    const text = Array.isArray(value) ? value.join(",") : String(value ?? "");
    if (text !== "") query.set(key, text);
  }
  const text = query.toString();
  return text ? `${path}?${text}` : path;
}
