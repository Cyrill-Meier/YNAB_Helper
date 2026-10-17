// Safe HTML building (DESIGN §9.4, F91/F96 for the web).
//
//   html`<td>${payee}</td>`      every interpolation is escaped (text and attribute safe;
//                                 always quote attribute values)
//   html`<ul>${items.map(i => html`<li>${i}</li>`)}</ul>`   nested html`` is kept as is
//   raw(markup)                  escape hatch for trusted constant markup (icons only)
//   setHTML(element, fragment)   the ONLY place that assigns innerHTML; it accepts nothing
//                                but a SafeHtml produced by html`` or raw()
//
// Defence in depth: a value interpolated into a URL attribute (href, src, action, data, …)
// must be a same-site relative URL ("/…", "?…", "#…") without whitespace, control
// characters or backslashes (browsers strip tabs/newlines and read "\\" as "/", so
// "/\t/evil.example" would leave the site), otherwise "#" is used. Interpolating into an
// event-handler, style or srcdoc attribute throws (the CSP would block the first two anyway;
// srcdoc is parsed as a document after entity decoding).

const ESCAPES = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;",
  "`": "&#96;",
};
const ESCAPE_RE = /[&<>"'`]/g;
const URL_ATTR_RE =
  /\s(?:href|src|action|formaction|xlink:href|poster|srcset|ping|data|cite|background)\s*=\s*["']?$/i;
const FORBIDDEN_ATTR_RE = /\s(?:on[a-z]+|style|srcdoc)\s*=\s*["']?$/i;
// C0 controls, space, DEL and backslash: anything a URL parser strips or reinterprets.
const UNSAFE_URL_CHARS_RE = /[\u0000-\u0020\u007f\\]/;

class SafeHtml {
  #markup;

  constructor(markup) {
    this.#markup = markup;
  }

  toString() {
    return this.#markup;
  }
}

/** Escape a value for HTML text or a quoted attribute value. */
export function escapeHtml(value) {
  return String(value).replace(ESCAPE_RE, (ch) => ESCAPES[ch]);
}

/** True for a fragment produced by html`` or raw(). */
export function isSafeHtml(value) {
  return value instanceof SafeHtml;
}

/** Trusted, constant markup (e.g. an SVG icon). Never pass user or server data. */
export function raw(markup) {
  return new SafeHtml(String(markup));
}

/** Whether `url` is a same-site relative URL that is safe in an href/src attribute. */
export function isSafeUrl(url) {
  if (typeof url !== "string" || url === "" || UNSAFE_URL_CHARS_RE.test(url)) return false;
  if (url.startsWith("#") || url.startsWith("?")) return true;
  return url.startsWith("/") && !url.startsWith("//");
}

function render(value) {
  if (value === null || value === undefined || value === false) return "";
  if (value instanceof SafeHtml) return value.toString();
  if (Array.isArray(value)) return value.map(render).join("");
  return escapeHtml(value);
}

/** Tagged template: escapes every interpolation, returns a SafeHtml fragment. */
export function html(strings, ...values) {
  let out = strings[0];
  for (let i = 0; i < values.length; i += 1) {
    const before = strings[i];
    let value = values[i];
    if (FORBIDDEN_ATTR_RE.test(before)) {
      throw new Error("html``: interpolation into an event-handler or style attribute");
    }
    if (URL_ATTR_RE.test(before) && !(value instanceof SafeHtml)) {
      value = isSafeUrl(value) ? value : "#";
    }
    out += render(value) + strings[i + 1];
  }
  return new SafeHtml(out);
}

/** Replace the children of `element` with a SafeHtml fragment. */
export function setHTML(element, fragment) {
  if (!(fragment instanceof SafeHtml)) {
    throw new TypeError("setHTML() needs a fragment built with html`` (or raw())");
  }
  element.innerHTML = fragment.toString();
}
