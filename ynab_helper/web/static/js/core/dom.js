// Small DOM helpers. Text goes in through textContent; markup only through
// setHTML(element, html`…`) from ./html.js.

/** The element with `id`; throws when the template lacks it (caught in development). */
export function byId(id) {
  const element = document.getElementById(id);
  if (!element) throw new Error(`#${id} is missing from the page`);
  return element;
}

/** Set an element's text (null/undefined → empty). */
export function setText(element, value) {
  element.textContent = value === null || value === undefined ? "" : String(value);
}

/** Show or hide an element with the `hidden` attribute. */
export function show(element, visible = true) {
  element.hidden = !visible;
}

/** Call `fn` once `ms` passed without another call; `.flush()` runs a pending call now. */
export function debounce(fn, ms) {
  let timer = null;
  let pending = null;
  const debounced = (...args) => {
    pending = args;
    clearTimeout(timer);
    timer = setTimeout(() => {
      timer = null;
      const call = pending;
      pending = null;
      fn(...call);
    }, ms);
  };
  debounced.flush = () => {
    if (timer === null) return;
    clearTimeout(timer);
    timer = null;
    const call = pending;
    pending = null;
    fn(...call);
  };
  debounced.cancel = () => {
    clearTimeout(timer);
    timer = null;
    pending = null;
  };
  return debounced;
}

/** A spinner element (decorative). */
export function spinner() {
  const element = document.createElement("span");
  element.className = "spinner";
  element.setAttribute("aria-hidden", "true");
  return element;
}
