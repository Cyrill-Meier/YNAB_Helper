// Toast notifications (F102): rendered into #toasts, a polite live region present from page
// load; error and warning toasts carry role="alert" so screen readers announce them at once.
// Text only (textContent). Each toast has a close button; timers pause on hover and focus.

const CLASSES = {
  ok: "toast ok pointer-events-auto",
  info: "toast info pointer-events-auto",
  warn: "toast warn pointer-events-auto",
  error: "toast error pointer-events-auto",
};
const DEFAULT_TTL = { ok: 5000, info: 5000, warn: 9000, error: 10000 };
const MAX_TOASTS = 5;

/**
 * Show a toast. `kind`: ok | info | warn | error. `ttl` in ms (0 = until dismissed).
 * `action`: optional {label, onClick} button (e.g. "Reload").
 */
export function toast(kind, message, { ttl, action } = {}) {
  const region = document.getElementById("toasts");
  if (!region) return null;
  const level = kind in CLASSES ? kind : "info";
  const element = document.createElement("div");
  element.className = CLASSES[level];
  if (level === "error" || level === "warn") element.setAttribute("role", "alert");

  const text = document.createElement("p");
  text.className = "flex-1";
  text.textContent = String(message ?? "");
  element.append(text);

  if (action && typeof action.onClick === "function") {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "shrink-0 font-medium underline underline-offset-2";
    button.textContent = action.label || "OK";
    button.addEventListener("click", () => action.onClick());
    element.append(button);
  }

  const close = document.createElement("button");
  close.type = "button";
  close.className = "shrink-0 rounded px-1 leading-none opacity-70 hover:opacity-100";
  close.setAttribute("aria-label", "Dismiss notification");
  close.textContent = "×";
  element.append(close);

  let timer = null;
  const lifetime = ttl ?? DEFAULT_TTL[level];
  const dismiss = () => {
    clearTimeout(timer);
    element.classList.add("leaving");
    setTimeout(() => element.remove(), 220);
  };
  const arm = () => {
    clearTimeout(timer);
    if (lifetime > 0) timer = setTimeout(dismiss, lifetime);
  };
  close.addEventListener("click", dismiss);
  element.addEventListener("mouseenter", () => clearTimeout(timer));
  element.addEventListener("mouseleave", arm);
  element.addEventListener("focusin", () => clearTimeout(timer));
  element.addEventListener("focusout", arm);

  while (region.children.length >= MAX_TOASTS) region.firstElementChild.remove();
  region.append(element);
  arm();
  return { dismiss };
}
