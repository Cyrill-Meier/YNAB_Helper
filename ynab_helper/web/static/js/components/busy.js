// Busy buttons: while `work` runs the button is disabled, marked aria-busy and shows a
// spinner with `label`. A second click while busy does nothing (in-flight guard, F103).

import { spinner } from "../core/dom.js";

/** True while `button` runs an operation started with withBusy(). */
export function isBusy(button) {
  return button.getAttribute("aria-busy") === "true";
}

/**
 * Run `work()` with `button` in its busy state; resolves with work's result, or undefined
 * (without calling work) when the button is already busy.
 */
export async function withBusy(button, label, work) {
  if (isBusy(button)) return undefined;
  const saved = [...button.childNodes].map((node) => node.cloneNode(true));
  const wasDisabled = button.disabled;
  const hadFocus = document.activeElement === button;
  button.setAttribute("aria-busy", "true");
  button.disabled = true;
  const text = document.createElement("span");
  text.textContent = label;
  button.replaceChildren(spinner(), text);
  try {
    return await work();
  } finally {
    button.replaceChildren(...saved);
    button.removeAttribute("aria-busy");
    button.disabled = wasDisabled;
    if (hadFocus && !button.disabled && document.activeElement === document.body) button.focus();
  }
}
