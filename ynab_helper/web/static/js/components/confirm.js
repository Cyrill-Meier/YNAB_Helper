// Confirmation dialog (F102): the native <dialog id="confirm-dialog"> of base.html opened
// with showModal() — focus moves into it and is trapped, Escape cancels, the page behind is
// inert. Destructive confirmations start with focus on "Cancel".

import { isSafeHtml, setHTML } from "../core/html.js";

const OK_CLASSES = { primary: "btn-primary", danger: "btn-danger" };
let open = false;

/**
 * Ask for confirmation. `body` is text or an html`` fragment. Resolves true only when the
 * user pressed the confirm button.
 */
export function confirmDialog({ title, body = "", confirmLabel = "Confirm", danger = false }) {
  const dialog = document.getElementById("confirm-dialog");
  if (!dialog || typeof dialog.showModal !== "function") {
    // Very old browsers: the plain confirm() box (text only).
    const text = isSafeHtml(body) ? "" : String(body);
    return Promise.resolve(window.confirm(text ? `${title}\n\n${text}` : title));
  }
  if (open) return Promise.resolve(false);
  open = true;

  const heading = dialog.querySelector("#confirm-title");
  const content = dialog.querySelector("#confirm-body");
  const ok = dialog.querySelector("#confirm-ok");
  const cancel = dialog.querySelector("#confirm-cancel");
  heading.textContent = title;
  if (isSafeHtml(body)) setHTML(content, body);
  else content.textContent = String(body);
  ok.textContent = confirmLabel;
  ok.className = danger ? OK_CLASSES.danger : OK_CLASSES.primary;
  const returnFocus = document.activeElement;

  return new Promise((resolve) => {
    const onOk = () => dialog.close("confirm");
    const onCancel = () => dialog.close("cancel");
    const onClose = () => {
      ok.removeEventListener("click", onOk);
      cancel.removeEventListener("click", onCancel);
      dialog.removeEventListener("close", onClose);
      open = false;
      if (returnFocus && typeof returnFocus.focus === "function" && returnFocus.isConnected) {
        returnFocus.focus();
      }
      resolve(dialog.returnValue === "confirm");
    };
    ok.addEventListener("click", onOk);
    cancel.addEventListener("click", onCancel);
    dialog.addEventListener("close", onClose);
    dialog.returnValue = "";
    dialog.showModal();
    (danger ? cancel : ok).focus();
  });
}
