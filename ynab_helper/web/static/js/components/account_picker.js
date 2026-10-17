// Account picker (F99): a disclosure button (aria-expanded) with a panel of checkboxes.
// The panel stays open while you tick several accounts — only Escape, the button, or a
// click/focus outside closes it. Changes are applied after a short pause and at once when
// the panel closes, so picking three accounts costs one reload, not three. The panel is
// rendered once; ticking updates it in place (focus never jumps).
//
// No selection means "all accounts" (nothing is sent to the API).
//
// The panel (w-72) is wider than the button, so it is anchored to the button's left edge
// (align: "left", for pickers at the start of a toolbar) or right edge (align: "right", for
// right-aligned toolbars), and flipped on opening when that side would push it out of the
// viewport — its checkboxes are never cut off at the screen edge.

import { debounce } from "../core/dom.js";
import { html, setHTML } from "../core/html.js";

const APPLY_DELAY_MS = 600;
const VIEWPORT_MARGIN = 8;
let counter = 0;

/**
 * The edge of the button to anchor a `width`-wide panel to: "left" (the panel extends to the
 * right) or "right" (it extends to the left). `preferred` wins when it fits in the viewport,
 * then the other side; when neither fits, the side with more room.
 */
export function panelSide(button, width, viewportWidth, preferred = "left") {
  const roomRight = viewportWidth - VIEWPORT_MARGIN - button.left; // left-anchored
  const roomLeft = button.right - VIEWPORT_MARGIN; // right-anchored
  const fits = { left: width <= roomRight, right: width <= roomLeft };
  const other = preferred === "right" ? "left" : "right";
  if (fits[preferred]) return preferred;
  if (fits[other]) return other;
  return roomRight >= roomLeft ? "left" : "right";
}

export class AccountPicker {
  /**
   * @param {HTMLElement} root  container (rendered into)
   * @param {object} options
   * @param {Array<{id: string, name: string, classification: string|null, closed: boolean}>} options.accounts
   * @param {string[]} [options.selected]  initially selected ids (unknown ids are dropped)
   * @param {(ids: string[]) => void} options.onChange
   * @param {"left"|"right"} [options.align]  preferred panel anchor (the button's edge)
   */
  constructor(root, { accounts, selected = [], onChange, align = "left" }) {
    this.root = root;
    this.accounts = accounts;
    this.onChange = onChange;
    this.align = align === "right" ? "right" : "left";
    const known = new Set(accounts.map((a) => a.id));
    this.selected = new Set(selected.filter((id) => known.has(id)));
    this.applied = this.key();
    this.uid = `acct-picker-${(counter += 1)}`;
    this.notify = debounce(() => this.apply(), APPLY_DELAY_MS);
    this.render();
  }

  /** The selected ids (empty = all accounts). */
  value() {
    return this.accounts.filter((a) => this.selected.has(a.id)).map((a) => a.id);
  }

  key() {
    return [...this.selected].sort().join(",");
  }

  label() {
    if (this.selected.size === 0) return "All accounts";
    if (this.selected.size === 1) {
      const [only] = this.selected;
      return this.accounts.find((a) => a.id === only)?.name || "1 account";
    }
    return `${this.selected.size} accounts`;
  }

  render() {
    const { uid } = this;
    const anchor = this.align === "right" ? "right-0" : "left-0"; // place() may flip it
    setHTML(
      this.root,
      html`<div class="relative">
        <button type="button" id="${uid}-button" aria-expanded="false" aria-controls="${uid}-panel"
                class="field flex min-w-[14rem] items-center justify-between gap-2 hover:bg-slate-50 dark:hover:bg-ink-700">
          <span class="sr-only">Accounts:</span>
          <span class="truncate" data-role="label">${this.label()}</span>
          <svg viewBox="0 0 20 20" class="h-4 w-4 shrink-0 opacity-60" fill="currentColor" aria-hidden="true" focusable="false"><path d="M5 8l5 5 5-5z"/></svg>
        </button>
        <div id="${uid}-panel" hidden
             class="absolute ${anchor} z-20 mt-1 w-72 max-w-[calc(100vw-2rem)] overflow-hidden rounded-md border border-slate-200 bg-white shadow-soft dark:border-ink-600 dark:bg-ink-800">
          <div class="flex items-center justify-between border-b border-slate-100 px-3 py-2 text-xs dark:border-ink-700">
            <button type="button" data-act="all" class="link">Select all</button>
            <span class="text-ink-500 dark:text-slate-400" data-role="count" aria-live="polite"></span>
            <button type="button" data-act="none" class="link">Clear</button>
          </div>
          <fieldset class="max-h-72 overflow-auto py-1">
            <legend class="sr-only">Show transactions of these accounts</legend>
            ${this.accounts.map(
              (a) => html`<label class="flex cursor-pointer items-center gap-2 px-3 py-1.5 text-sm hover:bg-slate-50 dark:hover:bg-ink-700">
                <input type="checkbox" class="checkbox" value="${a.id}"${this.selected.has(a.id) ? html` checked` : ""}>
                <span class="flex-1 truncate">${a.name}${a.closed ? " (closed)" : ""}</span>
                <span class="text-xs text-ink-500 dark:text-slate-400">${a.classification || ""}</span>
              </label>`,
            )}
          </fieldset>
        </div>
      </div>`,
    );
    this.button = this.root.querySelector(`#${uid}-button`);
    this.panel = this.root.querySelector(`#${uid}-panel`);
    this.refresh();

    this.button.addEventListener("click", () => this.setOpen(this.panel.hidden));
    this.panel.addEventListener("change", (event) => {
      const box = event.target;
      if (!(box instanceof HTMLInputElement) || box.type !== "checkbox") return;
      if (box.checked) this.selected.add(box.value);
      else this.selected.delete(box.value);
      this.changed();
    });
    this.panel.addEventListener("click", (event) => {
      const action = event.target.closest?.("button[data-act]")?.dataset.act;
      if (!action) return;
      this.selected = action === "all" ? new Set(this.accounts.map((a) => a.id)) : new Set();
      for (const box of this.panel.querySelectorAll("input[type=checkbox]")) {
        box.checked = this.selected.has(box.value);
      }
      this.changed();
    });
    this.root.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && !this.panel.hidden) {
        event.stopPropagation();
        this.setOpen(false);
        this.button.focus();
      }
    });
    this.root.addEventListener("focusout", (event) => {
      if (!this.panel.hidden && event.relatedTarget && !this.root.contains(event.relatedTarget)) {
        this.setOpen(false);
      }
    });
    document.addEventListener("click", (event) => {
      if (!this.panel.hidden && !this.root.contains(event.target)) this.setOpen(false);
    });
  }

  refresh() {
    this.root.querySelector('[data-role="label"]').textContent = this.label();
    const n = this.selected.size;
    this.root.querySelector('[data-role="count"]').textContent =
      n === 0 ? `All ${this.accounts.length}` : `${n} of ${this.accounts.length}`;
  }

  changed() {
    this.refresh();
    this.notify();
  }

  apply() {
    const key = this.key();
    if (key === this.applied) return;
    this.applied = key;
    this.onChange(this.value());
  }

  setOpen(open) {
    this.panel.hidden = !open;
    this.button.setAttribute("aria-expanded", open ? "true" : "false");
    if (open) this.place();
    else this.notify.flush();
  }

  /** Anchor the open panel so that it stays inside the viewport. */
  place() {
    const viewport = document.documentElement.clientWidth || window.innerWidth || 0;
    const width = this.panel.getBoundingClientRect().width;
    if (!viewport || !width) return;
    const side = panelSide(this.button.getBoundingClientRect(), width, viewport, this.align);
    this.panel.classList.toggle("left-0", side === "left");
    this.panel.classList.toggle("right-0", side === "right");
  }
}

/** Build a picker from GET /api/accounts items (deleted accounts are left out). */
export function pickerAccounts(items) {
  return (items || [])
    .filter((a) => !a.deleted)
    .map((a) => ({
      id: a.id,
      name: a.name || "(unnamed)",
      classification: a.classification,
      closed: Boolean(a.closed),
    }));
}
