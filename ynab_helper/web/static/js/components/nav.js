// Compact navigation: below the lg breakpoint the page links are a disclosure panel opened
// by #nav-toggle (aria-expanded); from lg up the CSS shows them as one row that fits — no
// horizontal scrollbar at any width (v1 overflowed at ~770 px). The toggle is the last
// control of the header and the panel follows it, so Tab moves from the toggle straight to
// the first link. Escape, a click outside, or focus moving away close the panel.

export function initNav() {
  const toggle = document.getElementById("nav-toggle");
  const nav = document.getElementById("site-nav");
  if (!toggle || !nav) return;

  const isOpen = () => nav.dataset.open === "true";
  const setOpen = (open) => {
    nav.dataset.open = open ? "true" : "false";
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
  };

  toggle.addEventListener("click", () => setOpen(!isOpen()));
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && isOpen()) {
      setOpen(false);
      toggle.focus();
    }
  });
  document.addEventListener("click", (event) => {
    if (isOpen() && !nav.contains(event.target) && !toggle.contains(event.target)) setOpen(false);
  });
  const onFocusOut = (event) => {
    const next = event.relatedTarget;
    if (isOpen() && next && !nav.contains(next) && !toggle.contains(next)) setOpen(false);
  };
  nav.addEventListener("focusout", onFocusOut);
  toggle.addEventListener("focusout", onFocusOut);
  const wide = window.matchMedia("(min-width: 1024px)");
  const onWide = () => {
    if (wide.matches) setOpen(false);
  };
  if (wide.addEventListener) wide.addEventListener("change", onWide);
}
