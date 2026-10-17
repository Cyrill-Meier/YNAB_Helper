// Colour theme: applied before the first paint (this classic script is loaded
// synchronously in <head>, so there is no light flash) and toggled by #theme-toggle.
// The choice is kept in localStorage "theme" ("dark" | "light", as v1); without a choice
// the OS preference wins and is followed live.
(function () {
  "use strict";
  var KEY = "theme";
  var root = document.documentElement;
  var media = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;

  function stored() {
    try {
      var value = window.localStorage.getItem(KEY);
      return value === "dark" || value === "light" ? value : null;
    } catch (err) {
      return null; // storage disabled
    }
  }

  function syncToggle() {
    var button = document.getElementById("theme-toggle");
    if (button) button.setAttribute("aria-pressed", root.classList.contains("dark") ? "true" : "false");
  }

  function apply() {
    var choice = stored();
    var dark = choice === "dark" || (choice === null && !!(media && media.matches));
    root.classList.toggle("dark", dark);
    syncToggle();
  }

  apply();

  if (media) {
    var onSystemChange = function () {
      if (stored() === null) apply();
    };
    if (media.addEventListener) media.addEventListener("change", onSystemChange);
    else if (media.addListener) media.addListener(onSystemChange);
  }

  document.addEventListener("click", function (event) {
    var target = event.target;
    var button = target && target.closest ? target.closest("#theme-toggle") : null;
    if (!button) return;
    var dark = root.classList.toggle("dark");
    try {
      window.localStorage.setItem(KEY, dark ? "dark" : "light");
    } catch (err) {
      // storage disabled: the choice lasts for this page only
    }
    syncToggle();
  });

  document.addEventListener("DOMContentLoaded", syncToggle);
})();
