/**
 * Tailwind CSS v3.4.17 configuration of the web UI (DESIGN §9.4).
 *
 * Built ahead of time by scripts/build_css.sh (pinned standalone CLI, no Node.js) into
 * ynab_helper/web/static/app.css, which is committed; CI fails when a rebuild changes it.
 * Class names must appear literally in the templates or JS modules (no string
 * concatenation of partial class names), or the build drops them.
 *
 * The palette and shadow are v1's (legacy web/templates/base.html); dark mode follows the
 * `dark` class on <html> (set by js/theme.js from localStorage "theme" or the OS).
 */
module.exports = {
  content: {
    relative: true,
    files: [
      "./ynab_helper/web/templates/**/*.html",
      "./ynab_helper/web/static/js/**/*.js",
    ],
  },
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        brand: {
          50: "#ECFFFB",
          100: "#C7FFF6",
          500: "#0EE3C8",
          600: "#0BB6A0",
          700: "#0A9081",
          // Darker than v1's palette: link/label text on light backgrounds (WCAG AA).
          800: "#07695E",
          900: "#063D38",
        },
        ink: {
          900: "#0B0F14",
          800: "#11171F",
          700: "#1A222D",
          600: "#283443",
          500: "#475569",
        },
      },
      boxShadow: {
        soft: "0 1px 2px rgba(0,0,0,0.04), 0 8px 24px rgba(0,0,0,0.06)",
      },
    },
  },
  plugins: [],
};
