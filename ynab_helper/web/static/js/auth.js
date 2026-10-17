// Submits the sign-in form of /auth automatically; the visible "Continue" button is the
// fallback when scripts are blocked. The GET of /auth never consumes the token (F88).
// Only a form marked data-autosubmit="true" is submitted: when the browser is already
// signed in, the server renders a "switch account?" confirmation without that marker and
// the user has to press Continue themselves.
document.addEventListener("DOMContentLoaded", () => {
  const form = document.getElementById("auth-form");
  if (form && form.dataset.autosubmit === "true") form.submit();
});
