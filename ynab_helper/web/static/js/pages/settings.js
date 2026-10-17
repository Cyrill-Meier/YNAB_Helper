// Settings: the auto-approve switch (a named role="switch", F102) and "sign out
// everywhere" (POST /api/logout-all, F61) behind a confirmation dialog.

import { get, post, reportError } from "../core/api.js";
import { withBusy } from "../components/busy.js";
import { confirmDialog } from "../components/confirm.js";
import { toast } from "../components/toast.js";
import { byId, setText } from "../core/dom.js";

export async function init() {
  const toggle = byId("set-auto");
  const setToggle = (on) => toggle.setAttribute("aria-checked", on ? "true" : "false");

  get("/api/me")
    .then((me) => {
      if (toggle.getAttribute("aria-busy") !== "true") setToggle(Boolean(me.auto_approve));
      setText(byId("set-budget"), me.budget_name || "—");
      setText(byId("set-account"), me.account_name || "—");
    })
    .catch((err) => reportError(err));

  toggle.addEventListener("click", async () => {
    if (toggle.getAttribute("aria-busy") === "true") return;
    const desired = toggle.getAttribute("aria-checked") !== "true";
    setToggle(desired);
    toggle.setAttribute("aria-busy", "true");
    toggle.disabled = true;
    try {
      const r = await post("/api/settings", { auto_approve: desired });
      setToggle(Boolean(r.auto_approve));
      toast("ok", `Auto-approve ${r.auto_approve ? "enabled" : "disabled"}.`);
    } catch (err) {
      setToggle(!desired);
      reportError(err, "The setting was not saved");
    } finally {
      toggle.removeAttribute("aria-busy");
      toggle.disabled = false;
      toggle.focus();
    }
  });

  const everywhere = byId("set-logout-all");
  everywhere.addEventListener("click", async () => {
    const ok = await confirmDialog({
      title: "Sign out everywhere?",
      body: "Every session of your account ends — this browser, other browsers and devices — and unused sign-in links stop working. Send /login to the bot to sign in again.",
      confirmLabel: "Sign out everywhere",
      danger: true,
    });
    if (!ok) return;
    await withBusy(everywhere, "Signing out…", async () => {
      try {
        await post("/api/logout-all");
        window.location.assign("/");
      } catch (err) {
        reportError(err, "Sign-out failed");
      }
    });
  });
}
