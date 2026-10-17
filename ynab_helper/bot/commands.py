"""The command and callback registry (DESIGN §8.2).

One table drives everything: dispatch (exact, case-insensitive names; ``@botname`` is
stripped by :func:`ynab_helper.bot.context.parse_command`), the authorization gate's
per-command state check, ``/help`` and ``setMyCommands`` (default scope for users, the
admin's chat scope for user + admin commands). ``/dedupe_delete`` and ``/dedupe_cancel``
are dispatched but stay out of the menus (v1). Admin commands only exist for the admin:
for anyone else they are unknown commands.

Callback routes are keyed by the prefix before the first ``:`` of ``callback_data``.
"""

from __future__ import annotations

import html
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from ynab_helper.bot import states as S

if TYPE_CHECKING:
    from ynab_helper.bot.context import Request

__all__ = ["CallbackRoute", "Command", "Handler", "Registry", "build_registry"]

Handler = Callable[["Request"], None]

ANY_APPROVED: Final = S.APPROVED_STATES
ALL_STATES: Final = S.APPROVED_STATES | S.BLOCKED_STATES


@dataclass(frozen=True, slots=True)
class Command:
    """A slash command: who may run it (``admin``), in which user states, and whether it
    appears in Telegram's command menu."""

    name: str
    description: str
    handler: Handler
    states: frozenset[str] = S.READY_STATES
    admin: bool = False
    menu: bool = True
    icon: str = "•"


@dataclass(frozen=True, slots=True)
class CallbackRoute:
    """Inline-button callbacks whose data starts with ``<prefix>:``."""

    prefix: str
    handler: Handler
    states: frozenset[str] = S.READY_STATES
    expired_text: str = "This button has expired."


class Registry:
    """Lookup tables built from :class:`Command` and :class:`CallbackRoute` lists."""

    def __init__(self, commands: Iterable[Command], callbacks: Iterable[CallbackRoute]) -> None:
        self.commands: tuple[Command, ...] = tuple(commands)
        self.callbacks: tuple[CallbackRoute, ...] = tuple(callbacks)
        self._by_name = {c.name: c for c in self.commands}
        self._by_prefix = {r.prefix: r for r in self.callbacks}
        if len(self._by_name) != len(self.commands):
            raise ValueError("duplicate command name")
        if len(self._by_prefix) != len(self.callbacks):
            raise ValueError("duplicate callback prefix")

    def command(self, name: str, *, is_admin: bool) -> Command | None:
        """The command called ``name`` (exact match); admin commands only for the admin."""
        command = self._by_name.get(name)
        if command is None or (command.admin and not is_admin):
            return None
        return command

    def callback(self, data: str) -> CallbackRoute | None:
        prefix = data.split(":", 1)[0]
        return self._by_prefix.get(prefix)

    def menu(self, *, admin: bool) -> list[tuple[str, str]]:
        """``(name, description)`` pairs for ``setMyCommands``."""
        users = [(c.name, c.description) for c in self.commands if c.menu and not c.admin]
        if not admin:
            return users
        return users + [(c.name, c.description) for c in self.commands if c.menu and c.admin]

    def help_lines(self, *, admin: bool) -> list[str]:
        """``/help`` lines (HTML: the descriptions are escaped)."""
        lines = [
            f"{c.icon} /{c.name} — {html.escape(c.description)}"
            for c in self.commands
            if c.menu and not c.admin
        ]
        if admin:
            lines += ["", "<b>Admin commands:</b>"]
            lines += [f"/{c.name} — {html.escape(c.description)}" for c in self.commands if c.admin]
        return lines


def build_registry() -> Registry:
    """The bot's commands and callbacks (handlers imported here to avoid import cycles)."""
    from ynab_helper.bot.handlers import (
        admin,
        cleanup,
        crypto,
        dedupe,
        general,
        login,
        onboarding,
        reconcile,
        settings,
        status,
    )

    ready = S.READY_STATES
    commands: Sequence[Command] = (
        Command(
            "reconcile",
            "Reconcile YNAB with your newest statement",
            reconcile.cmd_reconcile,
            ready,
            icon="🧮",
        ),
        Command(
            "cleanup_pending",
            "Strip stale (pending) memos from cleared txns",
            cleanup.cmd_cleanup_pending,
            ready,
            icon="🧹",
        ),
        Command(
            "dedupe", "Find stray duplicate imports in YNAB", dedupe.cmd_dedupe, ready, icon="🔎"
        ),
        Command(
            "dedupe_delete",
            "Delete selected duplicates (1,3-5 or all)",
            dedupe.cmd_dedupe_delete,
            ready,
            menu=False,
        ),
        Command(
            "dedupe_cancel",
            "Discard the duplicate selection",
            dedupe.cmd_dedupe_cancel,
            ready,
            menu=False,
        ),
        Command(
            "status", "Show YNAB account balance & last import", status.cmd_status, ready, icon="📊"
        ),
        Command(
            "setup",
            "Re-run YNAB setup (token / budget / account)",
            onboarding.cmd_setup,
            ready | S.SETUP_STATES,
            icon="🔧",
        ),
        Command("settings", "Open the settings menu", settings.cmd_settings, ready, icon="⚙"),
        Command(
            "auto_approve",
            "Toggle auto-approval of imported txns",
            settings.cmd_auto_approve,
            ready,
            icon="✅",
        ),
        Command(
            "login", "Get a sign-in button for the web dashboard", login.cmd_login, ready, icon="🌐"
        ),
        Command(
            "logout_all",
            "Sign out of the web dashboard everywhere",
            login.cmd_logout_all,
            ANY_APPROVED,
            icon="🚪",
        ),
        Command(
            "crypto", "Sync crypto portfolio value to YNAB", crypto.cmd_crypto, ready, icon="🪙"
        ),
        Command(
            "crypto_setup",
            "Configure BTC xpub / ETH address / tracking acct",
            crypto.cmd_crypto_setup,
            ready | S.CRYPTO_STATES,
            icon="🛠",
        ),
        Command(
            "crypto_status",
            "Show current crypto configuration",
            crypto.cmd_crypto_status,
            ready,
            icon="🔍",
        ),
        Command(
            "cancel", "Leave the current setup step", general.cmd_cancel, ANY_APPROVED, icon="✖"
        ),
        Command("help", "Show available commands", general.cmd_help, ANY_APPROVED, icon="❓"),
        Command("start", "Show available commands", general.cmd_help, ANY_APPROVED, menu=False),
        Command(
            "approve",
            "Approve a pending user (/approve <id>)",
            admin.cmd_approve,
            ALL_STATES,
            admin=True,
        ),
        Command("deny", "Deny a user (/deny <id>)", admin.cmd_deny, ALL_STATES, admin=True),
        Command("users", "List registered users", admin.cmd_users, ALL_STATES, admin=True),
        Command(
            "ip", "Show this host's public IP and hostname", admin.cmd_ip, ALL_STATES, admin=True
        ),
        Command(
            "logs", "Download the bot log (last N lines)", admin.cmd_logs, ALL_STATES, admin=True
        ),
        Command(
            "revoke",
            "Sign a user out of the web UI (/revoke <id>)",
            admin.cmd_revoke,
            ALL_STATES,
            admin=True,
        ),
    )
    callbacks = (
        CallbackRoute("st", settings.cb_settings, ready, "This menu has expired — send /settings."),
        CallbackRoute(
            "ob", onboarding.cb_onboarding, S.SETUP_STATES, "This setup step has expired."
        ),
        CallbackRoute(
            "cs", crypto.cb_crypto_setup, S.CRYPTO_STATES, "This crypto setup has expired."
        ),
        CallbackRoute(
            "rc", reconcile.cb_reconcile, ready, "This preview has expired — run /reconcile again."
        ),
        CallbackRoute("dx", dedupe.cb_dedupe, ready, "This list has expired — run /dedupe again."),
        CallbackRoute(
            "dd",
            dedupe.cb_legacy_dedupe,
            ANY_APPROVED,
            "This list has expired — run /dedupe again.",
        ),
    )
    return Registry(commands, callbacks)
