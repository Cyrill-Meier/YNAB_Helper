"""Admin commands: /approve /deny /users /ip /logs [N] /revoke (DESIGN §8.2, §8.3).

* ``/approve <id>`` only moves a ``pending`` or ``denied`` user to the token step; an
  onboarded user is left alone (F93).
* ``/deny <id>`` blocks the user, signs them out of the web UI (sessions and unused login
  links) and drops their in-memory flows (F18). The admin cannot deny themselves.
* ``/revoke <id>`` signs a user out of the web UI.
* ``/logs [N]`` sends the last N lines (default 500, at most 100 000) of the rotating log,
  newest file plus as many rotated files as needed, as a document.
* ``/ip`` reports the host's public IP (looked up on a worker) and hostname.
"""

from __future__ import annotations

import ipaddress
import logging
import socket
from collections import deque
from pathlib import Path
from typing import Final

from ynab_helper import net
from ynab_helper.bot import render
from ynab_helper.bot import states as S
from ynab_helper.bot.context import Request
from ynab_helper.bot.handlers import onboarding
from ynab_helper.bot.handlers._common import ascii_int
from ynab_helper.bot.render import code, h
from ynab_helper.errors import TelegramError
from ynab_helper.logging_setup import BACKUP_COUNT, current_log_file

log = logging.getLogger(__name__)

DEFAULT_LOG_LINES: Final = 500
MAX_LOG_LINES: Final = 100_000
_IP_SERVICES: Final = (
    ("https://api.ipify.org?format=json", "ip"),
    ("https://ifconfig.me/all.json", "ip_addr"),
)


def _target_id(req: Request, usage: str) -> int | None:
    parts = req.args.split()
    if not parts:
        req.reply(f"Usage: {h(usage)}")
        return None
    target = ascii_int(parts[0], 15)
    if target is None:
        req.reply("Invalid user ID.")
    return target


def _name(user_id: int, first_name: str | None) -> str:
    return f"{h(first_name or '?')} ({code(user_id)})"


def cmd_approve(req: Request) -> None:
    target = _target_id(req, "/approve <user_id>")
    if target is None:
        return
    users = req.bot.users
    user = users.get(target)
    if user is None:
        req.reply(f"User {code(target)} not found.")
        return
    if user.state not in S.BLOCKED_STATES:
        req.reply(f"User {_name(target, user.first_name)} is already {h(user.state)}.")
        return
    updated = users.update_if(
        target, states=S.BLOCKED_STATES, state=S.AWAITING_TOKEN, temp_data=None
    )
    if updated is None:
        req.reply("The user changed meanwhile — try again.")
        return
    log.info("admin approved user %s", target)
    req.reply(f"✅ User {_name(target, user.first_name)} approved.")
    req.bot.send(target, onboarding.approval_text())  # private chat id == user id (F56)


def cmd_deny(req: Request) -> None:
    target = _target_id(req, "/deny <user_id>")
    if target is None:
        return
    if target == req.bot.admin_id:
        req.reply("You cannot deny yourself.")
        return
    users = req.bot.users
    user = users.get(target)
    if user is None:
        req.reply(f"User {code(target)} not found.")
        return
    already = user.state == S.DENIED
    users.update(target, state=S.DENIED, temp_data=None)
    revoked = users.revoke_sessions(target)
    req.bot.forget_user(target)
    log.info("admin denied user %s (%d web sessions revoked)", target, revoked)
    req.reply(
        f"❌ User {_name(target, user.first_name)} denied"
        + (" (was already denied)" if already else "")
        + (f"; {revoked} web session(s)/sign-in link(s) revoked." if revoked else ".")
    )
    if not already:
        req.bot.send(target, "Your access has been denied.")


def cmd_revoke(req: Request) -> None:
    target = _target_id(req, "/revoke <user_id>")
    if target is None:
        return
    if req.bot.users.get(target) is None:
        req.reply(f"User {code(target)} not found.")
        return
    revoked = req.bot.users.revoke_sessions(target)
    log.info("admin revoked the web sessions of user %s (%d)", target, revoked)
    req.reply(f"🚪 Revoked {revoked} web session(s)/sign-in link(s) of user {code(target)}.")


def cmd_users(req: Request) -> None:
    req.reply(render.users_list(req.bot.users.list_all(), admin_id=req.bot.admin_id))


# ── /ip ──────────────────────────────────────────────────────────────────────


def public_ip() -> str:
    """The host's public IP from a lookup service (validated before it is shown)."""
    for url, key in _IP_SERVICES:
        try:
            reply = net.request_json("GET", url, timeout=8, retries=0, service="IP lookup")
        except Exception as exc:
            log.warning("/ip: %s failed: %s", url, exc)
            continue
        value = reply.get(key) if isinstance(reply, dict) else None
        if isinstance(value, str):
            try:
                return str(ipaddress.ip_address(value.strip()))
            except ValueError:
                continue
    return "(unknown — lookup failed)"


def _local_ips(hostname: str) -> list[str]:
    found: list[str] = []
    try:
        for info in socket.getaddrinfo(hostname, None):
            ip = str(info[4][0])
            if ip and not ip.startswith("127.") and ip != "::1" and ip not in found:
                found.append(ip)
    except OSError:
        pass
    return found[:10]


def cmd_ip(req: Request) -> None:
    lookup = req.bot.ip_lookup or public_ip

    def job() -> None:
        address = lookup()
        try:
            hostname = socket.gethostname()
        except OSError:
            hostname = "(unknown)"
        lines = [
            "🌐 <b>Bot host info</b>",
            f"Public IP: {code(address)}",
            f"Hostname: {code(hostname)}",
        ]
        local = _local_ips(hostname)
        if local:
            lines.append("Local IPs: " + ", ".join(code(ip) for ip in local))
        req.reply("\n".join(lines))

    req.bot.start_job(req, "ip", job)


# ── /logs ────────────────────────────────────────────────────────────────────


def tail_lines(path: Path, count: int, *, backups: int = BACKUP_COUNT) -> list[str]:
    """The last ``count`` lines of ``path`` and, if needed, its rotated files
    (``path.1`` … ``path.<backups>``, older first in the result)."""
    collected: list[list[str]] = []
    remaining = count
    for index in range(backups + 1):
        if remaining <= 0:
            break
        current = path if index == 0 else path.with_name(f"{path.name}.{index}")
        try:
            with current.open(encoding="utf-8", errors="replace") as handle:
                lines = list(deque(handle, maxlen=remaining))
        except FileNotFoundError:
            break
        collected.append(lines)
        remaining -= len(lines)
    result: list[str] = []
    for lines in reversed(collected):
        result.extend(lines)
    return result


def cmd_logs(req: Request) -> None:
    arg = req.args.split()[0] if req.args else ""
    parsed = ascii_int(arg, 9) if arg else DEFAULT_LOG_LINES
    if not parsed:
        req.reply("Usage: /logs [N] — the last N lines (default 500, at most 100000).")
        return
    requested = parsed
    count = min(requested, MAX_LOG_LINES)
    path = req.bot.log_file or current_log_file()
    if path is None or not path.exists():
        req.reply("No log file is configured (or it doesn't exist yet). Set LOG_FILE to enable it.")
        return

    def job() -> None:
        lines = tail_lines(path, count)
        content = "".join(lines).encode("utf-8")
        note = f" (capped at {MAX_LOG_LINES})" if requested > MAX_LOG_LINES else ""
        caption = f"📜 {h(path.name)} — last {len(lines)} lines{note}"
        try:
            req.bot.tg.send_document(req.chat_id, path.name, content, caption=caption)
        except TelegramError as exc:
            log.warning("sending the log failed: %s", exc)
            req.reply(f"❌ Failed to send the log: {h(exc.description or 'Telegram error')}")

    req.bot.start_job(req, "logs", job)
