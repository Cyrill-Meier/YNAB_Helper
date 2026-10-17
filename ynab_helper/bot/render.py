"""HTML formatters for the Telegram bot (docs/DESIGN.md §8.1, §8.3; F15, F25, F29, F47, F91, F96).

Every message is built from a structured service result — never from captured stdout —
and sent with ``parse_mode="HTML"``. Rules for every formatter here:

* every dynamic value (payee, account, budget, file and user names, error texts) goes
  through :func:`h`, so a payee called ``<b>`` or ``[Tap](https://evil)`` is shown verbatim
  and can never inject markup or links (F91);
* a tag is closed on the line that opens it, so :func:`ynab_helper.bot.telegram.split_message`
  may cut long messages on any line boundary;
* amounts are milliunits formatted with :func:`ynab_helper.money.format_money` and the
  currency code when known.

Inline-button labels are plain text (Telegram does not parse them), so they are not escaped.
"""

from __future__ import annotations

import html
from collections.abc import Iterable, Mapping, Sequence
from decimal import Decimal
from typing import Any, Final

from ynab_helper.balance.adjust import (
    OUTCOME_CREATED,
    OUTCOME_DRY_RUN,
    OUTCOME_DUPLICATE,
    OUTCOME_IN_SYNC,
)
from ynab_helper.balance.crypto.sync import CryptoSyncResult
from ynab_helper.balance.reconcile import ReconcilePreview, ReconcileResult
from ynab_helper.bot.telegram import MAX_MESSAGE_UNITS, button, inline_keyboard, utf16_len
from ynab_helper.importer.models import Change, ImportResult, Removal, TxRef
from ynab_helper.maintenance.cleanup import CleanupResult
from ynab_helper.maintenance.dedupe import (
    MAX_DELETE_IDS,
    DedupeCandidate,
    DedupeDeleteResult,
    DedupeScan,
)
from ynab_helper.money import decimal_to_milli, format_money
from ynab_helper.services import StatusInfo
from ynab_helper.storage.users import User

#: Items listed per section of a result (changes, removals, skipped rows, …).
MAX_LISTED: Final = 8
#: Candidates per page of the dedupe picker.
DEDUPE_PAGE_SIZE: Final = 6
PARTIAL_HINT: Final = "⚠ Partially applied — send the file again to retry."
BUSY_TEXT: Final = "⏳ Another operation is still running."
_PAYEE_WIDTH: Final = 40
_SECRET_HEAD: Final = 8
_SECRET_TAIL: Final = 6

_REMOVAL_REASONS: Final[Mapping[str, str]] = {
    "reverted": "reverted by Revolut",
    "superseded": "pending replaced by the settled transaction",
}
_CHANGE_REASONS: Final[Mapping[str, str]] = {
    "cleared": "completed",
    "amount_changed": "amount changed",
    "redated": "date changed",
    "pending_marker": "(pending) marker removed",
    "settlement": "settled",
    "duplicate_resolved": "matched an existing transaction",
}
#: How a job (``Bot.start_job`` op name) is called in the "bot restarted" notice.
_JOB_LABELS: Final[Mapping[str, str]] = {
    "import": "import",
    "reconcile": "reconcile",
    "dedupe": "dedupe",
    "cleanup": "cleanup",
    "crypto": "crypto sync",
    "crypto_setup": "crypto setup",
    "setup": "setup step",
    "status": "status check",
    "ip": "/ip lookup",
    "logs": "/logs export",
}
_STATE_ICONS: Final[Mapping[str, str]] = {
    "pending": "⏳",
    "awaiting_token": "🔑",
    "awaiting_budget": "📋",
    "awaiting_account": "🏦",
    "ready": "🟢",
    "denied": "🔴",
    "awaiting_crypto_account": "🪙",
    "awaiting_crypto_btc": "🪙",
    "awaiting_crypto_eth": "🪙",
}


# ── primitives ───────────────────────────────────────────────────────────────


def h(value: object) -> str:
    """HTML-escape ``value`` for Telegram (``None`` → empty string)."""
    if value is None:
        return ""
    return html.escape(str(value), quote=True)


def bold(value: object) -> str:
    return f"<b>{h(value)}</b>"


def code(value: object) -> str:
    return f"<code>{h(value)}</code>"


def clip(text: str | None, width: int = _PAYEE_WIDTH) -> str:
    """``text`` shortened to ``width`` characters (with an ellipsis); None → ``?``."""
    value = " ".join((text or "").split()) or "?"
    return value if len(value) <= width else value[: width - 1] + "…"


def money(milli: int | None, currency: str | None = None, *, sign: bool = False) -> str:
    """Escaped amount (``—`` when unknown)."""
    if milli is None:
        return "—"
    return h(format_money(milli, currency, sign=sign))


def interrupted(op: str, *, started: bool) -> str:
    """The notice for a user whose job a shutdown dropped (``started=False``) or cut off.

    Their update is already confirmed, so Telegram will not deliver it again: they must
    resend it. Re-sending is safe (imports converge through their import ids).
    """
    label = _JOB_LABELS.get(op, "request")
    retry = "send the file again" if op == "import" else "try again"
    if started:
        return (
            f"⚠ The bot restarted while your {h(label)} was running, so it may be "
            f"incomplete. Please {retry} in a minute."
        )
    return f"⚠ The bot restarted before your {h(label)} could start. Please {retry} in a minute."


def plural(count: int, word: str, many: str | None = None) -> str:
    return f"{count} {word if count == 1 else (many or word + 's')}"


def mask(value: str | None) -> str:
    """A key or address with only its ends visible (never echo an xpub in full)."""
    if not value:
        return ""
    text = value.strip()
    if len(text) <= _SECRET_HEAD + _SECRET_TAIL + 3:
        return text[:4] + "…"
    return f"{text[:_SECRET_HEAD]}…{text[-_SECRET_TAIL:]}"


def _listed(
    title: str, items: Sequence[str], *, total: int | None = None, limit: int = MAX_LISTED
) -> list[str]:
    if not items:
        return []
    count = len(items) if total is None else total
    lines = ["", f"{title} ({count}):"]
    lines.extend(f"  • {item}" for item in items[:limit])
    if count > min(limit, len(items)):
        lines.append(f"  … and {count - min(limit, len(items))} more")
    return lines


def _tx(tx: TxRef, currency: str | None) -> str:
    return f"{h(tx.date)} {h(clip(tx.payee))} {money(tx.amount, currency)}"


def _change_value(field: str, value: Any, currency: str | None) -> str:
    if value is None:
        return "—"
    if field == "amount" and isinstance(value, int) and not isinstance(value, bool):
        return money(value, currency)
    return h(clip(str(value), 30))


def _change(change: Change, currency: str | None) -> str:
    parts = []
    for field, (old, new) in change.fields.items():
        if field == "memo":
            parts.append("memo updated")
        else:
            parts.append(
                f"{h(field)} {_change_value(field, old, currency)} → "
                f"{_change_value(field, new, currency)}"
            )
    reason = _CHANGE_REASONS.get(change.reason, change.reason)
    detail = "; ".join(parts) if parts else h(reason)
    return f"{_tx(change.tx, currency)}: {detail}"


def _removal(removal: Removal, currency: str | None) -> str:
    reason = _REMOVAL_REASONS.get(removal.reason, removal.reason)
    return f"{_tx(removal.tx, currency)} — {h(reason)}"


# ── import ───────────────────────────────────────────────────────────────────


def import_result(result: ImportResult, *, file_name: str | None = None) -> str:
    """Summary of an import (F15/F47): counts, changes, removals, skips, warnings, failures."""
    currency = result.currency
    if not result.ok:
        title = "⚠ Import partially applied"
    elif result.has_changes:
        title = "✅ Import complete"
    else:
        title = "✅ Everything already up to date"
    if result.dry_run:
        title = "🔍 Dry run — nothing was written"
    lines = [title + (f" — {bold(clip(file_name, 60))}" if file_name else "")]
    if result.window:
        start, end = result.window
        lines.append(
            f"📅 {h(start)} → {h(end)} · {plural(result.rows_total, 'row')} "
            f"({result.rows_pending} pending, {result.rows_reverted} reverted)"
        )
    counts = [
        f"New: {len(result.created)}",
        f"Updated: {len(result.updated)}",
        f"Removed: {len(result.removed)}",
        f"Unchanged: {result.unchanged}",
    ]
    if result.linked:
        counts.insert(1, f"Linked: {len(result.linked)}")
    lines.append(" · ".join(counts))
    if result.ignored_deleted:
        lines.append(
            f"🚫 {plural(result.ignored_deleted, 'transaction')} you deleted in YNAB "
            "stayed deleted."
        )
    lines += _listed("✏ Changed in YNAB", [_change(c, currency) for c in result.updated])
    lines += _listed("🗑 Removed from YNAB", [_removal(r, currency) for r in result.removed])
    lines += _listed(
        "⏭ Skipped rows",
        [
            f"data row {int(line_no)}: {h(clip(reason, 120))}"
            for line_no, reason in result.skipped_rows
        ],
    )
    lines += _listed("⚠ Warnings", [h(clip(w, 300)) for w in result.warnings])
    failures = [
        f"{h(f.action)}"
        + (f" {_tx(f.tx, currency)}" if f.tx is not None else "")
        + f": {h(clip(f.error, 200))}"
        for f in result.failures
    ]
    lines += _listed("❌ Failed", failures)
    if result.rate_limited:
        lines += [
            "",
            "⏳ YNAB's rate limit (200 requests per hour) was reached"
            + (
                f"; {plural(result.not_applied, 'change')} not applied."
                if result.not_applied
                else "."
            ),
        ]
    elif result.not_applied:
        lines += ["", f"⏳ {plural(result.not_applied, 'change')} not applied."]
    if not result.ok:
        lines += ["", PARTIAL_HINT]
    return "\n".join(lines)


def balance_line(status: StatusInfo) -> str:
    """One line with the account's balances from the mirror (after an import)."""
    if not status.account_found or status.balance is None:
        return "💰 YNAB balance: not available yet."
    text = f"💰 {bold(clip(status.account_name or 'YNAB', 40))}: {money(status.balance, status.currency)}"
    if status.cleared_balance is not None and status.cleared_balance != status.balance:
        text += f" (cleared {money(status.cleared_balance, status.currency)})"
    if status.sync_note:
        text += f"\nℹ {h(status.sync_note)}"
    return text


# ── status ───────────────────────────────────────────────────────────────────


def status(info: StatusInfo) -> str:
    cur = info.currency
    name = info.account_name or info.account_id
    lines = [f"📊 {bold(clip(name, 60))}"]
    if not info.account_found:
        lines.append("⚠ This account is not in the synced YNAB data yet.")
    else:
        if info.account_deleted:
            lines.append("⚠ The account was deleted in YNAB — run /setup.")
        elif info.account_closed:
            lines.append("⚠ The account is closed in YNAB — run /setup.")
        lines.append(f"  Working balance: {money(info.balance, cur)}")
        lines.append(f"  Cleared: {money(info.cleared_balance, cur)}")
        lines.append(f"  Uncleared: {money(info.uncleared_balance, cur)}")
    statement = info.last_statement
    if statement is not None:
        window = (
            f", {h(statement.window[0])} → {h(statement.window[1])}" if statement.window else ""
        )
        lines.append("")
        lines.append(
            f"📄 Last statement: {h(clip(statement.original_name or 'statement', 60))}{window}"
        )
        lines.append(f"  Uploaded {h(statement.uploaded_at)} · status {h(statement.status)}")
    else:
        lines += ["", "📄 No statement imported yet — send me a Revolut CSV."]
    counts = {k: v for k, v in info.ledger_counts.items() if v}
    if counts:
        lines.append(
            "📒 Ledger: " + ", ".join(f"{h(k.lower())} {v}" for k, v in sorted(counts.items()))
        )
    lines.append(f"🔄 Last sync: {h(info.last_sync_at or 'never')}")
    if info.sync_note:
        lines.append(f"ℹ {h(info.sync_note)}")
    return "\n".join(lines)


# ── reconcile ────────────────────────────────────────────────────────────────


def reconcile_preview(preview: ReconcilePreview) -> str:
    """The numbers the user confirms before a reconciliation adjustment is posted (F04)."""
    cur = preview.currency
    lines = [
        f"🧮 Reconcile with {bold(clip(preview.csv_name, 60))}",
        f"Statement balance on {h(preview.as_of)}: {money(preview.csv_balance, cur)}",
        f"YNAB cleared balance on {h(preview.as_of)}: {money(preview.ynab_cleared_as_of, cur)}",
    ]
    if preview.cleared_after_as_of:
        lines.append(
            f"  (cleared now {money(preview.ynab_cleared_now, cur)}, of which "
            f"{money(preview.cleared_after_as_of, cur, sign=True)} is dated after {h(preview.as_of)})"
        )
    lines.append(f"Difference: {bold(format_money(preview.delta, cur, sign=True))}")
    for problem in preview.problems:
        lines.append(f"⚠ {h(problem)}")
    if preview.in_sync or preview.token is None:
        lines += ["", "✅ YNAB already matches the statement — nothing to post."]
    else:
        lines += [
            "",
            f"Post a {bold('CSV Reconciliation')} adjustment of "
            f"{money(preview.delta, cur, sign=True)} dated {h(preview.as_of)}?",
        ]
    return "\n".join(lines)


def reconcile_result(result: ReconcileResult) -> str:
    adjustment = result.adjustment
    cur = result.preview.currency
    if adjustment.outcome == OUTCOME_CREATED:
        return (
            f"✅ Posted a reconciliation adjustment of "
            f"{money(adjustment.delta_milli, cur, sign=True)} dated {h(adjustment.date)}."
        )
    if adjustment.outcome == OUTCOME_DUPLICATE:
        return (
            "⊘ YNAB already holds this adjustment (same amount and date) — nothing new was posted."
        )
    if adjustment.outcome == OUTCOME_IN_SYNC:
        return "✅ YNAB already matches the statement — nothing was posted."
    return f"ℹ Reconcile finished ({h(adjustment.outcome)})."


# ── cleanup ──────────────────────────────────────────────────────────────────


def cleanup_result(result: CleanupResult, currency: str | None = None) -> str:
    lines: list[str]
    if not result.candidates:
        lines = ["✓ No stale '(pending)' memos found."]
    else:
        lines = [
            f"✓ Cleaned {len(result.updated)} of "
            f"{plural(len(result.candidates), 'stale (pending) memo')}."
        ]
        lines += _listed(
            "Cleaned",
            [
                f"{h(fix.date)} {h(clip(fix.payee_name))} {money(fix.amount, currency)}"
                for fix in result.updated
            ],
        )
    if result.skipped_uncleared:
        lines.append(
            f"ℹ {plural(result.skipped_uncleared, 'uncleared transaction')} keep their marker "
            "(still pending)."
        )
    lines += _listed(
        "❌ Failed",
        [
            f"{h(f.fix.date)} {h(clip(f.fix.payee_name))}: {h(clip(f.error, 200))}"
            for f in result.failures
        ],
    )
    if result.rate_limited or result.not_applied:
        lines += [
            "",
            "⏳ YNAB's rate limit was reached"
            + (
                f"; {plural(result.not_applied, 'memo')} not cleaned."
                if result.not_applied
                else "."
            ),
        ]
    if not result.ok:
        lines += ["", "⚠ Partially applied — run /cleanup_pending again later."]
    return "\n".join(lines)


# ── crypto ───────────────────────────────────────────────────────────────────


def _amount(value: Decimal) -> str:
    text = format(value.quantize(Decimal("0.00000001")).normalize(), "f")
    return text if "." not in text else text.rstrip("0").rstrip(".")


def _price(value: Decimal, currency: str) -> str:
    try:
        return money(decimal_to_milli(value), currency)
    except (TypeError, ValueError):
        return h(f"{value} {currency}")


def crypto_result(
    result: CryptoSyncResult, *, account_name: str | None = None, currency: str = "CHF"
) -> str:
    """The /crypto summary from a structured result (F29): holdings, prices, values,
    portfolio total, YNAB's balance before, and what the adjustment did."""
    lines = [f"🪙 Crypto sync — {bold(clip(account_name or 'tracking account', 60))}"]
    if result.holdings:
        lines += ["", "Holdings:"]
        for holding in result.holdings:
            lines.append(
                f"  • {h(holding.symbol)} {h(_amount(holding.amount))} × "
                f"{_price(holding.price, currency)} = {money(holding.value_milli, currency)}"
            )
    else:
        lines += ["", "No holdings found."]
    if result.zero_balances:
        lines.append(f"  (no balance: {h(', '.join(result.zero_balances))})")
    if result.btc is not None and result.btc.source == "xpub":
        lines.append(
            f"  BTC from {h(result.btc.kind)}: {plural(result.btc.used_addresses, 'used address', 'used addresses')} "
            f"of {result.btc.scanned} scanned"
        )
    lines += [
        "",
        f"Portfolio: {bold(format_money(result.portfolio_milli, currency))}",
        f"YNAB before: {money(result.ynab_before_milli, currency)}"
        + (
            f" (incl. {money(result.ynab_uncleared_milli, currency)} uncleared)"
            if result.ynab_uncleared_milli
            else ""
        ),
    ]
    adjustment = result.adjustment
    if adjustment.outcome == OUTCOME_CREATED:
        lines.append(
            f"✅ Adjustment posted: {money(adjustment.delta_milli, currency, sign=True)} → "
            f"YNAB now {money(result.ynab_after_milli, currency)}"
        )
    elif adjustment.outcome == OUTCOME_DUPLICATE:
        lines.append(
            f"⊘ Already synced today with this difference "
            f"({money(adjustment.delta_milli, currency, sign=True)}) — nothing new was posted."
        )
    elif adjustment.outcome == OUTCOME_IN_SYNC:
        lines.append("✅ Already in sync — no adjustment needed.")
    elif adjustment.outcome == OUTCOME_DRY_RUN:
        lines.append(
            f"🔍 Would post {money(adjustment.delta_milli, currency, sign=True)} (dry run)."
        )
    for warning in result.warnings:
        lines.append(f"⚠ {h(warning)}")
    return "\n".join(lines)


def crypto_status(user: User) -> str:
    lines = ["🔍 <b>Crypto configuration</b>"]
    lines.append(
        f"Tracking account: {h(user.crypto_account_name) if user.crypto_account_id else '<i>not set</i>'}"
    )
    lines.append(
        f"BTC xpub/address: {code(mask(user.btc_xpub)) if user.btc_xpub else '<i>not set</i>'}"
    )
    lines.append(
        f"ETH address: {code(mask(user.eth_address)) if user.eth_address else '<i>not set</i>'}"
    )
    lines += ["", "Use /crypto_setup to change these, /crypto to sync."]
    return "\n".join(lines)


# ── dedupe ───────────────────────────────────────────────────────────────────


def _candidate_label(candidate: DedupeCandidate, selected: bool) -> str:
    check = "✅" if selected else "⬜"
    amount = format_money(candidate.amount)
    return f"{check} {candidate.date[5:]} {amount} {clip(candidate.payee_name, 18)}"


def _candidate_line(candidate: DedupeCandidate, currency: str | None) -> str:
    return (
        f"{h(candidate.date)} {money(candidate.amount, currency)} "
        f"{h(clip(candidate.payee_name, 30))}"
    )


def dedupe_pages(count: int) -> int:
    return max(1, -(-count // DEDUPE_PAGE_SIZE))


def dedupe_empty(scan: DedupeScan) -> str:
    return "\n".join(
        [
            f"📅 {h(scan.window[0])} → {h(scan.window[1])} · {h(clip(scan.csv_name, 60))}",
            f"📋 Statement rows: {scan.statement_rows} · YNAB transactions in that period: "
            f"{scan.ynab_count_in_window}",
            "",
            "✓ No stray imports found.",
        ]
    )


def dedupe_picker(
    scan: DedupeScan, selected: Iterable[int], page: int, *, nonce: str
) -> tuple[str, dict[str, Any]]:
    """Text and keyboard of one picker page; callbacks are bound to ``nonce`` (F20)."""
    chosen = set(selected)
    total = len(scan.candidates)
    pages = dedupe_pages(total)
    page = max(0, min(page, pages - 1))
    lines = [
        f"📅 {h(scan.window[0])} → {h(scan.window[1])} · {h(clip(scan.csv_name, 60))}",
        f"📋 Statement rows: {scan.statement_rows} · YNAB transactions in that period: "
        f"{scan.ynab_count_in_window}",
        "",
        f"⚠ Found {plural(total, 'transaction')} created by the importer that no statement "
        "row accounts for.",
        "Tick the ones to delete, then press 🗑.",
    ]
    if total > MAX_DELETE_IDS:
        lines.append(f"At most {MAX_DELETE_IDS} can be deleted at a time.")
    if pages > 1:
        lines.append(f"Page {page + 1}/{pages}")
    rows: list[list[dict[str, str]]] = []
    start = page * DEDUPE_PAGE_SIZE
    for index in range(start, min(start + DEDUPE_PAGE_SIZE, total)):
        rows.append(
            [
                button(
                    _candidate_label(scan.candidates[index], index in chosen),
                    f"dx:{nonce}:t:{index}",
                )
            ]
        )
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(button("« Prev", f"dx:{nonce}:p:{page - 1}"))
        nav.append(button(f"{page + 1}/{pages}", f"dx:{nonce}:p:{page}"))
        if page < pages - 1:
            nav.append(button("Next »", f"dx:{nonce}:p:{page + 1}"))
        rows.append(nav)
    rows.append([button("✅ All", f"dx:{nonce}:all"), button("⬜ None", f"dx:{nonce}:none")])
    rows.append(
        [
            button(f"🗑 Delete ({len(chosen)})", f"dx:{nonce}:del"),
            button("✖ Cancel", f"dx:{nonce}:c"),
        ]
    )
    return "\n".join(lines), inline_keyboard(rows)


def dedupe_confirm(
    scan: DedupeScan, selected: Iterable[int], *, nonce: str, currency: str | None = None
) -> tuple[str, dict[str, Any]]:
    """The confirmation view: lists the selected transactions (as many as fit one message)
    with their count and total; only this view offers ``ok`` (F20)."""
    indices = sorted(set(selected))
    chosen = [scan.candidates[i] for i in indices if 0 <= i < len(scan.candidates)]
    total_amount = sum(c.amount for c in chosen)
    head = [
        "⚠ <b>Confirm deletion</b>",
        f"Delete these {plural(len(chosen), 'transaction')} "
        f"(total {money(total_amount, currency)}) from YNAB? This cannot be undone.",
        "",
    ]
    budget = MAX_MESSAGE_UNITS - utf16_len("\n".join(head)) - 80
    body: list[str] = []
    for position, candidate in enumerate(chosen):
        line = f"• {_candidate_line(candidate, currency)}"
        cost = utf16_len(line) + 1
        if cost > budget:
            body.append(f"… and {len(chosen) - position} more")
            break
        body.append(line)
        budget -= cost
    keyboard = inline_keyboard(
        [
            [
                button(f"✅ Yes, delete {len(chosen)}", f"dx:{nonce}:ok"),
                button("↩ Back", f"dx:{nonce}:back"),
            ],
            [button("✖ Cancel", f"dx:{nonce}:c")],
        ]
    )
    return "\n".join(head + body), keyboard


def dedupe_delete_result(result: DedupeDeleteResult, currency: str | None = None) -> str:
    attempted = (
        len(result.deleted) + len(result.failures) + len(result.skipped) + len(result.not_attempted)
    )
    lines = [f"✓ Deleted {len(result.deleted)} of {plural(attempted, 'transaction')}."]
    if result.already_deleted:
        lines.append(f"  ({len(result.already_deleted)} were already gone)")
    lines += _listed(
        "⏭ Left alone",
        [f"{_candidate_line(c, currency)}: {h(reason)}" for c, reason in result.skipped],
    )
    lines += _listed(
        "❌ Failed",
        [f"{_candidate_line(c, currency)}: {h(clip(err, 120))}" for c, err in result.failures],
    )
    if result.rate_limited or result.not_attempted:
        lines += [
            "",
            "⏳ YNAB's rate limit was reached; "
            f"{plural(len(result.not_attempted), 'transaction')} not attempted. "
            "Run /dedupe again later.",
        ]
    return "\n".join(lines)


# ── users, settings, help ────────────────────────────────────────────────────


def users_list(users: Sequence[User], *, admin_id: int | None = None) -> str:
    if not users:
        return "No users registered yet."
    lines = ["<b>Registered users:</b>"]
    for user in users:
        icon = _STATE_ICONS.get(user.state, "?")
        name = h(clip(user.first_name or "?", 40))
        username = f" @{h(clip(user.username, 40))}" if user.username else ""
        account = f" → {h(clip(user.account_name or '?', 40))}" if user.state == "ready" else ""
        admin = " (admin)" if admin_id is not None and user.telegram_id == admin_id else ""
        lines.append(
            f"{icon} {code(user.telegram_id)} {name}{username} [{h(user.state)}]{account}{admin}"
        )
    return "\n".join(lines)


def new_user_notice(telegram_id: int, first_name: str | None, username: str | None) -> str:
    """The admin's DM for a new access request (names escaped: F91)."""
    name = h(clip(first_name or "?", 60))
    handle = f" (@{h(clip(username, 40))})" if username else ""
    return (
        "🆕 New access request:\n"
        f"  User: {name}{handle}\n"
        f"  ID: {code(telegram_id)}\n\n"
        f"Reply with /approve {telegram_id} or /deny {telegram_id}"
    )


def settings_menu(user: User) -> tuple[str, dict[str, Any]]:
    """The /settings menu (callback data unchanged from v1: ``st:*``)."""
    auto = bool(user.auto_approve)
    text = (
        "⚙ <b>Settings</b>\n\n"
        "Tap an option below to change it. Changes take effect on the next imported CSV."
    )
    keyboard = inline_keyboard(
        [
            [
                button(
                    f"{'✅' if auto else '⬜'} Auto-approve imports: {'ON' if auto else 'OFF'}",
                    f"st:auto:{0 if auto else 1}",
                )
            ],
            [button("🪙 Crypto setup", "st:crypto")],
            [button("🔧 Re-run YNAB setup", "st:setup")],
            [button("✖ Close", "st:close")],
        ]
    )
    return text, keyboard


def numbered(items: Sequence[str]) -> list[str]:
    return [f"  {i}. {h(item)}" for i, item in enumerate(items, 1)]
