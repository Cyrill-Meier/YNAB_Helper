"""Command-line interface: ``python3 revolut_to_ynab.py …`` / ``python -m ynab_helper cli …``.

Every v1 flag is accepted with its v1 meaning, and when several modes are given the v1
precedence decides which one runs::

    --db-stats > --list-budgets > --list-accounts > --sync > --crypto-sync
      > --brokerage-sync > --watch > --cleanup-pending-memos > --reconcile > import

A flag the chosen mode does not use (or a second mode flag) is reported as a warning
instead of being silently ignored. Settings come from :class:`ynab_helper.config.Settings`
(shell environment, then ``.env`` next to the code); command-line options override them.

The work is done by :mod:`ynab_helper.services`; this module only parses, asks for
confirmation where v1 did, and renders the structured results as text (results on stdout,
warnings and errors on stderr; the log file gets the audit trail).

Exit codes: ``0`` success; ``1`` configuration or usage error (missing settings, bad
option, file not found) or a declined prompt; ``2`` the operation failed or was only partly
applied (YNAB error, rate limit, refused statement, failed rows).

Fixes over v1: ``--skip-pending`` only stops *creating* pending rows and never deletes
pendings already in YNAB (F09); ``--dry-run`` writes nothing to YNAB or the import ledger
(F74); ``--db-stats`` and ``--sync`` use and show the ``--db-path`` database (C07);
``--watch`` honours ``--dry-run``, ``--db-path`` and ``--skip-pending``, imports a file only
once its size is stable, marks it done only after a successful import, retries failed
imports later, and waits out YNAB's rate limit or a file that cannot be read yet instead of
giving up on it (F51).
"""

from __future__ import annotations

import argparse
import dataclasses
import enum
import hashlib
import logging
import os
import sqlite3
import stat
import sys
import time
from collections.abc import Callable, Iterable, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, TextIO

from ynab_helper import config, logging_setup, services
from ynab_helper.balance.adjust import (
    OUTCOME_CREATED,
    OUTCOME_DRY_RUN,
    OUTCOME_DUPLICATE,
    OUTCOME_IN_SYNC,
    PAYEE_RECON,
    AdjustmentResult,
)
from ynab_helper.balance.brokerage import BrokerageSyncResult
from ynab_helper.balance.crypto.sync import CryptoSyncResult
from ynab_helper.balance.reconcile import ReconcilePreview, ReconcileResult, StatementUnavailable
from ynab_helper.errors import (
    Busy,
    ConfigError,
    CsvError,
    YnabAuthError,
    YnabHelperError,
    YnabRateLimited,
)
from ynab_helper.importer.models import Change, ImportResult, TxRef
from ynab_helper.maintenance.cleanup import CleanupResult
from ynab_helper.money import format_money
from ynab_helper.revolut.files import find_latest_revolut_csv
from ynab_helper.revolut.parser import (
    HEADER_SNIFF_BYTES,
    MAX_CSV_BYTES,
    STATE_COMPLETED,
    STATE_PENDING,
    looks_like_revolut_csv,
    parse_export,
    parse_filename_window,
)
from ynab_helper.services import (
    BudgetSummary,
    DatabaseStats,
    UserContext,
    ValuationTarget,
)
from ynab_helper.sync.ynab_sync import SyncResult
from ynab_helper.ynab.models import YnabAccount

__all__ = [
    "EXIT_FAILED",
    "EXIT_OK",
    "EXIT_USAGE",
    "MODES",
    "FolderWatcher",
    "RetryLater",
    "WatchOutcome",
    "WatchResult",
    "backup_dir_for",
    "build_parser",
    "main",
    "select_mode",
]

log = logging.getLogger(__name__)

EXIT_OK: Final = 0
EXIT_USAGE: Final = 1
EXIT_FAILED: Final = 2
EXIT_INTERRUPTED: Final = 130

PROG: Final = "revolut_to_ynab.py"
#: Seconds between two scans of the watched folder (v1: 5 s).
WATCH_INTERVAL: Final = 5.0
#: At most this many entries of each list (created, updated, …) are printed per import.
LIST_LIMIT: Final = 25

#: (mode, argparse dest) in v1 precedence order; "import" is the default mode.
MODES: Final[tuple[tuple[str, str], ...]] = (
    ("db-stats", "db_stats"),
    ("list-budgets", "list_budgets"),
    ("list-accounts", "list_accounts"),
    ("sync", "sync"),
    ("crypto-sync", "crypto_sync"),
    ("brokerage-sync", "brokerage_sync"),
    ("watch", "watch"),
    ("cleanup-pending-memos", "cleanup_pending_memos"),
    ("reconcile", "reconcile"),
)
_MODE_DESTS: Final = frozenset(dest for _, dest in MODES)
_IMPORT = "import"

#: Options each mode uses (logging options are always used). Anything else that was given
#: on the command line is reported as ignored.
_USED: Final[dict[str, frozenset[str]]] = {
    "db-stats": frozenset({"db_path"}),
    "list-budgets": frozenset({"token"}),
    "list-accounts": frozenset({"token", "budget_id"}),
    "sync": frozenset({"token", "budget_id", "account_id", "db_path"}),
    "crypto-sync": frozenset(
        {
            "token",
            "budget_id",
            "crypto_account_id",
            "btc_xpub",
            "eth_address",
            "dry_run",
            "allow_zero",
        }
    ),
    "brokerage-sync": frozenset(
        {
            "token",
            "budget_id",
            "brokerage_account_id",
            "ibkr_account_id",
            "ibkr_base_url",
            "dry_run",
        }
    ),
    "watch": frozenset({"token", "budget_id", "account_id", "db_path", "dry_run", "skip_pending"}),
    "cleanup-pending-memos": frozenset({"token", "budget_id", "account_id", "db_path", "dry_run"}),
    "reconcile": frozenset(
        {"token", "budget_id", "account_id", "db_path", "dry_run", "yes", "csv_file"}
    ),
    _IMPORT: frozenset(
        {
            "token",
            "budget_id",
            "account_id",
            "db_path",
            "dry_run",
            "skip_pending",
            "csv_file",
            "csv_dir",
            "yes",
        }
    ),
}
_ALWAYS_USED: Final = frozenset({"log_level", "log_file"})
#: Why an option is ignored when the generic "has no effect" is not enough.
_IGNORED_BECAUSE: Final[dict[tuple[str, str], str]] = {
    ("sync", "since_date"): "v2 keeps the whole budget in sync incrementally",
    ("reconcile", "csv_dir"): "reconcile uses the newest imported statement",
    ("cleanup-pending-memos", "csv_file"): "the cleanup works on YNAB's current state",
    ("crypto-sync", "db_path"): "valuation syncs need no local database",
    ("brokerage-sync", "db_path"): "valuation syncs need no local database",
}

_EPILOG: Final = f"""\
examples:
  {PROG}                          import the newest CSV in CSV_DIR (asks to confirm)
  {PROG} statement.csv            import a specific statement
  {PROG} -y                       auto-pick the newest CSV without asking
  {PROG} statement.csv --dry-run  show what an import would change, write nothing
  {PROG} --sync                   sync the local mirror with YNAB
  {PROG} --list-budgets           list your budgets (then --list-accounts)
  {PROG} --watch ~/Downloads      import new statements as they arrive
  {PROG} --db-stats               show what the local database holds
  {PROG} --reconcile              reconcile YNAB with the newest imported statement
  {PROG} --cleanup-pending-memos  strip stale '(pending)' memos from cleared transactions
  {PROG} --crypto-sync            post the crypto portfolio value to YNAB
  {PROG} --brokerage-sync         post the Interactive Brokers NAV to YNAB

settings: YNAB_TOKEN, YNAB_BUDGET_ID, YNAB_ACCOUNT_ID, YNAB_CRYPTO_ACCOUNT_ID,
  CRYPTO_BTC_XPUB, CRYPTO_ETH_ADDRESS, ANKR_API_KEY, YNAB_BROKERAGE_ACCOUNT_ID,
  IBKR_BASE_URL, IBKR_ACCOUNT_ID, LOG_LEVEL, LOG_FILE, CSV_DIR — from the environment or
  the .env file next to the program; command-line options win.

exit codes: 0 success, 1 configuration/usage error or declined prompt,
  2 the operation failed or was only partly applied.
"""


# ─── argument parsing ────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    """The v1 argument parser (same flags and meanings) plus ``--allow-zero``."""
    parser = argparse.ArgumentParser(
        prog=PROG,
        description="Import Revolut statements into YNAB (v2).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_EPILOG,
    )
    add = parser.add_argument
    add("csv_file", nargs="?", help="Path to a Revolut CSV statement")
    add("--token", help="YNAB personal access token (or set YNAB_TOKEN)")
    add("--budget-id", help="YNAB budget id (or set YNAB_BUDGET_ID)")
    add("--account-id", help="YNAB account id of your Revolut account (or set YNAB_ACCOUNT_ID)")
    add("--list-budgets", action="store_true", help="List your YNAB budgets")
    add("--list-accounts", action="store_true", help="List the open accounts of the budget")
    add("--watch", metavar="FOLDER", help="Watch a folder and import new Revolut CSVs")
    add("--sync", action="store_true", help="Sync the local mirror with YNAB")
    add("--since-date", help="Ignored (v1 option of --sync; v2 syncs incrementally)")
    add("--dry-run", action="store_true", help="Show what would change; write nothing")
    add(
        "--skip-pending",
        action="store_true",
        help="Do not create pending transactions (pendings already in YNAB are kept)",
    )
    add("--db-stats", action="store_true", help="Show local database statistics")
    add("--db-path", help=f"Local database (default: {config.DEFAULT_CLI_DB_PATH})")
    add("--crypto-sync", action="store_true", help="Post the crypto portfolio value to YNAB")
    add("--crypto-account-id", help="YNAB crypto tracking account (or YNAB_CRYPTO_ACCOUNT_ID)")
    add("--btc-xpub", help="BTC xpub/ypub/zpub or a single address (or CRYPTO_BTC_XPUB)")
    add("--eth-address", help="Ethereum address for ETH and tokens (or CRYPTO_ETH_ADDRESS)")
    add(
        "--allow-zero",
        action="store_true",
        help="--crypto-sync: post even if the Bitcoin wallet shows no transactions at all",
    )
    add("--brokerage-sync", action="store_true", help="Post the Interactive Brokers NAV to YNAB")
    add(
        "--brokerage-account-id",
        help="YNAB brokerage tracking account (or YNAB_BROKERAGE_ACCOUNT_ID)",
    )
    add("--ibkr-account-id", help="IB account id (or IBKR_ACCOUNT_ID; default: the first one)")
    add(
        "--ibkr-base-url",
        help=f"IB Gateway URL (or IBKR_BASE_URL; default: {config.DEFAULT_IBKR_BASE_URL})",
    )
    add(
        "--reconcile",
        action="store_true",
        help="Reconcile YNAB's cleared balance to the newest imported statement",
    )
    add(
        "--cleanup-pending-memos",
        action="store_true",
        help="Strip stale '(pending)' memos from cleared YNAB transactions",
    )
    add("--log-level", help="DEBUG, INFO, WARNING or ERROR (or set LOG_LEVEL)")
    add("--log-file", help='Log file (or set LOG_FILE; "" disables file logging)')
    add("--csv-dir", help="Folder to pick the newest CSV from (or CSV_DIR; default: ~/Downloads)")
    add("-y", "--yes", action="store_true", help="Do not ask for confirmation")
    return parser


def select_mode(args: argparse.Namespace) -> str:
    """The mode that runs, by v1 precedence (``import`` if no mode flag is given)."""
    for mode, dest in MODES:
        if getattr(args, dest):
            return mode
    return _IMPORT


def _option_names(parser: argparse.ArgumentParser) -> dict[str, str]:
    names: dict[str, str] = {}
    for action in parser._actions:
        if action.dest == "help":
            continue
        longs = [o for o in action.option_strings if o.startswith("--")]
        names[action.dest] = longs[0] if longs else "the CSV file argument"
    return names


def ignored_options(
    args: argparse.Namespace, mode: str, parser: argparse.ArgumentParser
) -> list[str]:
    """Warnings for every given option the selected ``mode`` does not use."""
    names = _option_names(parser)
    used = _USED[mode] | _ALWAYS_USED
    running = "an import" if mode == _IMPORT else f"--{mode}"
    warnings: list[str] = []
    for dest, flag in names.items():
        value = getattr(args, dest, None)
        if value is None or value is False:
            continue
        if dest in _MODE_DESTS:
            if dest != dict(MODES).get(mode):
                warnings.append(f"{flag} is ignored: {running} runs instead (one mode per run).")
            continue
        if dest in used:
            continue
        reason = _IGNORED_BECAUSE.get((mode, dest))
        warnings.append(f"{flag} is ignored with {running}" + (f": {reason}." if reason else "."))
    return warnings


# ─── settings ────────────────────────────────────────────────────────────────


def _expand(path: str, environ: MutableMapping[str, str]) -> Path:
    """``~`` expansion against HOME of ``environ`` (as config.Settings does)."""
    home = environ.get("HOME")
    if home and (path == "~" or path.startswith("~/")):
        return Path(home + path[1:])
    return Path(os.path.expanduser(path))


def _apply_overrides(
    settings: config.Settings, args: argparse.Namespace, environ: MutableMapping[str, str]
) -> tuple[config.Settings, list[str]]:
    """Command-line options over the environment (v1: an empty option falls back to it)."""
    warnings: list[str] = []
    log_level = settings.log.level
    if args.log_level is not None:
        normalized = config.normalize_log_level(args.log_level)
        if normalized is None:
            warnings.append(f"--log-level {args.log_level!r} is not a log level; using INFO.")
            log_level = config.DEFAULT_LOG_LEVEL
        else:
            log_level = normalized
    log_file = settings.log.file
    if args.log_file is not None:
        log_file = _expand(args.log_file.strip(), environ) if args.log_file.strip() else None
    ynab = dataclasses.replace(
        settings.ynab,
        token=(args.token or "").strip() or settings.ynab.token,
        budget_id=(args.budget_id or "").strip() or settings.ynab.budget_id,
        account_id=(args.account_id or "").strip() or settings.ynab.account_id,
    )
    crypto = dataclasses.replace(
        settings.crypto,
        account_id=(args.crypto_account_id or "").strip() or settings.crypto.account_id,
        btc_xpub=(args.btc_xpub or "").strip() or settings.crypto.btc_xpub,
        eth_address=(args.eth_address or "").strip() or settings.crypto.eth_address,
    )
    ibkr = dataclasses.replace(
        settings.ibkr,
        brokerage_account_id=(args.brokerage_account_id or "").strip()
        or settings.ibkr.brokerage_account_id,
        base_url=(args.ibkr_base_url or "").strip().rstrip("/") or settings.ibkr.base_url,
        account_id=(args.ibkr_account_id or "").strip() or settings.ibkr.account_id,
    )
    csv_dir = _expand(args.csv_dir.strip(), environ) if (args.csv_dir or "").strip() else None
    updated = dataclasses.replace(
        settings,
        ynab=ynab,
        crypto=crypto,
        ibkr=ibkr,
        log=config.LogSettings(level=log_level, file=log_file),
        csv_dir=csv_dir or settings.csv_dir,
    )
    return updated, warnings


def backup_dir_for(db_path: Path, settings: config.Settings) -> Path | None:
    """Where a migration backs up ``db_path``: the bot's ``<data dir>/backups`` for a
    database in the bot's data directory (an operator running the CLI on a bot user's
    ``transactions_<id>.db``, or the CLI inside the container, whose HOME is the data
    directory), else None — ``<db name>_backups`` next to the database."""
    try:
        if db_path.parent.resolve() == settings.data_dir.resolve():
            return settings.backups_dir
    except (OSError, RuntimeError):  # unresolvable path (symlink loop): keep the default
        pass
    return None


@dataclass(frozen=True, slots=True)
class _Run:
    """Everything a mode handler needs."""

    args: argparse.Namespace
    settings: config.Settings
    db_path: Path

    def user_context(self) -> UserContext:
        ynab = self.settings.ynab
        return UserContext.from_cli(
            token=ynab.token,
            budget_id=ynab.budget_id,
            account_id=ynab.account_id,
            db_path=self.db_path,
            backup_dir=backup_dir_for(self.db_path, self.settings),
        )

    def valuation_target(self, account_id: str) -> ValuationTarget:
        # No db_path: a valuation sync from the CLI touches no local file (as in v1).
        return ValuationTarget(
            key=str(self.db_path),
            ynab_token=self.settings.ynab.token,
            budget_id=self.settings.ynab.budget_id,
            account_id=account_id,
        )


# ─── output helpers ──────────────────────────────────────────────────────────


def _out(text: str = "") -> None:
    print(text, flush=True)


def _err(text: str) -> None:
    print(text, file=sys.stderr, flush=True)


def _money(milli: int, currency: str | None = None, *, sign: bool = False) -> str:
    return format_money(milli, currency, sign=sign)


def _clip(text: str | None, width: int = 40) -> str:
    text = (text or "").strip()
    return text if len(text) <= width else text[: width - 1] + "…"


def _limited(items: Sequence[Any], render: Callable[[Any], str], limit: int) -> list[str]:
    lines = [render(item) for item in items[:limit]]
    if len(items) > limit:
        lines.append(f"    … and {len(items) - limit} more")
    return lines


def _tx_line(mark: str, tx: TxRef, suffix: str = "") -> str:
    return f"  {mark} {tx.date}  {_money(tx.amount):>12}  {_clip(tx.payee)}{suffix}"


def _field_value(name: str, value: Any) -> str:
    if value is None or value == "":
        return "(none)"
    if name == "amount" and isinstance(value, int) and not isinstance(value, bool):
        return _money(value)
    return str(value)


def _change_line(change: Change) -> str:
    parts = [
        f"{name} {_field_value(name, old)} → {_field_value(name, new)}"
        for name, (old, new) in change.fields.items()
    ]
    return f"  Δ {change.tx.date}  {_clip(change.tx.payee)} — {'; '.join(parts)}"


def _retry_hint(retry_after: float | None) -> str:
    if retry_after is None or retry_after <= 0:
        return "Try again later."
    minutes = max(1, round(retry_after / 60))
    return f"Try again in about {minutes} minute{'s' if minutes != 1 else ''}."


def _table(rows: Sequence[tuple[str, str]], indent: str = "   ") -> list[str]:
    """``label: value`` rows, labels padded to one width and values right-aligned."""
    label_width = max(len(label) for label, _ in rows) + 1
    value_width = max(len(value) for _, value in rows)
    return [f"{indent}{label + ':':<{label_width}} {value:>{value_width}}" for label, value in rows]


# ─── rendering ───────────────────────────────────────────────────────────────


def format_import_result(result: ImportResult, *, limit: int = LIST_LIMIT) -> list[str]:
    """Readable summary of an import (v1-style lines; nothing is parsed from it)."""
    lines: list[str] = []
    dry = result.dry_run
    if result.window:
        states = []
        if result.rows_pending:
            states.append(f"{result.rows_pending} pending")
        if result.rows_reverted:
            states.append(f"{result.rows_reverted} reverted")
        detail = f" ({', '.join(states)})" if states else ""
        currency = f", {result.currency}" if result.currency else ""
        lines.append(
            f"   {result.rows_total} transaction(s){detail} from {result.window[0]} "
            f"to {result.window[1]}{currency}"
        )
    sections: list[tuple[str, Sequence[Any], Callable[[Any], str]]] = [
        ("Would create" if dry else "Created", result.created, lambda t: _tx_line("+", t)),
        (
            "Would link" if dry else "Linked to existing YNAB transactions",
            result.linked,
            lambda t: _tx_line("=", t),
        ),
        ("Would update" if dry else "Updated", result.updated, _change_line),
        (
            "Would remove" if dry else "Removed from YNAB",
            result.removed,
            lambda r: _tx_line("✗", r.tx, f" ({r.reason})"),
        ),
    ]
    for title, items, render in sections:
        if items:
            lines.append(f"{title}: {len(items)}")
            lines.extend(_limited(items, render, limit))
    if result.unchanged:
        lines.append(f"Already up to date: {result.unchanged}")
    if result.ignored_deleted:
        lines.append(f"Deleted in YNAB (not re-created): {result.ignored_deleted}")
    if result.skipped_rows:
        lines.append(f"Skipped rows: {len(result.skipped_rows)}")
        lines.extend(_limited(result.skipped_rows, lambda s: f"    data row {s[0]}: {s[1]}", limit))
    if result.warnings:
        lines.append("Warnings:")
        lines.extend(_limited(result.warnings, lambda w: f"  ! {w}", limit))
    if result.failures:
        lines.append(f"Failed: {len(result.failures)}")
        lines.extend(
            _limited(
                result.failures,
                lambda f: (
                    _tx_line("✗", f.tx, f" — {f.action}: {f.error}")
                    if f.tx
                    else f"  ✗ {f.action}: {f.error}"
                ),
                limit,
            )
        )
    if result.rate_limited or result.not_applied:
        lines.append(
            f"{result.not_applied} change(s) were not applied because YNAB's rate limit was "
            "reached. Run the import again later."
        )
    if dry:
        if not result.has_changes:
            lines.append("Dry run: nothing would change.")
        lines.append(
            "Dry run: nothing was written to YNAB and the import ledger is unchanged "
            "(only the local copy of YNAB was refreshed)."
        )
    elif not result.ok:
        lines.append("✗ The import was only partly applied. Run it again to retry the rest.")
    elif not result.has_changes:
        lines.append("✓ Nothing to do: YNAB is already up to date.")
    else:
        lines.append("✓ Import complete.")
    return lines


def format_sync_result(result: SyncResult) -> list[str]:
    if result.rate_limited:
        return [
            f"✗ YNAB's rate limit was reached; nothing was synced. {_retry_hint(result.retry_after)}"
        ]
    lines = [
        f"✓ Synced budget {result.budget_id}: {result.accounts_changed} account change(s), "
        f"{result.tx_changed} transaction change(s)" + (" (full download)." if result.full else ".")
    ]
    refresh = result.refresh
    if refresh is not None and (refresh.linked or refresh.marked_deleted or refresh.conflicts):
        lines.append(
            f"   Import ledger: {refresh.linked} linked, {refresh.marked_deleted} marked as "
            f"deleted in YNAB, {len(refresh.conflicts)} conflict(s)"
        )
    return lines


def format_database_stats(stats: DatabaseStats) -> list[str]:
    lines = [f"Database: {stats.path}"]
    if not stats.migrated:
        lines.append(
            f"   Schema: v1 (user_version {stats.schema_version}) — migrated to v2, with a "
            "backup, by the next import or sync."
        )
    else:
        lines.append(f"   Schema: v2 (user_version {stats.schema_version})")
    if stats.legacy_rows is not None:
        lines.append(f"   v1 archive table 'transactions': {stats.legacy_rows} row(s)")
    if not stats.migrated:
        return lines
    dates = f", {stats.ledger_dates[0]} → {stats.ledger_dates[1]}" if stats.ledger_dates else ""
    lines.append(f"   Import ledger: {stats.ledger_total} statement row(s){dates}")
    for account_id, counts in stats.ledger.items():
        label = f"account {account_id}" if account_id else "not yet claimed by an import"
        detail = ", ".join(f"{status} {count}" for status, count in sorted(counts.items()))
        lines.append(f"     {label}: {detail}")
    if not stats.budgets:
        lines.append("   YNAB mirror: never synced (run --sync)")
    for budget in stats.budgets:
        currency = f" ({budget.currency})" if budget.currency else ""
        lines.append(
            f"   Budget {budget.budget_id}{currency}: {budget.accounts} account(s), "
            f"{budget.transactions} transaction(s) (+{budget.deleted_transactions} deleted)"
        )
        if budget.last_sync_at:
            synced = budget.last_sync_at
        elif budget.server_knowledge is not None:
            synced = "outdated (YNAB was written to since; run --sync)"
        else:
            synced = "never"
        knowledge = budget.server_knowledge
        lines.append(
            f"     last sync: {synced}"
            + (f", server knowledge {knowledge}" if knowledge is not None else "")
        )
    latest = stats.latest_statement
    if latest is None:
        lines.append(f"   Statements: {stats.statements}")
    else:
        name = latest.original_name or Path(latest.stored_path).name
        lines.append(
            f"   Statements: {stats.statements}; newest: {name} ({latest.status}, "
            f"uploaded {latest.uploaded_at})"
        )
    return lines


def format_budgets(budgets: Sequence[BudgetSummary]) -> list[str]:
    lines = [f"Found {len(budgets)} budget(s):", ""]
    for budget in budgets:
        lines += [
            f"  Name:     {budget.name}",
            f"  ID:       {budget.id}",
            f"  Currency: {budget.currency or '?'}",
            "",
        ]
    return lines


def format_accounts(accounts: Sequence[YnabAccount]) -> list[str]:
    lines = [f"Found {len(accounts)} open account(s):", ""]
    for account in accounts:
        balance = _money(account.balance) if account.balance is not None else "?"
        lines += [
            f"  Name:    {account.name}",
            f"  ID:      {account.id}",
            f"  Type:    {account.type or '?'}",
            f"  Balance: {balance}",
            "",
        ]
    return lines


def format_reconcile_preview(preview: ReconcilePreview) -> list[str]:
    cur = preview.currency
    lines = [f"Statement: {preview.csv_name} (balance as of {preview.as_of})"]
    lines += _table(
        [
            ("Statement balance", _money(preview.csv_balance, cur)),
            (f"YNAB cleared balance on {preview.as_of}", _money(preview.ynab_cleared_as_of, cur)),
            ("Difference", _money(preview.delta, cur, sign=True)),
        ]
    )
    if preview.cleared_after_as_of:
        lines.append(
            f"   (YNAB's cleared balance today is {_money(preview.ynab_cleared_now, cur)}; "
            f"{_money(preview.cleared_after_as_of, cur, sign=True)} of it is dated after "
            f"{preview.as_of} and not compared)"
        )
    if preview.problems:
        lines.append("Check before confirming:")
        lines.extend(f"  ! {problem}" for problem in preview.problems)
    return lines


def _adjustment_line(adjustment: AdjustmentResult, currency: str | None) -> str:
    delta = _money(adjustment.delta_milli, currency, sign=True)
    if adjustment.outcome == OUTCOME_CREATED:
        return f"✓ Posted '{adjustment.payee}' {delta} dated {adjustment.date} ({adjustment.import_id})."
    if adjustment.outcome == OUTCOME_DUPLICATE:
        return f"✓ Already posted earlier: '{adjustment.payee}' {delta} dated {adjustment.date}."
    if adjustment.outcome == OUTCOME_DRY_RUN:
        return f"Dry run: would post '{adjustment.payee}' {delta} dated {adjustment.date}."
    if adjustment.outcome == OUTCOME_IN_SYNC:
        return "✓ Already in sync; nothing to post."
    return f"Outcome: {adjustment.outcome}"


def format_reconcile_result(result: ReconcileResult) -> list[str]:
    return [_adjustment_line(result.adjustment, result.preview.currency)]


def format_cleanup_result(result: CleanupResult, *, limit: int = LIST_LIMIT) -> list[str]:
    lines: list[str] = []
    if not result.candidates:
        lines.append("✓ No cleared transaction has a stale '(pending)' memo.")
    else:
        lines.append(
            f"Cleared transactions with a stale '(pending)' memo: {len(result.candidates)}"
        )
        lines.extend(
            _limited(
                result.candidates,
                lambda f: (
                    f"  {f.date}  {_money(f.amount):>12}  {_clip(f.payee_name)}: "
                    f"{f.old_memo!r} → {f.new_memo!r}"
                ),
                limit,
            )
        )
    if result.skipped_uncleared:
        lines.append(
            f"{result.skipped_uncleared} uncleared transaction(s) with the marker were left "
            "alone (they may still be pending)."
        )
    if result.failures:
        lines.append(f"Failed: {len(result.failures)}")
        lines.extend(
            _limited(
                result.failures,
                lambda f: f"  ✗ {f.fix.date} {_clip(f.fix.payee_name)}: {f.error}",
                limit,
            )
        )
    if result.rate_limited or result.not_applied:
        lines.append(
            f"{result.not_applied} memo(s) were not updated because YNAB's rate limit was "
            "reached. Run the cleanup again later."
        )
    if result.dry_run:
        lines.append("Dry run: nothing was changed in YNAB.")
    elif result.updated:
        lines.append(f"✓ Updated {len(result.updated)} memo(s).")
    return lines


def _amount_text(amount: Decimal) -> str:
    text = format(amount.normalize(), "f")
    return text


def format_crypto_result(result: CryptoSyncResult) -> list[str]:
    lines = ["Crypto portfolio (CHF):"]
    for holding in result.holdings:
        lines.append(
            f"   {holding.symbol:<6} {_amount_text(holding.amount):>18} × "
            f"{format_money_decimal(holding.price):>12} = {_money(holding.value_milli):>14}"
        )
    if result.zero_balances:
        lines.append(f"   No balance: {', '.join(result.zero_balances)}")
    if result.btc is not None:
        btc = result.btc
        lines.append(
            f"   Bitcoin ({btc.kind}): {btc.used_addresses} used address(es), {btc.scanned} checked"
        )
    lines += _table(
        [
            ("Portfolio value", _money(result.portfolio_milli, "CHF")),
            ("YNAB balance (before)", _money(result.ynab_before_milli, "CHF")),
            ("Difference", _money(result.adjustment.delta_milli, "CHF", sign=True)),
        ]
    )
    if result.ynab_uncleared_milli:
        lines.append(
            "   (YNAB's balance includes uncleared "
            f"{_money(result.ynab_uncleared_milli, 'CHF', sign=True)})"
        )
    lines.extend(f"  ! {warning}" for warning in result.warnings)
    lines.append(_adjustment_line(result.adjustment, "CHF"))
    return lines


def format_brokerage_result(result: BrokerageSyncResult) -> list[str]:
    return [
        f"IBKR account {result.ibkr_account_id}:",
        *_table(
            [
                ("Net asset value", _money(result.nav_milli, "CHF")),
                ("YNAB balance (before)", _money(result.ynab_before_milli, "CHF")),
                ("Difference", _money(result.adjustment.delta_milli, "CHF", sign=True)),
            ]
        ),
        _adjustment_line(result.adjustment, "CHF"),
    ]


def format_money_decimal(value: Decimal) -> str:
    """A CHF price with grouping and two decimals (display only)."""
    return f"{value:,.2f}"


# ─── statement selection (v1 confirm_csv_selection) ──────────────────────────


def describe_csv(path: Path, *, recent: int = 3) -> list[str]:
    """What v1 showed before asking "Use this file?": name, export date, size, rows."""
    lines = ["", "Newest Revolut CSV found:", f"   File:       {path}"]
    window = parse_filename_window(path.name)
    if window:
        lines.append(f"   Export to:  {window[1]}")
    try:
        info = path.stat()
        lines.append(f"   Modified:   {datetime.fromtimestamp(info.st_mtime):%Y-%m-%d %H:%M}")
        lines.append(f"   Size:       {max(1, info.st_size // 1024):,} KB")
    except OSError:
        pass
    try:
        export = parse_export(path, filename=path.name)
    except CsvError as exc:
        lines.append(f"   ! It cannot be read: {exc.user_message}")
        return lines
    pending = sum(1 for row in export.rows if row.state == STATE_PENDING)
    suffix = f" ({pending} pending)" if pending else ""
    lines.append(f"   Transactions: {len(export.rows)}{suffix}")
    newest = sorted(export.rows, key=lambda row: row.date, reverse=True)[:recent]
    if newest:
        lines += ["", "   Most recent transactions:"]
        for row in newest:
            mark = "✓" if row.state == STATE_COMPLETED else "◌"
            lines.append(f"     {mark} {row.date}  {_money(row.amount):>10}  {_clip(row.payee)}")
    return lines


def _confirm(question: str, *, default: bool) -> bool:
    """Ask on stdin; no answer possible (no TTY) counts as "no"."""
    try:
        answer = input(question).strip().lower()
    except EOFError:
        _out("")
        _out("(no terminal to answer — rerun with --yes to skip the question)")
        return False
    if not answer:
        return default
    return answer in ("y", "yes")


def _resolve_statement(run: _Run) -> Path | None:
    """The statement to import: the positional file, else the newest CSV in CSV_DIR after
    confirmation (``-y`` skips it). None (after a message) if there is none or it was declined."""
    args = run.args
    if args.csv_file:
        path = Path(args.csv_file).expanduser()
        if not path.is_file():
            _err(f"Error: file not found: {path}")
            return None
        return path.resolve()
    csv_dir = run.settings.csv_dir
    latest = find_latest_revolut_csv(csv_dir)
    if latest is None:
        _err(f"Error: no Revolut CSV found in {csv_dir}.")
        _err("  Pass a file explicitly, or set CSV_DIR / --csv-dir to the right folder.")
        return None
    for line in describe_csv(latest):
        _out(line)
    if args.yes:
        _out("   → Auto-confirmed (--yes)")
    elif not _confirm("\nUse this file? [Y/n]: ", default=True):
        _err("Aborted — pass the CSV path as an argument to import another file.")
        return None
    return latest.resolve()


# ─── folder watching (F51) ───────────────────────────────────────────────────


class WatchOutcome(enum.Enum):
    """What :class:`FolderWatcher` does with a file after its handler ran."""

    DONE = "done"  # imported: never again unless the file changes
    SKIP = "skip"  # not importable as it is (wrong file, refused): only after it changes
    RETRY = "retry"  # failed for a reason that may pass (network, YNAB error, partial)


@dataclass(frozen=True, slots=True)
class RetryLater:
    """Handler result for a failure that passes on its own: YNAB's rate limit, or a file
    that cannot be read yet (locked by the app that wrote it, a cloud placeholder still
    downloading).

    The file is tried again after ``after`` seconds (YNAB's ``Retry-After``, capped at the
    watcher's ``max_wait``) or the watcher's backoff, whichever is longer. Unlike
    :attr:`WatchOutcome.RETRY` these waits never count toward ``max_attempts``: a statement
    dropped while the token is rate limited is imported once the limit lifts (F51).
    """

    after: float | None = None


#: What a :class:`FolderWatcher` handler returns.
WatchResult = WatchOutcome | RetryLater


@dataclass(slots=True)
class _Candidate:
    signature: tuple[int, int]  # (size, mtime_ns)
    attempts: int = 0  # RETRY results (bounded by max_attempts)
    waits: int = 0  # RetryLater results (unbounded: they pass on their own)
    retry_at: float = 0.0


def _duration(seconds: float) -> str:
    """``"60 s"``, ``"240 s"``, ``"15 min"``, ``"1 h"`` for watch messages."""
    if seconds < 300:
        return f"{seconds:.0f} s"
    if seconds < 3600:
        return f"{round(seconds / 60)} min"
    hours = seconds / 3600
    return f"{hours:.0f} h" if hours == round(hours) else f"{hours:.1f} h"


class FolderWatcher:
    """Polls ``folder`` for new Revolut statements and hands each to ``handler`` once.

    * Files present when watching starts (:meth:`prime`) are left alone, as in v1.
    * A new or changed ``*.csv`` file is handled only when its size and mtime are the same
      on two consecutive polls — an AirDrop or download still in progress is not read
      half-written (F51).
    * A file counts as done only after the handler reports :attr:`WatchOutcome.DONE` (or
      ``SKIP``: readable but not a Revolut statement, or refused as it is); until the file
      changes again it is not handled a second time. ``RETRY`` schedules another attempt
      with exponential backoff (``retry_delay`` doubling up to ``max_retry_delay``); after
      ``max_attempts`` the file waits until it changes.
    * A file that cannot be read for the moment, and a handler result :class:`RetryLater`
      (rate limit), wait with the same backoff (or the requested delay, up to ``max_wait``)
      but are never given up on — such a condition clears without the file changing.
    """

    def __init__(
        self,
        folder: Path,
        handler: Callable[[Path], WatchResult],
        *,
        retry_delay: float = 60.0,
        max_retry_delay: float = 900.0,
        max_attempts: int = 5,
        max_wait: float = 3600.0,
        clock: Callable[[], float] | None = None,
        notify: Callable[[str], None] | None = None,
    ) -> None:
        self.folder = Path(folder)
        self._handler = handler
        self._retry_delay = retry_delay
        self._max_retry_delay = max_retry_delay
        self._max_attempts = max(1, max_attempts)
        self._max_wait = max_wait
        self._clock = clock or time.monotonic
        self._notify = notify or (lambda _message: None)
        self._done: dict[str, tuple[int, int]] = {}
        self._pending: dict[str, _Candidate] = {}
        self._unlisted = False

    def _scan(self) -> tuple[dict[str, tuple[Path, tuple[int, int]]], set[str]] | None:
        """``(found, unreadable)``: every ``*.csv`` file with its (size, mtime_ns), and the
        names that could not be examined this time. None if the folder cannot be listed
        (unmounted drive, cloud folder offline, …)."""
        try:
            entries = sorted(self.folder.iterdir())
        except OSError as exc:
            if not self._unlisted:
                log.warning("cannot list %s: %s", self.folder, exc)
                self._notify(f"Cannot read {self.folder} right now; still watching.")
            self._unlisted = True
            return None
        if self._unlisted:
            log.info("%s can be listed again", self.folder)
            self._unlisted = False
        found: dict[str, tuple[Path, tuple[int, int]]] = {}
        unreadable: set[str] = set()
        for path in entries:
            name = path.name
            if name.startswith(".") or not name.lower().endswith(".csv"):
                continue
            try:
                info = path.stat()
            except OSError:
                unreadable.add(name)
                continue
            if stat.S_ISREG(info.st_mode):
                found[name] = (path, (info.st_size, info.st_mtime_ns))
        return found, unreadable

    def prime(self) -> int:
        """Mark every statement already in the folder as done; returns how many."""
        scan = self._scan()
        if scan is not None:
            for name, (_path, signature) in scan[0].items():
                self._done[name] = signature
        return len(self._done)

    def poll(self) -> list[tuple[Path, WatchResult]]:
        """One scan: handle every file that is new (or changed) and has stopped growing.

        A file is forgotten only once a listing no longer shows it; while the folder (or a
        file) cannot be read, everything already handled stays handled, so a folder that
        comes back is not imported a second time."""
        now = self._clock()
        handled: list[tuple[Path, WatchResult]] = []
        scan = self._scan()
        if scan is None:
            return handled
        found, unreadable = scan
        for name, (path, signature) in found.items():
            if self._done.get(name) == signature:
                continue
            candidate = self._pending.get(name)
            if candidate is None or candidate.signature != signature:
                # First sighting, or still being written: look again on the next poll.
                self._pending[name] = _Candidate(signature)
                continue
            if signature[0] == 0 or now < candidate.retry_at:
                continue
            outcome = self._handle(path)
            handled.append((path, outcome))
            if isinstance(outcome, RetryLater):
                candidate.waits += 1
                requested = min(outcome.after or 0.0, self._max_wait)
                delay = max(requested, self._backoff(candidate.waits))
                candidate.retry_at = now + delay
                self._notify(f"Will retry {name} in {_duration(delay)}.")
            elif outcome is WatchOutcome.RETRY:
                candidate.attempts += 1
                if candidate.attempts >= self._max_attempts:
                    self._finish(name, signature)
                    self._notify(
                        f"Giving up on {name} after {candidate.attempts} attempts; it is "
                        "tried again when the file changes."
                    )
                else:
                    delay = self._backoff(candidate.attempts)
                    candidate.retry_at = now + delay
                    self._notify(f"Will retry {name} in {_duration(delay)}.")
            else:
                self._finish(name, signature)
        listed = found.keys() | unreadable
        for registry in (self._pending, self._done):
            for name in [n for n in registry if n not in listed]:
                del registry[name]
        return handled

    def _backoff(self, count: int) -> float:
        """``retry_delay`` doubled per earlier try, at most ``max_retry_delay``. The exponent
        is clamped: waits are unbounded, and ``60.0 * 2**1024`` would overflow a float."""
        return min(self._retry_delay * 2 ** min(max(0, count - 1), 30), self._max_retry_delay)

    def _handle(self, path: Path) -> WatchResult:
        try:
            with path.open("rb") as handle:
                head = handle.read(HEADER_SNIFF_BYTES)
        except OSError as exc:
            # Locked by the app writing it, permissions not set yet, a cloud placeholder
            # still downloading: no verdict on the content, look again later (F51).
            log.info("watch: cannot read %s yet: %s", path.name, exc)
            self._notify(f"Cannot read {path.name} yet ({exc.strerror or exc}).")
            return RetryLater()
        if not looks_like_revolut_csv(head):
            self._notify(f"Ignoring {path.name}: not a Revolut statement.")
            return WatchOutcome.SKIP
        return self._handler(path)

    def _finish(self, name: str, signature: tuple[int, int]) -> None:
        self._done[name] = signature
        self._pending.pop(name, None)

    def run(self, *, interval: float = WATCH_INTERVAL) -> None:
        """Poll forever (until KeyboardInterrupt), ``interval`` seconds apart."""
        while True:
            self.poll()
            time.sleep(interval)


# ─── modes ───────────────────────────────────────────────────────────────────


def _run_db_stats(run: _Run) -> int:
    stats = services.database_stats(run.db_path)
    if not stats.exists:
        _err(f"Error: no database at {run.db_path} (use --db-path to choose another one).")
        return EXIT_USAGE
    for line in format_database_stats(stats):
        _out(line)
    return EXIT_OK


def _run_list_budgets(run: _Run) -> int:
    for line in format_budgets(services.list_budgets(run.settings.ynab.token)):
        _out(line)
    return EXIT_OK


def _run_list_accounts(run: _Run) -> int:
    accounts = services.list_accounts(run.settings.ynab.token, run.settings.ynab.budget_id)
    for line in format_accounts(accounts):
        _out(line)
    return EXIT_OK


def _run_sync(run: _Run) -> int:
    _out(f"Syncing with YNAB into {run.db_path} …")
    result = services.sync(run.user_context(), force=True)
    assert result is not None  # a forced sync is never throttled
    for line in format_sync_result(result):
        _out(line)
    for line in format_database_stats(services.database_stats(run.db_path)):
        _out(line)
    return EXIT_FAILED if result.rate_limited else EXIT_OK


def _run_crypto(run: _Run) -> int:
    crypto = run.settings.crypto
    result = services.crypto_sync(
        run.valuation_target(crypto.account_id),
        btc_xpub=crypto.btc_xpub or None,
        eth_address=crypto.eth_address or None,
        dry_run=run.args.dry_run,
        allow_zero=run.args.allow_zero,
        ankr_api_key=crypto.ankr_api_key,
    )
    for line in format_crypto_result(result):
        _out(line)
    return EXIT_OK


def _run_brokerage(run: _Run) -> int:
    ibkr = run.settings.ibkr
    result = services.brokerage_sync(
        run.valuation_target(ibkr.brokerage_account_id),
        ibkr_base_url=ibkr.base_url,
        ibkr_account_id=ibkr.account_id or None,
        dry_run=run.args.dry_run,
    )
    for line in format_brokerage_result(result):
        _out(line)
    return EXIT_OK


def _run_cleanup(run: _Run) -> int:
    result = services.cleanup_pending(run.user_context(), dry_run=run.args.dry_run)
    for line in format_cleanup_result(result):
        _out(line)
    return EXIT_OK if result.ok else EXIT_FAILED


def _file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        data = handle.read(MAX_CSV_BYTES + 1)
    return hashlib.sha256(data).hexdigest()


def _run_reconcile(run: _Run) -> int:
    args = run.args
    ctx = run.user_context()
    expected: tuple[Path, str] | None = None
    if args.csv_file:
        path = Path(args.csv_file).expanduser()
        if not path.is_file():
            _err(f"Error: file not found: {path}")
            return EXIT_USAGE
        expected = (path, _file_sha256(path))
    preview = services.reconcile_preview(ctx)
    if expected is not None and preview.csv_sha256 != expected[1]:
        _err(
            f"Error: {expected[0].name} is not the newest imported statement "
            f"({preview.csv_name}); reconcile only uses an imported statement."
        )
        _err(f"  Import it first: {PROG} {expected[0]}")
        return EXIT_USAGE
    for line in format_reconcile_preview(preview):
        _out(line)
    if preview.in_sync or preview.token is None:
        _out("✓ YNAB's cleared balance matches the statement. Nothing to post.")
        return EXIT_OK
    if args.dry_run:
        _out("Dry run: nothing was posted.")
        return EXIT_OK
    question = (
        f"\nPost a '{PAYEE_RECON}' adjustment of "
        f"{_money(preview.delta, preview.currency, sign=True)} dated {preview.as_of}? [y/N]: "
    )
    if not args.yes and not _confirm(question, default=False):
        _err("Nothing was posted.")
        return EXIT_USAGE
    result = services.reconcile_apply(ctx, preview.token)
    for line in format_reconcile_result(result):
        _out(line)
    return EXIT_OK


def _run_import(run: _Run) -> int:
    path = _resolve_statement(run)
    if path is None:
        return EXIT_USAGE
    _out(f"\nReading: {path.name}")
    if run.args.dry_run:
        _out("   (dry run)")
    result = services.import_csv(
        run.user_context(),
        path,
        original_name=path.name,
        source_kind="cli",
        dry_run=run.args.dry_run,
        skip_pending=run.args.skip_pending,
    )
    for line in format_import_result(result):
        _out(line)
    return EXIT_OK if result.ok else EXIT_FAILED


def _watch_handler(run: _Run, ctx: UserContext) -> Callable[[Path], WatchResult]:
    def handle(path: Path) -> WatchResult:
        _out(f"\nNew Revolut statement: {path.name}")
        try:
            result = services.import_csv(
                ctx,
                path,
                original_name=path.name,
                source_kind="cli",
                dry_run=run.args.dry_run,
                skip_pending=run.args.skip_pending,
            )
        except YnabAuthError:
            raise  # the token is unusable for every later file too
        except YnabRateLimited as exc:
            # Nothing was written; the limit lifts on its own, so the file is never given up.
            _err(f"  ✗ {path.name}: {exc.user_message}")
            return RetryLater(exc.retry_after)
        except CsvError as exc:
            _err(f"  ✗ {path.name}: {exc.user_message}")
            if isinstance(exc.__cause__, OSError):
                return RetryLater()  # could not be read for the moment: not a verdict (F51)
            return WatchOutcome.SKIP  # refused as it is: only after the file changes
        except ConfigError as exc:
            # The account was deleted, closed or moved: no file can be imported as it is.
            _err(f"  ✗ {path.name}: {exc.user_message}")
            return WatchOutcome.SKIP
        except (YnabHelperError, OSError, sqlite3.Error) as exc:
            message = getattr(exc, "user_message", None) or str(exc)
            _err(f"  ✗ {path.name}: {message}")
            return WatchOutcome.RETRY
        except Exception:
            # Like v1, one file never stops the watch; retries are bounded per file.
            log.exception("watch: importing %s failed unexpectedly", path.name)
            _err(f"  ✗ {path.name}: unexpected failure (details in the log)")
            return WatchOutcome.RETRY
        for line in format_import_result(result):
            _out(line)
        if result.ok:
            return WatchOutcome.DONE
        # Partly applied: the next run converges. A rate limit mid-way is waited out.
        return RetryLater() if result.rate_limited else WatchOutcome.RETRY

    return handle


def _run_watch(run: _Run) -> int:
    folder = Path(run.args.watch).expanduser()
    if not folder.is_dir():
        _err(f"Error: {folder} is not a directory.")
        return EXIT_USAGE
    folder = folder.resolve()
    ctx = run.user_context()
    watcher = FolderWatcher(folder, _watch_handler(run, ctx), notify=_out)
    existing = watcher.prime()
    _out(f"Watching {folder} for new Revolut statements (every {WATCH_INTERVAL:.0f} s).")
    _out(f"   Database: {run.db_path}; {existing} existing CSV file(s) are left alone.")
    if run.args.dry_run:
        _out("   Dry run: nothing is written to YNAB or the import ledger.")
    if run.args.skip_pending:
        _out("   Pending transactions are not created (--skip-pending).")
    _out("   Press Ctrl+C to stop.")
    try:
        watcher.run(interval=WATCH_INTERVAL)
    except KeyboardInterrupt:
        _out("\nStopped watching.")
    return EXIT_OK


_HANDLERS: Final[dict[str, Callable[[_Run], int]]] = {
    "db-stats": _run_db_stats,
    "list-budgets": _run_list_budgets,
    "list-accounts": _run_list_accounts,
    "sync": _run_sync,
    "crypto-sync": _run_crypto,
    "brokerage-sync": _run_brokerage,
    "watch": _run_watch,
    "cleanup-pending-memos": _run_cleanup,
    "reconcile": _run_reconcile,
    _IMPORT: _run_import,
}


# ─── entry point ─────────────────────────────────────────────────────────────


def _report(exc: BaseException) -> int:
    """Print a handled error; returns the exit code."""
    if isinstance(exc, ConfigError):
        _err(f"Error: {exc.user_message}")
        return EXIT_USAGE
    if isinstance(exc, StatementUnavailable):
        _err(f"Error: {exc.user_message}")
        if exc.reason in ("no_statement", "not_imported", "missing_file", "file_changed"):
            _err(f"  Import the statement first: {PROG} <statement.csv>")
        return EXIT_FAILED
    if isinstance(exc, YnabRateLimited):
        _err(f"Error: {exc.user_message} {_retry_hint(exc.retry_after)}")
        return EXIT_FAILED
    if isinstance(exc, Busy):
        _err(f"Error: {exc.user_message}")
        return EXIT_FAILED
    if isinstance(exc, YnabHelperError):
        _err(f"Error: {exc.user_message}")
        return EXIT_FAILED
    _err(f"Error: {exc}")
    return EXIT_FAILED


def _print_lines(lines: Iterable[str], stream: TextIO) -> None:
    for line in lines:
        print(line, file=stream)


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: MutableMapping[str, str] | None = None,
    env_file: str | os.PathLike[str] | None = None,
) -> int:
    """Run the CLI and return the exit code (never calls ``sys.exit``).

    ``environ`` (default ``os.environ``) receives the ``.env`` file (``env_file``, default
    ``<repo root>/.env``; existing variables win) and is the source of all settings. Files
    created by the CLI are private (umask 077, restored on return).
    """
    parser = build_parser()
    try:
        args = parser.parse_args(None if argv is None else list(argv))
    except SystemExit as exc:  # --help, or a usage error argparse already printed
        return EXIT_OK if exc.code in (0, None) else EXIT_USAGE
    previous_umask = os.umask(0o077)
    try:
        return _main(parser, args, os.environ if environ is None else environ, env_file)
    finally:
        os.umask(previous_umask)


def _main(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    environ: MutableMapping[str, str],
    env_file: str | os.PathLike[str] | None,
) -> int:
    config.load_dotenv(env_file, environ=environ)
    settings, warnings = _apply_overrides(config.Settings.from_env(environ), args, environ)
    logging_setup.setup_logging(
        settings.log.level,
        settings.log.file,
        console_level="WARNING",
        secrets=settings.secret_values(),
    )
    mode = select_mode(args)
    warnings = settings.problems_for("LOG_LEVEL", "LOG_FILE", "CSV_DIR") + warnings
    warnings += ignored_options(args, mode, parser)
    _print_lines((f"Warning: {w}" for w in warnings), sys.stderr)
    errors = settings.validate_for_cli(mode)
    if errors:
        _print_lines((f"Error: {e}" for e in errors), sys.stderr)
        return EXIT_USAGE
    db_path = _expand(args.db_path or config.DEFAULT_CLI_DB_PATH, environ).absolute()
    log.info("cli: mode=%s db=%s dry_run=%s", mode, db_path, args.dry_run)
    try:
        return _HANDLERS[mode](_Run(args=args, settings=settings, db_path=db_path))
    except KeyboardInterrupt:
        _err("\nInterrupted.")
        return EXIT_INTERRUPTED
    except (YnabHelperError, OSError, sqlite3.Error) as exc:
        log.info("cli: %s failed: %s", mode, exc)
        return _report(exc)
    except Exception:
        log.exception("cli: %s failed unexpectedly", mode)
        _err("Error: unexpected failure (details in the log).")
        return EXIT_FAILED
