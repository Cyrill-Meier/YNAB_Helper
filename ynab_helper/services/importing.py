"""Statement import (docs/DESIGN.md §7 ``import_csv``)."""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

from ynab_helper.errors import CsvError, YnabHelperError
from ynab_helper.importer.executor import ProgressCallback, apply_plan
from ynab_helper.importer.models import ImportResult, PlanOptions
from ynab_helper.importer.planner import compute_allow_sweep, plan_import
from ynab_helper.locks import LOCKS
from ynab_helper.maintenance import dedupe as _dedupe
from ynab_helper.revolut.files import store_upload
from ynab_helper.revolut.parser import MAX_CSV_BYTES, ParsedExport, parse_export
from ynab_helper.services import _common
from ynab_helper.services.context import UserContext
from ynab_helper.storage.csv_files import CsvFileStore
from ynab_helper.storage.ledger import LedgerEntry, LedgerStore
from ynab_helper.storage.mirror import MirrorStore, MirrorView
from ynab_helper.sync.ynab_sync import sync_and_refresh
from ynab_helper.ynab.client import YnabClient

__all__ = ["SOURCE_KINDS", "SourceKind", "import_csv"]

log = logging.getLogger(__name__)

SourceKind = Literal["telegram", "web", "cli"]
#: Where an uploaded statement came from (stored in ``csv_files.source``).
SOURCE_KINDS: Final = frozenset({"telegram", "web", "cli"})

OP_IMPORT: Final = "import"


def import_csv(
    ctx: UserContext,
    source: bytes | bytearray | memoryview | str | os.PathLike[str],
    *,
    original_name: str | None,
    source_kind: SourceKind,
    dry_run: bool = False,
    skip_pending: bool = False,
    client: YnabClient | None = None,
    on_progress: ProgressCallback | None = None,
) -> ImportResult:
    """Import a Revolut statement into the user's YNAB account (§7).

    Under the user's ``import`` lock: store the upload (``csv_dir``; the CLI imports the file
    in place), check its currency against the budget's (refused before any YNAB write, F14),
    sync the mirror (forced), check that the account still exists, is open and belongs to the
    budget (refused before any write), claim unassigned legacy ledger rows and refresh the
    ledger from the mirror, plan and apply. The statement's ``csv_files`` row ends ``imported`` (``result.ok``),
    ``partial`` (some changes failed or were not applied) or ``failed`` (an exception, which
    is re-raised).

    ``dry_run`` sends no write to YNAB and changes neither the ledger nor ``csv_files``, and
    the upload is not stored (F74); it still syncs the mirror (a cache) and plans exactly what
    a real run would (claiming and the ledger refresh are simulated and rolled back).

    Raises :class:`~ynab_helper.errors.Busy`, :class:`~ynab_helper.errors.CsvError` (invalid
    statement, wrong or unknown currency), :class:`~ynab_helper.errors.ConfigError` (the
    account was deleted or closed in YNAB, or is in another budget),
    :class:`~ynab_helper.errors.YnabRateLimited` (the sync before planning was rate limited;
    nothing was written), other :class:`~ynab_helper.errors.YnabError` subclasses and
    ``ValueError`` for an unknown ``source_kind``.
    """
    if source_kind not in SOURCE_KINDS:
        raise ValueError(f"unknown source_kind {source_kind!r}")
    with LOCKS.operation(ctx.key, OP_IMPORT):
        return _import_csv_unlocked(
            ctx,
            source,
            original_name=original_name,
            source_kind=source_kind,
            dry_run=dry_run,
            skip_pending=skip_pending,
            client=client,
            on_progress=on_progress,
        )


@dataclass(slots=True)
class _Statement:
    export: ParsedExport
    stored_path: Path | None
    display_name: str | None


def _read_capped(path: Path) -> bytes:
    """The file's bytes; :class:`CsvError` above the statement size limit."""
    try:
        with path.open("rb") as handle:
            data = handle.read(MAX_CSV_BYTES + 1)
    except OSError as exc:
        raise CsvError(
            f"cannot read statement {path.name}: {exc.strerror or exc}",
            user_message="The statement file could not be read.",
        ) from exc
    if len(data) > MAX_CSV_BYTES:
        raise CsvError(
            f"statement is larger than {MAX_CSV_BYTES} bytes",
            user_message="The statement is larger than 25 MB.",
        )
    return data


def _newest_imported(files: CsvFileStore, account_id: str) -> list[Path]:
    """The newest statement of the account that was imported successfully (kept on disk by
    retention: reconcile is based on it)."""
    for record in files.recent(account_id=account_id, limit=50):
        if record.status == "imported":
            return [Path(record.stored_path)]
    return []


def _load_statement(
    ctx: UserContext,
    source: bytes | bytearray | memoryview | str | os.PathLike[str],
    original_name: str | None,
    files: CsvFileStore,
    *,
    dry_run: bool,
) -> _Statement:
    is_bytes = isinstance(source, bytes | bytearray | memoryview)
    path = None if is_bytes else Path(os.fspath(source)).expanduser()  # type: ignore[arg-type]
    name = original_name if original_name is not None else (path.name if path else None)
    if ctx.csv_dir is None or dry_run:
        # The CLI imports the file where it is; a dry run must not store (or prune) anything.
        if path is not None and ctx.csv_dir is None:
            export = parse_export(path, filename=name)
            return _Statement(export, path.resolve(), name)
        data = bytes(source) if is_bytes else _read_capped(path)  # type: ignore[arg-type]
        if not data:
            raise CsvError("statement is empty", user_message="The statement file is empty.")
        return _Statement(parse_export(data, filename=name), None, name)
    data = bytes(source) if is_bytes else _read_capped(path)  # type: ignore[arg-type]
    stored = store_upload(ctx.csv_dir, data, name, protect=_newest_imported(files, ctx.account_id))
    return _Statement(stored.export, stored.path, stored.original_name)


def _check_currency(
    ctx: UserContext, export: ParsedExport, mirror: MirrorStore, client: YnabClient
) -> str:
    """Refuse a statement whose currency is not the budget's (F14); returns the currency."""
    if not export.rows:
        detail = f" ({len(export.skipped)} row(s) skipped)" if export.skipped else ""
        raise CsvError(
            "statement has no importable rows",
            user_message="The statement contains no transactions of your Revolut current "
            f"account to import{detail}.",
        )
    if export.currency is None:
        raise CsvError(
            "statement has no currency",
            user_message="The statement does not say which currency it is in, so it cannot "
            "be checked against your YNAB budget. Export it again from the Revolut app.",
        )
    budget_currency = _common.budget_currency(ctx.budget_id, mirror, client)
    if not budget_currency:
        raise YnabHelperError(
            f"currency of budget {ctx.budget_id} unknown",
            user_message="YNAB did not report your budget's currency, so the statement "
            "cannot be checked against it. Nothing was imported.",
        )
    if export.currency.upper() != budget_currency.upper():
        raise CsvError(
            f"statement currency {export.currency} != budget currency {budget_currency}",
            user_message=f"This statement is in {export.currency}, but your YNAB budget uses "
            f"{budget_currency}. Nothing was imported — send the statement of your "
            f"{budget_currency} Revolut account.",
        )
    return export.currency


def _simulated_entries(
    conn: sqlite3.Connection, ledger: LedgerStore, account_id: str, view: MirrorView
) -> list[LedgerEntry]:
    """The ledger a real import would plan with (unassigned rows claimed, refreshed from the
    mirror), computed inside a transaction that is always rolled back (dry run, F74)."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        if ledger.list_unassigned():
            ledger.claim_unassigned(account_id)
        ledger.refresh_from_mirror(account_id, view)
        return ledger.list_for_account(account_id)
    finally:
        if conn.in_transaction:
            conn.execute("ROLLBACK")


def _import_csv_unlocked(
    ctx: UserContext,
    source: bytes | bytearray | memoryview | str | os.PathLike[str],
    *,
    original_name: str | None,
    source_kind: str,
    dry_run: bool,
    skip_pending: bool,
    client: YnabClient | None,
    on_progress: ProgressCallback | None,
) -> ImportResult:
    with closing(_common.open_db(ctx, assign_legacy=not dry_run)) as conn:
        files = CsvFileStore(conn)
        statement = _load_statement(ctx, source, original_name, files, dry_run=dry_run)
        export = statement.export
        file_id: int | None = None
        if not dry_run and statement.stored_path is not None:
            file_id = files.add(
                stored_path=str(statement.stored_path),
                sha256=export.sha256,
                source=source_kind,
                account_id=ctx.account_id,
                original_name=statement.display_name,
                currency=export.currency,
                row_count=len(export.rows),
                window=export.window,
            )
        try:
            result = _plan_and_apply(
                ctx,
                conn,
                export,
                client=client or _common.client_for(ctx),
                dry_run=dry_run,
                skip_pending=skip_pending,
                on_progress=on_progress,
            )
        except Exception as exc:
            if file_id is not None:
                _record_failure(files, file_id, exc)
            raise
        finally:
            if not dry_run:
                # The ledger (and YNAB) changed: a duplicate list scanned before is stale.
                _dedupe.DEDUPE_SCANS.discard(ctx.key)
        if file_id is not None:
            files.set_status(
                file_id, "imported" if result.ok else "partial", result_json=result.to_dict()
            )
        log.info(
            "import for %s (%s%s): ok=%s created=%d updated=%d removed=%d failures=%d",
            ctx.key,
            source_kind,
            ", dry run" if dry_run else "",
            result.ok,
            len(result.created),
            len(result.updated),
            len(result.removed),
            len(result.failures),
        )
        return result


def _record_failure(files: CsvFileStore, file_id: int, exc: BaseException) -> None:
    message = getattr(exc, "user_message", None) or type(exc).__name__
    try:
        files.set_status(
            file_id, "failed", result_json={"error": str(message), "error_type": type(exc).__name__}
        )
    except Exception:  # never mask the original error
        log.exception("could not record the failed import of csv_files row %d", file_id)


def _plan_and_apply(
    ctx: UserContext,
    conn: sqlite3.Connection,
    export: ParsedExport,
    *,
    client: YnabClient,
    dry_run: bool,
    skip_pending: bool,
    on_progress: ProgressCallback | None,
) -> ImportResult:
    mirror = MirrorStore(conn)
    ledger = LedgerStore(conn)
    _check_currency(ctx, export, mirror, client)
    # The mirror is a cache: synced in a dry run too. The ledger is refreshed below, after
    # the unassigned legacy rows were claimed, so they are refreshed as well.
    synced = sync_and_refresh(client, conn, ctx.budget_id, None, force=True)
    if synced.rate_limited:
        # Planning from a stale mirror could undo the user's edits in YNAB (F01).
        raise _common.rate_limited_error(synced)
    _common.check_account(ctx, mirror)
    view = mirror.view_for_account(ctx.budget_id, ctx.account_id)
    if dry_run:
        entries = _simulated_entries(conn, ledger, ctx.account_id, view)
    else:
        if ledger.list_unassigned():
            ledger.claim_unassigned(ctx.account_id)
        ledger.refresh_from_mirror(ctx.account_id, view)
        entries = ledger.list_for_account(ctx.account_id)
    opts = PlanOptions(
        account_id=ctx.account_id,
        approved=ctx.auto_approve,
        skip_pending=skip_pending,
        allow_sweep=compute_allow_sweep(export, entries, account_id=ctx.account_id),
    )
    plan = plan_import(export, entries, view, opts)
    return apply_plan(
        plan,
        client,
        conn,
        budget_id=ctx.budget_id,
        account_id=ctx.account_id,
        export=export,
        dry_run=dry_run,
        opts=opts,
        on_progress=on_progress,
    )
