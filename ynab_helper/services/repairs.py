"""Repairs on the user's YNAB account: duplicate cleanup and stale ``(pending)`` memos
(docs/DESIGN.md §6.7)."""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import closing
from datetime import datetime
from typing import Final

from ynab_helper.errors import YnabError
from ynab_helper.locks import LOCKS
from ynab_helper.maintenance import cleanup as _cleanup
from ynab_helper.maintenance import dedupe as _dedupe
from ynab_helper.maintenance.cleanup import CleanupResult
from ynab_helper.maintenance.dedupe import DedupeDeleteResult, DedupeScan, DedupeScans
from ynab_helper.services import _common
from ynab_helper.services.context import UserContext
from ynab_helper.ynab.client import YnabClient

__all__ = ["cleanup_pending", "dedupe_delete", "dedupe_scan"]

OP_DEDUPE: Final = "dedupe"
OP_CLEANUP: Final = "cleanup"


# ── dedupe / cleanup (§6.7) ──────────────────────────────────────────────────


def dedupe_scan(
    ctx: UserContext,
    *,
    client: YnabClient | None = None,
    now: datetime | None = None,
    registry: DedupeScans | None = None,
) -> DedupeScan:
    """List importer-created transactions of the account that no statement row accounts for.

    Under the user's ``dedupe`` lock, after a forced mirror sync. The candidates are kept
    server-side as the user's current scan (``scan.scan_id``, 10-minute TTL; ``registry``
    replaces the process-wide one in tests). Nothing is written to YNAB.

    Raises:
        Busy; YnabRateLimited; StatementUnavailable (no successfully imported statement);
        YnabError.
    """
    with LOCKS.operation(ctx.key, OP_DEDUPE), closing(_common.open_db(ctx)) as conn:
        _common.synced_mirror(ctx, conn, client or _common.client_for(ctx))
        return _dedupe.dedupe_scan(
            conn,
            budget_id=ctx.budget_id,
            account_id=ctx.account_id,
            user_key=ctx.key,
            now=now,
            registry=registry,
        )


def dedupe_delete(
    ctx: UserContext,
    scan_id: str,
    ids: Sequence[str],
    *,
    client: YnabClient | None = None,
    registry: DedupeScans | None = None,
) -> DedupeDeleteResult:
    """Delete the confirmed selection ``ids`` (at most 200) of the user's scan ``scan_id``.

    Under the user's ``dedupe`` lock. The scan is checked first (an expired or replaced
    scan costs no YNAB request), then the mirror is synced (forced) so every id is
    re-checked against YNAB's current state before its DELETE. Per-id failures are in the
    result; a 429 stops the run (``result.rate_limited``) and starts the sync backoff.

    Raises:
        Busy; StaleStateError (scan unknown, replaced, expired, of another account, or ids
        outside it); DedupeSelectionError (empty, malformed or too many ids);
        YnabRateLimited (the sync); YnabAuthError (deletions so far stay recorded).
    """
    scans = registry if registry is not None else _dedupe.DEDUPE_SCANS
    with LOCKS.operation(ctx.key, OP_DEDUPE):
        scans.lookup(ctx.key, scan_id)
        with closing(_common.open_db(ctx)) as conn:
            ynab = client or _common.client_for(ctx)
            _common.synced_mirror(ctx, conn, ynab)
            try:
                result = _dedupe.dedupe_delete(
                    conn,
                    ynab,
                    budget_id=ctx.budget_id,
                    account_id=ctx.account_id,
                    user_key=ctx.key,
                    scan_id=scan_id,
                    ids=ids,
                    registry=scans,
                )
            except YnabError:
                _common.mark_stale(conn, ctx.budget_id)
                raise
            if result.deleted or result.failures:
                _common.mark_stale(conn, ctx.budget_id)
            if result.rate_limited:
                _common.note_backoff(conn, ctx.budget_id, None)
            return result


def cleanup_pending(
    ctx: UserContext, *, dry_run: bool = False, client: YnabClient | None = None
) -> CleanupResult:
    """Strip stale ``(pending)`` memo markers from the account's cleared transactions.

    Under the user's ``cleanup`` lock, after a forced mirror sync. Only ``memo`` is ever
    sent, and only for cleared/reconciled transactions (F01, C02). A dry run lists the
    candidates and writes nothing to YNAB (and, on a first v2 open of a legacy DB, does not
    assign the legacy rows: F74). A 429 stops the run and starts the sync backoff.

    Raises:
        Busy; YnabRateLimited (the sync); YnabAuthError; YnabError.
    """
    with (
        LOCKS.operation(ctx.key, OP_CLEANUP),
        closing(_common.open_db(ctx, assign_legacy=not dry_run)) as conn,
    ):
        ynab = client or _common.client_for(ctx)
        _common.synced_mirror(ctx, conn, ynab)
        try:
            result = _cleanup.cleanup_pending_memos(
                conn, ynab, budget_id=ctx.budget_id, account_id=ctx.account_id, dry_run=dry_run
            )
        except YnabError:
            if not dry_run:
                _common.mark_stale(conn, ctx.budget_id)
            raise
        if result.updated or result.failures:
            _common.mark_stale(conn, ctx.budget_id)
        if result.rate_limited:
            _common.note_backoff(conn, ctx.budget_id, None)
        return result
