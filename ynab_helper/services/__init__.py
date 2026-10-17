"""Per-user operations shared by the CLI, the Telegram bot and the web UI (docs/DESIGN.md §7).

Every operation takes a :class:`UserContext` (a :class:`ValuationTarget` for the crypto and
IBKR syncs), opens the user's transactions DB for the duration of the call
(``with closing(...)``) and returns a structured result — nothing is printed, so concurrent
bot and web threads can never mix up each other's output (F15).

=====================  ===================================================================
Operation              Module
=====================  ===================================================================
``import_csv``         :mod:`.importing` — statement import (§6, §7)
``sync``               :mod:`.status` — mirror sync; ``account_status`` — /status numbers
``reconcile_preview``  :mod:`.balances` — two-step reconcile; ``crypto_sync`` and
``reconcile_apply``    ``brokerage_sync`` — valuation adjustments (§6.6)
``dedupe_scan``        :mod:`.repairs` — duplicate cleanup and ``cleanup_pending`` (§6.7)
``dedupe_delete``
``list_budgets``       :mod:`.admin` — CLI listings and ``database_stats``
=====================  ===================================================================

**Locking.** Every operation runs under the per-user lock
(:data:`ynab_helper.locks.LOCKS`, key :attr:`UserContext.key`); a second operation for the
same user is refused with :class:`~ynab_helper.errors.Busy` (F16). That includes the
read-only previews (:func:`reconcile_preview`, :func:`dedupe_scan`): they sync the mirror
first (a local write) and must never show — or issue a confirmation token for — the numbers
of a half-applied import. Only :func:`account_status` falls back to the cached mirror when
the lock is taken, and the listings/statistics touch no per-user state. Locks are not
re-entrant, so composed operations call the ``_..._unlocked`` helpers under one outer lock.

**Fresh state.** Operations that plan YNAB writes (import, reconcile, dedupe, cleanup) run
a *forced* mirror sync first and refuse to continue when it was rate limited
(:class:`~ynab_helper.errors.YnabRateLimited`): planning from a stale mirror could undo the
user's edits in YNAB (F01). They then refuse, with
:class:`~ynab_helper.errors.ConfigError` and before any write, an account YNAB no longer
offers (deleted, closed, or not in the budget). After a write the mirror is marked stale
(the next non-forced sync runs) and a 429 starts the sync backoff (F17).

**Legacy rows.** The first v2 open of a v1 database migrates it (with a backup) and, if
that open is allowed to, assigns the v1 ledger rows that have no account to
``ctx.account_id``. Dry runs, previews, scans, syncs, status and the valuation syncs never
assign them: they stay unclaimed (``''``). In practice the rows are claimed by the first
real import (which claims unclaimed rows on every run) or by a non-dry memo cleanup that
happens to be the first open; :func:`reconcile_apply` and :func:`dedupe_delete` allow the
assignment too, but always follow the preview or scan that already migrated the database
(F74).

**Hooks.** YNAB clients come from :func:`make_client`, which delegates to the module
attributes :data:`client_factory` (per :class:`UserContext`) and :data:`token_client_factory`
(per token; the default ``client_factory`` uses it too); :data:`ibkr_client_factory` builds
the IBKR gateway client and ``_open_db`` opens the user's DB. The operations look all of
them up on this package at call time, so tests replace them here
(``monkeypatch.setattr(services, "client_factory", …)``).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable

from ynab_helper.balance.brokerage import BrokerageSyncResult
from ynab_helper.balance.crypto.sync import CryptoSyncResult
from ynab_helper.balance.ibkr import IbkrClient
from ynab_helper.balance.reconcile import ReconcilePreview, ReconcileResult
from ynab_helper.importer.models import ImportResult
from ynab_helper.maintenance.cleanup import CleanupResult
from ynab_helper.maintenance.dedupe import DedupeDeleteResult, DedupeScan
from ynab_helper.services.admin import (
    BudgetSummary,
    BudgetSyncInfo,
    DatabaseStats,
    database_stats,
    list_accounts,
    list_budgets,
)
from ynab_helper.services.balances import (
    brokerage_sync,
    crypto_sync,
    reconcile_apply,
    reconcile_preview,
)
from ynab_helper.services.context import UserContext, ValuationTarget
from ynab_helper.services.importing import SOURCE_KINDS, SourceKind, import_csv
from ynab_helper.services.repairs import cleanup_pending, dedupe_delete, dedupe_scan
from ynab_helper.services.status import StatusInfo, account_status, sync
from ynab_helper.storage.db import open_tx_db
from ynab_helper.sync.ynab_sync import SyncResult
from ynab_helper.ynab.client import YnabClient

__all__ = [
    "SOURCE_KINDS",
    "BrokerageSyncResult",
    "BudgetSummary",
    "BudgetSyncInfo",
    "CleanupResult",
    "CryptoSyncResult",
    "DatabaseStats",
    "DedupeDeleteResult",
    "DedupeScan",
    "ImportResult",
    "ReconcilePreview",
    "ReconcileResult",
    "SourceKind",
    "StatusInfo",
    "SyncResult",
    "UserContext",
    "ValuationTarget",
    "account_status",
    "brokerage_sync",
    "cleanup_pending",
    "client_factory",
    "crypto_sync",
    "database_stats",
    "dedupe_delete",
    "dedupe_scan",
    "ibkr_client_factory",
    "import_csv",
    "list_accounts",
    "list_budgets",
    "make_client",
    "reconcile_apply",
    "reconcile_preview",
    "sync",
    "token_client_factory",
]


# ── hooks (replaceable; the operations look them up here at call time) ─────


def _default_token_client_factory(token: str) -> YnabClient:
    return YnabClient(token)


#: Builds a YNAB client for a bare token (CLI listings, valuation targets); tests replace it.
token_client_factory: Callable[[str], YnabClient] = _default_token_client_factory


def _default_client_factory(ctx: UserContext) -> YnabClient:
    return token_client_factory(ctx.ynab_token)


#: Builds the YNAB client for a context; tests replace it (``services.client_factory = …``).
client_factory: Callable[[UserContext], YnabClient] = _default_client_factory


def make_client(ctx: UserContext | ValuationTarget) -> YnabClient:
    """The YNAB client for ``ctx``: :data:`client_factory` for a :class:`UserContext`,
    :data:`token_client_factory` for a :class:`ValuationTarget` (looked up at call time)."""
    if isinstance(ctx, UserContext):
        return client_factory(ctx)
    return token_client_factory(ctx.ynab_token)


def _default_ibkr_client_factory(base_url: str) -> IbkrClient:
    return IbkrClient(base_url)


#: Builds the IBKR Client Portal Gateway client for a base URL; tests replace it.
ibkr_client_factory: Callable[[str], IbkrClient] = _default_ibkr_client_factory


def _open_db(ctx: UserContext, *, assign_legacy: bool = True) -> sqlite3.Connection:
    """Open (and migrate, with a backup first) the user's transactions DB.

    On the first v2 open of a legacy DB the migration assigns ledger rows without an account
    to ``ctx.account_id``. ``assign_legacy=False`` (read-only operations and dry runs, F74)
    leaves them unclaimed (``''``): which account owns the legacy history is decided by the
    next real import.
    """
    ctx.db_path.parent.mkdir(parents=True, exist_ok=True)
    cache = ctx.csv_dir if ctx.csv_dir is not None and ctx.csv_dir.is_dir() else None
    return open_tx_db(
        ctx.db_path,
        default_account_id=ctx.account_id if assign_legacy else "",
        backup_dir=ctx.backups,
        csv_cache_dir=cache,
    )
