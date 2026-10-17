"""Helpers shared by the service operations (internal to :mod:`ynab_helper.services`).

The pluggable hooks — :data:`ynab_helper.services.client_factory`,
:data:`~ynab_helper.services.token_client_factory`,
:data:`~ynab_helper.services.ibkr_client_factory` and ``services._open_db`` — live on the
package and are looked up there **at call time** (:func:`open_db`, :func:`client_for`, …),
so replacing them on the package (tests, embedders) affects every operation.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

# The package is still initialising when this module is imported; its attributes are only
# read inside the functions below.
from ynab_helper import services as _package
from ynab_helper.balance.ibkr import IbkrClient
from ynab_helper.errors import ConfigError, YnabRateLimited
from ynab_helper.services.context import UserContext, ValuationTarget, setup_hint
from ynab_helper.storage.mirror import MirrorStore
from ynab_helper.sync.ynab_sync import (
    SyncResult,
    mark_mirror_stale,
    note_rate_limited,
    sync_and_refresh,
)
from ynab_helper.ynab.client import YnabClient

log = logging.getLogger(__name__)


# ── hooks (late-bound on the package) ────────────────────────────────────────


def open_db(ctx: UserContext, *, assign_legacy: bool = True) -> sqlite3.Connection:
    """The user's migrated transactions DB, through ``services._open_db``."""
    return _package._open_db(ctx, assign_legacy=assign_legacy)


def client_for(ctx: UserContext | ValuationTarget) -> YnabClient:
    """The YNAB client for ``ctx``, through :func:`ynab_helper.services.make_client`."""
    return _package.make_client(ctx)


def token_client(token: str) -> YnabClient:
    """A YNAB client for a bare token, through ``services.token_client_factory``."""
    return _package.token_client_factory(token)


def ibkr_client(base_url: str) -> IbkrClient:
    """The IBKR gateway client, through ``services.ibkr_client_factory``."""
    return _package.ibkr_client_factory(base_url)


# ── mirror state ─────────────────────────────────────────────────────────────


def rate_limited_error(result: SyncResult) -> YnabRateLimited:
    return YnabRateLimited("YNAB sync was rate limited", retry_after=result.retry_after)


def synced_mirror(ctx: UserContext, conn: sqlite3.Connection, client: YnabClient) -> MirrorStore:
    """Forced sync (accounts, transactions, the account's ledger refresh) before an operation
    that plans YNAB writes or shows numbers to confirm; raises if YNAB rate limited it."""
    synced = sync_and_refresh(client, conn, ctx.budget_id, ctx.account_id, force=True)
    if synced.rate_limited:
        raise rate_limited_error(synced)
    return MirrorStore(conn)


def budget_currency(budget_id: str, mirror: MirrorStore, client: YnabClient) -> str | None:
    """The budget currency from the mirror, else fetched from YNAB (one GET) and cached."""
    currency = mirror.get_budget_currency(budget_id)
    if currency is None:
        currency = client.get_budget_currency(budget_id)
        if currency:
            mirror.set_budget_currency(budget_id, currency)
    return currency or None


def mark_stale(conn: sqlite3.Connection, budget_id: str) -> None:
    """After a YNAB write: the next non-forced sync must run. Best effort — never raises."""
    try:
        mark_mirror_stale(MirrorStore(conn), budget_id)
    except Exception:
        log.warning("could not mark the mirror of %s stale", budget_id, exc_info=True)


def note_backoff(conn: sqlite3.Connection, budget_id: str, retry_after: float | None) -> None:
    """After a 429 on a write: start the sync backoff (F17). Best effort — never raises."""
    try:
        note_rate_limited(MirrorStore(conn), budget_id, retry_after)
    except Exception:
        log.warning("could not record the rate limit of %s", budget_id, exc_info=True)


@contextmanager
def rate_limit_remembered(conn: sqlite3.Connection, budget_id: str) -> Iterator[None]:
    """Re-raise a :class:`YnabRateLimited` after starting the sync backoff (F17)."""
    try:
        yield
    except YnabRateLimited as exc:
        note_backoff(conn, budget_id, exc.retry_after)
        raise


def check_account(
    ctx: UserContext, mirror: MirrorStore, *, nothing: str = "Nothing was imported."
) -> None:
    """Refuse to write into an account YNAB no longer offers (after a fresh sync).

    A deleted, closed or unknown account (or one of another budget) would make YNAB reject
    every row — or put money into an account the user closed — so nothing is sent and the
    user is told to fix the setup. Every account of the budget carries its ``budget_id``
    after the forced sync (the first v2 sync of a budget lists all accounts), so a v1
    ``accounts`` row without one is an account this budget does not have. ``nothing`` says
    what did not happen ("Nothing was imported." for an import).
    """
    account = mirror.account(ctx.account_id)
    if account is None or account.budget_id != ctx.budget_id:
        problem = "is not in your YNAB budget"
    elif account.deleted:
        problem = "was deleted in YNAB"
    elif account.closed:
        problem = "is closed in YNAB"
    else:
        return
    name = f" '{account.name}'" if account is not None and account.name else ""
    raise ConfigError(
        f"account {ctx.account_id} {problem}",
        user_message=f"Your Revolut account{name} {problem}. {nothing} "
        + setup_hint(ctx.telegram_id, " to choose the account to import into"),
    )
