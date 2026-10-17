"""Balance operations: reconcile with a statement, crypto and IBKR valuation syncs
(docs/DESIGN.md §6.6)."""

from __future__ import annotations

import logging
import os
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import date, datetime
from typing import Final

from ynab_helper.balance import brokerage as _brokerage
from ynab_helper.balance import reconcile as _reconcile
from ynab_helper.balance.brokerage import BrokerageSyncResult
from ynab_helper.balance.crypto import btc as _btc
from ynab_helper.balance.crypto import eth as _eth
from ynab_helper.balance.crypto import sync as _crypto
from ynab_helper.balance.crypto.sync import CryptoSyncResult, PriceFetcher
from ynab_helper.balance.ibkr import DEFAULT_IBKR_BASE_URL, IbkrClient
from ynab_helper.balance.reconcile import ReconcilePreview, ReconcileResult, ReconcileTokens
from ynab_helper.errors import YnabError, YnabHelperError, YnabRateLimited
from ynab_helper.locks import LOCKS
from ynab_helper.services import _common
from ynab_helper.services.context import UserContext, ValuationTarget
from ynab_helper.storage.db import open_tx_db
from ynab_helper.storage.mirror import MirrorStore
from ynab_helper.ynab.client import YnabClient

__all__ = ["brokerage_sync", "crypto_sync", "reconcile_apply", "reconcile_preview"]

log = logging.getLogger(__name__)

OP_RECONCILE: Final = "reconcile"
OP_CRYPTO: Final = "crypto"
OP_BROKERAGE: Final = "brokerage"


# ── reconcile (§6.6, two steps) ──────────────────────────────────────────────


def reconcile_preview(
    ctx: UserContext,
    *,
    client: YnabClient | None = None,
    now: datetime | date | None = None,
    tokens: ReconcileTokens | None = None,
) -> ReconcilePreview:
    """Compare YNAB's cleared balance with the newest imported statement (step 1 of 2).

    Under the user's ``reconcile`` lock: forced mirror sync, account check, then
    :func:`ynab_helper.balance.reconcile.reconcile_preview` with the budget currency from
    the mirror (one more GET cross-checks the mirror's cleared balance). Nothing is written
    to YNAB. ``preview.token`` (None when already in sync) confirms exactly these numbers
    for :func:`reconcile_apply` within ``preview.token_ttl_seconds``. ``now`` (local) only
    drives the statement-age warning; ``tokens`` replaces the process-wide registry (tests).

    Raises:
        Busy; YnabRateLimited: the sync was rate limited (nothing computed).
        ConfigError: the account was deleted or closed in YNAB, or is in another budget.
        StatementUnavailable / ReconcileUnavailable: reconciling is not possible; the
            ``user_message`` says why (no imported statement, wrong currency, a statement
            older than what YNAB already reflects, …).
        StaleStateError: YNAB changed while the preview was computed.
        YnabError: any other YNAB failure.
    """
    with LOCKS.operation(ctx.key, OP_RECONCILE), closing(_common.open_db(ctx)) as conn:
        ynab = client or _common.client_for(ctx)
        mirror = _common.synced_mirror(ctx, conn, ynab)
        _common.check_account(ctx, mirror, nothing="Nothing can be reconciled.")
        return _reconcile.reconcile_preview(
            conn,
            ynab,
            budget_id=ctx.budget_id,
            account_id=ctx.account_id,
            budget_currency=_common.budget_currency(ctx.budget_id, mirror, ynab),
            now=now,
            tokens=tokens,
        )


def reconcile_apply(
    ctx: UserContext,
    token: str,
    *,
    client: YnabClient | None = None,
    now: datetime | date | None = None,
    tokens: ReconcileTokens | None = None,
) -> ReconcileResult:
    """Post the reconciliation adjustment confirmed by ``token`` (step 2 of 2).

    Under the user's ``reconcile`` lock: forced mirror sync and account check, then
    :func:`ynab_helper.balance.reconcile.reconcile_apply`, which consumes the token,
    recomputes the preview and refuses unless statement, balances and difference are
    exactly what the user confirmed. At most one ``CSV Reconciliation`` transaction is
    posted. A rate-limited sync raises before the token is used, so the user can retry.

    Raises:
        Busy; YnabRateLimited (the sync or the POST; the backoff is remembered);
        ConfigError; StaleStateError (unknown, used or expired token, or anything changed);
        StatementUnavailable / ReconcileUnavailable; YnabError (the POST failed).
    """
    with LOCKS.operation(ctx.key, OP_RECONCILE), closing(_common.open_db(ctx)) as conn:
        ynab = client or _common.client_for(ctx)
        mirror = _common.synced_mirror(ctx, conn, ynab)
        _common.check_account(ctx, mirror, nothing="Nothing was posted.")
        currency = _common.budget_currency(ctx.budget_id, mirror, ynab)
        try:
            with _common.rate_limit_remembered(conn, ctx.budget_id):
                return _reconcile.reconcile_apply(
                    conn,
                    ynab,
                    budget_id=ctx.budget_id,
                    account_id=ctx.account_id,
                    token=token,
                    budget_currency=currency,
                    now=now,
                    tokens=tokens,
                )
        finally:
            # Whatever happened, the POST may have reached YNAB: let the next status sync run.
            _common.mark_stale(conn, ctx.budget_id)


# ── valuation syncs: crypto, IBKR (§6.6) ─────────────────────────────────────


@contextmanager
def _valuation_db(target: ValuationTarget) -> Iterator[sqlite3.Connection | None]:
    """The target's transactions DB (migrated; legacy rows are never assigned here, the
    next import claims them), or None for a target without one."""
    if target.db_path is None:
        yield None
        return
    target.db_path.parent.mkdir(parents=True, exist_ok=True)
    backups = target.backups
    assert backups is not None
    with closing(open_tx_db(target.db_path, default_account_id="", backup_dir=backups)) as conn:
        yield conn


def _valuation_currency(target: ValuationTarget, client: YnabClient) -> str:
    """The budget currency (mirror cache, else YNAB); refuses to value an unknown budget."""
    with _valuation_db(target) as conn:
        if conn is not None:
            currency = _common.budget_currency(target.budget_id, MirrorStore(conn), client)
        else:
            currency = client.get_budget_currency(target.budget_id)
    if not currency:
        raise YnabHelperError(
            f"currency of budget {target.budget_id} unknown",
            user_message="YNAB did not report your budget's currency, so the valuation "
            "cannot be checked against it. Nothing was posted.",
        )
    return currency


def _after_valuation(
    target: ValuationTarget, *, rate_limited: bool = False, retry_after: float | None = None
) -> None:
    """Mark the mirror stale after a (possible) write; start the backoff after a 429."""
    if target.db_path is None:
        return
    try:
        with _valuation_db(target) as conn:
            assert conn is not None
            _common.mark_stale(conn, target.budget_id)
            if rate_limited:
                _common.note_backoff(conn, target.budget_id, retry_after)
    except Exception:
        log.warning("could not update the mirror state of %s", target.key, exc_info=True)


def crypto_sync(
    target: UserContext | ValuationTarget,
    *,
    btc_xpub: str | None = None,
    eth_address: str | None = None,
    dry_run: bool = False,
    allow_zero: bool = False,
    ankr_api_key: str | None = None,
    client: YnabClient | None = None,
    today: date | datetime | None = None,
    blockstream: _btc.BlockstreamClient | None = None,
    eth_rpc: _eth.EthRpc | None = None,
    price_fetcher: PriceFetcher | None = None,
) -> CryptoSyncResult:
    """Value the crypto wallets and post the difference to the crypto tracking account.

    ``target`` is a bot/web :class:`UserContext` (account and wallets from the user's
    settings: :meth:`UserContext.crypto_target`, ``btc_xpub``, ``eth_address``) or, for
    the CLI, a :class:`ValuationTarget` with the wallets passed explicitly. Explicit
    ``btc_xpub``/``eth_address`` always win. ``ankr_api_key`` defaults (None) to
    ``ANKR_API_KEY`` from the process environment; ``""`` means no Ankr endpoint.
    ``blockstream``, ``eth_rpc`` and ``price_fetcher`` are injectable for tests.

    Runs under the user's ``crypto`` lock. The budget currency comes from the mirror cache
    (or YNAB) and must be CHF; the user's DB is not kept open during the wallet scan. A dry
    run reads wallets, prices and YNAB but writes nothing.

    Raises:
        Busy; ConfigError (no tracking account, no wallet, an invalid key/address, a
        non-CHF budget); ExternalServiceError (chain or price lookup failed, a held asset
        has no price); AdjustmentRefused (the wallet scan found nothing while YNAB holds a
        value, unless ``allow_zero``); YnabRateLimited (backoff remembered); YnabError.
    """
    if isinstance(target, UserContext):
        valuation = target.crypto_target()
        btc_xpub = btc_xpub if btc_xpub is not None else target.btc_xpub
        eth_address = eth_address if eth_address is not None else target.eth_address
    else:
        valuation = target
    if ankr_api_key is None:  # not given (the bot): the process environment's key
        ankr_api_key = os.environ.get("ANKR_API_KEY", "")
    ankr_api_key = ankr_api_key.strip() or None
    ynab = client or _common.client_for(target)
    with LOCKS.operation(valuation.key, OP_CRYPTO):
        currency = _valuation_currency(valuation, ynab)
        try:
            result = _crypto.crypto_sync(
                ynab,
                valuation.budget_id,
                valuation.account_id,
                btc_xpub=btc_xpub,
                eth_address=eth_address,
                ankr_api_key=ankr_api_key,
                dry_run=dry_run,
                allow_zero=allow_zero,
                blockstream=blockstream,
                eth_rpc=eth_rpc,
                price_fetcher=price_fetcher,
                today=today,
                budget_currency=currency,
            )
        except YnabRateLimited as exc:
            _after_valuation(valuation, rate_limited=True, retry_after=exc.retry_after)
            raise
        except YnabError:
            if not dry_run:
                _after_valuation(valuation)
            raise
        if result.adjustment.posted:
            _after_valuation(valuation)
        return result


def brokerage_sync(
    target: ValuationTarget,
    *,
    ibkr: IbkrClient | None = None,
    ibkr_base_url: str = DEFAULT_IBKR_BASE_URL,
    ibkr_account_id: str | None = None,
    dry_run: bool = False,
    client: YnabClient | None = None,
    today: date | datetime | None = None,
) -> BrokerageSyncResult:
    """Post the difference between the IBKR net asset value and the tracking account.

    ``ibkr`` defaults to :data:`ibkr_client_factory` for ``ibkr_base_url`` (TLS verified
    except for a gateway on this machine, F80); ``ibkr_account_id`` defaults to the
    gateway's first account. Runs under the ``brokerage`` lock of ``target.key``; the
    budget currency must be CHF. A NAV of zero or less is never posted.

    Raises:
        Busy; ConfigError (bad gateway URL, non-CHF budget); ExternalServiceError (the
        gateway failed); AdjustmentRefused (NAV ≤ 0); YnabRateLimited; YnabError.
    """
    gateway = ibkr if ibkr is not None else _common.ibkr_client(ibkr_base_url)
    ynab = client or _common.client_for(target)
    with LOCKS.operation(target.key, OP_BROKERAGE):
        currency = _valuation_currency(target, ynab)
        try:
            result = _brokerage.brokerage_sync(
                ynab,
                target.budget_id,
                target.account_id,
                ibkr=gateway,
                ibkr_account_id=ibkr_account_id or None,
                dry_run=dry_run,
                today=today,
                budget_currency=currency,
            )
        except YnabRateLimited as exc:
            _after_valuation(target, rate_limited=True, retry_after=exc.retry_after)
            raise
        except YnabError:
            if not dry_run:
                _after_valuation(target)
            raise
        if result.adjustment.posted:
            _after_valuation(target)
        return result
