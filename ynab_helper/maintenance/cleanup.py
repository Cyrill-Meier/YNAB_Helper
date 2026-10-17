"""Strip stale ``(pending)`` markers from cleared transactions (DESIGN §6.7; F01, C02).

The importer writes the memo marker ``(pending)`` for rows Revolut still lists as pending
and removes it when the row completes. After a lost local DB (v1) or a manual clear in
YNAB, markers can survive on transactions that are long cleared. This repair:

* looks only at mirror transactions of the account whose ``cleared`` is ``cleared`` or
  ``reconciled`` and whose memo contains the marker — **uncleared rows are never
  touched**, since they may well still be pending (C02);
* sends ``{id, memo}`` only, where the memo is YNAB's current memo with just the marker
  token removed (:func:`~ynab_helper.revolut.parser.strip_pending_marker`) — ``cleared``
  is never sent, so nothing is ever un-reconciled (F01);
* uses the bulk PATCH endpoint (chunks of 50), falling back to per-transaction PUT for a
  chunk YNAB rejects, so one bad row cannot block the others;
* collects per-row failures, stops at the first 429 (remaining rows are ``not_applied``)
  and re-raises an authentication error.

The caller syncs the mirror first; nothing local is written (the next sync picks up the
new memos).
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import asdict, dataclass, field
from typing import Any

from ynab_helper.errors import (
    YnabAuthError,
    YnabError,
    YnabNotFound,
    YnabRateLimited,
    YnabValidationError,
)
from ynab_helper.money import format_money
from ynab_helper.revolut.parser import has_pending_marker, strip_pending_marker
from ynab_helper.storage.mirror import CLEARED_LIKE, MirrorStore
from ynab_helper.ynab.client import UPDATE_CHUNK_SIZE, YnabClient

__all__ = ["CleanupFailure", "CleanupResult", "MemoFix", "cleanup_pending_memos"]

log = logging.getLogger(__name__)
audit = logging.getLogger("ynab_helper.audit")


@dataclass(frozen=True, slots=True)
class MemoFix:
    """One transaction whose memo loses its ``(pending)`` marker."""

    tx_id: str
    date: str
    amount: int
    payee_name: str | None
    cleared: str
    old_memo: str
    new_memo: str | None  # None = memo cleared (it held only the marker)


@dataclass(frozen=True, slots=True)
class CleanupFailure:
    fix: MemoFix
    error: str  # user-safe message


@dataclass(slots=True)
class CleanupResult:
    """Outcome of :func:`cleanup_pending_memos`.

    ``candidates`` are the transactions that need the fix; ``updated`` the ones YNAB
    confirmed (always empty in a dry run). ``skipped_uncleared`` counts marked transactions
    left alone because they are uncleared.
    """

    dry_run: bool
    account_id: str
    candidates: list[MemoFix] = field(default_factory=list)
    updated: list[MemoFix] = field(default_factory=list)
    failures: list[CleanupFailure] = field(default_factory=list)
    skipped_uncleared: int = 0
    not_applied: int = 0
    rate_limited: bool = False

    @property
    def ok(self) -> bool:
        """No failures and not cut short by the rate limit."""
        return not self.failures and not self.rate_limited and not self.not_applied

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["ok"] = self.ok
        return data


def _new_memo(memo: str) -> str | None:
    stripped = strip_pending_marker(memo)
    return stripped or None


def _candidates(mirror: MirrorStore, budget_id: str, account_id: str) -> tuple[list[MemoFix], int]:
    fixes: list[MemoFix] = []
    uncleared = 0
    for tx in mirror.transactions_for_account(budget_id, account_id):
        if tx.deleted or not has_pending_marker(tx.memo):
            continue
        assert tx.memo is not None
        if tx.cleared not in CLEARED_LIKE:
            uncleared += 1
            continue
        new_memo = _new_memo(tx.memo)
        if new_memo == tx.memo:
            continue
        fixes.append(
            MemoFix(
                tx_id=tx.id,
                date=tx.date,
                amount=tx.amount,
                payee_name=tx.payee_name,
                cleared=tx.cleared,
                old_memo=tx.memo,
                new_memo=new_memo,
            )
        )
    return fixes, uncleared


def cleanup_pending_memos(
    conn: sqlite3.Connection,
    client: YnabClient,
    *,
    budget_id: str,
    account_id: str,
    dry_run: bool = False,
) -> CleanupResult:
    """Remove the ``(pending)`` marker from cleared/reconciled transactions of the account.

    ``conn``'s mirror must be fresh. Only ``memo`` is written, never ``cleared``.

    Raises:
        YnabAuthError: the token was rejected (nothing further is attempted).
    """
    fixes, uncleared = _candidates(MirrorStore(conn), budget_id, account_id)
    result = CleanupResult(
        dry_run=dry_run, account_id=account_id, candidates=fixes, skipped_uncleared=uncleared
    )
    log.info(
        "cleanup %s: %d marker(s) on cleared transactions, %d uncleared left alone%s",
        account_id,
        len(fixes),
        uncleared,
        " (dry run)" if dry_run else "",
    )
    if dry_run or not fixes:
        return result

    chunks = [fixes[i : i + UPDATE_CHUNK_SIZE] for i in range(0, len(fixes), UPDATE_CHUNK_SIZE)]
    for index, chunk in enumerate(chunks):
        if not _apply_chunk(client, budget_id, chunk, result):
            result.rate_limited = True
            result.not_applied += sum(len(c) for c in chunks[index + 1 :])
            break
    return result


def _payload(fix: MemoFix) -> dict[str, Any]:
    return {"id": fix.tx_id, "memo": fix.new_memo}


def _confirmed(fix: MemoFix, result: CleanupResult) -> None:
    result.updated.append(fix)
    audit.info(
        "updated memo (removed pending marker) tx=%s date=%s amount=%s payee=%r",
        fix.tx_id,
        fix.date,
        format_money(fix.amount),
        fix.payee_name,
    )


def _apply_chunk(
    client: YnabClient, budget_id: str, chunk: list[MemoFix], result: CleanupResult
) -> bool:
    """PATCH one chunk; returns False if the rate limit stopped the run."""
    try:
        returned = client.update_transactions(budget_id, [_payload(f) for f in chunk])
    except YnabRateLimited:
        result.not_applied += len(chunk)
        return False
    except YnabAuthError:
        raise
    except (YnabValidationError, YnabNotFound) as exc:
        log.warning("cleanup: bulk PATCH rejected (%s); retrying one by one", exc)
        return _apply_one_by_one(client, budget_id, chunk, result)
    except YnabError as exc:
        for fix in chunk:
            result.failures.append(CleanupFailure(fix, exc.user_message))
        return True
    confirmed = {tx.id for tx in returned}
    for fix in chunk:
        if fix.tx_id in confirmed:
            _confirmed(fix, result)
        else:
            result.failures.append(CleanupFailure(fix, "YNAB did not confirm the update."))
    return True


def _apply_one_by_one(
    client: YnabClient, budget_id: str, chunk: list[MemoFix], result: CleanupResult
) -> bool:
    for position, fix in enumerate(chunk):
        try:
            client.update_transaction(budget_id, fix.tx_id, {"memo": fix.new_memo})
        except YnabRateLimited:
            result.not_applied += len(chunk) - position
            return False
        except YnabAuthError:
            raise
        except YnabNotFound:
            result.failures.append(CleanupFailure(fix, "The transaction no longer exists in YNAB."))
        except YnabError as exc:
            result.failures.append(CleanupFailure(fix, exc.user_message))
        else:
            _confirmed(fix, result)
    return True
