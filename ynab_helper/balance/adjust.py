"""Balance adjustments shared by the reconcile, crypto and IBKR syncs (DESIGN §6.6).

One function, :func:`post_balance_adjustment`, turns "YNAB shows X, it should be Y" into at
most one YNAB transaction for the difference. The import_id format is the v1 format,
byte for byte (DESIGN §2.5), because YNAB dedupes on it and existing adjustments carry it::

    CRYPTO:{delta_milli}:{local_today}:1
    IBKR:{delta_milli}:{local_today}:1
    RECON:{delta_milli}:{as_of}:1

A difference under one cent (``|delta| < 10`` milliunits) counts as in sync and posts
nothing. A ``duplicate_import_ids`` answer means the very same adjustment was already
posted (same delta, same day) and is reported as outcome ``duplicate``, not as an error.

Valuation syncs (crypto, IBKR) compare against the account's *working* balance
(cleared + uncleared, :func:`working_balance`), not the cleared balance: a transfer into the
tracking account is usually uncleared on that side while the market value already includes
it, so comparing against the cleared balance counted it twice (finding F49).
"""

from __future__ import annotations

import logging
import re
from dataclasses import asdict, dataclass
from datetime import date as Date
from datetime import datetime
from typing import Any, Final

from ynab_helper.errors import ConfigError, YnabError, YnabHelperError, YnabValidationError
from ynab_helper.money import format_money
from ynab_helper.timeutil import local_today
from ynab_helper.ynab.client import MAX_IMPORT_ID_LENGTH, YnabClient

__all__ = [
    "ADJUSTMENT_PREFIXES",
    "IN_SYNC_THRESHOLD_MILLI",
    "MAX_MEMO_LENGTH",
    "OUTCOME_CREATED",
    "OUTCOME_DRY_RUN",
    "OUTCOME_DUPLICATE",
    "OUTCOME_IN_SYNC",
    "PAYEE_CRYPTO",
    "PAYEE_IBKR",
    "PAYEE_RECON",
    "PREFIX_CRYPTO",
    "PREFIX_IBKR",
    "PREFIX_RECON",
    "AccountBalances",
    "AdjustmentRefused",
    "AdjustmentResult",
    "account_balances",
    "adjustment_import_id",
    "check_valuation_currency",
    "clip_memo",
    "local_day",
    "post_balance_adjustment",
    "working_balance",
]

log = logging.getLogger(__name__)
audit = logging.getLogger("ynab_helper.audit")

#: Differences below one cent are "in sync" (v1 rule).
IN_SYNC_THRESHOLD_MILLI: Final = 10
#: v1 clipped adjustment memos to 200 characters ("…" as three dots).
MAX_MEMO_LENGTH: Final = 200

PREFIX_CRYPTO: Final = "CRYPTO"
PREFIX_IBKR: Final = "IBKR"
PREFIX_RECON: Final = "RECON"
#: The only import_id prefixes an adjustment may use (never the importer's ``YNAB:``).
ADJUSTMENT_PREFIXES: Final = frozenset({PREFIX_CRYPTO, PREFIX_IBKR, PREFIX_RECON})

#: Payee names (DESIGN §2.6). YNAB rejects payees starting with its reserved names such as
#: "Reconciliation Balance Adjustment", hence "CSV Reconciliation".
PAYEE_CRYPTO: Final = "Crypto Portfolio Adjustment"
PAYEE_IBKR: Final = "IBKR Portfolio Adjustment"
PAYEE_RECON: Final = "CSV Reconciliation"

OUTCOME_IN_SYNC: Final = "in_sync"
OUTCOME_CREATED: Final = "created"
OUTCOME_DUPLICATE: Final = "duplicate"
OUTCOME_DRY_RUN: Final = "dry_run"

_CLEARED_VALUES: Final = frozenset({"cleared", "uncleared", "reconciled"})
_ISO_DAY_RE: Final = re.compile(r"\d{4}-\d{2}-\d{2}")


class AdjustmentRefused(YnabHelperError):
    """A valuation looked wrong, so nothing was posted (``user_message`` explains why).

    Raised for example when an xpub scan found no used address while YNAB holds a value, or
    when IBKR reports a net asset value of zero or less. ``reason`` is a short machine code.
    """

    def __init__(self, message: str, *, reason: str, user_message: str | None = None) -> None:
        super().__init__(message, user_message=user_message)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class AdjustmentResult:
    """What :func:`post_balance_adjustment` did.

    ``outcome`` is one of ``in_sync`` (nothing to post), ``created`` (``ynab_tx_id`` is the
    new transaction), ``duplicate`` (YNAB already holds this import_id, nothing new was
    posted) or ``dry_run`` (would have posted ``delta_milli``). ``import_id`` is None only
    when in sync.
    """

    outcome: str
    delta_milli: int
    import_id: str | None
    ynab_tx_id: str | None
    target_milli: int
    current_milli: int
    account_id: str
    date: str
    payee: str
    memo: str | None
    cleared: str

    @property
    def in_sync(self) -> bool:
        return self.outcome == OUTCOME_IN_SYNC

    @property
    def posted(self) -> bool:
        """True only if this call created a YNAB transaction."""
        return self.outcome == OUTCOME_CREATED

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe representation (amounts stay integer milliunits)."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AccountBalances:
    """Balances of one YNAB account in milliunits, read straight from YNAB."""

    account_id: str
    working_milli: int  # cleared + uncleared ("balance" in YNAB's API)
    cleared_milli: int
    uncleared_milli: int
    closed: bool


# ── helpers ────────────────────────────────────────────────────────────────


def _require_milli(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be integer milliunits, got {type(value).__name__}")
    return value


def local_day(value: str | Date | None = None) -> Date:
    """The local calendar day an adjustment is dated with.

    ``None`` means today in the process timezone. A ``datetime`` (a subclass of ``date``,
    so it passes a ``date`` annotation) is reduced to its day: an aware one is converted to
    the process timezone first, a naive one is taken as local time. Without this, a
    ``datetime`` would leak its time part into the v1 import_id
    (``IBKR:…:2026-10-16T09:30:00:1``). A string must be exactly ``YYYY-MM-DD``.

    Raises:
        ValueError: a string that is not a valid ``YYYY-MM-DD`` date.
        TypeError: any other type.
    """
    if value is None:
        return local_today()
    if isinstance(value, datetime):
        return (value.astimezone() if value.tzinfo is not None else value).date()
    if isinstance(value, Date):
        return value
    if isinstance(value, str):
        if _ISO_DAY_RE.fullmatch(value):
            try:
                return Date.fromisoformat(value)
            except ValueError:
                pass
        raise ValueError(f"date must be a date or 'YYYY-MM-DD', got {value!r}")
    raise TypeError(f"date must be a date or 'YYYY-MM-DD', got {type(value).__name__}")


def _iso_date(value: str | Date) -> str:
    if value is None:
        raise ValueError("date is required")
    return local_day(value).isoformat()


def adjustment_import_id(prefix: str, delta_milli: int, day: str | Date) -> str:
    """The legacy adjustment import_id ``{prefix}:{delta}:{date}:1`` (DESIGN §2.5).

    ``day`` may be a ``date``, a ``datetime`` (reduced to its local day, see
    :func:`local_day`) or a ``YYYY-MM-DD`` string.

    Raises:
        ValueError: unknown prefix, invalid date, or an id longer than YNAB accepts.
        TypeError: ``day`` is neither a date nor a string.
    """
    if prefix not in ADJUSTMENT_PREFIXES:
        raise ValueError(f"unknown adjustment prefix {prefix!r}")
    import_id = f"{prefix}:{_require_milli('delta_milli', delta_milli)}:{_iso_date(day)}:1"
    if len(import_id) > MAX_IMPORT_ID_LENGTH:
        raise ValueError(f"adjustment import_id {import_id!r} exceeds {MAX_IMPORT_ID_LENGTH} chars")
    return import_id


def clip_memo(memo: str | None) -> str | None:
    """v1's memo rule: at most 200 characters (``…`` written as ``...``); empty → None."""
    if not memo:
        return None
    if len(memo) > MAX_MEMO_LENGTH:
        return memo[: MAX_MEMO_LENGTH - 3] + "..."
    return memo


def account_balances(client: YnabClient, budget_id: str, account_id: str) -> AccountBalances:
    """Read the account from YNAB (one GET) and return its balances.

    Raises:
        ConfigError: the account was deleted in YNAB.
        YnabError: YNAB omitted a balance field or reported inconsistent balances (the
            working balance must equal cleared + uncleared). Never defaults to 0 (F49).
    """
    account = client.get_account(budget_id, account_id)
    if account.deleted:
        raise ConfigError(
            f"YNAB account {account_id} is deleted",
            user_message="The configured YNAB account has been deleted. Choose another account.",
        )
    cleared, uncleared, working = (
        account.cleared_balance,
        account.uncleared_balance,
        account.balance,
    )
    if cleared is None or uncleared is None:
        raise YnabError(
            f"YNAB account {account_id}: cleared/uncleared balance missing",
            user_message="YNAB did not report the account balance; nothing was posted.",
        )
    if working is not None and working != cleared + uncleared:
        raise YnabError(
            f"YNAB account {account_id}: balance {working} != cleared {cleared} + "
            f"uncleared {uncleared}",
            user_message="YNAB reported inconsistent account balances; nothing was posted.",
        )
    return AccountBalances(
        account_id=account_id,
        working_milli=cleared + uncleared,
        cleared_milli=cleared,
        uncleared_milli=uncleared,
        closed=account.closed,
    )


def working_balance(client: YnabClient, budget_id: str, account_id: str) -> int:
    """The account's working balance (cleared + uncleared) in milliunits (F49).

    Raises:
        YnabError: a balance field is missing or inconsistent (see :func:`account_balances`).
    """
    return account_balances(client, budget_id, account_id).working_milli


def check_valuation_currency(budget_currency: str | None, valuation_currency: str) -> None:
    """Refuse to post a valuation in ``valuation_currency`` into a budget of another currency.

    ``budget_currency`` None means "not known to the caller" and is not checked.

    Raises:
        ConfigError: the currencies differ.
    """
    if budget_currency is None:
        return
    if budget_currency.strip().upper() != valuation_currency.upper():
        raise ConfigError(
            f"budget currency {budget_currency} != valuation currency {valuation_currency}",
            user_message=f"Values are in {valuation_currency}, but the YNAB budget is in "
            f"{budget_currency}; nothing was posted.",
        )


# ── posting ────────────────────────────────────────────────────────────────


def post_balance_adjustment(
    client: YnabClient,
    budget_id: str,
    account_id: str,
    *,
    target_milli: int,
    current_milli: int,
    payee: str,
    memo: str | None,
    prefix: str,
    date: str | Date,
    cleared: str,
    dry_run: bool = False,
) -> AdjustmentResult:
    """Post one adjustment of ``target_milli - current_milli`` (unless in sync or dry run).

    The transaction is ``{account_id, date, amount: delta, payee_name: payee, memo (clipped
    to 200), cleared, approved: True, import_id: {prefix}:{delta}:{date}:1}``.

    Raises:
        ValueError / TypeError: invalid arguments (nothing is sent).
        YnabError: the POST failed (``YnabRateLimited``, ``YnabAuthError``, …) or YNAB
            neither created the transaction nor reported it as a duplicate.
    """
    target = _require_milli("target_milli", target_milli)
    current = _require_milli("current_milli", current_milli)
    if cleared not in _CLEARED_VALUES:
        raise ValueError(f"invalid cleared value {cleared!r}")
    if not isinstance(payee, str) or not payee.strip():
        raise ValueError("payee must be a non-empty string")
    if prefix not in ADJUSTMENT_PREFIXES:
        raise ValueError(f"unknown adjustment prefix {prefix!r}")
    day = _iso_date(date)
    delta = target - current
    clipped = clip_memo(memo)

    def result(outcome: str, import_id: str | None, tx_id: str | None = None) -> AdjustmentResult:
        return AdjustmentResult(
            outcome=outcome,
            delta_milli=delta,
            import_id=import_id,
            ynab_tx_id=tx_id,
            target_milli=target,
            current_milli=current,
            account_id=account_id,
            date=day,
            payee=payee,
            memo=clipped,
            cleared=cleared,
        )

    if abs(delta) < IN_SYNC_THRESHOLD_MILLI:
        log.info("%s adjustment: in sync (delta %d milli)", prefix, delta)
        return result(OUTCOME_IN_SYNC, None)
    import_id = adjustment_import_id(prefix, delta, day)
    if dry_run:
        log.info("%s adjustment: dry run, would post %s", prefix, format_money(delta, sign=True))
        return result(OUTCOME_DRY_RUN, import_id)

    tx: dict[str, Any] = {
        "account_id": account_id,
        "date": day,
        "amount": delta,
        "payee_name": payee,
        "cleared": cleared,
        "approved": True,
        "import_id": import_id,
    }
    if clipped is not None:
        tx["memo"] = clipped
    try:
        created = client.create_transactions(budget_id, [tx])
    except YnabValidationError as exc:
        # The bulk endpoint answers duplicates with 201 + duplicate_import_ids; a 409 has
        # also been reported for the single-transaction form. Only treat it as a duplicate
        # if YNAB really holds the import_id.
        if exc.status != 409:
            raise
        existing = _find_by_import_id(client, budget_id, account_id, import_id, day)
        if existing is None:
            raise
        log.info("%s adjustment: duplicate import_id %s (409)", prefix, import_id)
        return result(OUTCOME_DUPLICATE, import_id, existing)

    if created.created:  # exactly one transaction was sent
        new_id = created.created[0].id
        audit.info(
            "created adjustment %s date=%s amount=%s payee=%r account=%s ynab_id=%s",
            import_id,
            day,
            format_money(delta, sign=True),
            payee,
            account_id,
            new_id,
        )
        return result(OUTCOME_CREATED, import_id, new_id)
    if import_id in created.duplicate_import_ids:
        log.info("%s adjustment: duplicate import_id %s, nothing posted", prefix, import_id)
        return result(OUTCOME_DUPLICATE, import_id)
    raise YnabError(
        f"YNAB neither created nor rejected adjustment {import_id}",
        user_message="YNAB did not confirm the adjustment. Check YNAB before trying again.",
    )


def _find_by_import_id(
    client: YnabClient, budget_id: str, account_id: str, import_id: str, day: str
) -> str | None:
    """Id of the live transaction carrying ``import_id`` in the account (dated ``day`` or later)."""
    txs, _ = client.get_transactions(budget_id, account_id=account_id, since_date=day)
    for tx in txs:
        if tx.import_id == import_id and not tx.deleted:
            return tx.id
    return None
