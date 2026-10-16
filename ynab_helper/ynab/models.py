"""Typed views of YNAB API objects (docs/DESIGN.md §4.2).

Every model keeps the dict it was built from in ``.raw`` so callers (the mirror sync,
the dedupe scan) can store or inspect fields this module does not model.

Amounts are integer **milliunits** exactly as YNAB sends them (``-12340`` = -12.34).
Parsing is strict for the fields that drive importer decisions (``id``, ``date``,
``amount``, ``account_id``, ``cleared``): a malformed payload raises :class:`YnabError`
instead of being defaulted, because a wrong default (for example treating a missing
``cleared`` as ``"uncleared"``) could make the importer downgrade a reconciled transaction.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from ynab_helper.errors import YnabError

__all__ = [
    "CLEARED_STATES",
    "YnabAccount",
    "YnabSubtransaction",
    "YnabTransaction",
]

#: Values YNAB uses for ``cleared``. Unknown values are kept verbatim (not rejected) so a
#: future API addition does not break syncing; consumers compare against these constants.
CLEARED_STATES = frozenset({"cleared", "uncleared", "reconciled"})

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _malformed(kind: str, reason: str) -> YnabError:
    return YnabError(
        f"malformed {kind} in YNAB response: {reason}",
        user_message="YNAB returned data in an unexpected format. Please try again later.",
    )


def _require_mapping(data: Any, kind: str) -> Mapping[str, Any]:
    if not isinstance(data, Mapping):
        raise _malformed(kind, f"expected an object, got {type(data).__name__}")
    return data


def _req_str(data: Mapping[str, Any], key: str, kind: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise _malformed(kind, f"field {key!r} missing or not a string")
    return value


def _opt_str(data: Mapping[str, Any], key: str, kind: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise _malformed(kind, f"field {key!r} is not a string")
    return value


def _req_milli(data: Mapping[str, Any], key: str, kind: str) -> int:
    value = data.get(key)
    # bool is a subclass of int; a float would mean a unit mix-up. Both are rejected.
    if not isinstance(value, int) or isinstance(value, bool):
        raise _malformed(kind, f"field {key!r} missing or not integer milliunits")
    return value


def _opt_milli(data: Mapping[str, Any], key: str, kind: str) -> int | None:
    if data.get(key) is None:
        return None
    return _req_milli(data, key, kind)


def _bool(data: Mapping[str, Any], key: str, kind: str, *, default: bool) -> bool:
    value = data.get(key, default)
    if value is None:
        return default
    if not isinstance(value, bool):
        raise _malformed(kind, f"field {key!r} is not a boolean")
    return value


def _req_date(data: Mapping[str, Any], key: str, kind: str) -> str:
    value = _req_str(data, key, kind)
    if not _DATE_RE.fullmatch(value):
        raise _malformed(kind, f"field {key!r} is not an ISO date")
    return value


@dataclass(frozen=True, slots=True)
class YnabSubtransaction:
    """One line of a split transaction."""

    id: str
    transaction_id: str
    amount: int
    memo: str | None
    payee_id: str | None
    payee_name: str | None
    category_id: str | None
    category_name: str | None
    transfer_account_id: str | None
    transfer_transaction_id: str | None
    deleted: bool
    raw: Mapping[str, Any] = field(repr=False, compare=False)

    @classmethod
    def from_api(cls, data: Any, *, parent_id: str | None = None) -> YnabSubtransaction:
        """Build from a YNAB ``SubTransaction`` object.

        ``parent_id`` fills ``transaction_id`` when the payload omits it.
        """
        kind = "subtransaction"
        d = _require_mapping(data, kind)
        transaction_id = _opt_str(d, "transaction_id", kind) or parent_id
        if not transaction_id:
            raise _malformed(kind, "field 'transaction_id' missing")
        return cls(
            id=_req_str(d, "id", kind),
            transaction_id=transaction_id,
            amount=_req_milli(d, "amount", kind),
            memo=_opt_str(d, "memo", kind),
            payee_id=_opt_str(d, "payee_id", kind),
            payee_name=_opt_str(d, "payee_name", kind),
            category_id=_opt_str(d, "category_id", kind),
            category_name=_opt_str(d, "category_name", kind),
            transfer_account_id=_opt_str(d, "transfer_account_id", kind),
            transfer_transaction_id=_opt_str(d, "transfer_transaction_id", kind),
            deleted=_bool(d, "deleted", kind, default=False),
            raw=d,
        )


@dataclass(frozen=True, slots=True)
class YnabTransaction:
    """A YNAB ``TransactionDetail`` (also used for the bodies of create/update responses)."""

    id: str
    account_id: str
    date: str
    amount: int
    cleared: str
    approved: bool
    memo: str | None
    payee_id: str | None
    payee_name: str | None
    category_id: str | None
    category_name: str | None
    flag_color: str | None
    account_name: str | None
    transfer_account_id: str | None
    transfer_transaction_id: str | None
    matched_transaction_id: str | None
    import_id: str | None
    import_payee_name: str | None
    import_payee_name_original: str | None
    deleted: bool
    subtransactions: tuple[YnabSubtransaction, ...]
    raw: Mapping[str, Any] = field(repr=False, compare=False)

    @classmethod
    def from_api(cls, data: Any) -> YnabTransaction:
        """Build from a YNAB transaction object; raises :class:`YnabError` if malformed."""
        kind = "transaction"
        d = _require_mapping(data, kind)
        tx_id = _req_str(d, "id", kind)
        subs_raw = d.get("subtransactions") or []
        if not isinstance(subs_raw, list):
            raise _malformed(kind, "field 'subtransactions' is not a list")
        return cls(
            id=tx_id,
            account_id=_req_str(d, "account_id", kind),
            date=_req_date(d, "date", kind),
            amount=_req_milli(d, "amount", kind),
            cleared=_req_str(d, "cleared", kind),
            approved=_bool(d, "approved", kind, default=False),
            memo=_opt_str(d, "memo", kind),
            payee_id=_opt_str(d, "payee_id", kind),
            payee_name=_opt_str(d, "payee_name", kind),
            category_id=_opt_str(d, "category_id", kind),
            category_name=_opt_str(d, "category_name", kind),
            flag_color=_opt_str(d, "flag_color", kind),
            account_name=_opt_str(d, "account_name", kind),
            transfer_account_id=_opt_str(d, "transfer_account_id", kind),
            transfer_transaction_id=_opt_str(d, "transfer_transaction_id", kind),
            matched_transaction_id=_opt_str(d, "matched_transaction_id", kind),
            import_id=_opt_str(d, "import_id", kind),
            import_payee_name=_opt_str(d, "import_payee_name", kind),
            import_payee_name_original=_opt_str(d, "import_payee_name_original", kind),
            deleted=_bool(d, "deleted", kind, default=False),
            subtransactions=tuple(
                YnabSubtransaction.from_api(s, parent_id=tx_id) for s in subs_raw
            ),
            raw=d,
        )

    @property
    def is_split(self) -> bool:
        """True if the transaction has (non-deleted) subtransactions."""
        return any(not s.deleted for s in self.subtransactions)

    @property
    def is_transfer(self) -> bool:
        return self.transfer_account_id is not None

    @property
    def is_cleared(self) -> bool:
        """Cleared or reconciled (the states the importer must never downgrade)."""
        return self.cleared in ("cleared", "reconciled")


@dataclass(frozen=True, slots=True)
class YnabAccount:
    """A YNAB ``Account``. Balances are milliunits; ``None`` only if YNAB omitted them."""

    id: str
    name: str
    type: str | None
    on_budget: bool
    closed: bool
    deleted: bool
    note: str | None
    balance: int | None
    cleared_balance: int | None
    uncleared_balance: int | None
    transfer_payee_id: str | None
    last_reconciled_at: str | None
    raw: Mapping[str, Any] = field(repr=False, compare=False)

    @classmethod
    def from_api(cls, data: Any) -> YnabAccount:
        """Build from a YNAB account object; raises :class:`YnabError` if malformed."""
        kind = "account"
        d = _require_mapping(data, kind)
        return cls(
            id=_req_str(d, "id", kind),
            name=_opt_str(d, "name", kind) or "",
            type=_opt_str(d, "type", kind),
            on_budget=_bool(d, "on_budget", kind, default=True),
            closed=_bool(d, "closed", kind, default=False),
            deleted=_bool(d, "deleted", kind, default=False),
            note=_opt_str(d, "note", kind),
            balance=_opt_milli(d, "balance", kind),
            cleared_balance=_opt_milli(d, "cleared_balance", kind),
            uncleared_balance=_opt_milli(d, "uncleared_balance", kind),
            transfer_payee_id=_opt_str(d, "transfer_payee_id", kind),
            last_reconciled_at=_opt_str(d, "last_reconciled_at", kind),
            raw=d,
        )
