"""Stateful in-memory fake of the YNAB API v1 (docs/DESIGN.md §12).

Plug it into the real client::

    fake = FakeYnab()
    fake.add_budget("b1", currency="CHF")
    fake.add_account("b1", "acc", "Revolut")
    client = fake.client()                      # YnabClient(transport=fake.transport, …)

or drive the frozen v1 importer with ``fake.v1_request`` (same signature as v1's
``ynab_request``).

Modelled YNAB semantics (the ones the importer's correctness depends on):

* ``server_knowledge`` per budget. Every successful mutating request (and every seed /
  simulated user edit) bumps it once and stamps the changed entities, so a delta request
  (``last_knowledge_of_server=N``) returns exactly the entities changed after ``N``,
  deleted ones included. A request *without* ``last_knowledge_of_server`` returns a full
  list that excludes deleted transactions/accounts (as the real API does).
* Account ``balance``/``cleared_balance``/``uncleared_balance`` are computed from the
  account's non-deleted transactions (cleared = cleared + reconciled). An account is part
  of an accounts delta when its balances changed.
* ``POST /transactions`` dedupes on ``import_id`` per account; by default deleted
  transactions still block their import_id (``dedupe_includes_deleted``). Duplicates are
  skipped and reported in ``duplicate_import_ids`` with HTTP 201.
* Requests are atomic: a 4xx changes nothing (a single bad row fails a whole bulk request).
* ``import_id`` is immutable: updates that carry a different one are applied without it
  (attempts are recorded in ``import_id_change_attempts``).
* ``DELETE`` is a soft delete (``deleted: true``), returns the row and is visible in deltas;
  deleting/updating an already deleted or unknown transaction is a 404.
* Changing the amount of a split transaction is a 400; a date change on a split parent is
  ignored (YNAB documents both). Transfers keep both sides' amount/date in sync and are
  deleted together.
* Validation mirrors YNAB's limits (import_id ≤ 36 chars, payee_name ≤ 200, memo ≤ 500,
  integer milliunits, ISO dates, known accounts/categories/payees). Unknown body fields
  are rejected (stricter than YNAB, to catch typos in our payloads). Future dates are
  rejected only when ``today`` is set.

Things the fake does *not* model raise ``NotImplementedError`` (moving or re-pointing a
transfer, transfer subtransactions) so a test can never silently rely on invented behaviour.
YNAB's matching of imported transactions with user-entered ones is not modelled.

Fault injection: :meth:`FakeYnab.fail_next`, :meth:`FakeYnab.fail_at_call`,
:meth:`FakeYnab.raise_network_error_next`, :meth:`FakeYnab.raise_network_error_at_call`;
each can fire *before* the request is applied (nothing changes) or ``after_apply`` (the
change happens but the response is lost — the classic "did my POST go through?" case).
"""

from __future__ import annotations

import json
import re
import threading
import urllib.error
import urllib.parse
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date as _date
from typing import Any, NamedTuple

from ynab_helper.ynab.client import DEFAULT_BASE_URL, YnabClient

__all__ = ["FAKE_TOKEN", "Call", "FakeYnab", "RecordedRequest"]

FAKE_TOKEN = "test-token"
CLEARED_VALUES = frozenset({"cleared", "uncleared", "reconciled"})
FLAG_COLORS = frozenset({"red", "orange", "yellow", "green", "blue", "purple"})
MUTATING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
SPLIT_CATEGORY_NAME = "Split (Multiple Categories)..."
INFLOW_CATEGORY_NAME = "Inflow: Ready to Assign"

MAX_IMPORT_ID = 36
MAX_PAYEE_NAME = 200
MAX_MEMO = 500

_TX_FIELDS = frozenset(
    {
        "account_id",
        "date",
        "amount",
        "payee_id",
        "payee_name",
        "category_id",
        "memo",
        "cleared",
        "approved",
        "flag_color",
        "import_id",
        "subtransactions",
    }
)
_UPDATE_FIELDS = _TX_FIELDS | {"id"}
_SUB_FIELDS = frozenset({"amount", "payee_id", "payee_name", "category_id", "memo"})
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

_CURRENCY_FORMATS: dict[str, dict[str, Any]] = {
    "CHF": {
        "example_format": "123'456.78",
        "decimal_separator": ".",
        "symbol_first": True,
        "group_separator": "'",
        "currency_symbol": "CHF",
    },
    "EUR": {
        "example_format": "123.456,78",
        "decimal_separator": ",",
        "symbol_first": False,
        "group_separator": ".",
        "currency_symbol": "€",
    },
    "USD": {
        "example_format": "123,456.78",
        "decimal_separator": ".",
        "symbol_first": True,
        "group_separator": ",",
        "currency_symbol": "$",
    },
    "GBP": {
        "example_format": "123,456.78",
        "decimal_separator": ".",
        "symbol_first": True,
        "group_separator": ",",
        "currency_symbol": "£",
    },
}


class Call(NamedTuple):
    """One request as seen by the fake: ``(method, path, query, body)``.

    ``path`` is relative to the API base (``/budgets/b1/transactions``) exactly as sent
    (percent-encoding preserved; ids are decoded only for routing), ``query`` maps
    parameter → first value (decoded), ``body`` is the decoded JSON (or None).
    """

    method: str
    path: str
    query: dict[str, str]
    body: Any


@dataclass
class RecordedRequest:
    """Full detail of one request (``status`` is None when a network error was raised)."""

    method: str
    path: str
    query: dict[str, str]
    body: Any
    headers: dict[str, str]
    timeout: float | None
    url: str
    status: int | None = None
    fault: str | None = None


class _ApiError(Exception):
    def __init__(self, status: int, error_id: str, name: str, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.body = {"error": {"id": error_id, "name": name, "detail": detail}}


def _bad_request(detail: str) -> _ApiError:
    return _ApiError(400, "400", "bad_request", detail)


def _not_found(detail: str = "Resource not found") -> _ApiError:
    return _ApiError(404, "404.2", "resource_not_found", detail)


_DEFAULT_ERROR_BODIES: dict[int, tuple[str, str, str]] = {
    400: ("400", "bad_request", "Bad request"),
    401: ("401", "unauthorized", "Unauthorized"),
    403: ("403.1", "subscription_lapsed", "Subscription for this account has lapsed."),
    404: ("404.2", "resource_not_found", "Resource not found"),
    409: ("409", "conflict", "Conflict"),
    429: ("429", "too_many_requests", "Too many requests"),
    500: ("500", "internal_server_error", "Internal server error"),
    502: ("502", "bad_gateway", "Bad gateway"),
    503: ("503", "service_unavailable", "Service unavailable"),
    504: ("504", "gateway_timeout", "Gateway timeout"),
}


def _error_body(status: int) -> dict[str, Any]:
    error_id, name, detail = _DEFAULT_ERROR_BODIES.get(
        status, (str(status), "error", f"HTTP {status}")
    )
    return {"error": {"id": error_id, "name": name, "detail": detail}}


@dataclass
class _Fault:
    method: str | None
    path_re: re.Pattern[str] | None
    remaining: int
    status: int | None  # None → network error
    retry_after: float | None = None
    body: Any = None
    after_apply: bool = False
    exc: BaseException | None = None
    at_mutating_call: int | None = None

    def matches(self, method: str, path: str, mutating_calls: int, is_mutating: bool) -> bool:
        if self.remaining <= 0:
            return False
        if self.at_mutating_call is not None:
            return is_mutating and mutating_calls == self.at_mutating_call
        if self.method is not None and self.method != method:
            return False
        return self.path_re is None or self.path_re.fullmatch(path) is not None


@dataclass
class _Sub:
    id: str
    transaction_id: str
    amount: int
    memo: str | None = None
    payee_id: str | None = None
    payee_name: str | None = None
    category_id: str | None = None
    deleted: bool = False


@dataclass
class _Tx:
    id: str
    budget_id: str
    account_id: str
    date: str
    amount: int
    seq: int
    memo: str | None = None
    cleared: str = "uncleared"
    approved: bool = False
    flag_color: str | None = None
    payee_id: str | None = None
    payee_name: str | None = None
    category_id: str | None = None
    transfer_account_id: str | None = None
    transfer_transaction_id: str | None = None
    matched_transaction_id: str | None = None
    import_id: str | None = None
    import_payee_name: str | None = None
    import_payee_name_original: str | None = None
    deleted: bool = False
    subtransactions: list[_Sub] = field(default_factory=list)
    knowledge: int = 0
    # (account_id, amount, counts as cleared) currently included in the account totals;
    # None while not counted (new or deleted). Maintained only by FakeYnab._touch.
    contrib: tuple[str, int, bool] | None = None

    @property
    def is_split(self) -> bool:
        return any(not s.deleted for s in self.subtransactions)


@dataclass
class _Account:
    id: str
    name: str
    type: str
    on_budget: bool
    closed: bool
    transfer_payee_id: str
    note: str | None = None
    deleted: bool = False
    knowledge: int = 0


@dataclass
class _Budget:
    id: str
    name: str
    currency: str | None
    seq: int
    knowledge: int = 0
    accounts: dict[str, _Account] = field(default_factory=dict)
    transactions: dict[str, _Tx] = field(default_factory=dict)
    payees: dict[str, str] = field(default_factory=dict)  # id → name
    categories: dict[str, str] = field(default_factory=dict)  # id → name
    # Indexes so large seeds (thousands of rows) and convergence sweeps stay fast.
    totals: dict[str, list[int]] = field(default_factory=dict)  # account → [bal, clr, unclr]
    by_import_id: dict[str, list[_Tx]] = field(default_factory=dict)
    payee_by_name: dict[str, str] = field(default_factory=dict)  # non-transfer payees


_ROUTES: list[tuple[str, re.Pattern[str], str]] = [
    (m, re.compile(p), h)
    for m, p, h in [
        ("GET", r"/user", "_get_user"),
        ("GET", r"/budgets", "_get_budgets"),
        ("GET", r"/budgets/(?P<b>[^/]+)/settings", "_get_settings"),
        ("GET", r"/budgets/(?P<b>[^/]+)/accounts", "_get_accounts"),
        ("GET", r"/budgets/(?P<b>[^/]+)/accounts/(?P<a>[^/]+)", "_get_account"),
        ("GET", r"/budgets/(?P<b>[^/]+)/accounts/(?P<a>[^/]+)/transactions", "_get_transactions"),
        ("GET", r"/budgets/(?P<b>[^/]+)/transactions", "_get_transactions"),
        ("POST", r"/budgets/(?P<b>[^/]+)/transactions", "_post_transactions"),
        ("PATCH", r"/budgets/(?P<b>[^/]+)/transactions", "_patch_transactions"),
        ("GET", r"/budgets/(?P<b>[^/]+)/transactions/(?P<t>[^/]+)", "_get_transaction"),
        ("PUT", r"/budgets/(?P<b>[^/]+)/transactions/(?P<t>[^/]+)", "_put_transaction"),
        ("PATCH", r"/budgets/(?P<b>[^/]+)/transactions/(?P<t>[^/]+)", "_put_transaction"),
        ("DELETE", r"/budgets/(?P<b>[^/]+)/transactions/(?P<t>[^/]+)", "_delete_transaction"),
    ]
]


class FakeYnab:
    """In-memory YNAB API v1. See the module docstring for the modelled semantics.

    Args:
        token: the only accepted bearer token (None accepts any token).
        base_url: URL prefix the transport answers to.
        dedupe_includes_deleted: deleted transactions still block their import_id on create.
        rate_limit: the limit reported in ``X-Rate-Limit`` (``"<requests>/<limit>"``).
        quota: if set, every request after the first ``quota`` ones gets a 429.
        today: if set, creates/updates with a date after it are rejected (YNAB forbids
            future dates); None disables the check so fixtures need no clock.
        single_duplicate_conflict: answer a duplicate import_id in a single-``transaction``
            POST with 409 instead of 201 + ``duplicate_import_ids`` (both shapes have been
            reported for the real API; tests of adjustment posting can cover both).
    """

    def __init__(
        self,
        *,
        token: str | None = FAKE_TOKEN,
        base_url: str = DEFAULT_BASE_URL,
        dedupe_includes_deleted: bool = True,
        rate_limit: int = 200,
        quota: int | None = None,
        today: _date | str | None = None,
        single_duplicate_conflict: bool = False,
    ) -> None:
        self.token = token
        self.base_url = base_url.rstrip("/")
        self.dedupe_includes_deleted = dedupe_includes_deleted
        self.rate_limit = rate_limit
        self.quota = quota
        self.today = _date.fromisoformat(today) if isinstance(today, str) else today
        self.single_duplicate_conflict = single_duplicate_conflict
        self.user_id = "00000000-0000-4000-8000-0000000000aa"

        self.requests: list[RecordedRequest] = []
        self.request_count = 0
        self.mutating_calls = 0
        self.slept: list[float] = []
        self.import_id_change_attempts: list[tuple[str, str | None, Any]] = []

        self._budgets: dict[str, _Budget] = {}
        self._faults: list[_Fault] = []
        self._ids = 0
        self._seq = 0
        self._lock = threading.RLock()

    # ── seeding ─────────────────────────────────────────────────────────────

    def add_budget(
        self, budget_id: str, currency: str | None = "CHF", *, name: str | None = None
    ) -> str:
        """Add a budget (``currency=None`` → ``currency_format: null``). Returns its id."""
        with self._lock:
            if budget_id in self._budgets:
                raise ValueError(f"budget {budget_id!r} already exists")
            budget = _Budget(budget_id, name or f"Budget {budget_id}", currency, self._next_seq())
            budget.categories[self.inflow_category_id(budget_id)] = INFLOW_CATEGORY_NAME
            self._budgets[budget_id] = budget
            return budget_id

    def add_account(
        self,
        budget_id: str,
        account_id: str,
        name: str,
        type: str = "checking",
        on_budget: bool = True,
        closed: bool = False,
        *,
        note: str | None = None,
        deleted: bool = False,
    ) -> str:
        """Add an account (with its ``Transfer : <name>`` payee). Returns its id."""
        with self._lock:
            budget = self._budget_or_value_error(budget_id)
            if account_id in budget.accounts:
                raise ValueError(f"account {account_id!r} already exists")
            payee_id = self._new_id("payee")
            budget.payees[payee_id] = f"Transfer : {name}"
            account = _Account(
                account_id, name, type, on_budget, closed, payee_id, note=note, deleted=deleted
            )
            budget.accounts[account_id] = account
            self._bump_account(budget, account)
            return account_id

    def add_category(self, budget_id: str, category_id: str, name: str) -> str:
        """Register a category so transactions can reference ``category_id``."""
        with self._lock:
            self._budget_or_value_error(budget_id).categories[category_id] = name
            return category_id

    @staticmethod
    def inflow_category_id(budget_id: str) -> str:
        """Id of the budget's built-in ``Inflow: Ready to Assign`` category."""
        return f"inflow-{budget_id}"

    def add_transaction(
        self,
        budget_id: str,
        account_id: str,
        *,
        amount: int,
        date: str,
        payee_name: str | None = None,
        memo: str | None = None,
        cleared: str = "cleared",
        approved: bool = True,
        import_id: str | None = None,
        id: str | None = None,
        category_id: str | None = None,
        category_name: str | None = None,
        flag_color: str | None = None,
        deleted: bool = False,
        subtransactions: Iterable[Mapping[str, Any]] | None = None,
        transfer_account_id: str | None = None,
        matched_transaction_id: str | None = None,
    ) -> str:
        """Seed a transaction as if the user (or a bank import) had created it in YNAB.

        Unlike the API this bypasses future-date checks and lets ``cleared`` be anything
        valid, but it refuses a second *live* transaction with the same import_id in the
        account (YNAB cannot hold that). ``category_name`` registers the category if needed.
        ``subtransactions`` items take ``amount, memo, payee_name, category_id,
        category_name``. ``transfer_account_id`` also creates the counterpart (amount
        negated, ``uncleared``). Returns the transaction id.
        """
        with self._lock:
            budget = self._budget_or_value_error(budget_id)
            if account_id not in budget.accounts:
                raise ValueError(f"unknown account {account_id!r}")
            if transfer_account_id is not None and (
                transfer_account_id not in budget.accounts or transfer_account_id == account_id
            ):
                raise ValueError(f"invalid transfer account {transfer_account_id!r}")
            if transfer_account_id is not None and subtransactions is not None:
                raise NotImplementedError("split transfers are not modelled by FakeYnab")
            _check_int(amount, "amount")
            _check_date_str(date)
            if cleared not in CLEARED_VALUES:
                raise ValueError(f"invalid cleared {cleared!r}")
            if import_id is not None and not deleted:
                clash = self._find_import_id(budget, account_id, import_id, include_deleted=False)
                if clash is not None:
                    raise ValueError(f"live transaction with import_id {import_id!r} exists")
            if category_name is not None and category_id is None:
                category_id = self._category_by_name(budget, category_name)
            elif category_id is not None and category_id not in budget.categories:
                budget.categories[category_id] = category_name or category_id
            tx = _Tx(
                id=id or self._new_id("tx"),
                budget_id=budget_id,
                account_id=account_id,
                date=date,
                amount=amount,
                seq=self._next_seq(),
                memo=memo,
                cleared=cleared,
                approved=approved,
                flag_color=flag_color,
                category_id=category_id,
                import_id=import_id,
                import_payee_name=payee_name if import_id else None,
                import_payee_name_original=payee_name if import_id else None,
                matched_transaction_id=matched_transaction_id,
                deleted=deleted,
            )
            if tx.id in budget.transactions:
                raise ValueError(f"transaction id {tx.id!r} already exists")
            self._set_payee_name(budget, tx, payee_name)
            if subtransactions is not None:
                subs = list(subtransactions)
                if sum(_check_int(s["amount"], "subtransaction amount") for s in subs) != amount:
                    raise ValueError("subtransaction amounts must sum to the transaction amount")
                for s in subs:
                    sub_cat = s.get("category_id")
                    if s.get("category_name") is not None and sub_cat is None:
                        sub_cat = self._category_by_name(budget, s["category_name"])
                    sub = _Sub(
                        id=self._new_id("sub"),
                        transaction_id=tx.id,
                        amount=s["amount"],
                        memo=s.get("memo"),
                        category_id=sub_cat,
                        deleted=deleted,
                    )
                    if s.get("payee_name") is not None:
                        sub.payee_id = self._payee_id_for(budget, s["payee_name"])
                        sub.payee_name = s["payee_name"]
                    tx.subtransactions.append(sub)
            self._register(budget, tx)
            changed = [tx]
            if transfer_account_id is not None:
                changed.append(self._link_transfer(budget, tx, transfer_account_id))
            self._touch(budget, changed)
            return tx.id

    def user_edit_transaction(self, tx_id: str, **fields: Any) -> dict[str, Any]:
        """Simulate an edit in the YNAB app (e.g. reconcile, memo edit, re-date).

        Accepts ``date, amount, memo, cleared, approved, flag_color, payee_name,
        category_id, category_name, account_id``. Bypasses API validation (any ``cleared``
        transition, dates on splits), keeps transfer pairs in sync and bumps knowledge.
        Changing a split's total is refused: the app requires its lines to be edited too.
        A deleted transaction cannot be edited (the app does not show it): ``ValueError``.
        """
        allowed = {
            "date",
            "amount",
            "memo",
            "cleared",
            "approved",
            "flag_color",
            "payee_name",
            "category_id",
            "category_name",
            "account_id",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unsupported fields: {sorted(unknown)}")
        with self._lock:
            budget, tx = self._locate(tx_id)
            if tx.deleted:
                raise ValueError(f"transaction {tx_id!r} is deleted; the app cannot edit it")
            # Validate everything before mutating: a partial edit would bypass _touch.
            if "cleared" in fields and fields["cleared"] not in CLEARED_VALUES:
                raise ValueError(f"invalid cleared {fields['cleared']!r}")
            if "account_id" in fields:
                if fields["account_id"] not in budget.accounts:
                    raise ValueError("unknown account")
                if tx.transfer_account_id is not None:
                    raise NotImplementedError("moving a transfer is not modelled")
            if "date" in fields:
                _check_date_str(fields["date"])
            if "amount" in fields:
                _check_int(fields["amount"], "amount")
                if tx.is_split and fields["amount"] != tx.amount:
                    raise ValueError("changing a split transaction's total is not modelled")

            changed = [tx]
            for key in (
                "account_id",
                "date",
                "amount",
                "memo",
                "cleared",
                "approved",
                "flag_color",
            ):
                if key in fields:
                    setattr(tx, key, fields[key])
            if "payee_name" in fields:
                self._set_payee_name(budget, tx, fields["payee_name"])
            if "category_name" in fields and "category_id" not in fields:
                name = fields["category_name"]
                tx.category_id = None if name is None else self._category_by_name(budget, name)
            elif "category_id" in fields:
                tx.category_id = fields["category_id"]
            partner = self._partner(budget, tx)
            if partner is not None:
                partner.amount = -tx.amount
                partner.date = tx.date
                changed.append(partner)
            self._touch(budget, changed)
            return self._render_tx(budget, tx)

    def user_delete_transaction(self, tx_id: str) -> dict[str, Any]:
        """Simulate the user deleting a transaction in the YNAB app (soft delete).

        Deleting an already deleted transaction is impossible in the app: ``ValueError``.
        """
        with self._lock:
            budget, tx = self._locate(tx_id)
            if tx.deleted:
                raise ValueError(f"transaction {tx_id!r} is already deleted")
            self._touch(budget, self._soft_delete(budget, tx))
            return self._render_tx(budget, tx)

    def edit_account(self, budget_id: str, account_id: str, **fields: Any) -> None:
        """Change ``name, type, on_budget, closed, deleted, note`` of an account."""
        allowed = {"name", "type", "on_budget", "closed", "deleted", "note"}
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unsupported fields: {sorted(unknown)}")
        with self._lock:
            budget = self._budget_or_value_error(budget_id)
            account = budget.accounts[account_id]
            for key, value in fields.items():
                setattr(account, key, value)
            if "name" in fields:
                budget.payees[account.transfer_payee_id] = f"Transfer : {account.name}"
            self._bump_account(budget, account)

    # ── inspection ──────────────────────────────────────────────────────────

    def tx(self, tx_id: str) -> dict[str, Any]:
        """The transaction as the API would render it (deleted ones included)."""
        with self._lock:
            budget, tx = self._locate(tx_id)
            return self._render_tx(budget, tx)

    def find_by_import_id(
        self, account_id: str, import_id: str, *, include_deleted: bool = True
    ) -> dict[str, Any] | None:
        """The (newest) transaction with ``import_id`` in ``account_id``, or None."""
        with self._lock:
            for budget in self._budgets.values():
                if account_id in budget.accounts:
                    tx = self._find_import_id(
                        budget, account_id, import_id, include_deleted=include_deleted
                    )
                    return None if tx is None else self._render_tx(budget, tx)
            return None

    def transactions(
        self,
        budget_id: str | None = None,
        *,
        account_id: str | None = None,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        """Rendered transactions sorted by (date, creation order)."""
        with self._lock:
            out = []
            for budget in self._budgets.values():
                if budget_id is not None and budget.id != budget_id:
                    continue
                for tx in sorted(budget.transactions.values(), key=lambda t: (t.date, t.seq)):
                    if account_id is not None and tx.account_id != account_id:
                        continue
                    if tx.deleted and not include_deleted:
                        continue
                    out.append(self._render_tx(budget, tx))
            return out

    def account(self, budget_id: str, account_id: str) -> dict[str, Any]:
        """The account as the API would render it (with computed balances)."""
        with self._lock:
            budget = self._budget_or_value_error(budget_id)
            return self._render_account(budget, budget.accounts[account_id])

    def balances(self, budget_id: str, account_id: str) -> tuple[int, int, int]:
        """``(balance, cleared_balance, uncleared_balance)`` in milliunits."""
        with self._lock:
            budget = self._budget_or_value_error(budget_id)
            return self._account_balances(budget, account_id)

    def server_knowledge(self, budget_id: str) -> int:
        with self._lock:
            return self._budget_or_value_error(budget_id).knowledge

    def check_invariants(self) -> None:
        """Assert the fake's internal consistency (incremental balances, indexes, links).

        Cheap enough to call at the end of any test that drives many operations.
        """
        with self._lock:
            for budget in self._budgets.values():
                full = self._recompute_balances(budget)
                for account_id in set(full) | set(budget.totals):
                    got = self._account_balances(budget, account_id)
                    want = full.get(account_id, (0, 0, 0))
                    assert got == want, f"balance drift in {account_id}: {got} != {want}"
                indexed = {id(t) for ts in budget.by_import_id.values() for t in ts}
                expected = {id(t) for t in budget.transactions.values() if t.import_id}
                assert indexed == expected, "import_id index out of sync"
                for tx in budget.transactions.values():
                    partner = self._partner(budget, tx)
                    if tx.transfer_transaction_id is not None:
                        assert partner is not None, f"dangling transfer on {tx.id}"
                        assert partner.transfer_transaction_id == tx.id
                        assert partner.amount == -tx.amount and partner.date == tx.date
                        assert partner.deleted == tx.deleted
                    if tx.is_split:
                        live = sum(s.amount for s in tx.subtransactions if not s.deleted)
                        assert live == tx.amount, f"split sum mismatch on {tx.id}"

    def snapshot(self, *, include_deleted: bool = False) -> list[dict[str, Any]]:
        """Canonical, id-free view of every transaction, for convergence assertions.

        Two runs that reach the same YNAB state produce equal snapshots even if they
        created transactions in a different order (ids are excluded; import_ids are kept).
        """
        with self._lock:
            rows = []
            for budget in self._budgets.values():
                for tx in budget.transactions.values():
                    if tx.deleted and not include_deleted:
                        continue
                    rows.append(
                        {
                            "budget_id": budget.id,
                            "account_id": tx.account_id,
                            "date": tx.date,
                            "amount": tx.amount,
                            "payee_name": tx.payee_name,
                            "memo": tx.memo,
                            "cleared": tx.cleared,
                            "approved": tx.approved,
                            "flag_color": tx.flag_color,
                            "category_name": self._category_name(budget, tx),
                            "import_id": tx.import_id,
                            "transfer_account_id": tx.transfer_account_id,
                            "deleted": tx.deleted,
                            "subtransactions": sorted(
                                (
                                    {
                                        "amount": s.amount,
                                        "memo": s.memo,
                                        "payee_name": s.payee_name,
                                        "category_name": budget.categories.get(s.category_id)
                                        if s.category_id
                                        else None,
                                        "deleted": s.deleted,
                                    }
                                    for s in tx.subtransactions
                                    if include_deleted or not s.deleted
                                ),
                                key=lambda s: json.dumps(s, sort_keys=True),
                            ),
                        }
                    )
            rows.sort(key=lambda r: json.dumps(r, sort_keys=True))
            return rows

    @property
    def calls(self) -> list[Call]:
        """Request log as ``(method, path, query, body)`` tuples (failed requests included)."""
        with self._lock:
            return [Call(r.method, r.path, r.query, r.body) for r in self.requests]

    def calls_matching(
        self, method: str | None = None, path_regex: str | None = None
    ) -> list[Call]:
        """Subset of :attr:`calls` (``path_regex`` must match the whole path)."""
        pattern = re.compile(path_regex) if path_regex is not None else None
        return [
            c
            for c in self.calls
            if (method is None or c.method == method)
            and (pattern is None or pattern.fullmatch(c.path))
        ]

    def reset_log(self) -> None:
        """Clear the request log and the request/mutating counters (state is kept).

        Pending :meth:`fail_at_call` faults keep their relative position.
        """
        with self._lock:
            for fault in self._faults:
                if fault.at_mutating_call is not None:
                    fault.at_mutating_call -= self.mutating_calls
            self.requests.clear()
            self.request_count = 0
            self.mutating_calls = 0
            self.slept.clear()

    # ── fault injection ─────────────────────────────────────────────────────

    def fail_next(
        self,
        method: str | None,
        path_regex: str | None,
        status: int,
        times: int = 1,
        retry_after: float | None = None,
        body: Any = None,
        *,
        after_apply: bool = False,
    ) -> None:
        """Answer the next ``times`` matching requests with ``status``.

        ``method`` None/``"*"`` matches any method; ``path_regex`` (None = any) must match
        the whole path as logged in :attr:`calls`, e.g. ``r"/budgets/[^/]+/transactions"``.
        ``body`` (dict or bytes) replaces the default YNAB error body. With ``after_apply``
        the request is processed first and only its response is replaced.
        """
        with self._lock:
            self._faults.append(
                _Fault(
                    method=None if method in (None, "*") else method.upper(),
                    path_re=None if path_regex is None else re.compile(path_regex),
                    remaining=times,
                    status=status,
                    retry_after=retry_after,
                    body=body,
                    after_apply=after_apply,
                )
            )

    def fail_at_call(
        self,
        n: int,
        status: int = 500,
        *,
        retry_after: float | None = None,
        body: Any = None,
        after_apply: bool = False,
    ) -> None:
        """Fail the ``n``-th mutating request (POST/PUT/PATCH/DELETE) counted from now.

        Every mutating request the transport receives counts, including failed ones and
        client retries (a retried 5xx is call ``n + 1``). Only one ``*_at_call`` fault can
        target a given call (a second one would never fire): ``ValueError``.
        """
        if n < 1:
            raise ValueError("n is 1-based")
        with self._lock:
            self._check_call_free(self.mutating_calls + n)
            self._faults.append(
                _Fault(
                    method=None,
                    path_re=None,
                    remaining=1,
                    status=status,
                    retry_after=retry_after,
                    body=body,
                    after_apply=after_apply,
                    at_mutating_call=self.mutating_calls + n,
                )
            )

    def raise_network_error_next(
        self,
        method: str | None = None,
        path_regex: str | None = None,
        times: int = 1,
        *,
        exc: BaseException | None = None,
        after_apply: bool = False,
    ) -> None:
        """Make the transport raise (default ``TimeoutError``) for the next matching requests."""
        with self._lock:
            self._faults.append(
                _Fault(
                    method=None if method in (None, "*") else method.upper(),
                    path_re=None if path_regex is None else re.compile(path_regex),
                    remaining=times,
                    status=None,
                    after_apply=after_apply,
                    exc=exc,
                )
            )

    def raise_network_error_at_call(
        self, n: int, *, exc: BaseException | None = None, after_apply: bool = False
    ) -> None:
        """Like :meth:`fail_at_call` but the transport raises a network error."""
        if n < 1:
            raise ValueError("n is 1-based")
        with self._lock:
            self._check_call_free(self.mutating_calls + n)
            self._faults.append(
                _Fault(
                    method=None,
                    path_re=None,
                    remaining=1,
                    status=None,
                    after_apply=after_apply,
                    exc=exc,
                    at_mutating_call=self.mutating_calls + n,
                )
            )

    def _check_call_free(self, call_number: int) -> None:
        if any(f.at_mutating_call == call_number for f in self._faults):
            raise ValueError(f"a fault is already registered for mutating call {call_number}")

    def clear_faults(self) -> None:
        """Drop every pending injected fault."""
        with self._lock:
            self._faults.clear()

    @property
    def pending_faults(self) -> int:
        """Number of injected faults that have not fired yet."""
        with self._lock:
            return sum(f.remaining for f in self._faults)

    # ── client adapters ─────────────────────────────────────────────────────

    def client(self, **kwargs: Any) -> YnabClient:
        """A real :class:`YnabClient` wired to this fake.

        Defaults: the fake's token and base URL, ``sleep`` recording into :attr:`slept`
        (no real waiting) and a constant jitter so backoff delays are deterministic.
        """
        token = kwargs.pop("token", self.token or FAKE_TOKEN)
        kwargs.setdefault("base_url", self.base_url)
        kwargs.setdefault("transport", self.transport)
        kwargs.setdefault("sleep", self.slept.append)
        kwargs.setdefault("jitter", lambda: 1.0)
        return YnabClient(token, **kwargs)

    def v1_request(self, method: str, path: str, token: str, body: Any = None) -> Any:
        """Drop-in for v1's ``ynab_request(method, path, token, body=None)``.

        Reproduces v1's contract *including its gaps*, so oracle tests see v1 exactly as it
        behaves in production:

        * 2xx: the decoded JSON (a non-JSON body raises ``JSONDecodeError`` raw).
        * Other statuses: prints ``  ✗ YNAB API error (<status>): <body>`` and raises
          ``RuntimeError("YNAB API <status>: <body[:200]>")``.
        * ``urllib.error.URLError`` (what ``urlopen`` raises for connect-time failures):
          prints ``  ✗ YNAB network error: <err>`` and raises
          ``RuntimeError("YNAB network error: <err>")``.
        * Any other transport exception (the fake's default ``TimeoutError``, a reset, a
          ``RemoteDisconnected``) escapes unchanged, as a read timeout did in v1. Inject
          ``exc=urllib.error.URLError(...)`` to simulate a failure v1 did catch.

        v1's callers scraped these stdout lines (findings F15/F47), hence the prints.
        """
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        data = json.dumps(body).encode() if body else None
        try:
            status, _headers, raw = self.transport(method, self.base_url + path, headers, data, 20)
        except urllib.error.URLError as exc:
            print(f"  ✗ YNAB network error: {exc}")
            raise RuntimeError(f"YNAB network error: {exc}") from exc
        if not 200 <= status < 300:
            try:
                error_body = raw.decode()
            except UnicodeDecodeError:
                error_body = ""
            print(f"  ✗ YNAB API error ({status}): {error_body}")
            raise RuntimeError(f"YNAB API {status}: {error_body[:200]}")
        return json.loads(raw.decode())

    # ── transport ───────────────────────────────────────────────────────────

    def transport(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> tuple[int, dict[str, str], bytes]:
        """The :data:`ynab_helper.ynab.client.Transport` implementation."""
        with self._lock:
            method = method.upper()
            path, query = self._split_url(url)
            hdrs = {str(k).lower(): str(v) for k, v in headers.items()}
            decoded, body_error = _decode_body(body)
            record = RecordedRequest(method, path, query, decoded, hdrs, timeout, url)
            self.requests.append(record)
            self.request_count += 1
            is_mutating = method in MUTATING_METHODS
            if is_mutating:
                self.mutating_calls += 1

            fault = self._take_fault(method, path, is_mutating)
            if fault is not None and not fault.after_apply:
                return self._fault_response(fault, record)

            status, payload = self._handle(method, path, query, hdrs, decoded, body_error)
            if fault is not None:
                return self._fault_response(fault, record)
            record.status = status
            return status, self._response_headers(), json.dumps(payload).encode("utf-8")

    def _split_url(self, url: str) -> tuple[str, dict[str, str]]:
        parts = urllib.parse.urlsplit(url)
        base = urllib.parse.urlsplit(self.base_url)
        path = parts.path
        if (parts.scheme, parts.netloc) == (base.scheme, base.netloc) and path.startswith(
            base.path
        ):
            path = path[len(base.path) :] or "/"
        else:
            path = "<foreign>" + path
        query = {k: v[0] for k, v in urllib.parse.parse_qs(parts.query).items()}
        return path, query

    def _response_headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "X-Rate-Limit": f"{self.request_count}/{self.rate_limit}",
        }
        if extra:
            headers.update(extra)
        return headers

    def _take_fault(self, method: str, path: str, is_mutating: bool) -> _Fault | None:
        ordered = sorted(self._faults, key=lambda f: f.at_mutating_call is None)
        for fault in ordered:
            if fault.matches(method, path, self.mutating_calls, is_mutating):
                fault.remaining -= 1
                if fault.remaining <= 0:
                    self._faults.remove(fault)
                return fault
        return None

    def _fault_response(
        self, fault: _Fault, record: RecordedRequest
    ) -> tuple[int, dict[str, str], bytes]:
        if fault.status is None:
            record.fault = "network"
            raise fault.exc if fault.exc is not None else TimeoutError("timed out (injected)")
        record.status = fault.status
        record.fault = f"status {fault.status}"
        extra = {}
        if fault.retry_after is not None:
            extra["Retry-After"] = (
                str(int(fault.retry_after))
                if float(fault.retry_after).is_integer()
                else str(fault.retry_after)
            )
        if isinstance(fault.body, bytes):
            raw = fault.body
        else:
            raw = json.dumps(fault.body if fault.body is not None else _error_body(fault.status))
            raw = raw.encode("utf-8")
        return fault.status, self._response_headers(extra), raw

    def _handle(
        self,
        method: str,
        path: str,
        query: dict[str, str],
        headers: dict[str, str],
        body: Any,
        body_error: str | None,
    ) -> tuple[int, Any]:
        if self.token is not None and headers.get("authorization") != f"Bearer {self.token}":
            return 401, _error_body(401)
        if self.quota is not None and self.request_count > self.quota:
            return 429, _error_body(429)
        path_matched = False
        for route_method, pattern, handler in _ROUTES:
            match = pattern.fullmatch(path)
            if match is None:
                continue
            path_matched = True
            if route_method != method:
                continue
            params = {k: urllib.parse.unquote(v) for k, v in match.groupdict().items()}
            try:
                if body_error is not None and method in MUTATING_METHODS:
                    raise _bad_request(body_error)
                return getattr(self, handler)(query=query, body=body, **params)
            except _ApiError as err:
                return err.status, err.body
        if path_matched:
            return 405, {
                "error": {"id": "405", "name": "method_not_allowed", "detail": "Method not allowed"}
            }
        return 404, {
            "error": {
                "id": "404.1",
                "name": "not_found",
                "detail": "The specified URI does not exist.",
            }
        }

    # ── read endpoints ──────────────────────────────────────────────────────

    def _get_user(self, *, query: dict[str, str], body: Any) -> tuple[int, Any]:
        return 200, {"data": {"user": {"id": self.user_id}}}

    def _get_budgets(self, *, query: dict[str, str], body: Any) -> tuple[int, Any]:
        include_accounts = query.get("include_accounts", "").lower() == "true"
        budgets = []
        for budget in self._budgets.values():
            summary = self._render_budget(budget)
            if include_accounts:
                summary["accounts"] = [
                    self._render_account(budget, a)
                    for a in budget.accounts.values()
                    if not a.deleted
                ]
            budgets.append(summary)
        return 200, {"data": {"budgets": budgets, "default_budget": None}}

    def _get_settings(self, *, query: dict[str, str], body: Any, b: str) -> tuple[int, Any]:
        budget = self._budget(b)
        settings = {
            "date_format": {"format": "DD.MM.YYYY"},
            "currency_format": _currency_format(budget.currency),
        }
        return 200, {"data": {"settings": settings}}

    def _get_accounts(self, *, query: dict[str, str], body: Any, b: str) -> tuple[int, Any]:
        budget = self._budget(b)
        last_knowledge = _parse_knowledge(query)
        accounts = [
            self._render_account(budget, account)
            for account in budget.accounts.values()
            if (
                account.knowledge > last_knowledge
                if last_knowledge is not None
                else not account.deleted
            )
        ]
        return 200, {"data": {"accounts": accounts, "server_knowledge": budget.knowledge}}

    def _get_account(self, *, query: dict[str, str], body: Any, b: str, a: str) -> tuple[int, Any]:
        budget = self._budget(b)
        account = budget.accounts.get(a)
        if account is None:
            raise _not_found()
        return 200, {"data": {"account": self._render_account(budget, account)}}

    def _get_transactions(
        self, *, query: dict[str, str], body: Any, b: str, a: str | None = None
    ) -> tuple[int, Any]:
        budget = self._budget(b)
        if a is not None and a not in budget.accounts:
            raise _not_found()
        since = query.get("since_date")
        if since is not None and not _valid_date(since):
            raise _bad_request("since_date is invalid")
        last_knowledge = _parse_knowledge(query)
        tx_type = query.get("type")
        if tx_type not in (None, "uncategorized", "unapproved"):
            raise _bad_request("type is invalid")
        out = []
        for tx in sorted(budget.transactions.values(), key=lambda t: (t.date, t.seq)):
            if a is not None and tx.account_id != a:
                continue
            if since is not None and tx.date < since:
                continue
            if last_knowledge is not None:
                if tx.knowledge <= last_knowledge:
                    continue
            elif tx.deleted:
                continue
            if tx_type == "unapproved" and tx.approved:
                continue
            if tx_type == "uncategorized" and (
                tx.category_id is not None or tx.is_split or tx.transfer_account_id is not None
            ):
                continue
            out.append(self._render_tx(budget, tx))
        return 200, {"data": {"transactions": out, "server_knowledge": budget.knowledge}}

    def _get_transaction(
        self, *, query: dict[str, str], body: Any, b: str, t: str
    ) -> tuple[int, Any]:
        budget = self._budget(b)
        tx = budget.transactions.get(t)
        if tx is None:
            raise _not_found()
        return 200, {
            "data": {
                "transaction": self._render_tx(budget, tx),
                "server_knowledge": budget.knowledge,
            }
        }

    # ── write endpoints ─────────────────────────────────────────────────────

    def _post_transactions(self, *, query: dict[str, str], body: Any, b: str) -> tuple[int, Any]:
        budget = self._budget(b)
        if not isinstance(body, dict):
            raise _bad_request("request body must be an object")
        has_single, has_bulk = "transaction" in body, "transactions" in body
        if has_single == has_bulk:
            raise _bad_request("specify exactly one of 'transaction' or 'transactions'")
        items = [body["transaction"]] if has_single else body["transactions"]
        if not isinstance(items, list) or not items:
            raise _bad_request("transactions must be a non-empty list")

        specs = [self._validate_new(budget, item) for item in items]  # validate all first
        to_create: list[dict[str, Any]] = []
        duplicates: list[str] = []
        claimed: set[tuple[str, str]] = set()
        for spec in specs:
            import_id = spec["import_id"]
            if import_id is not None:
                key = (spec["account_id"], import_id)
                taken = key in claimed or self._find_import_id(
                    budget,
                    spec["account_id"],
                    import_id,
                    include_deleted=self.dedupe_includes_deleted,
                )
                if taken:
                    duplicates.append(import_id)
                    continue
                claimed.add(key)
            to_create.append(spec)
        if has_single and duplicates and self.single_duplicate_conflict:
            raise _ApiError(
                409, "409", "conflict", "A transaction with the same import_id already exists"
            )

        created: list[_Tx] = []
        changed: list[_Tx] = []
        for spec in to_create:
            tx, partner = self._insert(budget, spec)
            created.append(tx)
            changed.append(tx)
            if partner is not None:
                changed.append(partner)
        if changed:
            self._touch(budget, changed)
        data: dict[str, Any] = {
            "transaction_ids": [tx.id for tx in created],
            "duplicate_import_ids": duplicates,
            "server_knowledge": budget.knowledge,
        }
        rendered = [self._render_tx(budget, tx) for tx in created]
        if has_single:
            if rendered:
                data["transaction"] = rendered[0]
        else:
            data["transactions"] = rendered
        return 201, {"data": data}

    def _patch_transactions(self, *, query: dict[str, str], body: Any, b: str) -> tuple[int, Any]:
        budget = self._budget(b)
        items = body.get("transactions") if isinstance(body, dict) else None
        if not isinstance(items, list) or not items:
            raise _bad_request("transactions must be a non-empty list")
        plans: list[tuple[_Tx, dict[str, Any]]] = []
        seen: set[str] = set()
        for item in items:
            if not isinstance(item, dict):
                raise _bad_request("each transaction must be an object")
            tx = self._resolve_update_target(budget, item)
            if tx.id in seen:
                raise _bad_request(f"transaction {tx.id} appears more than once")
            seen.add(tx.id)
            plans.append((tx, self._validate_update(budget, tx, item)))
        changed: list[_Tx] = []
        for tx, changes in plans:
            changed.extend(self._apply_update(budget, tx, changes))
        if changed:
            self._touch(budget, changed)
        txs = [tx for tx, _ in plans]
        return 209, {
            "data": {
                "transaction_ids": [tx.id for tx in txs],
                "transactions": [self._render_tx(budget, tx) for tx in txs],
                "duplicate_import_ids": [],
                "server_knowledge": budget.knowledge,
            }
        }

    def _put_transaction(
        self, *, query: dict[str, str], body: Any, b: str, t: str
    ) -> tuple[int, Any]:
        budget = self._budget(b)
        tx = budget.transactions.get(t)
        if tx is None or tx.deleted:
            raise _not_found()
        item = body.get("transaction") if isinstance(body, dict) else None
        if not isinstance(item, dict):
            raise _bad_request("request body must contain a 'transaction' object")
        changes = self._validate_update(budget, tx, item)
        changed = self._apply_update(budget, tx, changes)
        if changed:
            self._touch(budget, changed)
        return 200, {
            "data": {
                "transaction": self._render_tx(budget, tx),
                "server_knowledge": budget.knowledge,
            }
        }

    def _delete_transaction(
        self, *, query: dict[str, str], body: Any, b: str, t: str
    ) -> tuple[int, Any]:
        budget = self._budget(b)
        tx = budget.transactions.get(t)
        if tx is None or tx.deleted:
            raise _not_found()
        self._touch(budget, self._soft_delete(budget, tx))
        return 200, {
            "data": {
                "transaction": self._render_tx(budget, tx),
                "server_knowledge": budget.knowledge,
            }
        }

    # ── validation ──────────────────────────────────────────────────────────

    def _validate_new(self, budget: _Budget, item: Any) -> dict[str, Any]:
        if not isinstance(item, dict):
            raise _bad_request("each transaction must be an object")
        _reject_unknown(item, _TX_FIELDS)
        account_id = item.get("account_id")
        account = budget.accounts.get(account_id) if isinstance(account_id, str) else None
        if account is None or account.deleted:
            raise _bad_request("account_id does not exist")
        if "date" not in item or "amount" not in item:
            raise _bad_request("date and amount are required")
        spec: dict[str, Any] = {
            "account_id": account_id,
            "date": self._validate_date(item["date"]),
            "amount": _api_int(item["amount"], "amount"),
            "cleared": _api_cleared(item.get("cleared", "uncleared")),
            "approved": _api_bool(item.get("approved", False), "approved"),
            "flag_color": _api_flag(item.get("flag_color")),
            "memo": _api_text(item.get("memo"), "memo", MAX_MEMO),
            "payee_name": _api_text(item.get("payee_name"), "payee_name", MAX_PAYEE_NAME),
            "import_id": _api_text(item.get("import_id"), "import_id", MAX_IMPORT_ID),
            "category_id": self._api_category(budget, item.get("category_id")),
            "payee_id": None,
            "transfer_account_id": None,
            "subtransactions": None,
        }
        payee_id = item.get("payee_id")
        if payee_id is not None:
            transfer_to = self._transfer_target(budget, payee_id)
            if transfer_to is not None:
                if transfer_to == account_id:
                    raise _bad_request("a transfer cannot target its own account")
                if budget.accounts[transfer_to].deleted:
                    raise _bad_request("the transfer account does not exist")
                spec["transfer_account_id"] = transfer_to
            elif payee_id not in budget.payees:
                raise _bad_request("payee_id does not exist")
            else:
                spec["payee_id"] = payee_id
        if item.get("subtransactions") is not None:
            if spec["transfer_account_id"] is not None:
                raise NotImplementedError("split transfers are not modelled by FakeYnab")
            spec["subtransactions"] = self._validate_subs(
                budget, item["subtransactions"], spec["amount"]
            )
        return spec

    def _validate_subs(self, budget: _Budget, subs: Any, amount: int) -> list[dict[str, Any]]:
        if not isinstance(subs, list) or not subs:
            raise _bad_request("subtransactions must be a non-empty list")
        out = []
        for sub in subs:
            if not isinstance(sub, dict):
                raise _bad_request("each subtransaction must be an object")
            _reject_unknown(sub, _SUB_FIELDS)
            if sub.get("payee_id") is not None and self._transfer_target(budget, sub["payee_id"]):
                raise NotImplementedError("transfer subtransactions are not modelled by FakeYnab")
            out.append(
                {
                    "amount": _api_int(sub.get("amount"), "subtransaction amount"),
                    "memo": _api_text(sub.get("memo"), "memo", MAX_MEMO),
                    "payee_name": _api_text(sub.get("payee_name"), "payee_name", MAX_PAYEE_NAME),
                    "category_id": self._api_category(budget, sub.get("category_id")),
                }
            )
        if sum(s["amount"] for s in out) != amount:
            raise _bad_request("subtransaction amounts must add up to the transaction amount")
        return out

    def _resolve_update_target(self, budget: _Budget, item: dict[str, Any]) -> _Tx:
        tx_id = item.get("id")
        if tx_id is not None:
            tx = budget.transactions.get(tx_id) if isinstance(tx_id, str) else None
            if tx is None or tx.deleted:
                raise _not_found(f"transaction {tx_id} not found")
            return tx
        import_id = item.get("import_id")
        if not isinstance(import_id, str) or not import_id:
            raise _bad_request("each transaction needs an id or an import_id")
        account_id = item.get("account_id")
        found = [
            tx
            for tx in budget.by_import_id.get(import_id, ())
            if not tx.deleted and (account_id is None or tx.account_id == account_id)
        ]
        if not found:
            raise _not_found(f"no transaction with import_id {import_id}")
        if len(found) > 1:
            raise _bad_request(f"import_id {import_id} is ambiguous; add account_id")
        return found[0]

    def _validate_update(self, budget: _Budget, tx: _Tx, item: dict[str, Any]) -> dict[str, Any]:
        _reject_unknown(item, _UPDATE_FIELDS)
        changes: dict[str, Any] = {}
        if "import_id" in item and item["import_id"] != tx.import_id:
            # Immutable: YNAB keeps the stored import_id. Recorded for assertions.
            self.import_id_change_attempts.append((tx.id, tx.import_id, item["import_id"]))
        if "account_id" in item and item["account_id"] != tx.account_id:
            account = budget.accounts.get(item["account_id"])
            if account is None or account.deleted:
                raise _bad_request("account_id does not exist")
            if tx.transfer_account_id is not None:
                raise NotImplementedError("moving a transfer is not modelled by FakeYnab")
            changes["account_id"] = item["account_id"]
        if "date" in item:
            new_date = self._validate_date(item["date"])
            if not tx.is_split:  # YNAB ignores date changes on split transactions
                changes["date"] = new_date
        if "amount" in item:
            amount = _api_int(item["amount"], "amount")
            if amount != tx.amount and tx.is_split and "subtransactions" not in item:
                raise _bad_request("the amount of a split transaction cannot be changed")
            changes["amount"] = amount
        if "cleared" in item:
            changes["cleared"] = _api_cleared(item["cleared"])
        if "approved" in item:
            changes["approved"] = _api_bool(item["approved"], "approved")
        if "flag_color" in item:
            changes["flag_color"] = _api_flag(item["flag_color"])
        if "memo" in item:
            changes["memo"] = _api_text(item["memo"], "memo", MAX_MEMO)
        if "category_id" in item:
            changes["category_id"] = self._api_category(budget, item["category_id"])
        payee_id = item.get("payee_id")
        if payee_id is not None and payee_id != tx.payee_id:
            if self._transfer_target(budget, payee_id) is not None or tx.transfer_account_id:
                raise NotImplementedError("re-pointing transfers is not modelled by FakeYnab")
            if payee_id not in budget.payees:
                raise _bad_request("payee_id does not exist")
            changes["payee_id"] = payee_id
        elif "payee_name" in item and payee_id is None:
            name = _api_text(item["payee_name"], "payee_name", MAX_PAYEE_NAME)
            if name != tx.payee_name:
                if tx.transfer_account_id is not None:
                    raise NotImplementedError("renaming a transfer payee is not modelled")
                changes["payee_name"] = name
        if item.get("subtransactions") is not None:
            if tx.is_split:
                raise _bad_request(
                    "updating subtransactions on an existing split transaction is not supported"
                )
            if tx.transfer_account_id is not None:
                raise NotImplementedError("split transfers are not modelled by FakeYnab")
            changes["subtransactions"] = self._validate_subs(
                budget, item["subtransactions"], changes.get("amount", tx.amount)
            )
        return changes

    def _validate_date(self, value: Any) -> str:
        if not isinstance(value, str) or not _valid_date(value):
            raise _bad_request("date must be a valid ISO date (YYYY-MM-DD)")
        if self.today is not None and _date.fromisoformat(value) > self.today:
            raise _bad_request("date must not be in the future")
        return value

    def _api_category(self, budget: _Budget, category_id: Any) -> str | None:
        if category_id is None:
            return None
        if not isinstance(category_id, str) or category_id not in budget.categories:
            raise _bad_request("category_id does not exist")
        return category_id

    # ── state changes ───────────────────────────────────────────────────────

    def _insert(self, budget: _Budget, spec: dict[str, Any]) -> tuple[_Tx, _Tx | None]:
        tx = _Tx(
            id=self._new_id("tx"),
            budget_id=budget.id,
            account_id=spec["account_id"],
            date=spec["date"],
            amount=spec["amount"],
            seq=self._next_seq(),
            memo=spec["memo"],
            cleared=spec["cleared"],
            approved=spec["approved"],
            flag_color=spec["flag_color"],
            category_id=spec["category_id"],
            import_id=spec["import_id"],
        )
        if spec["payee_id"] is not None:
            tx.payee_id = spec["payee_id"]
            tx.payee_name = budget.payees[spec["payee_id"]]
        else:
            self._set_payee_name(budget, tx, spec["payee_name"])
        if tx.import_id is not None:
            tx.import_payee_name = tx.payee_name
            tx.import_payee_name_original = spec["payee_name"]
        for sub in spec["subtransactions"] or []:
            tx.subtransactions.append(self._new_sub(budget, tx.id, sub))
        self._register(budget, tx)
        partner = None
        if spec["transfer_account_id"] is not None:
            partner = self._link_transfer(budget, tx, spec["transfer_account_id"])
        return tx, partner

    def _new_sub(self, budget: _Budget, tx_id: str, sub: dict[str, Any]) -> _Sub:
        new = _Sub(
            id=self._new_id("sub"),
            transaction_id=tx_id,
            amount=sub["amount"],
            memo=sub["memo"],
            category_id=sub["category_id"],
        )
        if sub["payee_name"] is not None:
            new.payee_id = self._payee_id_for(budget, sub["payee_name"])
            new.payee_name = sub["payee_name"]
        return new

    def _apply_update(self, budget: _Budget, tx: _Tx, changes: dict[str, Any]) -> list[_Tx]:
        """Apply validated changes; returns the transactions that actually changed."""
        before = self._render_tx(budget, tx)
        for key in ("account_id", "date", "amount", "cleared", "approved", "flag_color", "memo"):
            if key in changes:
                setattr(tx, key, changes[key])
        if "category_id" in changes:
            tx.category_id = changes["category_id"]
        if "payee_id" in changes:
            tx.payee_id = changes["payee_id"]
            tx.payee_name = budget.payees[changes["payee_id"]]
        elif "payee_name" in changes:
            self._set_payee_name(budget, tx, changes["payee_name"])
        if "subtransactions" in changes:
            tx.subtransactions = [
                self._new_sub(budget, tx.id, s) for s in changes["subtransactions"]
            ]
        changed = [tx] if self._render_tx(budget, tx) != before else []
        partner = self._partner(budget, tx)
        if partner is not None and (partner.amount != -tx.amount or partner.date != tx.date):
            partner.amount = -tx.amount
            partner.date = tx.date
            changed.append(partner)
        return changed

    def _soft_delete(self, budget: _Budget, tx: _Tx) -> list[_Tx]:
        changed = []
        for item in (tx, self._partner(budget, tx)):
            if item is None or item.deleted:
                continue
            item.deleted = True
            for sub in item.subtransactions:
                sub.deleted = True
            changed.append(item)
        return changed

    def _link_transfer(self, budget: _Budget, tx: _Tx, target_account_id: str) -> _Tx:
        target = budget.accounts.get(target_account_id)
        if target is None:
            raise ValueError(f"unknown transfer account {target_account_id!r}")
        source = budget.accounts[tx.account_id]
        partner = _Tx(
            id=self._new_id("tx"),
            budget_id=budget.id,
            account_id=target.id,
            date=tx.date,
            amount=-tx.amount,
            seq=self._next_seq(),
            memo=tx.memo,
            cleared="uncleared",
            approved=tx.approved,
            payee_id=source.transfer_payee_id,
            payee_name=budget.payees[source.transfer_payee_id],
            transfer_account_id=source.id,
            transfer_transaction_id=tx.id,
            deleted=tx.deleted,
        )
        tx.payee_id = target.transfer_payee_id
        tx.payee_name = budget.payees[target.transfer_payee_id]
        tx.transfer_account_id = target.id
        tx.transfer_transaction_id = partner.id
        tx.category_id = None
        self._register(budget, partner)
        return partner

    def _register(self, budget: _Budget, tx: _Tx) -> None:
        budget.transactions[tx.id] = tx
        if tx.import_id is not None:
            budget.by_import_id.setdefault(tx.import_id, []).append(tx)

    def _touch(self, budget: _Budget, txs: Iterable[_Tx]) -> None:
        """Bump the budget's knowledge once and stamp the changed transactions.

        Every state change must pass *all* transactions it modified (transfer partners
        included) through here: account totals are maintained incrementally from each
        transaction's previous contribution, and accounts whose balances changed are
        stamped too so they show up in accounts deltas.
        """
        budget.knowledge += 1
        before: dict[str, tuple[int, ...]] = {}
        for tx in txs:
            tx.knowledge = budget.knowledge
            new = None if tx.deleted else (tx.account_id, tx.amount, _counts_cleared(tx.cleared))
            if new == tx.contrib:
                continue
            for sign, contrib in ((-1, tx.contrib), (1, new)):
                if contrib is None:
                    continue
                account_id, amount, is_cleared = contrib
                row = budget.totals.setdefault(account_id, [0, 0, 0])
                before.setdefault(account_id, tuple(row))
                row[0] += sign * amount
                row[1 if is_cleared else 2] += sign * amount
            tx.contrib = new
        for account_id, old in before.items():
            account = budget.accounts.get(account_id)
            if account is not None and tuple(budget.totals[account_id]) != old:
                account.knowledge = budget.knowledge

    def _bump_account(self, budget: _Budget, account: _Account) -> None:
        budget.knowledge += 1
        account.knowledge = budget.knowledge

    # ── lookups / rendering ─────────────────────────────────────────────────

    def _budget(self, budget_id: str) -> _Budget:
        if budget_id == "last-used" and self._budgets:
            return max(self._budgets.values(), key=lambda b: b.seq)
        budget = self._budgets.get(budget_id)
        if budget is None:
            raise _not_found()
        return budget

    def _budget_or_value_error(self, budget_id: str) -> _Budget:
        budget = self._budgets.get(budget_id)
        if budget is None:
            raise ValueError(f"unknown budget {budget_id!r}")
        return budget

    def _locate(self, tx_id: str) -> tuple[_Budget, _Tx]:
        for budget in self._budgets.values():
            tx = budget.transactions.get(tx_id)
            if tx is not None:
                return budget, tx
        raise KeyError(tx_id)

    def _partner(self, budget: _Budget, tx: _Tx) -> _Tx | None:
        if tx.transfer_transaction_id is None:
            return None
        return budget.transactions.get(tx.transfer_transaction_id)

    def _find_import_id(
        self, budget: _Budget, account_id: str, import_id: str, *, include_deleted: bool
    ) -> _Tx | None:
        candidates = [
            tx
            for tx in budget.by_import_id.get(import_id, ())
            if tx.account_id == account_id and (include_deleted or not tx.deleted)
        ]
        # Prefer a live transaction, then the newest one.
        return max(candidates, key=lambda t: (not t.deleted, t.seq), default=None)

    def _transfer_target(self, budget: _Budget, payee_id: Any) -> str | None:
        for account in budget.accounts.values():
            if account.transfer_payee_id == payee_id:
                return account.id
        return None

    def _payee_id_for(self, budget: _Budget, name: str) -> str:
        payee_id = budget.payee_by_name.get(name)
        if payee_id is None:
            payee_id = self._new_id("payee")
            budget.payees[payee_id] = name
            budget.payee_by_name[name] = payee_id
        return payee_id

    def _set_payee_name(self, budget: _Budget, tx: _Tx, name: str | None) -> None:
        tx.payee_name = name
        tx.payee_id = None if name is None else self._payee_id_for(budget, name)

    def _category_by_name(self, budget: _Budget, name: str) -> str:
        for category_id, category_name in budget.categories.items():
            if category_name == name:
                return category_id
        category_id = self._new_id("category")
        budget.categories[category_id] = name
        return category_id

    def _category_name(self, budget: _Budget, tx: _Tx) -> str | None:
        if tx.category_id is not None:
            return budget.categories.get(tx.category_id)
        if tx.is_split:
            return SPLIT_CATEGORY_NAME
        return None

    @staticmethod
    def _account_balances(budget: _Budget, account_id: str) -> tuple[int, int, int]:
        row = budget.totals.get(account_id, (0, 0, 0))
        return row[0], row[1], row[2]

    @staticmethod
    def _recompute_balances(budget: _Budget) -> dict[str, tuple[int, int, int]]:
        """Reference implementation of the account totals (full scan)."""
        totals: dict[str, list[int]] = {}
        for tx in budget.transactions.values():
            if tx.deleted:
                continue
            row = totals.setdefault(tx.account_id, [0, 0, 0])
            row[0] += tx.amount
            row[1 if _counts_cleared(tx.cleared) else 2] += tx.amount
        return {k: (v[0], v[1], v[2]) for k, v in totals.items()}

    def _render_budget(self, budget: _Budget) -> dict[str, Any]:
        return {
            "id": budget.id,
            "name": budget.name,
            "last_modified_on": "2026-10-16T00:00:00+00:00",
            "first_month": None,
            "last_month": None,
            "date_format": {"format": "DD.MM.YYYY"},
            "currency_format": _currency_format(budget.currency),
        }

    def _render_account(self, budget: _Budget, account: _Account) -> dict[str, Any]:
        balance, cleared, uncleared = self._account_balances(budget, account.id)
        return {
            "id": account.id,
            "name": account.name,
            "type": account.type,
            "on_budget": account.on_budget,
            "closed": account.closed,
            "note": account.note,
            "balance": balance,
            "cleared_balance": cleared,
            "uncleared_balance": uncleared,
            "transfer_payee_id": account.transfer_payee_id,
            "direct_import_linked": False,
            "direct_import_in_error": False,
            "last_reconciled_at": None,
            "debt_original_balance": None,
            "debt_interest_rates": {},
            "debt_minimum_payments": {},
            "debt_escrow_amounts": {},
            "deleted": account.deleted,
        }

    def _render_tx(self, budget: _Budget, tx: _Tx) -> dict[str, Any]:
        account = budget.accounts.get(tx.account_id)
        return {
            "id": tx.id,
            "date": tx.date,
            "amount": tx.amount,
            "memo": tx.memo,
            "cleared": tx.cleared,
            "approved": tx.approved,
            "flag_color": tx.flag_color,
            "flag_name": None,
            "account_id": tx.account_id,
            "account_name": account.name if account else None,
            "payee_id": tx.payee_id,
            "payee_name": tx.payee_name,
            "category_id": tx.category_id,
            "category_name": self._category_name(budget, tx),
            "transfer_account_id": tx.transfer_account_id,
            "transfer_transaction_id": tx.transfer_transaction_id,
            "matched_transaction_id": tx.matched_transaction_id,
            "import_id": tx.import_id,
            "import_payee_name": tx.import_payee_name,
            "import_payee_name_original": tx.import_payee_name_original,
            "debt_transaction_type": None,
            "deleted": tx.deleted,
            "subtransactions": [
                {
                    "id": s.id,
                    "transaction_id": s.transaction_id,
                    "amount": s.amount,
                    "memo": s.memo,
                    "payee_id": s.payee_id,
                    "payee_name": s.payee_name,
                    "category_id": s.category_id,
                    "category_name": budget.categories.get(s.category_id)
                    if s.category_id
                    else None,
                    "transfer_account_id": None,
                    "transfer_transaction_id": None,
                    "deleted": s.deleted,
                }
                for s in tx.subtransactions
            ],
        }

    def _new_id(self, kind: str) -> str:
        """Deterministic UUID-shaped ids (stable across runs of the same test)."""
        self._ids += 1
        prefix = {"tx": "7a", "sub": "5b", "payee": "9c", "category": "ca"}[kind]
        return f"{prefix}000000-0000-4000-8000-{self._ids:012d}"

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq


# ── module helpers ─────────────────────────────────────────────────────────


def _counts_cleared(cleared: str) -> bool:
    """YNAB's cleared_balance includes cleared and reconciled transactions."""
    return cleared in ("cleared", "reconciled")


def _decode_body(body: bytes | None) -> tuple[Any, str | None]:
    if not body:
        return None, None
    try:
        return json.loads(body.decode("utf-8")), None
    except (UnicodeDecodeError, ValueError):
        return None, "request body is not valid JSON"


def _currency_format(iso: str | None) -> dict[str, Any] | None:
    if iso is None:
        return None
    fmt = _CURRENCY_FORMATS.get(
        iso,
        {
            "example_format": "123,456.78",
            "decimal_separator": ".",
            "symbol_first": True,
            "group_separator": ",",
            "currency_symbol": iso,
        },
    )
    return {"iso_code": iso, "decimal_digits": 2, "display_symbol": True, **fmt}


def _valid_date(value: str) -> bool:
    if not _DATE_RE.fullmatch(value):
        return False
    try:
        _date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _parse_knowledge(query: Mapping[str, str]) -> int | None:
    raw = query.get("last_knowledge_of_server")
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError:
        raise _bad_request("last_knowledge_of_server is invalid") from None
    if value < 0:
        raise _bad_request("last_knowledge_of_server is invalid")
    return value


def _reject_unknown(item: Mapping[str, Any], allowed: frozenset[str]) -> None:
    unknown = sorted(set(item) - allowed)
    if unknown:
        raise _bad_request(f"unknown field(s): {', '.join(unknown)}")


def _api_int(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise _bad_request(f"{name} must be an integer (milliunits)")
    return value


def _api_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise _bad_request(f"{name} must be a boolean")
    return value


def _api_cleared(value: Any) -> str:
    if value not in CLEARED_VALUES:
        raise _bad_request("cleared must be one of cleared, uncleared, reconciled")
    return value


def _api_flag(value: Any) -> str | None:
    if value is not None and value not in FLAG_COLORS:
        raise _bad_request("flag_color is invalid")
    return value


def _api_text(value: Any, name: str, max_len: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise _bad_request(f"{name} must be a string")
    if len(value) > max_len:
        raise _bad_request(f"{name} is too long (maximum is {max_len} characters)")
    return value


def _check_int(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be integer milliunits")
    return value


def _check_date_str(value: Any) -> str:
    if not isinstance(value, str) or not _valid_date(value):
        raise ValueError(f"invalid date {value!r}")
    return value
