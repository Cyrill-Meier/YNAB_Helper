"""YNAB mirror: a local copy of the budget's accounts and transactions.

Written only by the YNAB → mirror sync (:mod:`ynab_helper.sync.ynab_sync`), which hands over
the JSON objects exactly as YNAB returns them (``TransactionDetail`` with its
``subtransactions`` list, ``Account``). Transactions are keyed by YNAB's id with ``account_id``
an ordinary column, so a transaction moved between accounts stays one row (finding F39), and
every field — date included (F76) — is overwritten on each sync. Deleted transactions are
kept with ``deleted = 1`` so the importer can tell "deleted in YNAB" from "never existed".

The importer reads it through a :class:`MirrorView` snapshot; the web UI through the query
helpers at the bottom of :class:`MirrorStore`.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ynab_helper.errors import YnabError
from ynab_helper.storage.db import transaction
from ynab_helper.timeutil import utc_now_iso

log = logging.getLogger(__name__)

CLEARED = "cleared"
UNCLEARED = "uncleared"
RECONCILED = "reconciled"
#: ``cleared`` values that count as cleared (reconciled is "cleared and locked").
CLEARED_LIKE = frozenset({CLEARED, RECONCILED})

# sync_meta keys (DESIGN §5.2). Legacy v1 keys ('server_knowledge:…') are left untouched.
META_MIRROR_KNOWLEDGE = "mirror_knowledge:{budget_id}"
META_ACCOUNTS_KNOWLEDGE = "accounts_knowledge:{budget_id}"
META_BUDGET_CURRENCY = "budget_currency:{budget_id}"
META_BUDGET_CURRENCY_CHECKED = "budget_currency_checked_at:{budget_id}"
META_LAST_SYNC_AT = "last_sync_at:{budget_id}"

CLASSIFICATIONS = ("cash", "credit", "tracking")
_CASH_TYPES = frozenset({"checking", "savings", "cash"})
_CREDIT_TYPES = frozenset(
    {
        "creditCard",
        "lineOfCredit",
        "mortgage",
        "autoLoan",
        "studentLoan",
        "personalLoan",
        "medicalDebt",
        "otherDebt",
    }
)
_TRACKING_TYPES = frozenset({"otherAsset", "otherLiability"})
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SORT_COLUMNS = {"date": "t.date", "amount": "t.amount", "payee_name": "t.payee_name"}


def classify_account(account_type: str | None, on_budget: bool) -> str:
    """Bucket a YNAB account into ``cash`` / ``credit`` / ``tracking`` (DESIGN §6.5).

    cash: checking, savings, cash. credit: creditCard, lineOfCredit, loans and debts.
    tracking: otherAsset, otherLiability. Unknown types: cash if on budget, else tracking.
    """
    kind = account_type or ""
    if kind in _CASH_TYPES:
        return "cash"
    lowered = kind.lower()
    if kind in _CREDIT_TYPES or "debt" in lowered or "loan" in lowered:
        return "credit"
    if kind in _TRACKING_TYPES:
        return "tracking"
    return "cash" if on_budget else "tracking"


# ── value objects ─────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class YnabTx:
    """A mirrored YNAB transaction as the importer sees it."""

    id: str
    account_id: str
    date: str
    amount: int
    payee_name: str | None
    memo: str | None
    cleared: str
    approved: bool
    deleted: bool
    import_id: str | None
    category_name: str | None
    transfer_account_id: str | None
    has_subtransactions: bool  # has at least one non-deleted subtransaction (a split)


@dataclass(frozen=True, slots=True)
class MirrorAccount:
    """A mirrored YNAB account (row of the ``accounts`` table)."""

    id: str
    budget_id: str | None
    name: str | None
    type: str | None
    classification: str | None
    on_budget: bool
    closed: bool
    deleted: bool
    note: str | None
    balance: int | None  # working balance (cleared + uncleared), milliunits
    cleared_balance: int | None
    uncleared_balance: int | None
    transfer_payee_id: str | None
    last_seen_at: str | None


class MirrorView:
    """Immutable snapshot of one account's mirror for the (pure) import planner.

    * :meth:`by_id` finds any transaction in the snapshot, deleted ones included — the account's
      own transactions plus those linked from its ledger even if they moved to another account.
    * :meth:`by_import_id` searches only this account; a live transaction wins over a deleted
      one carrying the same import_id.
    * :attr:`used_import_ids` holds every import_id in this account, deleted included (YNAB
      keeps refusing those ids).

    Tests and callers may build one directly from :class:`YnabTx` objects.
    """

    __slots__ = ("_account_txs", "_by_id", "_by_import_id", "_used_import_ids", "account_id")

    def __init__(self, account_id: str, transactions: Iterable[YnabTx]) -> None:
        self.account_id = account_id
        self._by_id: dict[str, YnabTx] = {}
        for tx in transactions:
            self._by_id[tx.id] = tx
        self._account_txs = tuple(
            sorted(
                (tx for tx in self._by_id.values() if tx.account_id == account_id),
                key=lambda tx: (tx.date, tx.id),
            )
        )
        by_import: dict[str, YnabTx] = {}
        # Preference: live over deleted, then newest date, then id (deterministic).
        for tx in sorted(self._account_txs, key=lambda tx: (not tx.deleted, tx.date, tx.id)):
            if tx.import_id:
                by_import[tx.import_id] = tx
        self._by_import_id = by_import
        self._used_import_ids = frozenset(by_import)

    def by_id(self, tx_id: str | None) -> YnabTx | None:
        return self._by_id.get(tx_id) if tx_id else None

    def by_import_id(self, import_id: str | None) -> YnabTx | None:
        return self._by_import_id.get(import_id) if import_id else None

    @property
    def used_import_ids(self) -> frozenset[str]:
        return self._used_import_ids

    def transactions(self, *, include_deleted: bool = False) -> list[YnabTx]:
        """This account's transactions ordered by date, id."""
        if include_deleted:
            return list(self._account_txs)
        return [tx for tx in self._account_txs if not tx.deleted]


# ── payload parsing (strict where importer decisions depend on it) ───────────


def _malformed(kind: str, reason: str) -> YnabError:
    return YnabError(
        f"malformed {kind} from YNAB: {reason}",
        user_message="YNAB returned data in an unexpected format. Please try again later.",
    )


def _as_mapping(item: Any, kind: str) -> Mapping[str, Any]:
    if isinstance(item, Mapping):
        return item
    raw = getattr(item, "raw", None)  # ynab.models objects keep the API dict in .raw
    if isinstance(raw, Mapping):
        return raw
    raise _malformed(kind, f"expected an object, got {type(item).__name__}")


def _req_str(data: Mapping[str, Any], key: str, kind: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise _malformed(kind, f"field {key!r} missing or not a string")
    return value


def _opt_str(data: Mapping[str, Any], key: str, kind: str) -> str | None:
    value = data.get(key)
    if value is None or isinstance(value, str):
        return value
    raise _malformed(kind, f"field {key!r} is not a string")


def _req_int(data: Mapping[str, Any], key: str, kind: str) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise _malformed(kind, f"field {key!r} missing or not integer milliunits")
    return value


def _opt_int(data: Mapping[str, Any], key: str, kind: str) -> int | None:
    return None if data.get(key) is None else _req_int(data, key, kind)


def _flag(data: Mapping[str, Any], key: str, kind: str) -> int:
    value = data.get(key)
    if value is None:
        return 0
    if not isinstance(value, bool):
        raise _malformed(kind, f"field {key!r} is not a boolean")
    return int(value)


def _transaction_row(budget_id: str, d: Mapping[str, Any], synced_at: str) -> tuple[Any, ...]:
    kind = "transaction"
    date = _req_str(d, "date", kind)
    if not _DATE_RE.match(date):
        raise _malformed(kind, f"field 'date' is not an ISO date: {date!r}")
    return (
        _req_str(d, "id", kind),
        budget_id,
        _req_str(d, "account_id", kind),
        date,
        _req_int(d, "amount", kind),
        _opt_str(d, "payee_id", kind),
        _opt_str(d, "payee_name", kind),
        _opt_str(d, "memo", kind),
        _req_str(d, "cleared", kind),
        _flag(d, "approved", kind),
        _opt_str(d, "flag_color", kind),
        _opt_str(d, "category_id", kind),
        _opt_str(d, "category_name", kind),
        _opt_str(d, "transfer_account_id", kind),
        _opt_str(d, "transfer_transaction_id", kind),
        _opt_str(d, "matched_transaction_id", kind),
        _opt_str(d, "import_id", kind),
        _flag(d, "deleted", kind),
        synced_at,
    )


def _subtransaction_rows(parent_id: str, subs: Any) -> list[tuple[Any, ...]]:
    kind = "subtransaction"
    if not isinstance(subs, list):
        raise _malformed("transaction", "field 'subtransactions' is not a list")
    rows = []
    for item in subs:
        s = _as_mapping(item, kind)
        rows.append(
            (
                _req_str(s, "id", kind),
                parent_id,
                _req_int(s, "amount", kind),
                _opt_str(s, "memo", kind),
                _opt_str(s, "payee_id", kind),
                _opt_str(s, "payee_name", kind),
                _opt_str(s, "category_id", kind),
                _opt_str(s, "category_name", kind),
                _opt_str(s, "transfer_account_id", kind),
                _flag(s, "deleted", kind),
            )
        )
    return rows


def _account_row(budget_id: str, d: Mapping[str, Any], now: str) -> tuple[Any, ...]:
    kind = "account"
    account_type = _opt_str(d, "type", kind)
    on_budget = _flag(d, "on_budget", kind)
    return (
        _req_str(d, "id", kind),
        budget_id,
        _opt_str(d, "name", kind),
        account_type,
        classify_account(account_type, bool(on_budget)),
        on_budget,
        _flag(d, "closed", kind),
        _flag(d, "deleted", kind),
        _opt_str(d, "note", kind),
        _opt_int(d, "balance", kind),
        _opt_int(d, "cleared_balance", kind),
        _opt_int(d, "uncleared_balance", kind),
        _opt_str(d, "transfer_payee_id", kind),
        now,  # last_seen_at
        now,  # first_seen_at (insert only)
        now,  # created_at (insert only)
        now,  # updated_at
    )


_TX_COLUMNS = (
    "id, budget_id, account_id, date, amount, payee_id, payee_name, memo, cleared, approved, "
    "flag_color, category_id, category_name, transfer_account_id, transfer_transaction_id, "
    "matched_transaction_id, import_id, deleted, synced_at"
)
_UPSERT_TX = f"""
    INSERT INTO ynab_transactions ({_TX_COLUMNS})
    VALUES ({", ".join("?" for _ in _TX_COLUMNS.split(","))})
    ON CONFLICT(id) DO UPDATE SET
        budget_id = excluded.budget_id, account_id = excluded.account_id,
        date = excluded.date, amount = excluded.amount, payee_id = excluded.payee_id,
        payee_name = excluded.payee_name, memo = excluded.memo, cleared = excluded.cleared,
        approved = excluded.approved, flag_color = excluded.flag_color,
        category_id = excluded.category_id, category_name = excluded.category_name,
        transfer_account_id = excluded.transfer_account_id,
        transfer_transaction_id = excluded.transfer_transaction_id,
        matched_transaction_id = excluded.matched_transaction_id,
        import_id = excluded.import_id, deleted = excluded.deleted,
        synced_at = excluded.synced_at
"""
_INSERT_SUB = """
    INSERT OR REPLACE INTO ynab_subtransactions (id, transaction_id, amount, memo, payee_id,
        payee_name, category_id, category_name, transfer_account_id, deleted)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
_UPSERT_ACCOUNT = """
    INSERT INTO accounts (account_id, budget_id, name, type, classification, on_budget, closed,
        deleted, note, balance, cleared_balance, uncleared_balance, transfer_payee_id,
        last_seen_at, first_seen_at, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(account_id) DO UPDATE SET
        budget_id = excluded.budget_id, name = excluded.name, type = excluded.type,
        classification = excluded.classification, on_budget = excluded.on_budget,
        closed = excluded.closed, deleted = excluded.deleted, note = excluded.note,
        balance = excluded.balance, cleared_balance = excluded.cleared_balance,
        uncleared_balance = excluded.uncleared_balance,
        transfer_payee_id = excluded.transfer_payee_id,
        last_seen_at = excluded.last_seen_at, updated_at = excluded.updated_at
"""
_TX_SELECT = """
    SELECT t.id, t.account_id, t.date, t.amount, t.payee_name, t.memo, t.cleared, t.approved,
           t.deleted, t.import_id, t.category_name, t.transfer_account_id,
           EXISTS (SELECT 1 FROM ynab_subtransactions s
                   WHERE s.transaction_id = t.id AND s.deleted = 0) AS has_subtransactions
    FROM ynab_transactions t
"""
_ACCOUNT_SELECT = """
    SELECT account_id, budget_id, name, type, classification, on_budget, closed, deleted, note,
           balance, cleared_balance, uncleared_balance, transfer_payee_id, last_seen_at
    FROM accounts
"""


def _tx(row: sqlite3.Row) -> YnabTx:
    return YnabTx(
        id=row["id"],
        account_id=row["account_id"],
        date=row["date"],
        amount=row["amount"],
        payee_name=row["payee_name"],
        memo=row["memo"],
        cleared=row["cleared"],
        approved=bool(row["approved"]),
        deleted=bool(row["deleted"]),
        import_id=row["import_id"],
        category_name=row["category_name"],
        transfer_account_id=row["transfer_account_id"],
        has_subtransactions=bool(row["has_subtransactions"]),
    )


def _account(row: sqlite3.Row) -> MirrorAccount:
    return MirrorAccount(
        id=row["account_id"],
        budget_id=row["budget_id"],
        name=row["name"],
        type=row["type"],
        classification=row["classification"],
        on_budget=bool(row["on_budget"]),
        closed=bool(row["closed"]),
        deleted=bool(row["deleted"]),
        note=row["note"],
        balance=row["balance"],
        cleared_balance=row["cleared_balance"],
        uncleared_balance=row["uncleared_balance"],
        transfer_payee_id=row["transfer_payee_id"],
        last_seen_at=row["last_seen_at"],
    )


def _escape_like(text: str) -> str:
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _require_budget(budget_id: str) -> None:
    if not isinstance(budget_id, str) or not budget_id:
        raise ValueError("budget_id must be a non-empty string")


def _placeholders(values: Sequence[Any]) -> str:
    return ", ".join("?" for _ in values)


# ── store ─────────────────────────────────────────────────────────────────────


class MirrorStore:
    """Access to the mirror tables (``accounts``, ``ynab_transactions``,
    ``ynab_subtransactions``) and the ``sync_meta`` key/value table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    # ── writes (sync only) ──

    def upsert_accounts(
        self,
        budget_id: str,
        accounts: Iterable[Any],
        *,
        server_knowledge: int | None = None,
        full: bool = False,
    ) -> int:
        """Insert/update YNAB ``Account`` objects of ``budget_id``; returns the number written.

        ``server_knowledge`` (if given) is stored in the same transaction, so the delta cursor
        never advances past data that was not saved. ``full=True`` means ``accounts`` is the
        complete list for the budget: accounts of this budget that are missing from it are
        marked deleted (YNAB omits deleted entities from non-delta responses).
        Raises :class:`YnabError` for a malformed object (nothing is written then).
        """
        _require_budget(budget_id)
        now = utc_now_iso()
        rows = [_account_row(budget_id, _as_mapping(a, "account"), now) for a in accounts]
        with transaction(self._conn):
            self._conn.executemany(_UPSERT_ACCOUNT, rows)
            if full:
                self._mark_missing_deleted(
                    "UPDATE accounts SET deleted = 1, updated_at = ? WHERE budget_id = ? "
                    "AND deleted = 0 AND account_id NOT IN (SELECT id FROM temp._mirror_seen)",
                    budget_id,
                    [row[0] for row in rows],
                    now,
                )
            if server_knowledge is not None:
                self.set_accounts_knowledge(budget_id, server_knowledge)
        return len(rows)

    def upsert_transactions(
        self,
        budget_id: str,
        transactions: Iterable[Any],
        *,
        server_knowledge: int | None = None,
        full: bool = False,
    ) -> int:
        """Insert/update YNAB ``TransactionDetail`` objects; returns the number written.

        Every field is replaced (account, date, cleared, deleted …). When an object carries a
        ``subtransactions`` list, the parent's stored subtransactions are replaced by it
        (deleted lines included, flagged); objects without that key leave them as they are.
        ``server_knowledge`` and ``full`` behave as in :meth:`upsert_accounts` (``full``:
        transactions of the budget missing from the list are marked deleted).
        Raises :class:`YnabError` for a malformed object (nothing is written then).
        """
        _require_budget(budget_id)
        now = utc_now_iso()
        parents: list[tuple[Any, ...]] = []
        subs: dict[str, list[tuple[Any, ...]]] = {}
        for item in transactions:
            data = _as_mapping(item, "transaction")
            row = _transaction_row(budget_id, data, now)
            parents.append(row)
            if "subtransactions" in data:
                subs[row[0]] = _subtransaction_rows(row[0], data.get("subtransactions") or [])
        with transaction(self._conn):
            self._conn.executemany(_UPSERT_TX, parents)
            for parent_id, sub_rows in subs.items():
                self._conn.execute(
                    "DELETE FROM ynab_subtransactions WHERE transaction_id = ?", (parent_id,)
                )
                self._conn.executemany(_INSERT_SUB, sub_rows)
            if full:
                self._mark_missing_deleted(
                    "UPDATE ynab_transactions SET deleted = 1, synced_at = ? WHERE budget_id = ? "
                    "AND deleted = 0 AND id NOT IN (SELECT id FROM temp._mirror_seen)",
                    budget_id,
                    [row[0] for row in parents],
                    now,
                )
            if server_knowledge is not None:
                self.set_server_knowledge(budget_id, server_knowledge)
        return len(parents)

    def _mark_missing_deleted(
        self, update_sql: str, budget_id: str, seen_ids: list[str], now: str
    ) -> None:
        """Run ``update_sql`` (which excludes ``temp._mirror_seen``) for a complete listing.

        The ids go through a temp table: a budget can hold more transactions than SQLite
        allows bound parameters. An empty "complete" listing is not trusted — a budget does
        not lose every transaction at once, an API glitch is likelier — so nothing is marked.
        """
        if not seen_ids:
            log.warning("full sync of %s returned nothing; not marking rows deleted", budget_id)
            return
        self._conn.execute("CREATE TEMP TABLE IF NOT EXISTS _mirror_seen (id TEXT PRIMARY KEY)")
        self._conn.execute("DELETE FROM temp._mirror_seen")
        self._conn.executemany(
            "INSERT OR IGNORE INTO temp._mirror_seen (id) VALUES (?)", [(i,) for i in seen_ids]
        )
        marked = self._conn.execute(update_sql, (now, budget_id)).rowcount
        self._conn.execute("DELETE FROM temp._mirror_seen")
        if marked:
            log.info("full sync of %s: %d rows absent from YNAB marked deleted", budget_id, marked)

    # ── sync_meta ──

    def get_meta(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM sync_meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("sync_meta values are strings")
        self._conn.execute(
            "INSERT INTO sync_meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def delete_meta(self, key: str) -> None:
        self._conn.execute("DELETE FROM sync_meta WHERE key = ?", (key,))

    def _get_int_meta(self, key: str) -> int | None:
        value = self.get_meta(key)
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            log.warning("ignoring non-integer sync_meta %s=%r (forces a full sync)", key, value)
            return None

    def _set_int_meta(self, key: str, value: int) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"server knowledge must be a non-negative int, got {value!r}")
        self.set_meta(key, str(value))

    def get_server_knowledge(self, budget_id: str) -> int | None:
        """Budget-wide transactions delta cursor (None → next sync is a full fetch)."""
        return self._get_int_meta(META_MIRROR_KNOWLEDGE.format(budget_id=budget_id))

    def set_server_knowledge(self, budget_id: str, knowledge: int) -> None:
        self._set_int_meta(META_MIRROR_KNOWLEDGE.format(budget_id=budget_id), knowledge)

    def get_accounts_knowledge(self, budget_id: str) -> int | None:
        return self._get_int_meta(META_ACCOUNTS_KNOWLEDGE.format(budget_id=budget_id))

    def set_accounts_knowledge(self, budget_id: str, knowledge: int) -> None:
        self._set_int_meta(META_ACCOUNTS_KNOWLEDGE.format(budget_id=budget_id), knowledge)

    def reset_knowledge(self, budget_id: str) -> None:
        """Forget both delta cursors of ``budget_id`` so the next sync fetches everything."""
        with transaction(self._conn):
            self.delete_meta(META_MIRROR_KNOWLEDGE.format(budget_id=budget_id))
            self.delete_meta(META_ACCOUNTS_KNOWLEDGE.format(budget_id=budget_id))

    def get_budget_currency(self, budget_id: str) -> str | None:
        """ISO code of the budget currency as last fetched (e.g. ``CHF``)."""
        return self.get_meta(META_BUDGET_CURRENCY.format(budget_id=budget_id))

    def budget_currency_checked_at(self, budget_id: str) -> str | None:
        """UTC ISO time the currency was last fetched (for the once-a-day refresh)."""
        return self.get_meta(META_BUDGET_CURRENCY_CHECKED.format(budget_id=budget_id))

    def set_budget_currency(
        self, budget_id: str, iso_code: str, *, checked_at: str | None = None
    ) -> None:
        with transaction(self._conn):
            self.set_meta(META_BUDGET_CURRENCY.format(budget_id=budget_id), iso_code)
            self.set_meta(
                META_BUDGET_CURRENCY_CHECKED.format(budget_id=budget_id),
                checked_at or utc_now_iso(),
            )

    def get_last_sync_at(self, budget_id: str) -> str | None:
        return self.get_meta(META_LAST_SYNC_AT.format(budget_id=budget_id))

    def set_last_sync_at(self, budget_id: str, when: str | None = None) -> None:
        self.set_meta(META_LAST_SYNC_AT.format(budget_id=budget_id), when or utc_now_iso())

    # ── reads for the importer / services ──

    def account(self, account_id: str) -> MirrorAccount | None:
        """One account by id (closed and deleted included), or None."""
        row = self._conn.execute(
            f"{_ACCOUNT_SELECT} WHERE account_id = ?", (account_id,)
        ).fetchone()
        return _account(row) if row else None

    def accounts(self, budget_id: str, include_closed: bool = False) -> list[MirrorAccount]:
        """Non-deleted accounts of ``budget_id`` (open only unless ``include_closed``).

        Legacy rows whose budget is still unknown (NULL) are not returned until a sync claims
        them. Ordered cash, credit, tracking, then by name.
        """
        closed_clause = "" if include_closed else " AND closed = 0"
        rows = self._conn.execute(
            f"{_ACCOUNT_SELECT} WHERE budget_id = ? AND deleted = 0{closed_clause} "
            "ORDER BY CASE classification WHEN 'cash' THEN 0 WHEN 'credit' THEN 1 "
            "WHEN 'tracking' THEN 2 ELSE 3 END, name COLLATE NOCASE, account_id",
            (budget_id,),
        ).fetchall()
        return [_account(r) for r in rows]

    def get_transaction(self, tx_id: str) -> YnabTx | None:
        """One mirrored transaction by id (deleted included), or None."""
        row = self._conn.execute(f"{_TX_SELECT} WHERE t.id = ?", (tx_id,)).fetchone()
        return _tx(row) if row else None

    def view_for_account(self, budget_id: str, account_id: str) -> MirrorView:
        """Snapshot for the planner: the account's transactions (deleted included) plus any
        transaction linked from this account's (or unclaimed) ledger entries, wherever it is."""
        rows = self._conn.execute(
            f"""{_TX_SELECT}
            WHERE t.budget_id = ? AND (
                t.account_id = ?
                OR t.id IN (SELECT ynab_tx_id FROM import_ledger
                            WHERE account_id IN (?, '') AND ynab_tx_id IS NOT NULL))""",
            (budget_id, account_id, account_id),
        ).fetchall()
        return MirrorView(account_id, (_tx(r) for r in rows))

    def transactions_for_account(
        self,
        budget_id: str,
        account_id: str,
        *,
        since_date: str | None = None,
        until_date: str | None = None,
        include_deleted: bool = False,
    ) -> list[YnabTx]:
        """The account's transactions (optionally within [since_date, until_date]) by date."""
        clauses = ["t.budget_id = ?", "t.account_id = ?"]
        params: list[Any] = [budget_id, account_id]
        if since_date:
            clauses.append("t.date >= ?")
            params.append(since_date)
        if until_date:
            clauses.append("t.date <= ?")
            params.append(until_date)
        if not include_deleted:
            clauses.append("t.deleted = 0")
        rows = self._conn.execute(
            f"{_TX_SELECT} WHERE {' AND '.join(clauses)} ORDER BY t.date, t.id", params
        ).fetchall()
        return [_tx(r) for r in rows]

    def cleared_sum_after(self, account_id: str, date: str) -> int:
        """Σ amount of live cleared/reconciled transactions in the account dated after ``date``.

        Used by reconcile: ``cleared_balance − cleared_sum_after(as_of)`` is the cleared
        balance as of ``as_of``.
        """
        row = self._conn.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM ynab_transactions "
            "WHERE account_id = ? AND deleted = 0 AND date > ? AND cleared IN (?, ?)",
            (account_id, date, CLEARED, RECONCILED),
        ).fetchone()
        return int(row[0])

    # ── reads for the web UI ──

    def search_transactions(
        self,
        budget_id: str,
        *,
        account_ids: Sequence[str] | None = None,
        text: str | None = None,
        cleared: str | None = None,
        category: str | None = None,
        uncategorized: bool = False,
        date_from: str | None = None,
        date_to: str | None = None,
        sort: str = "-date",
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Paginated list of live transactions for the web UI; returns ``(total, rows)``.

        ``text`` matches payee or memo as a literal substring (LIKE wildcards escaped).
        ``cleared`` ∈ {cleared, reconciled, uncleared} matches exactly that bucket. ``sort`` is
        one of date/amount/payee_name with an optional ``-`` prefix for descending. Rows are
        dicts with the transaction columns plus ``account_name`` and
        ``account_classification``.
        """
        clauses = ["t.budget_id = ?", "t.deleted = 0"]
        params: list[Any] = [budget_id]
        if account_ids:
            clauses.append(f"t.account_id IN ({_placeholders(account_ids)})")
            params.extend(account_ids)
        if text:
            like = f"%{_escape_like(text)}%"
            clauses.append("(t.payee_name LIKE ? ESCAPE '\\' OR t.memo LIKE ? ESCAPE '\\')")
            params.extend([like, like])
        if cleared:
            if cleared not in (CLEARED, UNCLEARED, RECONCILED):
                raise ValueError(f"unknown cleared filter {cleared!r}")
            clauses.append("t.cleared = ?")
            params.append(cleared)
        if uncategorized:
            clauses.append("(t.category_name IS NULL OR t.category_name = '')")
        elif category:
            clauses.append("t.category_name = ?")
            params.append(category)
        if date_from:
            clauses.append("t.date >= ?")
            params.append(date_from)
        if date_to:
            clauses.append("t.date <= ?")
            params.append(date_to)
        descending = sort.startswith("-")
        column = _SORT_COLUMNS.get(sort.lstrip("-"), "t.date")
        direction = "DESC" if descending else "ASC"
        where = " AND ".join(clauses)
        total = self._conn.execute(
            f"SELECT COUNT(*) FROM ynab_transactions t WHERE {where}", params
        ).fetchone()[0]
        rows = self._conn.execute(
            f"""SELECT t.id, t.account_id, t.date, t.amount, t.payee_name, t.memo, t.cleared,
                       t.approved, t.category_name, t.import_id, t.transfer_account_id,
                       t.flag_color, a.name AS account_name,
                       a.classification AS account_classification
                FROM ynab_transactions t LEFT JOIN accounts a ON a.account_id = t.account_id
                WHERE {where}
                ORDER BY {column} {direction}, t.id {direction}
                LIMIT ? OFFSET ?""",
            (*params, max(0, int(limit)), max(0, int(offset))),
        ).fetchall()
        return int(total), [dict(r) for r in rows]

    def categories(
        self, budget_id: str, *, account_ids: Sequence[str] | None = None
    ) -> list[dict[str, Any]]:
        """Distinct category names of live transactions with line count and outflow sum.

        Split parents contribute through their live subtransactions. Uncategorised lines are
        reported under the name ``''``.
        """
        clauses = ["t.budget_id = ?", "t.deleted = 0"]
        params: list[Any] = [budget_id]
        if account_ids:
            clauses.append(f"t.account_id IN ({_placeholders(account_ids)})")
            params.extend(account_ids)
        where = " AND ".join(clauses)
        rows = self._conn.execute(
            f"""SELECT COALESCE(name, '') AS name, COUNT(*) AS n,
                       SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS spent
                FROM (
                    SELECT t.category_name AS name, t.amount AS amount
                    FROM ynab_transactions t
                    WHERE {where} AND NOT EXISTS (
                        SELECT 1 FROM ynab_subtransactions s
                        WHERE s.transaction_id = t.id AND s.deleted = 0)
                    UNION ALL
                    SELECT s.category_name AS name, s.amount AS amount
                    FROM ynab_subtransactions s JOIN ynab_transactions t ON t.id = s.transaction_id
                    WHERE {where} AND s.deleted = 0)
                GROUP BY COALESCE(name, '') ORDER BY name COLLATE NOCASE""",
            (*params, *params),
        ).fetchall()
        return [{"name": r["name"], "count": r["n"], "spent": r["spent"] or 0} for r in rows]

    def analytics_rows(
        self,
        budget_id: str,
        *,
        date_from: str | None = None,
        date_to: str | None = None,
        expand_splits: bool = True,
    ) -> list[dict[str, Any]]:
        """Live transactions joined with their account, for :mod:`ynab_helper.web.analytics`.

        With ``expand_splits`` a split parent is replaced by its non-deleted subtransactions
        (sub amount/category/transfer; payee and memo fall back to the parent's). Each row:
        ``id, parent_id, date, amount, payee_name, memo, category_name, cleared, import_id,
        transfer_account_id, account_id, account_name, on_budget, classification``.
        """
        clauses = ["t.budget_id = ?", "t.deleted = 0"]
        params: list[Any] = [budget_id]
        if date_from:
            clauses.append("t.date >= ?")
            params.append(date_from)
        if date_to:
            clauses.append("t.date <= ?")
            params.append(date_to)
        where = " AND ".join(clauses)
        account_cols = (
            "a.name AS account_name, COALESCE(a.on_budget, 0) AS on_budget, "
            "a.classification AS classification"
        )
        parents_sql = f"""
            SELECT t.id, NULL AS parent_id, t.date, t.amount, t.payee_name, t.memo,
                   t.category_name, t.cleared, t.import_id, t.transfer_account_id,
                   t.account_id, {account_cols}
            FROM ynab_transactions t LEFT JOIN accounts a ON a.account_id = t.account_id
            WHERE {where}"""
        if not expand_splits:
            rows = self._conn.execute(f"{parents_sql} ORDER BY t.date, t.id", params).fetchall()
            return [dict(r) for r in rows]
        has_live_subs = (
            "EXISTS (SELECT 1 FROM ynab_subtransactions s "
            "WHERE s.transaction_id = t.id AND s.deleted = 0)"
        )
        sql = f"""
            {parents_sql} AND NOT {has_live_subs}
            UNION ALL
            SELECT s.id, t.id AS parent_id, t.date, s.amount,
                   COALESCE(s.payee_name, t.payee_name), COALESCE(s.memo, t.memo),
                   s.category_name, t.cleared, t.import_id, s.transfer_account_id,
                   t.account_id, {account_cols}
            FROM ynab_subtransactions s
            JOIN ynab_transactions t ON t.id = s.transaction_id
            LEFT JOIN accounts a ON a.account_id = t.account_id
            WHERE {where} AND s.deleted = 0
            ORDER BY 3, 1"""
        rows = self._conn.execute(sql, (*params, *params)).fetchall()
        return [dict(r) for r in rows]

    def account_activity(self, budget_id: str) -> dict[str, tuple[int, str | None]]:
        """``account_id → (live transaction count, newest date)`` for the budget."""
        rows = self._conn.execute(
            "SELECT account_id, COUNT(*), MAX(date) FROM ynab_transactions "
            "WHERE budget_id = ? AND deleted = 0 GROUP BY account_id",
            (budget_id,),
        ).fetchall()
        return {r[0]: (int(r[1]), r[2]) for r in rows}
