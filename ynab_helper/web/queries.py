"""Read-only SQL over the YNAB mirror for the web UI's lists (docs/DESIGN.md §9.2).

The transactions list reads the **mirror** (``ynab_transactions`` joined with
``accounts``) of the user's budget — what YNAB has, every account, filtered by account on
request. The WHERE clause is built from fixed fragments with ``?`` parameters only; **every
fragment is wrapped in parentheses** before the fragments are joined with ``AND`` (the
PENTEST M-1 regression guard: an unparenthesised ``a OR b`` joined with ``AND`` silently
drops the other filters), and LIKE wildcards in the search text are escaped.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final

__all__ = [
    "CLEARED_STATES",
    "SORTS",
    "UNCATEGORIZED",
    "TransactionFilters",
    "account_tx_counts",
    "build_where",
    "escape_like",
    "search_transactions",
    "where_fragments",
]

#: ``category=__none__`` selects uncategorized lines (v1 sentinel).
UNCATEGORIZED: Final = "__none__"
CLEARED_STATES: Final = frozenset({"cleared", "reconciled", "uncleared"})
#: ``sort`` values (optionally prefixed with ``-`` for descending) → ORDER BY expression.
SORTS: Final = {
    "date": "t.date",
    "amount": "t.amount",
    "payee_name": "COALESCE(t.payee_name, '') COLLATE NOCASE",
}
#: v1 also offered ``imported_at``; the mirror has no such column, so it sorts by date.
SORT_ALIASES: Final = {"imported_at": "date"}
DEFAULT_SORT: Final = "-date"

_LIVE_SUBS: Final = (
    "SELECT 1 FROM ynab_subtransactions s WHERE s.transaction_id = t.id AND s.deleted = 0"
)


def escape_like(text: str) -> str:
    """Escape ``\\``, ``%`` and ``_`` for ``LIKE … ESCAPE '\\'``."""
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


@dataclass(frozen=True, slots=True)
class TransactionFilters:
    """Validated filters of ``GET /api/transactions``."""

    text: str | None = None
    state: str | None = None  # cleared | reconciled | uncleared (None = all)
    category: str | None = None  # a name or UNCATEGORIZED
    account_ids: tuple[str, ...] = ()
    date_from: str | None = None
    date_to: str | None = None
    sort: str = DEFAULT_SORT
    limit: int = 50
    offset: int = 0


def build_where(budget_id: str, f: TransactionFilters) -> tuple[str, list[Any]]:
    """The WHERE clause (without the keyword) and its parameters.

    Each fragment of :func:`where_fragments` is parenthesised before joining with ``AND``
    (M-1 guard).
    """
    fragments, params = where_fragments(budget_id, f)
    return " AND ".join(f"({fragment})" for fragment in fragments), params


def where_fragments(budget_id: str, f: TransactionFilters) -> tuple[list[str], list[Any]]:
    """The filter conditions (unparenthesised) and their parameters, in order."""
    fragments: list[str] = ["t.budget_id = ?", "t.deleted = 0"]
    params: list[Any] = [budget_id]
    if f.account_ids:
        fragments.append(f"t.account_id IN ({', '.join('?' for _ in f.account_ids)})")
        params.extend(f.account_ids)
    if f.text:
        like = f"%{escape_like(f.text)}%"
        fragments.append(
            "t.payee_name LIKE ? ESCAPE '\\' OR t.memo LIKE ? ESCAPE '\\' OR EXISTS ("
            f"{_LIVE_SUBS} AND (s.payee_name LIKE ? ESCAPE '\\' OR s.memo LIKE ? ESCAPE '\\'))"
        )
        params.extend([like, like, like, like])
    if f.state:
        if f.state not in CLEARED_STATES:
            raise ValueError(f"unknown state filter {f.state!r}")
        fragments.append("t.cleared = ?")
        params.append(f.state)
    if f.category == UNCATEGORIZED:
        # On-budget, not a transfer, and either an unsplit line without a category or a
        # split with at least one uncategorized live line.
        fragments.append("COALESCE(a.on_budget, 0) = 1")
        fragments.append("t.transfer_account_id IS NULL")
        fragments.append(
            f"(NOT EXISTS ({_LIVE_SUBS}) AND (t.category_name IS NULL OR t.category_name = ''))"
            f" OR EXISTS ({_LIVE_SUBS} AND (s.category_name IS NULL OR s.category_name = '')"
            " AND s.transfer_account_id IS NULL)"
        )
    elif f.category:
        # A split matches through its live lines only (its own category is YNAB's "Split"
        # placeholder), exactly as the analytics count it.
        fragments.append(
            f"(NOT EXISTS ({_LIVE_SUBS}) AND t.category_name = ?)"
            f" OR EXISTS ({_LIVE_SUBS} AND s.category_name = ?)"
        )
        params.extend([f.category, f.category])
    if f.date_from:
        fragments.append("t.date >= ?")
        params.append(f.date_from)
    if f.date_to:
        fragments.append("t.date <= ?")
        params.append(f.date_to)
    return fragments, params


def _order_by(sort: str) -> str:
    descending = sort.startswith("-")
    name = sort.lstrip("-")
    name = SORT_ALIASES.get(name, name)
    if name not in SORTS:
        name, descending = "date", True
    direction = "DESC" if descending else "ASC"
    return f"{SORTS[name]} {direction}, t.id {direction}"


def search_transactions(
    conn: sqlite3.Connection, budget_id: str, filters: TransactionFilters
) -> tuple[int, list[dict[str, Any]]]:
    """``(total, rows)`` of live mirror transactions matching ``filters``.

    Each row: ``id, date, amount, payee_name, memo, cleared, approved, flag_color,
    category_name, import_id, transfer_account_id, account_id, account_name,
    account_classification, subtransactions`` (live lines of a split, else ``[]``).
    """
    where, params = build_where(budget_id, filters)
    base = "FROM ynab_transactions t LEFT JOIN accounts a ON a.account_id = t.account_id"
    total = conn.execute(f"SELECT COUNT(*) {base} WHERE {where}", params).fetchone()[0]
    rows = conn.execute(
        f"""SELECT t.id, t.date, t.amount, t.payee_name, t.memo, t.cleared, t.approved,
                   t.flag_color, t.category_name, t.import_id, t.transfer_account_id,
                   t.account_id, a.name AS account_name,
                   a.classification AS account_classification
            {base} WHERE {where}
            ORDER BY {_order_by(filters.sort)}
            LIMIT ? OFFSET ?""",
        (*params, max(0, int(filters.limit)), max(0, int(filters.offset))),
    ).fetchall()
    items = [dict(r) for r in rows]
    subs = _subtransactions(conn, [item["id"] for item in items])
    for item in items:
        item["approved"] = bool(item["approved"])
        item["subtransactions"] = subs.get(item["id"], [])
    return int(total), items


def _subtransactions(conn: sqlite3.Connection, parent_ids: Sequence[str]) -> dict[str, list]:
    if not parent_ids:
        return {}
    rows = conn.execute(
        "SELECT id, transaction_id, amount, memo, payee_name, category_name, "
        "transfer_account_id FROM ynab_subtransactions "
        f"WHERE (deleted = 0) AND (transaction_id IN ({', '.join('?' for _ in parent_ids)})) "
        "ORDER BY transaction_id, id",
        list(parent_ids),
    ).fetchall()
    out: dict[str, list] = {}
    for r in rows:
        out.setdefault(r["transaction_id"], []).append(
            {
                "id": r["id"],
                "amount": r["amount"],
                "memo": r["memo"],
                "payee_name": r["payee_name"],
                "category_name": r["category_name"],
                "transfer_account_id": r["transfer_account_id"],
            }
        )
    return out


def account_tx_counts(conn: sqlite3.Connection, budget_id: str, account_id: str) -> dict[str, int]:
    """Live mirror transactions of one account by cleared state, plus ``total``."""
    rows = conn.execute(
        "SELECT cleared, COUNT(*) FROM ynab_transactions "
        "WHERE (budget_id = ?) AND (account_id = ?) AND (deleted = 0) GROUP BY cleared",
        (budget_id, account_id),
    ).fetchall()
    counts = {"cleared": 0, "reconciled": 0, "uncleared": 0}
    for state, n in rows:
        counts[str(state)] = counts.get(str(state), 0) + int(n)
    counts["total"] = sum(counts.values())
    return counts
