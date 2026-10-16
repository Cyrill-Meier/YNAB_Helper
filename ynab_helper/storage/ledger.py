"""Importer ledger: what we know about every Revolut row we have seen (``import_ledger``).

The ledger is written only by the importer (and the v0 → v1 migration). It never stores YNAB's
view of a transaction — that lives in the mirror (:mod:`ynab_helper.storage.mirror`) — so a
YNAB sync can no longer overwrite the CSV identity (payee, amount, date, state) the importer
matches on (findings F01/F02/F11). Rows are keyed per account (F12): the same Revolut key may
exist once per YNAB account.

Invariants enforced here (see docs/DESIGN.md §5.2):

* ``status`` ∈ :class:`LedgerStatus`; ``ACTIVE`` requires ``ynab_tx_id``.
* ``csv_state`` ∈ :class:`CsvState`; ``identity`` ∈ {``csv``, ``legacy``}.
* ``pushed_cleared`` is ``cleared``/``uncleared``/None — we never write ``reconciled``.
* ``account_id`` ``''`` marks a legacy row not yet claimed by an account.
* Within an account, a live YNAB transaction is linked from at most one ACTIVE entry; extra
  claimants become REKEYED tombstones (legacy migration, :meth:`LedgerStore.refresh_from_mirror`).
"""

from __future__ import annotations

import dataclasses
import logging
import sqlite3
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from ynab_helper.storage.db import RecordExists, RecordNotFound, transaction
from ynab_helper.timeutil import utc_now_iso

if TYPE_CHECKING:
    from ynab_helper.storage.mirror import MirrorView, YnabTx

log = logging.getLogger(__name__)

UNASSIGNED_ACCOUNT = ""


class LedgerStatus(StrEnum):
    """Lifecycle of a ledger entry (values are stored verbatim)."""

    ACTIVE = "ACTIVE"  # linked to a live YNAB transaction
    UNLINKED = "UNLINKED"  # YNAB should have it (created/duplicate) but the id is unknown
    REVERTED = "REVERTED"  # Revolut reverted it; removed from YNAB (or never created)
    SUPERSEDED = "SUPERSEDED"  # pending settled under other rows; removed from YNAB by us
    DELETED_IN_YNAB = "DELETED_IN_YNAB"  # deleted in YNAB by someone else; never recreated
    REKEYED = "REKEYED"  # tombstone: identity moved to ``rekeyed_to``


class CsvState(StrEnum):
    """Revolut State of the row as last seen in an export."""

    COMPLETED = "COMPLETED"
    PENDING = "PENDING"
    REVERTED = "REVERTED"


IDENTITY_CSV = "csv"
IDENTITY_LEGACY = "legacy"
IDENTITIES = frozenset({IDENTITY_CSV, IDENTITY_LEGACY})
PUSHED_CLEARED_VALUES = frozenset({"cleared", "uncleared"})

LEDGER_STATUSES = frozenset(s.value for s in LedgerStatus)
CSV_STATES = frozenset(s.value for s in CsvState)


def _require_str(name: str, value: Any, *, allow_none: bool = False) -> None:
    if value is None and allow_none:
        return
    if not isinstance(value, str):
        raise ValueError(f"LedgerEntry.{name} must be a string, got {value!r}")


def _require_int(name: str, value: Any, *, allow_none: bool = False) -> None:
    if value is None and allow_none:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"LedgerEntry.{name} must be an int (milliunits), got {value!r}")


@dataclass(frozen=True, slots=True, kw_only=True)
class LedgerEntry:
    """One ``import_ledger`` row; fields are exactly the table's columns, in column order.

    Construct with keywords. Timestamps left empty are filled by :meth:`LedgerStore.insert`.
    Use :func:`dataclasses.replace` to derive a modified copy (validation runs again).
    """

    account_id: str
    key: str
    ynab_import_id: str | None = None
    ynab_tx_id: str | None = None
    status: str
    csv_date: str
    csv_amount: int
    csv_payee: str | None = None
    csv_type: str | None = None
    csv_state: str
    started_at: str | None = None
    pushed_cleared: str | None = None
    pushed_amount: int | None = None
    pushed_date: str | None = None
    identity: str = IDENTITY_CSV
    rekeyed_to: str | None = None
    last_error: str | None = None
    first_seen_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        _require_str("account_id", self.account_id)
        _require_str("key", self.key)
        if not self.key:
            raise ValueError("LedgerEntry.key must not be empty")
        for name in ("ynab_import_id", "ynab_tx_id", "csv_payee", "csv_type", "started_at"):
            _require_str(name, getattr(self, name), allow_none=True)
        for name in ("pushed_date", "rekeyed_to", "last_error"):
            _require_str(name, getattr(self, name), allow_none=True)
        _require_str("csv_date", self.csv_date)
        _require_int("csv_amount", self.csv_amount)
        _require_int("pushed_amount", self.pushed_amount, allow_none=True)
        # Normalise enum members to plain strings so equality/repr are stable.
        if self.status not in LEDGER_STATUSES:
            raise ValueError(f"unknown ledger status {self.status!r}")
        object.__setattr__(self, "status", str(self.status))
        if self.csv_state not in CSV_STATES:
            raise ValueError(f"unknown csv_state {self.csv_state!r}")
        object.__setattr__(self, "csv_state", str(self.csv_state))
        if self.identity not in IDENTITIES:
            raise ValueError(f"unknown identity {self.identity!r}")
        if self.pushed_cleared is not None and self.pushed_cleared not in PUSHED_CLEARED_VALUES:
            raise ValueError(
                f"pushed_cleared must be 'cleared' or 'uncleared', got {self.pushed_cleared!r}"
            )
        if self.status == LedgerStatus.ACTIVE and not self.ynab_tx_id:
            raise ValueError(f"ACTIVE ledger entry {self.key!r} needs a ynab_tx_id")

    @property
    def import_id_in_ynab(self) -> str:
        """The import_id YNAB holds (or would hold) for this entry: ynab_import_id or key."""
        return self.ynab_import_id or self.key


LEDGER_COLUMNS: tuple[str, ...] = tuple(f.name for f in dataclasses.fields(LedgerEntry))
_UPDATABLE_COLUMNS = frozenset(LEDGER_COLUMNS) - {
    "account_id",
    "key",
    "first_seen_at",
    "updated_at",
}
_SELECT = f"SELECT {', '.join(LEDGER_COLUMNS)} FROM import_ledger"
_INSERT_COLUMNS = (
    f"INTO import_ledger ({', '.join(LEDGER_COLUMNS)}) "
    f"VALUES ({', '.join('?' for _ in LEDGER_COLUMNS)})"
)
_INSERT = f"INSERT {_INSERT_COLUMNS}"
_INSERT_OR_IGNORE = f"INSERT OR IGNORE {_INSERT_COLUMNS}"


def _entry(row: sqlite3.Row) -> LedgerEntry:
    return LedgerEntry(**{name: row[name] for name in LEDGER_COLUMNS})


def _values(entry: LedgerEntry) -> tuple[Any, ...]:
    return tuple(getattr(entry, name) for name in LEDGER_COLUMNS)


def _with_timestamps(entry: LedgerEntry, now: str) -> LedgerEntry:
    if entry.first_seen_at and entry.updated_at:
        return entry
    return dataclasses.replace(
        entry,
        first_seen_at=entry.first_seen_at or now,
        updated_at=entry.updated_at or now,
    )


def _link_holders(entries: list[LedgerEntry]) -> dict[str, str]:
    """YNAB transaction id → key of the entry that links it (an ACTIVE one if any).

    REKEYED tombstones never hold a link: their identity moved to ``rekeyed_to``.
    """
    holders: dict[str, LedgerEntry] = {}
    for entry in entries:
        if not entry.ynab_tx_id or entry.status == LedgerStatus.REKEYED:
            continue
        current = holders.get(entry.ynab_tx_id)
        if current is None or (
            entry.status == LedgerStatus.ACTIVE and current.status != LedgerStatus.ACTIVE
        ):
            holders[entry.ynab_tx_id] = entry
    return {tx_id: entry.key for tx_id, entry in holders.items()}


@dataclass(frozen=True, slots=True)
class RefreshResult:
    """Outcome of :meth:`LedgerStore.refresh_from_mirror`.

    ``conflicts`` lists the keys of UNLINKED entries whose live YNAB transaction is already
    linked from another entry of the account. They were turned into REKEYED tombstones
    pointing at that entry, so the planner can never LINK a second entry to the same YNAB
    transaction (an F02 ghost, which the superseded sweep could then DELETE).
    """

    import_ids_corrected: int = 0
    linked: int = 0
    marked_deleted: int = 0
    conflicts: tuple[str, ...] = ()


class LedgerStore:
    """Read/write access to ``import_ledger`` over one connection (see :func:`db.connect`)."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    # ── reads ──

    def list_for_account(self, account_id: str) -> list[LedgerEntry]:
        """Every entry of ``account_id`` (all statuses), ordered by csv_date, key."""
        rows = self._conn.execute(
            f"{_SELECT} WHERE account_id = ? ORDER BY csv_date, key", (account_id,)
        ).fetchall()
        return [_entry(r) for r in rows]

    def list_unassigned(self) -> list[LedgerEntry]:
        """Legacy entries not yet claimed by an account (``account_id = ''``); read-only.

        Lets a dry run plan with these rows without claiming them (finding F74).
        """
        return self.list_for_account(UNASSIGNED_ACCOUNT)

    def get(self, account_id: str, key: str) -> LedgerEntry | None:
        row = self._conn.execute(
            f"{_SELECT} WHERE account_id = ? AND key = ?", (account_id, key)
        ).fetchone()
        return _entry(row) if row else None

    def by_ynab_tx_id(self, ynab_tx_id: str, *, account_id: str | None = None) -> list[LedgerEntry]:
        """Entries (any status) linked to ``ynab_tx_id``, optionally within one account."""
        if account_id is None:
            rows = self._conn.execute(
                f"{_SELECT} WHERE ynab_tx_id = ? ORDER BY account_id, key", (ynab_tx_id,)
            ).fetchall()
        else:
            rows = self._conn.execute(
                f"{_SELECT} WHERE ynab_tx_id = ? AND account_id = ? ORDER BY key",
                (ynab_tx_id, account_id),
            ).fetchall()
        return [_entry(r) for r in rows]

    def active_tx_ids(self, account_id: str) -> set[str]:
        """YNAB transaction ids linked from ACTIVE entries of ``account_id``."""
        rows = self._conn.execute(
            "SELECT ynab_tx_id FROM import_ledger "
            "WHERE account_id = ? AND status = ? AND ynab_tx_id IS NOT NULL",
            (account_id, LedgerStatus.ACTIVE.value),
        ).fetchall()
        return {r[0] for r in rows}

    def keys_in_use(self, account_id: str) -> set[str]:
        """Every key and ynab_import_id of ``account_id`` in any status (for §6.4 id choice)."""
        rows = self._conn.execute(
            "SELECT key, ynab_import_id FROM import_ledger WHERE account_id = ?", (account_id,)
        ).fetchall()
        used: set[str] = set()
        for key, import_id in rows:
            used.add(key)
            if import_id:
                used.add(import_id)
        return used

    def newest_active_date(self, account_id: str) -> str | None:
        """Newest csv_date among ACTIVE entries of ``account_id`` (None if there are none)."""
        row = self._conn.execute(
            "SELECT MAX(csv_date) FROM import_ledger WHERE account_id = ? AND status = ?",
            (account_id, LedgerStatus.ACTIVE.value),
        ).fetchone()
        return row[0] if row else None

    def counts_by_status(self, account_id: str) -> dict[str, int]:
        """Number of entries per status for ``account_id``; every status is present (0 if none)."""
        counts = {status.value: 0 for status in LedgerStatus}
        for status, n in self._conn.execute(
            "SELECT status, COUNT(*) FROM import_ledger WHERE account_id = ? GROUP BY status",
            (account_id,),
        ):
            counts[status] = n
        return counts

    # ── writes ──

    def insert(self, entry: LedgerEntry) -> LedgerEntry:
        """Insert a new entry; empty timestamps are set to now. Raises :class:`RecordExists`."""
        stored = _with_timestamps(entry, utc_now_iso())
        try:
            self._conn.execute(_INSERT, _values(stored))
        except sqlite3.IntegrityError as exc:
            raise RecordExists(
                f"ledger entry {stored.account_id!r}/{stored.key!r} already exists"
            ) from exc
        return stored

    def insert_many(self, entries: list[LedgerEntry]) -> list[LedgerEntry]:
        """Insert several entries atomically (all or none)."""
        with transaction(self._conn):
            return [self.insert(entry) for entry in entries]

    def insert_missing(self, entries: list[LedgerEntry]) -> int:
        """Insert the entries whose ``(account_id, key)`` is not in the ledger yet, atomically.

        Existing rows are left exactly as they are (re-running the legacy migration must not
        undo importer activity). Empty timestamps are set to now. Returns how many were added.
        """
        now = utc_now_iso()
        inserted = 0
        with transaction(self._conn):
            for entry in entries:
                stored = _with_timestamps(entry, now)
                inserted += self._conn.execute(_INSERT_OR_IGNORE, _values(stored)).rowcount
        return inserted

    def update(self, account_id: str, key: str, /, **fields: Any) -> LedgerEntry:
        """Change ``fields`` of one entry and bump ``updated_at``; returns the new entry.

        Only data columns may be changed (not ``account_id``, ``key`` or timestamps — use
        :meth:`rekey` / :meth:`claim_unassigned`). The merged entry is validated before it is
        written. Raises :class:`RecordNotFound` if the entry does not exist.
        """
        unknown = set(fields) - _UPDATABLE_COLUMNS
        if unknown:
            raise ValueError(f"ledger columns not updatable: {sorted(unknown)}")
        with transaction(self._conn):
            current = self.get(account_id, key)
            if current is None:
                raise RecordNotFound(f"no ledger entry {account_id!r}/{key!r}")
            if not fields:
                return current
            updated = dataclasses.replace(current, **fields, updated_at=utc_now_iso())
            columns = [*fields, "updated_at"]
            self._conn.execute(
                f"UPDATE import_ledger SET {', '.join(f'{c} = ?' for c in columns)} "
                "WHERE account_id = ? AND key = ?",
                (*(getattr(updated, c) for c in columns), account_id, key),
            )
            return updated

    def set_status(
        self, account_id: str, key: str, status: str, *, last_error: str | None = None
    ) -> LedgerEntry:
        """Set ``status`` (and ``last_error``, cleared by default) of one entry."""
        return self.update(account_id, key, status=status, last_error=last_error)

    def rekey(self, account_id: str, old_key: str, new_key: str) -> LedgerEntry:
        """Move an entry's identity to ``new_key``; the old row becomes a REKEYED tombstone.

        The new row copies every field (status, links, csv_* and pushed_* data, first_seen_at)
        and keeps the import_id YNAB holds for the transaction (``ynab_import_id or old_key``)
        because YNAB never changes a transaction's import_id. An existing REKEYED tombstone at
        ``new_key`` is replaced; any other existing entry raises :class:`RecordExists`.
        Returns the new entry.
        """
        if old_key == new_key:
            raise ValueError("rekey needs two different keys")
        now = utc_now_iso()
        with transaction(self._conn):
            old = self.get(account_id, old_key)
            if old is None:
                raise RecordNotFound(f"no ledger entry {account_id!r}/{old_key!r}")
            if old.status == LedgerStatus.REKEYED:
                raise ValueError(f"ledger entry {old_key!r} is already a REKEYED tombstone")
            existing = self.get(account_id, new_key)
            if existing is not None:
                if existing.status != LedgerStatus.REKEYED:
                    raise RecordExists(
                        f"cannot rekey {old_key!r}: {new_key!r} exists with status {existing.status}"
                    )
                self._conn.execute(
                    "DELETE FROM import_ledger WHERE account_id = ? AND key = ?",
                    (account_id, new_key),
                )
            moved = dataclasses.replace(
                old,
                key=new_key,
                ynab_import_id=old.ynab_import_id or old_key,
                rekeyed_to=None,
                updated_at=now,
            )
            self._conn.execute(_INSERT, _values(moved))
            self._conn.execute(
                "UPDATE import_ledger SET status = ?, rekeyed_to = ?, updated_at = ? "
                "WHERE account_id = ? AND key = ?",
                (LedgerStatus.REKEYED.value, new_key, now, account_id, old_key),
            )
        log.info("ledger: rekeyed %s -> %s (account %s)", old_key, new_key, account_id)
        return moved

    def claim_unassigned(self, account_id: str) -> int:
        """Assign every unclaimed legacy entry (``account_id = ''``) to ``account_id``.

        Entries whose key already exists in the target account stay unclaimed (logged) rather
        than overwriting it. Returns the number of claimed entries. Callers must not claim
        during a dry run (F74); plan with :meth:`list_unassigned` instead.
        """
        if not account_id:
            raise ValueError("claim_unassigned needs a real account id")
        with transaction(self._conn):
            claimed = self._conn.execute(
                "UPDATE OR IGNORE import_ledger SET account_id = ?, updated_at = ? "
                "WHERE account_id = ?",
                (account_id, utc_now_iso(), UNASSIGNED_ACCOUNT),
            ).rowcount
            left = self._conn.execute(
                "SELECT COUNT(*) FROM import_ledger WHERE account_id = ?", (UNASSIGNED_ACCOUNT,)
            ).fetchone()[0]
        if claimed:
            log.info("ledger: claimed %d unassigned legacy entries for %s", claimed, account_id)
        if left:
            log.warning(
                "ledger: %d unassigned entries collide with keys of %s and stay unclaimed",
                left,
                account_id,
            )
        return claimed

    def refresh_from_mirror(self, account_id: str, mirror_view: MirrorView) -> RefreshResult:
        """Reconcile ledger links with a fresh mirror (DESIGN §5.3, run after every sync).

        * Linked entries (any status but REKEYED): ``ynab_import_id`` := the mirror
          transaction's import_id when YNAB has one and it differs (v1 re-keys left the local
          key different from YNAB's immutable import_id, finding F02).
        * UNLINKED entries: look up ``ynab_import_id`` then ``key`` among the account's mirror
          transactions.
          * Deleted match → DELETED_IN_YNAB (never written to again, so sharing the id with
            another entry is harmless).
          * Live match that no other entry links → ACTIVE (linked). ``pushed_*`` fields that
            are still unknown are filled from what YNAB holds.
          * Live match that another entry already links → REKEYED tombstone pointing at that
            entry, reported in ``conflicts``. One YNAB transaction keeps exactly one live
            entry, the same rule the legacy migration applies to v1 ghost rows (F02).

        ACTIVE entries whose transaction vanished are *not* changed here — the importer reports
        those itself. All changes happen in one transaction.
        """
        corrected = linked = deleted = 0
        conflicts: list[str] = []
        with transaction(self._conn):
            entries = self.list_for_account(account_id)
            holders = _link_holders(entries)
            for entry in entries:
                if entry.status == LedgerStatus.REKEYED:
                    continue
                if entry.ynab_tx_id:
                    corrected += self._correct_import_id(entry, mirror_view)
                    continue
                if entry.status != LedgerStatus.UNLINKED:
                    continue
                tx = self._match_unlinked(entry, mirror_view)
                if tx is None:
                    continue
                if tx.deleted:
                    self._link(entry, tx, LedgerStatus.DELETED_IN_YNAB)
                    deleted += 1
                elif tx.id in holders:
                    self._tombstone(entry, tx, holders[tx.id])
                    conflicts.append(entry.key)
                else:
                    self._link(entry, tx, LedgerStatus.ACTIVE)
                    holders[tx.id] = entry.key
                    linked += 1
        if corrected or linked or deleted or conflicts:
            log.info(
                "ledger refresh %s: import_ids corrected=%d linked=%d deleted=%d conflicts=%d",
                account_id,
                corrected,
                linked,
                deleted,
                len(conflicts),
            )
        return RefreshResult(corrected, linked, deleted, tuple(conflicts))

    def _correct_import_id(self, entry: LedgerEntry, mirror_view: MirrorView) -> int:
        """Adopt the import_id YNAB holds for a linked entry; returns 1 if it changed."""
        tx = mirror_view.by_id(entry.ynab_tx_id) if entry.ynab_tx_id else None
        if tx is None or not tx.import_id or tx.import_id == entry.ynab_import_id:
            return 0
        self.update(entry.account_id, entry.key, ynab_import_id=tx.import_id)
        return 1

    def _link(self, entry: LedgerEntry, tx: YnabTx, status: LedgerStatus) -> None:
        # Known pushed_* values are kept: they let the planner tell a user's edit in YNAB
        # from a change in the CSV. Unknown ones are what YNAB holds now ("observed at link").
        self.update(
            entry.account_id,
            entry.key,
            status=status.value,
            ynab_tx_id=tx.id,
            ynab_import_id=tx.import_id or entry.ynab_import_id,
            pushed_cleared=entry.pushed_cleared
            or ("uncleared" if tx.cleared == "uncleared" else "cleared"),
            pushed_amount=entry.pushed_amount if entry.pushed_amount is not None else tx.amount,
            pushed_date=entry.pushed_date or tx.date,
        )

    def _tombstone(self, entry: LedgerEntry, tx: YnabTx, holder_key: str) -> None:
        log.warning(
            "ledger: UNLINKED %s resolves to YNAB %s, already linked from %s; "
            "it became a REKEYED tombstone pointing there",
            entry.key,
            tx.id,
            holder_key,
        )
        self.update(
            entry.account_id,
            entry.key,
            status=LedgerStatus.REKEYED.value,
            rekeyed_to=holder_key,
            ynab_tx_id=tx.id,
            ynab_import_id=tx.import_id or entry.ynab_import_id,
        )

    @staticmethod
    def _match_unlinked(entry: LedgerEntry, mirror_view: MirrorView) -> YnabTx | None:
        candidates = [entry.ynab_import_id, entry.key]
        for import_id in dict.fromkeys(c for c in candidates if c):
            tx = mirror_view.by_import_id(import_id)
            if tx is not None:
                return tx
        return None
