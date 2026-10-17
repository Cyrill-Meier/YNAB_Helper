"""Revolut account-statement parser (docs/DESIGN.md §6.1).

The parser turns a Revolut CSV export into :class:`RevolutRow` objects. The derivation
of ``key`` (the legacy YNAB ``import_id``), ``amount``, ``date``, ``memo`` and ``payee``
is **byte-identical** to v1's ``parse_revolut_csv`` for every input v1 accepted,
because about 6,600 existing YNAB transactions carry those import_ids and YNAB
deduplicates on them (§2.5). In particular:

* ``amount`` uses v1's float formula ``int(round((float(Amount) - float(Fee)) * 1000))``,
  the fee being subtracted only when it is non-zero (Python's ``round`` is banker's
  rounding on the binary float, so a Decimal re-implementation would *not* be identical);
* ``key`` is ``YNAB:{amount}:{date}:{occurrence}`` where ``occurrence`` counts rows with
  the same ``(amount, date)`` in file order over every tracked-product row whose date and
  amount parsed, **whatever its state** (v1 counted DECLINED/FAILED rows too);
* the CSV is decoded as ``utf-8-sig`` with universal newlines and read by
  :class:`csv.DictReader`, exactly like v1's ``open(..., encoding="utf-8-sig")``.

Intended differences from v1 (all on inputs v1 mishandled or crashed on):

* rows whose State is not COMPLETED/PENDING/REVERTED are skipped and reported (F37),
  but still consume an occurrence number;
* non-finite or absurd amounts, incomplete rows and unparseable rows are reported in
  :attr:`ParsedExport.skipped` instead of crashing the import or being dropped silently
  (F85);
* Currency is parsed; a file mixing currencies or carrying a Currency value that is not
  a three-letter code raises :class:`CsvError` (F14);
* Completed Date and Balance are parsed; the file is limited to 25 MB and
  :data:`MAX_COLUMNS` columns; the required header columns are checked up front.

Locations in messages are given as "data row N": the N-th record after the header
(:attr:`RevolutRow.line_no`). In a spreadsheet that record is row N + 1.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import math
import os
import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from types import MappingProxyType

from ynab_helper.errors import CsvError

log = logging.getLogger(__name__)

#: Largest statement accepted (bytes). Telegram bot downloads and web uploads use the same cap.
MAX_CSV_BYTES = 25 * 1024 * 1024

#: Bytes of a file inspected by :func:`looks_like_revolut_csv` callers that sniff a head.
HEADER_SNIFF_BYTES = 64 * 1024

#: Largest absolute Amount, Fee or net amount (currency units) accepted for a row.
MAX_ABS_AMOUNT = 1_000_000_000

#: Balances whose absolute value reaches this (currency units) are ignored with a warning
#: (exclusive bound, unlike amounts). The Balance drives the reconcile adjustment, so an
#: absurd value such as "1e9" (F85) must not become a balance; reconcile still has to show
#: the resulting delta for confirmation (two-step, DESIGN §6.6).
MAX_ABS_BALANCE = 1_000_000_000

#: Most header columns accepted. Revolut writes 10. The bound keeps the header checks and
#: :class:`csv.DictReader` (which pads every short row to the header width) linear in the
#: file size.
MAX_COLUMNS = 64

#: Earliest Started Date year accepted; Revolut did not exist before, and older years would
#: make ``strftime`` and ``isoformat`` disagree (years < 1000 are not zero-padded everywhere).
MIN_YEAR = 1970

#: Revolut "Product" values imported into YNAB. An empty Product counts as tracked (v1).
TRACKED_PRODUCTS = frozenset({"Current"})

#: Header columns a file must contain to be accepted as a Revolut statement.
REQUIRED_COLUMNS = ("Started Date", "Completed Date", "Description", "Amount", "State")

#: Every column the parser reads (the full Revolut header).
KNOWN_COLUMNS = frozenset({*REQUIRED_COLUMNS, "Type", "Product", "Fee", "Currency", "Balance"})

#: Memo marker the importer adds to PENDING rows (v1 format, kept for compatibility).
PENDING_MARKER = "(pending)"

STATE_COMPLETED = "COMPLETED"
STATE_PENDING = "PENDING"
STATE_REVERTED = "REVERTED"

#: State allowlist → YNAB ``cleared`` value for a create. REVERTED rows are tracked (they
#: delete a previously imported pending) but never created, hence ``None``. Every other
#: State (DECLINED, FAILED, …) is skipped and reported.
STATE_ALLOWLIST: Mapping[str, str | None] = MappingProxyType(
    {STATE_COMPLETED: "cleared", STATE_PENDING: "uncleared", STATE_REVERTED: None}
)

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
_DATE_FORMAT = "%Y-%m-%d"

#: Columns v1 dereferenced with ``.strip()`` / ``float()``: a row missing one of them
#: (fewer fields than the header) crashed v1, so v2 may skip it without breaking parity.
_STRICT_COLUMNS = ("Type", "Started Date", "Description", "Amount", "State")

_FILENAME_WINDOW_RE = re.compile(
    r"account-statement_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})", re.IGNORECASE
)
_LINE_BREAK_RE = re.compile(r"\r\n|\r|\n")
#: Shape of a Currency cell after strip/upper (ISO 4217 alphabetic code).
_CURRENCY_RE = re.compile(r"[A-Z]{3}")

#: Warnings kept per export; further ones are summarised in one line.
_MAX_WARNINGS = 50
#: Currencies named in the mixed-currency error before it says "and N more".
_MAX_CURRENCIES_SHOWN = 5


# ─── Skip codes ──────────────────────────────────────────────────────────────

SKIP_INCOMPLETE = "incomplete_row"
SKIP_MISSING_DATE = "missing_date"
SKIP_INVALID_DATE = "invalid_date"
SKIP_IMPLAUSIBLE_DATE = "implausible_date"
SKIP_INVALID_AMOUNT = "invalid_amount"
SKIP_NON_FINITE = "non_finite_amount"
SKIP_ABSURD_AMOUNT = "absurd_amount"
SKIP_UNSUPPORTED_STATE = "unsupported_state"


# ─── Data model ──────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class RevolutRow:
    """One tracked-product row of a Revolut export.

    Invariants: ``state`` is in :data:`STATE_ALLOWLIST`; ``date`` is
    ``started_at.date().isoformat()``; ``key`` is ``YNAB:{amount}:{date}:{n}``;
    ``currency`` is ``""`` or three letters A-Z.

    ``payee`` follows v1 byte for byte: the stripped Description if non-empty, else the
    *unstripped* Type cell (so it may be ``""`` or padded), else ``"Unknown"`` only when
    the file has no Type column; truncated to 100 characters. Consumers that compare
    payees must trim them first.
    """

    line_no: int  # data row number: N-th record after the header, 1-based (spreadsheet row N+1)
    key: str  # legacy import_id (§2.5)
    type: str  # Revolut Type, stripped
    product: str  # Revolut Product, stripped ("" or a tracked product)
    description: str  # Revolut Description, stripped
    payee: str  # v1 rule, see the class docstring: may be "" or padded
    started_at: datetime  # naive, as rendered by this export
    completed_at: datetime | None  # None for PENDING/REVERTED rows (or unparseable)
    date: str  # YYYY-MM-DD of started_at
    amount: int  # milliunits, fee included (legacy float formula)
    fee: float  # as parsed (0.0 when empty or unparseable, as v1)
    currency: str  # upper-cased three-letter code, "" when the column is missing/empty
    state: str  # Revolut State, stripped and upper-cased; one of STATE_ALLOWLIST
    balance: int | None  # Balance in milliunits; None when empty, invalid or >= MAX_ABS_BALANCE
    memo: str | None  # legacy memo (Type | (pending) | Fee: x)[:200], None if empty
    started_has_time: bool = True  # False for legacy exports without a time of day

    @property
    def cleared(self) -> str:
        """YNAB cleared value v1 used for this row ("uncleared" for REVERTED too)."""
        return "cleared" if self.state == STATE_COMPLETED else "uncleared"

    @property
    def started_at_text(self) -> str:
        """Started timestamp as stored in the ledger: ``YYYY-MM-DD HH:MM:SS``."""
        return self.started_at.strftime(_TIMESTAMP_FORMAT)


@dataclass(frozen=True, slots=True)
class SkippedRow:
    """A tracked-product row that was not turned into a :class:`RevolutRow`.

    ``line_no`` has the meaning of :attr:`RevolutRow.line_no`; render it as "data row N"
    (or as spreadsheet row N + 1), never as a file line. ``reason`` is human-readable and
    safe to show to the statement's owner (it may quote a clipped raw cell value, so
    renderers must escape it). ``code`` is one of the ``SKIP_*`` constants. ``key`` is
    set when the row consumed an occurrence number.
    """

    line_no: int
    reason: str
    code: str
    key: str | None = None

    def as_tuple(self) -> tuple[int, str]:
        """``(line_no, reason)`` as stored in ``ImportResult.skipped_rows``."""
        return (self.line_no, self.reason)


@dataclass(slots=True)
class ParsedExport:
    """Result of :func:`parse_export`."""

    rows: list[RevolutRow]  # tracked product only, allowlisted states, file order
    currency: str | None  # the single currency of `rows` (None if no row has one)
    window: tuple[str, str] | None  # (min date, max date) over `rows`
    skipped: list[SkippedRow]  # tracked-product rows that were not imported
    other_products: dict[str, int]  # Product → count (not tracked, reported only)
    filename_window: tuple[str, str] | None  # from account-statement_<from>_<to>_….csv
    sha256: str  # of the raw file bytes
    warnings: list[str] = field(default_factory=list)  # non-fatal oddities (row kept)
    data_rows: int = 0  # every data row read, all products

    def state_counts(self) -> dict[str, int]:
        """Number of rows per state, e.g. ``{"COMPLETED": 40, "PENDING": 2}``."""
        return dict(Counter(row.state for row in self.rows))


# ─── Public helpers ──────────────────────────────────────────────────────────


def looks_like_revolut_csv(head: bytes) -> bool:
    """Return True if ``head`` (the first bytes of a file) starts with a Revolut header.

    The header line must contain every column in :data:`REQUIRED_COLUMNS` (exact,
    case-sensitive names, optional UTF-8 BOM). Only the first
    :data:`HEADER_SNIFF_BYTES` bytes are inspected, so callers may pass a whole file.
    """
    if not head:
        return False
    # A truncated multi-byte character at the end of the sniffed head must not fail.
    text = bytes(head[:HEADER_SNIFF_BYTES]).decode("utf-8-sig", errors="replace")
    first_line = _LINE_BREAK_RE.split(text, maxsplit=1)[0]
    try:
        fields = next(csv.reader([first_line]), [])
    except csv.Error:
        return False
    return all(column in fields for column in REQUIRED_COLUMNS)


def parse_filename_window(name: str | os.PathLike[str] | None) -> tuple[str, str] | None:
    """Return ``(from, to)`` from a Revolut name like ``account-statement_<from>_<to>_….csv``.

    Only the last path component is inspected (``/`` and ``\\`` both separate). The match
    is case-insensitive and may occur anywhere in the name (v1 ``parse_csv_date_range``).
    Returns None when there is no match, a date is not a real calendar date, or
    ``from > to``.
    """
    if name is None:
        return None
    base = re.split(r"[\\/]", os.fspath(name))[-1]
    match = _FILENAME_WINDOW_RE.search(base)
    if not match:
        return None
    start, end = match.group(1), match.group(2)
    try:
        if date.fromisoformat(start) > date.fromisoformat(end):
            return None
    except ValueError:
        return None
    return (start, end)


_MARKER_RE = re.compile(
    r"(?P<pre>\s*\|\s*|\s*)" + re.escape(PENDING_MARKER) + r"(?P<post>\s*\|\s*|\s*)"
)


def has_pending_marker(memo: str | None) -> bool:
    """True if ``memo`` contains the importer's ``(pending)`` marker."""
    return bool(memo) and PENDING_MARKER in memo


def strip_pending_marker(memo: str | None) -> str | None:
    """Remove the ``(pending)`` marker and one adjacent separator from ``memo``.

    Everything else the user wrote is preserved byte for byte (unlike v1, which also
    stripped any leading/trailing spaces and pipes of the whole memo)::

        "Transfer | (pending) | Fee: 0.5"  → "Transfer | Fee: 0.5"
        "(pending) | Fee: 0.5"             → "Fee: 0.5"
        "Transfer | (pending)"             → "Transfer"
        "(pending)"                        → ""

    ``None`` and ``""`` are returned unchanged; a memo without the marker is returned
    unchanged (the same object).
    """
    if not memo or PENDING_MARKER not in memo:
        return memo

    def _replace(match: re.Match[str]) -> str:
        pre, post = match.group("pre"), match.group("post")
        if "|" in pre and "|" in post:
            return post  # between two parts: keep exactly one separator, as written
        if match.start() == 0 or match.end() == len(memo):
            # Marker at either edge: drop it with its own separator/whitespace.
            return ""
        if "|" in post:
            return post
        if "|" in pre:
            return pre
        # Marker inside free text: keep one of the surrounding whitespace runs.
        return pre or post

    return _MARKER_RE.sub(_replace, memo)


# ─── Parsing ─────────────────────────────────────────────────────────────────


def parse_export(
    source: Path | os.PathLike[str] | str | bytes | bytearray | memoryview,
    *,
    filename: str | None = None,
) -> ParsedExport:
    """Parse a Revolut statement from a file path or from its raw bytes.

    ``filename`` (the original upload name) is used only for
    :attr:`ParsedExport.filename_window`; for a path it defaults to the file's name.

    Raises :class:`CsvError` when the file is unreadable, larger than
    :data:`MAX_CSV_BYTES`, not UTF-8, has no Revolut header (or more than
    :data:`MAX_COLUMNS` columns, or a duplicated known column), is malformed CSV, mixes
    currencies, or has an imported row whose Currency is not a three-letter code.
    Row-level problems never raise; they are reported in ``skipped``.
    """
    data, name = _read_source(source, filename)
    text = _decode(data)
    reader = csv.DictReader(io.StringIO(text, newline=None))
    builder = _ExportBuilder()
    header_read = False
    try:
        _check_header(reader.fieldnames)
        header_read = True
        for line_no, raw in enumerate(reader, start=1):
            builder.add(line_no, raw)
    except csv.Error as exc:
        # The physical line drifts from the record number when quoted fields span lines,
        # so name both. DictReader.line_num is only updated after a row parses, hence the
        # underlying reader's counter.
        file_line = reader.reader.line_num
        where = (
            f"data row {builder.data_rows + 1}, file line {file_line}"
            if header_read
            else f"header, file line {file_line}"
        )
        raise CsvError(
            f"malformed CSV ({where}): {exc}",
            user_message=f"The statement is not a valid CSV file ({where}).",
        ) from exc
    return builder.finish(
        sha256=hashlib.sha256(data).hexdigest(),
        filename_window=parse_filename_window(name),
    )


def _read_source(
    source: Path | os.PathLike[str] | str | bytes | bytearray | memoryview,
    filename: str | None,
) -> tuple[bytes, str | None]:
    if isinstance(source, bytes | bytearray | memoryview):
        data = bytes(source)
        _check_size(len(data))
        return data, filename
    path = Path(source)
    try:
        with path.open("rb") as fh:
            _check_size(os.fstat(fh.fileno()).st_size)
            # Read one byte past the limit: the file may have grown since fstat.
            data = fh.read(MAX_CSV_BYTES + 1)
    except OSError as exc:
        raise CsvError(
            f"cannot read statement {path.name!r}: {exc.strerror or exc}",
            user_message="The statement file could not be read.",
        ) from exc
    _check_size(len(data))
    return data, filename if filename is not None else path.name


def _check_size(size: int) -> None:
    if size > MAX_CSV_BYTES:
        raise CsvError(
            f"statement is {size} bytes, limit is {MAX_CSV_BYTES}",
            user_message="The statement is larger than 25 MB.",
        )


def _decode(data: bytes) -> str:
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvError(
            f"statement is not UTF-8 (byte offset {exc.start})",
            user_message="The statement is not a UTF-8 Revolut CSV export.",
        ) from exc


def _check_header(fieldnames: list[str] | None) -> None:
    """Validate the header row; every check is linear in the number of columns."""
    if not fieldnames:
        raise CsvError("statement is empty", user_message="The statement file is empty.")
    if len(fieldnames) > MAX_COLUMNS:
        raise CsvError(
            f"statement header has {len(fieldnames)} columns, limit is {MAX_COLUMNS}",
            user_message=(
                "This does not look like a Revolut account statement "
                f"(the header has more than {MAX_COLUMNS} columns)."
            ),
        )
    missing = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
    if missing:
        raise CsvError(
            f"not a Revolut statement: missing column(s) {', '.join(missing)}",
            user_message=(
                "This does not look like a Revolut account statement "
                f"(missing column(s): {', '.join(missing)})."
            ),
        )
    # Only columns the parser reads matter; DictReader would silently keep the last one.
    counts = Counter(fieldnames)
    duplicated = sorted(name for name in KNOWN_COLUMNS if counts[name] > 1)
    if duplicated:
        raise CsvError(
            f"duplicate column(s) {', '.join(duplicated)}",
            user_message="The statement has duplicate column names.",
        )


def _parse_timestamp(text: str) -> tuple[datetime, bool] | None:
    """Parse a Revolut timestamp with v1's two formats; returns (value, has_time)."""
    try:
        return datetime.strptime(text, _TIMESTAMP_FORMAT), True
    except ValueError:
        pass
    try:
        return datetime.strptime(text, _DATE_FORMAT), False
    except ValueError:
        return None


def _clip(value: str, limit: int = 40) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


class _ExportBuilder:
    """Accumulates rows in file order; owns the v1 occurrence counter."""

    def __init__(self) -> None:
        self.rows: list[RevolutRow] = []
        self.skipped: list[SkippedRow] = []
        self.other_products: Counter[str] = Counter()
        self.warnings: list[str] = []
        self._suppressed_warnings = 0
        self._occurrences: Counter[str] = Counter()
        self._data_rows = 0

    @property
    def data_rows(self) -> int:
        """Data rows processed so far (every product)."""
        return self._data_rows

    def add(self, line_no: int, raw: dict[str | None, str | None]) -> None:
        """Process one ``csv.DictReader`` row (same check order as v1).

        Raises :class:`CsvError` for an imported row whose Currency is malformed: the
        file's currency must be checked against the budget, so it cannot be guessed.
        """
        self._data_rows += 1
        product = (raw.get("Product") or "").strip()
        if product and product not in TRACKED_PRODUCTS:
            self.other_products[product] += 1
            return
        if any(column in raw and raw[column] is None for column in _STRICT_COLUMNS):
            self._skip(line_no, SKIP_INCOMPLETE, "row has fewer columns than the header")
            return
        started = self._parse_started(line_no, raw["Started Date"] or "")
        if started is None:
            return
        started_at, has_time = started
        amounts = self._parse_amounts(line_no, raw)
        if amounts is None:
            return
        amount_units, fee, milli = amounts

        iso_date = started_at.strftime(_DATE_FORMAT)
        # From here on the row consumes an occurrence number, exactly as in v1, even if
        # v2 then refuses it (implausible values, non-allowlisted state).
        occurrence_key = f"{milli}:{iso_date}"
        self._occurrences[occurrence_key] += 1
        key = f"YNAB:{occurrence_key}:{self._occurrences[occurrence_key]}"

        raw_state = (raw["State"] or "").strip()
        state = raw_state.upper()
        if not self._acceptable(line_no, key, started_at, amount_units, fee, state):
            return

        currency = _parse_currency(line_no, raw.get("Currency"))
        description = (raw["Description"] or "").strip()
        tx_type_cell = raw.get("Type", "Unknown")  # v1: unstripped fallback payee
        tx_type = (raw.get("Type") or "").strip()
        self.rows.append(
            RevolutRow(
                line_no=line_no,
                key=key,
                type=tx_type,
                product=product,
                description=description,
                payee=(description or tx_type_cell or "")[:100],
                started_at=started_at,
                completed_at=self._parse_completed(line_no, raw.get("Completed Date")),
                date=iso_date,
                amount=milli,
                fee=fee,
                currency=currency,
                state=state,
                balance=self._parse_balance(line_no, raw.get("Balance")),
                memo=_legacy_memo(tx_type, raw_state, fee),
                started_has_time=has_time,
            )
        )

    # -- field parsing -------------------------------------------------------

    def _parse_started(self, line_no: int, cell: str) -> tuple[datetime, bool] | None:
        text = cell.strip()
        if not text:
            self._skip(line_no, SKIP_MISSING_DATE, "Started Date is empty")
            return None
        parsed = _parse_timestamp(text)
        if parsed is None:
            self._skip(line_no, SKIP_INVALID_DATE, f"unparseable Started Date {_clip(text)!r}")
        return parsed

    def _parse_amounts(
        self, line_no: int, raw: dict[str | None, str | None]
    ) -> tuple[float, float, int] | None:
        """Return (amount, fee, milliunits) with v1's float arithmetic, or None if skipped."""
        amount_cell = raw["Amount"] or ""
        try:
            amount = float(amount_cell)
        except ValueError:
            self._skip(line_no, SKIP_INVALID_AMOUNT, f"invalid Amount {_clip(amount_cell)!r}")
            return None
        fee_cell = raw.get("Fee", "0") or "0"
        try:
            fee = float(fee_cell)
        except ValueError:
            # v1 silently treated an unparseable Fee as 0; keep that (the key depends on
            # it) but surface it.
            fee = 0.0
            self._warn(f"data row {line_no}: unparseable Fee {_clip(fee_cell)!r} treated as 0")
        net = amount - fee if fee else amount
        scaled = net * 1000
        if not (math.isfinite(amount) and math.isfinite(fee) and math.isfinite(scaled)):
            self._skip(line_no, SKIP_NON_FINITE, "Amount or Fee is not a finite number")
            return None
        # Built-in round() of a float: banker's rounding on the binary value, as v1.
        return amount, fee, round(scaled)

    def _acceptable(
        self,
        line_no: int,
        key: str,
        started_at: datetime,
        amount: float,
        fee: float,
        state: str,
    ) -> bool:
        if started_at.year < MIN_YEAR:
            self._skip(
                line_no, SKIP_IMPLAUSIBLE_DATE, f"implausible date {started_at:%Y-%m-%d}", key
            )
            return False
        if max(abs(amount), abs(fee), abs(amount - fee)) > MAX_ABS_AMOUNT:
            self._skip(
                line_no,
                SKIP_ABSURD_AMOUNT,
                f"amount out of range (more than {MAX_ABS_AMOUNT:,})",
                key,
            )
            return False
        if state not in STATE_ALLOWLIST:
            shown = _clip(state) if state else "(empty)"
            self._skip(
                line_no,
                SKIP_UNSUPPORTED_STATE,
                f"state {shown} is not imported (only COMPLETED, PENDING and REVERTED are)",
                key,
            )
            return False
        return True

    def _parse_completed(self, line_no: int, cell: str | None) -> datetime | None:
        text = (cell or "").strip()
        if not text:
            return None
        parsed = _parse_timestamp(text)
        if parsed is None:
            self._warn(f"data row {line_no}: unparseable Completed Date {_clip(text)!r} ignored")
            return None
        return parsed[0]

    def _parse_balance(self, line_no: int, cell: str | None) -> int | None:
        text = (cell or "").strip()
        if not text:
            return None
        try:
            value = float(text)
        except ValueError:
            self._warn(f"data row {line_no}: unparseable Balance {_clip(text)!r} ignored")
            return None
        scaled = value * 1000
        if not math.isfinite(scaled) or abs(value) >= MAX_ABS_BALANCE:
            self._warn(f"data row {line_no}: Balance {_clip(text)!r} out of range ignored")
            return None
        return round(scaled)  # same float formula as amounts

    # -- bookkeeping -----------------------------------------------------------

    def _skip(self, line_no: int, code: str, reason: str, key: str | None = None) -> None:
        self.skipped.append(SkippedRow(line_no=line_no, reason=reason, code=code, key=key))

    def _warn(self, message: str) -> None:
        if len(self.warnings) < _MAX_WARNINGS:
            self.warnings.append(message)
        else:
            self._suppressed_warnings += 1

    def finish(self, *, sha256: str, filename_window: tuple[str, str] | None) -> ParsedExport:
        currency = _single_currency(self.rows)
        dates = [row.date for row in self.rows]
        warnings = list(self.warnings)
        if self._suppressed_warnings:
            warnings.append(f"… and {self._suppressed_warnings} more warning(s)")
        if self.other_products:
            # Counts only: payees and amounts never go to the log (F92).
            log.info(
                "revolut export: ignored other products (%s)",
                ", ".join(f"{k}={v}" for k, v in sorted(self.other_products.items())),
            )
        if self.skipped:
            log.info(
                "revolut export: skipped %d row(s) (%s)",
                len(self.skipped),
                ", ".join(
                    f"{k}={v}" for k, v in sorted(Counter(s.code for s in self.skipped).items())
                ),
            )
        return ParsedExport(
            rows=self.rows,
            currency=currency,
            window=(min(dates), max(dates)) if dates else None,
            skipped=self.skipped,
            other_products=dict(sorted(self.other_products.items())),
            filename_window=filename_window,
            sha256=sha256,
            warnings=warnings,
            data_rows=self._data_rows,
        )


def _legacy_memo(tx_type: str, raw_state: str, fee: float) -> str | None:
    """v1 memo: ``Type`` (unless Card Payment) | ``(pending)`` | ``Fee: {fee}``, [:200].

    ``raw_state`` is the stripped but *not* upper-cased State: v1 compared it
    case-sensitively, and the memo must stay byte-identical.
    """
    parts: list[str] = []
    if tx_type and tx_type != "Card Payment":
        parts.append(tx_type)
    if raw_state == STATE_PENDING:
        parts.append(PENDING_MARKER)
    if fee:
        parts.append(f"Fee: {fee}")
    return " | ".join(parts)[:200] or None


def _parse_currency(line_no: int, cell: str | None) -> str:
    """Upper-cased three-letter Currency of an imported row, or ``""`` if absent/empty.

    Anything else raises :class:`CsvError`, quoting the value clipped: a statement with
    an unknown currency cannot be checked against the budget currency (F14).
    """
    currency = (cell or "").strip().upper()
    if currency and not _CURRENCY_RE.fullmatch(currency):
        shown = _clip((cell or "").strip(), 12)
        raise CsvError(
            f"invalid Currency {shown!r} in data row {line_no}",
            user_message=(
                f"The statement has an invalid Currency value {shown!r} in data row "
                f"{line_no}; expected a three-letter code such as CHF."
            ),
        )
    return currency


def _single_currency(rows: list[RevolutRow]) -> str | None:
    """The one currency of ``rows``; :class:`CsvError` if they disagree (F14)."""
    currencies = {row.currency for row in rows}
    if len(currencies) > 1:
        names = sorted(c or "(none)" for c in currencies)
        shown = ", ".join(names[:_MAX_CURRENCIES_SHOWN])
        if len(names) > _MAX_CURRENCIES_SHOWN:
            shown += f" and {len(names) - _MAX_CURRENCIES_SHOWN} more"
        raise CsvError(
            f"statement mixes currencies: {shown}",
            user_message=(
                f"This statement mixes currencies ({shown}). "
                "Export each Revolut currency account separately."
            ),
        )
    only = next(iter(currencies), "")
    return only or None
