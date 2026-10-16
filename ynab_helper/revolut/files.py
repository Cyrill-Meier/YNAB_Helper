"""Statement files on disk (docs/DESIGN.md §7 import_csv, §3 revolut/files.py).

Uploaded statements are stored under a **server-generated** name
``<UTC stamp>_<8 hex>.csv`` inside the user's ``csv_cache/<key>/`` directory. The name a
user (or a Telegram client) supplied is never used to build a path; it is kept only as
sanitised display metadata (F19). Files are written atomically (temp file in the same
directory, ``fsync``, ``os.replace``, directory ``fsync``) so a concurrent reader never
sees a half-written statement (F83).

Content is fully parsed *before* anything in the cache is touched (F84): an upload that
:func:`~ynab_helper.revolut.parser.parse_export` rejects is never stored and never prunes
anything. Retention keeps the newest :data:`KEEP_STORED` statements plus any ``protect``
paths. A statement that parses but is refused later (budget currency mismatch, YNAB
failure, no importable rows) does count toward retention, so callers pass the newest
successfully imported statement as ``protect`` to keep the reconcile basis on disk.

The CLI helpers (:func:`find_latest_revolut_csv`, :func:`parse_filename_window`) keep v1's
semantics for picking the newest export in a download folder.

Callers are expected to hold the per-user operation lock (``locks.py``) around
:func:`store_upload` so that pruning never races another operation of the same user.
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
import secrets
import stat
import time
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

from ynab_helper.errors import CsvError, YnabHelperError
from ynab_helper.revolut.parser import (
    HEADER_SNIFF_BYTES,
    MAX_CSV_BYTES,
    ParsedExport,
    looks_like_revolut_csv,
    parse_export,
    parse_filename_window,
)
from ynab_helper.timeutil import utc_stamp

__all__ = [
    "DISPLAY_NAME_MAX",
    "KEEP_STORED",
    "REVOLUT_FILENAME_RE",
    "STORED_NAME_RE",
    "StoredCsv",
    "find_latest_revolut_csv",
    "is_revolut_csv",
    "latest_stored",
    "list_stored",
    "parse_filename_window",
    "prune_stored",
    "sanitize_display_name",
    "store_upload",
]

log = logging.getLogger(__name__)

#: Statements kept per user directory by :func:`store_upload`.
KEEP_STORED = 5

#: Longest display name kept as metadata (code points).
DISPLAY_NAME_MAX = 120

#: Names produced by :func:`store_upload`: ``20261016T183000Z_1a2b3c4d.csv``.
STORED_NAME_RE = re.compile(r"^(?P<stamp>\d{8}T\d{6}Z)_(?P<rand>[0-9a-f]{8})\.csv$")

#: Revolut download name; the second date is the export's "to" date (v1 CLI ranking).
REVOLUT_FILENAME_RE = re.compile(
    r"^account-statement_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_.*\.csv$",
    re.IGNORECASE,
)

_TMP_PREFIX = ".upload-"
_TMP_SUFFIX = ".tmp"
#: Temp files older than this are leftovers of a crashed write and are removed on prune.
_STALE_TMP_SECONDS = 3600
_NAME_ATTEMPTS = 8
#: Unicode categories dropped from display names: controls (incl. NUL), format characters
#: (bidi overrides, zero-width), surrogates, private use and unassigned code points.
_DROPPED_CATEGORIES = frozenset({"Cc", "Cf", "Cs", "Co", "Cn"})
_SAVE_FAILED = "The statement could not be saved on the server."

#: Paths accepted as ``protect``: one path or several.
ProtectedPaths = str | os.PathLike[str] | Iterable[str | os.PathLike[str]] | None


@dataclass(frozen=True, slots=True)
class StoredCsv:
    """A statement written by :func:`store_upload`.

    ``export`` is the parse of exactly the stored bytes (with ``original_name`` as the
    filename), so callers need not parse the file again.
    """

    path: Path  # csv_dir / "<UTC stamp>_<8 hex>.csv"
    sha256: str  # hex digest of the stored bytes
    original_name: str | None  # sanitised display name (metadata only, never a path)
    size: int  # bytes
    export: ParsedExport = field(repr=False, compare=False)


# ─── Display names ───────────────────────────────────────────────────────────


def sanitize_display_name(name: str | None, *, max_length: int = DISPLAY_NAME_MAX) -> str | None:
    """Return a safe, short display form of a user-supplied file name, or None.

    Keeps only the last path component (``/`` and ``\\`` both separate), drops control,
    format (bidi overrides, zero-width), surrogate, private-use and unassigned
    characters, collapses whitespace, and truncates to ``max_length`` code points while
    keeping a short extension. The result is display metadata: it must still be escaped
    when rendered and must never be used to build a filesystem path.
    """
    if name is None:
        return None
    if max_length < 8:
        raise ValueError("max_length must be at least 8")
    text = unicodedata.normalize("NFC", str(name))
    text = re.split(r"[\\/]", text)[-1]
    kept = []
    for char in text:
        category = unicodedata.category(char)
        if category in _DROPPED_CATEGORIES:
            continue
        kept.append(" " if category.startswith("Z") else char)
    text = " ".join("".join(kept).split())
    if text in ("", ".", ".."):
        return None
    if len(text) <= max_length:
        return text
    stem, dot, ext = text.rpartition(".")
    if dot and stem and 0 < len(ext) <= 10:
        return stem[: max_length - len(ext) - 2] + "…." + ext
    return text[: max_length - 1] + "…"


# ─── Storing uploads ─────────────────────────────────────────────────────────


def store_upload(
    csv_dir: Path,
    content: bytes,
    original_name: str | None,
    *,
    keep: int = KEEP_STORED,
    protect: ProtectedPaths = (),
) -> StoredCsv:
    """Validate, parse and atomically store an uploaded statement, then apply retention.

    The file is written to ``csv_dir/<UTC stamp>_<8 hex>.csv`` (mode 0600, directory
    created with mode 0700 if missing). ``original_name`` is only sanitised and returned
    as metadata. Nothing in ``csv_dir`` is modified unless ``content`` is non-empty, at
    most :data:`MAX_CSV_BYTES`, starts with a Revolut header and parses
    (:func:`~ynab_helper.revolut.parser.parse_export`); the parse is returned as
    :attr:`StoredCsv.export`.

    Retention then keeps the newest ``keep`` statements, the new file, and every path in
    ``protect`` (pass the newest successfully imported statement so that refused uploads
    cannot evict it).

    Raises :class:`CsvError` for invalid content and :class:`YnabHelperError` when the
    file cannot be written (the partial temp file is removed). Retention failures are
    logged, never raised: the statement is stored at that point.
    """
    if keep < 1:
        raise ValueError("keep must be >= 1")
    data = bytes(content)
    display_name = sanitize_display_name(original_name)
    export = _validate_upload(data, display_name)
    csv_dir = Path(csv_dir)
    try:
        csv_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        final = _unused_stored_path(csv_dir)
        _atomic_write(final, data)
    except OSError as exc:
        raise YnabHelperError(
            f"cannot store statement in {csv_dir}: {exc.strerror or exc}",
            user_message=_SAVE_FAILED,
        ) from exc
    stored = StoredCsv(
        path=final,
        sha256=export.sha256,
        original_name=display_name,
        size=len(data),
        export=export,
    )
    log.info("stored statement %s (%d bytes)", final.name, stored.size)
    try:
        prune_stored(csv_dir, keep=keep, protect=[final, *_as_paths(protect)])
    except OSError:
        log.warning("could not prune %s", csv_dir, exc_info=True)
    return stored


def _validate_upload(data: bytes, display_name: str | None) -> ParsedExport:
    """Reject content that cannot be imported; return its parse (F84).

    The cheap checks run first so that a non-statement (an image, a PDF, UTF-16) gets a
    specific message without being decoded.
    """
    if not data:
        raise CsvError("statement is empty", user_message="The statement file is empty.")
    if len(data) > MAX_CSV_BYTES:
        raise CsvError(
            f"statement is {len(data)} bytes, limit is {MAX_CSV_BYTES}",
            user_message="The statement is larger than 25 MB.",
        )
    if not looks_like_revolut_csv(data[:HEADER_SNIFF_BYTES]):
        raise CsvError(
            "content does not start with a Revolut statement header",
            user_message=(
                "This does not look like a Revolut account statement (CSV export with "
                "Started Date, Completed Date, Description, Amount and State columns)."
            ),
        )
    return parse_export(data, filename=display_name)


def _unused_stored_path(csv_dir: Path) -> Path:
    for _ in range(_NAME_ATTEMPTS):
        candidate = csv_dir / f"{utc_stamp()}_{secrets.token_hex(4)}.csv"
        _ensure_direct_child(candidate, csv_dir)
        if not os.path.lexists(candidate):
            return candidate
    raise FileExistsError(f"no unused statement name in {csv_dir}")


def _ensure_direct_child(path: Path, directory: Path) -> None:
    """Invariant (F19): stored files live directly inside ``directory``.

    Only a bug (e.g. a changed ``utc_stamp`` format) can trip this. The server path goes
    to the log, never into ``user_message``.
    """
    if path.parent != directory or not STORED_NAME_RE.match(path.name):
        log.error("refusing to store statement as %r in %s", path.name, directory)
        raise YnabHelperError(
            f"refusing to write outside {directory}: {path.name!r}",
            user_message=_SAVE_FAILED,
        )


def _atomic_write(final: Path, data: bytes) -> None:
    """Write ``data`` to a temp file next to ``final``, fsync, and rename over ``final``."""
    tmp = final.parent / f"{_TMP_PREFIX}{secrets.token_hex(8)}{_TMP_SUFFIX}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(tmp, flags | getattr(os, "O_CLOEXEC", 0), 0o600)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, final)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise
    _fsync_directory(final.parent)


def _fsync_directory(directory: Path) -> None:
    """Persist the rename itself; not supported on every platform/filesystem."""
    try:
        dir_fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    except OSError:
        pass
    finally:
        os.close(dir_fd)


# ─── Listing and retention ───────────────────────────────────────────────────


def list_stored(csv_dir: Path) -> list[Path]:
    """Statements in ``csv_dir``, newest first.

    Files named by :func:`store_upload` are ranked by their UTC stamp (then mtime, then
    name). Any other ``*.csv`` regular file — e.g. a v1 upload kept under its original
    name — ranks as older than every v2 file, by mtime. Hidden files, symlinks,
    directories and non-CSV files are ignored (and never pruned).
    """
    csv_dir = Path(csv_dir)
    ranked: list[tuple[tuple[int, str, int, str], Path]] = []
    try:
        with os.scandir(csv_dir) as scan:
            entries = list(scan)
    except (FileNotFoundError, NotADirectoryError):
        return []
    for entry in entries:
        name = entry.name
        if name.startswith(".") or not name.lower().endswith(".csv"):
            continue
        try:
            info = entry.stat(follow_symlinks=False)
        except OSError:
            continue
        if not stat.S_ISREG(info.st_mode):
            continue
        match = STORED_NAME_RE.match(name)
        stamp = match.group("stamp") if match else ""
        # v2 names rank above every v1 name; then by stamp, mtime and name.
        rank = (1 if match else 0, stamp, info.st_mtime_ns, name)
        ranked.append((rank, csv_dir / name))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [path for _, path in ranked]


def latest_stored(csv_dir: Path) -> Path | None:
    """The newest statement in ``csv_dir`` (see :func:`list_stored`), or None."""
    stored = list_stored(csv_dir)
    return stored[0] if stored else None


def prune_stored(
    csv_dir: Path, *, keep: int = KEEP_STORED, protect: ProtectedPaths = None
) -> list[Path]:
    """Delete all but the newest ``keep`` statements and stale temp files; return removed.

    ``protect`` (one path or several, e.g. the file just written and the newest imported
    statement) is never deleted, whatever its rank, and does not use up a ``keep`` slot
    unless it is among the newest ``keep``. Paths are compared after resolving their
    directory, so relative and absolute spellings match.
    """
    if keep < 1:
        raise ValueError("keep must be >= 1")
    csv_dir = Path(csv_dir)
    protected = {_canonical(path) for path in _as_paths(protect)}
    removed: list[Path] = []
    for path in list_stored(csv_dir)[keep:]:
        if _canonical(path) in protected:
            continue
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
            removed.append(path)
    removed.extend(_remove_stale_temp_files(csv_dir))
    if removed:
        log.info("pruned %d old statement file(s) in %s", len(removed), csv_dir)
    return removed


def _as_paths(protect: ProtectedPaths) -> list[Path]:
    if protect is None:
        return []
    if isinstance(protect, str | os.PathLike):
        return [Path(protect)]
    return [Path(path) for path in protect]


def _canonical(path: Path) -> Path:
    """``path`` with its directory resolved (the file itself may be gone or a symlink)."""
    return Path(os.path.realpath(path.parent)) / path.name


def _remove_stale_temp_files(csv_dir: Path) -> list[Path]:
    removed: list[Path] = []
    cutoff = time.time() - _STALE_TMP_SECONDS
    try:
        with os.scandir(csv_dir) as scan:
            entries = list(scan)
    except (FileNotFoundError, NotADirectoryError):
        return removed
    for entry in entries:
        if not (entry.name.startswith(_TMP_PREFIX) and entry.name.endswith(_TMP_SUFFIX)):
            continue
        try:
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISREG(info.st_mode) and info.st_mtime < cutoff:
                os.unlink(entry.path)
                removed.append(Path(entry.path))
        except FileNotFoundError:
            continue
    return removed


# ─── CLI discovery (v1 semantics) ────────────────────────────────────────────


def is_revolut_csv(path: Path | str) -> bool:
    """True if ``path`` is a ``*.csv`` file whose content starts with a Revolut header.

    v1 also accepted any name containing "revolut" or "account-statement" without
    looking at the content (F84); v2 always checks the header.
    """
    path = Path(path)
    if not path.name.lower().endswith(".csv"):
        return False
    try:
        with path.open("rb") as fh:
            head = fh.read(HEADER_SNIFF_BYTES)
    except OSError:
        return False
    return looks_like_revolut_csv(head)


def find_latest_revolut_csv(directory: Path | str) -> Path | None:
    """Return the newest Revolut export in ``directory`` (not recursive), or None.

    v1 ranking: files named ``account-statement_<from>_<to>_….csv`` rank by the ``to``
    date (the export date); other CSVs qualify only if their header is a Revolut header
    and rank below every dated name. Ties are broken by newest mtime.
    """
    directory = Path(str(directory)).expanduser()
    if not directory.is_dir():
        return None
    candidates: list[tuple[str, float, Path]] = []
    for path in directory.iterdir():
        if not path.is_file():
            continue
        match = REVOLUT_FILENAME_RE.match(path.name)
        if match:
            export_to = match.group(2)
        elif is_revolut_csv(path):
            export_to = ""  # unknown: ranks after every dated name, then by mtime
        else:
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError:
            mtime = 0.0
        candidates.append((export_to, mtime, path))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return candidates[0][2]
