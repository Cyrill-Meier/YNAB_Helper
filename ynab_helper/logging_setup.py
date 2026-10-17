"""Process-wide logging configuration (docs/DESIGN.md §4.6).

* Root logger → stderr (always resolves the *current* ``sys.stderr``) and, optionally, a
  size-rotated log file (5 MB × 5 backups; v1's plain FileHandler grew forever, F110).
* ``ynab_helper.audit`` carries one INFO line per YNAB write; it is pinned at INFO and
  propagates, so the audit trail in bot.log survives ``LOG_LEVEL=WARNING``.
* Registered secrets (bot token, YNAB token, …) are masked in every formatted line,
  including tracebacks, as a last line of defence.
* :func:`setup_logging` is idempotent: it replaces only the handlers it installed itself
  and never touches handlers added by others (e.g. pytest's caplog).
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from collections.abc import Iterable
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Final, TextIO

from ynab_helper.config import normalize_log_level

AUDIT_LOGGER: Final[str] = "ynab_helper.audit"
LOG_FORMAT: Final[str] = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
MAX_BYTES: Final[int] = 5 * 1024 * 1024
BACKUP_COUNT: Final[int] = 5
NOISY_LOGGERS: Final[tuple[str, ...]] = (
    "python_multipart",
    "multipart",
    "urllib3",
    "asyncio",
    "uvicorn.access",
)
REDACTED: Final[str] = "[REDACTED]"
# Shorter values are too likely to occur in ordinary text to be masked safely.
MIN_SECRET_LENGTH: Final[int] = 8

_OWNED_MARK: Final[str] = "_ynab_helper_owned"
_setup_lock = threading.Lock()
_secrets_lock = threading.Lock()
_secrets: set[str] = set()
# Longest first so a secret that contains another one is masked as a whole. Replaced (never
# mutated) under _secrets_lock, so formatters can read it without locking.
_secrets_ordered: tuple[str, ...] = ()


class _CurrentStderrHandler(logging.StreamHandler):  # type: ignore[type-arg]
    """StreamHandler bound to whatever ``sys.stderr`` is at emit time.

    Binding the stream once breaks when stderr is replaced later (pytest capture, daemon
    wrappers) and leads to writes into closed streams.
    """

    @property  # type: ignore[override]
    def stream(self) -> TextIO:
        return sys.stderr

    @stream.setter
    def stream(self, _value: TextIO) -> None:
        # StreamHandler.__init__ assigns the stream; the property always wins.
        pass


class _RedactingFormatter(logging.Formatter):
    """Masks registered secrets in the fully formatted line (message and traceback)."""

    def format(self, record: logging.LogRecord) -> str:
        return redact(super().format(record))


def redact(text: str) -> str:
    """Replace every registered secret in ``text`` with ``[REDACTED]``."""
    for secret in _secrets_ordered:
        if secret in text:
            text = text.replace(secret, REDACTED)
    return text


def add_redactions(values: Iterable[str]) -> None:
    """Register secret values to mask in all log output from now on.

    Values shorter than :data:`MIN_SECRET_LENGTH` characters are ignored (masking them
    would corrupt ordinary log text); surrounding whitespace is stripped.
    """
    global _secrets_ordered
    cleaned = {value.strip() for value in values if isinstance(value, str)}
    with _secrets_lock:
        _secrets.update(value for value in cleaned if len(value) >= MIN_SECRET_LENGTH)
        _secrets_ordered = tuple(sorted(_secrets, key=len, reverse=True))


def clear_redactions() -> None:
    """Forget all registered secrets (tests)."""
    global _secrets_ordered
    with _secrets_lock:
        _secrets.clear()
        _secrets_ordered = ()


def resolve_level(level: str | int | None) -> tuple[int, bool]:
    """Map a level name/number to a logging level; returns ``(level, valid)``.

    Names are interpreted by :func:`ynab_helper.config.normalize_log_level` (the same rule
    as ``Settings.log.level``: case-insensitive, WARN/FATAL aliases, NOTSET rejected).
    None or ``""`` mean INFO. Unknown values resolve to INFO with ``valid=False`` so
    callers can warn.
    """
    if isinstance(level, bool):
        return logging.INFO, False
    if isinstance(level, int):
        return (level, True) if level >= 0 else (logging.INFO, False)
    name = normalize_log_level(level or "INFO")
    if name is None:
        return logging.INFO, False
    return logging.getLevelNamesMapping()[name], True


def setup_logging(
    level: str | int | None = "INFO",
    log_file: str | os.PathLike[str] | None = None,
    *,
    console: bool = True,
    console_level: str | int | None = None,
    secrets: Iterable[str] = (),
) -> Path | None:
    """Configure the root logger; safe to call repeatedly.

    Args:
        level: root level name (``DEBUG``/``INFO``/…) or number; unknown → INFO + warning.
        log_file: file for the rotating handler; None or ``""`` disables file logging.
            ``~`` is expanded and the parent directory is created.
        console: add the stderr handler (default True).
        console_level: separate threshold for stderr (e.g. ``WARNING`` for the CLI, whose
            stdout/stderr is for humans); defaults to no extra filtering.
        secrets: values to mask in every log line (see :func:`add_redactions`).

    Returns:
        The absolute path of the log file in use, or None when file logging is disabled
        or the file could not be opened (a warning is logged; the process keeps running).
    """
    add_redactions(secrets)
    with _setup_lock:
        return _configure(level, log_file, console=console, console_level=console_level)


def _configure(
    level: str | int | None,
    log_file: str | os.PathLike[str] | None,
    *,
    console: bool,
    console_level: str | int | None,
) -> Path | None:
    root_level, level_ok = resolve_level(level)
    formatter = _RedactingFormatter(LOG_FORMAT, DATE_FORMAT)

    root = logging.getLogger()
    _remove_owned_handlers(root)
    root.setLevel(root_level)

    if console:
        stderr_handler = _CurrentStderrHandler()
        stderr_handler.setFormatter(formatter)
        if console_level is not None:
            stderr_handler.setLevel(resolve_level(console_level)[0])
        _own(stderr_handler)
        root.addHandler(stderr_handler)

    for name in NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)

    audit = logging.getLogger(AUDIT_LOGGER)
    audit.setLevel(logging.INFO)
    audit.propagate = True

    log = logging.getLogger(__name__)
    if not level_ok:
        log.warning("unknown log level %r; using INFO", level)

    path = _open_file_handler(root, log_file, formatter)
    if path is not None:
        log.info("logging to %s (level %s)", path, logging.getLevelName(root_level))
    return path


def _open_file_handler(
    root: logging.Logger, log_file: str | os.PathLike[str] | None, formatter: logging.Formatter
) -> Path | None:
    raw = os.fspath(log_file).strip() if log_file is not None else ""
    if not raw:
        return None
    path = Path(raw).expanduser().absolute()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            path, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8"
        )
    except OSError as exc:
        logging.getLogger(__name__).warning(
            "cannot open log file %s (%s); logging to stderr only",
            path,
            exc.strerror or exc,
        )
        return None
    handler.setFormatter(formatter)
    _own(handler)
    root.addHandler(handler)
    return path


def _own(handler: logging.Handler) -> None:
    setattr(handler, _OWNED_MARK, True)


def _remove_owned_handlers(root: logging.Logger) -> None:
    for handler in list(root.handlers):
        if getattr(handler, _OWNED_MARK, False):
            root.removeHandler(handler)
            handler.close()


def current_log_file() -> Path | None:
    """Path of the rotating log file installed by :func:`setup_logging`, if any.

    Used by the admin ``/logs`` command (v1 looked up the FileHandler's baseFilename).
    """
    for handler in logging.getLogger().handlers:
        if getattr(handler, _OWNED_MARK, False) and isinstance(handler, RotatingFileHandler):
            return Path(handler.baseFilename)
    return None


def reset_logging() -> None:
    """Remove the handlers installed by :func:`setup_logging` (tests, shutdown)."""
    with _setup_lock:
        _remove_owned_handlers(logging.getLogger())
