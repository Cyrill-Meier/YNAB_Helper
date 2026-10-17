"""Configuration: ``.env`` loading and typed, validated settings (docs/DESIGN.md §11).

Two rules shape this module:

* ``.env`` parsing keeps v1's semantics (production's ``bot.env`` is read by it; a parity
  test compares against the frozen v1 loader). Only failure modes differ: an unreadable or
  non-regular file is reported instead of silently ignored, a UTF-8 BOM no longer becomes
  part of the first key, and a NUL character skips that one variable instead of crashing.
* :meth:`Settings.from_env` never raises on malformed values. Problems are collected in
  ``Settings.problems`` and the entrypoints decide what is fatal (``validate_for_bot`` /
  ``validate_for_cli``). A typo in an optional variable must never take the bot down (F60).
"""

from __future__ import annotations

import logging
import os
import re
import stat
import threading
from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Final

log = logging.getLogger(__name__)

# The directory that contains the ``ynab_helper`` package: the repository root locally,
# ``/app`` in the container (where ``/app/.env`` and ``/app/bot_data`` are mounted).
REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
DEFAULT_ENV_FILE: Final[Path] = REPO_ROOT / ".env"
DEFAULT_DATA_DIR: Final[Path] = REPO_ROOT / "bot_data"

# '~' defaults are expanded when settings are built (not at import time) because the
# container entrypoint changes HOME after dropping privileges.
DEFAULT_LOG_FILE: Final[str] = "~/.revolut_to_ynab.log"
DEFAULT_CSV_DIR: Final[str] = "~/Downloads"
DEFAULT_CLI_DB_PATH: Final[str] = "~/.revolut_to_ynab.db"
DEFAULT_IBKR_BASE_URL: Final[str] = "https://localhost:5050"
DEFAULT_TELEGRAM_API_BASE: Final[str] = "https://api.telegram.org"
DEFAULT_LOG_LEVEL: Final[str] = "INFO"
DEFAULT_APP_ID: Final[int] = 10001
DEFAULT_BUILD_SHA: Final[str] = "dev"
DEFAULT_BUILD_DATE: Final[str] = "local"
# BUILD_SHA values that mean "not built from a known commit" (v1's Dockerfile used "local").
NON_RELEASE_SHAS: Final[frozenset[str]] = frozenset({DEFAULT_BUILD_SHA, "local"})

LOG_LEVELS: Final[tuple[str, ...]] = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
# Alternative spellings the logging module itself defines (v1 resolved LOG_LEVEL with
# getattr(logging, name), so both worked there).
LOG_LEVEL_ALIASES: Final[Mapping[str, str]] = MappingProxyType(
    {"WARN": "WARNING", "FATAL": "CRITICAL"}
)

# File names inside the data directory shared by several modules.
HEARTBEAT_FILENAME: Final[str] = ".heartbeat"
USERS_DB_FILENAME: Final[str] = "bot_users.db"
BACKUPS_DIRNAME: Final[str] = "backups"
CSV_CACHE_DIRNAME: Final[str] = "csv_cache"

# Upper bound for uid/gid values we are willing to switch to (uid_t is 32-bit; -1 is special).
_MAX_ID: Final[int] = 2**31 - 1
_ASCII_INT_RE: Final = re.compile(r"[+-]?[0-9]+")
_MAX_INT_DIGITS: Final[int] = 18
_TRUE_WORDS: Final = frozenset({"1", "true", "yes", "on"})
_FALSE_WORDS: Final = frozenset({"0", "false", "no", "off", ""})
_SHA_SAFE_RE: Final = re.compile(r"[^0-9A-Za-z]")
_SHA_LENGTH: Final[int] = 7
_UNPRINTABLE_RE: Final = re.compile(r"[^\x20-\x7e]")
_MAX_ECHO: Final[int] = 32
_ECHO_PREFIX: Final[int] = 8
_MAX_BUILD_DATE: Final[int] = 40

CLI_MODES: Final[frozenset[str]] = frozenset(
    {
        "import",
        "sync",
        "watch",
        "reconcile",
        "cleanup-pending-memos",
        "list-budgets",
        "list-accounts",
        "crypto-sync",
        "brokerage-sync",
        "db-stats",
    }
)


# ─── .env loading ────────────────────────────────────────────────────────────


def parse_dotenv(text: str) -> list[tuple[str, str]]:
    """Parse ``.env`` content into ``(key, value)`` pairs in file order.

    Semantics are exactly v1's ``_load_dotenv``: lines are stripped; blank lines and lines
    starting with ``#`` are ignored; an ``export `` prefix is removed; lines without ``=``
    are ignored; a value starting with ``'`` or ``"`` keeps what is inside the quotes
    (everything after the closing quote is discarded, an unterminated quote keeps the rest
    of the line); an unquoted value loses a trailing comment that starts with ``#`` at the
    beginning of the value or after whitespace. Duplicate keys are all returned; which one
    wins is decided by :func:`load_dotenv`.
    """
    pairs: list[tuple[str, str]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.lstrip()
        if value and value[0] in ("'", '"'):
            end = value.find(value[0], 1)
            value = value[1:end] if end != -1 else value[1:]
        else:
            value = _strip_inline_comment(value)
        if key:
            pairs.append((key, value))
    return pairs


def _strip_inline_comment(value: str) -> str:
    for i, char in enumerate(value):
        if char == "#" and (i == 0 or value[i - 1].isspace()):
            return value[:i].rstrip()
    return value.rstrip()


# Files this process has already loaded into os.environ. The container entrypoint reads
# the root-owned 0600 /app/.env before dropping privileges; when the bot later calls
# load_dotenv() again as uid 10001 the read fails, which is expected and must not produce
# a warning. Loads into other mappings are not remembered.
_loaded_files: set[str] = set()
_loaded_lock = threading.Lock()


class _NotARegularFileError(OSError):
    pass


def load_dotenv(
    path: str | os.PathLike[str] | None = None,
    override: bool = False,
    *,
    environ: MutableMapping[str, str] | None = None,
) -> bool:
    """Load ``KEY=VALUE`` pairs from a ``.env`` file into the environment.

    Args:
        path: the file to read; defaults to ``<repo root>/.env`` (``/app/.env`` in the
            container).
        override: when False (default) variables that already exist win over the file,
            and the first occurrence of a duplicate key in the file wins. When True the
            file wins and the last occurrence of a duplicate key wins (v1 semantics).
        environ: mapping to update instead of ``os.environ`` (tests, health check).

    Returns:
        True if the file was read (or had already been read by this process and is no
        longer readable after the privilege drop), False if it is missing or unreadable.
        A missing file is normal (logged at DEBUG); any other failure is logged as a
        warning and nothing is applied.
    """
    target: MutableMapping[str, str] = os.environ if environ is None else environ
    env_path = DEFAULT_ENV_FILE if path is None else Path(path)
    try:
        text = _read_env_file(env_path)
    except FileNotFoundError:
        log.debug("no .env file at %s", env_path)
        return False
    except PermissionError as exc:
        if not override and _was_loaded(env_path, target):
            log.debug("%s is no longer readable; it was loaded earlier by this process", env_path)
            return True
        log.warning("cannot read %s: %s; its settings are NOT loaded", env_path, _reason(exc))
        return False
    except UnicodeDecodeError as exc:
        log.warning(
            "cannot read %s: not valid UTF-8 (byte offset %d); its settings are NOT loaded",
            env_path,
            exc.start,
        )
        return False
    except OSError as exc:
        log.warning("cannot read %s: %s; its settings are NOT loaded", env_path, _reason(exc))
        return False

    for key, value in parse_dotenv(text):
        if not override and key in target:
            continue
        try:
            target[key] = value
        except ValueError:
            # os.environ rejects NUL characters; v1 crashed here. Never echo the value.
            name = key if "\x00" not in key else "<invalid name>"
            log.warning("ignoring %s from %s: it contains a NUL character", name, env_path)
    _mark_loaded(env_path, target)
    return True


def _read_env_file(path: Path) -> str:
    st = os.stat(path)
    if not stat.S_ISREG(st.st_mode):
        # Docker creates a directory at the mount point when the host file is missing.
        raise _NotARegularFileError("not a regular file")
    # utf-8-sig: a BOM would otherwise become part of the first key (a v1 bug).
    return path.read_bytes().decode("utf-8-sig")


def _reason(exc: OSError) -> str:
    return exc.strerror or str(exc) or type(exc).__name__


def _was_loaded(path: Path, target: MutableMapping[str, str]) -> bool:
    if target is not os.environ:
        return False
    with _loaded_lock:
        return os.path.realpath(path) in _loaded_files


def _mark_loaded(path: Path, target: MutableMapping[str, str]) -> None:
    if target is os.environ:
        with _loaded_lock:
            _loaded_files.add(os.path.realpath(path))


# ─── Value parsing helpers (shared with web/config.py) ───────────────────────


def parse_bool(raw: str | None) -> bool | None:
    """``1/true/yes/on`` → True, ``0/false/no/off``/empty → False, anything else → None."""
    if raw is None:
        return False
    word = str(raw).strip().lower()
    if word in _TRUE_WORDS:
        return True
    if word in _FALSE_WORDS:
        return False
    return None


def parse_int(
    raw: str | None,
    *,
    name: str,
    default: int,
    problems: list[str],
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    """Parse an ASCII integer leniently: blank → ``default``; malformed or out of range →
    a message appended to ``problems`` and ``default`` returned. Never raises."""
    text = "" if raw is None else str(raw).strip()
    if not text:
        return default
    value = _ascii_int(text)
    if value is None:
        problems.append(f"{name} must be an integer, got {_echo(text)}; using {default}.")
        return default
    if (minimum is not None and value < minimum) or (maximum is not None and value > maximum):
        bounds = _bounds_text(minimum, maximum)
        problems.append(f"{name} must be {bounds}, got {_echo(text)}; using {default}.")
        return default
    return value


def _ascii_int(text: str) -> int | None:
    """``int(text)`` for plain ASCII decimal integers of at most 18 digits, else None.

    ``int()`` alone would accept ``1_000`` and non-ASCII digits, and raises ValueError for
    strings longer than 4300 digits; 18 digits also keeps the value inside SQLite's int64.
    """
    if not _ASCII_INT_RE.fullmatch(text):
        return None
    digits = text.lstrip("+-").lstrip("0") or "0"
    if len(digits) > _MAX_INT_DIGITS:
        return None
    return -int(digits) if text.startswith("-") else int(digits)


def _bounds_text(minimum: int | None, maximum: int | None) -> str:
    if minimum is not None and maximum is not None:
        return f"between {minimum} and {maximum}"
    if minimum is not None:
        return f"at least {minimum}"
    return f"at most {maximum}"


def _echo(value: str) -> str:
    """Quote a malformed value for an error message: printable ASCII only, and long values
    are cut to a short prefix, in case a secret was pasted into the wrong variable."""
    shown = _UNPRINTABLE_RE.sub("?", value)
    if len(shown) > _MAX_ECHO:
        return f"{shown[:_ECHO_PREFIX]!r}… ({len(value)} characters)"
    return repr(shown)


def normalize_log_level(raw: str | None) -> str | None:
    """Canonical level name (one of :data:`LOG_LEVELS`) for ``raw``, or None if unknown.

    Case-insensitive; surrounding whitespace is ignored; ``WARN``/``FATAL`` are aliases.
    ``NOTSET`` is deliberately rejected: on the root logger it means "log everything",
    whereas v1 set it on a child logger, which then inherited root's WARNING. Guessing
    either meaning could hide or flood the audit log, so the caller falls back to INFO and
    reports the problem. Shared by :class:`Settings` and ``logging_setup.resolve_level`` so
    both always agree on what a value means.
    """
    if raw is None:
        return None
    name = str(raw).strip().upper()
    name = LOG_LEVEL_ALIASES.get(name, name)
    return name if name in LOG_LEVELS else None


def _get(environ: Mapping[str, str], key: str) -> str:
    value = environ.get(key)
    return "" if value is None else str(value).strip()


def _expand(path: str, environ: Mapping[str, str]) -> Path:
    """Expand a leading ``~`` using HOME from ``environ`` (falls back to the process).

    Never raises: if the user database lookup for ``~name`` fails, the path is kept as is.
    A HOME containing NUL (impossible in ``os.environ``) is ignored like a missing one.
    """
    home = environ.get("HOME")
    if home and "\x00" not in home and (path == "~" or path.startswith("~/")):
        return Path(home + path[1:])
    try:
        return Path(os.path.expanduser(path))
    except ValueError:
        # pwd lookups reject NUL characters; callers reject such paths anyway.
        return Path(path)


def _http_url(raw: str, *, name: str, default: str, problems: list[str]) -> str:
    if not raw:
        return default
    url = raw.rstrip("/")
    if not url.startswith(("http://", "https://")) or any(c.isspace() for c in url):
        problems.append(f"{name} must be an http:// or https:// URL, got {_echo(raw)}.")
        return default
    return url


# ─── Settings ────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class YnabSettings:
    """YNAB_TOKEN / YNAB_BUDGET_ID / YNAB_ACCOUNT_ID (CLI defaults, admin auto-register)."""

    token: str = field(default="", repr=False)
    budget_id: str = ""
    account_id: str = ""

    @property
    def complete(self) -> bool:
        """True when token, budget and account are all set (admin auto-register rule)."""
        return bool(self.token and self.budget_id and self.account_id)


@dataclass(frozen=True, slots=True)
class CryptoSettings:
    """YNAB_CRYPTO_ACCOUNT_ID, CRYPTO_BTC_XPUB, CRYPTO_ETH_ADDRESS, ANKR_API_KEY."""

    account_id: str = ""
    btc_xpub: str = field(default="", repr=False)
    eth_address: str = ""
    ankr_api_key: str = field(default="", repr=False)


@dataclass(frozen=True, slots=True)
class IbkrSettings:
    """YNAB_BROKERAGE_ACCOUNT_ID, IBKR_BASE_URL, IBKR_ACCOUNT_ID (CLI brokerage sync)."""

    brokerage_account_id: str = ""
    base_url: str = DEFAULT_IBKR_BASE_URL
    account_id: str = ""


@dataclass(frozen=True, slots=True)
class LogSettings:
    """LOG_LEVEL (upper-case, validated) and LOG_FILE (None = file logging disabled)."""

    level: str = DEFAULT_LOG_LEVEL
    file: Path | None = None


@dataclass(frozen=True, slots=True)
class TelegramSettings:
    """TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID, TELEGRAM_API_BASE.

    ``admin_id`` is None when the variable is missing or malformed; ``validate_for_bot``
    reports that, so a running bot always has an int here.
    """

    bot_token: str = field(default="", repr=False)
    admin_id: int | None = None
    api_base: str = DEFAULT_TELEGRAM_API_BASE


@dataclass(frozen=True, slots=True)
class BuildInfo:
    """BUILD_SHA (first 7 alphanumerics, default ``dev``) and BUILD_DATE (default ``local``).

    Characters other than ASCII letters and digits are dropped from BUILD_SHA *before* it
    is cut to 7 characters, so the sha is always URL-safe and as long as the input allows.
    A timezone-aware ISO-8601 BUILD_DATE is normalised to ``YYYY-MM-DD HH:MM UTC``; any
    other value (including one that cannot be converted to UTC) is shown verbatim, limited
    to printable ASCII and at most 40 characters. Building this never raises.
    """

    sha: str = DEFAULT_BUILD_SHA
    date: str = DEFAULT_BUILD_DATE

    @property
    def is_release(self) -> bool:
        """False for local/dev builds that carry no real commit sha."""
        return self.sha not in NON_RELEASE_SHAS

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> BuildInfo:
        """Read BUILD_SHA / BUILD_DATE from ``environ`` (default ``os.environ``)."""
        env = os.environ if environ is None else environ
        sha = _SHA_SAFE_RE.sub("", _get(env, "BUILD_SHA"))[:_SHA_LENGTH] or DEFAULT_BUILD_SHA
        return cls(sha=sha, date=_format_build_date(_get(env, "BUILD_DATE")))


def _format_build_date(raw: str) -> str:
    if not raw:
        return DEFAULT_BUILD_DATE
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            # isoformat, not strftime: glibc's %Y does not zero-pad years below 1000.
            utc = parsed.astimezone(UTC).replace(tzinfo=None)
            return f"{utc.isoformat(sep=' ', timespec='minutes')} UTC"
    except (ValueError, OverflowError):
        # Not ISO-8601, or so close to year 1/9999 that the UTC conversion leaves the
        # datetime range: show it as given.
        pass
    shown = _UNPRINTABLE_RE.sub("", raw)[:_MAX_BUILD_DATE]
    return shown or DEFAULT_BUILD_DATE


@dataclass(frozen=True)
class Settings:
    """Typed process configuration built from the environment by :meth:`from_env`.

    ``environ`` is a read-only snapshot of the mapping the settings were built from, so
    ``web/config.py`` can parse its WEB_UI_* variables from the same source.
    ``problems`` lists every malformed value that was replaced by its default.
    """

    ynab: YnabSettings = field(default_factory=YnabSettings)
    crypto: CryptoSettings = field(default_factory=CryptoSettings)
    ibkr: IbkrSettings = field(default_factory=IbkrSettings)
    log: LogSettings = field(default_factory=LogSettings)
    telegram: TelegramSettings = field(default_factory=TelegramSettings)
    build: BuildInfo = field(default_factory=BuildInfo)
    data_dir: Path = DEFAULT_DATA_DIR
    csv_dir: Path = field(default_factory=lambda: Path(os.path.expanduser(DEFAULT_CSV_DIR)))
    app_uid: int = DEFAULT_APP_ID
    app_gid: int = DEFAULT_APP_ID
    environ: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({}), repr=False, compare=False
    )
    problems: list[str] = field(default_factory=list, compare=False)
    problems_by_var: Mapping[str, tuple[str, ...]] = field(
        default_factory=lambda: MappingProxyType({}), repr=False, compare=False
    )

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        """Build settings from ``environ`` (default ``os.environ``). Never raises on
        malformed values: each one is reported in ``problems`` and replaced by its default.
        """
        env: dict[str, str] = dict(os.environ if environ is None else environ)
        by_var: dict[str, list[str]] = {}

        def sink(var: str) -> list[str]:
            return by_var.setdefault(var, [])

        settings = cls(
            ynab=YnabSettings(
                token=_get(env, "YNAB_TOKEN"),
                budget_id=_get(env, "YNAB_BUDGET_ID"),
                account_id=_get(env, "YNAB_ACCOUNT_ID"),
            ),
            crypto=CryptoSettings(
                account_id=_get(env, "YNAB_CRYPTO_ACCOUNT_ID"),
                btc_xpub=_get(env, "CRYPTO_BTC_XPUB"),
                eth_address=_get(env, "CRYPTO_ETH_ADDRESS"),
                ankr_api_key=_get(env, "ANKR_API_KEY"),
            ),
            ibkr=IbkrSettings(
                brokerage_account_id=_get(env, "YNAB_BROKERAGE_ACCOUNT_ID"),
                base_url=_http_url(
                    _get(env, "IBKR_BASE_URL"),
                    name="IBKR_BASE_URL",
                    default=DEFAULT_IBKR_BASE_URL,
                    problems=sink("IBKR_BASE_URL"),
                ),
                account_id=_get(env, "IBKR_ACCOUNT_ID"),
            ),
            log=LogSettings(
                level=_log_level(env, sink("LOG_LEVEL")),
                file=_log_file(env, sink("LOG_FILE")),
            ),
            telegram=TelegramSettings(
                bot_token=_get(env, "TELEGRAM_BOT_TOKEN"),
                admin_id=_admin_id(env, sink("TELEGRAM_ADMIN_ID")),
                api_base=_http_url(
                    _get(env, "TELEGRAM_API_BASE"),
                    name="TELEGRAM_API_BASE",
                    default=DEFAULT_TELEGRAM_API_BASE,
                    problems=sink("TELEGRAM_API_BASE"),
                ),
            ),
            build=BuildInfo.from_env(env),
            data_dir=_data_dir(env, sink("BOT_DATA_DIR")),
            csv_dir=_csv_dir(env, sink("CSV_DIR")),
            app_uid=_app_id(env, "APP_UID", sink("APP_UID")),
            app_gid=_app_id(env, "APP_GID", sink("APP_GID")),
            environ=MappingProxyType(env),
            problems=[msg for msgs in by_var.values() for msg in msgs],
            problems_by_var=MappingProxyType(
                {var: tuple(msgs) for var, msgs in by_var.items() if msgs}
            ),
        )
        return settings

    # ── derived paths ──

    @property
    def users_db_path(self) -> Path:
        return self.data_dir / USERS_DB_FILENAME

    @property
    def heartbeat_path(self) -> Path:
        return self.data_dir / HEARTBEAT_FILENAME

    @property
    def backups_dir(self) -> Path:
        return self.data_dir / BACKUPS_DIRNAME

    @property
    def csv_cache_dir(self) -> Path:
        return self.data_dir / CSV_CACHE_DIRNAME

    def tx_db_path(self, telegram_id: int) -> Path:
        """Per-user transactions database (``transactions_<telegram_id>.db``)."""
        if isinstance(telegram_id, bool) or not isinstance(telegram_id, int):
            raise TypeError("telegram_id must be an int")
        return self.data_dir / f"transactions_{telegram_id}.db"

    # ── validation ──

    def problems_for(self, *names: str) -> list[str]:
        """Problems recorded for the given variable names, in the given order."""
        return [msg for name in names for msg in self.problems_by_var.get(name, ())]

    def validate_for_bot(self) -> list[str]:
        """Errors that must stop the Telegram bot from starting (empty list = OK)."""
        errors: list[str] = []
        if not self.telegram.bot_token:
            errors.append("TELEGRAM_BOT_TOKEN is not set.")
        if self.telegram.admin_id is None:
            if self.problems_by_var.get("TELEGRAM_ADMIN_ID"):
                errors.extend(self.problems_for("TELEGRAM_ADMIN_ID"))
            else:
                errors.append("TELEGRAM_ADMIN_ID is not set (your numeric Telegram user id).")
        errors.extend(self.problems_for("TELEGRAM_API_BASE"))
        return errors

    def validate_for_cli(self, mode: str) -> list[str]:
        """Errors for a CLI mode (see :data:`CLI_MODES`); empty list = OK.

        CLI flags override environment values: apply them first with
        ``dataclasses.replace`` and validate the result.
        """
        if mode not in CLI_MODES:
            raise ValueError(f"unknown CLI mode {mode!r}")
        if mode == "db-stats":
            return []
        errors: list[str] = []
        if not self.ynab.token:
            errors.append(
                "YNAB token required. Set YNAB_TOKEN or use --token "
                "(app.ynab.com → Account Settings → Developer Settings)."
            )
        if mode == "list-budgets":
            return errors
        if not self.ynab.budget_id:
            errors.append(
                "YNAB budget id required. Set YNAB_BUDGET_ID or use --budget-id "
                "(run --list-budgets)."
            )
        if mode == "list-accounts":
            return errors
        if mode == "crypto-sync":
            errors.extend(self._crypto_errors())
        elif mode == "brokerage-sync":
            errors.extend(self._brokerage_errors())
        elif not self.ynab.account_id:
            errors.append(
                "YNAB account id required. Set YNAB_ACCOUNT_ID or use --account-id "
                "(run --list-accounts)."
            )
        return errors

    def _crypto_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.crypto.account_id:
            errors.append(
                "Crypto tracking account required. Set YNAB_CRYPTO_ACCOUNT_ID or use "
                "--crypto-account-id (run --list-accounts)."
            )
        if not self.crypto.btc_xpub and not self.crypto.eth_address:
            errors.append(
                "At least one of CRYPTO_BTC_XPUB (--btc-xpub) or CRYPTO_ETH_ADDRESS "
                "(--eth-address) is required."
            )
        return errors

    def _brokerage_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.ibkr.brokerage_account_id:
            errors.append(
                "Brokerage tracking account required. Set YNAB_BROKERAGE_ACCOUNT_ID or use "
                "--brokerage-account-id (run --list-accounts)."
            )
        errors.extend(self.problems_for("IBKR_BASE_URL"))
        return errors

    def secret_values(self) -> tuple[str, ...]:
        """Every configured secret (non-empty), for log redaction. Never log these."""
        candidates: Iterable[str] = (
            self.telegram.bot_token,
            self.ynab.token,
            self.crypto.ankr_api_key,
            self.crypto.btc_xpub,
            _get(self.environ, "WEB_UI_SECRET_KEY"),
        )
        return tuple(value for value in candidates if value)


def _log_level(env: Mapping[str, str], problems: list[str]) -> str:
    raw = _get(env, "LOG_LEVEL")
    if not raw:
        return DEFAULT_LOG_LEVEL
    level = normalize_log_level(raw)
    if level is not None:
        return level
    aliases = ", ".join(LOG_LEVEL_ALIASES)
    problems.append(
        f"LOG_LEVEL must be one of {', '.join(LOG_LEVELS)} (or {aliases}), got {_echo(raw)}; "
        f"using {DEFAULT_LOG_LEVEL}."
    )
    return DEFAULT_LOG_LEVEL


def _log_file(env: Mapping[str, str], problems: list[str]) -> Path | None:
    # v1: unset → default file; set but empty → file logging disabled.
    default = _expand(DEFAULT_LOG_FILE, env)
    if "LOG_FILE" not in env:
        return default
    raw = _get(env, "LOG_FILE")
    if not raw:
        return None
    if _has_nul("LOG_FILE", raw, default, problems):
        return default
    return _expand(raw, env)


def _data_dir(env: Mapping[str, str], problems: list[str]) -> Path:
    """BOT_DATA_DIR keeps v1's meaning: used verbatim, no ``~`` expansion; a relative path
    is relative to the working directory (``/app`` in the container)."""
    raw = _get(env, "BOT_DATA_DIR")
    if not raw or _has_nul("BOT_DATA_DIR", raw, DEFAULT_DATA_DIR, problems):
        return DEFAULT_DATA_DIR
    return Path(raw)


def _csv_dir(env: Mapping[str, str], problems: list[str]) -> Path:
    default = _expand(DEFAULT_CSV_DIR, env)
    raw = _get(env, "CSV_DIR")
    if not raw or _has_nul("CSV_DIR", raw, default, problems):
        return default
    return _expand(raw, env)


def _has_nul(name: str, raw: str, default: Path, problems: list[str]) -> bool:
    # os.environ cannot hold NUL, but other mappings (tests, web) can; any file system call
    # with such a path would raise ValueError much later and far from the cause.
    if "\x00" not in raw:
        return False
    problems.append(f"{name} contains a NUL character; using {default}.")
    return True


def _admin_id(env: Mapping[str, str], problems: list[str]) -> int | None:
    raw = _get(env, "TELEGRAM_ADMIN_ID")
    if not raw:
        return None
    value = _ascii_int(raw)
    if value is None or value <= 0:
        problems.append(
            f"TELEGRAM_ADMIN_ID must be your positive numeric Telegram user id, got {_echo(raw)}."
        )
        return None
    return value


def _app_id(env: Mapping[str, str], name: str, problems: list[str]) -> int:
    # 0 would mean "stay root"; the entrypoint must never do that because of a typo.
    return parse_int(
        env.get(name),
        name=name,
        default=DEFAULT_APP_ID,
        problems=problems,
        minimum=1,
        maximum=_MAX_ID,
    )
