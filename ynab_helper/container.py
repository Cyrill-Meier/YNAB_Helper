"""Container entrypoint: ``ENTRYPOINT ["python3", "-m", "ynab_helper.container"]``, ``CMD ["bot"]``.

Production starts the image as root (no ``user:`` override) with a root-owned
``/app/bot_data`` bind mount and a root-only (0600) ``/app/.env``. A plain ``USER`` line
would crash-loop the bot (F70), so this entrypoint does the migration itself:

1. load ``/app/.env`` into the environment while still root (existing variables win);
2. ``umask 077`` so everything created from now on is private to the app user (F57);
3. pin ``BOT_DATA_DIR`` to its absolute path, create it (missing parents 0755) and hand
   every entry in it that is not yet owned by ``APP_UID:APP_GID`` (default 10001) to that
   user — dirs 0700, files 0600 — without ever following symlinks or touching hard-linked
   files;
4. ``setgroups([])``, ``setgid``, ``setuid`` and verify the drop; set HOME to the data dir;
5. run the requested command in-process: ``bot`` (default), ``cli …``, ``selfcheck``,
   ``healthcheck``; any other command is exec'd as-is (e.g. ``python3 revolut_to_ynab.py``).

When not started as root, steps 1–4 are skipped and the command simply runs.
Secrets are never logged: only paths, counts and ids.
"""

from __future__ import annotations

import errno
import hashlib
import importlib
import logging
import os
import pkgutil
import signal
import sqlite3
import stat
import sys
import tempfile
from collections.abc import Callable, MutableMapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from ynab_helper import config, logging_setup
from ynab_helper.errors import YnabHelperError

log = logging.getLogger(__name__)

COMMANDS: Final[tuple[str, ...]] = ("bot", "cli", "selfcheck", "healthcheck")
DEFAULT_COMMAND: Final[str] = "bot"
DIR_MODE: Final[int] = 0o700
FILE_MODE: Final[int] = 0o600
PARENT_DIR_MODE: Final[int] = 0o755
PRIVATE_UMASK: Final[int] = 0o077
EXIT_USAGE: Final[int] = 2
# bot_data is a few levels deep at most; a bound keeps the number of open fds bounded.
MAX_DEPTH: Final[int] = 32

# RIPEMD-160 test vectors (Bosselaers' reference page).
_RIPEMD160_ABC: Final[str] = "8eb208f7e05d987a9b044a8e98c6b087f15a0bfc"

_O_CLOEXEC: Final[int] = getattr(os, "O_CLOEXEC", 0)
_O_NOFOLLOW: Final[int] = getattr(os, "O_NOFOLLOW", 0)
_O_DIRECTORY: Final[int] = getattr(os, "O_DIRECTORY", 0)
_O_NONBLOCK: Final[int] = getattr(os, "O_NONBLOCK", 0)
_O_NOCTTY: Final[int] = getattr(os, "O_NOCTTY", 0)

USAGE: Final[str] = """\
usage: python3 -m ynab_helper.container [COMMAND [ARGS...]]

commands:
  bot           run the Telegram bot (default)
  cli ARGS...   run the command-line importer (same flags as revolut_to_ynab.py)
  selfcheck     verify the image: imports, RIPEMD-160, writable data dir; prints OK
  healthcheck   Docker health check (heartbeat + web /health)
  other         (container entrypoint only) executed as-is after the privilege drop
"""


class PrivilegeDropError(YnabHelperError):
    """Switching from root to the app user failed; the process must not continue."""


@dataclass(slots=True)
class ChownReport:
    """Outcome of :func:`fix_ownership` (counts only; per-entry problems are logged)."""

    changed: int = 0
    already_owned: int = 0
    skipped: int = 0
    errors: int = 0


# ─── ownership of the data directory ─────────────────────────────────────────


def fix_ownership(top: Path, uid: int, gid: int) -> ChownReport:
    """Give ``top`` and every entry below it to ``uid:gid`` where not already so.

    * Entries are opened relative to their parent directory's fd with ``O_NOFOLLOW`` and
      re-checked with ``fstat`` before ``fchown``/``fchmod``, so a symlink planted in the
      data dir can never redirect the chown to a file outside it.
    * Symlinks, special files, hard-linked files (``st_nlink > 1``) and anything on
      another filesystem are skipped and logged.
    * Ownership changes also reset the mode: directories 0700, files 0600.
    * Per-entry errors are logged and counted and the walk continues; this includes
      ``top`` itself (a bind mount that refuses chown may still be usable). Whether the
      result is usable is decided later, by the app user's first write.
    """
    report = ChownReport()
    try:
        # The top directory is configured by the operator (BOT_DATA_DIR), so following a
        # symlink here is intended; everything below it is untrusted.
        top_fd = os.open(top, os.O_RDONLY | _O_DIRECTORY | _O_CLOEXEC)
    except OSError as exc:
        log.error("cannot open data dir %s: %s", top, _why(exc))
        report.errors += 1
        return report
    try:
        top_dev = os.fstat(top_fd).st_dev
        try:
            _fix_entry(top_fd, DIR_MODE, uid, gid, report, str(top))
        except OSError as exc:
            log.error("cannot fix ownership of data dir %s: %s", top, _why(exc))
            report.errors += 1
        _walk(top_fd, top_dev, uid, gid, report, rel=".", depth=0)
    finally:
        os.close(top_fd)
    return report


def _walk(
    dir_fd: int, dev: int, uid: int, gid: int, report: ChownReport, *, rel: str, depth: int
) -> None:
    try:
        with os.scandir(dir_fd) as it:
            names = [entry.name for entry in it]
    except OSError as exc:
        log.warning("cannot list %s: %s", rel, _why(exc))
        report.errors += 1
        return
    for name in names:
        entry_rel = name if rel == "." else f"{rel}/{name}"
        try:
            _visit(dir_fd, name, dev, uid, gid, report, rel=entry_rel, depth=depth)
        except OSError as exc:
            log.warning("cannot fix ownership of %s: %s", entry_rel, _why(exc))
            report.errors += 1


def _visit(
    dir_fd: int,
    name: str,
    dev: int,
    uid: int,
    gid: int,
    report: ChownReport,
    *,
    rel: str,
    depth: int,
) -> None:
    st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    if stat.S_ISLNK(st.st_mode):
        log.info("not following symlink %s", rel)
        report.skipped += 1
        return
    if st.st_dev != dev:
        log.warning("skipping %s: on a different filesystem", rel)
        report.skipped += 1
        return
    if stat.S_ISDIR(st.st_mode):
        if depth >= MAX_DEPTH:
            log.warning("skipping %s: nested deeper than %d levels", rel, MAX_DEPTH)
            report.skipped += 1
            return
        fd = os.open(name, os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | _O_CLOEXEC, dir_fd=dir_fd)
        try:
            if _fix_entry(fd, DIR_MODE, uid, gid, report, rel, expect=stat.S_ISDIR):
                _walk(fd, dev, uid, gid, report, rel=rel, depth=depth + 1)
        finally:
            os.close(fd)
        return
    if stat.S_ISREG(st.st_mode):
        flags = os.O_RDONLY | _O_NOFOLLOW | _O_NONBLOCK | _O_NOCTTY | _O_CLOEXEC
        fd = os.open(name, flags, dir_fd=dir_fd)
        try:
            _fix_entry(fd, FILE_MODE, uid, gid, report, rel, expect=stat.S_ISREG)
        finally:
            os.close(fd)
        return
    log.info("skipping %s: not a regular file or directory", rel)
    report.skipped += 1


def _fix_entry(
    fd: int,
    mode: int,
    uid: int,
    gid: int,
    report: ChownReport,
    rel: str,
    *,
    expect: Callable[[int], bool] = stat.S_ISDIR,
) -> bool:
    """chown/chmod the open ``fd`` if needed. Returns False if the entry was skipped."""
    st = os.fstat(fd)
    if not expect(st.st_mode):
        # Replaced between stat and open: leave it alone.
        log.warning("skipping %s: changed type while being inspected", rel)
        report.skipped += 1
        return False
    if stat.S_ISREG(st.st_mode) and st.st_nlink > 1:
        # A hard link could point at a file outside the data dir (e.g. one planted by a
        # compromised app user); chowning it would hand that file over.
        log.warning("skipping %s: file has %d hard links", rel, st.st_nlink)
        report.skipped += 1
        return False
    if st.st_uid == uid and st.st_gid == gid:
        report.already_owned += 1
        return True
    os.fchown(fd, uid, gid)
    os.fchmod(fd, mode)
    report.changed += 1
    return True


def _why(exc: OSError) -> str:
    return exc.strerror or errno.errorcode.get(exc.errno or 0, "") or type(exc).__name__


# ─── privilege drop ──────────────────────────────────────────────────────────


def drop_privileges(uid: int, gid: int) -> None:
    """Irreversibly switch to ``uid:gid`` with no supplementary groups, then verify.

    Raises :class:`PrivilegeDropError` if any step fails or the process could regain root.
    """
    try:
        os.setgroups([])
        os.setgid(gid)
        os.setuid(uid)
    except OSError as exc:
        raise PrivilegeDropError(f"cannot switch to uid {uid} gid {gid}: {_why(exc)}") from exc
    ids = (os.getuid(), os.geteuid(), os.getgid(), os.getegid())
    if ids != (uid, uid, gid, gid):
        raise PrivilegeDropError(f"privilege drop incomplete: uid/euid/gid/egid = {ids}")


def _ensure_data_dir(data_dir: Path) -> None:
    """Create the absolute ``data_dir`` (0700) and any missing parent directories (0755).

    Parents are created by root and stay root-owned (only the data dir is chowned), so
    they must be traversable by the app user. The private umask would make them 0700,
    hence the explicit chmod; 0755 is what ``mkdir -p`` gives under the default umask.
    """
    for parent in reversed(data_dir.parents):
        if parent.is_dir():
            continue
        try:
            os.mkdir(parent, PARENT_DIR_MODE)
        except FileExistsError:
            continue  # created concurrently, or a non-directory: mkdir below reports it
        os.chmod(parent, PARENT_DIR_MODE)
        log.info("created parent directory %s (mode 0755)", parent)
    data_dir.mkdir(exist_ok=True, mode=DIR_MODE)


def prepare_runtime(environ: MutableMapping[str, str] | None = None) -> bool:
    """Root-only container preparation (steps 1–4 of the module docstring).

    No-op returning False when the process is not root. Returns True after a successful
    drop. Shims that may be started as root via ``docker exec`` should call this first so
    they never create root-owned files in the data directory.
    """
    if os.geteuid() != 0:
        return False
    env: MutableMapping[str, str] = os.environ if environ is None else environ
    loaded = config.load_dotenv(environ=env)
    os.umask(PRIVATE_UMASK)
    settings = config.Settings.from_env(env)
    for problem in settings.problems_for("APP_UID", "APP_GID", "BOT_DATA_DIR"):
        log.warning("%s", problem)
    uid, gid = settings.app_uid, settings.app_gid
    # Pin the directory prepared here for everything that runs after the drop: the bot, the
    # CLI and exec'd commands re-read BOT_DATA_DIR, and HOME (set to it below) must be
    # absolute. Path.absolute() keeps '..' for the kernel to resolve, as open() would.
    data_dir = settings.data_dir.absolute()
    env["BOT_DATA_DIR"] = str(data_dir)
    _ensure_data_dir(data_dir)
    report = fix_ownership(data_dir, uid, gid)
    log.info(
        "data dir %s: %d entries handed to %d:%d, %d already owned, %d skipped, %d errors",
        data_dir,
        report.changed,
        uid,
        gid,
        report.already_owned,
        report.skipped,
        report.errors,
    )
    drop_privileges(uid, gid)
    env["HOME"] = str(data_dir)
    log.info("running as uid %d gid %d (.env %s)", uid, gid, "loaded" if loaded else "not found")
    return True


# ─── commands ────────────────────────────────────────────────────────────────


def _exit_code(result: object) -> int:
    if result is None:
        return 0
    if isinstance(result, int) and not isinstance(result, bool):
        return result
    log.error("command returned a non-integer result; treating as failure")
    return 1


def _run_bot(args: Sequence[str]) -> int:
    if args:
        sys.stderr.write(f"the bot takes no arguments (got {len(args)})\n\n{USAGE}")
        return EXIT_USAGE
    try:
        bot_main = importlib.import_module("ynab_helper.bot.main")
    except ImportError as exc:
        log.error("cannot start the bot: %s", exc, exc_info=True)
        return 1
    return _exit_code(bot_main.main())


def _run_cli(args: Sequence[str]) -> int:
    try:
        cli = importlib.import_module("ynab_helper.cli")
    except ImportError as exc:
        log.error("cannot start the CLI: %s", exc, exc_info=True)
        return 1
    return _exit_code(cli.main(list(args)))


def _run_healthcheck(args: Sequence[str]) -> int:
    from ynab_helper import healthcheck

    return healthcheck.main(list(args))


def _exec(argv: Sequence[str]) -> int:
    try:
        os.execvp(argv[0], list(argv))
    except OSError as exc:
        sys.stderr.write(f"cannot run {argv[0]!r}: {_why(exc)}\n")
        return 127
    return 127  # pragma: no cover - execvp only returns by raising


# ─── selfcheck (CI image smoke test) ─────────────────────────────────────────


def _check_imports() -> tuple[bool, str]:
    import ynab_helper

    failures: dict[str, str] = {}
    names: list[str] = []

    def on_error(name: str) -> None:
        failures[name] = repr(sys.exc_info()[1])

    for module in pkgutil.walk_packages(ynab_helper.__path__, "ynab_helper.", onerror=on_error):
        # Importing a __main__ module would run its entrypoint.
        if module.name.rsplit(".", 1)[-1] != "__main__":
            names.append(module.name)
    for name in names:
        if name in failures:
            continue
        try:
            importlib.import_module(name)
        except Exception as exc:
            failures[name] = repr(exc)
    if failures:
        detail = "; ".join(f"{name}: {error}" for name, error in sorted(failures.items()))
        return False, f"imports failed: {detail}"
    return True, f"imported {len(names)} modules"


def _check_ripemd160() -> tuple[bool, str]:
    try:
        from Crypto.Hash import RIPEMD160  # pycryptodome: OpenSSL 3 drops ripemd160

        if RIPEMD160.new(b"abc").hexdigest() == _RIPEMD160_ABC:
            return True, "RIPEMD-160 available (pycryptodome)"
    except ImportError:
        pass
    try:
        if hashlib.new("ripemd160", b"abc").hexdigest() == _RIPEMD160_ABC:
            return True, "RIPEMD-160 available (hashlib)"
    except ValueError:
        pass
    return False, "RIPEMD-160 unavailable (install pycryptodome)"


def _check_data_dir(data_dir: Path) -> tuple[bool, str]:
    try:
        data_dir.mkdir(parents=True, exist_ok=True, mode=DIR_MODE)
        with tempfile.TemporaryDirectory(prefix=".selfcheck-", dir=data_dir) as tmp:
            db_path = Path(tmp) / "check.db"
            conn = sqlite3.connect(db_path)
            try:
                mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                conn.execute("CREATE TABLE t (x INTEGER)")
                conn.execute("INSERT INTO t VALUES (1)")
                conn.commit()
            finally:
                conn.close()
    except (OSError, sqlite3.Error) as exc:
        return False, f"data dir {data_dir} not writable: {exc}"
    if str(mode).lower() != "wal":
        return False, f"data dir {data_dir}: SQLite WAL mode unavailable ({mode})"
    return True, f"data dir {data_dir} writable (SQLite {sqlite3.sqlite_version}, WAL)"


def selfcheck() -> int:
    """Image smoke test (CI: ``docker run --rm IMAGE selfcheck``). Needs no secrets."""
    config.load_dotenv()
    settings = config.Settings.from_env()
    checks = (_check_imports(), _check_ripemd160(), _check_data_dir(settings.data_dir))
    for ok, detail in checks:
        sys.stdout.write(f"{'ok  ' if ok else 'FAIL'} {detail}\n")
    passed = all(ok for ok, _ in checks)
    sys.stdout.write("OK\n" if passed else "FAILED\n")
    sys.stdout.flush()
    return 0 if passed else 1


# ─── dispatch ────────────────────────────────────────────────────────────────


def _install_pid1_sigterm() -> None:
    """As PID 1, a signal with the default disposition is ignored by the kernel, so
    ``docker stop`` would wait 10 s and SIGKILL (F40). Turn SIGTERM into SystemExit
    until the command installs its own graceful handler."""
    if os.getpid() != 1 or signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL:
        return

    def _terminate(signum: int, _frame: object) -> None:
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, _terminate)


def _run(argv: Sequence[str] | None, *, privileged_entry: bool) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    command, rest = (args[0], args[1:]) if args else (DEFAULT_COMMAND, [])
    if command in ("-h", "--help", "help"):
        sys.stdout.write(USAGE)
        return 0
    if command.startswith("-"):
        sys.stderr.write(f"unknown option {command!r}\n\n{USAGE}")
        return EXIT_USAGE

    if command == "healthcheck":
        # Runs every minute: read-only, no chown, no extra log lines (one status line).
        return _run_healthcheck(rest)

    # File logging is configured later by the command itself, never as root (a log file
    # created now would be root-owned).
    logging_setup.setup_logging(os.environ.get("LOG_LEVEL", "INFO"), None)
    _install_pid1_sigterm()
    # Financial data and tokens: everything this process creates is private (F57).
    os.umask(PRIVATE_UMASK)
    if privileged_entry:
        try:
            prepare_runtime()
        except (OSError, YnabHelperError) as exc:
            log.error("refusing to start: container preparation failed: %s", exc)
            return 1
    elif os.geteuid() == 0:
        log.warning(
            "running as root: files created now will be root-owned; "
            "use `python3 -m ynab_helper.container` to drop privileges"
        )

    if command == "bot":
        return _run_bot(rest)
    if command == "cli":
        return _run_cli(rest)
    if command == "selfcheck":
        return selfcheck()
    if privileged_entry:
        # docker run IMAGE python3 revolut_to_ynab.py … runs unprivileged like everything else.
        return _exec(args)
    sys.stderr.write(f"unknown command {command!r}\n\n{USAGE}")
    return EXIT_USAGE


def main(argv: Sequence[str] | None = None) -> int:
    """Container entrypoint: root preparation (if root), then dispatch ``argv[0]``."""
    return _run(argv, privileged_entry=True)


def dispatch(argv: Sequence[str] | None = None) -> int:
    """``python -m ynab_helper``: same commands, without any privilege handling."""
    return _run(argv, privileged_entry=False)


if __name__ == "__main__":
    sys.exit(main())
