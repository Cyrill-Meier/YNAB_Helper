"""container.py / __main__.py: root preparation, privilege drop, dispatch (§10, F70, F57, F40)."""

from __future__ import annotations

import logging
import os
import shutil
import signal
import stat
import subprocess
import sys
import types
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from ynab_helper import config, container, healthcheck
from ynab_helper.container import ChownReport, PrivilegeDropError

REPO_ROOT = Path(__file__).resolve().parents[2]
_REAL_UMASK = os.umask


@pytest.fixture(autouse=True)
def _keep_umask() -> Iterator[None]:
    """The entrypoints set a process-wide umask; never leak it into other tests."""
    saved = _REAL_UMASK(0o022)
    _REAL_UMASK(saved)
    yield
    _REAL_UMASK(saved)


# ─── fake privileged OS ──────────────────────────────────────────────────────


class FakeIds:
    """Stands in for the os.* id functions; records privileged calls in order."""

    def __init__(self, calls: list[Any], *, uid: int = 0, gid: int = 0) -> None:
        self.calls = calls
        self.uid = self.euid = uid
        self.gid = self.egid = gid
        self.setuid_error: OSError | None = None
        self.setuid_noop = False

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(os, "geteuid", lambda: self.euid)
        monkeypatch.setattr(os, "getuid", lambda: self.uid)
        monkeypatch.setattr(os, "getgid", lambda: self.gid)
        monkeypatch.setattr(os, "getegid", lambda: self.egid)
        monkeypatch.setattr(os, "setgroups", self.setgroups)
        monkeypatch.setattr(os, "setgid", self.setgid)
        monkeypatch.setattr(os, "setuid", self.setuid)
        monkeypatch.setattr(os, "umask", self.umask)

    def setgroups(self, groups: list[int]) -> None:
        self.calls.append(("setgroups", tuple(groups)))

    def setgid(self, gid: int) -> None:
        self.calls.append(("setgid", gid))
        self.gid = self.egid = gid

    def setuid(self, uid: int) -> None:
        self.calls.append(("setuid", uid))
        if self.setuid_error is not None:
            raise self.setuid_error
        if not self.setuid_noop:
            self.uid = self.euid = uid

    def umask(self, mask: int) -> int:
        self.calls.append(("umask", mask))
        return 0o022


@pytest.fixture
def root_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """A pretend root process: fake ids, a private .env file, recorded chown."""
    calls: list[Any] = []
    ids = FakeIds(calls)
    ids.install(monkeypatch)

    env_file = tmp_path / "dotenv"
    env_file.write_text("APP_UID=2000\nAPP_GID=3000\nYH_FROM_DOTENV=secret-value\n")
    monkeypatch.setattr(config, "DEFAULT_ENV_FILE", env_file)

    real_load = config.load_dotenv

    def recording_load(*args: Any, **kwargs: Any) -> bool:
        calls.append("load_env")
        return real_load(*args, **kwargs)

    monkeypatch.setattr(config, "load_dotenv", recording_load)

    def recording_chown(top: Path, uid: int, gid: int) -> ChownReport:
        calls.append(("chown", top, uid, gid))
        return ChownReport(changed=3)

    monkeypatch.setattr(container, "fix_ownership", recording_chown)
    data_dir = tmp_path / "bot_data"
    return {"calls": calls, "ids": ids, "data_dir": data_dir, "env_file": env_file}


def test_F70_root_container_drops_to_app_user(root_env: dict[str, Any]) -> None:
    """Order: .env read as root → private umask → chown data dir → drop groups, gid, uid."""
    data_dir: Path = root_env["data_dir"]
    env = {"BOT_DATA_DIR": str(data_dir), "HOME": "/root"}
    assert container.prepare_runtime(env) is True
    assert root_env["calls"] == [
        "load_env",
        ("umask", 0o077),
        ("chown", data_dir, 2000, 3000),
        ("setgroups", ()),
        ("setgid", 3000),
        ("setuid", 2000),
    ]
    assert data_dir.is_dir()
    assert env["YH_FROM_DOTENV"] == "secret-value"  # loaded while root could read it
    assert env["HOME"] == str(data_dir)  # root's HOME is not writable for the app user


def test_existing_environment_wins_over_dotenv(root_env: dict[str, Any]) -> None:
    env = {"BOT_DATA_DIR": str(root_env["data_dir"]), "APP_UID": "4000"}
    container.prepare_runtime(env)
    assert ("setuid", 4000) in root_env["calls"]
    assert ("setgid", 3000) in root_env["calls"]


def test_default_app_ids_are_10001(root_env: dict[str, Any]) -> None:
    root_env["env_file"].write_text("")
    container.prepare_runtime({"BOT_DATA_DIR": str(root_env["data_dir"])})
    assert root_env["calls"][-3:] == [("setgroups", ()), ("setgid", 10001), ("setuid", 10001)]


def test_malformed_app_uid_never_stays_root(root_env: dict[str, Any]) -> None:
    env = {"BOT_DATA_DIR": str(root_env["data_dir"]), "APP_UID": "0", "APP_GID": "root"}
    container.prepare_runtime(env)
    assert root_env["calls"][-2:] == [("setgid", 10001), ("setuid", 10001)]


def test_F57_private_umask_before_anything_is_created(root_env: dict[str, Any]) -> None:
    container.prepare_runtime({"BOT_DATA_DIR": str(root_env["data_dir"])})
    calls = root_env["calls"]
    assert calls.index(("umask", 0o077)) < next(
        i for i, c in enumerate(calls) if isinstance(c, tuple) and c[0] == "chown"
    )


def test_setuid_failure_raises(root_env: dict[str, Any]) -> None:
    root_env["ids"].setuid_error = PermissionError(1, "Operation not permitted")
    with pytest.raises(PrivilegeDropError, match="cannot switch"):
        container.prepare_runtime({"BOT_DATA_DIR": str(root_env["data_dir"])})


def test_incomplete_drop_is_detected(root_env: dict[str, Any]) -> None:
    root_env["ids"].setuid_noop = True
    with pytest.raises(PrivilegeDropError, match="incomplete"):
        container.prepare_runtime({"BOT_DATA_DIR": str(root_env["data_dir"])})


def test_not_root_is_a_no_op(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[Any] = []
    ids = FakeIds(calls, uid=1000, gid=1000)
    ids.install(monkeypatch)
    env = {"BOT_DATA_DIR": str(tmp_path / "d")}
    assert container.prepare_runtime(env) is False
    assert calls == []
    assert env == {"BOT_DATA_DIR": str(tmp_path / "d")}
    assert not (tmp_path / "d").exists()


def _chowned_dir(calls: list[Any]) -> Path:
    return next(c[1] for c in calls if isinstance(c, tuple) and c[0] == "chown")


@pytest.mark.parametrize("raw", ["~/data", "rel/bot_data", "./x/../bot_data"])
def test_data_dir_is_the_same_before_and_after_the_drop(
    root_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    """Review: the entrypoint chowned <root home>/data, the bot then used <data dir>/data
    because HOME had changed; a relative BOT_DATA_DIR made HOME relative."""
    monkeypatch.chdir(tmp_path)
    env = {"HOME": str(tmp_path / "root"), "BOT_DATA_DIR": raw}
    container.prepare_runtime(env)
    prepared = _chowned_dir(root_env["calls"])
    assert prepared.is_absolute()
    assert prepared == tmp_path / raw  # v1 meaning: verbatim, relative to the working dir
    assert env["BOT_DATA_DIR"] == str(prepared)
    assert env["HOME"] == str(prepared)
    # What the bot and the CLI compute after the drop (HOME is now the data dir).
    assert config.Settings.from_env(env).data_dir == prepared
    # The Docker health check runs as root with the image's HOME and reads /app/.env.
    assert config.Settings.from_env({"HOME": "/root", "BOT_DATA_DIR": raw}).data_dir.absolute() == (
        prepared
    )


def test_missing_parents_of_data_dir_are_traversable(
    root_env: dict[str, Any], tmp_path: Path
) -> None:
    """Review: under umask 077, mkdir -p as root made every new parent root-only 0700,
    so the app user could not reach its own data dir after the drop."""
    _REAL_UMASK(0o077)  # what the entrypoint has set by now (os.umask itself is faked)
    data_dir = tmp_path / "mk" / "a" / "b" / "bot_data"
    container.prepare_runtime({"BOT_DATA_DIR": str(data_dir)})
    for parent in (tmp_path / "mk", tmp_path / "mk" / "a", tmp_path / "mk" / "a" / "b"):
        assert stat.S_IMODE(parent.stat().st_mode) == 0o755, parent
    assert stat.S_IMODE(data_dir.stat().st_mode) == 0o700
    assert _chowned_dir(root_env["calls"]) == data_dir  # only the data dir is handed over


def test_existing_parents_are_left_alone(tmp_path: Path) -> None:
    _REAL_UMASK(0o077)
    parent = tmp_path / "p"
    parent.mkdir(mode=0o700)
    container._ensure_data_dir(parent / "bot_data")
    assert stat.S_IMODE(parent.stat().st_mode) == 0o700
    assert stat.S_IMODE((parent / "bot_data").stat().st_mode) == 0o700


def test_data_dir_below_a_file_fails(tmp_path: Path) -> None:
    (tmp_path / "f").write_text("x")
    with pytest.raises(OSError):
        container._ensure_data_dir(tmp_path / "f" / "bot_data")


# ─── main(): preparation then dispatch ───────────────────────────────────────


def _fake_module(monkeypatch: pytest.MonkeyPatch, name: str, main: Callable[..., Any]) -> None:
    module = types.ModuleType(name)
    module.main = main  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, name, module)


def test_main_runs_bot_after_privilege_drop(
    root_env: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = root_env["calls"]
    monkeypatch.setenv("BOT_DATA_DIR", str(root_env["data_dir"]))
    monkeypatch.setenv("HOME", "/root")
    for name in ("YH_FROM_DOTENV", "APP_UID", "APP_GID"):
        monkeypatch.setenv(name, "x")
        monkeypatch.delenv(name)  # restored to unset after the test

    def bot_main() -> int:
        calls.append(("bot", os.getuid(), os.environ.get("YH_FROM_DOTENV")))
        return 0

    _fake_module(monkeypatch, "ynab_helper.bot.main", bot_main)
    assert container.main([]) == 0
    assert calls[-1] == ("bot", 2000, "secret-value")
    assert calls[-2] == ("setuid", 2000)


def test_main_refuses_to_run_when_drop_fails(
    root_env: dict[str, Any], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("BOT_DATA_DIR", str(root_env["data_dir"]))
    monkeypatch.setenv("HOME", "/root")
    for name in ("YH_FROM_DOTENV", "APP_UID", "APP_GID"):
        monkeypatch.setenv(name, "x")
        monkeypatch.delenv(name)
    root_env["ids"].setuid_error = PermissionError(1, "Operation not permitted")
    ran: list[bool] = []
    _fake_module(monkeypatch, "ynab_helper.bot.main", lambda: ran.append(True))
    assert container.main(["bot"]) == 1
    assert ran == []
    assert "refusing to start" in capsys.readouterr().err


def test_dispatch_as_root_warns_but_never_drops(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    calls: list[Any] = []
    FakeIds(calls).install(monkeypatch)
    _fake_module(monkeypatch, "ynab_helper.bot.main", lambda: 0)
    with caplog.at_level(logging.WARNING, logger="ynab_helper.container"):
        assert container.dispatch(["bot"]) == 0
    assert calls == [("umask", 0o077)]  # private files, but no chown and no drop
    assert "running as root" in caplog.text


# ─── dispatch ────────────────────────────────────────────────────────────────


@pytest.fixture
def unprivileged(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    calls: list[Any] = []
    FakeIds(calls, uid=1000, gid=1000).install(monkeypatch)
    return calls


def test_F57_every_entrypoint_sets_private_umask(
    unprivileged: list[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_module(monkeypatch, "ynab_helper.bot.main", lambda: 0)
    assert container.main(["bot"]) == 0
    assert container.dispatch(["bot"]) == 0
    assert unprivileged == [("umask", 0o077), ("umask", 0o077)]


@pytest.mark.usefixtures("unprivileged")
@pytest.mark.parametrize(("result", "code"), [(None, 0), (0, 0), (3, 3), ("weird", 1)])
def test_bot_exit_codes(monkeypatch: pytest.MonkeyPatch, result: object, code: int) -> None:
    _fake_module(monkeypatch, "ynab_helper.bot.main", lambda: result)
    assert container.main(["bot"]) == code
    assert container.dispatch([]) == code  # bot is the default command


@pytest.mark.usefixtures("unprivileged")
def test_bot_import_failure_exits_1(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setitem(sys.modules, "ynab_helper.bot.main", None)
    assert container.main(["bot"]) == 1
    assert "cannot start the bot" in capsys.readouterr().err


@pytest.mark.usefixtures("unprivileged")
def test_bot_rejects_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_module(monkeypatch, "ynab_helper.bot.main", lambda: 0)
    assert container.dispatch(["bot", "--now"]) == container.EXIT_USAGE


@pytest.mark.usefixtures("unprivileged")
def test_cli_receives_remaining_args(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[list[str]] = []

    def cli_main(argv: list[str]) -> int:
        seen.append(argv)
        return 4

    _fake_module(monkeypatch, "ynab_helper.cli", cli_main)
    assert container.main(["cli", "--sync", "-y"]) == 4
    assert seen == [["--sync", "-y"]]


@pytest.mark.usefixtures("unprivileged")
def test_cli_import_failure_exits_1(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setitem(sys.modules, "ynab_helper.cli", None)
    assert container.dispatch(["cli"]) == 1
    assert "cannot start the CLI" in capsys.readouterr().err


@pytest.mark.usefixtures("unprivileged")
def test_healthcheck_dispatch_is_quiet(monkeypatch: pytest.MonkeyPatch) -> None:
    root = logging.getLogger()
    before = list(root.handlers)
    monkeypatch.setattr(healthcheck, "main", lambda argv=None: 7)
    assert container.main(["healthcheck"]) == 7
    assert root.handlers == before  # no logging setup: output is one status line


@pytest.mark.usefixtures("unprivileged")
@pytest.mark.parametrize("flag", ["-h", "--help", "help"])
def test_help(flag: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert container.dispatch([flag]) == 0
    assert "selfcheck" in capsys.readouterr().out


@pytest.mark.usefixtures("unprivileged")
def test_unknown_option_and_command(capsys: pytest.CaptureFixture[str]) -> None:
    assert container.main(["--frobnicate"]) == container.EXIT_USAGE
    assert container.dispatch(["frobnicate"]) == container.EXIT_USAGE
    assert "unknown command" in capsys.readouterr().err


class _Execd(Exception):
    pass


@pytest.mark.usefixtures("unprivileged")
def test_container_execs_other_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    """`docker run IMAGE python3 revolut_to_ynab.py …` still works (after the drop)."""

    def fake_execvp(file: str, args: list[str]) -> None:
        raise _Execd(file, args)

    monkeypatch.setattr(os, "execvp", fake_execvp)
    with pytest.raises(_Execd) as info:
        container.main(["python3", "revolut_to_ynab.py", "--sync"])
    assert info.value.args == ("python3", ["python3", "revolut_to_ynab.py", "--sync"])


@pytest.mark.usefixtures("unprivileged")
def test_exec_failure_exits_127(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def missing(file: str, args: list[str]) -> None:
        raise FileNotFoundError(2, "No such file or directory")

    monkeypatch.setattr(os, "execvp", missing)
    assert container.main(["nope"]) == 127
    assert "cannot run 'nope'" in capsys.readouterr().err


def test_F40_pid1_gets_a_sigterm_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    """PID 1 ignores SIGTERM with the default disposition; docker stop would SIGKILL."""
    if signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL:
        pytest.skip("SIGTERM already handled in this test process")
    monkeypatch.setattr(os, "getpid", lambda: 1)
    try:
        container._install_pid1_sigterm()
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler)
        with pytest.raises(SystemExit) as info:
            handler(signal.SIGTERM, None)  # type: ignore[operator]
        assert info.value.code == 128 + signal.SIGTERM
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)


def test_no_sigterm_handler_when_not_pid1() -> None:
    if signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL:
        pytest.skip("SIGTERM already handled in this test process")
    container._install_pid1_sigterm()
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


# ─── fix_ownership (real files, recorded fchown/fchmod) ──────────────────────


@pytest.fixture
def tree(tmp_path: Path) -> dict[str, Path]:
    top = tmp_path / "bot_data"
    (top / "csv_cache" / "59213669").mkdir(parents=True)
    paths = {
        "top": top,
        "db": top / "bot_users.db",
        "log": top / "bot.log",
        "cache": top / "csv_cache",
        "user_cache": top / "csv_cache" / "59213669",
        "csv": top / "csv_cache" / "59213669" / "statement.csv",
        "outside": tmp_path / "outside.txt",
    }
    for key in ("db", "log", "csv", "outside"):
        paths[key].write_text(key)
    (top / "evil_link").symlink_to(paths["outside"])
    (top / "loop_link").symlink_to(top)
    (top / "csv_cache" / "up").symlink_to(tmp_path)
    paths["hard_a"] = top / "hard_a"
    paths["hard_a"].write_text("h")
    os.link(paths["hard_a"], top / "hard_b")
    os.mkfifo(top / "fifo")
    return paths


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[Any]]:
    """Record fchown/fchmod per inode instead of performing them (we are not root)."""
    seen: dict[int, list[Any]] = {}

    def fchown(fd: int, uid: int, gid: int) -> None:
        seen.setdefault(os.fstat(fd).st_ino, []).append(("chown", uid, gid))

    def fchmod(fd: int, mode: int) -> None:
        seen.setdefault(os.fstat(fd).st_ino, []).append(("chmod", mode))

    monkeypatch.setattr(os, "fchown", fchown)
    monkeypatch.setattr(os, "fchmod", fchmod)
    return seen


def _ino(path: Path) -> int:
    return path.stat(follow_symlinks=False).st_ino


def test_fix_ownership_changes_owner_and_modes(
    tree: dict[str, Path], recorder: dict[int, list[Any]]
) -> None:
    report = container.fix_ownership(tree["top"], 10001, 10001)
    dirs = ("top", "cache", "user_cache")
    files = ("db", "log", "csv")
    for key in dirs:
        assert recorder[_ino(tree[key])] == [("chown", 10001, 10001), ("chmod", 0o700)], key
    for key in files:
        assert recorder[_ino(tree[key])] == [("chown", 10001, 10001), ("chmod", 0o600)], key
    # Never followed: symlink targets, hard-linked files, FIFOs.
    assert _ino(tree["outside"]) not in recorder
    assert _ino(tree["hard_a"]) not in recorder
    assert _ino(tree["top"] / "fifo") not in recorder
    assert len(recorder) == len(dirs) + len(files)
    # 3 symlinks + 2 hard-linked files + 1 FIFO
    assert report == ChownReport(changed=6, already_owned=0, skipped=6, errors=0)


def test_fix_ownership_skips_already_owned(
    tree: dict[str, Path], recorder: dict[int, list[Any]]
) -> None:
    report = container.fix_ownership(tree["top"], os.getuid(), os.getgid())
    assert recorder == {}
    assert report.changed == 0
    assert report.already_owned == 6


def test_fix_ownership_only_touches_foreign_entries(
    tree: dict[str, Path], recorder: dict[int, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Production: everything root-owned except what an earlier start already fixed."""
    foreign = {_ino(tree["db"]), _ino(tree["cache"])}
    real_fstat = os.fstat

    def fstat(fd: int) -> os.stat_result:
        st = real_fstat(fd)
        if st.st_ino in foreign:
            fields = list(st[:10])
            fields[4] = fields[5] = 0  # pretend root:root
            return os.stat_result(fields)
        return st

    monkeypatch.setattr(os, "fstat", fstat)
    report = container.fix_ownership(tree["top"], os.getuid(), os.getgid())
    assert set(recorder) == foreign
    assert report.changed == 2
    assert report.already_owned == 4


def test_fix_ownership_continues_after_errors(
    tree: dict[str, Path], recorder: dict[int, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    failing = _ino(tree["db"])
    recording_fchown = os.fchown

    def fchown(fd: int, uid: int, gid: int) -> None:
        if os.fstat(fd).st_ino == failing:
            raise PermissionError(1, "Operation not permitted")
        recording_fchown(fd, uid, gid)

    monkeypatch.setattr(os, "fchown", fchown)
    report = container.fix_ownership(tree["top"], 10001, 10001)
    assert report.errors == 1
    assert report.changed == 5
    assert failing not in recorder
    assert _ino(tree["csv"]) in recorder


def test_fix_ownership_top_dir_error_is_logged_and_walk_continues(
    tree: dict[str, Path],
    recorder: dict[int, list[Any]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Review: a bind mount that refuses chown on the data dir itself (NFS root_squash,
    some NAS mounts) made fix_ownership raise and the container crash-loop."""
    failing = _ino(tree["top"])
    recording_fchown = os.fchown

    def fchown(fd: int, uid: int, gid: int) -> None:
        if os.fstat(fd).st_ino == failing:
            raise PermissionError(1, "Operation not permitted")
        recording_fchown(fd, uid, gid)

    monkeypatch.setattr(os, "fchown", fchown)
    with caplog.at_level(logging.ERROR, logger="ynab_helper.container"):
        report = container.fix_ownership(tree["top"], 10001, 10001)
    assert report.errors == 1
    assert report.changed == 5  # every child is still handed over
    assert failing not in recorder
    assert _ino(tree["db"]) in recorder and _ino(tree["csv"]) in recorder
    assert "cannot fix ownership of data dir" in caplog.text


def test_top_dir_chown_error_does_not_stop_the_start(
    root_env: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def fix_with_top_error(top: Path, uid: int, gid: int) -> ChownReport:
        root_env["calls"].append(("chown", top, uid, gid))
        return ChownReport(errors=1)

    monkeypatch.setattr(container, "fix_ownership", fix_with_top_error)
    assert container.prepare_runtime({"BOT_DATA_DIR": str(root_env["data_dir"])}) is True
    assert root_env["calls"][-1] == ("setuid", 2000)


def test_fix_ownership_skips_other_filesystems(
    tree: dict[str, Path], recorder: dict[int, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    real_stat = os.stat

    def fake_stat(path: Any, *args: Any, **kwargs: Any) -> os.stat_result:
        st = real_stat(path, *args, **kwargs)
        if path == "csv_cache":
            fields = list(st[:10])
            fields[2] = st.st_dev + 1
            return os.stat_result(fields)
        return st

    monkeypatch.setattr(os, "stat", fake_stat)
    container.fix_ownership(tree["top"], 10001, 10001)
    assert _ino(tree["cache"]) not in recorder
    assert _ino(tree["csv"]) not in recorder
    assert _ino(tree["db"]) in recorder


def test_fix_ownership_depth_limit(
    tree: dict[str, Path], recorder: dict[int, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(container, "MAX_DEPTH", 1)
    container.fix_ownership(tree["top"], 10001, 10001)
    assert _ino(tree["cache"]) in recorder
    assert _ino(tree["user_cache"]) not in recorder
    assert _ino(tree["csv"]) not in recorder


def test_fix_ownership_missing_top(tmp_path: Path, recorder: dict[int, list[Any]]) -> None:
    report = container.fix_ownership(tmp_path / "missing", 10001, 10001)
    assert report == ChownReport(errors=1)
    assert recorder == {}


def test_fix_ownership_real_modes_when_owned_by_us(tmp_path: Path) -> None:
    """Without mocks: entries we already own are left exactly as they are."""
    top = tmp_path / "d"
    top.mkdir(mode=0o755)
    (top / "f").write_text("x")
    (top / "f").chmod(0o644)
    container.fix_ownership(top, os.getuid(), os.getgid())
    assert stat.S_IMODE((top / "f").stat().st_mode) == 0o644


# ─── selfcheck and `python -m ynab_helper` (subprocess) ──────────────────────

_OWN_MODULES = (
    "__init__.py",
    "__main__.py",
    "errors.py",
    "timeutil.py",
    "config.py",
    "logging_setup.py",
    "version.py",
    "locks.py",
    "money.py",
    "container.py",
    "healthcheck.py",
)


def _isolated_package(root: Path) -> Path:
    """Copy of the runtime modules only: other packages are being written concurrently."""
    pkg = root / "ynab_helper"
    pkg.mkdir(parents=True)
    for name in _OWN_MODULES:
        shutil.copy2(REPO_ROOT / "ynab_helper" / name, pkg / name)
    return pkg


def _clean_env(**extra: str) -> dict[str, str]:
    drop = ("TELEGRAM_", "YNAB_", "WEB_UI_", "BOT_DATA", "PYTHONPATH", "LOG_")
    env = {k: v for k, v in os.environ.items() if not k.startswith(drop)}
    env.update(extra)
    return env


def _selfcheck_cmd() -> list[str]:
    # As root the container entrypoint would chown/drop to uid 10001, which cannot reach
    # pytest's private tmp dir; that path is covered by the mocked tests above.
    module = "ynab_helper" if os.geteuid() == 0 else "ynab_helper.container"
    return [sys.executable, "-m", module, "selfcheck"]


def test_selfcheck_exits_0_without_telegram_settings(tmp_path: Path) -> None:
    _isolated_package(tmp_path / "src")
    data_dir = tmp_path / "data"
    proc = subprocess.run(
        _selfcheck_cmd(),
        cwd=tmp_path / "src",
        env=_clean_env(BOT_DATA_DIR=str(data_dir)),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    lines = proc.stdout.splitlines()
    assert lines[-1] == "OK"
    assert any("RIPEMD-160 available" in line for line in lines)
    assert any("writable" in line for line in lines)
    assert data_dir.is_dir()
    assert list(data_dir.iterdir()) == []  # leaves nothing behind


def test_selfcheck_fails_on_broken_module(tmp_path: Path) -> None:
    pkg = _isolated_package(tmp_path / "src")
    (pkg / "broken.py").write_text("raise RuntimeError('half-written module')\n")
    proc = subprocess.run(
        _selfcheck_cmd(),
        cwd=tmp_path / "src",
        env=_clean_env(BOT_DATA_DIR=str(tmp_path / "data")),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 1
    assert "ynab_helper.broken" in proc.stdout
    assert proc.stdout.splitlines()[-1] == "FAILED"


def test_selfcheck_fails_when_data_dir_unwritable(tmp_path: Path) -> None:
    _isolated_package(tmp_path / "src")
    blocker = tmp_path / "file"
    blocker.write_text("")
    proc = subprocess.run(
        _selfcheck_cmd(),
        cwd=tmp_path / "src",
        env=_clean_env(BOT_DATA_DIR=str(blocker / "data")),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 1
    assert "not writable" in proc.stdout


def test_python_dash_m_ynab_helper(tmp_path: Path) -> None:
    env = _clean_env(PYTHONPATH=str(REPO_ROOT), BOT_DATA_DIR=str(tmp_path), WEB_UI_ENABLED="0")
    run = lambda *args: subprocess.run(  # noqa: E731
        [sys.executable, "-m", "ynab_helper", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    help_proc = run("--help")
    assert help_proc.returncode == 0
    assert "healthcheck" in help_proc.stdout
    assert run("frobnicate").returncode == 2
    health = run("healthcheck")
    assert health.returncode == 1
    assert health.stdout.startswith("UNHEALTHY: heartbeat")
