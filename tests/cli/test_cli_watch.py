"""--watch (F51): stable size before import, done only after success, retries, and the
--dry-run / --db-path / --skip-pending options honoured."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from tests.fakes.fake_ynab import FakeYnab
from ynab_helper import cli, services
from ynab_helper.cli import FolderWatcher, RetryLater, WatchOutcome, WatchResult
from ynab_helper.errors import CsvError

from .conftest import BUDGET, FIXTURES, PENDING_A, Cli, live, mutations

STATEMENT = (FIXTURES / PENDING_A).read_bytes()


class Clock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


class Recorder:
    """A watch handler returning scripted outcomes and recording the files it saw."""

    def __init__(self, *outcomes: WatchResult) -> None:
        self.outcomes = list(outcomes)
        self.seen: list[str] = []

    def __call__(self, path: Path) -> WatchResult:
        self.seen.append(path.name)
        return self.outcomes.pop(0) if self.outcomes else WatchOutcome.DONE


@pytest.fixture
def folder(tmp_path: Path) -> Path:
    path = tmp_path / "inbox"
    path.mkdir()
    return path


def _watcher(folder: Path, handler: Callable[[Path], WatchResult], clock: Clock) -> FolderWatcher:
    return FolderWatcher(folder, handler, retry_delay=60, max_retry_delay=300, clock=clock)


# ── FolderWatcher ───────────────────────────────────────────────────────────


def test_F51_a_file_is_imported_only_once_its_size_is_stable(folder: Path):
    handler = Recorder()
    watcher = _watcher(folder, handler, Clock())
    target = folder / "statement.csv"
    target.write_bytes(STATEMENT[:40])  # an AirDrop still writing
    assert watcher.poll() == []  # first sighting
    target.write_bytes(STATEMENT[:200])
    assert watcher.poll() == []  # still growing
    target.write_bytes(STATEMENT)
    assert watcher.poll() == []
    assert watcher.poll() == [(target, WatchOutcome.DONE)]
    assert handler.seen == ["statement.csv"]
    assert watcher.poll() == []  # done: never again while unchanged


def test_F51_files_present_at_start_are_left_alone(folder: Path):
    (folder / "old.csv").write_bytes(STATEMENT)
    handler = Recorder()
    watcher = _watcher(folder, handler, Clock())
    assert watcher.prime() == 1
    watcher.poll()
    watcher.poll()
    assert handler.seen == []


def test_F51_a_failed_import_is_retried_later_and_done_only_after_success(folder: Path):
    clock = Clock()
    handler = Recorder(WatchOutcome.RETRY, WatchOutcome.RETRY, WatchOutcome.DONE)
    notes: list[str] = []
    watcher = FolderWatcher(
        folder, handler, retry_delay=60, max_retry_delay=300, clock=clock, notify=notes.append
    )
    (folder / "s.csv").write_bytes(STATEMENT)
    watcher.poll()
    assert [o for _, o in watcher.poll()] == [WatchOutcome.RETRY]
    assert watcher.poll() == []  # not before the retry delay
    clock.now += 60
    assert [o for _, o in watcher.poll()] == [WatchOutcome.RETRY]
    clock.now += 60
    assert watcher.poll() == []  # the delay doubled to 120 s
    clock.now += 60
    assert [o for _, o in watcher.poll()] == [WatchOutcome.DONE]
    clock.now += 1_000
    assert watcher.poll() == []
    assert handler.seen == ["s.csv"] * 3
    assert notes == ["Will retry s.csv in 60 s.", "Will retry s.csv in 120 s."]


def test_retries_stop_after_max_attempts_until_the_file_changes(folder: Path):
    clock = Clock()
    handler = Recorder(*[WatchOutcome.RETRY] * 10)
    watcher = FolderWatcher(folder, handler, retry_delay=1, max_attempts=3, clock=clock)
    target = folder / "s.csv"
    target.write_bytes(STATEMENT)
    for _ in range(10):
        watcher.poll()
        clock.now += 1_000
    assert len(handler.seen) == 3
    target.write_bytes(STATEMENT + b"\n")  # the user saved it again
    watcher.poll()
    watcher.poll()
    assert len(handler.seen) == 4


def test_a_skipped_file_is_tried_again_only_after_it_changed(folder: Path):
    handler = Recorder(WatchOutcome.SKIP)
    watcher = _watcher(folder, handler, Clock())
    target = folder / "s.csv"
    target.write_bytes(STATEMENT)
    watcher.poll()
    watcher.poll()
    watcher.poll()
    assert handler.seen == ["s.csv"]
    target.write_bytes(STATEMENT + b"\n")
    watcher.poll()
    watcher.poll()
    assert handler.seen == ["s.csv", "s.csv"]


def test_non_statements_and_empty_files_are_never_handed_over(folder: Path):
    handler = Recorder()
    watcher = _watcher(folder, handler, Clock())
    (folder / "notes.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    (folder / "empty.csv").write_bytes(b"")
    (folder / "photo.jpg").write_bytes(STATEMENT)
    (folder / ".hidden.csv").write_bytes(STATEMENT)
    for _ in range(3):
        watcher.poll()
    assert handler.seen == []


def test_a_removed_file_is_forgotten(folder: Path):
    handler = Recorder()
    watcher = _watcher(folder, handler, Clock())
    target = folder / "s.csv"
    target.write_bytes(STATEMENT)
    watcher.poll()
    watcher.poll()
    target.unlink()
    watcher.poll()
    target.write_bytes(STATEMENT)  # downloaded again
    watcher.poll()
    watcher.poll()
    assert handler.seen == ["s.csv", "s.csv"]


def test_F51_a_folder_that_is_briefly_unreadable_is_not_imported_again(folder: Path):
    notes: list[str] = []
    handler = Recorder()
    watcher = FolderWatcher(folder, handler, clock=Clock(), notify=notes.append)
    (folder / "old.csv").write_bytes(STATEMENT)
    watcher.prime()
    (folder / "new.csv").write_bytes(STATEMENT)
    watcher.poll()
    watcher.poll()
    assert handler.seen == ["new.csv"]

    away = folder.with_name("inbox-away")
    folder.rename(away)  # the drive is unmounted / the cloud folder goes offline
    for _ in range(3):
        assert watcher.poll() == []
    assert notes == [f"Cannot read {folder} right now; still watching."]  # said once
    away.rename(folder)
    for _ in range(3):
        watcher.poll()
    assert handler.seen == ["new.csv"]  # neither the primed nor the imported file again


def test_F51_a_file_that_cannot_be_examined_for_a_moment_stays_done(
    folder: Path, monkeypatch: pytest.MonkeyPatch
):
    handler = Recorder()
    watcher = _watcher(folder, handler, Clock())
    target = folder / "s.csv"
    target.write_bytes(STATEMENT)
    watcher.poll()
    watcher.poll()
    assert handler.seen == ["s.csv"]

    real_stat = Path.stat

    def flaky_stat(self: Path, *args: object, **kwargs: object):
        if self.name == "s.csv":
            raise PermissionError("busy")
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", flaky_stat)
    watcher.poll()
    monkeypatch.setattr(Path, "stat", real_stat)
    watcher.poll()
    watcher.poll()
    assert handler.seen == ["s.csv"]


def _unreadable(monkeypatch: pytest.MonkeyPatch, name: str) -> Callable[[], None]:
    """Make ``open()`` of the file ``name`` fail (locked by the app writing it, a cloud
    placeholder still downloading, permissions not set yet); returns the undo."""
    real_open = Path.open

    def locked_open(self: Path, *args: object, **kwargs: object):
        if self.name == name:
            raise PermissionError(13, "Permission denied")
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", locked_open)
    return lambda: monkeypatch.setattr(Path, "open", real_open)


def test_F51_a_statement_that_cannot_be_read_for_a_moment_is_imported_once_readable(
    folder: Path, monkeypatch: pytest.MonkeyPatch
):
    clock = Clock()
    notes: list[str] = []
    handler = Recorder()
    watcher = FolderWatcher(
        folder, handler, retry_delay=60, max_attempts=2, clock=clock, notify=notes.append
    )
    target = folder / "s.csv"
    target.write_bytes(STATEMENT)
    watcher.poll()  # first sighting
    readable_again = _unreadable(monkeypatch, "s.csv")
    for _ in range(4):  # more often than max_attempts: an unreadable file is never given up
        assert [o for _, o in watcher.poll()] == [RetryLater()]
        clock.now += 1_000
    assert handler.seen == []
    assert notes[:2] == ["Cannot read s.csv yet (Permission denied).", "Will retry s.csv in 60 s."]
    assert not any("Ignoring" in n or "Giving up" in n for n in notes)  # v2 review: no SKIP
    readable_again()  # the file did not change: same size and mtime
    assert watcher.poll() == [(target, WatchOutcome.DONE)]
    assert handler.seen == ["s.csv"]


def test_F51_a_rate_limited_file_waits_as_asked_and_is_never_given_up(folder: Path):
    clock = Clock()
    notes: list[str] = []
    waits = [RetryLater(1_800.0)] * 6 + [RetryLater(None), RetryLater(86_400.0)]
    handler = Recorder(*waits)  # then DONE
    watcher = FolderWatcher(
        folder,
        handler,
        retry_delay=60,
        max_retry_delay=900,
        max_attempts=2,
        max_wait=3_600,
        clock=clock,
        notify=notes.append,
    )
    (folder / "s.csv").write_bytes(STATEMENT)
    watcher.poll()
    assert [o for _, o in watcher.poll()] == [RetryLater(1_800.0)]
    clock.now += 1_799
    assert watcher.poll() == []  # YNAB's Retry-After is honoured, not the 60 s backoff
    clock.now += 1
    assert len(watcher.poll()) == 1
    for _ in range(4):
        clock.now += 1_800
        watcher.poll()
    clock.now += 1_800
    assert [o for _, o in watcher.poll()] == [RetryLater(None)]  # no Retry-After: backoff
    clock.now += 899
    assert watcher.poll() == []
    clock.now += 1
    assert [o for _, o in watcher.poll()] == [RetryLater(86_400.0)]  # capped at max_wait
    clock.now += 3_600
    assert [o for _, o in watcher.poll()] == [WatchOutcome.DONE]
    assert len(handler.seen) == 9  # far more than max_attempts=2
    assert notes[0] == "Will retry s.csv in 30 min."
    assert notes[-2:] == ["Will retry s.csv in 15 min.", "Will retry s.csv in 1 h."]
    assert not any("Giving up" in n for n in notes)


def test_a_file_unreadable_for_weeks_does_not_crash_the_watch(
    folder: Path, monkeypatch: pytest.MonkeyPatch
):
    clock = Clock()
    watcher = FolderWatcher(folder, Recorder(), max_retry_delay=900, clock=clock)
    (folder / "s.csv").write_bytes(STATEMENT)
    watcher.poll()
    _unreadable(monkeypatch, "s.csv")
    for _ in range(1_200):  # > 1024 waits: an unclamped 2**n backoff overflows a float
        assert [o for _, o in watcher.poll()] == [RetryLater()]
        clock.now += 900


def test_rate_limit_waits_do_not_use_up_the_attempts_of_real_failures(folder: Path):
    clock = Clock()
    handler = Recorder(RetryLater(10.0), RetryLater(10.0), WatchOutcome.RETRY, WatchOutcome.RETRY)
    watcher = FolderWatcher(folder, handler, retry_delay=1, max_attempts=2, clock=clock)
    (folder / "s.csv").write_bytes(STATEMENT)
    for _ in range(20):
        watcher.poll()
        clock.now += 1_000
    assert len(handler.seen) == 4  # 2 waits + 2 failures, then given up until it changes


# ── the CLI's --watch mode ──────────────────────────────────────────────────


def _drop_after_first_sleep(
    monkeypatch: pytest.MonkeyPatch, folder: Path, polls: int = 3
) -> list[float]:
    """Fake time.sleep: drops the statement into ``folder`` after the first poll and stops
    the watch (Ctrl+C) after ``polls`` polls."""
    slept: list[float] = []

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)
        if len(slept) == 1:
            (folder / "account-statement_2026-10-01_2026-10-13_en_x.csv").write_bytes(STATEMENT)
        if len(slept) >= polls:
            raise KeyboardInterrupt

    monkeypatch.setattr(cli.time, "sleep", fake_sleep)
    return slept


def test_F51_watch_honours_dry_run_and_db_path(
    run_cli: Cli, fake: FakeYnab, folder: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    custom = tmp_path / "watch.db"
    _drop_after_first_sleep(monkeypatch, folder)
    code, out, _err = run_cli.run("--watch", str(folder), "--dry-run", "--db-path", str(custom))
    assert code == cli.EXIT_OK
    assert f"Database: {custom}" in out and "Dry run" in out
    assert "New Revolut statement: account-statement_2026-10-01" in out
    assert "Would create: 6" in out and "Stopped watching." in out
    assert mutations(fake) == []  # v1 imported for real (F51)
    assert custom.exists() and not run_cli.db.exists()  # v1 always used the default DB


def test_F51_watch_honours_skip_pending(
    run_cli: Cli, fake: FakeYnab, folder: Path, monkeypatch: pytest.MonkeyPatch
):
    _drop_after_first_sleep(monkeypatch, folder)
    code, out, _err = run_cli.run("--watch", str(folder), "--skip-pending")
    assert code == cli.EXIT_OK and "Created: 3" in out
    assert all(t["cleared"] == "cleared" for t in live(fake)) and len(live(fake)) == 3


def test_F51_watch_retries_a_rate_limited_import(
    run_cli: Cli, fake: FakeYnab, folder: Path, monkeypatch: pytest.MonkeyPatch
):
    clock = Clock()
    monkeypatch.setattr(cli.time, "monotonic", clock)
    sleeps = 0

    def fake_sleep(seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        clock.now += seconds
        if sleeps == 1:  # poll 2 sees the file, poll 3 imports it: 429
            (folder / "s.csv").write_bytes(STATEMENT)
            fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 429, retry_after=30)
        if sleeps == 4:  # poll 4 was too early; poll 5 retries
            clock.now += 60
        if sleeps >= 5:
            raise KeyboardInterrupt

    monkeypatch.setattr(cli.time, "sleep", fake_sleep)
    code, out, err = run_cli.run("--watch", str(folder))
    assert code == cli.EXIT_OK
    assert "rate limit" in err and "Will retry s.csv in 60 s." in out
    assert "Created: 6" in out and len(live(fake)) == 6


def test_F51_watch_outlasts_an_hour_long_rate_limit(
    run_cli: Cli, fake: FakeYnab, folder: Path, monkeypatch: pytest.MonkeyPatch
):
    """v2 review: the watch gave up after ~15 minutes (5 attempts), so a statement dropped
    while the token was rate limited was never imported."""
    clock = Clock()
    monkeypatch.setattr(cli.time, "monotonic", clock)
    sleeps = 0

    def fake_sleep(seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        clock.now += 300  # a coarse clock: each poll is 5 minutes after the last
        if sleeps == 1:
            (folder / "s.csv").write_bytes(STATEMENT)
            fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 429, times=7, retry_after=600)
        if live(fake) or sleeps >= 200:
            raise KeyboardInterrupt

    monkeypatch.setattr(cli.time, "sleep", fake_sleep)
    code, out, err = run_cli.run("--watch", str(folder))
    assert code == cli.EXIT_OK
    assert err.count("✗ s.csv: YNAB rate limit reached") == 7 and "Giving up" not in out
    assert "Will retry s.csv in 10 min." in out
    assert "Created: 6" in out and len(live(fake)) == 6
    assert clock.now - 1_000 > 3_600  # imported more than an hour after it was dropped


def test_F51_watch_waits_out_a_rate_limit_hit_while_posting(
    run_cli: Cli, fake: FakeYnab, folder: Path, monkeypatch: pytest.MonkeyPatch
):
    """The sync succeeded but YNAB's limit was hit by the create: a partly applied result
    with ``rate_limited`` is waited out, not counted toward the attempts."""
    clock = Clock()
    monkeypatch.setattr(cli.time, "monotonic", clock)
    sleeps = 0

    def fake_sleep(seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        clock.now += 300
        if sleeps == 1:
            (folder / "s.csv").write_bytes(STATEMENT)
            fake.fail_next(
                "POST", rf"/budgets/{BUDGET}/transactions", 429, times=6, retry_after=600
            )
        if live(fake) or sleeps >= 200:
            raise KeyboardInterrupt

    monkeypatch.setattr(cli.time, "sleep", fake_sleep)
    code, out, _err = run_cli.run("--watch", str(folder))
    assert code == cli.EXIT_OK
    assert out.count("rate limit") >= 6 and "Giving up" not in out
    assert "Created: 6" in out and len(live(fake)) == 6


def test_F51_watch_retries_a_statement_the_import_could_not_read(
    run_cli: Cli, fake: FakeYnab, folder: Path, monkeypatch: pytest.MonkeyPatch
):
    """parse_export wraps an OSError in CsvError; that is no verdict on the content."""
    real_import = services.import_csv
    calls: list[str] = []

    def flaky_import(ctx: object, source: Path, **kwargs: object):
        calls.append(source.name)
        if len(calls) <= 6:  # more often than max_attempts
            raise CsvError(
                "cannot read", user_message="The statement file could not be read."
            ) from PermissionError(13, "Permission denied")
        return real_import(ctx, source, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(services, "import_csv", flaky_import)
    clock = Clock()
    monkeypatch.setattr(cli.time, "monotonic", clock)
    sleeps = 0

    def fake_sleep(seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        clock.now += 300
        if sleeps == 1:
            (folder / "s.csv").write_bytes(STATEMENT)
        if live(fake) or sleeps >= 200:
            raise KeyboardInterrupt

    monkeypatch.setattr(cli.time, "sleep", fake_sleep)
    code, out, err = run_cli.run("--watch", str(folder))
    assert code == cli.EXIT_OK
    assert err.count("✗ s.csv: The statement file could not be read.") == 6
    assert "Giving up" not in out
    assert len(calls) == 7 and "Created: 6" in out and len(live(fake)) == 6


def test_watch_stops_on_a_rejected_token(
    run_cli: Cli, fake: FakeYnab, folder: Path, monkeypatch: pytest.MonkeyPatch
):
    _drop_after_first_sleep(monkeypatch, folder, polls=10)
    fake.fail_next("GET", None, 401, times=50)
    code, _out, err = run_cli.run("--watch", str(folder))
    assert code == cli.EXIT_FAILED and "Error:" in err


def test_watch_without_an_import_service_failure_keeps_running(
    run_cli: Cli, folder: Path, monkeypatch: pytest.MonkeyPatch
):
    calls: list[str] = []

    def refused(ctx: object, source: object, **kwargs: object) -> None:
        from ynab_helper.errors import CsvError

        calls.append(str(source))
        raise CsvError("bad", user_message="The statement is broken.")

    monkeypatch.setattr(services, "import_csv", refused)
    _drop_after_first_sleep(monkeypatch, folder, polls=6)
    code, _out, err = run_cli.run("--watch", str(folder))
    assert code == cli.EXIT_OK and "The statement is broken." in err
    assert len(calls) == 1  # a refused file waits until it changes


def test_an_unexpected_failure_does_not_stop_the_watch(
    run_cli: Cli, folder: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    attempts: list[str] = []

    def broken(ctx: object, source: object, **kwargs: object) -> None:
        attempts.append(str(source))
        raise RuntimeError("bug")

    monkeypatch.setattr(services, "import_csv", broken)
    _drop_after_first_sleep(monkeypatch, folder, polls=4)
    code, _out, err = run_cli.run("--watch", str(folder))
    assert code == cli.EXIT_OK and "unexpected failure" in err
    assert len(attempts) == 1  # retried only after the retry delay
    assert "RuntimeError: bug" in (tmp_path / "cli.log").read_text(encoding="utf-8")
