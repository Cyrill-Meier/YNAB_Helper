"""locks.py: per-user, non-blocking operation locks (§4.5, F16)."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from ynab_helper.errors import Busy
from ynab_helper.locks import LOCKS, LockToken, UserLocks


def test_operation_acquires_and_releases() -> None:
    locks = UserLocks()
    assert locks.current("1") is None
    with locks.operation("1", "import"):
        assert locks.current("1") == "import"
        assert locks.snapshot() == {"1": "import"}
    assert locks.current("1") is None
    assert locks.snapshot() == {}


def test_busy_names_running_operation() -> None:
    locks = UserLocks()
    with locks.operation("1", "import"), pytest.raises(Busy) as info:
        locks.acquire("1", "reconcile")
    assert info.value.operation == "import"
    assert "import" in info.value.user_message
    assert locks.current("1") is None


def test_not_reentrant_on_same_thread() -> None:
    """A re-entrant lock owned by the event-loop thread would let two async requests in."""
    locks = UserLocks()
    with locks.operation("1", "sync"), pytest.raises(Busy):
        locks.acquire("1", "sync")


def test_released_on_exception() -> None:
    locks = UserLocks()
    with pytest.raises(RuntimeError, match="boom"), locks.operation("1", "import"):
        raise RuntimeError("boom")
    assert locks.current("1") is None
    with locks.operation("1", "import"):
        pass


def test_released_on_base_exception() -> None:
    locks = UserLocks()
    with pytest.raises(KeyboardInterrupt), locks.operation("1", "import"):
        raise KeyboardInterrupt
    assert locks.current("1") is None


def test_keys_are_independent() -> None:
    locks = UserLocks()
    with locks.operation("1", "import"), locks.operation("2", "import"):
        assert locks.snapshot() == {"1": "import", "2": "import"}


def test_key_normalisation() -> None:
    """str(telegram_id) for bot/web and the db path for the CLI must hit the same lock
    whichever way a caller spells them."""
    locks = UserLocks()
    with locks.operation(59213669, "import"):
        assert locks.current("59213669") == "import"
        with pytest.raises(Busy):
            locks.acquire("59213669", "dedupe")
    with locks.operation(Path("/data/tx.db"), "import"):
        assert locks.current("/data/tx.db") == "import"


@pytest.mark.parametrize("key", [True, 1.5, None, b"1", ""])
def test_invalid_keys(key: object) -> None:
    locks = UserLocks()
    with pytest.raises((TypeError, ValueError)):
        locks.acquire(key, "import")  # type: ignore[arg-type]


@pytest.mark.parametrize("name", ["", None, 5])
def test_invalid_names(name: object) -> None:
    with pytest.raises(ValueError):
        UserLocks().acquire("1", name)  # type: ignore[arg-type]


def test_acquire_release_tokens() -> None:
    locks = UserLocks()
    token = locks.acquire("1", "import")
    assert isinstance(token, LockToken)
    assert (token.key, token.name) == ("1", "import")
    locks.release(token)
    with pytest.raises(RuntimeError):
        locks.release(token)  # double release
    newer = locks.acquire("1", "sync")
    with pytest.raises(RuntimeError):
        locks.release(token)  # a stale token cannot release a newer holder
    assert locks.current("1") == "sync"
    locks.release(newer)


def test_release_from_another_thread() -> None:
    """Bot handlers may acquire and hand the token to a worker thread."""
    locks = UserLocks()
    token = locks.acquire("1", "import")
    worker = threading.Thread(target=locks.release, args=(token,))
    worker.start()
    worker.join()
    assert locks.current("1") is None


def test_F16_web_reconcile_refused_while_bot_import_runs() -> None:
    """A web reconcile during a Telegram import used to post a wrong adjustment."""
    locks = UserLocks()
    import_started = threading.Event()
    finish_import = threading.Event()
    outcome: dict[str, object] = {}

    def bot_import() -> None:
        with locks.operation("59213669", "import"):
            import_started.set()
            finish_import.wait(5)

    def web_reconcile() -> None:
        try:
            with locks.operation("59213669", "reconcile"):
                outcome["ran"] = True
        except Busy as exc:
            outcome["busy"] = exc.operation

    bot = threading.Thread(target=bot_import)
    bot.start()
    assert import_started.wait(5)
    web = threading.Thread(target=web_reconcile)
    web.start()
    web.join(5)
    assert outcome == {"busy": "import"}
    finish_import.set()
    bot.join(5)
    web_reconcile()
    assert outcome == {"busy": "import", "ran": True}


def test_mutual_exclusion_under_contention() -> None:
    locks = UserLocks()
    active = 0
    max_active = 0
    entered = 0
    busy = 0
    guard = threading.Lock()
    start = threading.Barrier(16)

    def worker() -> None:
        nonlocal active, max_active, entered, busy
        start.wait()
        for _ in range(200):
            try:
                with locks.operation("k", "op"):
                    with guard:
                        active += 1
                        entered += 1
                        max_active = max(max_active, active)
                    time.sleep(0)
                    with guard:
                        active -= 1
            except Busy:
                with guard:
                    busy += 1

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert max_active == 1
    assert entered + busy == 16 * 200
    assert entered >= 1
    assert locks.snapshot() == {}


def test_singleton() -> None:
    assert isinstance(LOCKS, UserLocks)
    from ynab_helper import locks as module

    assert module.LOCKS is LOCKS
