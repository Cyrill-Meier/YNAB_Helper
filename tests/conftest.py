"""Shared pytest configuration.

Every test runs with outbound network access disabled: only loopback and Unix
sockets may connect. Real YNAB / Telegram / blockchain calls must go through
the fakes in tests/fakes/.

Logging is restored after every test, because setup_logging() (called by the
bot, CLI and runtime tests) reconfigures the root, noisy and audit loggers.
"""

from __future__ import annotations

import logging
import socket
from collections.abc import Iterator
from pathlib import Path

import pytest

from ynab_helper import logging_setup

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"

_LOOPBACK = {"127.0.0.1", "::1", "localhost", "0.0.0.0"}
_real_connect = socket.socket.connect
_TOUCHED_LOGGERS = (*logging_setup.NOISY_LOGGERS, logging_setup.AUDIT_LOGGER)


def _guarded_connect(self, address):
    if self.family == getattr(socket, "AF_UNIX", object()):
        return _real_connect(self, address)
    host = address[0] if isinstance(address, tuple) else address
    if host not in _LOOPBACK:
        raise RuntimeError(f"network access blocked in tests: {address!r}")
    return _real_connect(self, address)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    monkeypatch.setattr(socket.socket, "connect", _guarded_connect)
    yield


@pytest.fixture(autouse=True)
def _restore_logging() -> Iterator[None]:
    """setup_logging() reconfigures the root logger; put everything back afterwards."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    saved = {
        name: (logging.getLogger(name).level, logging.getLogger(name).propagate)
        for name in _TOUCHED_LOGGERS
    }
    yield
    logging_setup.reset_logging()
    logging_setup.clear_redactions()
    for handler in list(root.handlers):
        if handler not in saved_handlers:
            root.removeHandler(handler)
    for handler in saved_handlers:
        if handler not in root.handlers:
            root.addHandler(handler)
    root.setLevel(saved_level)
    for name, (level, propagate) in saved.items():
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = propagate


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES
