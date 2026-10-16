"""Shared pytest configuration.

Every test runs with outbound network access disabled: only loopback and Unix
sockets may connect. Real YNAB / Telegram / blockchain calls must go through
the fakes in tests/fakes/.
"""

from __future__ import annotations

import socket
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"

_LOOPBACK = {"127.0.0.1", "::1", "localhost", "0.0.0.0"}
_real_connect = socket.socket.connect


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


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES
