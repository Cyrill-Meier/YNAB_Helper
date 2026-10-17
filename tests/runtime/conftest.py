"""Fixtures for the runtime tests (config, logging, locks, container, health check).

Logging is restored after every test by the suite-wide fixture in tests/conftest.py.
"""

from __future__ import annotations

import pytest

from ynab_helper import config


@pytest.fixture(autouse=True)
def _fresh_dotenv_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test starts with no .env file remembered as already loaded."""
    monkeypatch.setattr(config, "_loaded_files", set())
