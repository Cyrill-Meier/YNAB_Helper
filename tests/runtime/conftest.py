"""Fixtures for the runtime tests (config, logging, locks, container, health check)."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest

from ynab_helper import config, logging_setup

_TOUCHED_LOGGERS = (*logging_setup.NOISY_LOGGERS, logging_setup.AUDIT_LOGGER)


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


@pytest.fixture(autouse=True)
def _fresh_dotenv_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test starts with no .env file remembered as already loaded."""
    monkeypatch.setattr(config, "_loaded_files", set())
