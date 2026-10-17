"""Fixtures for the CLI tests: the fake YNAB behind every client, a private HOME and
environment, and the global logging state restored after each test."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FAKE_TOKEN, FakeYnab
from ynab_helper import cli, config, logging_setup, services
from ynab_helper.maintenance.dedupe import DEDUPE_SCANS

BUDGET = "b1"
ACCT = "acct-main"
OTHER = "acct-other"
CRYPTO = "acct-crypto"
IBKR = "acct-ibkr"
FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
PENDING_A = "export_pending_then_completed_a.csv"
PENDING_B = "export_pending_then_completed_b.csv"
_TOUCHED_LOGGERS = (*logging_setup.NOISY_LOGGERS, logging_setup.AUDIT_LOGGER)


@pytest.fixture(autouse=True)
def _restore_logging() -> Iterator[None]:
    """cli.main() configures the root logger and registers secrets; undo that."""
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
    root.setLevel(saved_level)
    for name, (level, propagate) in saved.items():
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = propagate


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(config, "_loaded_files", set())
    DEDUPE_SCANS.clear()
    yield
    DEDUPE_SCANS.clear()


@pytest.fixture
def fake() -> FakeYnab:
    f = FakeYnab()
    f.add_budget(BUDGET, currency="CHF", name="Main")
    f.add_account(BUDGET, ACCT, "Revolut")
    f.add_account(BUDGET, OTHER, "Savings", type="savings")
    f.add_account(BUDGET, CRYPTO, "Crypto", type="otherAsset", on_budget=False)
    f.add_account(BUDGET, IBKR, "IBKR", type="otherAsset", on_budget=False)
    return f


@pytest.fixture(autouse=True)
def _fake_clients(monkeypatch: pytest.MonkeyPatch, fake: FakeYnab) -> None:
    # No client retries: an injected fault must reach the code under test.
    monkeypatch.setattr(services, "client_factory", lambda ctx: fake.client(max_retries=0))
    monkeypatch.setattr(
        services, "token_client_factory", lambda token: fake.client(token=token, max_retries=0)
    )


@dataclass
class Cli:
    """Runs ``cli.main`` with a private environment and captures its output."""

    home: Path
    env: dict[str, str]
    capsys: pytest.CaptureFixture[str]
    monkeypatch: pytest.MonkeyPatch
    answers: list[str]

    @property
    def db(self) -> Path:
        return self.home / ".revolut_to_ynab.db"

    @property
    def downloads(self) -> Path:
        return self.home / "Downloads"

    def run(self, *argv: str, answers: list[str] | None = None) -> tuple[int, str, str]:
        if answers is not None:
            self.answers[:] = answers
        self.capsys.readouterr()
        code = cli.main(list(argv), environ=self.env, env_file=self.home / "absent.env")
        captured = self.capsys.readouterr()
        return code, captured.out, captured.err

    def statement(self, name: str = PENDING_A, *, to: str | None = None) -> Path:
        """Copy a fixture statement into Downloads (as ``to``) and return its path."""
        target = self.downloads / (to or name)
        target.write_bytes((FIXTURES / name).read_bytes())
        return target


@pytest.fixture
def run_cli(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> Cli:
    home = tmp_path / "home"
    (home / "Downloads").mkdir(parents=True)
    env = {
        "HOME": str(home),
        "YNAB_TOKEN": FAKE_TOKEN,
        "YNAB_BUDGET_ID": BUDGET,
        "YNAB_ACCOUNT_ID": ACCT,
        "LOG_FILE": str(tmp_path / "cli.log"),
    }
    answers: list[str] = []

    def fake_input(prompt: str = "") -> str:
        print(prompt, end="")
        if not answers:
            raise EOFError
        return answers.pop(0)

    monkeypatch.setattr("builtins.input", fake_input)
    return Cli(home=home, env=env, capsys=capsys, monkeypatch=monkeypatch, answers=answers)


def mutations(fake: FakeYnab) -> list[tuple[str, str]]:
    return [(c.method, c.path) for c in fake.calls if c.method != "GET"]


def live(fake: FakeYnab, account_id: str = ACCT) -> list[dict[str, Any]]:
    return fake.transactions(BUDGET, account_id=account_id)
