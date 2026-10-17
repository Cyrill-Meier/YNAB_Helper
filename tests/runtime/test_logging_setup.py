"""logging_setup.py: idempotency, rotation (F110), audit trail, redaction (§4.6)."""

from __future__ import annotations

import io
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

from ynab_helper import logging_setup


def _owned(root: logging.Logger) -> list[logging.Handler]:
    return [h for h in root.handlers if getattr(h, "_ynab_helper_owned", False)]


def _read(path: Path) -> str:
    for handler in logging.getLogger().handlers:
        handler.flush()
    return path.read_text(encoding="utf-8")


def test_setup_returns_resolved_path_and_creates_parent(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "dir" / "bot.log"
    result = logging_setup.setup_logging("INFO", str(target))
    assert result == target
    assert target.parent.is_dir()
    logging.getLogger("ynab_helper.test").info("hello")
    assert "hello" in _read(target)
    assert logging_setup.current_log_file() == target


def test_setup_expands_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    assert logging_setup.setup_logging("INFO", "~/x.log") == tmp_path / "x.log"


@pytest.mark.parametrize("log_file", [None, "", "   "])
def test_no_file_logging(log_file: str | None) -> None:
    assert logging_setup.setup_logging("INFO", log_file) is None
    assert logging_setup.current_log_file() is None
    handlers = _owned(logging.getLogger())
    assert len(handlers) == 1
    assert not isinstance(handlers[0], logging.FileHandler)


def test_idempotent_no_duplicate_handlers(tmp_path: Path) -> None:
    root = logging.getLogger()
    foreign = logging.NullHandler()
    root.addHandler(foreign)
    try:
        logging_setup.setup_logging("INFO", tmp_path / "a.log")
        logging_setup.setup_logging("DEBUG", tmp_path / "a.log")
        logging_setup.setup_logging("INFO", tmp_path / "a.log")
        owned = _owned(root)
        assert len(owned) == 2
        assert sum(isinstance(h, RotatingFileHandler) for h in owned) == 1
        assert foreign in root.handlers  # other people's handlers are left alone
        logging.getLogger("ynab_helper.test").info("once")
        assert _read(tmp_path / "a.log").count("once") == 1
    finally:
        root.removeHandler(foreign)


def test_reconfigure_switches_file(tmp_path: Path) -> None:
    logging_setup.setup_logging("INFO", tmp_path / "a.log")
    logging_setup.setup_logging("INFO", tmp_path / "b.log")
    logging.getLogger("ynab_helper.test").info("to-b")
    assert "to-b" not in _read(tmp_path / "a.log")
    assert "to-b" in _read(tmp_path / "b.log")


def test_F110_rotating_handler_limits(tmp_path: Path) -> None:
    logging_setup.setup_logging("INFO", tmp_path / "bot.log")
    (handler,) = [h for h in _owned(logging.getLogger()) if isinstance(h, RotatingFileHandler)]
    assert handler.maxBytes == 5 * 1024 * 1024
    assert handler.backupCount == 5


def test_F110_log_file_rotates_and_keeps_five_backups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(logging_setup, "MAX_BYTES", 300)
    path = tmp_path / "bot.log"
    logging_setup.setup_logging("INFO", path)
    logger = logging.getLogger("ynab_helper.test")
    for i in range(200):
        logger.info("line %03d %s", i, "x" * 40)
    for handler in logging.getLogger().handlers:
        handler.flush()
    names = sorted(p.name for p in tmp_path.iterdir())
    assert names == ["bot.log", "bot.log.1", "bot.log.2", "bot.log.3", "bot.log.4", "bot.log.5"]
    assert all(p.stat().st_size <= 400 for p in tmp_path.iterdir())
    assert "line 199" in path.read_text()


def test_unopenable_log_file_does_not_crash(tmp_path: Path) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("")
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logging.getLogger("ynab_helper.logging_setup").addHandler(handler)
    try:
        assert logging_setup.setup_logging("INFO", blocker / "bot.log") is None
    finally:
        logging.getLogger("ynab_helper.logging_setup").removeHandler(handler)
    assert "cannot open log file" in stream.getvalue()
    assert len(_owned(logging.getLogger())) == 1  # stderr still configured


def test_noisy_loggers_quieted() -> None:
    logging_setup.setup_logging("DEBUG", None)
    for name in ("python_multipart", "urllib3", "asyncio", "uvicorn.access"):
        assert logging.getLogger(name).level == logging.WARNING


def test_audit_lines_survive_warning_level(tmp_path: Path) -> None:
    """bot.log is the user's audit trail: YNAB writes are logged even at LOG_LEVEL=WARNING."""
    path = tmp_path / "bot.log"
    logging_setup.setup_logging("WARNING", path)
    audit = logging.getLogger(logging_setup.AUDIT_LOGGER)
    assert audit.propagate is True
    audit.info("created 2026-10-16 -12.50 Coffee")
    logging.getLogger("ynab_helper.importer").info("chatty detail")
    text = _read(path)
    assert "created 2026-10-16 -12.50 Coffee" in text
    assert "chatty detail" not in text
    assert "ynab_helper.audit" in text  # logger name is part of the format


def test_format_has_time_level_and_logger(tmp_path: Path) -> None:
    path = tmp_path / "bot.log"
    logging_setup.setup_logging("INFO", path)
    logging.getLogger("ynab_helper.x").warning("msg")
    line = _read(path).strip().splitlines()[-1]
    date, time_, level, name, msg = line.split(maxsplit=4)
    assert len(date) == 10 and len(time_) == 8
    assert (level, name, msg) == ("WARNING", "ynab_helper.x:", "msg")


@pytest.mark.parametrize(
    ("level", "expected", "valid"),
    [
        ("debug", logging.DEBUG, True),
        ("WARN", logging.WARNING, True),
        ("fatal", logging.CRITICAL, True),
        (" error ", logging.ERROR, True),
        ("", logging.INFO, True),
        ("  ", logging.INFO, False),
        # Registered by the logging module but not a LOG_LEVEL value (same rule as config).
        ("TRACE_CUSTOM", logging.INFO, False),
        (logging.INFO, logging.INFO, True),
        (None, logging.INFO, True),
        ("loud", logging.INFO, False),
        ("NOTSET", logging.INFO, False),
        (-1, logging.INFO, False),
        (True, logging.INFO, False),
    ],
)
def test_resolve_level(level: object, expected: int, valid: bool) -> None:
    assert logging_setup.resolve_level(level) == (expected, valid)  # type: ignore[arg-type]


def test_invalid_level_falls_back_to_info_with_warning(
    capsys: pytest.CaptureFixture[str],
) -> None:
    logging_setup.setup_logging("loud", None)
    assert logging.getLogger().level == logging.INFO
    assert "unknown log level 'loud'" in capsys.readouterr().err


def test_stderr_handler_follows_current_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    logging_setup.setup_logging("INFO", None)
    logging.getLogger("ynab_helper.test").info("to-stderr")
    assert "to-stderr" in capsys.readouterr().err


def test_console_level_filters_stderr_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "cli.log"
    logging_setup.setup_logging("INFO", path, console_level="WARNING")
    logger = logging.getLogger("ynab_helper.test")
    logger.info("file-only")
    logger.warning("both")
    err = capsys.readouterr().err
    assert "file-only" not in err and "both" in err
    text = _read(path)
    assert "file-only" in text and "both" in text


def test_console_disabled() -> None:
    logging_setup.setup_logging("INFO", None, console=False)
    assert _owned(logging.getLogger()) == []


def test_F92_secrets_redacted_in_messages_and_tracebacks(tmp_path: Path) -> None:
    token = "123456789:AAH-super-secret-bot-token"
    path = tmp_path / "bot.log"
    logging_setup.setup_logging("INFO", path, secrets=[token, "short", ""])
    logger = logging.getLogger("ynab_helper.test")
    logger.error("GET https://api.telegram.org/bot%s/getUpdates failed", token)
    try:
        raise RuntimeError(f"boom with {token}")
    except RuntimeError:
        logger.exception("request failed")
    logger.info("a short word stays visible")
    text = _read(path)
    assert token not in text
    assert text.count(logging_setup.REDACTED) == 2  # message + exception text
    assert "short word" in text  # values under 8 characters are never masked


def test_redaction_masks_longest_secret_first() -> None:
    logging_setup.add_redactions(["abcdefgh", "abcdefgh-ijklmnop"])
    assert logging_setup.redact("x abcdefgh-ijklmnop y abcdefgh") == "x [REDACTED] y [REDACTED]"
    logging_setup.clear_redactions()
    assert logging_setup.redact("abcdefgh") == "abcdefgh"
