"""config.py: .env parity with v1, never-raising settings, validation (§11, F59/F60, F70)."""

from __future__ import annotations

import dataclasses
import logging
import os
import random
import stat
from pathlib import Path

import pytest

from tests.legacy import v1_revolut_to_ynab as v1
from ynab_helper import config, logging_setup, version
from ynab_helper.config import Settings

# ─── .env parsing parity with v1 ─────────────────────────────────────────────


def _v1_load(
    monkeypatch: pytest.MonkeyPatch, path: Path, override: bool, initial: dict[str, str]
) -> tuple[bool, dict[str, str]]:
    """Run v1's loader against a private dict standing in for os.environ."""
    fake = dict(initial)
    with monkeypatch.context() as m:
        m.setattr(os, "environ", fake)
        result = v1._load_dotenv(path, override=override)
    return result, fake


def _v2_load(path: Path, override: bool, initial: dict[str, str]) -> tuple[bool, dict[str, str]]:
    env = dict(initial)
    result = config.load_dotenv(path, override=override, environ=env)
    return result, env


HANDWRITTEN_CASES = [
    "KEY=value",
    "KEY = value ",
    "export KEY=value",
    "export   KEY=value",
    "export\tKEY=value",
    "exportKEY=value",
    "  # comment\nKEY=1",
    "#KEY=1",
    "KEY",
    "=value",
    " = value",
    'KEY="quoted value"',
    "KEY='single quoted'",
    'KEY="unterminated',
    "KEY='unterminated # not a comment",
    'KEY="a"trailing',
    "KEY='a' # comment",
    'KEY=""',
    "KEY=''",
    "KEY=a#b",
    "KEY=a #b",
    "KEY=a\t#b",
    "KEY=#b",
    "KEY= #b",
    "KEY=value # comment # more",
    "KEY=a=b=c",
    "KEY==x",
    'KEY= "spaced"  ',
    "KEY=  value with   spaces   ",
    "KEY=1\nKEY=2",
    "A=1\r\nB=2\r\n",
    "A=1\rB=2",
    "A=1\r\r\nB=2",
    "A=1\x0bB=2\x0cC=3",
    "A=1\x85B=2 C=3 D=4",
    "A= x ",
    " A=1",
    "K Y=1",
    "ÄÖÜ=umlaut",
    "export",
    "export =1",
    "export A",
    "TOKEN=123456:ABC-def_ghi",
    "URL=https://example.com/#anchor",
    "URL=https://example.com/ #comment",
    "",
    "\n\n\n",
]


@pytest.mark.parametrize("override", [False, True])
@pytest.mark.parametrize("content", HANDWRITTEN_CASES)
def test_dotenv_parity_with_v1_handwritten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, content: str, override: bool
) -> None:
    path = tmp_path / ".env"
    path.write_text(content, encoding="utf-8", newline="")
    initial = {"KEY": "preexisting", "B": "env-b"}
    assert _v2_load(path, override, initial) == _v1_load(monkeypatch, path, override, initial)


_FRAGMENTS = [
    "A",
    "KEY",
    "K_2",
    "export ",
    "export  ",
    "export\t",
    "=",
    " = ",
    "==",
    "#",
    " #",
    "\t#",
    "'",
    '"',
    " ",
    "  ",
    "\t",
    " ",
    "x",
    "val",
    "v w",
    "\r",
    "\n",
    "\r\n",
    "\x0b",
    "\x85",
    " ",
    "é",
    "1",
    "0",
    ":",
    "/",
]


def _random_dotenv(rng: random.Random) -> str:
    lines = []
    for _ in range(rng.randint(0, 8)):
        lines.append("".join(rng.choice(_FRAGMENTS) for _ in range(rng.randint(0, 9))))
    return rng.choice(["\n", "\r\n", "\r"]).join(lines)


@pytest.mark.parametrize("seed", range(40))
def test_dotenv_parity_with_v1_randomized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, seed: int
) -> None:
    rng = random.Random(seed)
    for i in range(50):
        path = tmp_path / f"{i}.env"
        path.write_text(_random_dotenv(rng), encoding="utf-8", newline="")
        initial = {k: "pre" for k in ("A", "KEY", "x", "val") if rng.random() < 0.4}
        override = rng.random() < 0.5
        assert _v2_load(path, override, initial) == _v1_load(monkeypatch, path, override, initial)


def test_parse_dotenv_returns_pairs_in_file_order() -> None:
    text = "B=2\nexport A='x y' # c\nB=3\n# skip\nC=v # comment\n"
    assert config.parse_dotenv(text) == [("B", "2"), ("A", "x y"), ("B", "3"), ("C", "v")]


def test_load_dotenv_existing_env_wins_and_first_duplicate_wins(tmp_path: Path) -> None:
    path = tmp_path / ".env"
    path.write_text("A=file\nB=first\nB=second\n")
    env = {"A": "shell"}
    assert config.load_dotenv(path, environ=env) is True
    assert env == {"A": "shell", "B": "first"}


def test_load_dotenv_override_file_wins_and_last_duplicate_wins(tmp_path: Path) -> None:
    path = tmp_path / ".env"
    path.write_text("A=file\nB=first\nB=second\n")
    env = {"A": "shell"}
    assert config.load_dotenv(path, override=True, environ=env) is True
    assert env == {"A": "file", "B": "second"}


def test_load_dotenv_writes_os_environ_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("YH_TEST_DOTENV_KEY", "placeholder")
    monkeypatch.delenv("YH_TEST_DOTENV_KEY")  # restored to "unset" after the test
    path = tmp_path / ".env"
    path.write_text("YH_TEST_DOTENV_KEY=from-file\n")
    assert config.load_dotenv(path) is True
    assert os.environ["YH_TEST_DOTENV_KEY"] == "from-file"


def test_default_env_file_is_repo_root_dotenv() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    assert repo_root == config.REPO_ROOT
    assert repo_root / ".env" == config.DEFAULT_ENV_FILE
    assert repo_root / "bot_data" == config.DEFAULT_DATA_DIR


def test_load_dotenv_default_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / ".env"
    path.write_text("FROM_DEFAULT=1\n")
    monkeypatch.setattr(config, "DEFAULT_ENV_FILE", path)
    env: dict[str, str] = {}
    assert config.load_dotenv(environ=env) is True
    assert env == {"FROM_DEFAULT": "1"}


def test_load_dotenv_missing_file_is_silent(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    env: dict[str, str] = {}
    with caplog.at_level(logging.DEBUG, logger="ynab_helper.config"):
        assert config.load_dotenv(tmp_path / "nope.env", environ=env) is False
    assert env == {}
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


@pytest.mark.skipif(os.geteuid() == 0, reason="root can read 0000 files")
def test_load_dotenv_unreadable_file_warns(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    path = tmp_path / ".env"
    path.write_text("SECRET_TOKEN=do-not-log-me\n")
    path.chmod(0)
    env: dict[str, str] = {}
    try:
        with caplog.at_level(logging.WARNING, logger="ynab_helper.config"):
            assert config.load_dotenv(path, environ=env) is False
    finally:
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    assert env == {}
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "NOT loaded" in warnings[0].getMessage()
    assert "do-not-log-me" not in caplog.text


@pytest.mark.skipif(os.geteuid() == 0, reason="root can read 0000 files")
def test_load_dotenv_already_loaded_then_unreadable_is_quiet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The container entrypoint reads /app/.env into os.environ as root; the bot re-reads
    it after the privilege drop. That second, failing read must not warn."""
    monkeypatch.setenv("YH_TEST_LOADED", "x")
    monkeypatch.delenv("YH_TEST_LOADED")
    path = tmp_path / ".env"
    path.write_text("YH_TEST_LOADED=1\n")
    assert config.load_dotenv(path) is True
    path.chmod(0)
    try:
        with caplog.at_level(logging.DEBUG, logger="ynab_helper.config"):
            assert config.load_dotenv(path) is True
            # override=True cannot be honoured: that one must warn and report failure.
            assert config.load_dotenv(path, override=True) is False
    finally:
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1


@pytest.mark.skipif(os.geteuid() == 0, reason="root can read 0000 files")
def test_load_dotenv_into_other_mapping_does_not_count_as_loaded(tmp_path: Path) -> None:
    path = tmp_path / ".env"
    path.write_text("A=1\n")
    assert config.load_dotenv(path, environ={}) is True
    path.chmod(0)
    try:
        assert config.load_dotenv(path, environ={}) is False
    finally:
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)


def test_load_dotenv_directory_warns(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Docker creates a directory at the mount point when the host file is missing."""
    path = tmp_path / ".env"
    path.mkdir()
    with caplog.at_level(logging.WARNING, logger="ynab_helper.config"):
        assert config.load_dotenv(path, environ={}) is False
    assert "not a regular file" in caplog.text


def test_load_dotenv_invalid_utf8_warns_and_applies_nothing(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    path = tmp_path / ".env"
    path.write_bytes(b"A=1\nB=\xff\xfe\n")
    env: dict[str, str] = {}
    with caplog.at_level(logging.WARNING, logger="ynab_helper.config"):
        assert config.load_dotenv(path, environ=env) is False
    assert env == {}
    assert "not valid UTF-8" in caplog.text


def test_load_dotenv_strips_utf8_bom(tmp_path: Path) -> None:
    """v1 turned a BOM into part of the first key; v2 reads with utf-8-sig."""
    path = tmp_path / ".env"
    path.write_bytes("﻿TELEGRAM_BOT_TOKEN=abc\nB=2\n".encode())
    env: dict[str, str] = {}
    assert config.load_dotenv(path, environ=env) is True
    assert env == {"TELEGRAM_BOT_TOKEN": "abc", "B": "2"}


class _StrictEnv(dict):
    """Mimics os.environ, which rejects NUL characters."""

    def __setitem__(self, key: str, value: str) -> None:
        if "\x00" in key or "\x00" in value:
            raise ValueError("embedded null byte")
        super().__setitem__(key, value)


def test_load_dotenv_nul_value_is_skipped_not_fatal(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    path = tmp_path / ".env"
    path.write_text("A=1\nBAD=sec\x00ret\nC=3\n")
    env = _StrictEnv()
    with caplog.at_level(logging.WARNING, logger="ynab_helper.config"):
        assert config.load_dotenv(path, environ=env) is True
    assert env == {"A": "1", "C": "3"}
    assert "BAD" in caplog.text
    assert "sec" not in caplog.text


# ─── Settings.from_env ───────────────────────────────────────────────────────


def test_defaults_from_empty_environment() -> None:
    s = Settings.from_env({"HOME": "/home/u"})
    assert s.problems == []
    assert s.data_dir == config.DEFAULT_DATA_DIR
    assert s.csv_dir == Path("/home/u/Downloads")
    assert s.log == config.LogSettings(level="INFO", file=Path("/home/u/.revolut_to_ynab.log"))
    assert s.telegram == config.TelegramSettings(
        bot_token="", admin_id=None, api_base="https://api.telegram.org"
    )
    assert s.ibkr.base_url == "https://localhost:5050"
    assert s.build == config.BuildInfo(sha="dev", date="local")
    assert (s.app_uid, s.app_gid) == (10001, 10001)
    assert s.ynab == config.YnabSettings()
    assert s.crypto == config.CryptoSettings()


def test_production_like_environment() -> None:
    env = {
        "TELEGRAM_BOT_TOKEN": " 123456:AAbbCC ",
        "TELEGRAM_ADMIN_ID": "59213669",
        "YNAB_TOKEN": "ynab-secret-token",
        "YNAB_BUDGET_ID": "budget-uuid",
        "YNAB_ACCOUNT_ID": "account-uuid",
        "YNAB_CRYPTO_ACCOUNT_ID": "crypto-uuid",
        "CRYPTO_BTC_XPUB": "xpub6CUGRUonZSQ4TWtTMmzXdrXDtyPWKiKbERr4d5qkSmh5h17C1TjvMt7DJ9Qve4dRxm91CDv6cNfKsq2mK1rMsJKhtRUPZz7MQtp3y6atC1U",
        "CRYPTO_ETH_ADDRESS": "0x0000000000000000000000000000000000000001",
        "LOG_LEVEL": "debug",
        "LOG_FILE": "/app/bot_data/bot.log",
        "WEB_UI_ENABLED": "1",
        "WEB_UI_PUBLIC_URL": "https://bot.example.com",
        "WEB_UI_SECRET_KEY": "s" * 48,
        "BOT_DATA_DIR": "/app/bot_data",
    }
    s = Settings.from_env(env)
    assert s.problems == []
    assert s.telegram.bot_token == "123456:AAbbCC"
    assert s.telegram.admin_id == 59213669
    assert s.ynab.complete
    assert s.crypto.account_id == "crypto-uuid"
    assert s.log == config.LogSettings(level="DEBUG", file=Path("/app/bot_data/bot.log"))
    assert s.data_dir == Path("/app/bot_data")
    assert s.users_db_path == Path("/app/bot_data/bot_users.db")
    assert s.heartbeat_path == Path("/app/bot_data/.heartbeat")
    assert s.backups_dir == Path("/app/bot_data/backups")
    assert s.csv_cache_dir == Path("/app/bot_data/csv_cache")
    assert s.tx_db_path(59213669) == Path("/app/bot_data/transactions_59213669.db")
    assert s.validate_for_bot() == []
    # The web layer parses WEB_UI_* from the same snapshot.
    assert s.environ["WEB_UI_PUBLIC_URL"] == "https://bot.example.com"


def test_environ_is_a_read_only_snapshot() -> None:
    env = {"A": "1"}
    s = Settings.from_env(env)
    env["A"] = "2"
    assert s.environ["A"] == "1"
    with pytest.raises(TypeError):
        s.environ["A"] = "3"  # type: ignore[index]


def test_from_env_defaults_to_os_environ(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELEGRAM_ADMIN_ID", "42")
    assert Settings.from_env().telegram.admin_id == 42


def test_secrets_never_in_repr() -> None:
    env = {
        "TELEGRAM_BOT_TOKEN": "tg-secret-value",
        "YNAB_TOKEN": "ynab-secret-value",
        "ANKR_API_KEY": "ankr-secret-value",
        "CRYPTO_BTC_XPUB": "xpub-secret-value",
        "WEB_UI_SECRET_KEY": "web-secret-value",
    }
    text = repr(Settings.from_env(env))
    for value in env.values():
        assert value not in text


def test_secret_values_lists_configured_secrets() -> None:
    env = {
        "TELEGRAM_BOT_TOKEN": "tg-secret-value",
        "YNAB_TOKEN": "ynab-secret-value",
        "WEB_UI_SECRET_KEY": "web-secret-value",
    }
    assert set(Settings.from_env(env).secret_values()) == set(env.values())
    assert Settings.from_env({}).secret_values() == ()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, Path("/h/.revolut_to_ynab.log")),
        ("", None),
        ("   ", None),
        ("~/logs/x.log", Path("/h/logs/x.log")),
        ("/abs/bot.log", Path("/abs/bot.log")),
    ],
)
def test_log_file_semantics(raw: str | None, expected: Path | None) -> None:
    env = {"HOME": "/h"}
    if raw is not None:
        env["LOG_FILE"] = raw
    assert Settings.from_env(env).log.file == expected


@pytest.mark.parametrize(
    ("raw", "level", "problem"),
    [
        ("info", "INFO", False),
        ("WARN", "WARNING", False),
        ("warn", "WARNING", False),
        (" error ", "ERROR", False),
        ("CRITICAL", "CRITICAL", False),
        ("FATAL", "CRITICAL", False),
        ("fatal", "CRITICAL", False),
        ("NOTSET", "INFO", True),
        ("verbose", "INFO", True),
        ("10", "INFO", True),
    ],
)
def test_log_level(raw: str, level: str, problem: bool) -> None:
    s = Settings.from_env({"LOG_LEVEL": raw})
    assert s.log.level == level
    assert bool(s.problems_for("LOG_LEVEL")) is problem


@pytest.mark.parametrize(
    "raw",
    ["debug", "INFO", "Warning", "WARN", "ERROR", "CRITICAL", "FATAL", "NOTSET", "loud", "10"],
)
def test_log_level_agrees_with_logging_setup(raw: str) -> None:
    """Review: config rejected FATAL while logging_setup.resolve_level accepted it."""
    s = Settings.from_env({"LOG_LEVEL": raw})
    number, valid = logging_setup.resolve_level(raw)
    assert valid is not bool(s.problems_for("LOG_LEVEL"))
    assert logging.getLevelName(number) == s.log.level
    assert config.normalize_log_level(raw) == (s.log.level if valid else None)


def test_csv_dir_expands_home_and_empty_means_default() -> None:
    s = Settings.from_env({"HOME": "/h", "CSV_DIR": "~/csv"})
    assert s.csv_dir == Path("/h/csv")
    s = Settings.from_env({"HOME": "/h", "CSV_DIR": "", "BOT_DATA_DIR": "  "})
    assert s.csv_dir == Path("/h/Downloads")
    assert s.data_dir == config.DEFAULT_DATA_DIR


@pytest.mark.parametrize("raw", ["/app/bot_data", "bot_data", "./bot_data", "~/data", "a/../b"])
def test_bot_data_dir_keeps_v1_meaning(raw: str) -> None:
    """v1: Path(os.environ.get("BOT_DATA_DIR", default)) — verbatim, no '~' expansion.

    Expanding '~' made the directory depend on HOME, which the container entrypoint changes
    during the privilege drop (review: entrypoint and bot used different directories).
    """
    assert Settings.from_env({"HOME": "/h", "BOT_DATA_DIR": raw}).data_dir == Path(raw)


@pytest.mark.parametrize(
    ("name", "raw"),
    [
        ("BOT_DATA_DIR", "/app/bot\x00data"),
        ("CSV_DIR", "~\x00"),
        ("CSV_DIR", "/x\x00"),
        ("LOG_FILE", "~\x00"),
        ("LOG_FILE", "/var/log/\x00bot.log"),
    ],
)
def test_nul_in_path_is_a_problem_not_a_crash(name: str, raw: str) -> None:
    """Review fuzz: '~\\x00' without HOME made os.path.expanduser raise ValueError."""
    s = Settings.from_env({name: raw})  # no HOME on purpose
    assert len(s.problems_for(name)) == 1
    assert "NUL" in s.problems_for(name)[0]
    for path in (s.data_dir, s.csv_dir, s.log.file):
        assert path is not None and "\x00" not in str(path)


def test_home_with_nul_is_ignored_for_tilde_defaults() -> None:
    s = Settings.from_env({"HOME": "/h\x00x"})
    for path in (s.csv_dir, s.log.file):
        assert path is not None and "\x00" not in str(path)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://api.telegram.org", "https://api.telegram.org"),
        ("http://127.0.0.1:8081/", "http://127.0.0.1:8081"),
    ],
)
def test_telegram_api_base_valid(raw: str, expected: str) -> None:
    s = Settings.from_env({"TELEGRAM_API_BASE": raw})
    assert s.telegram.api_base == expected
    assert s.problems == []


@pytest.mark.parametrize("raw", ["api.telegram.org", "ftp://x", "https://a b"])
def test_telegram_api_base_invalid_blocks_bot(raw: str) -> None:
    s = Settings.from_env(
        {"TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_ADMIN_ID": "1", "TELEGRAM_API_BASE": raw}
    )
    assert s.problems_for("TELEGRAM_API_BASE")
    assert s.validate_for_bot() == s.problems_for("TELEGRAM_API_BASE")


# ─── Malformed values never raise ────────────────────────────────────────────
# F60 itself is a WEB_UI_* crash in web/config.py. This module provides the never-raising
# building blocks (parse_int/parse_bool, Settings.from_env); the end-to-end F60 regression
# (WEB_UI_SESSION_TTL=30m → web disabled, bot keeps running) belongs to web/config.py.


@pytest.mark.parametrize(
    ("name", "raw", "default"),
    [
        ("WEB_UI_SESSION_TTL", "30m", 1800),
        ("WEB_UI_LOGIN_TTL", "5min", 300),
        ("WEB_UI_SESSION_ABS_TTL", "12h", 43200),
        ("WEB_UI_PORT", "80a", 8080),
    ],
)
def test_F60_parse_int_turns_the_crashing_web_values_into_problems(
    name: str, raw: str, default: int
) -> None:
    """v1: int('30m') raised ValueError at startup and crash-looped the whole bot."""
    problems: list[str] = []
    value = config.parse_int(raw, name=name, default=default, problems=problems, minimum=1)
    assert value == default
    assert len(problems) == 1 and problems[0].startswith(f"{name} ")


_MALFORMED = [
    "30m",
    "abc",
    "1.5",
    "1e3",
    "0x10",
    "1_000",
    "٣",  # Arabic-Indic digit: int() accepts it, we do not
    "-",
    "+",
    " ",
    "\x00",
    "9" * 19,  # beyond int64: SQLite would raise OverflowError later
    "9" * 5000,  # int() raises ValueError beyond 4300 digits
    "-5",
    "0",
    "nan",
    "∞",
]


@pytest.mark.parametrize("raw", _MALFORMED, ids=lambda v: repr(v)[:16])
def test_settings_malformed_values_never_raise(raw: str) -> None:
    env = {name: raw for name in ("TELEGRAM_ADMIN_ID", "APP_UID", "APP_GID", "LOG_LEVEL")}
    env.update(
        {
            "WEB_UI_PORT": raw,
            "WEB_UI_SESSION_TTL": raw,
            "HEALTHCHECK_MAX_AGE": raw,
            "BUILD_SHA": raw,
            "BUILD_DATE": raw,
            "IBKR_BASE_URL": raw,
            "TELEGRAM_API_BASE": raw,
        }
    )
    s = Settings.from_env(env)  # must not raise
    assert (s.app_uid, s.app_gid) == (10001, 10001)
    assert s.telegram.admin_id is None
    assert s.validate_for_bot()  # but the bot refuses to start with a bad admin id


def test_settings_randomized_environment_never_raises() -> None:
    rng = random.Random(1234)
    alphabet = "0123456789-+_.:/ abcxyzXYZ\t\x00é∞\n#'\"=~TZ"
    names = [
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_ADMIN_ID",
        "TELEGRAM_API_BASE",
        "APP_UID",
        "APP_GID",
        "LOG_LEVEL",
        "LOG_FILE",
        "CSV_DIR",
        "BOT_DATA_DIR",
        "IBKR_BASE_URL",
        "BUILD_SHA",
        "BUILD_DATE",
        "HOME",
        "YNAB_TOKEN",
    ]
    for _ in range(500):
        env = {
            name: "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 12)))
            for name in names
            if rng.random() < 0.7
        }
        s = Settings.from_env(env)
        s.validate_for_bot()
        for mode in config.CLI_MODES:
            s.validate_for_cli(mode)
        version.version_line(env)
        version.cache_bust_token(env)


def test_build_date_fuzz_near_datetime_limits_never_raises() -> None:
    """Review fuzz: timezone-aware dates in year 1 / 9999 overflowed in astimezone()."""
    rng = random.Random(4321)
    for _ in range(2000):
        year = rng.choice([1, 2, 1970, 2026, 9998, 9999])
        stamp = f"{year:04d}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}T"
        stamp += f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}"
        offset = rng.choice(
            ["Z", "", f"{rng.choice('+-')}{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}"]
        )
        env = {"BUILD_DATE": stamp + offset}
        info = config.BuildInfo.from_env(env)
        assert info.date
        assert version.version_line(env).startswith("v")
        Settings.from_env(env)


@pytest.mark.parametrize(
    ("raw", "expected", "problem"),
    [
        (None, 7, False),
        ("", 7, False),
        (" 42 ", 42, False),
        ("+3", 3, False),
        ("-3", 7, True),  # below minimum
        ("100", 7, True),  # above maximum
        ("30m", 7, True),
        ("1_000", 7, True),
        ("٣", 7, True),
        ("9" * 5000, 7, True),
        ("0" * 5000 + "5", 5, False),
        ("-0", 0, False),
    ],
    ids=lambda v: repr(v)[:16],
)
def test_parse_int(raw: str | None, expected: int, problem: bool) -> None:
    problems: list[str] = []
    value = config.parse_int(raw, name="X", default=7, problems=problems, minimum=0, maximum=99)
    assert value == expected
    assert bool(problems) is problem
    if problems:
        assert problems[0].startswith("X ")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, False),
        ("", False),
        ("1", True),
        ("true", True),
        (" YES ", True),
        ("on", True),
        ("0", False),
        ("off", False),
        ("no", False),
        ("enabled", None),
    ],
)
def test_parse_bool(raw: str | None, expected: bool | None) -> None:
    assert config.parse_bool(raw) is expected


def test_long_malformed_values_are_not_echoed_in_full() -> None:
    """A secret pasted into the wrong variable must not end up in the logs."""
    secret = "123456789:AAH-very-secret-bot-token-value-xyz"
    s = Settings.from_env({"TELEGRAM_ADMIN_ID": secret})
    text = " ".join(s.problems)
    assert "very-secret" not in text
    assert "characters" in text


# ─── APP_UID / APP_GID (F70 support) ─────────────────────────────────────────


def test_app_ids_parse() -> None:
    s = Settings.from_env({"APP_UID": "2000", "APP_GID": "3000"})
    assert (s.app_uid, s.app_gid) == (2000, 3000)


@pytest.mark.parametrize("raw", ["0", "-1", "root", "4294967295"])
def test_app_uid_never_zero_or_malformed(raw: str) -> None:
    """A typo must never make the entrypoint stay root (uid 0) or crash."""
    s = Settings.from_env({"APP_UID": raw, "APP_GID": raw})
    assert (s.app_uid, s.app_gid) == (10001, 10001)
    assert len(s.problems_for("APP_UID", "APP_GID")) == 2


# ─── validation ──────────────────────────────────────────────────────────────


def test_validate_for_bot_requires_token_and_admin() -> None:
    errors = Settings.from_env({}).validate_for_bot()
    assert len(errors) == 2
    assert "TELEGRAM_BOT_TOKEN" in errors[0]
    assert "TELEGRAM_ADMIN_ID" in errors[1]


@pytest.mark.parametrize("raw", ["abc", "-5", "0", "12.5", "@admin"])
def test_validate_for_bot_rejects_non_integer_admin(raw: str) -> None:
    errors = Settings.from_env(
        {"TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_ADMIN_ID": raw}
    ).validate_for_bot()
    assert len(errors) == 1
    assert "TELEGRAM_ADMIN_ID" in errors[0]


def test_validate_for_bot_ok_ignores_unrelated_problems() -> None:
    s = Settings.from_env(
        {
            "TELEGRAM_BOT_TOKEN": "t",
            "TELEGRAM_ADMIN_ID": " 42 ",
            "LOG_LEVEL": "loud",
            "APP_UID": "x",
            "WEB_UI_PORT": "nope",
        }
    )
    assert s.telegram.admin_id == 42
    assert s.validate_for_bot() == []
    assert len(s.problems) == 2  # LOG_LEVEL, APP_UID — web vars belong to web/config.py


def test_validate_for_cli_modes() -> None:
    empty = Settings.from_env({})
    assert empty.validate_for_cli("db-stats") == []
    assert len(empty.validate_for_cli("list-budgets")) == 1
    assert len(empty.validate_for_cli("list-accounts")) == 2
    for mode in ("import", "sync", "watch", "reconcile", "cleanup-pending-memos"):
        assert len(empty.validate_for_cli(mode)) == 3, mode

    full = Settings.from_env({"YNAB_TOKEN": "t", "YNAB_BUDGET_ID": "b", "YNAB_ACCOUNT_ID": "a"})
    for mode in ("import", "sync", "list-accounts", "list-budgets", "reconcile"):
        assert full.validate_for_cli(mode) == []
    crypto_errors = full.validate_for_cli("crypto-sync")
    assert any("YNAB_CRYPTO_ACCOUNT_ID" in e for e in crypto_errors)
    assert any("CRYPTO_BTC_XPUB" in e for e in crypto_errors)
    assert any("YNAB_BROKERAGE_ACCOUNT_ID" in e for e in full.validate_for_cli("brokerage-sync"))

    crypto = dataclasses.replace(
        full, crypto=config.CryptoSettings(account_id="c", eth_address="0xabc")
    )
    assert crypto.validate_for_cli("crypto-sync") == []


def test_validate_for_cli_reports_bad_ibkr_url() -> None:
    s = Settings.from_env(
        {
            "YNAB_TOKEN": "t",
            "YNAB_BUDGET_ID": "b",
            "YNAB_BROKERAGE_ACCOUNT_ID": "x",
            "IBKR_BASE_URL": "localhost:5050",
        }
    )
    assert s.ibkr.base_url == config.DEFAULT_IBKR_BASE_URL
    errors = s.validate_for_cli("brokerage-sync")
    assert len(errors) == 1 and "IBKR_BASE_URL" in errors[0]


def test_validate_for_cli_unknown_mode() -> None:
    with pytest.raises(ValueError, match="unknown CLI mode"):
        Settings.from_env({}).validate_for_cli("explode")


def test_tx_db_path_rejects_non_int() -> None:
    s = Settings.from_env({})
    with pytest.raises(TypeError):
        s.tx_db_path("1/../../etc")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        s.tx_db_path(True)


# ─── BuildInfo ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("env", "sha", "date"),
    [
        ({}, "dev", "local"),
        ({"BUILD_SHA": "", "BUILD_DATE": ""}, "dev", "local"),
        ({"BUILD_SHA": "0123456789abcdef"}, "0123456", "local"),
        ({"BUILD_SHA": " abc1234 "}, "abc1234", "local"),
        ({"BUILD_SHA": "<script>"}, "script", "local"),
        ({"BUILD_SHA": "../.."}, "dev", "local"),
        # Review: the sha was cut to 7 characters before unsafe ones were dropped.
        ({"BUILD_SHA": "a-b-c-d-e-f-g-h"}, "abcdefg", "local"),
        ({"BUILD_SHA": "v2.0.0-abc1234"}, "v200abc", "local"),
        ({"BUILD_SHA": "--abc1234def"}, "abc1234", "local"),
        # Review: converting these to UTC leaves the datetime range (OverflowError).
        (
            {"BUILD_DATE": "0001-01-01T00:00:00+01:00"},
            "dev",
            "0001-01-01T00:00:00+01:00",
        ),
        (
            {"BUILD_DATE": "9999-12-31T23:59:59-01:00"},
            "dev",
            "9999-12-31T23:59:59-01:00",
        ),
        ({"BUILD_DATE": "0001-01-01T01:00:00+01:00"}, "dev", "0001-01-01 00:00 UTC"),
        ({"BUILD_DATE": "2026-10-16T20:00:00Z"}, "dev", "2026-10-16 20:00 UTC"),
        ({"BUILD_DATE": "2026-10-16T22:00:00+02:00"}, "dev", "2026-10-16 20:00 UTC"),
        ({"BUILD_DATE": "2026-10-16 20:00 UTC"}, "dev", "2026-10-16 20:00 UTC"),
        ({"BUILD_DATE": "2026-10-16"}, "dev", "2026-10-16"),
        ({"BUILD_DATE": "a\nb\x1bc"}, "dev", "abc"),
        ({"BUILD_DATE": "x" * 100}, "dev", "x" * 40),
    ],
)
def test_build_info(env: dict[str, str], sha: str, date: str) -> None:
    assert config.BuildInfo.from_env(env) == config.BuildInfo(sha=sha, date=date)


def test_build_info_is_release() -> None:
    assert not config.BuildInfo().is_release
    assert not config.BuildInfo(sha="local").is_release
    assert config.BuildInfo(sha="abc1234").is_release
