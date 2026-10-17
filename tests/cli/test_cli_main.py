"""cli.main: v1 flags, mode precedence, ignored-flag warnings, settings and exit codes."""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from tests.fakes.fake_ynab import FAKE_TOKEN, FakeYnab
from tests.legacy import v1_revolut_to_ynab as v1
from ynab_helper import cli, config, services

from .conftest import ACCT, BUDGET, OTHER, Cli, live, mutations

#: Every v1 option with a sample value (store_true options get None).
V1_OPTIONS: dict[str, str | None] = {
    "--token": "t",
    "--budget-id": "b",
    "--account-id": "a",
    "--list-budgets": None,
    "--list-accounts": None,
    "--watch": "folder",
    "--sync": None,
    "--since-date": "2026-01-01",
    "--dry-run": None,
    "--skip-pending": None,
    "--db-stats": None,
    "--db-path": "x.db",
    "--crypto-sync": None,
    "--crypto-account-id": "c",
    "--btc-xpub": "xpub",
    "--eth-address": "0x0",
    "--brokerage-sync": None,
    "--brokerage-account-id": "i",
    "--ibkr-account-id": "U1",
    "--ibkr-base-url": "https://localhost:5050",
    "--reconcile": None,
    "--cleanup-pending-memos": None,
    "--log-level": "DEBUG",
    "--log-file": "x.log",
    "--csv-dir": "dir",
    "--yes": None,
}


def _parse(*argv: str) -> argparse.Namespace:
    return cli.build_parser().parse_args(list(argv))


# ── parsing and precedence ──────────────────────────────────────────────────


def test_every_v1_option_is_accepted():
    argv: list[str] = ["statement.csv"]
    for option, value in V1_OPTIONS.items():
        argv += [option] if value is None else [option, value]
    args = _parse(*argv, "-y")
    assert args.csv_file == "statement.csv" and args.yes is True
    for option, value in V1_OPTIONS.items():
        dest = option.lstrip("-").replace("-", "_")
        assert getattr(args, dest) == (True if value is None else value), option


@pytest.mark.parametrize(
    ("argv", "mode"),
    [
        ((), "import"),
        (("statement.csv",), "import"),
        (("--db-stats", "--sync", "--list-budgets"), "db-stats"),
        (("--list-budgets", "--list-accounts"), "list-budgets"),
        (("--list-accounts", "--sync"), "list-accounts"),
        (("--sync", "--crypto-sync"), "sync"),
        (("--crypto-sync", "--brokerage-sync"), "crypto-sync"),
        (("--brokerage-sync", "--watch", "d"), "brokerage-sync"),
        (("--watch", "d", "--cleanup-pending-memos"), "watch"),
        (("--cleanup-pending-memos", "--reconcile"), "cleanup-pending-memos"),
        (("--reconcile", "statement.csv"), "reconcile"),
    ],
)
def test_modes_follow_the_v1_precedence(argv, mode):
    assert cli.select_mode(_parse(*argv)) == mode


def test_help_exits_zero_and_creates_nothing(run_cli: Cli, tmp_path: Path, fake: FakeYnab):
    code, out, _err = run_cli.run("--help")
    assert code == cli.EXIT_OK
    assert "usage: revolut_to_ynab.py" in out and "--cleanup-pending-memos" in out
    assert not (tmp_path / "cli.log").exists() and not run_cli.db.exists()
    assert fake.calls == []


def test_an_unknown_option_is_a_usage_error(run_cli: Cli, fake: FakeYnab):
    code, _out, err = run_cli.run("--no-such-option")
    assert code == cli.EXIT_USAGE and "unrecognized arguments" in err
    assert fake.calls == []


# ── ignored flags ───────────────────────────────────────────────────────────


def test_flags_the_chosen_mode_ignores_are_reported(run_cli: Cli, fake: FakeYnab):
    code, out, err = run_cli.run(
        "--sync", "--reconcile", "--since-date", "2026-01-01", "--skip-pending", "-y"
    )
    assert code == cli.EXIT_OK  # the sync ran
    assert "--reconcile is ignored: --sync runs instead" in err
    assert "--since-date is ignored with --sync: v2 keeps the whole budget in sync" in err
    assert "--skip-pending is ignored with --sync." in err
    assert "--yes is ignored with --sync." in err
    assert "--db-path" not in err  # used by --sync
    assert "Synced budget" in out


def test_options_an_import_uses_are_not_reported(run_cli: Cli):
    statement = run_cli.statement()
    code, _out, err = run_cli.run(str(statement), "--dry-run", "--skip-pending", "-y")
    assert code == cli.EXIT_OK and "Warning" not in err


def test_an_import_reports_valuation_options(run_cli: Cli):
    statement = run_cli.statement()
    code, _out, err = run_cli.run(str(statement), "--dry-run", "--btc-xpub", "xpub")
    assert code == cli.EXIT_OK
    assert "--btc-xpub is ignored with an import." in err


# ── settings ────────────────────────────────────────────────────────────────


def test_a_missing_token_is_a_configuration_error(run_cli: Cli, fake: FakeYnab):
    del run_cli.env["YNAB_TOKEN"]
    code, _out, err = run_cli.run("--sync")
    assert code == cli.EXIT_USAGE and "YNAB_TOKEN" in err
    assert fake.calls == []


@pytest.mark.parametrize(
    ("argv", "missing"),
    [
        (("--sync",), "YNAB_ACCOUNT_ID"),
        (("--reconcile",), "YNAB_ACCOUNT_ID"),
        (("--cleanup-pending-memos",), "YNAB_ACCOUNT_ID"),
        (("--watch", "."), "YNAB_ACCOUNT_ID"),
        (("statement.csv",), "YNAB_ACCOUNT_ID"),
        (("--crypto-sync",), "YNAB_CRYPTO_ACCOUNT_ID"),
        (("--brokerage-sync",), "YNAB_BROKERAGE_ACCOUNT_ID"),
    ],
)
def test_each_mode_names_the_setting_it_misses(run_cli: Cli, fake: FakeYnab, argv, missing):
    del run_cli.env["YNAB_ACCOUNT_ID"]
    code, _out, err = run_cli.run(*argv)
    assert code == cli.EXIT_USAGE and missing in err
    assert fake.calls == []


def test_list_accounts_needs_a_budget(run_cli: Cli, fake: FakeYnab):
    del run_cli.env["YNAB_BUDGET_ID"]
    code, _out, err = run_cli.run("--list-accounts")
    assert code == cli.EXIT_USAGE and "YNAB_BUDGET_ID" in err


def test_options_override_the_environment(run_cli: Cli, fake: FakeYnab):
    statement = run_cli.statement()
    code, _out, _err = run_cli.run(str(statement), "--account-id", OTHER)
    assert code == cli.EXIT_OK
    assert live(fake, OTHER) and not live(fake, ACCT)


def test_the_env_file_fills_in_what_the_shell_does_not_set(run_cli: Cli, fake: FakeYnab):
    del run_cli.env["YNAB_ACCOUNT_ID"]
    run_cli.env["YNAB_BUDGET_ID"] = BUDGET
    (run_cli.home / "absent.env").write_text(
        f"YNAB_ACCOUNT_ID={OTHER}\nYNAB_BUDGET_ID=from-file\n", encoding="utf-8"
    )
    statement = run_cli.statement()
    code, _out, _err = run_cli.run(str(statement))
    assert code == cli.EXIT_OK
    assert live(fake, OTHER)  # from the file; the shell's budget id won over the file's


def test_logging_goes_to_the_configured_file_without_the_token(
    run_cli: Cli, tmp_path: Path, fake: FakeYnab
):
    code, _out, _err = run_cli.run("--sync", "--log-level", "debug", "--token", FAKE_TOKEN)
    assert code == cli.EXIT_OK
    text = (tmp_path / "cli.log").read_text(encoding="utf-8")
    assert "cli: mode=sync" in text and FAKE_TOKEN not in text


def test_an_empty_log_file_option_disables_file_logging(run_cli: Cli, tmp_path: Path):
    code, _out, _err = run_cli.run("--sync", "--log-file", "")
    assert code == cli.EXIT_OK and not (tmp_path / "cli.log").exists()


def test_an_invalid_log_level_is_reported_and_info_used(run_cli: Cli):
    code, _out, err = run_cli.run("--sync", "--log-level", "LOUD")
    assert code == cli.EXIT_OK and "--log-level 'LOUD' is not a log level" in err


def test_the_umask_is_private_while_running_and_restored(run_cli: Cli):
    import os

    before = os.umask(0o022)
    os.umask(before)
    code, _out, _err = run_cli.run("--sync")
    assert code == cli.EXIT_OK
    assert run_cli.db.stat().st_mode & 0o077 == 0  # created 0600
    now = os.umask(before)
    assert now == before


# ── exit codes ──────────────────────────────────────────────────────────────


def test_a_ynab_failure_exits_two(run_cli: Cli, fake: FakeYnab):
    fake.fail_next("GET", rf"/budgets/{BUDGET}/accounts", 500, times=5)
    code, _out, err = run_cli.run("--sync")
    assert code == cli.EXIT_FAILED and err.startswith("Error:")


def test_a_rejected_token_exits_two_with_its_message(run_cli: Cli, fake: FakeYnab):
    fake.fail_next("GET", None, 401)
    code, _out, err = run_cli.run("--list-budgets")
    assert code == cli.EXIT_FAILED
    assert "Error: YNAB rejected the access token" in err and FAKE_TOKEN not in err


def test_ctrl_c_exits_130(run_cli: Cli, monkeypatch: pytest.MonkeyPatch):
    def interrupted(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(services, "sync", interrupted)
    code, _out, err = run_cli.run("--sync")
    assert code == cli.EXIT_INTERRUPTED and "Interrupted" in err


def test_an_unexpected_error_exits_two_and_is_logged(
    run_cli: Cli, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    def broken(*args: object, **kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(services, "sync", broken)
    code, _out, err = run_cli.run("--sync")
    assert code == cli.EXIT_FAILED and "unexpected failure" in err
    assert "RuntimeError: boom" in (tmp_path / "cli.log").read_text(encoding="utf-8")


def test_another_operation_on_the_same_db_exits_two(run_cli: Cli, fake: FakeYnab):
    from ynab_helper.locks import LOCKS

    with LOCKS.operation(str(run_cli.db), "import"):
        code, _out, err = run_cli.run("--sync")
    assert code == cli.EXIT_FAILED and "Another operation (import)" in err
    assert fake.calls == []


def test_no_v1_mode_writes_when_only_listing(run_cli: Cli, fake: FakeYnab):
    expected = {
        "--list-budgets": cli.EXIT_OK,
        "--list-accounts": cli.EXIT_OK,
        "--db-stats": cli.EXIT_USAGE,  # no database yet: reported, and none is created
    }
    for option, exit_code in expected.items():
        code, _out, _err = run_cli.run(option)
        assert code == exit_code, option
    assert mutations(fake) == []
    assert not run_cli.db.exists()


def test_a_file_that_is_not_a_database_exits_two(run_cli: Cli, tmp_path: Path, fake: FakeYnab):
    junk = tmp_path / "junk.db"
    junk.write_bytes(b"this is not a SQLite database" * 64)
    for argv in (("--sync",), ("--db-stats",)):
        code, _out, err = run_cli.run(*argv, "--db-path", str(junk))
        assert code == cli.EXIT_FAILED and err.startswith("Error:"), argv
        assert "unexpected" not in err
    assert fake.calls == []


# ── backups of a migrated database ──────────────────────────────────────────


def test_a_database_outside_the_bot_data_dir_is_backed_up_next_to_itself(tmp_path: Path):
    settings = config.Settings.from_env(
        {"HOME": str(tmp_path), "BOT_DATA_DIR": str(tmp_path / "bot_data")}
    )
    assert cli.backup_dir_for(tmp_path / "bot_data" / "transactions_42.db", settings) == (
        settings.backups_dir
    )
    assert cli.backup_dir_for(tmp_path / ".revolut_to_ynab.db", settings) is None


def test_the_cli_backs_up_a_bot_users_database_where_the_bot_keeps_its_backups(
    run_cli: Cli, tmp_path: Path, fake: FakeYnab
):
    data = tmp_path / "bot_data"
    data.mkdir()
    run_cli.env["BOT_DATA_DIR"] = str(data)
    db = data / "transactions_42.db"
    v1.init_db(str(db)).close()  # a v1 database: the first v2 open migrates it
    code, _out, _err = run_cli.run("--sync", "--db-path", str(db))
    assert code == cli.EXIT_OK
    backups = sorted(p.name for p in (data / "backups").iterdir())
    assert len(backups) == 1 and backups[0].startswith("transactions_42.db.")
    assert backups[0].endswith(".pre-v1.db")
    assert not (data / "transactions_42_backups").exists()
