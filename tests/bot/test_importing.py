"""CSV import end to end: FakeTelegram → bot → services → FakeYnab (F06, F15, F16, F19, F47)."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.bot.conftest import ACCT, BUDGET, USER, deliver, ready_user
from tests.fakes.fake_telegram import FakeTelegram
from tests.fakes.fake_ynab import FakeYnab
from ynab_helper.bot import render
from ynab_helper.bot.app import Bot
from ynab_helper.bot.users import UsersRepo
from ynab_helper.locks import LOCKS

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
BASIC = (FIXTURES / "export_basic.csv").read_bytes()


def send_csv(
    bot: Bot, tg: FakeTelegram, content: bytes, name: str = "account-statement.csv"
) -> None:
    tg.document(USER, name, content)
    deliver(bot, tg)


@pytest.fixture
def user(users: UsersRepo):
    return ready_user(users)


@pytest.mark.usefixtures("user")
def test_import_end_to_end(bot: Bot, tg: FakeTelegram, ynab: FakeYnab, data_dir: Path):
    send_csv(bot, tg, BASIC)
    texts = tg.texts(USER)
    assert texts[0].startswith("📥 Importing account-statement.csv")
    report = texts[1]
    assert report.startswith("✅ Import complete — account-statement.csv")
    assert "2026-10-01 → 2026-10-13" in report
    created = len([t for t in ynab.transactions(BUDGET, account_id=ACCT)])
    assert f"New: {created}" in report and created > 10
    assert texts[2].startswith("💰 Revolut:")
    assert "CHF" in texts[2]
    # The download was temporary; the stored copy has a server-generated name (F19).
    assert list((data_dir / "tmp").iterdir()) == []
    stored = list((data_dir / "csv_cache" / str(USER)).iterdir())
    assert len(stored) == 1 and stored[0].name != "account-statement.csv"

    send_csv(bot, tg, BASIC)
    assert tg.texts(USER)[-2].startswith("✅ Everything already up to date")


@pytest.mark.usefixtures("user")
def test_F06_F47_partial_failure_is_reported_and_converges(
    bot: Bot, tg: FakeTelegram, ynab: FakeYnab
):
    ynab.fail_next("POST", r"/budgets/[^/]+/transactions", 500, times=1)
    send_csv(bot, tg, BASIC)
    report = tg.texts(USER)[1]
    assert report.startswith("⚠ Import partially applied")
    assert "❌ Failed" in report
    assert render.PARTIAL_HINT in report
    assert "Removed from YNAB" not in report  # an error is never shown as a removal (F47)
    send_csv(bot, tg, BASIC)
    final = tg.texts(USER)[-2]
    assert final.startswith("✅ Import complete")
    assert len(ynab.transactions(BUDGET, account_id=ACCT)) > 10


@pytest.mark.usefixtures("user")
def test_rate_limit_is_explained(bot: Bot, tg: FakeTelegram, ynab: FakeYnab):
    ynab.fail_next("POST", r"/budgets/[^/]+/transactions", 429, retry_after=60)
    send_csv(bot, tg, BASIC)
    report = tg.texts(USER)[-1]
    assert "rate limit" in report and render.PARTIAL_HINT in report


@pytest.mark.usefixtures("user")
def test_F16_busy_when_another_operation_runs(bot: Bot, tg: FakeTelegram):
    with LOCKS.operation(str(USER), "web upload"):
        send_csv(bot, tg, BASIC)
    assert tg.texts(USER) == [render.BUSY_TEXT]
    assert tg.calls_of("getFile") == []


@pytest.mark.usefixtures("user")
def test_busy_inside_the_service_is_reported(
    bot: Bot, tg: FakeTelegram, monkeypatch: pytest.MonkeyPatch
):
    """The lock may be taken between the bot's check and the service call."""
    from ynab_helper import services
    from ynab_helper.errors import Busy

    def busy(*args, **kwargs):
        raise Busy("import")

    monkeypatch.setattr(services, "import_csv", busy)
    send_csv(bot, tg, BASIC)
    assert tg.texts(USER)[-1] == render.BUSY_TEXT


@pytest.mark.usefixtures("user")
@pytest.mark.parametrize("name", ["statement.pdf", "statement.csv.exe", "noext"])
def test_non_csv_names_are_refused(bot: Bot, tg: FakeTelegram, name: str):
    send_csv(bot, tg, BASIC, name)
    assert "doesn't look like a CSV" in tg.texts(USER)[-1]
    assert tg.calls_of("getFile") == []


@pytest.mark.usefixtures("user")
def test_oversized_documents_are_refused_before_download(bot: Bot, tg: FakeTelegram):
    tg.document(USER, "big.csv", None, file_size=26 * 1024 * 1024)
    deliver(bot, tg)
    assert "larger than 25 MB" in tg.texts(USER)[-1]
    assert tg.calls_of("getFile") == []


@pytest.mark.usefixtures("user")
def test_non_revolut_content_is_refused(bot: Bot, tg: FakeTelegram, ynab: FakeYnab):
    send_csv(bot, tg, b"name,amount\nfoo,1\n")
    assert "doesn't look like a Revolut" in tg.texts(USER)[-1]
    assert ynab.transactions(BUDGET, account_id=ACCT) == []


@pytest.mark.usefixtures("user")
def test_currency_mismatch_is_refused_with_the_service_message(
    bot: Bot, tg: FakeTelegram, ynab: FakeYnab
):
    send_csv(bot, tg, (FIXTURES / "export_eur.csv").read_bytes())
    assert "EUR" in tg.texts(USER)[-1] and "Nothing was imported" in tg.texts(USER)[-1]
    assert ynab.transactions(BUDGET, account_id=ACCT) == []


@pytest.mark.usefixtures("user")
def test_download_failure_is_reported(bot: Bot, tg: FakeTelegram):
    tg.fail_next("getFile", 400, "Bad Request: file is too big")
    send_csv(bot, tg, BASIC)
    assert "could not be downloaded" in tg.texts(USER)[-1]


@pytest.mark.usefixtures("user")
def test_hostile_file_and_payee_names_are_escaped(bot: Bot, tg: FakeTelegram, ynab: FakeYnab):
    """F91/F96: names are shown verbatim; the strict fake would reject broken HTML."""
    hostile = BASIC.replace(b"Coop Pronto", b"<b>Coop</b> & <a href='x'>Pronto</a>")
    send_csv(bot, tg, hostile, "<i>evil</i>&co.csv")
    texts = tg.texts(USER)
    assert texts[0] == "📥 Importing <i>evil</i>&co.csv…"
    assert texts[1].startswith("✅ Import complete — <i>evil</i>&co.csv")
    assert all(m.parse_mode == "HTML" for m in tg.sent(USER))  # no plain-text fallback needed


@pytest.mark.usefixtures("user")
def test_auth_error_points_to_setup(bot: Bot, tg: FakeTelegram, ynab: FakeYnab):
    ynab.fail_next("GET", r"/budgets/[^/]+/settings", 401)
    send_csv(bot, tg, BASIC)
    assert "/setup" in tg.texts(USER)[-1]
