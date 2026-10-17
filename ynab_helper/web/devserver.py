"""Local development server with realistic, fully offline data (never used in production).

    python -m ynab_helper.web.devserver [--port 8766] [--host 127.0.0.1] [--data-dir DIR]

It seeds a fresh data directory (a temporary one unless ``--data-dir`` is given) with

* ``bot_users.db`` holding one ready user (Telegram id 4242, "Dev"),
* an in-process fake YNAB (``tests/fakes/fake_ynab.py`` — run from a repository checkout)
  with a budget in CHF and the accounts *Revolut* (the import account), *UBS Private*,
  *Savings*, *Visa Card* (credit card), *Crypto Portfolio* (tracking) and a closed
  *Old PostFinance* account; about ten months of categorized transactions with salary,
  rent, groceries, dining, transport, transfers to savings, credit-card payments, a split,
  a refund, monthly subscriptions (Netflix with a price rise, Spotify, Swisscom), a yearly
  charge, crypto valuation adjustments, a reconciliation adjustment, pending and
  future-dated entries,
* a Revolut statement of the last six weeks imported through the real service layer
  (so ``csv_files``, the ledger, reconcile — with a small difference — and dedupe — with
  two stray importer transactions — all work),

then serves the real app (``make_app``) on ``--host``/``--port`` and prints a one-time
login URL. ``GET /dev/login`` (devserver only) mints a fresh link whenever needed. Every
YNAB call goes to the fake: sync, upload, reconcile and dedupe work offline.
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import random
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Final

__all__ = ["DEV_SECRET", "DEV_USER_ID", "SeededData", "main", "seed"]

log = logging.getLogger(__name__)

DEV_USER_ID: Final = 4242
#: Development-only HMAC key (the devserver never handles real data).
DEV_SECRET: Final = "devserver-secret-key-not-for-production-0123456789"
BUDGET: Final = "budget-dev"
REVOLUT: Final = "acct-revolut"
UBS: Final = "acct-ubs"
SAVINGS: Final = "acct-savings"
VISA: Final = "acct-visa"
CRYPTO: Final = "acct-crypto"
OLD: Final = "acct-old"
CSV_DAYS: Final = 42
MONTHS: Final = 10
STATEMENT_GAP: Final = 12_350  # the statement is 12.35 above YNAB: reconcile has work to do


@dataclass(slots=True)
class SeededData:
    """What :func:`seed` created."""

    data_dir: Path
    users_db_path: Path
    fake: Any  # tests.fakes.fake_ynab.FakeYnab
    statement_name: str
    stray_ids: list[str]


def _fake_ynab_class() -> Any:
    try:
        from tests.fakes.fake_ynab import FakeYnab
    except ImportError:
        from ynab_helper.config import REPO_ROOT

        if str(REPO_ROOT) not in sys.path:
            sys.path.insert(0, str(REPO_ROOT))
        try:
            from tests.fakes.fake_ynab import FakeYnab
        except ImportError as exc:  # pragma: no cover - only outside a checkout
            raise RuntimeError(
                "The devserver needs tests/fakes/fake_ynab.py: run it from a repository "
                "checkout (python -m ynab_helper.web.devserver)."
            ) from exc
    return FakeYnab


def _month_day(today: date, months_back: int, day: int) -> date:
    index = today.year * 12 + today.month - 1 - months_back
    year, month = index // 12, index % 12 + 1
    for d in range(day, 0, -1):
        try:
            return date(year, month, d)
        except ValueError:
            continue
    raise AssertionError("unreachable")  # pragma: no cover


def _seed_ynab(fake: Any, today: date, rng: random.Random) -> int:
    """Seed accounts and history; returns the Revolut cleared balance before the CSV."""
    fake.add_budget(BUDGET, currency="CHF", name="Household")
    fake.add_account(BUDGET, REVOLUT, "Revolut", "checking")
    fake.add_account(BUDGET, UBS, "UBS Private", "checking")
    fake.add_account(BUDGET, SAVINGS, "Savings", "savings")
    fake.add_account(BUDGET, VISA, "Visa Card", "creditCard")
    fake.add_account(BUDGET, CRYPTO, "Crypto Portfolio", "otherAsset", on_budget=False)
    fake.add_account(BUDGET, OLD, "Old PostFinance", "checking", closed=True)

    transfers: list[str] = []

    def add(
        account: str, day: date, amount: float, payee: str, category: str | None = None, **kw: Any
    ) -> str | None:
        if day > today and not kw.pop("future", False):
            return None
        tx_id = fake.add_transaction(
            BUDGET,
            account,
            amount=round(amount * 1000),
            date=day.isoformat(),
            payee_name=payee,
            category_name=category,
            **kw,
        )
        if kw.get("transfer_account_id") and (today - day).days > 2:
            transfers.append(tx_id)
        return tx_id

    csv_start = today - timedelta(days=CSV_DAYS)
    fake.add_transaction(
        BUDGET,
        REVOLUT,
        amount=350_000,
        date=_month_day(today, MONTHS + 1, 28).isoformat(),
        payee_name="Starting Balance",
        category_name="Inflow: Ready to Assign",
        cleared="reconciled",
    )
    revolut_opening = 350_000
    visa_spend: dict[int, int] = {}
    crypto_value = 8_000_000
    for back in range(MONTHS, -1, -1):
        add(UBS, _month_day(today, back, 25), 6_850.00, "Employer AG", "Inflow: Ready to Assign")
        add(UBS, _month_day(today, back, 1), -1_850.00, "Immo Verwaltung AG", "Rent")
        add(UBS, _month_day(today, back, 3), -94.30, "EWZ Strom", "Utilities")
        add(
            UBS,
            _month_day(today, back, 26),
            -500.00,
            "Transfer : Savings",
            transfer_account_id=SAVINGS,
        )
        if back > 0:
            payment = visa_spend.get(back + 1, 0)
            if payment:
                add(
                    UBS,
                    _month_day(today, back, 15),
                    -payment / 1000,
                    "Transfer : Visa Card",
                    transfer_account_id=VISA,
                )
        # Visa: subscriptions and shopping.
        spend = 0
        netflix = 17.90 if back <= 3 else 15.90
        for day, amount, payee, category in (
            (5, -49.90, "Swisscom (Schweiz) AG", "Phone & Internet"),
            (12, -netflix, "NETFLIX.COM" if back % 2 else "Netflix.com", "Subscriptions"),
            (20, -12.95, "Spotify", "Subscriptions"),
            (rng.randint(3, 27), -rng.choice([34.50, 61.20, 89.00, 129.90]), "Galaxus", "Shopping"),
        ):
            when = _month_day(today, back, day)
            if when > today:
                continue
            add(
                VISA,
                when,
                amount,
                payee,
                category,
                cleared="uncleared" if (today - when).days < 3 else "cleared",
            )
            spend += round(-amount * 1000)
        visa_spend[back] = spend
        # Revolut history before the statement window (imported by v1: YNAB: import_ids).
        topup = _month_day(today, back, 1)
        if topup < csv_start:
            milli = 1_200_000
            add(
                REVOLUT,
                topup,
                milli / 1000,
                "Top-Up by *4242",
                import_id=f"YNAB:{milli}:{topup.isoformat()}:1",
                cleared="reconciled",
            )
            revolut_opening += milli
        for week in range(4):
            when = _month_day(today, back, 2 + week * 7 + rng.randint(0, 2))
            if when >= csv_start or when > today:
                continue
            spends = [
                (-rng.uniform(35, 120), rng.choice(["Migros", "Coop", "Denner"]), "Groceries"),
                (-rng.uniform(18, 75), rng.choice(["Hiltl", "Tibits", "Holy Cow"]), "Dining Out"),
            ]
            if rng.random() < 0.5:
                spends.append((-rng.choice([4.40, 6.80, 8.80]), "SBB CFF FFS", "Transport"))
            for amount, payee, category in spends:
                cents = round(amount * 100) / 100
                milli = round(cents * 1000)
                add(
                    REVOLUT,
                    when,
                    cents,
                    payee,
                    category,
                    import_id=f"YNAB:{milli}:{when.isoformat()}:1",
                    cleared="reconciled",
                )
                revolut_opening += milli
        # Crypto valuation adjustments on the tracking account.
        when = _month_day(today, back, 28)
        if when <= today:
            delta = rng.randint(-600_000, 900_000)
            crypto_value += delta
            fake.add_transaction(
                BUDGET,
                CRYPTO,
                amount=delta,
                date=when.isoformat(),
                payee_name="Crypto Portfolio Adjustment",
                import_id=f"CRYPTO:{delta}:{when.isoformat()}:1",
            )

    # A split, a refund, a yearly charge, a reconciliation, pending and future entries.
    split_day = _month_day(today, 3, 14)
    fake.add_transaction(
        BUDGET,
        VISA,
        amount=-86_400,
        date=split_day.isoformat(),
        payee_name="Migros",
        subtransactions=[
            {"amount": -60_000, "category_name": "Groceries"},
            {"amount": -26_400, "category_name": "Household", "memo": "cleaning stuff"},
        ],
    )
    add(VISA, _month_day(today, 2, 6), -129.00, "Zalando", "Shopping")
    add(VISA, _month_day(today, 2, 19), 129.00, "Zalando", "Shopping", memo="Refund")
    # Yearly, newest charge five months ago: only found with the extra yearly history (F98).
    add(UBS, _month_day(today, 17, 19), -190.00, "SBB Halbtax", "Transport")
    add(UBS, _month_day(today, 5, 19), -190.00, "SBB Halbtax", "Transport")
    recon_day = csv_start - timedelta(days=20)
    fake.add_transaction(
        BUDGET,
        REVOLUT,
        amount=-2_150,
        date=recon_day.isoformat(),
        payee_name="CSV Reconciliation",
        import_id=f"RECON:-2150:{recon_day.isoformat()}:1",
        cleared="reconciled",
    )
    revolut_opening -= 2_150
    add(
        UBS,
        today + timedelta(days=9),
        -280.00,
        "Zahnarztpraxis Dr. Muster",
        "Health",
        cleared="uncleared",
        future=True,
    )
    add(OLD, _month_day(today, MONTHS, 10), -25.00, "PostFinance Gebühren", "Bank Fees")
    # YNAB creates the other side of a transfer uncleared; the user cleared the old ones.
    for tx_id in transfers:
        other = fake.tx(tx_id).get("transfer_transaction_id")
        if other:
            fake.user_edit_transaction(other, cleared="cleared")
    return revolut_opening


def _statement_rows(today: date, rng: random.Random, opening: int) -> tuple[str, bytes]:
    """A Revolut export of the last CSV_DAYS days: completed rows in completion order with
    running balances, plus pending rows (no completion date/balance) interleaved."""
    start = today - timedelta(days=CSV_DAYS)
    rows: list[dict[str, Any]] = []
    merchants = [
        ("Migros", -45.0, -110.0),
        ("Coop Pronto", -6.0, -25.0),
        ("Hiltl", -22.0, -68.0),
        ("SBB CFF FFS", -4.4, -8.8),
        ("Amavita Apotheke", -12.0, -48.0),
        ("Galaxus", -19.0, -149.0),
    ]
    day = start
    while day <= today:
        if day.day in (2, 16):
            rows.append(("Topup", day, 9, "Top-Up by *4242", 400.0))
        for _ in range(rng.randint(0, 2)):
            name, low, high = rng.choice(merchants)
            rows_amount = round(rng.uniform(high, low), 2)
            rows.append(("Card Payment", day, rng.randint(7, 21), name, rows_amount))
        day += timedelta(days=1)
    records: list[dict[str, Any]] = []
    for seq, (kind, when, hour, name, amount) in enumerate(rows, start=1):
        started = datetime(when.year, when.month, when.day, hour, (seq * 7) % 60, seq % 60)
        pending = (today - when).days < 2 and kind == "Card Payment"
        completed = None if pending else started + timedelta(hours=rng.randint(1, 30))
        records.append(
            {
                "type": kind,
                "started": started,
                "completed": completed,
                "name": name,
                "amount": amount,
                "state": "PENDING" if pending else "COMPLETED",
            }
        )
    completed_rows = sorted(
        (r for r in records if r["completed"] is not None), key=lambda r: r["completed"]
    )
    balance = opening + STATEMENT_GAP
    for r in completed_rows:
        balance += round(r["amount"] * 1000)
        r["balance"] = balance
    ordered = sorted(records, key=lambda r: (r["completed"] or r["started"], r["started"]))
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(
        [
            "Type",
            "Product",
            "Started Date",
            "Completed Date",
            "Description",
            "Amount",
            "Fee",
            "Currency",
            "State",
            "Balance",
        ]
    )
    for r in ordered:
        writer.writerow(
            [
                r["type"],
                "Current",
                r["started"].strftime("%Y-%m-%d %H:%M:%S"),
                r["completed"].strftime("%Y-%m-%d %H:%M:%S") if r["completed"] else "",
                r["name"],
                f"{r['amount']:.2f}",
                "0.00",
                "CHF",
                r["state"],
                f"{r['balance'] / 1000:.2f}" if r["completed"] else "",
            ]
        )
    name = f"account-statement_{start.isoformat()}_{today.isoformat()}_en-gb_dev.csv"
    return name, out.getvalue().encode("utf-8")


def seed(data_dir: Path, *, today: date | None = None, seed_value: int = 7) -> SeededData:
    """Create the dev user, the fake YNAB and the imported statement under ``data_dir``.

    Also points :mod:`ynab_helper.services`' client factories at the fake.
    """
    from ynab_helper import services
    from ynab_helper.services import UserContext
    from ynab_helper.storage import UserStore, open_users_db

    fake_cls = _fake_ynab_class()
    day = today or date.today()
    rng = random.Random(seed_value)
    fake = fake_cls()
    opening = _seed_ynab(fake, day, rng)
    services.client_factory = lambda ctx: fake.client(max_retries=0)
    services.token_client_factory = lambda token: fake.client(token=token, max_retries=0)

    data_dir.mkdir(parents=True, exist_ok=True)
    users_db = data_dir / "bot_users.db"
    conn = open_users_db(users_db, backup_dir=data_dir / "backups")
    try:
        user = UserStore(conn).upsert(
            DEV_USER_ID,
            state="ready",
            chat_id=DEV_USER_ID,
            username="dev",
            first_name="Dev",
            ynab_token=fake.token,
            budget_id=BUDGET,
            budget_name="Household",
            account_id=REVOLUT,
            account_name="Revolut",
            crypto_account_id=CRYPTO,
            crypto_account_name="Crypto Portfolio",
            auto_approve=True,
        )
    finally:
        conn.close()
    ctx = UserContext.from_user_row(user, data_dir)
    services.sync(ctx, force=True)
    name, content = _statement_rows(day, rng, opening)
    result = services.import_csv(ctx, content, original_name=name, source_kind="web")
    log.info("devserver: imported %s: %s", name, "ok" if result.ok else "with problems")
    # Two stray importer transactions inside the statement window: dedupe candidates.
    strays = []
    for offset, amount in ((5, -45_500), (11, -18_900)):
        when = (day - timedelta(days=offset)).isoformat()
        strays.append(
            fake.add_transaction(
                BUDGET,
                REVOLUT,
                amount=amount,
                date=when,
                payee_name="Migros (duplicate)",
                import_id=f"YNAB:{amount}:{when}:7",
                cleared="uncleared",
            )
        )
    services.sync(ctx, force=True)
    return SeededData(data_dir, users_db, fake, name, strays)


def _login_url(data: SeededData, hasher: Any, base_url: str, ttl: int) -> str:
    from ynab_helper import sessions
    from ynab_helper.storage import connect

    conn = connect(data.users_db_path)
    try:
        token = sessions.issue_login_token(conn, hasher, DEV_USER_ID, ttl_seconds=ttl)
    finally:
        conn.close()
    return f"{base_url}/auth?t={token}"


def main(argv: Sequence[str] | None = None) -> int:
    """Seed and serve until Ctrl+C."""
    parser = argparse.ArgumentParser(prog="python -m ynab_helper.web.devserver")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--data-dir", type=Path, default=None, help="default: a new temp dir")
    parser.add_argument("--seed", type=int, default=7, help="random seed of the demo data")
    args = parser.parse_args(argv)

    import uvicorn
    from starlette.responses import RedirectResponse, Response

    from ynab_helper.logging_setup import setup_logging
    from ynab_helper.version import cache_bust_token, version_line
    from ynab_helper.web.app import make_app
    from ynab_helper.web.config import WebConfig

    setup_logging("INFO", None)
    data_dir = args.data_dir or Path(tempfile.mkdtemp(prefix="ynab-helper-dev-"))
    try:
        data = seed(data_dir, seed_value=args.seed)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1
    base_url = f"http://{args.host}:{args.port}"
    config = WebConfig(
        enabled=True,
        host=args.host,
        port=args.port,
        public_url=base_url,
        secret_key=DEV_SECRET,
        login_ttl=3600,
        data_dir=data_dir,
    )
    app = make_app(
        config,
        users_db_path=data.users_db_path,
        data_dir=data_dir,
        version_line=f"{version_line()} — devserver",
        cache_bust=cache_bust_token(),
        delete_telegram_message=lambda chat, msg: log.info(
            "devserver: would delete Telegram message %s/%s", chat, msg
        ),
    )
    hasher = config.token_hasher()

    def dev_login() -> Response:
        return RedirectResponse(_login_url(data, hasher, base_url, 3600), status_code=303)

    app.add_api_route("/dev/login", dev_login, methods=["GET"], include_in_schema=False)
    login = _login_url(data, hasher, base_url, 3600)
    print(f"Data dir: {data_dir}", flush=True)
    print(f"LOGIN: {login}", flush=True)
    print(f"Fresh login link any time: {base_url}/dev/login", flush=True)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info", log_config=None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
