"""The five import-flow scenarios of v1's ``tests/test_import_flows.py``, ported to v2.

v1 drove ``import_and_track`` with hand-made dicts and a stub ``ynab_request`` and asserted
on captured stdout. Here every scenario imports real Revolut-shaped statements through
``services.import_csv`` into the stateful fake YNAB and asserts on the structured
:class:`~ynab_helper.importer.ImportResult` and on YNAB's state:

1. reverted rows are deleted once, never created, and a re-import converges;
2. a date-shifted row (statement exported in another timezone) is re-dated, not duplicated;
3. a +9 h export offset is learned from the statement, evening rows are re-dated;
4. a pending that vanished behind differently-named settled rows is removed as superseded
   (v1's own example, a pending that settled under the same payee, is a settlement in v2);
5. amount changes are reported as such; a plain clearing carries no amount change.

Where v2's rules differ from v1 on purpose, the test says so (window boundary F10,
settlement F35).
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_ynab import FakeYnab
from tests.importer.helpers import csv_bytes
from tests.legacy import v1_revolut_to_ynab as v1
from ynab_helper import services
from ynab_helper.importer import ImportResult
from ynab_helper.services import UserContext

from .helpers import ACCT, BUDGET, new_fake, user_ctx

Row = Sequence[str]


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeYnab:
    f = new_fake()
    monkeypatch.setattr(services, "client_factory", lambda ctx: f.client(max_retries=0))
    return f


@pytest.fixture
def ctx(tmp_path: Path) -> UserContext:
    data = tmp_path / "bot_data"
    data.mkdir()
    return user_ctx(data)


def _import(ctx: UserContext, rows: Sequence[Row]) -> ImportResult:
    return services.import_csv(
        ctx, csv_bytes(rows), original_name="statement.csv", source_kind="telegram"
    )


def _writes(fake: FakeYnab) -> list[tuple[str, Any]]:
    return [(c.method, c.body) for c in fake.calls if c.method != "GET"]


def _live(fake: FakeYnab) -> list[dict[str, Any]]:
    return fake.transactions(BUDGET, account_id=ACCT)


def _payees(fake: FakeYnab) -> list[str]:
    return sorted(t["payee_name"] for t in _live(fake))


def _refs(items: Sequence[Any]) -> list[tuple[str, int, str]]:
    return [(t.date, t.amount, t.payee) for t in items]


# ── 1. reverted rows ────────────────────────────────────────────────────────


def test_reverted_rows_are_deleted_once_never_created_and_converge(ctx, fake):
    _import(ctx, [("2026-08-09 12:00:00", "Shop", "-50.00", "PENDING")])
    shop = next(t for t in _live(fake) if t["payee_name"] == "Shop")
    fake.reset_log()

    statement = [
        ("2026-08-09 12:00:00", "Shop", "-50.00", "REVERTED"),
        ("2026-08-10 09:00:00", "Kiosk", "-3.00", "REVERTED"),  # never imported: skipped
        ("2026-08-10 12:00:00", "Cafe", "-8.00"),
    ]
    result = _import(ctx, statement)
    assert result.ok
    assert [(r.tx.payee, r.reason) for r in result.removed] == [("Shop", "reverted")]
    assert _refs(result.created) == [("2026-08-10", -8_000, "Cafe")]
    assert len(result.skipped_rows) == 1 and result.rows_reverted == 2
    assert [m for m, _ in _writes(fake)] == ["POST", "DELETE"]
    assert len(fake.calls_matching("POST")[0].body["transactions"]) == 1
    assert fake.tx(shop["id"])["deleted"] is True
    assert _payees(fake) == ["Cafe"]

    fake.reset_log()
    again = _import(ctx, statement)
    assert again.ok and not again.has_changes and _writes(fake) == []


# ── 2. date-shifted rows ────────────────────────────────────────────────────


def test_a_date_shifted_row_is_redated_not_duplicated(ctx, fake):
    _import(ctx, [("2026-08-04 02:32:04", "Top-up", "2000.00", "COMPLETED", "Topup")])
    (topup,) = _live(fake)
    fake.reset_log()

    # The same top-up, exported with the phone four hours behind: it moves to the day before.
    shifted = [("2026-08-03 22:32:04", "Top-up", "2000.00", "COMPLETED", "Topup")]
    result = _import(ctx, shifted)
    assert result.ok and result.created == []
    (change,) = result.updated
    assert change.reason == "redated"
    assert dict(change.fields) == {"date": ("2026-08-04", "2026-08-03")}
    assert [m for m, _ in _writes(fake)] == ["PATCH"]
    assert [(t["id"], t["date"]) for t in _live(fake)] == [(topup["id"], "2026-08-03")]

    fake.reset_log()
    assert not _import(ctx, shifted).has_changes and _writes(fake) == []


def test_a_same_time_next_day_twin_is_a_new_transaction(ctx, fake):
    # Round-time rows a whole day apart are two purchases, not one re-rendered row.
    _import(ctx, [("2026-08-06 12:00:00", "Cafe Noon", "-2.50")])
    fake.reset_log()
    result = _import(ctx, [("2026-08-07 12:00:00", "Cafe Noon", "-2.50")])
    assert _refs(result.created) == [("2026-08-07", -2_500, "Cafe Noon")]
    assert [m for m, _ in _writes(fake)] == ["POST"]
    assert len(_live(fake)) == 2


# ── 3. a +9 h export offset ─────────────────────────────────────────────────

LAS_VEGAS = [  # exported in UTC−7
    ("2026-08-22 12:15:10", "Lunch", "-20.00"),
    ("2026-08-22 19:30:05", "Dinner", "-30.00"),
    ("2026-08-22 22:10:00", "Toll", "-0.18"),
]
ZURICH = [  # the same period exported in UTC+2: every timestamp +9 h
    ("2026-08-22 21:15:10", "Lunch", "-20.00"),  # same day: reveals the offset
    ("2026-08-23 04:30:05", "Dinner", "-30.00"),  # crossed midnight (outside v1's 3 h gate)
    ("2026-08-23 06:10:00", "Toll", "-0.18"),  # a genuinely new toll (+8 h)
    ("2026-08-23 07:10:00", "Toll", "-0.18"),  # the old toll, shifted +9 h
]


def test_a_9h_export_offset_redates_the_evening_rows(ctx, fake):
    _import(ctx, LAS_VEGAS)
    old_toll = next(t for t in _live(fake) if t["payee_name"] == "Toll")
    fake.reset_log()

    result = _import(ctx, ZURICH)
    assert result.ok
    assert _refs(result.created) == [("2026-08-23", -180, "Toll")]
    redated = {c.tx.payee: dict(c.fields) for c in result.updated}
    assert redated == {
        "Dinner": {"date": ("2026-08-22", "2026-08-23")},
        "Toll": {"date": ("2026-08-22", "2026-08-23")},
    }
    (post,) = fake.calls_matching("POST")
    assert [t["import_id"] for t in post.body["transactions"]] == ["YNAB:-180:2026-08-23:1"]
    assert fake.tx(old_toll["id"])["date"] == "2026-08-23"  # the old toll kept its id
    assert sorted((t["date"], t["payee_name"]) for t in _live(fake)) == [
        ("2026-08-22", "Lunch"),
        ("2026-08-23", "Dinner"),
        ("2026-08-23", "Toll"),
        ("2026-08-23", "Toll"),
    ]

    fake.reset_log()
    assert not _import(ctx, ZURICH).has_changes and _writes(fake) == []


def test_a_neighbour_that_is_not_timezone_shaped_is_a_new_transaction(ctx, fake):
    _import(ctx, [("2026-08-25 23:10:00", "Cafe", "-4.50")])
    fake.reset_log()
    result = _import(ctx, [("2026-08-26 02:27:00", "Cafe", "-4.50")])  # 3 h 17 min later
    assert _refs(result.created) == [("2026-08-26", -4_500, "Cafe")]
    assert [m for m, _ in _writes(fake)] == ["POST"]


def test_legacy_rows_without_a_timestamp_use_the_near_midnight_rule(ctx, fake):
    """v1 rows (no started_at) shift a day only when the new row started near midnight.

    v2 also requires both dates inside the statement (F10: v1 matched across the window
    boundary and swallowed real transactions), so this statement starts on 08-27.
    """
    bar = fake.add_transaction(
        BUDGET, ACCT, amount=-7_000, date="2026-08-27", payee_name="Bar",
        import_id="YNAB:-7000:2026-08-27:1", cleared="cleared",
    )  # fmt: skip
    shop = fake.add_transaction(
        BUDGET, ACCT, amount=-8_000, date="2026-08-27", payee_name="Shop",
        import_id="YNAB:-8000:2026-08-27:1", cleared="cleared",
    )  # fmt: skip
    conn = v1.init_db(str(ctx.db_path))  # the user's v1 database, before v2 ever opened it
    for key, amount, payee, tx_id in (
        ("YNAB:-7000:2026-08-27:1", -7_000, "Bar", bar),
        ("YNAB:-8000:2026-08-27:1", -8_000, "Shop", shop),
    ):
        conn.execute(
            "INSERT INTO transactions (import_id, date, amount, payee_name, cleared, state, "
            "ynab_tx_id, account_id, source, imported_at) VALUES (?, '2026-08-27', ?, ?, "
            "'cleared', 'COMPLETED', ?, ?, 'revolut', 'x')",
            (key, amount, payee, tx_id, ACCT),
        )
    conn.commit()
    conn.close()
    fake.reset_log()

    result = _import(
        ctx,
        [
            ("2026-08-27 09:00:00", "Bakery", "-3.20"),
            ("2026-08-28 00:40:00", "Bar", "-7.00"),  # 40 min after midnight: the same bar bill
            ("2026-08-28 15:00:00", "Shop", "-8.00"),  # mid-afternoon: a new purchase
        ],
    )
    assert result.ok
    assert sorted(_refs(result.created)) == [
        ("2026-08-27", -3_200, "Bakery"),
        ("2026-08-28", -8_000, "Shop"),
    ]
    assert [(c.tx.payee, dict(c.fields)) for c in result.updated] == [
        ("Bar", {"date": ("2026-08-27", "2026-08-28")})
    ]
    assert fake.tx(bar)["date"] == "2026-08-28" and fake.tx(shop)["date"] == "2026-08-27"


# ── 4. superseded pendings ──────────────────────────────────────────────────


def test_a_superseded_pending_is_removed_and_reported_and_guards_hold(ctx, fake):
    _import(
        ctx,
        [
            ("2026-07-01 09:00:00", "Old Hold", "-990.00", "PENDING"),
            ("2026-08-18 10:59:18", "Hotel Deposit", "-200.00", "PENDING"),
            ("2026-08-19 08:00:00", "Hotel Hold", "-110.00", "PENDING"),
        ],
    )
    ids = {t["payee_name"]: t["id"] for t in _live(fake)}
    fake.reset_log()

    later = [  # the deposit settled as two rows with other names; it is gone from Revolut
        # The statement starts before the deposit: only then is its absence proof that it
        # vanished (the coverage starts at the first completion, ExportFrame.coverage).
        ("2026-08-17 09:00:00", "Bakery", "-3.00"),
        ("2026-08-18 11:00:00", "Coffee", "-4.00"),
        ("2026-08-19 08:00:00", "Hotel Hold", "-110.00", "PENDING"),  # still pending
        ("2026-08-20 09:30:12", "Hotel Bellevue", "-150.00"),
        ("2026-08-20 09:31:40", "Hotel Minibar", "-50.00"),
    ]
    result = _import(ctx, later)
    assert result.ok
    assert [(r.tx.payee, r.tx.amount, r.reason) for r in result.removed] == [
        ("Hotel Deposit", -200_000, "superseded")
    ]
    assert fake.tx(ids["Hotel Deposit"])["deleted"] is True
    # Outside the statement's window, and still pending in it: untouched.
    assert fake.tx(ids["Old Hold"])["deleted"] is False
    assert fake.tx(ids["Hotel Hold"])["deleted"] is False
    assert [m for m, _ in _writes(fake)] == ["POST", "DELETE"]

    fake.reset_log()
    assert not _import(ctx, later).has_changes and _writes(fake) == []


def test_v1s_tanta_settlement_is_an_amount_update_in_v2(ctx, fake):
    """v1's own superseded case: a dinner pending of -57.48 that settled as "Tanta" -51.06
    plus a separate "Propina Tanta" tip of -5.23. v1 deleted the pending and created both
    rows; v2 recognises the settlement (same payee, same day, other amount, F35) and updates
    the pending in place — keeping its id, category and any edits — and creates only the tip.
    The guards of the v1 test still hold: an old pending outside the statement and a pending
    that is still pending are left alone."""
    _import(
        ctx,
        [
            ("2026-07-01 09:00:00", "Old Hold", "-990.00", "PENDING"),
            ("2026-08-18 10:59:18", "Tanta", "-57.48", "PENDING"),
            ("2026-08-19 08:00:00", "Hotel Hold", "-110.00", "PENDING"),
        ],
    )
    ids = {t["payee_name"]: t["id"] for t in _live(fake)}
    fake.reset_log()

    settled = [
        ("2026-08-18 10:59:18", "Tanta", "-51.06"),
        ("2026-08-18 13:59:18", "Propina Tanta", "-5.23"),
        ("2026-08-19 08:00:00", "Hotel Hold", "-110.00", "PENDING"),
    ]
    result = _import(ctx, settled)
    assert result.ok and result.removed == []
    assert _refs(result.created) == [("2026-08-18", -5_230, "Propina Tanta")]
    (change,) = result.updated
    assert change.tx.payee == "Tanta"
    assert change.fields["amount"] == (-57_480, -51_060)
    assert change.fields["cleared"] == ("uncleared", "cleared")
    tanta = fake.tx(ids["Tanta"])
    assert (tanta["deleted"], tanta["amount"], tanta["cleared"]) == (False, -51_060, "cleared")
    assert fake.tx(ids["Old Hold"])["deleted"] is False
    assert fake.tx(ids["Hotel Hold"])["deleted"] is False
    assert sorted(m for m, _ in _writes(fake)) == ["PATCH", "POST"]

    fake.reset_log()
    assert not _import(ctx, settled).has_changes and _writes(fake) == []


def test_a_pending_still_cleared_in_ynab_is_never_swept(ctx, fake):
    _import(ctx, [("2026-08-18 10:59:18", "Hotel Deposit", "-200.00", "PENDING")])
    (deposit,) = _live(fake)
    fake.user_edit_transaction(deposit["id"], cleared="cleared")  # the user cleared it
    fake.reset_log()
    result = _import(
        ctx,
        [("2026-08-17 09:00:00", "Bakery", "-3.00"), ("2026-08-18 11:00:00", "Coffee", "-4.00")],
    )
    assert result.removed == [] and fake.tx(deposit["id"])["deleted"] is False
    assert any("Hotel Deposit" in warning for warning in result.warnings)


# ── 5. amount changes ───────────────────────────────────────────────────────


def test_amount_changes_are_reported_and_plain_clearing_carries_no_amount(ctx, fake):
    _import(
        ctx,
        [
            ("2026-08-09 10:00:00", "EV Charge", "-45.00", "PENDING"),
            ("2026-08-10 08:30:00", "Cafe", "-9.99", "PENDING"),
        ],
    )
    fake.reset_log()
    result = _import(
        ctx,
        [
            ("2026-08-09 10:00:00", "EV Charge", "-41.20"),  # settled at another amount
            ("2026-08-10 08:30:00", "Cafe", "-9.99"),  # just completed
        ],
    )
    assert result.ok and result.created == [] and result.removed == []
    changes = {c.tx.payee: c for c in result.updated}
    assert changes["EV Charge"].fields["amount"] == (-45_000, -41_200)
    assert changes["EV Charge"].fields["cleared"] == ("uncleared", "cleared")
    assert "amount" not in changes["Cafe"].fields
    assert changes["Cafe"].fields["cleared"] == ("uncleared", "cleared")
    assert sum(1 for c in result.updated if "amount" in c.fields) == 1
    assert sorted((t["payee_name"], t["amount"], t["cleared"]) for t in _live(fake)) == [
        ("Cafe", -9_990, "cleared"),
        ("EV Charge", -41_200, "cleared"),
    ]
    # v1 deleted and re-created such a settlement (F35); v2 updates the same transaction.
    assert [m for m, _ in _writes(fake)] == ["PATCH"]
