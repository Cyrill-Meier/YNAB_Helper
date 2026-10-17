"""Formatters: escaping of hostile names (F91, F96), result rendering (F15, F47), limits."""

from __future__ import annotations

from decimal import Decimal

import pytest

from tests.fakes.fake_telegram import check_html
from ynab_helper.balance.adjust import AdjustmentResult
from ynab_helper.balance.reconcile import ReconcilePreview, ReconcileResult
from ynab_helper.bot import render
from ynab_helper.bot.telegram import MAX_MESSAGE_UNITS, split_message, utf16_len
from ynab_helper.importer.models import Change, Failure, ImportResult, Removal, TxRef
from ynab_helper.maintenance.cleanup import CleanupFailure, CleanupResult, MemoFix
from ynab_helper.maintenance.dedupe import DedupeCandidate, DedupeDeleteResult, DedupeScan
from ynab_helper.services import StatusInfo
from ynab_helper.storage.csv_files import CsvFile
from ynab_helper.storage.users import User

HOSTILE = '<script>alert("x")</script> & <b>bold</b> [Tap](https://evil.example) _x_ *y*'


def tx(i: int = 0, payee: str = HOSTILE) -> TxRef:
    return TxRef(
        date=f"2026-10-{1 + i % 28:02d}", amount=-1_234_560 - i, payee=payee, ynab_tx_id=f"t{i}"
    )


def full_result(n: int = 20) -> ImportResult:
    return ImportResult(
        dry_run=False,
        account_id="a",
        currency="CHF",
        window=("2026-10-01", "2026-10-31"),
        rows_total=50,
        rows_pending=3,
        rows_reverted=1,
        created=[tx(i) for i in range(n)],
        updated=[
            Change(
                tx(i), {"cleared": ("uncleared", "cleared"), "amount": (-1000, -1200)}, "cleared"
            )
            for i in range(n)
        ],
        removed=[Removal(tx(i), "superseded" if i % 2 else "reverted") for i in range(n)],
        unchanged=7,
        ignored_deleted=2,
        skipped_rows=[(i + 2, f"State {HOSTILE} not imported") for i in range(n)],
        warnings=[f"warning {HOSTILE}"] * n,
        failures=[Failure("update", tx(i), f"YNAB said {HOSTILE}") for i in range(n)],
        not_applied=4,
        rate_limited=True,
    )


def test_import_result_lists_at_most_eight_per_section_and_escapes_everything():
    text = render.import_result(full_result(), file_name=f"{HOSTILE}.csv")
    plain = check_html(text)  # valid Telegram HTML despite every hostile string
    assert plain.startswith(f"⚠ Import partially applied — {HOSTILE[:59]}")
    assert "New: 20 · Updated: 20 · Removed: 20 · Unchanged: 7" in plain
    assert "2 transactions you deleted in YNAB stayed deleted" in plain
    for title in (
        "✏ Changed in YNAB (20)",
        "🗑 Removed from YNAB (20)",
        "⏭ Skipped rows (20)",
        "⚠ Warnings (20)",
        "❌ Failed (20)",
    ):
        assert title in plain
    assert plain.count("… and 12 more") == 5
    assert "data row 2: State" in plain
    assert "cleared uncleared → cleared; amount -1.00 CHF → -1.20 CHF" in plain
    assert "pending replaced by the settled transaction" in plain and "reverted by Revolut" in plain
    assert "rate limit" in plain and "4 changes not applied" in plain
    assert plain.rstrip().endswith(render.PARTIAL_HINT)
    assert "<a" not in text and "<script" not in text


def test_up_to_date_and_complete_titles():
    empty = ImportResult(
        dry_run=False, account_id="a", currency="CHF", window=("2026-10-01", "2026-10-02")
    )
    assert render.import_result(empty).startswith("✅ Everything already up to date")
    one = ImportResult(dry_run=False, account_id="a", currency="CHF", created=[tx()])
    assert render.import_result(one).startswith("✅ Import complete")
    assert render.PARTIAL_HINT not in render.import_result(one)


def test_huge_results_split_into_valid_chunks():
    text = render.import_result(full_result(500))
    chunks = split_message(text)
    assert all(utf16_len(c) <= MAX_MESSAGE_UNITS for c in chunks)
    for chunk in chunks:
        check_html(chunk)


def test_reconcile_preview_and_result():
    preview = ReconcilePreview(
        budget_id="b",
        account_id="a",
        csv_file_id=1,
        csv_name=f"{HOSTILE}.csv",
        csv_sha256="x",
        as_of="2026-10-14",
        csv_balance=6_810_600,
        ynab_cleared_as_of=4_310_600,
        ynab_cleared_now=4_000_000,
        cleared_after_as_of=-310_600,
        delta=2_500_000,
        currency="CHF",
        problems=[f"Statement is old {HOSTILE}"],
        token="tok",
    )
    plain = check_html(render.reconcile_preview(preview))
    assert "Difference: +2,500.00 CHF" in plain
    assert "-310.60 CHF is dated after 2026-10-14" in plain
    assert f"⚠ Statement is old {HOSTILE}" in plain
    assert "lock" not in plain.lower()  # it does not lock other transactions (§6.6)
    adjustment = AdjustmentResult(
        "created",
        2_500_000,
        "RECON:2500000:2026-10-14:1",
        "t",
        0,
        0,
        "a",
        "2026-10-14",
        "CSV Reconciliation",
        None,
        "reconciled",
    )
    result = check_html(render.reconcile_result(ReconcileResult(preview, adjustment)))
    assert "+2,500.00 CHF dated 2026-10-14" in result


def test_cleanup_result():
    fix = MemoFix("t", "2026-10-01", -5000, HOSTILE, "cleared", "(pending)", None)
    result = CleanupResult(
        dry_run=False,
        account_id="a",
        candidates=[fix, fix],
        updated=[fix],
        failures=[CleanupFailure(fix, f"boom {HOSTILE}")],
        skipped_uncleared=3,
    )
    plain = check_html(render.cleanup_result(result, "CHF"))
    assert "Cleaned 1 of 2 stale (pending) memos" in plain
    assert "3 uncleared transactions keep their marker" in plain
    assert "Partially applied" in plain


def scan(count: int) -> DedupeScan:
    candidates = [
        DedupeCandidate(
            f"id{i}",
            f"2026-10-{1 + i % 28:02d}",
            -1000 * (i + 1),
            HOSTILE,
            None,
            f"YNAB:{i}",
            "cleared",
            None,
        )
        for i in range(count)
    ]
    return DedupeScan(
        "scan1234567",
        "b",
        "a",
        1,
        f"{HOSTILE}.csv",
        ("2026-10-01", "2026-10-31"),
        candidates,
        40,
        50,
        "now",
        600,
    )


def test_dedupe_picker_and_confirm_views():
    text, markup = render.dedupe_picker(scan(14), {0, 13}, 2, nonce="scan1234567")
    plain = check_html(text)
    assert "Page 3/3" in plain
    data = [b["callback_data"] for row in markup["inline_keyboard"] for b in row]
    assert "dx:scan1234567:t:13" in data and "dx:scan1234567:del" in data
    assert all(len(d.encode()) <= 64 for d in data)
    text, markup = render.dedupe_confirm(scan(250), set(range(200)), nonce="scan1234567")
    plain = check_html(text)
    assert utf16_len(text) <= MAX_MESSAGE_UNITS
    assert "Delete these 200 transactions" in plain and "more" in plain
    assert [b["callback_data"] for b in markup["inline_keyboard"][0]] == [
        "dx:scan1234567:ok",
        "dx:scan1234567:back",
    ]


def test_dedupe_delete_result():
    candidates = scan(3).candidates
    result = DedupeDeleteResult(
        "s",
        deleted=[candidates[0]],
        skipped=[(candidates[1], "belongs to a statement row")],
        failures=[(candidates[2], f"HTTP 500 {HOSTILE}")],
    )
    plain = check_html(render.dedupe_delete_result(result))
    assert plain.startswith("✓ Deleted 1 of 3 transactions")
    assert "belongs to a statement row" in plain


def test_status_and_balance_line():
    statement = CsvFile(
        1,
        "a",
        "/x",
        f"{HOSTILE}.csv",
        "sha",
        "2026-10-15T10:00:00Z",
        "telegram",
        "CHF",
        10,
        "2026-10-01",
        "2026-10-14",
        "imported",
        None,
        None,
    )
    info = StatusInfo(
        account_id="a",
        account_name=HOSTILE,
        account_found=True,
        account_closed=False,
        account_deleted=False,
        currency="CHF",
        balance=6_810_600,
        cleared_balance=6_700_000,
        uncleared_balance=110_600,
        last_sync_at="2026-10-17T08:00:00Z",
        last_statement=statement,
        ledger_counts={"ACTIVE": 12, "REVERTED": 0},
        sync_note=f"note {HOSTILE}",
    )
    plain = check_html(render.status(info))
    assert "Working balance: 6,810.60 CHF" in plain and "Uncleared: 110.60 CHF" in plain
    assert "status imported" in plain and "active 12" in plain and "reverted" not in plain
    line = check_html(render.balance_line(info))
    assert "6,810.60 CHF (cleared 6,700.00 CHF)" in line


def user(**fields) -> User:
    values = dict(
        telegram_id=1,
        chat_id=1,
        username=None,
        first_name=None,
        ynab_token=None,
        budget_id=None,
        budget_name=None,
        account_id=None,
        account_name=None,
        crypto_account_id=None,
        crypto_account_name=None,
        btc_xpub=None,
        eth_address=None,
        state="ready",
        temp_data=None,
        created_at="x",
        updated_at="x",
        auto_approve=True,
    )
    values.update(fields)
    return User(**values)


def test_users_list_and_new_user_notice_escape_names():
    users = [user(telegram_id=1, first_name=HOSTILE, username="a_b", account_name=HOSTILE)]
    check_html(render.users_list(users, admin_id=1))
    check_html(render.new_user_notice(5, HOSTILE, "x_y_z"))


@pytest.mark.parametrize("auto", [True, False])
def test_settings_menu_keeps_v1_callback_data(auto: bool):
    text, markup = render.settings_menu(user(auto_approve=auto))
    check_html(text)
    data = [b["callback_data"] for row in markup["inline_keyboard"] for b in row]
    assert data == [f"st:auto:{0 if auto else 1}", "st:crypto", "st:setup", "st:close"]


def test_mask_never_shows_a_whole_key():
    key = "zpub6rFR7y4Q2AijBEqTUquhVz398htDFrtymD9xYYfG1m4wAcvPhXNfE3EfH1r1ADqtfSdVCToUG868RvUUkgDKf31mGDtKsAYz2oz2AGutZYs"
    masked = render.mask(key)
    assert key not in masked and masked.startswith(key[:8]) and masked.endswith(key[-6:])
    assert render.mask("short") == "shor…"
    assert render.h(None) == "" and render.h(Decimal("1.5")) == "1.5"
