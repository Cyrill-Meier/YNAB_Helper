"""Constants and helpers shared by the session tests."""

from __future__ import annotations

import sqlite3

from ynab_helper.storage import UserStore

SECRET = "k" * 16 + "-test-secret-key-0123456789"  # 43 characters
OTHER_SECRET = "z" * 48
READY_ID = 4242
OTHER_ID = 777
T0 = 1_800_000_000.0  # a fixed "now" (2027-01-15) so expiry arithmetic is exact
LOGIN_TTL = 300
SESSION_TTL = 1800
ABSOLUTE_TTL = 43200


def make_ready(conn: sqlite3.Connection, telegram_id: int = READY_ID) -> None:
    """Create (or complete) a fully onboarded user."""
    UserStore(conn).upsert(
        telegram_id,
        state="ready",
        ynab_token="ynab-pat",
        budget_id="budget-1",
        account_id="account-1",
    )
