"""Small time helpers so every module formats timestamps the same way."""

from __future__ import annotations

from datetime import UTC, date, datetime


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_now_iso() -> str:
    """UTC timestamp like ``2026-10-16T18:30:00Z`` (seconds precision)."""
    return utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def local_today() -> date:
    """Today's date in the process timezone (the container sets TZ=Europe/Zurich)."""
    return datetime.now().date()


def utc_stamp() -> str:
    """Compact UTC stamp for file names, e.g. ``20261016T183000Z``."""
    return utc_now().strftime("%Y%m%dT%H%M%SZ")
