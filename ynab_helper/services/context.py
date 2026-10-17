"""Who an operation acts for (docs/DESIGN.md §7).

* :class:`UserContext` — a bot/web user (from the ``users`` row) or the CLI (from its
  configuration): token, budget, the Revolut account, the user's transactions DB and the
  per-user lock key.
* :class:`ValuationTarget` — the YNAB tracking account a crypto or IBKR valuation posts to;
  it needs no Revolut account (the CLI's ``--crypto-sync`` / ``--brokerage-sync``).
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ynab_helper.config import CSV_CACHE_DIRNAME
from ynab_helper.errors import ConfigError
from ynab_helper.storage.db import default_backup_dir, tx_db_path

__all__ = ["UserContext", "ValuationTarget", "setup_hint"]


def setup_hint(telegram_id: int | None, purpose: str = " first") -> str:
    """How to fix the YNAB setup: /setup for bot and web users, the configuration for the CLI."""
    if telegram_id is not None:
        return f"Run /setup{purpose}."
    return "Check YNAB_TOKEN, YNAB_BUDGET_ID and YNAB_ACCOUNT_ID (or the matching options)."


def _clean(value: Any) -> str | None:
    """A stripped non-empty string, or None."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _field(row: Any, name: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(name)
    if isinstance(row, sqlite3.Row):
        try:
            return row[name]
        except (IndexError, KeyError):
            return None
    return getattr(row, name, None)


@dataclass(frozen=True, slots=True)
class UserContext:
    """Everything an operation needs to act for one user (§7).

    * ``key`` — the lock key: ``str(telegram_id)`` for bot/web users, the DB path for the CLI.
    * ``db_path`` — the user's transactions DB; ``csv_dir`` — where uploads are stored
      (``None`` for the CLI, which imports files in place).
    * ``backup_dir`` — where the DB migration puts its backups (default: ``<db name>_backups``
      next to the DB, e.g. ``~/.revolut_to_ynab_backups`` for the CLI's default DB; bot/web
      users get the bot's ``<data_dir>/backups``).

    The token is excluded from ``repr`` so a context can be logged safely.
    """

    key: str
    ynab_token: str = field(repr=False)
    budget_id: str
    account_id: str
    db_path: Path
    csv_dir: Path | None
    auto_approve: bool = True
    telegram_id: int | None = None
    crypto_account_id: str | None = None
    btc_xpub: str | None = field(default=None, repr=False)
    eth_address: str | None = field(default=None, repr=False)
    backup_dir: Path | None = None

    def __post_init__(self) -> None:
        for name in ("key", "ynab_token", "budget_id", "account_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ConfigError(
                    f"UserContext.{name} is missing",
                    user_message="Your YNAB setup is incomplete "
                    "(token, budget or account missing). " + setup_hint(self.telegram_id),
                )
        object.__setattr__(self, "db_path", Path(self.db_path))
        if self.csv_dir is not None:
            object.__setattr__(self, "csv_dir", Path(self.csv_dir))
        if self.backup_dir is not None:
            object.__setattr__(self, "backup_dir", Path(self.backup_dir))

    @property
    def backups(self) -> Path:
        """Directory for DB backups (``backup_dir``, else ``<db name>_backups`` next to the
        DB — specific to this DB, never a generic ``~/backups``)."""
        if self.backup_dir is not None:
            return self.backup_dir
        return self.db_path.with_name(f"{self.db_path.stem}_backups")

    @classmethod
    def from_user_row(cls, row: Any, data_dir: str | os.PathLike[str]) -> UserContext:
        """Context for a bot/web user from a ``users`` row (``storage.users.User``, a mapping or
        a ``sqlite3.Row``) and the bot's data directory.

        Paths follow v1: ``<data_dir>/transactions_<id>.db`` and ``<data_dir>/csv_cache/<id>``;
        backups go to ``<data_dir>/backups``. Raises :class:`ConfigError` if the user has no
        token, budget or account yet.
        """
        raw_id = _field(row, "telegram_id")
        if isinstance(raw_id, bool) or raw_id is None:
            raise ConfigError("user row has no telegram_id")
        try:
            telegram_id = int(raw_id)
        except (TypeError, ValueError) as exc:
            raise ConfigError("user row has an invalid telegram_id") from exc
        token = _clean(_field(row, "ynab_token"))
        budget_id = _clean(_field(row, "budget_id"))
        account_id = _clean(_field(row, "account_id"))
        if not (token and budget_id and account_id):
            raise ConfigError(
                f"user {telegram_id} has no complete YNAB setup",
                user_message="Your YNAB setup is incomplete. " + setup_hint(telegram_id),
            )
        auto = _field(row, "auto_approve")
        root = Path(data_dir)
        return cls(
            key=str(telegram_id),
            ynab_token=token,
            budget_id=budget_id,
            account_id=account_id,
            db_path=tx_db_path(root, telegram_id),
            csv_dir=root / CSV_CACHE_DIRNAME / str(telegram_id),
            auto_approve=True if auto is None else bool(auto),
            telegram_id=telegram_id,
            crypto_account_id=_clean(_field(row, "crypto_account_id")),
            btc_xpub=_clean(_field(row, "btc_xpub")),
            eth_address=_clean(_field(row, "eth_address")),
            backup_dir=default_backup_dir(root),
        )

    @classmethod
    def from_cli(
        cls,
        *,
        token: str,
        budget_id: str,
        account_id: str,
        db_path: str | os.PathLike[str],
        auto_approve: bool = True,
        crypto_account_id: str | None = None,
        btc_xpub: str | None = None,
        eth_address: str | None = None,
        backup_dir: str | os.PathLike[str] | None = None,
    ) -> UserContext:
        """Context for the CLI: the lock key is the DB path, statements are not copied.

        Backups of the DB go to ``backup_dir``, by default ``<db name>_backups`` next to the
        DB (``~/.revolut_to_ynab_backups`` for the default ``~/.revolut_to_ynab.db``).
        """
        path = Path(db_path).expanduser()
        return cls(
            key=str(path),
            ynab_token=token,
            budget_id=budget_id,
            account_id=account_id,
            db_path=path,
            csv_dir=None,
            auto_approve=auto_approve,
            crypto_account_id=crypto_account_id,
            btc_xpub=btc_xpub,
            eth_address=eth_address,
            backup_dir=Path(backup_dir) if backup_dir is not None else None,
        )

    def crypto_target(self) -> ValuationTarget:
        """The crypto tracking account as a :class:`ValuationTarget` (same lock key and DB).

        Raises :class:`ConfigError` when no crypto tracking account is configured.
        """
        account = _clean(self.crypto_account_id)
        if account is None:
            hint = (
                "Run /crypto_setup first."
                if self.telegram_id is not None
                else "Set YNAB_CRYPTO_ACCOUNT_ID (or --crypto-account-id)."
            )
            raise ConfigError(
                "no crypto tracking account configured",
                user_message=f"No crypto tracking account is configured. {hint}",
            )
        return ValuationTarget(
            key=self.key,
            ynab_token=self.ynab_token,
            budget_id=self.budget_id,
            account_id=account,
            db_path=self.db_path,
            backup_dir=self.backups,
            telegram_id=self.telegram_id,
        )


@dataclass(frozen=True, slots=True)
class ValuationTarget:
    """The YNAB tracking account a valuation sync (crypto, IBKR) posts its adjustment to.

    Unlike :class:`UserContext` it needs no Revolut account: the CLI's ``--crypto-sync`` and
    ``--brokerage-sync`` only require a token, a budget and the tracking account (v1).

    * ``key`` — the lock key (the user's :attr:`UserContext.key`, so a valuation sync and an
      import of the same user exclude each other).
    * ``db_path`` — the user's transactions DB, used as a cache for the budget currency and
      marked stale after a posted adjustment. ``None`` (the CLI) touches no local file: the
      currency is read from YNAB.
    """

    key: str
    ynab_token: str = field(repr=False)
    budget_id: str
    account_id: str
    db_path: Path | None = None
    backup_dir: Path | None = None
    telegram_id: int | None = None

    def __post_init__(self) -> None:
        for name in ("key", "ynab_token", "budget_id", "account_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ConfigError(
                    f"ValuationTarget.{name} is missing",
                    user_message="The YNAB token, budget or tracking account is missing. "
                    + setup_hint(self.telegram_id),
                )
        if self.db_path is not None:
            object.__setattr__(self, "db_path", Path(self.db_path))
        if self.backup_dir is not None:
            object.__setattr__(self, "backup_dir", Path(self.backup_dir))

    @property
    def backups(self) -> Path | None:
        """Directory for DB backups (as :attr:`UserContext.backups`); None without a DB."""
        if self.backup_dir is not None:
            return self.backup_dir
        if self.db_path is None:
            return None
        return self.db_path.with_name(f"{self.db_path.stem}_backups")
