"""CSV statements sent as documents (DESIGN §8.3; F06, F15, F19, F47).

The handler checks the name (``.csv``) and the announced size, replies right away and does
the rest on a worker: ``getFile`` → download into a private temp file (size-capped,
atomic) → a quick header check → :func:`ynab_helper.services.import_csv`, which stores the
statement under a server-generated name (the Telegram file name is only metadata, F19),
imports it under the user's lock and returns a structured
:class:`~ynab_helper.importer.models.ImportResult`. The reply is rendered from that result
(counts, changes, removals, skipped rows, warnings, failures and the "partially applied"
hint, F47), followed by a balance line from the freshly synced mirror.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Final

from ynab_helper import services
from ynab_helper.bot import render
from ynab_helper.bot.context import Request
from ynab_helper.bot.render import bold, clip, h
from ynab_helper.bot.telegram import MAX_DOWNLOAD_BYTES, DownloadTooLarge
from ynab_helper.errors import ConfigError, TelegramError, YnabHelperError
from ynab_helper.revolut.parser import HEADER_SNIFF_BYTES, looks_like_revolut_csv
from ynab_helper.services import UserContext

log = logging.getLogger(__name__)

DOWNLOAD_DIRNAME: Final = "tmp"
#: Name prefix of the bot's statement downloads (see TelegramClient.download_file).
DOWNLOAD_PREFIX: Final = ".download-"


def sweep_downloads(data_dir: Path) -> int:
    """Delete statement downloads left in ``<data_dir>/tmp`` by an import that a shutdown
    cut off (its ``finally`` never ran). Run at startup, before any import can start.
    Returns how many files were removed; problems are logged, never raised."""
    directory = Path(data_dir) / DOWNLOAD_DIRNAME
    removed = 0
    try:
        entries = list(directory.iterdir())
    except FileNotFoundError:
        return 0
    except OSError as exc:
        log.warning("cannot list %s: %s", directory, exc)
        return 0
    for entry in entries:
        if not entry.name.startswith(DOWNLOAD_PREFIX):
            continue
        try:
            if entry.is_symlink() or entry.is_file():
                entry.unlink()
                removed += 1
        except OSError as exc:
            log.warning("cannot delete the stale download %s: %s", entry.name, exc)
    return removed


def on_document(req: Request) -> None:
    """A document from a ready user: import it if it is a CSV statement."""
    document = req.incoming.document or {}
    raw_name = document.get("file_name")
    name = raw_name.strip() if isinstance(raw_name, str) and raw_name.strip() else "statement.csv"
    if not name.lower().endswith(".csv"):
        req.reply(
            f"{bold(clip(name, 60))} doesn't look like a CSV. Please send a Revolut "
            "account-statement CSV."
        )
        return
    size = document.get("file_size")
    if isinstance(size, int) and size > MAX_DOWNLOAD_BYTES:
        req.reply("The file is larger than 25 MB — that is not a Revolut statement.")
        return
    file_id = document.get("file_id")
    if not isinstance(file_id, str) or not file_id:
        req.reply("Telegram did not send the file. Please try again.")
        return
    assert req.user is not None
    try:
        ctx = req.bot.user_context(req.user)
    except ConfigError as exc:
        req.reply(f"❌ {h(exc.user_message)}")
        return
    req.bot.start_job(
        req,
        "import",
        lambda: _import(req, ctx, file_id, name),
        working=f"📥 Importing {bold(clip(name, 60))}…",
    )


def _download(req: Request, file_id: str) -> Path | None:
    try:
        info = req.bot.tg.get_file(file_id)
        size = info.get("file_size")
        if isinstance(size, int) and size > MAX_DOWNLOAD_BYTES:
            raise DownloadTooLarge(MAX_DOWNLOAD_BYTES)
        file_path = info.get("file_path")
        if not isinstance(file_path, str):
            raise TelegramError("Telegram returned no file path")
        return req.bot.tg.download_file(
            file_path,
            req.bot.data_dir / DOWNLOAD_DIRNAME,
            max_bytes=MAX_DOWNLOAD_BYTES,
            suffix=".csv",
        )
    except DownloadTooLarge as exc:
        req.reply(f"❌ {h(exc.user_message)}")
    except TelegramError as exc:
        log.warning("downloading a statement of user %s failed: %s", req.user_id, exc)
        req.reply("❌ The file could not be downloaded from Telegram. Please send it again.")
    return None


def _import(req: Request, ctx: UserContext, file_id: str, name: str) -> None:
    """Worker: download, check, import, report, balance."""
    path = _download(req, file_id)
    if path is None:
        return
    try:
        with path.open("rb") as handle:
            head = handle.read(HEADER_SNIFF_BYTES)
        if not looks_like_revolut_csv(head):
            req.reply(
                "This doesn't look like a Revolut account statement CSV (the expected columns "
                "are missing)."
            )
            return
        result = services.import_csv(ctx, path, original_name=name, source_kind="telegram")
    finally:
        try:
            path.unlink()
        except OSError:
            pass
    req.reply(render.import_result(result, file_name=name))
    if result.rate_limited:
        return  # asking YNAB again now would only fail too
    try:
        status = services.account_status(ctx)
    except YnabHelperError as exc:
        log.info("balance after the import of user %s unavailable: %s", req.user_id, exc)
        return
    req.reply(render.balance_line(status))
