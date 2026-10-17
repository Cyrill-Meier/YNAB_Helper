#!/usr/bin/env python3
"""Telegram bot for the Revolut → YNAB importer: the documented entry point (v2 shim).

Since v2 the bot lives in :mod:`ynab_helper.bot` (see docs/DESIGN.md §8). This file only
keeps the documented command working (DESIGN §2.3)::

    python3 revolut_ynab_bot.py
    docker exec revolut-ynab-bot python3 revolut_ynab_bot.py

The implementation is :func:`ynab_helper.bot.main.main`, also reachable as
``python3 -m ynab_helper bot``. When started as root (``docker exec`` bypasses the image's
entrypoint), the shim first does what the entrypoint does
(:func:`ynab_helper.container.prepare_runtime`: load ``/app/.env``, hand the data
directory to the app user, drop privileges), so the bot never creates root-owned files.

The package needs Python 3.12+: this file avoids newer syntax (no f-strings, no
``__future__`` imports), so an older ``python3`` gets a one-line error instead of an
ImportError traceback. Importing this module runs nothing.
"""

import os
import sys


def main() -> int:
    """Drop root privileges like the container entrypoint, then run the bot."""
    if sys.version_info < (3, 12):
        sys.stderr.write(
            "YNAB Helper v2 needs Python 3.12 or newer, not " + sys.version.split()[0] + ": run "
            "it with .venv/bin/python (see the README) or in the container (docker exec "
            "revolut-ynab-bot python3 revolut_ynab_bot.py).\n"
        )
        return 1
    from ynab_helper import container
    from ynab_helper.bot import main as bot_main
    from ynab_helper.errors import YnabHelperError

    # os.geteuid does not exist on Windows: no root to drop there.
    geteuid = getattr(os, "geteuid", None)
    if geteuid is not None and geteuid() == 0:
        try:
            container.prepare_runtime()
        except (OSError, YnabHelperError) as exc:
            sys.stderr.write(
                "refusing to run as root: preparing the runtime failed: " + str(exc) + "\n"
            )
            return 1
    return bot_main.main()


if __name__ == "__main__":
    raise SystemExit(main())
