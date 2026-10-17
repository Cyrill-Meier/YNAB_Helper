#!/usr/bin/env python3
"""Revolut CSV → YNAB importer: the command-line entry point (v2 shim).

Since v2 the importer lives in the ``ynab_helper`` package (see docs/DESIGN.md). This file
only keeps the documented command working (DESIGN §2.3)::

    python3 revolut_to_ynab.py [statement.csv] [options]
    docker exec revolut-ynab-bot python3 revolut_to_ynab.py --sync

Every v1 option is still accepted (``--help`` lists them); the implementation is
:func:`ynab_helper.cli.main`, also reachable as ``python3 -m ynab_helper cli …``.

``docker exec`` bypasses the image's entrypoint and runs as root. In that case the shim
first does what the entrypoint does (:func:`ynab_helper.container.prepare_runtime`: load
``/app/.env``, hand the data directory to the app user, drop privileges), so the CLI never
leaves root-owned files in ``/app/bot_data`` that the bot could not open afterwards.

The package needs Python 3.12+, but v1 ran on any ``python3``: this file avoids newer syntax
(no f-strings, no ``__future__`` imports), so an older interpreter gets a one-line error
instead of an ImportError traceback. Importing this module runs nothing.
"""

import os
import sys
from collections.abc import Sequence


def main(argv: "Sequence[str] | None" = None) -> int:
    """Drop root privileges like the container entrypoint, then run the CLI."""
    if sys.version_info < (3, 12):
        sys.stderr.write(
            "YNAB Helper v2 needs Python 3.12 or newer, not " + sys.version.split()[0] + ": run "
            "it with .venv/bin/python (see the README) or in the container (docker exec "
            "revolut-ynab-bot python3 revolut_to_ynab.py ...).\n"
        )
        return 1
    from ynab_helper import cli, container
    from ynab_helper.errors import YnabHelperError

    # os.geteuid does not exist on Windows (v1 ran wherever Python ran): no root to drop.
    geteuid = getattr(os, "geteuid", None)
    if geteuid is not None and geteuid() == 0:
        try:
            container.prepare_runtime()
        except (OSError, YnabHelperError) as exc:
            sys.stderr.write(
                "refusing to run as root: preparing the runtime failed: " + str(exc) + "\n"
            )
            return 1
    return cli.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
