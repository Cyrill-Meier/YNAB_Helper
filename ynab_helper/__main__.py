"""``python -m ynab_helper [bot|cli|selfcheck|healthcheck] ...``.

Same commands as the container entrypoint (:mod:`ynab_helper.container`) but without the
root preparation and privilege drop; use this when running from a checkout.

Like the ``revolut_*.py`` shims, this file avoids newer syntax and checks the interpreter
before importing the package, so an older ``python3`` says which Python it needs instead of
dying with an ImportError traceback.
"""

import sys


def main() -> int:
    if sys.version_info < (3, 12):
        sys.stderr.write(
            "YNAB Helper v2 needs Python 3.12 or newer, not " + sys.version.split()[0] + ": run "
            "it with .venv/bin/python -m ynab_helper (see the README).\n"
        )
        return 1
    from ynab_helper.container import dispatch

    return dispatch(sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
