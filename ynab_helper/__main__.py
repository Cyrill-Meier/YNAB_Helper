"""``python -m ynab_helper [bot|cli|selfcheck|healthcheck] ...``.

Same commands as the container entrypoint (:mod:`ynab_helper.container`) but without the
root preparation and privilege drop; use this when running from a checkout.
"""

from __future__ import annotations

import sys

from ynab_helper.container import dispatch

if __name__ == "__main__":
    sys.exit(dispatch(sys.argv[1:]))
