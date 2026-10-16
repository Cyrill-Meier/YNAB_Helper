"""Version line shown in /help, the admin startup DM and the web footer (F112).

``__version__`` lives in ``ynab_helper/__init__.py``; the commit and build time come from
the ``BUILD_SHA`` / ``BUILD_DATE`` build args that CI bakes into the image, so every
deployed image identifies itself without hand-bumping anything.
"""

from __future__ import annotations

import re
import time
from collections.abc import Mapping
from typing import Final

from ynab_helper import __version__
from ynab_helper.config import BuildInfo

# Fixed once per process: dev builds still get a cache-busting token that changes on
# every restart but stays stable for the lifetime of the process.
_PROCESS_START: Final[int] = int(time.time())
_URL_UNSAFE_RE: Final = re.compile(r"[^0-9A-Za-z._-]")


def version_info(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """``{"version": "2.0.0", "sha": "abc1234" | "dev", "date": "<build date>" | "local"}``."""
    build = BuildInfo.from_env(environ)
    return {"version": __version__, "sha": build.sha, "date": build.date}


def version_line(environ: Mapping[str, str] | None = None) -> str:
    """Compact one-line version, e.g. ``v2.0.0 (abc1234, 2026-10-16 20:00 UTC)``."""
    info = version_info(environ)
    return f"v{info['version']} ({info['sha']}, {info['date']})"


def cache_bust_token(environ: Mapping[str, str] | None = None) -> str:
    """URL-path-safe token that changes with every deployed build.

    ``2.0.0-abc1234`` for a release build; ``2.0.0-dev-<process start epoch>`` when no
    commit sha is known, so local restarts still invalidate cached static assets.
    """
    build = BuildInfo.from_env(environ)
    token = (
        f"{__version__}-{build.sha}" if build.is_release else f"{__version__}-dev-{_PROCESS_START}"
    )
    return _URL_UNSAFE_RE.sub("", token)
