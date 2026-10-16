"""Docker HEALTHCHECK: ``python3 -m ynab_helper.healthcheck`` (F69).

Healthy means:

* the bot's poll loop is alive: ``<BOT_DATA_DIR>/.heartbeat`` exists and was touched less
  than ``HEALTHCHECK_MAX_AGE`` seconds ago (default 180; the loop touches it at least once
  per 50 s long poll), and
* when ``WEB_UI_ENABLED`` is truthy, ``GET /health`` on the local web server answers 200
  within 3 s.

Docker runs the health check with the container's environment, which does not include the
variables the entrypoint loaded from ``/app/.env``; the check therefore reads that file
itself (without overriding real environment variables).

This module must stay import-light: it runs every minute and must not import FastAPI.
Prints one status line; exit code 0 = healthy, 1 = unhealthy.
"""

from __future__ import annotations

import http.client
import math
import os
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from ynab_helper import config

DEFAULT_MAX_AGE: Final[float] = 180.0
DEFAULT_WEB_PORT: Final[int] = 8080
WEB_TIMEOUT: Final[float] = 3.0
_MAX_BODY: Final[int] = 64 * 1024
_MAX_REASON: Final[int] = 200
# Wildcard bind addresses are not connectable; probe the matching loopback address.
_LOOPBACK_FOR: Final[Mapping[str, str]] = {"": "127.0.0.1", "0.0.0.0": "127.0.0.1", "::": "::1"}


@dataclass(frozen=True, slots=True)
class CheckResult:
    """One check: ``ok`` plus a short human-readable ``detail``."""

    ok: bool
    detail: str


@dataclass(frozen=True, slots=True)
class HealthReport:
    """Overall verdict and the individual checks, rendered by :attr:`line`."""

    heartbeat: CheckResult
    web: CheckResult | None
    notes: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.heartbeat.ok and (self.web is None or self.web.ok)

    @property
    def line(self) -> str:
        parts = [self.heartbeat.detail, self.web.detail if self.web else "web UI disabled"]
        parts.extend(self.notes)
        return f"{'OK' if self.ok else 'UNHEALTHY'}: {'; '.join(parts)}"


def check_heartbeat(path: Path, *, max_age: float, now: float) -> CheckResult:
    """Fresh heartbeat file = the poll loop made progress within ``max_age`` seconds."""
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return CheckResult(False, f"heartbeat {path} missing")
    except OSError as exc:
        return CheckResult(False, f"heartbeat {path} unreadable ({exc.strerror or exc})")
    except ValueError:  # e.g. a NUL character in the path
        return CheckResult(False, "heartbeat path invalid")
    # A heartbeat from the "future" (clock step) counts as fresh rather than failing.
    age = max(0.0, now - mtime)
    if age > max_age:
        return CheckResult(False, f"heartbeat {age:.0f}s old (max {max_age:.0f}s)")
    return CheckResult(True, f"heartbeat {age:.0f}s old")


def check_web(host: str, port: int, *, timeout: float = WEB_TIMEOUT) -> CheckResult:
    """``GET http://host:port/health`` must return 200 (redirects are not followed).

    Never raises: a malformed host (``WEB_UI_HOST`` with spaces, control characters or an
    over-long label) is reported as a failed probe like any connection error.
    """
    conn: http.client.HTTPConnection | None = None
    try:
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("GET", "/health", headers={"User-Agent": "ynab-helper-healthcheck"})
        response = conn.getresponse()
        response.read(_MAX_BODY)
        status = response.status
    except http.client.InvalidURL:
        return CheckResult(False, "web /health failed (invalid WEB_UI_HOST)")
    except (OSError, http.client.HTTPException, ValueError) as exc:
        # ValueError: UnicodeError from IDNA encoding of a malformed host name.
        reason = getattr(exc, "strerror", None) or str(exc) or type(exc).__name__
        return CheckResult(False, f"web /health failed ({_one_line(reason)})")
    finally:
        if conn is not None:
            conn.close()
    if status != 200:
        return CheckResult(False, f"web /health returned {status}")
    return CheckResult(True, "web /health 200")


def _one_line(text: str) -> str:
    # Docker keeps the health check output; the contract is exactly one status line.
    return " ".join(str(text).split())[:_MAX_REASON]


def _max_age(env: Mapping[str, str], notes: list[str]) -> float:
    raw = (env.get("HEALTHCHECK_MAX_AGE") or "").strip()
    if not raw:
        return DEFAULT_MAX_AGE
    try:
        value = float(raw)
    except ValueError:
        value = math.nan
    if not math.isfinite(value) or value <= 0:
        notes.append(f"ignoring invalid HEALTHCHECK_MAX_AGE, using {DEFAULT_MAX_AGE:.0f}s")
        return DEFAULT_MAX_AGE
    return value


def _web_target(env: Mapping[str, str], notes: list[str]) -> tuple[str, int] | None:
    if not config.parse_bool(env.get("WEB_UI_ENABLED")):
        return None
    problems: list[str] = []
    port = config.parse_int(
        env.get("WEB_UI_PORT"),
        name="WEB_UI_PORT",
        default=DEFAULT_WEB_PORT,
        problems=problems,
        minimum=1,
        maximum=65535,
    )
    if problems:
        notes.append(f"invalid WEB_UI_PORT, probing {port}")
    host = (env.get("WEB_UI_HOST") or "").strip()
    return _LOOPBACK_FOR.get(host, host), port


def run_checks(environ: Mapping[str, str], *, now: float | None = None) -> HealthReport:
    """Evaluate health from an environment mapping (no I/O besides the two probes)."""
    notes: list[str] = []
    settings = config.Settings.from_env(environ)
    heartbeat = check_heartbeat(
        settings.heartbeat_path,
        max_age=_max_age(environ, notes),
        now=time.time() if now is None else now,
    )
    target = _web_target(environ, notes)
    web = check_web(*target) if target is not None else None
    return HealthReport(heartbeat=heartbeat, web=web, notes=tuple(notes))


def main(argv: Sequence[str] | None = None) -> int:
    """Print one status line and return the exit code (0 healthy, 1 unhealthy).

    Any unexpected error is reported as UNHEALTHY on that one line rather than as a
    traceback (Docker stores the output, and exit codes other than 0/1 are reserved).
    """
    del argv  # no options; accepted for a uniform entrypoint signature
    try:
        env = dict(os.environ)
        config.load_dotenv(environ=env)
        report = run_checks(env)
        line, code = report.line, 0 if report.ok else 1
    except Exception as exc:  # the one-line contract outranks the details
        line, code = f"UNHEALTHY: health check error ({type(exc).__name__})", 1
    sys.stdout.write(line + "\n")
    sys.stdout.flush()
    return code


if __name__ == "__main__":
    sys.exit(main())
