"""Shared JSON-over-HTTP helper (DESIGN §4.3).

Every outbound call to Blockstream, Ethereum RPC nodes, CoinGecko, the IBKR gateway and
Telegram goes through :func:`request_json` so that they all share one policy:

* every request has a finite per-operation timeout (v1's CoinGecko call had none and could
  hang the bot) plus a per-attempt deadline, so a server trickling its body byte by byte
  cannot hold a worker indefinitely either;
* transient failures — HTTP statuses in ``retry_statuses``, socket timeouts (also while
  reading the body), ``URLError``, ``ConnectionError``, ``http.client`` protocol errors and
  undecodable JSON — are retried with capped, jittered exponential backoff, honouring
  ``Retry-After``;
* every failure surfaces as :class:`~ynab_helper.errors.ExternalServiceError` (never
  ``sys.exit``), with secrets passed in ``redact`` replaced by ``***`` in every message.

Retries re-send the request. Callers making non-idempotent requests where a duplicate
side effect matters must pass ``retries=0``.
"""

from __future__ import annotations

import http.client
import json
import logging
import math
import random
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Collection, Iterable, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Final

from ynab_helper import __version__
from ynab_helper.errors import ExternalServiceError

log = logging.getLogger(__name__)

USER_AGENT: Final = f"ynab-helper/{__version__}"
DEFAULT_RETRY_STATUSES: Final[tuple[int, ...]] = (429, 500, 502, 503, 504)
#: Upper bound for a successful response body; protects the bot from a hostile or broken
#: endpoint streaming endless data into memory.
DEFAULT_MAX_RESPONSE_BYTES: Final = 32 * 1024 * 1024
REDACTED: Final = "***"

#: Default per-attempt deadline, as a multiple of the per-operation ``timeout``.
DEFAULT_TOTAL_TIMEOUT_FACTOR: Final = 3

# Error bodies are only read for diagnostics, so a small cap is plenty.
_MAX_ERROR_BODY_BYTES: Final = 64 * 1024
# Bodies are read in chunks so the deadline can be checked between reads.
_READ_CHUNK_BYTES: Final = 64 * 1024
_ERROR_SNIPPET_CHARS: Final = 200
_ALLOWED_SCHEMES: Final = frozenset({"http", "https"})

Opener = Callable[..., Any]
"""``urllib.request.urlopen``-compatible callable: ``opener(request, timeout=, context=)``
returning a context manager whose value has ``status``, ``headers`` and ``read(n)``."""

_rng = random.Random()


class HttpStatusError(ExternalServiceError):
    """The server answered with a non-2xx status.

    ``payload`` is the decoded JSON error body when there was one (e.g. Telegram's
    ``{"ok": false, "description": …}``), else ``None``. ``retry_after`` is the parsed
    ``Retry-After`` header in seconds, if any.
    """

    def __init__(
        self,
        message: str,
        *,
        service: str,
        status: int,
        retriable: bool,
        retry_after: float | None = None,
        payload: Any = None,
        user_message: str | None = None,
    ) -> None:
        super().__init__(
            message,
            service=service,
            status=status,
            retriable=retriable,
            user_message=user_message,
        )
        self.retry_after = retry_after
        self.payload = payload


def redact_text(text: str, secrets: Iterable[str]) -> str:
    """Return ``text`` with every non-empty secret (raw or URL-encoded) replaced by ``***``."""
    return _Redactor(secrets)(text)


class _Redactor:
    """Replaces secrets in strings, including their URL-encoded spellings."""

    def __init__(self, secrets: Iterable[str]) -> None:
        forms: set[str] = set()
        for secret in secrets:
            if not secret:
                continue
            forms.update(
                {
                    secret,
                    urllib.parse.quote(secret, safe=""),
                    urllib.parse.quote_plus(secret, safe=""),
                }
            )
        # Longest first so a secret that contains another one is replaced as a whole.
        self._forms = sorted(forms, key=len, reverse=True)
        #: Length of the longest form; text cut at an arbitrary point may end in a partial
        #: secret of up to this many characters, which no longer matches any form.
        self.longest = len(self._forms[0]) if self._forms else 0

    def __call__(self, text: str) -> str:
        for form in self._forms:
            text = text.replace(form, REDACTED)
        return text


def request_json(
    method: str,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    json_body: Any = None,
    data: bytes | None = None,
    timeout: float = 20,
    total_timeout: float | None = None,
    retries: int = 3,
    retry_statuses: Collection[int] = DEFAULT_RETRY_STATUSES,
    backoff: float = 1.0,
    max_backoff: float = 30,
    service: str = "HTTP",
    redact: Iterable[str] = (),
    ssl_context: ssl.SSLContext | None = None,
    sleep: Callable[[float], None] = time.sleep,
    opener: Opener | None = None,
    parse_float: Callable[[str], Any] | None = None,
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    clock: Callable[[], float] = time.monotonic,
) -> Any:
    """Perform an HTTP request and return the decoded JSON body (``None`` for an empty body).

    Args:
        method: HTTP method, e.g. ``"GET"`` or ``"POST"``.
        url: absolute ``http(s)://`` URL. May contain secrets as long as they are listed
            in ``redact``.
        headers: extra request headers; they override the defaults (User-Agent, Accept).
        json_body: object serialised as the JSON request body (sets Content-Type).
        data: raw request body; mutually exclusive with ``json_body``.
        timeout: socket timeout in seconds for connecting and for each blocking read; must
            be finite > 0. It bounds every single socket operation, not the whole request.
        total_timeout: deadline in seconds for one attempt (default
            ``DEFAULT_TOTAL_TIMEOUT_FACTOR * timeout``). It is checked when the response
            headers have arrived and before every body chunk, so one blocked read can
            overrun it by at most ``timeout``. A server that trickles its *headers* is
            bounded only by ``timeout`` per operation.
        retries: additional attempts after the first one for retriable failures.
        retry_statuses: HTTP statuses treated as transient.
        backoff: base delay in seconds; attempt *n* waits about ``backoff * 2**n``.
        max_backoff: cap for any single wait. A ``Retry-After`` above the cap is not
            waited for: the error is raised immediately (``retriable=True``).
        service: human-readable service name used in errors and logs.
        redact: secrets that must never appear in messages or logs.
        ssl_context: TLS context passed to the opener (``None`` = default verification).
        sleep: injectable sleep function (tests).
        opener: injectable ``urlopen`` replacement (tests); defaults to
            ``urllib.request.urlopen`` looked up at call time.
        parse_float: passed to :func:`json.loads`, e.g. ``decimal.Decimal`` for exact
            prices and balances.
        max_response_bytes: bodies larger than this are rejected.
        clock: injectable monotonic clock for the deadline (tests).

    Raises:
        HttpStatusError: non-2xx response (``retriable`` iff the status is in
            ``retry_statuses``).
        ExternalServiceError: network failure, timeout, missed deadline, truncated,
            oversized or invalid JSON body.
        ValueError: invalid arguments (bad URL or port, missing timeout, …) — a programming
            or configuration error, never retried.
    """
    method = method.upper()
    _validate_arguments(url, timeout, total_timeout, retries, backoff, max_backoff, json_body, data)
    redactor = _Redactor(redact)
    call = _Call(
        method=method,
        url=url,
        safe_url=redactor(url),
        headers=_build_headers(headers, json_body is not None),
        body=json.dumps(json_body).encode("utf-8") if json_body is not None else data,
        timeout=float(timeout),
        total_timeout=float(
            DEFAULT_TOTAL_TIMEOUT_FACTOR * timeout if total_timeout is None else total_timeout
        ),
        retry_statuses=frozenset(retry_statuses),
        service=service,
        redactor=redactor,
        ssl_context=ssl_context,
        opener=opener or urllib.request.urlopen,
        parse_float=parse_float,
        max_response_bytes=max_response_bytes,
        clock=clock,
    )
    attempt = 0
    while True:
        try:
            return call.perform()
        except ExternalServiceError as exc:
            delay = _retry_delay(exc, attempt, retries, backoff, max_backoff)
            if delay is None:
                raise
            log.warning(
                "%s %s %s failed (%s); retry %d/%d in %.1fs",
                service,
                method,
                call.safe_url,
                exc,
                attempt + 1,
                retries,
                delay,
            )
            sleep(delay)
            attempt += 1


def backoff_delay(attempt: int, backoff: float, max_backoff: float) -> float:
    """Jittered exponential delay for retry ``attempt`` (0-based), capped at ``max_backoff``.

    The jitter (50–100 % of the nominal delay) spreads retries of concurrent callers.
    """
    nominal = min(max_backoff, backoff * (2**attempt))
    return nominal * (0.5 + 0.5 * _rng.random())


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """Parse a ``Retry-After`` header (delta-seconds or HTTP-date) into seconds ≥ 0."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = (when - (now or datetime.now(UTC))).total_seconds()
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)


# ── internals ────────────────────────────────────────────────────────────────


def _validate_arguments(
    url: str,
    timeout: float,
    total_timeout: float | None,
    retries: int,
    backoff: float,
    max_backoff: float,
    json_body: Any,
    data: bytes | None,
) -> None:
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in _ALLOWED_SCHEMES or not parts.hostname:
        raise ValueError("url must be an absolute http(s) URL with a host")
    if any(ch.isspace() or ord(ch) < 0x20 for ch in url):
        raise ValueError("url must not contain whitespace or control characters")
    try:
        # urllib would only fail on a bad port inside the retry loop, where it looks like
        # a transient network error; it is a permanent configuration error.
        parts.port  # noqa: B018 - evaluated for its validation side effect
    except ValueError:
        raise ValueError("url has an invalid port") from None
    _validate_seconds("timeout", timeout)
    if total_timeout is not None:
        _validate_seconds("total_timeout", total_timeout)
    if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
        raise ValueError("retries must be an int >= 0")
    if backoff < 0 or max_backoff < 0:
        raise ValueError("backoff and max_backoff must be >= 0")
    if json_body is not None and data is not None:
        raise ValueError("pass either json_body or data, not both")


def _validate_seconds(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{name} must be a number of seconds")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and > 0")


def _build_headers(extra: Mapping[str, str] | None, has_json_body: bool) -> dict[str, str]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if has_json_body:
        headers["Content-Type"] = "application/json"
    if extra:
        headers.update(extra)
    return headers


def _retry_delay(
    exc: ExternalServiceError, attempt: int, retries: int, backoff: float, max_backoff: float
) -> float | None:
    """Seconds to wait before the next attempt, or ``None`` to give up and raise."""
    if not exc.retriable or attempt >= retries:
        return None
    retry_after = getattr(exc, "retry_after", None)
    if retry_after is not None:
        # Waiting less than the server asked for would only burn the attempt; waiting
        # longer than our cap would block the caller (e.g. the bot's worker) too long.
        return retry_after if retry_after <= max_backoff else None
    return backoff_delay(attempt, backoff, max_backoff)


def _reject_json_constant(name: str) -> Any:
    # NaN/Infinity are not JSON; accepting them would let a non-finite price or balance
    # through to arithmetic that posts money.
    raise ValueError(f"non-standard JSON constant {name}")


class _DeadlineExceeded(Exception):
    """Internal: the attempt's ``total_timeout`` passed while reading the response."""


def _ensure_complete(resp: Any) -> None:
    """Raise ``IncompleteRead`` if the peer closed before ``Content-Length`` bytes arrived.

    ``http.client`` returns a short body without an error in that case; a truncated JSON
    number would still parse, so the check matters for balances.
    """
    remaining = getattr(resp, "length", None)
    if isinstance(remaining, int) and not isinstance(remaining, bool) and remaining > 0:
        raise http.client.IncompleteRead(b"", remaining)


def _status_of(resp: Any) -> int:
    status = getattr(resp, "status", None)
    if status is None:
        status = resp.getcode()
    return int(status)


def _header(headers: Any, name: str) -> str | None:
    if headers is None:
        return None
    try:
        value = headers.get(name)
    except AttributeError:
        return None
    return str(value) if value is not None else None


def _is_timeout(exc: BaseException) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    return isinstance(exc, urllib.error.URLError) and isinstance(exc.reason, TimeoutError)


def _is_certificate_error(exc: BaseException) -> bool:
    if isinstance(exc, ssl.SSLCertVerificationError):
        return True
    return isinstance(exc, urllib.error.URLError) and isinstance(
        exc.reason, ssl.SSLCertVerificationError
    )


def _is_permanent(exc: BaseException) -> bool:
    """Failures that would fail the same way on every retry."""
    # A certificate that fails verification, or a URL http.client cannot use (normally
    # caught earlier by _validate_arguments), will not get better by waiting.
    return _is_certificate_error(exc) or isinstance(exc, http.client.InvalidURL)


class _Call:
    """One logical request (possibly attempted several times)."""

    def __init__(
        self,
        *,
        method: str,
        url: str,
        safe_url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
        total_timeout: float,
        retry_statuses: frozenset[int],
        service: str,
        redactor: _Redactor,
        ssl_context: ssl.SSLContext | None,
        opener: Opener,
        parse_float: Callable[[str], Any] | None,
        max_response_bytes: int,
        clock: Callable[[], float],
    ) -> None:
        self.method = method
        self.url = url
        self.safe_url = safe_url
        self.headers = headers
        self.body = body
        self.timeout = timeout
        self.total_timeout = total_timeout
        self.retry_statuses = retry_statuses
        self.service = service
        self.redact = redactor
        self.ssl_context = ssl_context
        self.opener = opener
        self.parse_float = parse_float
        self.max_response_bytes = max_response_bytes
        self.clock = clock

    def perform(self) -> Any:
        request = urllib.request.Request(
            self.url, data=self.body, headers=self.headers, method=self.method
        )
        deadline = self.clock() + self.total_timeout
        # Exceptions are re-raised "from None": the originals (e.g. HTTPError.url) can carry
        # the unredacted URL into tracebacks.
        try:
            with self.opener(request, timeout=self.timeout, context=self.ssl_context) as resp:
                status = _status_of(resp)
                if not 200 <= status < 300:
                    raw = self._read_error_body(resp, deadline)
                    raise self._status_error(status, resp.headers, raw)
                raw = self._read_body(resp, self.max_response_bytes, deadline)
                if len(raw) <= self.max_response_bytes:
                    _ensure_complete(resp)
        except urllib.error.HTTPError as exc:
            raise self._http_error(exc, deadline) from None
        except _DeadlineExceeded:
            raise self._deadline_error() from None
        except (OSError, http.client.HTTPException) as exc:
            # OSError covers URLError, TimeoutError/socket.timeout and ConnectionError.
            raise self._network_error(exc) from None
        if len(raw) > self.max_response_bytes:
            raise ExternalServiceError(
                f"{self._what()} returned more than {self.max_response_bytes} bytes",
                service=self.service,
                status=status,
                retriable=False,
                user_message=f"{self.service} returned an unexpectedly large response.",
            )
        return self._decode(raw, status)

    def _what(self) -> str:
        return f"{self.service} {self.method} {self.safe_url}"

    def _read_body(self, stream: Any, limit: int, deadline: float) -> bytes:
        """Read to EOF; stop early (returning ``limit + 1`` bytes) if the body is larger.

        ``read1`` (one socket read per call) is preferred: ``read(n)`` on a buffered stream
        keeps reading until ``n`` bytes arrive, which would hide a trickling server from the
        deadline check.

        Raises:
            _DeadlineExceeded: ``deadline`` passed before the body was complete.
        """
        read1 = getattr(stream, "read1", None)
        read = read1 if callable(read1) else stream.read
        chunks: list[bytes] = []
        size = 0
        while size <= limit:
            if self.clock() >= deadline:
                raise _DeadlineExceeded
            chunk = read(min(_READ_CHUNK_BYTES, limit + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
        return b"".join(chunks)

    def _read_error_body(self, stream: Any, deadline: float) -> bytes:
        # The status already decides the outcome; the body is diagnostics only, so a body
        # that fails or takes too long to read is simply left out.
        try:
            return self._read_body(stream, _MAX_ERROR_BODY_BYTES, deadline)
        except (_DeadlineExceeded, OSError, http.client.HTTPException, AttributeError):
            return b""

    def _decode(self, raw: bytes, status: int) -> Any:
        if not raw.strip():
            return None
        try:
            return json.loads(
                raw,
                parse_float=self.parse_float,
                parse_constant=_reject_json_constant,
            )
        except ValueError as exc:  # JSONDecodeError, UnicodeDecodeError, NaN/Infinity
            raise ExternalServiceError(
                f"{self._what()} returned invalid JSON (HTTP {status}): "
                f"{self.redact(str(exc))[:_ERROR_SNIPPET_CHARS]}",
                service=self.service,
                status=status,
                retriable=True,
                user_message=f"{self.service} returned an invalid response. Please try again later.",
            ) from None

    def _http_error(self, exc: urllib.error.HTTPError, deadline: float) -> HttpStatusError:
        try:
            raw = self._read_error_body(exc, deadline)
        finally:
            exc.close()
        return self._status_error(exc.code, exc.headers, raw)

    def _status_error(self, status: int, headers: Any, raw: bytes) -> HttpStatusError:
        retriable = status in self.retry_statuses
        payload: Any = None
        truncated = len(raw) > _MAX_ERROR_BODY_BYTES
        text = raw[:_MAX_ERROR_BODY_BYTES].decode("utf-8", errors="replace")
        if truncated:
            # The read limit may have cut a secret in two, and a partial secret escapes the
            # redactor; whitespace collapsing below could then pull it into the snippet.
            text = text[: max(0, len(text) - self.redact.longest)]
        text = text.strip()
        if text and not truncated:
            try:
                payload = json.loads(text, parse_constant=_reject_json_constant)
            except ValueError:
                payload = None
        # Redact before cutting: a secret straddling the cut would otherwise leak a prefix.
        snippet = self.redact(" ".join(text.split()))[:_ERROR_SNIPPET_CHARS]
        message = f"{self._what()} failed: HTTP {status}"
        if snippet:
            message += f": {snippet}"
        hint = " Please try again later." if retriable else ""
        return HttpStatusError(
            message,
            service=self.service,
            status=status,
            retriable=retriable,
            retry_after=parse_retry_after(_header(headers, "Retry-After")),
            payload=payload,
            user_message=f"{self.service} returned an error (HTTP {status}).{hint}",
        )

    def _network_error(self, exc: BaseException) -> ExternalServiceError:
        if _is_timeout(exc):
            reason = f"timed out after {self.timeout:g}s"
        else:
            detail = exc.reason if isinstance(exc, urllib.error.URLError) else exc
            reason = f"{type(detail).__name__}: {detail}" if str(detail) else type(detail).__name__
        reason = self.redact(reason)[:_ERROR_SNIPPET_CHARS]
        retriable = not _is_permanent(exc)
        hint = " Please try again later." if retriable else ""
        return ExternalServiceError(
            f"{self._what()} failed: {reason}",
            service=self.service,
            status=None,
            retriable=retriable,
            user_message=f"Could not reach {self.service} ({reason}).{hint}",
        )

    def _deadline_error(self) -> ExternalServiceError:
        reason = f"no complete response within {self.total_timeout:g}s"
        return ExternalServiceError(
            f"{self._what()} timed out: {reason}",
            service=self.service,
            status=None,
            retriable=True,
            user_message=f"{self.service} did not respond in time ({reason}). "
            "Please try again later.",
        )
