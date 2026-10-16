"""YNAB REST API v1 client (docs/DESIGN.md §4.2).

Design points that matter for the user's financial data:

* **Typed errors.** Every failure becomes a subclass of :class:`~ynab_helper.errors.YnabError`
  (mapped from the HTTP status and YNAB's ``{"error": {"id", "name", "detail"}}`` body);
  raw ``OSError``/``JSONDecodeError`` never escape (finding F17).
* **Conservative retries.** Only network errors and 5xx are retried, at most
  ``max_retries`` times, with jittered exponential backoff (or the server's ``Retry-After``
  when it is at most :data:`MAX_RETRY_AFTER` seconds). A 429 is never retried: YNAB's quota
  is 200 requests/hour per token, so waiting inside a request would block the bot for up
  to an hour; :class:`~ynab_helper.errors.YnabRateLimited` is raised instead.
  A ``POST`` is retried only when every transaction in it carries an ``import_id`` — YNAB
  dedupes those, so a retry after a lost response cannot create a second copy.
* **Bulk endpoints.** Creates are sent in chunks of 100 and updates through the bulk
  ``PATCH /budgets/{b}/transactions`` in chunks of 50 to save quota.
* **No secrets in errors or logs.** The token is only ever placed in the ``Authorization``
  header; every message derived from external data is passed through a redactor, and
  transport exceptions are not chained (their text could include anything).
* **Caller bugs fail before any request.** Payloads are materialised once (a generator is
  never half-consumed by validation), amounts must be integer milliunits, import_ids at
  most 36 characters, and NaN/Infinity can never be serialised.
"""

from __future__ import annotations

import http.client
import json
import logging
import random
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from ynab_helper import __version__
from ynab_helper.errors import (
    ConfigError,
    YnabAuthError,
    YnabError,
    YnabNetworkError,
    YnabNotFound,
    YnabRateLimited,
    YnabServerError,
    YnabValidationError,
)
from ynab_helper.net import parse_retry_after, redact_text
from ynab_helper.ynab.models import YnabAccount, YnabTransaction

__all__ = [
    "CREATE_CHUNK_SIZE",
    "DEFAULT_BASE_URL",
    "MAX_IMPORT_ID_LENGTH",
    "MAX_RETRY_AFTER",
    "UPDATE_CHUNK_SIZE",
    "CreateResult",
    "ResponseTooLarge",
    "Transport",
    "YnabClient",
    "urllib_transport",
]

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.ynab.com/v1"
#: YNAB accepts at most this many transactions per create request in our usage.
CREATE_CHUNK_SIZE = 100
#: Bulk PATCH chunk size (kept small so one rejected row affects few others).
UPDATE_CHUNK_SIZE = 50
#: YNAB rejects longer import_ids.
MAX_IMPORT_ID_LENGTH = 36
#: The longest server-requested ``Retry-After`` (seconds) we are willing to sleep for.
MAX_RETRY_AFTER = 10.0
#: Upper bound for the computed (non Retry-After) backoff delay.
MAX_BACKOFF = 10.0
#: Defensive cap on response bodies (a full budget delta is a few MB).
MAX_RESPONSE_BYTES = 64 * 1024 * 1024

USER_AGENT = f"ynab-helper/{__version__}"

#: ``(method, url, headers, body, timeout) -> (status, headers, body)``.
#: Implementations return every HTTP status (including 4xx/5xx) as a value and raise
#: ``OSError`` (``TimeoutError``, ``ConnectionError``, ``urllib.error.URLError`` …) or
#: ``http.client.HTTPException`` when no HTTP response was obtained.
Transport = Callable[
    [str, str, Mapping[str, str], bytes | None, float],
    tuple[int, Mapping[str, str], bytes],
]

_NETWORK_ERRORS: tuple[type[BaseException], ...] = (OSError, http.client.HTTPException)
_ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


# ─── default transport ──────────────────────────────────────────────────────


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse redirects: urllib would replay the Authorization header to the new host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


class ResponseTooLarge(http.client.HTTPException):
    """The response body exceeded :data:`MAX_RESPONSE_BYTES`.

    Raised by :func:`urllib_transport`; the client does not retry it (the next attempt
    would download the same oversized body again).
    """


def _read_capped(stream: Any) -> bytes:
    payload = stream.read(MAX_RESPONSE_BYTES + 1)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise ResponseTooLarge(f"response body exceeds {MAX_RESPONSE_BYTES} bytes")
    return payload


def urllib_transport(
    method: str,
    url: str,
    headers: Mapping[str, str],
    body: bytes | None,
    timeout: float,
) -> tuple[int, Mapping[str, str], bytes]:
    """Default :data:`Transport` built on ``urllib`` (stdlib only, redirects disabled)."""
    request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    try:
        with _OPENER.open(request, timeout=timeout) as response:
            return response.status, dict(response.headers.items()), _read_capped(response)
    except urllib.error.HTTPError as err:
        # An HTTP error status is a response, not a transport failure.
        try:
            payload = _read_capped(err)
        except _NETWORK_ERRORS:
            payload = b""
        finally:
            err.close()
        err_headers = dict(err.headers.items()) if err.headers is not None else {}
        return err.code, err_headers, payload


# ─── results ────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class CreateResult:
    """Outcome of :meth:`YnabClient.create_transactions`.

    ``created`` holds the transactions YNAB created (with their ``import_id``),
    ``duplicate_import_ids`` the import_ids YNAB skipped because the account already has
    them (possibly as a deleted transaction). ``server_knowledge`` is from the last chunk.
    """

    created: list[YnabTransaction] = field(default_factory=list)
    duplicate_import_ids: list[str] = field(default_factory=list)
    server_knowledge: int | None = None


# ─── helpers ────────────────────────────────────────────────────────────────


def _parse_rate_limit(value: str | None) -> tuple[int, int] | None:
    """``"36/200"`` → ``(36, 200)``; None for anything else."""
    if not value:
        return None
    used, sep, total = value.strip().partition("/")
    if not sep:
        return None
    try:
        return int(used), int(total)
    except ValueError:
        return None


def _segment(value: str, name: str) -> str:
    """Quote one path segment so ids from storage/callbacks cannot alter the URL path."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return urllib.parse.quote(value, safe="")


def _iso_date(value: str | date) -> str:
    """``YYYY-MM-DD`` for a ``since_date`` argument.

    A ``datetime`` is reduced to its own calendar date: YNAB only accepts dates, and
    dropping the time can only move the lower bound earlier (never skips a transaction).
    """
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and _ISO_DATE_RE.fullmatch(value):
        try:
            date.fromisoformat(value)
        except ValueError:
            pass
        else:
            return value
    raise ValueError("since_date must be a date or an ISO date string (YYYY-MM-DD)")


def _materialise(items: Iterable[Mapping[str, Any]], name: str) -> list[Mapping[str, Any]]:
    """Turn ``items`` into a list exactly once.

    Validation and chunking both iterate the payloads; a generator consumed by the first
    pass would otherwise make the second one send nothing, silently.
    """
    if isinstance(items, (str, bytes, Mapping)):
        raise TypeError(f"{name} must be a sequence of mappings, not {type(items).__name__}")
    out = list(items)
    for item in out:
        if not isinstance(item, Mapping):
            raise TypeError(f"every item of {name} must be a mapping, not {type(item).__name__}")
    return out


def _chunks(items: list[Any], size: int) -> Iterator[list[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _check_amount(value: Any, name: str) -> None:
    # bool is an int subclass and floats lose milliunit precision: both are caller bugs.
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be integer milliunits")


def _check_import_id(value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not value or len(value) > MAX_IMPORT_ID_LENGTH:
        raise ValueError(
            f"import_id must be a non-empty string of at most {MAX_IMPORT_ID_LENGTH} chars"
        )


def _check_subtransactions(value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, (list, tuple)) or not value:
        raise ValueError("subtransactions must be a non-empty list")
    for sub in value:
        if not isinstance(sub, Mapping):
            raise ValueError("every subtransaction must be a mapping")
        _check_amount(sub.get("amount"), "subtransaction amount")


def _check_create_payload(tx: Mapping[str, Any]) -> None:
    """Catch programming errors in a create payload before they reach YNAB (and burn quota)."""
    _check_amount(tx.get("amount"), "transaction amount")
    if not tx.get("account_id") or not tx.get("date"):
        raise ValueError("transaction needs account_id and date")
    _check_import_id(tx.get("import_id"))
    _check_subtransactions(tx.get("subtransactions"))


def _check_update_fields(fields: Mapping[str, Any]) -> None:
    """Same checks for the fields of an update (only the fields that are present)."""
    if "amount" in fields:
        _check_amount(fields["amount"], "transaction amount")
    _check_import_id(fields.get("import_id"))
    _check_subtransactions(fields.get("subtransactions"))


def _check_bulk_updates(updates: list[Mapping[str, Any]]) -> None:
    seen: set[str] = set()
    for update in updates:
        tx_id = update.get("id")
        if not isinstance(tx_id, str) or not tx_id:
            raise ValueError("every bulk update needs a transaction 'id'")
        if tx_id in seen:
            # Two updates for one transaction in a batch make the result order-dependent.
            raise ValueError("a bulk update lists the same transaction id twice")
        seen.add(tx_id)
        _check_update_fields(update)


def _encode_json(body: Any) -> bytes:
    """Compact UTF-8 JSON; ``ValueError`` for NaN/Infinity, ``TypeError`` for non-JSON types."""
    # allow_nan=False: NaN/Infinity are not JSON and must never reach a money field.
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
        "utf-8"
    )


def _is_permanent_network_error(exc: BaseException) -> bool:
    """Transport failures that would fail the same way on every retry."""
    reason = exc.reason if isinstance(exc, urllib.error.URLError) else exc
    return isinstance(
        reason, (ssl.SSLCertVerificationError, ResponseTooLarge, http.client.InvalidURL)
    )


@dataclass(frozen=True, slots=True)
class _Failure:
    """A failed attempt: the error to raise and whether a retry may help."""

    error: YnabError
    retriable: bool
    retry_after: float | None = None


# ─── client ─────────────────────────────────────────────────────────────────


class YnabClient:
    """Synchronous YNAB API client with typed errors, retries and bulk helpers.

    ``sleep`` and ``jitter`` are injectable so tests can verify backoff without waiting.
    After each response ``rate_limit_used`` / ``rate_limit_total`` hold the parsed
    ``X-Rate-Limit`` header (``"36/200"``), or None if YNAB did not send it.
    """

    def __init__(
        self,
        token: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30.0,
        transport: Transport | None = None,
        max_retries: int = 2,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = random.random,
        backoff_base: float = 1.0,
    ) -> None:
        token = (token or "").strip() if isinstance(token, str) else ""
        if not token:
            raise ConfigError("YNAB token is empty", user_message="The YNAB token is missing.")
        # Header injection guard; also keeps http.client from echoing the header value
        # (which contains the token) in a ValueError.
        if any(ch.isspace() or not ch.isprintable() or ord(ch) > 126 for ch in token):
            raise ConfigError(
                "YNAB token contains invalid characters",
                user_message="The YNAB token contains invalid characters. Please paste it again.",
            )
        parsed = urllib.parse.urlsplit(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ConfigError(f"invalid YNAB base URL scheme/host: {parsed.scheme!r}")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        self._token = token
        self.base_url = base_url.rstrip("/")
        self.timeout = float(timeout)
        self.max_retries = int(max_retries)
        self._transport: Transport = transport or urllib_transport
        self._sleep = sleep
        self._jitter = jitter
        self._backoff_base = float(backoff_base)
        self.rate_limit_used: int | None = None
        self.rate_limit_total: int | None = None
        self.requests_sent = 0

    def __repr__(self) -> str:  # never include the token
        return f"YnabClient(base_url={self.base_url!r})"

    # ── public API ──────────────────────────────────────────────────────────

    def get_user(self) -> dict[str, Any]:
        """``GET /user`` → the user object (used to validate a token)."""
        return self._data_obj(self._request("GET", "/user"), "user")

    def get_budgets(self) -> list[dict[str, Any]]:
        """``GET /budgets`` → raw budget summaries."""
        data = self._request("GET", "/budgets")
        budgets = data.get("budgets")
        if not isinstance(budgets, list):
            raise self._shape_error("budgets")
        return budgets

    def get_budget_currency(self, budget_id: str) -> str | None:
        """ISO currency code of the budget (``settings.currency_format.iso_code``), or None."""
        path = f"/budgets/{_segment(budget_id, 'budget_id')}/settings"
        settings = self._data_obj(self._request("GET", path), "settings")
        currency_format = settings.get("currency_format")
        if not isinstance(currency_format, Mapping):
            return None
        iso = currency_format.get("iso_code")
        return iso if isinstance(iso, str) and iso else None

    def get_accounts(
        self, budget_id: str, last_knowledge: int | None = None
    ) -> tuple[list[YnabAccount], int]:
        """``GET /budgets/{b}/accounts`` (delta when ``last_knowledge`` is given)."""
        path = f"/budgets/{_segment(budget_id, 'budget_id')}/accounts"
        query = {} if last_knowledge is None else {"last_knowledge_of_server": int(last_knowledge)}
        data = self._request("GET", path, query=query)
        accounts = data.get("accounts")
        if not isinstance(accounts, list):
            raise self._shape_error("accounts")
        return [YnabAccount.from_api(a) for a in accounts], self._knowledge(data)

    def get_account(self, budget_id: str, account_id: str) -> YnabAccount:
        """``GET /budgets/{b}/accounts/{a}``."""
        path = (
            f"/budgets/{_segment(budget_id, 'budget_id')}"
            f"/accounts/{_segment(account_id, 'account_id')}"
        )
        return YnabAccount.from_api(self._data_obj(self._request("GET", path), "account"))

    def get_transactions(
        self,
        budget_id: str,
        *,
        account_id: str | None = None,
        since_date: str | date | None = None,
        last_knowledge: int | None = None,
    ) -> tuple[list[YnabTransaction], int]:
        """Budget-wide or per-account transactions; returns ``(transactions, server_knowledge)``.

        Without ``last_knowledge`` YNAB returns a full list *excluding* deleted transactions;
        with it, a delta of everything changed since, deleted ones included. ``since_date``
        is a ``date`` (a ``datetime`` counts as its calendar date) or ``"YYYY-MM-DD"``.
        """
        path = f"/budgets/{_segment(budget_id, 'budget_id')}"
        if account_id is not None:
            path += f"/accounts/{_segment(account_id, 'account_id')}"
        path += "/transactions"
        query: dict[str, Any] = {}
        if since_date is not None:
            query["since_date"] = _iso_date(since_date)
        if last_knowledge is not None:
            query["last_knowledge_of_server"] = int(last_knowledge)
        data = self._request("GET", path, query=query)
        txs = data.get("transactions")
        if not isinstance(txs, list):
            raise self._shape_error("transactions")
        return [YnabTransaction.from_api(t) for t in txs], self._knowledge(data)

    def iter_create_transactions(
        self, budget_id: str, txs: Iterable[Mapping[str, Any]]
    ) -> Iterator[CreateResult]:
        """POST ``txs`` in chunks of :data:`CREATE_CHUNK_SIZE`, yielding one result per chunk.

        Lets the executor persist each chunk as soon as YNAB confirmed it; if a later chunk
        raises, earlier chunks have already been yielded. ``txs`` may be any iterable; it
        is read once and validated completely (``ValueError``/``TypeError``) before this
        method returns, so no request is sent for a batch containing a bad payload.
        """
        items = _materialise(txs, "txs")
        for tx in items:
            _check_create_payload(tx)
        # A non-JSON value in chunk 3 must not surface after chunks 1 and 2 were sent.
        _encode_json([dict(tx) for tx in items])
        path = f"/budgets/{_segment(budget_id, 'budget_id')}/transactions"
        return self._create_chunks(path, items)

    def _create_chunks(self, path: str, items: list[Mapping[str, Any]]) -> Iterator[CreateResult]:
        for chunk in _chunks(items, CREATE_CHUNK_SIZE):
            payload = {"transactions": [dict(tx) for tx in chunk]}
            # Safe to retry only if YNAB will dedupe a replay of this exact request.
            idempotent = all(tx.get("import_id") for tx in chunk)
            data = self._request("POST", path, body=payload, idempotent=idempotent)
            created = data.get("transactions") or []
            dupes = data.get("duplicate_import_ids") or []
            if not isinstance(created, list) or not isinstance(dupes, list):
                raise self._shape_error("transactions")
            yield CreateResult(
                created=[YnabTransaction.from_api(t) for t in created],
                duplicate_import_ids=[str(d) for d in dupes],
                server_knowledge=self._knowledge(data, required=False),
            )

    def create_transactions(self, budget_id: str, txs: Iterable[Mapping[str, Any]]) -> CreateResult:
        """Create transactions (bulk POST, chunked by 100) and merge the chunk results.

        If a chunk fails, the exception propagates and the results of earlier (already
        created) chunks are not returned; callers that must record progress per chunk use
        :meth:`iter_create_transactions`. A replay is safe when every row has an import_id.
        """
        created: list[YnabTransaction] = []
        dupes: list[str] = []
        knowledge: int | None = None
        for result in self.iter_create_transactions(budget_id, txs):
            created.extend(result.created)
            dupes.extend(result.duplicate_import_ids)
            knowledge = result.server_knowledge
        return CreateResult(created=created, duplicate_import_ids=dupes, server_knowledge=knowledge)

    def iter_update_transactions(
        self, budget_id: str, updates: Iterable[Mapping[str, Any]]
    ) -> Iterator[list[YnabTransaction]]:
        """Bulk ``PATCH /budgets/{b}/transactions`` in chunks of 50, yielding YNAB's rows.

        Every update needs a unique ``id``. Like :meth:`iter_create_transactions`, the
        input is read once and validated completely before this method returns.
        """
        items = _materialise(updates, "updates")
        _check_bulk_updates(items)
        _encode_json([dict(u) for u in items])  # see iter_create_transactions
        path = f"/budgets/{_segment(budget_id, 'budget_id')}/transactions"
        return self._update_chunks(path, items)

    def _update_chunks(
        self, path: str, items: list[Mapping[str, Any]]
    ) -> Iterator[list[YnabTransaction]]:
        for chunk in _chunks(items, UPDATE_CHUNK_SIZE):
            payload = {"transactions": [dict(u) for u in chunk]}
            data = self._request("PATCH", path, body=payload)
            txs = data.get("transactions") or []
            if not isinstance(txs, list):
                raise self._shape_error("transactions")
            yield [YnabTransaction.from_api(t) for t in txs]

    def update_transactions(
        self, budget_id: str, updates: Iterable[Mapping[str, Any]]
    ) -> list[YnabTransaction]:
        """Bulk-update transactions (each update has ``id``); returns the updated rows."""
        out: list[YnabTransaction] = []
        for chunk in self.iter_update_transactions(budget_id, updates):
            out.extend(chunk)
        return out

    def update_transaction(
        self, budget_id: str, tx_id: str, fields: Mapping[str, Any]
    ) -> YnabTransaction:
        """``PUT /budgets/{b}/transactions/{id}`` with ``{"transaction": fields}``.

        Only the given fields change. YNAB ignores ``import_id`` on updates (immutable).
        """
        if not isinstance(fields, Mapping):
            raise TypeError(f"fields must be a mapping, not {type(fields).__name__}")
        _check_update_fields(fields)
        path = (
            f"/budgets/{_segment(budget_id, 'budget_id')}/transactions/{_segment(tx_id, 'tx_id')}"
        )
        data = self._request("PUT", path, body={"transaction": dict(fields)})
        return YnabTransaction.from_api(self._data_obj(data, "transaction"))

    def delete_transaction(self, budget_id: str, tx_id: str) -> YnabTransaction:
        """``DELETE /budgets/{b}/transactions/{id}``; YNAB soft-deletes and returns the row."""
        path = (
            f"/budgets/{_segment(budget_id, 'budget_id')}/transactions/{_segment(tx_id, 'tx_id')}"
        )
        data = self._request("DELETE", path)
        return YnabTransaction.from_api(self._data_obj(data, "transaction"))

    # ── request machinery ───────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, Any] | None = None,
        body: Any = None,
        idempotent: bool | None = None,
    ) -> dict[str, Any]:
        """Send one API call (with retries) and return the response's ``data`` object.

        ``idempotent`` defaults to True for every method except POST.
        """
        if idempotent is None:
            idempotent = method != "POST"
        url = self.base_url + path
        if query:
            url += "?" + urllib.parse.urlencode({k: str(v) for k, v in query.items()})
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }
        payload: bytes | None = None
        if body is not None:
            payload = _encode_json(body)
            headers["Content-Type"] = "application/json; charset=utf-8"

        attempt = 0
        while True:
            outcome = self._attempt(method, path, url, headers, payload)
            if not isinstance(outcome, _Failure):
                return outcome
            if not (idempotent and outcome.retriable and attempt < self.max_retries):
                # Raised outside any except block: no transport exception is chained.
                raise outcome.error
            delay = outcome.retry_after
            if delay is None:
                delay = self._backoff(attempt)
            attempt += 1
            log.warning(
                "YNAB %s %s failed (%s); retry %d/%d in %.1fs",
                method,
                path,
                outcome.error.detail or type(outcome.error).__name__,
                attempt,
                self.max_retries,
                delay,
            )
            self._sleep(delay)

    def _attempt(
        self,
        method: str,
        path: str,
        url: str,
        headers: Mapping[str, str],
        payload: bytes | None,
    ) -> dict[str, Any] | _Failure:
        """One HTTP exchange → the ``data`` object, or a :class:`_Failure`."""
        started = time.monotonic()
        transport_error: BaseException | None = None
        try:
            status, raw_headers, raw_body = self._transport(
                method, url, headers, payload, self.timeout
            )
        except _NETWORK_ERRORS as exc:
            transport_error = exc
        self.requests_sent += 1
        if transport_error is not None:
            return self._network_failure(method, path, transport_error)

        resp_headers = {str(k).lower(): str(v) for k, v in (raw_headers or {}).items()}
        self._note_rate_limit(resp_headers.get("x-rate-limit"))
        log.debug(
            "YNAB %s %s -> %s in %.0f ms (rate limit %s/%s)",
            method,
            path,
            status,
            (time.monotonic() - started) * 1000,
            self.rate_limit_used,
            self.rate_limit_total,
        )
        if 200 <= status < 300:
            data = self._parse_success(raw_body)
            if data is None:
                return _Failure(
                    YnabError(
                        f"YNAB {method} {path}: HTTP {status} response is not a JSON 'data' object",
                        status=status,
                        user_message="YNAB returned an unexpected response. Please try again later.",
                    ),
                    retriable=False,
                )
            return data
        error = self._map_error(method, path, status, resp_headers, raw_body)
        if not isinstance(error, YnabServerError):
            return _Failure(error, retriable=False)
        retry_after = parse_retry_after(resp_headers.get("retry-after"))
        if retry_after is not None and retry_after > MAX_RETRY_AFTER:
            # The server asked for a long pause: give up now instead of burning quota on a
            # retry that is expected to fail.
            return _Failure(error, retriable=False)
        return _Failure(error, retriable=True, retry_after=retry_after)

    def _network_failure(self, method: str, path: str, exc: BaseException) -> _Failure:
        detail = self._redact(f"{type(exc).__name__}: {exc}")[:300]
        retriable = not _is_permanent_network_error(exc)
        reason = exc.reason if isinstance(exc, urllib.error.URLError) else exc
        if isinstance(reason, ssl.SSLCertVerificationError):
            user_message = (
                "Could not verify YNAB's TLS certificate. Check the system clock and the "
                "CA certificates of this machine."
            )
        elif isinstance(reason, ResponseTooLarge):
            user_message = "YNAB returned an unexpectedly large response."
        elif not retriable:
            user_message = "Could not reach YNAB: the configured API address is invalid."
        else:
            user_message = "Could not reach YNAB (network problem). Please try again later."
        error = YnabNetworkError(
            f"YNAB {method} {path}: network error ({detail})",
            detail=detail,
            retriable=retriable,
            user_message=user_message,
        )
        return _Failure(error, retriable=retriable)

    def _backoff(self, attempt: int) -> float:
        """Jittered exponential delay before retry number ``attempt + 1``.

        Same formula as :func:`ynab_helper.net.backoff_delay`, but with the injectable
        ``jitter`` so tests can assert exact delays.
        """
        base = min(MAX_BACKOFF, self._backoff_base * (2**attempt))
        return base * (0.5 + 0.5 * self._jitter())

    @staticmethod
    def _parse_success(raw: bytes) -> dict[str, Any] | None:
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return None
        data = parsed.get("data") if isinstance(parsed, dict) else None
        return data if isinstance(data, dict) else None

    def _map_error(
        self,
        method: str,
        path: str,
        status: int,
        headers: Mapping[str, str],
        raw: bytes,
    ) -> YnabError:
        error_id, name, detail = self._parse_error_body(raw)
        label = " ".join(p for p in (name, detail) if p) or "no details"
        message = f"YNAB {method} {path}: HTTP {status} ({label})"
        common: dict[str, Any] = {"error_id": error_id, "detail": detail}
        if status == 401:
            return YnabAuthError(
                message,
                status=status,
                user_message="YNAB rejected the access token. Please check your personal access token.",
                **common,
            )
        if status == 403:
            return YnabAuthError(
                message,
                status=status,
                user_message=(
                    "YNAB denied access"
                    + (f": {detail}" if detail else "")
                    + ". Check your YNAB subscription and token."
                ),
                **common,
            )
        if status == 404:
            return YnabNotFound(
                message,
                status=status,
                user_message="YNAB could not find the budget, account or transaction.",
                **common,
            )
        if status == 429:
            retry_after = parse_retry_after(headers.get("retry-after"))
            return YnabRateLimited(message, retry_after=retry_after, **common)
        if status in (400, 409, 422):
            return YnabValidationError(
                message,
                status=status,
                user_message="YNAB rejected the request" + (f": {detail}" if detail else "."),
                **common,
            )
        if 500 <= status < 600:
            return YnabServerError(
                message,
                status=status,
                retriable=True,
                user_message=f"YNAB is temporarily unavailable (HTTP {status}). Please try again later.",
                **common,
            )
        return YnabError(
            message,
            status=status,
            user_message=f"YNAB request failed (HTTP {status}).",
            **common,
        )

    def _parse_error_body(self, raw: bytes) -> tuple[str | None, str | None, str | None]:
        """Extract ``(id, name, detail)`` from YNAB's error body; Nones for non-JSON bodies."""
        try:
            parsed = json.loads(raw.decode("utf-8")) if raw else None
        except (UnicodeDecodeError, ValueError):
            return None, None, None
        err = parsed.get("error") if isinstance(parsed, dict) else None
        if not isinstance(err, dict):
            return None, None, None

        def field_(key: str) -> str | None:
            value = err.get(key)
            if value is None:
                return None
            return self._redact(str(value))[:300]

        return field_("id"), field_("name"), field_("detail")

    def _note_rate_limit(self, header: str | None) -> None:
        parsed = _parse_rate_limit(header)
        if parsed is None:
            return
        self.rate_limit_used, self.rate_limit_total = parsed
        if parsed[1] and parsed[0] >= 0.9 * parsed[1]:
            log.warning("YNAB rate limit nearly exhausted: %d/%d requests used", *parsed)

    def _redact(self, text: str) -> str:
        return redact_text(text, (self._token,))

    def _knowledge(self, data: Mapping[str, Any], *, required: bool = True) -> int | None:
        value = data.get("server_knowledge")
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if required:
            raise self._shape_error("server_knowledge")
        return None

    def _data_obj(self, data: Mapping[str, Any], key: str) -> dict[str, Any]:
        value = data.get(key)
        if not isinstance(value, dict):
            raise self._shape_error(key)
        return value

    @staticmethod
    def _shape_error(key: str) -> YnabError:
        return YnabError(
            f"YNAB response is missing {key!r}",
            user_message="YNAB returned an unexpected response. Please try again later.",
        )
