"""Exception hierarchy shared by every layer (see docs/DESIGN.md §4.1).

``user_message`` is always safe to show to an end user (Telegram reply, web toast):
it never contains tokens, keys or stack traces.
"""

from __future__ import annotations


class YnabHelperError(Exception):
    """Base class. ``user_message`` defaults to the exception text."""

    def __init__(self, message: str = "", *, user_message: str | None = None) -> None:
        super().__init__(message)
        self.user_message = user_message if user_message is not None else message


class ConfigError(YnabHelperError):
    """Invalid or missing configuration."""


class CsvError(YnabHelperError):
    """A statement file that cannot be imported (wrong format, mixed currency, too large…)."""


class Busy(YnabHelperError):
    """Another operation for the same user is already running."""

    def __init__(self, operation: str) -> None:
        super().__init__(
            f"operation {operation!r} is already running",
            user_message=f"Another operation ({operation}) is still running. Try again in a moment.",
        )
        self.operation = operation


class StaleStateError(YnabHelperError):
    """A confirmation token, scan id or button refers to state that expired or changed."""


class ExternalServiceError(YnabHelperError):
    """A call to an external service (YNAB, Telegram, Blockstream, …) failed."""

    def __init__(
        self,
        message: str,
        *,
        service: str,
        status: int | None = None,
        retriable: bool = False,
        user_message: str | None = None,
    ) -> None:
        super().__init__(message, user_message=user_message or f"{service} request failed: {message}")
        self.service = service
        self.status = status
        self.retriable = retriable


class YnabError(ExternalServiceError):
    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        error_id: str | None = None,
        detail: str | None = None,
        retriable: bool = False,
        user_message: str | None = None,
    ) -> None:
        super().__init__(
            message,
            service="YNAB",
            status=status,
            retriable=retriable,
            user_message=user_message,
        )
        self.error_id = error_id
        self.detail = detail


class YnabAuthError(YnabError):
    """401: the personal access token is invalid or revoked."""


class YnabNotFound(YnabError):
    """404: budget, account or transaction does not exist (or was deleted)."""


class YnabRateLimited(YnabError):
    """429: YNAB's 200 requests/hour limit is exhausted."""

    def __init__(self, message: str, *, retry_after: float | None = None, **kw) -> None:
        kw.setdefault(
            "user_message",
            "YNAB rate limit reached (200 requests per hour). Please try again later.",
        )
        super().__init__(message, status=429, retriable=True, **kw)
        self.retry_after = retry_after


class YnabValidationError(YnabError):
    """400/409/422: YNAB rejected the request content."""


class YnabServerError(YnabError):
    """5xx from YNAB."""


class YnabNetworkError(YnabError):
    """Timeout, DNS failure, connection reset…"""


class TelegramError(ExternalServiceError):
    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        description: str | None = None,
        retry_after: float | None = None,
        retriable: bool = False,
    ) -> None:
        super().__init__(message, service="Telegram", status=error_code, retriable=retriable)
        self.error_code = error_code
        self.description = description
        self.retry_after = retry_after
