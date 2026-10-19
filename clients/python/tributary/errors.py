"""Exceptions a handler raises to tell Tributary how a job failed."""


class TributaryError(Exception):
    """An API call failed."""

    def __init__(self, status, message):
        super().__init__(f"{status}: {message}")
        self.status = status
        self.message = message


class Throttled(Exception):
    """The upstream provider is rate limiting. Retry after a pause; does not use an attempt.

    scope is "message" (default), "key", "subscription" or "pool".
    """

    def __init__(self, retry_after=1.0, scope="message"):
        super().__init__(f"throttled, retry after {retry_after}s")
        self.retry_after = retry_after
        self.scope = scope


class QuotaExhausted(Exception):
    """A spend cap or hard quota was hit. Pauses the scope (default: the pool) until resumed,
    or for retry_after seconds if given. Does not use an attempt."""

    def __init__(self, retry_after=None, scope="pool"):
        super().__init__("quota exhausted")
        self.retry_after = retry_after
        self.scope = scope


class Permanent(Exception):
    """The job can never succeed. It goes straight to the dead-letter topic."""

    def __init__(self, reason="permanent failure"):
        super().__init__(reason)
        self.reason = reason
