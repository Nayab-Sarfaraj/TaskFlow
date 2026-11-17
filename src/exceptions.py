class JobCreationError(Exception):
    """Raised when a job and its outbox event cannot be created."""


class QueueCapacityExceeded(Exception):
    """Raised when the system-wide pending-job capacity is exhausted."""

    retry_after_seconds = 10


class IdempotencyConflictError(Exception):
    """Raised when an idempotency key is reused with different parameters."""


class JobRetryNotFoundError(Exception):
    """Raised when a failed job cannot be retried for the requesting client."""


class JobRetryConflictError(Exception):
    """Raised when a job exists but is not eligible for manual retry."""


class JobRetryError(Exception):
    """Raised when manually retrying a job cannot be committed."""

class ClientRegisterationError(Exception):
    pass


class APIKeyNotFoundError(Exception):
    """Raised when an API key is not found."""
    pass


KeyNotFoundError = APIKeyNotFoundError
