class JobCreationError(Exception):
    """Raised when a job and its outbox event cannot be created."""


class IdempotencyConflictError(Exception):
    """Raised when an idempotency key is reused with different parameters."""


class JobRetryNotFoundError(Exception):
    """Raised when a failed job cannot be retried for the requesting client."""


class JobRetryConflictError(Exception):
    """Raised when a job exists but is not eligible for manual retry."""


class JobRetryError(Exception):
    """Raised when manually retrying a job cannot be committed."""
