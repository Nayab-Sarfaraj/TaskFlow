class JobCreationError(Exception):
    """Raised when a job and its outbox event cannot be created."""


class IdempotencyConflictError(Exception):
    """Raised when an idempotency key is reused with different parameters."""
