from pydantic import ValidationError


class RetryableJobError(Exception):
    pass


class PermanentJobError(Exception):
    pass


def is_retryable(exception: Exception) -> bool:
    if isinstance(exception, RetryableJobError):
        return True
    if isinstance(exception, PermanentJobError):
        return False
    if isinstance(exception, TimeoutError):
        return True
    if isinstance(exception, ConnectionError):
        return True
    if isinstance(exception, ValidationError):
        return False

    status_code = getattr(getattr(exception, "response", None), "status_code", None)
    if status_code is None:
        status_code = getattr(exception, "status_code", None)
    return status_code in {408, 429, 500, 502, 503, 504}
