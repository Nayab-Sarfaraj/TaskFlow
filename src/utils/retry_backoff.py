from datetime import datetime, timedelta, timezone
import random

from src.config.retry import RetryPolicy


def get_delay(attempt_number, retry_policy: RetryPolicy) -> float:
    if attempt_number < 1:
        raise ValueError("attempt number cannot be less than 1")

    capped_delay = min(
        retry_policy.max_delay,
        retry_policy.base_delay * (2 ** (attempt_number - 1)),
    )
    return random.uniform(0, capped_delay)


def calculate_next_execution_time(delay: float) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=delay)


def should_retry(attempt_number, retry_policy: RetryPolicy) -> bool:
    if attempt_number < 1:
        raise ValueError("attempt number cannot be less than 1")
    return attempt_number < retry_policy.max_attempts
