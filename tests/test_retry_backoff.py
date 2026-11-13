import pytest
from datetime import datetime, timedelta, timezone

from src.config.retry import RetryPolicy
from src.models.jobs import JobType
from src.utils.retry_backoff import calculate_next_execution_time
from src.utils.retry_backoff import get_delay
from src.utils.retry_backoff import should_retry


def test_calculate_next_execution_time_returns_utc_future_timestamp():
    before = datetime.now(timezone.utc)
    scheduled_at = calculate_next_execution_time(30)
    after = datetime.now(timezone.utc)

    assert scheduled_at.tzinfo == timezone.utc
    assert scheduled_at >= before
    assert scheduled_at <= after + timedelta(seconds=30)
    assert timedelta(seconds=29) <= scheduled_at - before <= timedelta(seconds=31)


def test_get_delay_returns_random_value_within_capped_backoff(monkeypatch):
    calls = []

    def fake_uniform(lower, upper):
        calls.append((lower, upper))
        return upper / 2

    monkeypatch.setattr("src.utils.retry_backoff.random.uniform", fake_uniform)
    policy = RetryPolicy(JobType.EMAIL)

    delay = get_delay(attempt_number=5, retry_policy=policy)

    assert delay == 60
    assert calls == [(0, 120)]


@pytest.mark.parametrize(
    ("attempt_number", "expected_cap"),
    ((1, 10), (2, 20), (3, 40)),
)
def test_get_delay_uses_exponential_backoff_caps(
    monkeypatch, attempt_number, expected_cap
):
    calls = []

    def fake_uniform(lower, upper):
        calls.append((lower, upper))
        return upper

    monkeypatch.setattr("src.utils.retry_backoff.random.uniform", fake_uniform)
    policy = RetryPolicy(JobType.EMAIL)

    assert get_delay(attempt_number, policy) == expected_cap
    assert calls == [(0, expected_cap)]


def test_get_delay_never_exceeds_max_delay(monkeypatch):
    calls = []

    def fake_uniform(lower, upper):
        calls.append((lower, upper))
        return upper

    monkeypatch.setattr("src.utils.retry_backoff.random.uniform", fake_uniform)
    policy = RetryPolicy(JobType.REPORT)

    delay = get_delay(attempt_number=20, retry_policy=policy)

    assert delay == policy.max_delay
    assert calls == [(0, policy.max_delay)]


def test_get_delay_jitter_stays_between_zero_and_cap():
    policy = RetryPolicy(JobType.EMAIL)
    cap = policy.base_delay * 4

    delays = [get_delay(attempt_number=3, retry_policy=policy) for _ in range(100)]

    assert all(0 <= delay <= cap for delay in delays)


@pytest.mark.parametrize("attempt_number", [0, -1])
def test_invalid_attempt_numbers_are_rejected(attempt_number):
    policy = RetryPolicy(JobType.EMAIL)

    with pytest.raises(ValueError):
        get_delay(attempt_number, policy)
    with pytest.raises(ValueError):
        should_retry(attempt_number, policy)


def test_exhausted_attempts_are_not_eligible_for_retry():
    policy = RetryPolicy(JobType.EMAIL)

    assert should_retry(policy.max_attempts - 1, policy) is True
    assert should_retry(policy.max_attempts, policy) is False
    assert should_retry(policy.max_attempts + 1, policy) is False
