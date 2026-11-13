import httpx
import pytest
from pydantic import BaseModel, ValidationError

from src.config.retry import RetryPolicy
from src.models.jobs import JobType
from src.utils.failure_classifier import is_retryable


class InputModel(BaseModel):
    required: int


@pytest.mark.parametrize(
    "exception",
    [TimeoutError("timed out"), ConnectionError("connection lost")],
)
def test_transport_errors_are_retryable(exception):
    assert is_retryable(exception) is True


def test_http_503_is_retryable():
    error = httpx.HTTPStatusError(
        "server unavailable",
        request=httpx.Request("GET", "https://example.test"),
        response=httpx.Response(503),
    )

    assert is_retryable(error) is True


def test_http_400_is_permanent():
    error = httpx.HTTPStatusError(
        "bad request",
        request=httpx.Request("GET", "https://example.test"),
        response=httpx.Response(400),
    )

    assert is_retryable(error) is False


def test_validation_error_is_permanent():
    with pytest.raises(ValidationError) as captured:
        InputModel(required="not-an-integer")

    assert is_retryable(captured.value) is False


def test_unknown_exception_is_permanent():
    assert is_retryable(RuntimeError("unexpected failure")) is False


def test_invalid_retry_configuration_is_rejected():
    with pytest.raises(ValueError, match="valid JobType"):
        RetryPolicy("not-a-job-type")


def test_missing_job_specific_configuration_uses_default_policy():
    policy = RetryPolicy(None)

    assert policy.max_attempts == 4
    assert policy.base_delay == 5
    assert policy.max_delay == 60
