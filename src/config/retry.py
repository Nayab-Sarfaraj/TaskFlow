from dataclasses import dataclass, field
from typing import Optional
from src.models.jobs import JobType


@dataclass(frozen=True)
class RetryPolicy:

    max_attempts: int = field(init=False)
    base_delay: int = field(init=False)
    max_delay: int = field(init=False)
    job_type: Optional[JobType]

    def __post_init__(self):
        if self.job_type is not None and not isinstance(self.job_type, JobType):
            raise ValueError("job_type must be a valid JobType or None")

        if self.job_type == JobType.EMAIL:
            max_attempts = 5
            base_delay = 10
            max_delay = 120
        elif self.job_type == JobType.REPORT:
            max_attempts = 4
            base_delay = 5
            max_delay = 60
        elif self.job_type == JobType.SCRAPING:
            max_attempts = 6
            base_delay = 15
            max_delay = 180
        elif self.job_type == JobType.VIDEO:
            max_attempts = 3
            base_delay = 30
            max_delay = 300
        else:
            max_attempts = 4
            base_delay = 5
            max_delay = 60

        object.__setattr__(self, "base_delay", base_delay)
        object.__setattr__(self, "max_delay", max_delay)
        object.__setattr__(self, "max_attempts", max_attempts)

        if self.max_attempts < 1:
            raise ValueError("max_attempt must be greater than equal to  1")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be positive")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")
