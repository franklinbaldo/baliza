"""
Circuit breaker implementation for PNCP API resilience
"""

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Callable, Any
from enum import Enum
import logging
import threading

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: int = 300  # 5 minutes
    success_threshold: int = 2  # For half-open state
    timeout: int = 60  # Request timeout


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open"""

    pass


from typing import List


class CircuitBreaker:
    """Circuit breaker with adaptive behavior for PNCP API"""

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.last_success_time: Optional[datetime] = None
        self._lock = threading.Lock()

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""

        with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    logger.info("Circuit breaker transitioning to HALF_OPEN")
                else:
                    raise CircuitBreakerError(
                        f"Circuit breaker is OPEN. Last failure: {self.last_failure_time}"
                    )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result

        except Exception as e:
            self._on_failure(e)
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if circuit breaker should attempt to reset"""
        if not self.last_failure_time:
            return True

        time_since_failure = datetime.now() - self.last_failure_time
        return time_since_failure.total_seconds() >= self.config.recovery_timeout

    def _on_success(self):
        """Handle successful call"""
        with self._lock:
            self.last_success_time = datetime.now()

            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    logger.info(
                        "Circuit breaker reset to CLOSED after successful recovery"
                    )
            elif self.state == CircuitState.CLOSED:
                # Reset failure count on success
                self.failure_count = max(0, self.failure_count - 1)

    def _on_failure(self, exception: Exception):
        """Handle failed call"""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now()
            self.success_count = 0  # Reset success count

            if self.failure_count >= self.config.failure_threshold:
                if self.state != CircuitState.OPEN:
                    self.state = CircuitState.OPEN
                    logger.warning(
                        f"Circuit breaker OPENED after {self.failure_count} failures. "
                        f"Last error: {exception}"
                    )


class AdaptiveRateLimiter:
    """Rate limiter that adapts based on server responses"""

    def __init__(self, requests_per_minute: int = 120):
        self.requests_per_minute = requests_per_minute
        self.request_times: List[datetime] = []
        self.adaptive_factor = 1.0  # 1.0 = normal, < 1.0 = reduced rate
        self.last_adaptive_check = datetime.now()

    async def acquire(self):
        """Acquire permission to make a request"""
        now = datetime.now()

        # Clean old request times (older than 1 minute)
        cutoff = now - timedelta(minutes=1)
        self.request_times = [t for t in self.request_times if t > cutoff]

        # Calculate current rate limit with adaptive factor
        current_limit = int(self.requests_per_minute * self.adaptive_factor)

        if len(self.request_times) >= current_limit:
            # Need to wait
            oldest_request = min(self.request_times)
            wait_time = 60 - (now - oldest_request).total_seconds()

            if wait_time > 0:
                await self._sleep(wait_time)

        self.request_times.append(datetime.now())

    async def _sleep(self, seconds: float):
        """Sleep function - can be overridden for testing"""
        import asyncio

        await asyncio.sleep(seconds)

    def adapt_rate(self, response_time: float, status_code: int):
        """Adapt rate based on server response"""
        if status_code >= 500 or response_time > 10.0 or status_code == 429:
            self.adaptive_factor = 0.8
        elif status_code == 200 and response_time < 2.0:
            self.adaptive_factor = 1.0


class RetryConfig:
    """Configuration for exponential backoff retry"""

    def __init__(
        self,
        max_attempts: int = 3,
        backoff_factor: float = 2.0,
        backoff_max: int = 300,
        jitter: bool = True,
    ):
        self.max_attempts = max_attempts
        self.backoff_factor = backoff_factor
        self.backoff_max = backoff_max
        self.jitter = jitter


async def retry_with_backoff(
    func: Callable,
    config: RetryConfig,
    retryable_exceptions: tuple = (Exception,),
    *args,
    **kwargs,
):
    """Execute function with exponential backoff retry"""

    last_exception: Optional[Exception] = None

    for attempt in range(config.max_attempts):
        try:
            return await func(*args, **kwargs)

        except retryable_exceptions as e:
            last_exception = e

            if attempt == config.max_attempts - 1:
                # Last attempt failed
                break

            # Calculate backoff delay
            delay = min(config.backoff_factor**attempt, config.backoff_max)

            # Add jitter to avoid thundering herd
            if config.jitter:
                delay *= 0.5 + random.random() * 0.5

            logger.warning(
                f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f} seconds..."
            )

            import asyncio

            await asyncio.sleep(delay)

    # All attempts failed
    if last_exception:
        raise last_exception
    raise RuntimeError("Retry failed without an exception")
