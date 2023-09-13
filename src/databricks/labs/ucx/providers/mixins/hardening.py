import logging
import threading
import time
from functools import wraps

logger = logging.getLogger(__name__)


class RateLimiter:
    """Used to limit the rate of requests to a certain maximum rate within a specified burst period,
    defaulting to one second. It is nice to control and manage the flow of requests to prevent
    overloading a service.

    It uses a variation of the Leaky Bucket algorithm (https://en.wikipedia.org/wiki/Leaky_bucket)
    for its simplicity.
    """

    def __init__(self, *, max_requests: int = 30, burst_period_seconds: int = 1):
        self._capacity = max_requests
        self._burst_period_seconds = burst_period_seconds
        self._lock = threading.RLock()
        self._last = time.time()
        self._bucket = 0

    def throttle(self):
        """The throttle method is used to check and control the rate of requests. It's intended to be called
        before making a new request and is thread-safe.

        It calculates the time elapsed since the last request and subtracts this from the _burst_period_seconds
        to determine how much time remains in the current burst period. If the calculated delay is less than zero,
        it means that the burst period has elapsed, so it resets the request count and updates the last request
        timestamp. If the request count exceeds the allowed maximum (_max_requests), it sleeps for the remaining
        time in the burst period (delay) to enforce the rate limit.
        """
        with self._lock:
            now = time.time()
            delay = self._burst_period_seconds - (now - self._last)
            if delay < 0:
                # If the bucket is empty, it stops leaking.
                self._bucket = 0
                self._last = now
            self._bucket += 1
            if self._bucket > self._capacity:
                # If the bucket is over capacity, start leaking
                # by sleeping on the current thread.
                logger.debug(f"Throttled for {delay}s")
                time.sleep(delay)


def rate_limited(*, max_requests: int = 30, burst_period_seconds: int = 1):
    def decorator(func):
        rate_limiter = RateLimiter(max_requests=max_requests, burst_period_seconds=burst_period_seconds)

        @wraps(func)
        def wrapper(*args, **kwargs):
            rate_limiter.throttle()
            return func(*args, **kwargs)

        return wrapper

    return decorator
