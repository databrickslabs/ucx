"""Temporary replacement for databricks.sdk.retries.

After evaluation this can either be removed or moved upstream.
"""

import datetime as dt
import functools
import logging
import os
import time
from collections.abc import Callable, Sequence
from random import random
from typing import ParamSpec, TypeVar

from databricks.sdk.clock import Clock

logger = logging.getLogger(__name__)


class TimerClock(Clock):
    """A clock suitable for use when measuring elapsed (wall-clock) time.

    This clock is resilient in the face of system time adjustments, with the caveat that times can only be compared
    against each other. Comparing times yields the difference between them, measured in seconds.
    """

    __slots__ = ("_clock",)

    def __init__(self) -> None:
        self._clock = time.perf_counter

    def time(self) -> float:
        """Return the current time.

        The value returned can be compared to other times returned by this method. The difference represents the
        number of seconds (potentially with a fractional component) that elapsed between them.

        :return: A value that represents the current time.
        """
        return self._clock()

    def sleep(self, seconds: float) -> None:
        """Delay execution for a given number of seconds.

        The argument may have a fractional component for sub-second precision.

        This method is blocking; it is not async-aware.

        :param seconds: The duration to wait before returning from this method.
        """
        time.sleep(seconds)


def _default_timeout_dilation() -> float:
    """Load the default timeout dilation factor.

    This factor is used to scale timeouts by a default, which can be useful in environments where tests are slower than
    expected.

    :returns: the default time dilation to use for timeouts.
    """
    dilation = float(os.environ["TIMEOUT_DILATION"]) if "TIMEOUT_DILATION" in os.environ else 1.0
    if dilation != 1.0:
        logger.info(f"Non-default timeout dilation: all timeouts will be scaled by a factor of {dilation}.")
    return dilation


P = ParamSpec("P")
T = TypeVar("T")


class _Retrier:
    __slots__ = ("_on", "_is_retryable", "_timeout", "_clock", "_min_attempts", "_max_attempts")

    def __init__(
        self,
        *,
        on: Sequence[type[BaseException]] | None,
        is_retryable: Callable[[BaseException], str | None] | None,
        timeout: dt.timedelta | None,
        clock: Clock,
        timeout_dilation: float,
        min_attempts: int,
        max_attempts: int | None = None,
    ) -> None:
        has_allowlist = on is not None
        has_callback = is_retryable is not None
        if has_callback == has_allowlist:
            raise SyntaxError('either on=[Exception] or callback=lambda x: .. is required')
        if timeout is None and max_attempts is None:
            raise SyntaxError('either timeout=... or max_attempts=... is required')
        if max_attempts is not None and max_attempts < min_attempts:
            msg = f'max_attempts=x must be equal or higher to min_attempts ({min_attempts})'
            raise SyntaxError(msg)

        self._on = on
        self._is_retryable = is_retryable

        dilated_timeout = timeout * timeout_dilation if timeout is not None else None
        self._timeout = dilated_timeout
        self._clock = clock
        self._min_attempts = min_attempts
        self._max_attempts = max_attempts

    def _can_retry(self, err: BaseException) -> str | None:
        if self._on is not None:
            return self._subclass_checker(err)
        if self._is_retryable is not None:
            return self._is_retryable(err)
        # Unreachable
        return None

    def _subclass_checker(self, err: BaseException) -> str | None:
        assert self._on is not None
        for candidate in self._on:
            if isinstance(err, candidate):
                return f'{type(err).__name__} is allowed to retry'
        return None

    def _deadline(self) -> float | None:
        timeout = self._timeout
        return self._clock.time() + timeout.total_seconds() if timeout is not None else None

    def _check_attempt_duration(self, func: Callable[P, T], attempt_duration: dt.timedelta) -> None:
        timeout = self._timeout
        if timeout is not None:
            if timeout <= attempt_duration:
                logger.error(
                    f"Retryable call ({func.__name__}) attempt took {attempt_duration}, longer than retry timeout ({timeout}). Adjust timeout accordingly!"
                )
            elif (timeout * 0.80) <= attempt_duration:
                logger.warning(
                    f"Retryable call ({func.__name__}) attempt took {attempt_duration}, within 20% of the specified timeout ({timeout}); please adjust timeout accordingly."
                )

    def _check_retries_exhausted(self, attempt: int, deadline: float | None, pending_sleep: float) -> str | None:
        if self._min_attempts < attempt:
            max_attempts = self._max_attempts
            if max_attempts is not None and max_attempts < attempt:
                return f"Timed out after exceeding maximum number of attempts ({self._max_attempts})"
            if deadline is not None:
                now = self._clock.time()
                if deadline < now:
                    return f"Timed out after {self._timeout}"
                if (deadline - pending_sleep) < now:
                    return f"Will time out after {self._timeout}"
        return None

    def decorator(self, func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            deadline = self._deadline()
            attempt = 1
            while True:
                started_attempt_at = self._clock.time()
                try:
                    result = func(*args, **kwargs)
                    completed_attempt_at = self._clock.time()
                    attempt_duration = dt.timedelta(seconds=completed_attempt_at - started_attempt_at)
                    self._check_attempt_duration(func, attempt_duration)
                    return result
                except Exception as err:  # pylint: disable=broad-exception-caught
                    last_err = err
                    # sleep 10s max per attempt, unless it's HTTP 429 or 503
                    retry_after_secs = getattr(err, 'retry_after_secs', None)
                    if retry_after_secs is not None:
                        # cannot depend on DatabricksError directly because of circular dependency
                        retry_reason = 'throttled by platform'
                        sleep = retry_after_secs
                    else:
                        maybe_retry_reason = self._can_retry(err)

                        if maybe_retry_reason is None:
                            # raise if exception is not retryable
                            raise err
                        retry_reason = maybe_retry_reason
                        sleep = min(10, attempt)

                attempt += 1
                logger.debug(f'Retrying (attempt={attempt}): {retry_reason} (sleeping ~{sleep}s)')

                # Save some time upon failure by failing early if the deadline will be exceeded after sleeping.
                sleep_duration = sleep + random()

                msg = self._check_retries_exhausted(attempt, deadline, sleep_duration)
                if msg is not None:
                    raise TimeoutError(msg) from last_err

                self._clock.sleep(sleep_duration)

        return wrapper


def retried(
    *,
    on: Sequence[type[BaseException]] | None = None,
    is_retryable: Callable[[BaseException], str | None] | None = None,
    timeout: dt.timedelta | None = dt.timedelta(minutes=20),
    clock: Clock = TimerClock(),
    timeout_dilation: float = _default_timeout_dilation(),
    min_attempts: int = 2,
    max_attempts: int | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    # TODO: Docstring
    retrier = _Retrier(
        on=on,
        is_retryable=is_retryable,
        timeout=timeout,
        clock=clock,
        timeout_dilation=timeout_dilation,
        min_attempts=min_attempts,
        max_attempts=max_attempts,
    )
    return retrier.decorator
