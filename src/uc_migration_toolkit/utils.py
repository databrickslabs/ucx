import concurrent
import datetime as dt
from collections.abc import Callable
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor
from typing import Generic, TypeVar

from ratelimit import limits, sleep_and_retry

from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger

ExecutableResult = TypeVar("ExecutableResult")
ExecutableFunction = Callable[..., ExecutableResult]


class ProgressReporter:
    def __init__(self, total_executables: int, message_prefix: str | None = "threaded execution - processed: "):
        self.counter = 0
        self._total_executables = total_executables
        self.start_time = dt.datetime.now()
        self._message_prefix = message_prefix

    def progress_report(self, _):
        self.counter += 1
        measuring_time = dt.datetime.now()
        delta_from_start = measuring_time - self.start_time
        rps = self.counter / delta_from_start.total_seconds()
        offset = len(str(self._total_executables))
        if self.counter % 10 == 0 or self.counter == self._total_executables:
            logger.info(
                f"{self._message_prefix}{self.counter:>{offset}d}/{self._total_executables}, rps: {rps:.3f}/sec"
            )


class ThreadedExecution(Generic[ExecutableResult]):
    def __init__(self, executables: list[ExecutableFunction], done_callback: Callable[..., None] | None = None):
        self._executables = executables
        self._futures = []
        self._done_callback = (
            done_callback if done_callback else self._prepare_default_done_callback(len(self._executables))
        )

    @staticmethod
    def _prepare_default_done_callback(total_executables: int):
        progress_reporter = ProgressReporter(total_executables)
        return progress_reporter.progress_report

    def run(self) -> list[ExecutableResult]:
        logger.info("Starting threaded execution")

        @sleep_and_retry
        @limits(
            calls=config_provider.config.rate_limit.max_requests_per_period,
            period=config_provider.config.rate_limit.period_in_seconds,
        )
        def rate_limited_wrapper(func: ExecutableFunction) -> ExecutableResult:
            return func()

        with ThreadPoolExecutor(config_provider.config.num_threads) as executor:
            for executable in self._executables:
                future = executor.submit(rate_limited_wrapper, executable)
                if self._done_callback:
                    future.add_done_callback(self._done_callback)
                self._futures.append(future)

            results = concurrent.futures.wait(self._futures, return_when=ALL_COMPLETED)

        logger.info("Collecting the results from threaded execution")
        collected = [future.result() for future in results.done]
        return collected
