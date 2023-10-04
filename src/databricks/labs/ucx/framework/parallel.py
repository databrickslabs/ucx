import concurrent
import datetime as dt
import functools
import logging
from collections.abc import Callable
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor
from typing import Generic, TypeVar

ExecutableResult = TypeVar("ExecutableResult")
ExecutableFunction = Callable[..., ExecutableResult]
logger = logging.getLogger(__name__)


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
    def __init__(
        self,
        name,
        executables: list[ExecutableFunction],
        num_threads: int | None = 4,
        progress_reporter: ProgressReporter | None = None,
    ):
        self._name = name
        self._num_threads = num_threads
        self._executables = executables
        self._futures = []
        _reporter = ProgressReporter(len(executables)) if not progress_reporter else progress_reporter
        self._done_callback: Callable = _reporter.progress_report

    @classmethod
    def gather(cls, name: str, tasks: list[ExecutableFunction]) -> list[ExecutableResult]:
        reporter = ProgressReporter(len(tasks), f"{name}: ")
        return cls(name, tasks, num_threads=4, progress_reporter=reporter).run()

    def run(self) -> list[ExecutableResult]:
        logger.debug(f"Starting {len(self._executables)} tasks in {self._num_threads} threads")

        def error_handler(func, name):
            @functools.wraps(func)
            def inner(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    msg = getattr(ex, "message", ex)
                    logger.warning(f"An error occurred while '{name}', message: {msg}")
                    return None

            return inner

        with ThreadPoolExecutor(self._num_threads) as executor:
            for executable in self._executables:
                future = executor.submit(error_handler(executable, self._name))
                future.add_done_callback(self._done_callback)
                self._futures.append(future)

            results = concurrent.futures.wait(self._futures, return_when=ALL_COMPLETED)

        logger.debug("Collecting the results from threaded execution")
        collected = [future.result() for future in results.done if future.result()]
        failed_count = len(self._executables) - len(collected)
        if failed_count > 0:
            logger.warning(f"The parallel run of '{self._name}' incurred {failed_count} failed tasks")
        return collected


def noop():
    pass
