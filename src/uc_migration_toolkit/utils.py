import concurrent
import datetime as dt
import enum
import json
from collections.abc import Callable
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor
from typing import Generic, TypeVar

from ratelimit import limits, sleep_and_retry

from uc_migration_toolkit.config import RateLimitConfig
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
    def __init__(
        self,
        executables: list[ExecutableFunction],
        num_threads: int | None = None,
        rate_limit: RateLimitConfig | None = None,
        done_callback: Callable[..., None] | None = None,
    ):
        self._num_threads = num_threads if num_threads else config_provider.config.num_threads
        self._rate_limit = rate_limit if rate_limit else config_provider.config.rate_limit
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
        logger.trace("Starting threaded execution")

        @sleep_and_retry
        @limits(calls=self._rate_limit.max_requests_per_period, period=self._rate_limit.period_in_seconds)
        def rate_limited_wrapper(func: ExecutableFunction) -> ExecutableResult:
            return func()

        with ThreadPoolExecutor(self._num_threads) as executor:
            for executable in self._executables:
                future = executor.submit(rate_limited_wrapper, executable)
                if self._done_callback:
                    future.add_done_callback(self._done_callback)
                self._futures.append(future)

            results = concurrent.futures.wait(self._futures, return_when=ALL_COMPLETED)

        logger.trace("Collecting the results from threaded execution")
        collected = [future.result() for future in results.done]
        return collected


class Request:
    def __init__(self, req: dict):
        self.request = req

    def as_dict(self) -> dict:
        return self.request


class StrEnum(str, enum.Enum):  # re-exported for compatability with older python versions
    def __new__(cls, value, *args, **kwargs):
        if not isinstance(value, str | enum.auto):
            msg = f"Values of StrEnums must be strings: {value!r} is a {type(value)}"
            raise TypeError(msg)
        return super().__new__(cls, value, *args, **kwargs)

    def __str__(self):
        return str(self.value)

    def _generate_next_value_(name, *_):  # noqa: N805
        return name


class WorkspaceLevelEntitlement(StrEnum):
    WORKSPACE_ACCESS = "workspace-access"
    DATABRICKS_SQL_ACCESS = "databricks-sql-access"
    ALLOW_CLUSTER_CREATE = "allow-cluster-create"
    ALLOW_INSTANCE_POOL_CREATE = "allow-instance-pool-create"


# TODO: using this because SDK doesn't know how to properly write enums, highlight this to the SDK team
class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, enum.Enum):
            return obj.name
        return json.JSONEncoder.default(self, obj)
