import datetime as dt
from collections.abc import Iterator
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait

from databricks.sdk.service.ml import ModelDatabricks
from databricks.sdk.service.workspace import ObjectType
from ratelimit import limits, sleep_and_retry

from uc_migration_toolkit.config import RateLimitConfig
from uc_migration_toolkit.providers.client import ImprovedWorkspaceClient, provider
from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger


class CustomListing:
    """
    Provides utility functions for custom listing operations
    """

    @staticmethod
    def list_models() -> Iterator[ModelDatabricks]:
        for model in provider.ws.model_registry.list_models():
            model_with_id = provider.ws.model_registry.get_model(model.name).registered_model_databricks
            yield model_with_id


class WorkspaceListing:
    def __init__(
        self,
        ws: ImprovedWorkspaceClient,
        num_threads: int,
        *,
        with_directories: bool = True,
        rate_limit: RateLimitConfig | None = None,
    ):
        self.start_time = None
        self._ws = ws
        self.results = []
        self._num_threads = num_threads
        self._with_directories = with_directories
        self._counter = 0
        self._rate_limit = rate_limit if rate_limit else config_provider.config.rate_limit

        @sleep_and_retry
        @limits(calls=self._rate_limit.max_requests_per_period, period=self._rate_limit.period_in_seconds)
        def _rate_limited_listing(path: str) -> Iterator[ObjectType]:
            return self._ws.workspace.list(path=path, recursive=False)

        self._rate_limited_listing = _rate_limited_listing

    def _progress_report(self, _):
        self._counter += 1
        measuring_time = dt.datetime.now()
        delta_from_start = measuring_time - self.start_time
        rps = self._counter / delta_from_start.total_seconds()
        if self._counter % 10 == 0:
            logger.info(
                f"Made {self._counter} workspace listing calls, "
                f"collected {len(self.results)} objects, rps: {rps:.3f}/sec"
            )

    def _walk(self, _path: str):
        futures = []
        with ThreadPoolExecutor(self._num_threads) as executor:
            for _obj in self._rate_limited_listing(_path):
                if _obj.object_type == ObjectType.DIRECTORY:
                    if self._with_directories:
                        self.results.append(_obj)
                    future = executor.submit(self._walk, _obj.path)
                    future.add_done_callback(self._progress_report)
                    futures.append(future)
                else:
                    self.results.append(_obj)
            wait(futures, return_when=ALL_COMPLETED)

    def walk(self, path: str):
        self.start_time = dt.datetime.now()
        self._walk(path)
        self._progress_report(None)  # report the final progress
