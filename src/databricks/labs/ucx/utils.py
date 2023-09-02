import concurrent
import datetime as dt
import logging
from collections.abc import Callable
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor
from typing import Generic, TypeVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import AclItem

from databricks.labs.ucx.generic import StrEnum

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
        executables: list[ExecutableFunction],
        num_threads: int | None = 4,
        progress_reporter: ProgressReporter | None = None,
    ):
        self._num_threads = num_threads
        self._executables = executables
        self._futures = []
        _reporter = ProgressReporter(len(executables)) if not progress_reporter else progress_reporter
        self._done_callback = _reporter.progress_report

    @classmethod
    def gather(cls, name: str, tasks: list[ExecutableFunction]) -> list[ExecutableResult]:
        reporter = ProgressReporter(len(tasks), f"{name}: ")
        return cls(tasks, num_threads=4, progress_reporter=reporter).run()

    def run(self) -> list[ExecutableResult]:
        logger.debug(f"Starting {len(self._executables)} tasks in {self._num_threads} threads")

        with ThreadPoolExecutor(self._num_threads) as executor:
            for executable in self._executables:
                future = executor.submit(executable)
                if self._done_callback:
                    future.add_done_callback(self._done_callback)
                # TODO: errors are not handled yet - https://github.com/databricks/UC-Upgrade/issues/89
                self._futures.append(future)

            results = concurrent.futures.wait(self._futures, return_when=ALL_COMPLETED)

        logger.debug("Collecting the results from threaded execution")
        collected = [future.result() for future in results.done]
        return collected


class Request:
    def __init__(self, req: dict):
        self.request = req

    def as_dict(self) -> dict:
        return self.request


class WorkspaceLevelEntitlement(StrEnum):
    WORKSPACE_ACCESS = "workspace-access"
    DATABRICKS_SQL_ACCESS = "databricks-sql-access"
    ALLOW_CLUSTER_CREATE = "allow-cluster-create"
    ALLOW_INSTANCE_POOL_CREATE = "allow-instance-pool-create"


def safe_get_scope_acls_for_principal(ws: WorkspaceClient, scope_name: str, group_name: str) -> AclItem | None:
    all_acls = ws.secrets.list_acls(scope=scope_name)
    for acl in all_acls:
        if acl.principal == group_name:
            return acl
