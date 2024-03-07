import datetime as dt
import logging
from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from itertools import groupby

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InternalError, NotFound, ResourceDoesNotExist
from databricks.sdk.retries import retried
from databricks.sdk.service.workspace import ObjectInfo, ObjectType

logger = logging.getLogger(__name__)


class WorkspaceListing:
    def __init__(
        self,
        ws: WorkspaceClient,
        num_threads: int,
        verify_timeout: dt.timedelta | None = dt.timedelta(minutes=1),
        *,
        with_directories: bool = True,
    ):
        self.start_time = None
        self._ws = ws
        self.results: list[ObjectInfo] = []
        self._num_threads = num_threads
        self._with_directories = with_directories
        self._counter = 0
        self._verify_timeout = verify_timeout

    def _progress_report(self, _):
        self._counter += 1
        measuring_time = dt.datetime.now()
        delta_from_start = measuring_time - self.start_time
        rps = self._counter / delta_from_start.total_seconds()
        directory_count = len([r for r in self.results if r.object_type == ObjectType.DIRECTORY])
        other_count = len([r for r in self.results if r.object_type != ObjectType.DIRECTORY])
        if self._counter % 10 == 0:
            logger.info(
                f"Made {self._counter} workspace listing calls, "
                f"collected {len(self.results)} objects ({directory_count} dirs and {other_count} other objects),"
                f" rps: {rps:.3f}/sec"
            )

    def _list_workspace(self, path: str) -> Iterable[ObjectInfo]:
        list_retry_on_value_error = retried(on=[InternalError], timeout=self._verify_timeout)
        list_retried_check = list_retry_on_value_error(self._ws.workspace.list)
        return list_retried_check(path=path, recursive=False)

    def _list_and_analyze(self, obj: ObjectInfo) -> tuple[list[ObjectInfo], list[ObjectInfo]]:
        directories = []
        others = []
        if not obj.path:
            return [], []
        try:
            list_workspace = self._list_workspace(obj.path)

            def is_dir(obj: ObjectInfo) -> bool:
                return obj.object_type == ObjectType.DIRECTORY

            grouped_iterator: Iterable[tuple[bool, Iterable[ObjectInfo]]] = groupby(list_workspace, key=is_dir)
            for is_directory, objects in grouped_iterator:
                if is_directory:
                    directories.extend(list(objects))
                else:
                    others.extend(list(objects))
            logger.debug(f"Listed {obj.path}, found {len(directories)} sub-directories and {len(others)} other objects")
        except NotFound:
            # See https://github.com/databrickslabs/ucx/issues/230
            logger.warning(f"{obj.path} is not listable. Ignoring")
        except TimeoutError as e:
            logger.warning(f"Could not list {obj.path} due to backend error: {e}")
        return directories, others

    def walk(self, start_path="/"):
        self.start_time = dt.datetime.now()
        logger.info(f"Recursive WorkspaceFS listing started at {self.start_time}")
        try:
            root_object = self._ws.workspace.get_status(start_path)
        except ResourceDoesNotExist:
            logger.warning(f"removed on the backend {start_path}")
            return self.results
        self.results.append(root_object)
        with ThreadPoolExecutor(self._num_threads) as executor:
            initial_future = executor.submit(self._list_and_analyze, root_object)
            initial_future.add_done_callback(self._progress_report)
            futures_to_objects = {initial_future: root_object}
            while futures_to_objects:
                futures_done, _ = wait(futures_to_objects, return_when=FIRST_COMPLETED)
                for future in futures_done:
                    futures_to_objects.pop(future)
                    directories, others = future.result()
                    self.results.extend(directories)
                    self.results.extend(others)
                    if not directories:
                        continue
                    new_futures = {}
                    for directory in directories:
                        new_future = executor.submit(self._list_and_analyze, directory)
                        new_future.add_done_callback(self._progress_report)
                        new_futures[new_future] = directory
                    futures_to_objects.update(new_futures)
            logger.info(
                f"Recursive WorkspaceFS listing finished at {dt.datetime.now()}. "
                f"Total time taken for workspace listing: {dt.datetime.now() - self.start_time}"
            )
            self._progress_report(None)
        return self.results
