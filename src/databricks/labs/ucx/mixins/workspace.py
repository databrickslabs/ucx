import logging
import os
from collections.abc import Iterable, Iterator
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Queue
from threading import Condition

from databricks.sdk.core import DatabricksError
from databricks.sdk.service.workspace import ObjectInfo, ObjectType

_LOG = logging.getLogger(__name__)


def workspace_list(
    parent_list,
    path: str,
    *,
    notebooks_modified_after: int | None = None,
    yield_folders: bool | None = False,
    threads: int | None = None,
    max_depth: int | None = None,
) -> Iterator[ObjectInfo]:
    if not threads:
        threads = os.cpu_count()
    yield from _ParallelRecursiveListing(path, parent_list, threads, yield_folders, notebooks_modified_after, max_depth)


# staged for SDK in https://github.com/databricks/databricks-sdk-py/pull/284
class _ParallelRecursiveListing(Iterable[ObjectInfo]):

    def __init__(self, path, listing, threads, yield_folders, notebooks_modified_after, max_depth):
        self._path = path
        self._listing = listing
        self._threads = threads
        self._max_depth = max_depth
        self._yield_folders = yield_folders
        self._notebooks_modified_after = notebooks_modified_after
        self._scans = 0
        self._in_progress = 0
        # work queue is seeded from main thread and written/consumed from worker threads
        self._work = Queue()
        # results queue is written from worker threads and consumed from the main thread
        self._results = Queue()
        # running condition guards self._in_progress, read from main/worker/reporter threads
        # and modified by worker threads
        self._cond = Condition()
        self._reporter_interval = 60
        self._reporter_cond = Condition()

    def _enter_folder(self, path: str):
        with self._cond:
            _LOG.debug(f"Entering folder: {path}")
            self._in_progress += 1
        self._work.put_nowait(path)

    def _leave_folder(self, path: str):
        self._work.task_done()
        with self._cond:
            self._in_progress -= 1
            self._scans += 1
            _LOG.debug(f"Leaving folder: {path}")
            if self._in_progress > 0:
                return
            self._cond.notify_all()
            _LOG.debug("Sending poison pills to other workers")
            for _ in range(0, self._threads - 1):
                self._work.put_nowait(None)
        with self._reporter_cond:
            self._reporter_cond.notify()

    def _is_running(self):
        with self._cond:
            return self._in_progress > 0

    def _reporter(self):
        _LOG.debug("Starting workspace listing reporter")
        while self._is_running():
            with self._reporter_cond:
                self._reporter_cond.wait(self._reporter_interval)
            scans = self._scans
            took = datetime.now() - self._started
            rps = int(scans / took.total_seconds())
            _LOG.info(f"Scanned {scans} workspace folders at {rps}rps")
        _LOG.debug("Finished workspace listing reporter")

    def _worker(self, num):
        _LOG.debug(f"Starting workspace listing worker {num}")
        while self._is_running():
            path = self._work.get()
            if path is None:
                self._work.task_done()
                break # poison pill
            try:
                self._list(path)
            except DatabricksError as err:
                # See https://github.com/databrickslabs/ucx/issues/230
                if err.error_code != "RESOURCE_DOES_NOT_EXIST":
                    with self._cond:
                        self._in_progress = 0
                        self._results.put(err)
                    break
                _LOG.warning(f"{path} is not listable. Ignoring")
            except Exception as err:
                self._results.put(None)
                with self._cond:
                    self._in_progress = 0
                    raise err
                return
            finally:
                self._leave_folder(path)
        _LOG.debug(f"Finished workspace listing worker {num}")

    def _list(self, path):
        listing = self._listing(path, notebooks_modified_after=self._notebooks_modified_after)
        for object_info in sorted(listing, key=lambda _: _.path):
            if object_info.object_type != ObjectType.DIRECTORY:
                if self._max_depth is not None and len(object_info.path.split('/')) > self._max_depth:
                    msg = f"Folder is too deep (max depth {self._max_depth}): {object_info.path}. Skipping"
                    _LOG.warning(msg)
                    continue
                self._enter_folder(object_info.path)
                if self._yield_folders:
                    self._results.put(object_info)
                continue
            self._results.put(object_info)

    def __iter__(self):
        self._started = datetime.now()
        with ThreadPoolExecutor(max_workers=self._threads) as pool:
            self._enter_folder(self._path)
            pool.submit(self._reporter)
            for num in range(self._threads):
                pool.submit(self._worker, num)
            err = None
            while self._is_running():
                item = self._results.get()
                self._results.task_done()
                if item is None:
                    raise ValueError('...')
                yield item
            if err is not None:
                raise err
        took = datetime.now() - self._started
        _LOG.debug(f"Finished iterating over {self._path}. Took {took}")