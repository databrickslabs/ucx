import concurrent
import datetime as dt
import functools
import logging
import os
import re
import threading
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Generic, TypeVar

MIN_THREADS = 8

Result = TypeVar("Result")
Task = Callable[[], Result | None] | functools.partial[Result | None]
logger = logging.getLogger(__name__)


class ManyError(RuntimeError):
    def __init__(self, errs):
        strs = [str(_) for _ in errs]
        msg = f'Detected {len(errs)} failures: {",".join(strs)}'
        super().__init__(msg)


class Threads(Generic[Result]):
    def __init__(self, name, tasks: Sequence[Task[Result]], num_threads: int):
        self._name = name
        self._tasks = list(tasks)
        self._task_fail_error_pct = 50
        self._num_threads = num_threads
        self._started = dt.datetime.now()
        self._lock = threading.Lock()
        self._completed_cnt = 0
        self._large_log_every = 3000
        self._default_log_every = 100

    @classmethod
    def gather(
        cls, name: str, tasks: Sequence[Task[Result]], num_threads: int | None = None
    ) -> tuple[Iterable[Result], list[Exception]]:
        if num_threads is None:
            num_cpus = os.cpu_count()
            if num_cpus is None:
                num_cpus = 1
            num_threads = num_cpus * 2
            if num_threads < MIN_THREADS:
                num_threads = MIN_THREADS
        return cls(name, tasks, num_threads=num_threads)._run()

    @classmethod
    def strict(cls, name: str, tasks: Sequence[Task[Result]]):
        _, errs = cls.gather(name, tasks)
        if errs:
            raise ManyError(errs)

    def _run(self) -> tuple[Iterable[Result], list[Exception]]:
        given_cnt = len(self._tasks)
        if given_cnt == 0:
            return [], []
        logger.debug(f"Starting {given_cnt} tasks in {self._num_threads} threads")

        collected = []
        errors = []
        for future in self._execute():
            return_value = future.result()
            if return_value is None:
                continue
            result, err = return_value
            if err is not None:
                errors.append(err)
                continue
            if result is None:
                continue
            collected.append(result)
        self._on_finish(given_cnt, len(collected), len(errors))

        return collected, errors

    def _on_finish(self, given_cnt: int, collected_cnt: int, failed_cnt: int):
        since = dt.datetime.now() - self._started
        success_pct = collected_cnt / given_cnt * 100
        stats = f"{success_pct:.0f}% results available ({collected_cnt}/{given_cnt}). Took {since}"
        if failed_cnt == given_cnt:
            logger.critical(f"All '{self._name}' tasks failed!!!")
        elif failed_cnt > 0 and success_pct <= self._task_fail_error_pct:
            logger.error(f"More than half '{self._name}' tasks failed: {stats}")
        elif failed_cnt > 0:
            logger.warning(f"Some '{self._name}' tasks failed: {stats}")
        else:
            logger.info(f"Finished '{self._name}' tasks: {stats}")

    def _execute(self):
        thread_name_prefix = re.sub(r"\W+", "_", self._name)
        with ThreadPoolExecutor(self._num_threads, thread_name_prefix) as pool:
            futures = []
            for task in self._tasks:
                if task is None:
                    continue
                future = pool.submit(self._wrap_result(task, self._name))
                future.add_done_callback(self._progress_report)
                futures.append(future)
            return concurrent.futures.as_completed(futures)

    def _progress_report(self, _):
        total_cnt = len(self._tasks)
        log_every = self._default_log_every
        if total_cnt > self._large_log_every:
            log_every = 500
        elif total_cnt <= self._default_log_every:
            log_every = 10
        with self._lock:
            self._completed_cnt += 1
            since = dt.datetime.now() - self._started
            rps = self._completed_cnt / since.total_seconds()
            if self._completed_cnt % log_every == 0 or self._completed_cnt == total_cnt:
                msg = f"{self._name} {self._completed_cnt}/{total_cnt}, rps: {rps:.3f}/sec"
                logger.info(msg)

    @staticmethod
    def _wrap_result(func, name):
        """This method emulates GoLang's error return style"""

        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs), None
            except Exception as err:
                logger.error(f"{name} task failed: {err!s}", exc_info=err)
                return None, err

        return inner
