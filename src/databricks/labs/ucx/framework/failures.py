import logging
import os
import threading
import time
from dataclasses import dataclass, field
from functools import partial
from typing import ClassVar

from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.framework.tasks import (
    JOB_ID_ENVKEY,
    PARENT_RUN_ID_ENVKEY,
    TASK_NAME_ENVKEY,
    WORKFLOW_NAME_ENVKEY,
)

logger = logging.getLogger(__name__)


class ObjectFailureError(Exception):
    def __init__(
        self, object_type: str | None = None, object_id: str | None = None, root_cause: Exception | None = None
    ):
        self._object_type = object_type
        self._object_id = object_id
        self._root_cause = root_cause

    def __str__(self):
        return str(self._root_cause)

    @property
    def object_type(self):
        return self._object_type

    @property
    def object_id(self):
        return self._object_id

    @property
    def root_cause(self):
        return self._root_cause


@dataclass
class ObjectFailure:
    event_time: float = field(default_factory=time.time)
    step_name: str = field(default_factory=partial(os.environ.get, WORKFLOW_NAME_ENVKEY, "n/a"))
    task_name: str = field(default_factory=partial(os.environ.get, TASK_NAME_ENVKEY, "n/a"))
    parent_run_id: str = field(default_factory=partial(os.environ.get, PARENT_RUN_ID_ENVKEY, "n/a"))
    job_id: str = field(default_factory=partial(os.environ.get, JOB_ID_ENVKEY, "n/a"))
    object_type: str | None = None
    object_id: str | None = None
    error_info: str | None = None

    @staticmethod
    def make(error: Exception):
        object_type = error.object_type if isinstance(error, ObjectFailureError) else None
        object_id = error.object_id if isinstance(error, ObjectFailureError) else None
        return ObjectFailure(object_type=object_type, object_id=object_id, error_info=str(error))


def set_error_info(self, exception: Exception):
    self.error_info = str(exception)


class FailureReporter:
    _buffer: ClassVar[list[ObjectFailure]] = []
    _events_lock = threading.Lock()

    def __init__(self, backend: SqlBackend, catalog: str, schema: str, table: str = "failures"):
        self._backend = backend
        self._catalog = catalog
        self._schema = schema
        self._table = table

    def report(self, failure: ObjectFailure):
        with self._events_lock:
            self._buffer.append(failure)

    def flush(self):
        with self._events_lock:
            if len(self._buffer) > 0:
                full_name = f"{self._catalog}.{self._schema}.{self._table}"
                logger.debug(f"Persisting {len(self._buffer)} new records in {full_name}")
                self._backend.save_table(full_name, self._buffer, ObjectFailure, mode="append")
                self._buffer.clear()
