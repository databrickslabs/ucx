from collections.abc import Iterable

from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Task, job_task


class Workflow:
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self):
        return self._name

    def tasks(self) -> Iterable[Task]:
        # return __task__ from every method in this class that has this attribute
        for attr in dir(self):
            if attr.startswith("_"):
                continue
            fn = getattr(self, attr)
            if hasattr(fn, "__task__"):
                yield fn.__task__

    @job_task
    def parse_logs(self, ctx: RuntimeContext):
        """Parse and store the warning log messages."""
        ctx.task_run_warning_recorder.snapshot()
