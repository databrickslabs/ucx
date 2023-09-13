import logging
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

from databricks.labs.ucx.config import MigrationConfig
from databricks.labs.ucx.logger import _install

_TASKS: dict[str, "Task"] = {}


@dataclass
class Task:
    workflow: str
    name: str
    doc: str
    fn: Callable[[MigrationConfig], None]
    depends_on: list[str] = None
    job_cluster: str = "main"


def task(workflow, *, depends_on=None, job_cluster="main"):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Perform any task-specific logic here
            # For example, you can log when the task is started and completed
            print(f"Task '{workflow}' is starting...")
            result = func(*args, **kwargs)
            print(f"Task '{workflow}' is completed!")
            return result

        deps = []
        if depends_on is not None:
            if not isinstance(depends_on, list):
                msg = "depends_on has to be a list"
                raise SyntaxError(msg)
            for fn in depends_on:
                if _TASKS[fn.__name__].workflow != workflow:
                    # for now, we filter out the cross-task
                    # dependencies within the same job.
                    #
                    # Technically, we can check it and fail
                    # the job if the previous steps didn't
                    # run before.
                    continue
                deps.append(fn.__name__)

        _TASKS[func.__name__] = Task(
            workflow=workflow, name=func.__name__, doc=func.__doc__, fn=func, depends_on=deps, job_cluster=job_cluster
        )

        return wrapper

    return decorator


def trigger(*argv):
    args = dict(a[2:].split("=") for a in argv if "--" == a[0:2])
    if "config" not in args:
        msg = "no --config specified"
        raise KeyError(msg)

    task_name = args.get("task", "not specified")
    if task_name not in _TASKS:
        msg = f'task "{task_name}" not found. Valid tasks are: {", ".join(_TASKS.keys())}'
        raise KeyError(msg)

    current_task = _TASKS[task_name]
    print(current_task.doc)

    _install()

    cfg = MigrationConfig.from_file(Path(args["config"]))
    logging.getLogger("databricks").setLevel(cfg.log_level)

    current_task.fn(cfg)
