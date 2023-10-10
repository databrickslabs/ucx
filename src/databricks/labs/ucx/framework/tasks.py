import logging
import pathlib
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.logger import _install

_TASKS: dict[str, "Task"] = {}


@dataclass
class Task:
    task_id: int
    workflow: str
    name: str
    doc: str
    fn: Callable[[WorkspaceConfig], None]
    depends_on: list[str] = None
    job_cluster: str = "main"
    notebook: str = None
    dashboard: str = None


def task(workflow, *, depends_on=None, job_cluster="main", notebook: str | None = None, dashboard: str | None = None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Perform any task-specific logic here
            # For example, you can log when the task is started and completed
            logger = logging.getLogger(func.__name__)
            logger.info(f"Task '{workflow}' is starting...")
            result = func(*args, **kwargs)
            logger.info(f"Task '{workflow}' is completed!")
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

        if not func.__doc__:
            msg = f"Task {func.__name__} must have documentation"
            raise SyntaxError(msg)

        _TASKS[func.__name__] = Task(
            task_id=len(_TASKS),
            workflow=workflow,
            name=func.__name__,
            doc=func.__doc__,
            fn=func,
            depends_on=deps,
            job_cluster=job_cluster,
            notebook=notebook,
            dashboard=dashboard,
        )

        return wrapper

    return decorator


def trigger(*argv):
    args = dict(a[2:].split("=") for a in argv if "--" == a[0:2])
    if "config" not in args:
        msg = "no --config specified"
        raise KeyError(msg)
    task_name = args.get("task", "not specified")
    parent_run_id = args.get("parent_run_id", "unknown_run_id")
    job_id = args.get("job_id")
    if task_name not in _TASKS:
        msg = f'task "{task_name}" not found. Valid tasks are: {", ".join(_TASKS.keys())}'
        raise KeyError(msg)

    current_task = _TASKS[task_name]
    print(current_task.doc)

    config_path = Path(args["config"])
    cfg = WorkspaceConfig.from_file(config_path)
    _install()

    logger = logging.getLogger("databricks")
    logger.setLevel(cfg.log_level)

    filepath = pathlib.Path(config_path).parent
    logpath = filepath.joinpath(f"logs/{current_task.workflow}/{parent_run_id}")
    try:
        logpath.mkdir(parents=True)
    except OSError as error:
        logger.info(f"Failed to create log folder: {error}")
    logfile = logpath.joinpath(f"ucx_{task_name}.log")
    file_handler = logging.FileHandler(logfile.as_posix())
    file_handler.setFormatter(
        logging.Formatter(fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s", datefmt="%H:%M:%S")
    )
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.info(f"Setup File Logging at {logfile}")

    md_file = logpath.joinpath("README.md")
    if not md_file.exists():
        with md_file.open() as f:
            f.write(f"# Logs for the {current_task.workflow} workflow\n")
            f.write("This folders contains UCX log files.<br/>\n")
            f.write(f"[These logs belong to job #{job_id} run #{parent_run_id}](/#job/{job_id}/run/{parent_run_id})\n")

    try:
        logger.info(f"Starting {current_task.workflow} - {task_name}")
        current_task.fn(cfg)
        logger.info(f"Completed {current_task.workflow} - {task_name}")
    except Exception as error:
        logger.error(f"Task failed with:{error}")
        raise
    finally:
        file_handler.close()
