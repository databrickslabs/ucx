import logging
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

from databricks.labs.ucx.__about__ import __version__
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


@staticmethod
def _remove_extra_indentation(doc: str) -> str:
    lines = doc.splitlines()
    stripped = []
    for line in lines:
        if line.startswith(" " * 4):
            stripped.append(line[4:])
        else:
            stripped.append(line)
    return "\n".join(stripped)


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
            doc=_remove_extra_indentation(func.__doc__),
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
    # `{{parent_run_id}}` is the run of entire workflow, whereas `{{run_id}}` is the run of a task
    workflow_run_id = args.get("parent_run_id", "unknown_run_id")
    job_id = args.get("job_id")
    if task_name not in _TASKS:
        msg = f'task "{task_name}" not found. Valid tasks are: {", ".join(_TASKS.keys())}'
        raise KeyError(msg)

    print(f"UCX v{__version__}")

    current_task = _TASKS[task_name]
    print(current_task.doc)

    config_path = Path(args["config"])
    cfg = WorkspaceConfig.from_file(config_path)

    # see https://docs.python.org/3/howto/logging-cookbook.html
    databricks_logger = logging.getLogger("databricks")
    databricks_logger.setLevel(logging.DEBUG)

    ucx_logger = logging.getLogger("databricks.labs.ucx")
    ucx_logger.setLevel(logging.DEBUG)

    log_path = config_path.parent / "logs" / current_task.workflow / f"run-{workflow_run_id}"
    log_path.mkdir(parents=True, exist_ok=True)

    log_file = log_path / f"{task_name}.log"
    file_handler = logging.FileHandler(log_file.as_posix())
    log_format = "%(asctime)s %(levelname)s [%(name)s] {%(threadName)s} %(message)s"
    log_formatter = logging.Formatter(fmt=log_format, datefmt="%H:%M:%S")
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.DEBUG)

    console_handler = _install(cfg.log_level)
    databricks_logger.removeHandler(console_handler)
    databricks_logger.addHandler(file_handler)

    ucx_logger.info(f"See debug logs at {log_file}")

    log_readme = log_path.joinpath("README.md")
    if not log_readme.exists():
        # this may race when run from multiple tasks, but let's accept the risk for now.
        with log_readme.open(mode="w") as f:
            f.write(f"# Logs for the UCX {current_task.workflow} workflow\n")
            f.write("This folder contains UCX log files.\n\n")
            f.write(f"See the [{current_task.workflow} job](/#job/{job_id}) and ")
            f.write(f"[run #{workflow_run_id}](/#job/{job_id}/run/{workflow_run_id})\n")

    try:
        current_task.fn(cfg)
    except BaseException as error:
        log_file_for_cli = str(log_file).lstrip("/Workspace")
        cli_command = f"databricks workspace export /{log_file_for_cli}"
        ucx_logger.error(f"Task crashed. Execute `{cli_command}` locally to troubleshoot with more details. {error}")
        databricks_logger.debug("Task crash details", exc_info=error)
        file_handler.flush()
        raise
    finally:
        file_handler.close()
