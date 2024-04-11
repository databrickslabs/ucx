import contextlib
import logging
import os
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.logger import install_logger
from databricks.labs.lsql.backends import SqlBackend, RuntimeBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.retries import retried

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import WorkspaceConfig

_TASKS: dict[str, "Task"] = {}


@dataclass
class Task:
    workflow: str
    name: str
    doc: str
    fn: Callable[[WorkspaceConfig, WorkspaceClient, SqlBackend, Installation], None]
    depends_on: list[str] | None = None
    job_cluster: str = "main"
    notebook: str | None = None
    dashboard: str | None = None
    cloud: str | None = None

    def is_testing(self):
        return self.workflow in {"failing"}

    def dependencies(self):
        if not self.depends_on:
            return []
        return self.depends_on

    def cloud_compatible(self, config: Config) -> bool:
        """Test compatibility between workspace config and task"""
        if self.cloud:
            if self.cloud.lower() == "aws":
                return config.is_aws
            if self.cloud.lower() == "azure":
                return config.is_azure
            if self.cloud.lower() == "gcp":
                return config.is_gcp
            return True
        return True


def remove_extra_indentation(doc: str) -> str:
    lines = doc.splitlines()
    stripped = []
    for line in lines:
        if line.startswith(" " * 4):
            stripped.append(line[4:])
        else:
            stripped.append(line)
    return "\n".join(stripped)


class TaskLogger(contextlib.AbstractContextManager):
    # files are available in the workspace only once their handlers are closed,
    # so we rotate files log every 10 minutes.
    #
    # See https://docs.python.org/3/library/logging.handlers.html#logging.handlers.TimedRotatingFileHandler
    # See https://docs.python.org/3/howto/logging-cookbook.html

    def __init__(
        self,
        install_dir: Path,
        workflow: str,
        workflow_id: str,
        task_name: str,
        workflow_run_id: str,
        log_level="INFO",
        attempt: str = "0",
    ):
        self._log_level = log_level
        self._workflow = workflow
        self._workflow_id = workflow_id
        self._workflow_run_id = workflow_run_id
        self._databricks_logger = logging.getLogger("databricks")
        self._app_logger = logging.getLogger("databricks.labs.ucx")
        self._log_path = self.log_path(install_dir, workflow, workflow_run_id, attempt)
        self.log_file = self._log_path / f"{task_name}.log"
        self._app_logger.info(f"UCX v{__version__} After job finishes, see debug logs at {self.log_file}")

    @classmethod
    def log_path(cls, install_dir: Path, workflow: str, workflow_run_id: str | int, attempt: str | int) -> Path:
        return install_dir / "logs" / workflow / f"run-{workflow_run_id}-{attempt}"

    def __repr__(self):
        return self.log_file.as_posix()

    def __enter__(self):
        self._log_path.mkdir(parents=True, exist_ok=True)
        self._init_debug_logfile()
        self._init_run_readme()
        self._databricks_logger.setLevel(logging.DEBUG)
        self._app_logger.setLevel(logging.DEBUG)
        console_handler = install_logger(self._log_level)
        self._databricks_logger.removeHandler(console_handler)
        self._databricks_logger.addHandler(self._file_handler)
        return self

    def __exit__(self, _t, error, _tb):
        if error:
            log_file_for_cli = str(self.log_file).removeprefix("/Workspace")
            cli_command = f"databricks workspace export /{log_file_for_cli}"
            self._app_logger.error(f"Execute `{cli_command}` locally to troubleshoot with more details. {error}")
            self._databricks_logger.debug("Task crash details", exc_info=error)
        self._file_handler.flush()
        self._file_handler.close()

    def _init_debug_logfile(self):
        log_format = "%(asctime)s %(levelname)s [%(name)s] {%(threadName)s} %(message)s"
        log_formatter = logging.Formatter(fmt=log_format, datefmt="%H:%M:%S")
        self._file_handler = TimedRotatingFileHandler(self.log_file.as_posix(), when="M", interval=1)
        self._file_handler.setFormatter(log_formatter)
        self._file_handler.setLevel(logging.DEBUG)

    def _init_run_readme(self):
        log_readme = self._log_path.joinpath("README.md")
        if log_readme.exists():
            return
        # this may race when run from multiple tasks, therefore it must be multiprocess safe
        with self._exclusive_open(str(log_readme), mode="w") as f:
            f.write(f"# Logs for the UCX {self._workflow} workflow\n")
            f.write("This folder contains UCX log files.\n\n")
            f.write(f"See the [{self._workflow} job](/#job/{self._workflow_id}) and ")
            f.write(f"[run #{self._workflow_run_id}](/#job/{self._workflow_id}/run/{self._workflow_run_id})\n")

    @classmethod
    @contextmanager
    def _exclusive_open(cls, filename: str, **kwargs):
        """Open a file with exclusive access across multiple processes.
        Requires write access to the directory containing the file.

        Arguments are the same as the built-in open.

        Returns a context manager that closes the file and releases the lock.
        """
        lockfile_name = filename + ".lock"
        lockfile = cls._create_lock(lockfile_name)

        try:
            with open(filename, encoding="utf8", **kwargs) as f:
                yield f
        finally:
            try:
                os.close(lockfile)
            finally:
                os.unlink(lockfile_name)

    @staticmethod
    @retried(on=[FileExistsError], timeout=timedelta(seconds=5))
    def _create_lock(lockfile_name):
        while True:  # wait until the lock file can be opened
            f = os.open(lockfile_name, os.O_CREAT | os.O_EXCL)
            break
        return f


def parse_args(*argv) -> dict[str, str]:
    args = dict(a[2:].split("=") for a in argv if a[0:2] == "--")
    if "config" not in args:
        msg = "no --config specified"
        raise KeyError(msg)
    return args


def run_task(
    args: dict[str, str],
    install_dir: Path,
    cfg: WorkspaceConfig,
    workspace_client: WorkspaceClient,
    sql_backend: RuntimeBackend,
    installation: Installation,
):
    # TODO: remove this function
    task_name = args.get("task", "not specified")
    if task_name not in _TASKS:
        msg = f'task "{task_name}" not found. Valid tasks are: {", ".join(_TASKS.keys())}'
        raise KeyError(msg)
    print(f"UCX v{__version__}")
    current_task = _TASKS[task_name]
    print(current_task.doc)

    # `{{parent_run_id}}` is the run of entire workflow, whereas `{{run_id}}` is the run of a task
    workflow_run_id = args.get("parent_run_id", "unknown_run_id")
    job_id = args.get("job_id", "unknown_job_id")

    with TaskLogger(
        install_dir,
        workflow=current_task.workflow,
        workflow_id=job_id,
        task_name=task_name,
        workflow_run_id=workflow_run_id,
        log_level=cfg.log_level,
    ) as task_logger:
        ucx_logger = logging.getLogger("databricks.labs.ucx")
        ucx_logger.info(f"UCX v{__version__} After job finishes, see debug logs at {task_logger}")
        current_task.fn(cfg, workspace_client, sql_backend, installation)


def trigger(*argv):
    args = parse_args(*argv)
    config_path = Path(args["config"])

    cfg = Installation.load_local(WorkspaceConfig, config_path)
    sql_backend = RuntimeBackend(debug_truncate_bytes=cfg.connect.debug_truncate_bytes)
    workspace_client = WorkspaceClient(config=cfg.connect, product='ucx', product_version=__version__)
    install_folder = config_path.parent.as_posix().removeprefix("/Workspace")
    installation = Installation(workspace_client, "ucx", install_folder=install_folder)

    run_task(args, config_path.parent, cfg, workspace_client, sql_backend, installation)


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


def job_task(
    fn=None,
    *,
    depends_on=None,
    job_cluster="main",
    notebook: str | None = None,
    dashboard: str | None = None,
    cloud: str | None = None,
) -> Callable[[Callable], Callable]:
    def register(func):
        if not func.__doc__:
            raise SyntaxError(f"{func.__name__} must have some doc comment")
        deps = []
        this_class = func.__qualname__.split('.')[0]
        if depends_on is not None:
            if not isinstance(depends_on, list):
                msg = "depends_on has to be a list"
                raise SyntaxError(msg)
            for fn in depends_on:
                other_class, task_name = fn.__qualname__.split('.')
                if other_class != this_class:
                    continue
                deps.append(task_name)
        func.__task__ = Task(
            workflow='<unknown>',
            name=func.__name__,
            doc=remove_extra_indentation(func.__doc__),
            fn=func,
            depends_on=deps,
            job_cluster=job_cluster,
            notebook=notebook,
            dashboard=dashboard,
            cloud=cloud,
        )
        return func

    if fn is None:
        return register
    register(fn)
    return fn
