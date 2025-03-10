import logging
import shutil
import tempfile
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from urllib import parse

from databricks.labs.blueprint.paths import DBFSPath
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist, BadRequest, InvalidParameterValue, DatabricksError
from databricks.sdk.service import compute, jobs, pipelines
from databricks.sdk.service.compute import DataSecurityMode
from databricks.sdk.service.jobs import Source

from databricks.labs.ucx.assessment.crawlers import runtime_version_tuple
from databricks.labs.ucx.mixins.cached_workspace_path import WorkspaceCache, InvalidPath
from databricks.labs.ucx.source_code.base import (
    LineageAtom,
)
from databricks.labs.ucx.source_code.graph import (
    Dependency,
    DependencyGraph,
    DependencyProblem,
    SourceContainer,
    WrappingLoader,
)
from databricks.labs.ucx.source_code.path_lookup import PathLookup

logger = logging.getLogger(__name__)


@dataclass
class JobProblem:
    job_id: int
    job_name: str
    task_key: str
    path: str
    code: str
    message: str
    start_line: int
    start_col: int
    end_line: int
    end_col: int

    def as_message(self) -> str:
        message = f"{self.path}:{self.start_line} [{self.code}] {self.message}"
        return message


class WorkflowTask(Dependency):
    def __init__(self, ws: WorkspaceClient, task: jobs.Task, job: jobs.Job):
        loader = WrappingLoader(WorkflowTaskContainer(ws, task, job))
        super().__init__(loader, Path(f'/jobs/{task.task_key}'), inherits_context=False)
        self._task = task
        self._job = job

    def load(self, path_lookup: PathLookup) -> SourceContainer | None:
        return self._loader.load_dependency(path_lookup, self)

    def __repr__(self):
        return f'WorkflowTask<{self._task.task_key} of {self._job.settings.name}>'

    @property
    def lineage(self) -> list[LineageAtom]:
        job_name = (None if self._job.settings is None else self._job.settings.name) or "unknown job"
        job_lineage = LineageAtom("WORKFLOW", str(self._job.job_id), {"name": job_name})
        task_lineage = LineageAtom("TASK", f"{self._job.job_id}/{self._task.task_key}")
        return [job_lineage, task_lineage]


class WorkflowTaskContainer(SourceContainer):
    def __init__(self, ws: WorkspaceClient, task: jobs.Task, job: jobs.Job):
        self._task = task
        self._job = job
        self._ws = ws
        self._cache = WorkspaceCache(ws)
        self._named_parameters: dict[str, str] | None = {}
        self._parameters: list[str] | None = []
        self._spark_conf: dict[str, str] | None = {}
        self._spark_version: str | None = None
        self._data_security_mode: DataSecurityMode | None = None
        self._is_serverless = False

    @property
    def named_parameters(self) -> dict[str, str]:
        return self._named_parameters or {}

    @property
    def spark_conf(self) -> dict[str, str]:
        return self._spark_conf or {}

    @property
    def runtime_version(self) -> tuple[int, int]:
        version_tuple = runtime_version_tuple(self._spark_version)
        if not version_tuple:
            return 0, 0
        return version_tuple

    @property
    def data_security_mode(self) -> compute.DataSecurityMode:
        return self._data_security_mode or compute.DataSecurityMode.NONE

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        return list(self._register_task_dependencies(parent))

    def _register_task_dependencies(self, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        yield from self._register_cluster_info()
        yield from self._register_libraries(graph)
        yield from self._register_existing_cluster_id(graph)
        yield from self._register_notebook(graph)
        yield from self._register_spark_python_task(graph)
        yield from self._register_python_wheel_task(graph)
        yield from self._register_spark_jar_task(graph)
        yield from self._register_run_job_task(graph)
        yield from self._register_pipeline_task(graph)
        yield from self._register_spark_submit_task(graph)

    def _register_libraries(self, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        if not self._task.libraries:
            return
        for library in self._task.libraries:
            yield from self._register_library(graph, library)

    def _as_path(self, path: str) -> Path:
        parsed_path = parse.urlparse(path)
        match parsed_path.scheme:
            case "":
                return self._cache.get_workspace_path(path)
            case "dbfs":
                return DBFSPath(self._ws, parsed_path.path)
            case other:
                msg = f"Unsupported schema: {other} (only DBFS or Workspace paths are allowed)"
                raise InvalidPath(msg)

    @classmethod
    @contextmanager
    def _temporary_copy(cls, path: Path) -> Generator[Path, None, None]:
        try:
            with tempfile.TemporaryDirectory() as directory:
                temporary_path = Path(directory) / path.name
                with path.open("rb") as src, temporary_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                yield temporary_path
        except DatabricksError as e:
            # Cover cases like `ResourceDoesNotExist: Path (/Volumes/...-py3-none-any.whl) doesn't exist.`
            raise InvalidPath(f"Cannot load file: {path}") from e

    def _register_library(self, graph: DependencyGraph, library: compute.Library) -> Iterable[DependencyProblem]:
        if library.pypi:
            problems = graph.register_library(library.pypi.package)
            if problems:
                yield from problems
        try:
            if library.egg:
                yield from self._register_egg(graph, library)
            if library.whl:
                yield from self._register_whl(graph, library)
            if library.requirements:
                yield from self._register_requirements_txt(graph, library)
        except InvalidPath as e:
            yield DependencyProblem('cannot-load-file', str(e))
        except BadRequest as e:
            # see https://github.com/databrickslabs/ucx/issues/2916
            yield DependencyProblem('library-error', f'Cannot retrieve library: {e}')
        if library.jar:
            # TODO: https://github.com/databrickslabs/ucx/issues/1641
            yield DependencyProblem('not-yet-implemented', 'Jar library is not yet implemented')

    def _register_requirements_txt(self, graph, library) -> Iterable[DependencyProblem]:
        # https://pip.pypa.io/en/stable/reference/requirements-file-format/
        logger.info(f"Registering libraries from {library.requirements}")
        requirements_path = self._as_path(library.requirements)
        with requirements_path.open() as requirements:
            for requirement in requirements:
                requirement = requirement.rstrip()
                clean_requirement = requirement.replace(" ", "")  # requirements.txt may contain spaces
                if clean_requirement.startswith("-r"):
                    logger.warning(f"Reference to other requirements file is not supported: {requirement}")
                    continue
                if clean_requirement.startswith("-c"):
                    logger.warning(f"Reference to constraints file is not supported: {requirement}")
                    continue
                yield from graph.register_library(clean_requirement)

    def _register_whl(self, graph, library) -> Iterable[DependencyProblem]:
        try:
            wheel_path = self._as_path(library.whl)
            with self._temporary_copy(wheel_path) as local_file:
                yield from graph.register_library(local_file.as_posix())
        except InvalidPath as e:
            yield DependencyProblem('cannot-load-file', str(e))

    def _register_egg(self, graph, library) -> Iterable[DependencyProblem]:
        if self.runtime_version > (14, 0):
            yield DependencyProblem(
                code='not-supported',
                message='Installing eggs is no longer supported on Databricks 14.0 or higher',
            )
        logger.info(f"Registering library from {library.egg}")
        try:
            egg_path = self._as_path(library.egg)
            with self._temporary_copy(egg_path) as local_file:
                yield from graph.register_library(local_file.as_posix())
        except InvalidPath as e:
            yield DependencyProblem('cannot-load-file', str(e))

    def _register_notebook(self, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        if not self._task.notebook_task:
            return []
        if self._task.notebook_task.source == Source.GIT:
            # see https://github.com/databrickslabs/ucx/issues/2888
            message = 'Notebooks are in GIT. Use "databricks labs ucx lint-local-code" CLI command to discover problems'
            return [DependencyProblem("not-supported", message)]
        self._named_parameters = self._task.notebook_task.base_parameters
        notebook_path = self._task.notebook_task.notebook_path
        logger.info(f'Discovering {self._task.task_key} entrypoint: {notebook_path}')
        try:
            # Notebooks can't be on DBFS.
            path = self._cache.get_workspace_path(notebook_path)
        except InvalidPath as e:
            return [DependencyProblem('cannot-load-notebook', str(e))]
        return graph.register_notebook(path, False)

    def _register_spark_python_task(self, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        if not self._task.spark_python_task:
            return []
        self._parameters = self._task.spark_python_task.parameters
        python_file = self._task.spark_python_task.python_file
        logger.info(f'Discovering {self._task.task_key} entrypoint: {python_file}')
        try:
            path = self._as_path(python_file)
        except InvalidPath as e:
            return [DependencyProblem('cannot-load-file', str(e))]
        return graph.register_file(path)

    @staticmethod
    def _find_first_matching_distribution(path_lookup: PathLookup, name: str) -> metadata.Distribution | None:
        # Prepared exists in importlib.metadata.__init__pyi, but is not defined in importlib.metadata.__init__.py
        normalize_name = metadata.Prepared.normalize  # type: ignore
        normalized_name = normalize_name(name)
        for library_root in path_lookup.library_roots:
            for path in library_root.glob("*.dist-info"):
                distribution = metadata.Distribution.at(path)
                if normalize_name(distribution.name) == normalized_name:
                    return distribution
        return None

    def _register_python_wheel_task(self, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        if not self._task.python_wheel_task:
            return []
        self._named_parameters = self._task.python_wheel_task.named_parameters
        self._parameters = self._task.python_wheel_task.parameters
        distribution_name = self._task.python_wheel_task.package_name
        distribution = self._find_first_matching_distribution(graph.path_lookup, distribution_name)
        if distribution is None:
            return [DependencyProblem("distribution-not-found", f"Could not find distribution for {distribution_name}")]
        entry_point_name = self._task.python_wheel_task.entry_point
        try:
            entry_point = distribution.entry_points[entry_point_name]
        except KeyError:
            return [
                DependencyProblem(
                    "distribution-entry-point-not-found",
                    f"Could not find distribution entry point for {distribution_name}.{entry_point_name}",
                )
            ]
        return graph.register_import(entry_point.module)

    def _register_spark_jar_task(self, _) -> Iterable[DependencyProblem]:
        if not self._task.spark_jar_task:
            return
        # TODO: https://github.com/databrickslabs/ucx/issues/1641
        self._parameters = self._task.spark_jar_task.parameters
        yield DependencyProblem('not-yet-implemented', 'Spark Jar task is not yet implemented')

    def _register_run_job_task(self, _) -> Iterable[DependencyProblem]:
        if not self._task.run_job_task:
            return
        # TODO: it's not clear how to terminate the graph
        yield DependencyProblem('not-yet-implemented', 'Run job task is not yet implemented')

    def _register_pipeline_task(self, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        if not self._task.pipeline_task:
            return

        try:
            pipeline = self._ws.pipelines.get(self._task.pipeline_task.pipeline_id)
        except ResourceDoesNotExist:
            yield DependencyProblem(
                'pipeline-not-found', f'Could not find pipeline: {self._task.pipeline_task.pipeline_id}'
            )
            return

        if not pipeline.spec or not pipeline.spec.libraries:
            return

        for library in pipeline.spec.libraries:
            yield from self._register_pipeline_library(graph, library)

    def _register_pipeline_library(
        self, graph: DependencyGraph, library: pipelines.PipelineLibrary
    ) -> Iterable[DependencyProblem]:
        if library.notebook and library.notebook.path:
            yield from self._register_notebook_path(graph, library.notebook.path)
        if library.jar:
            yield from self._register_library(graph, compute.Library(jar=library.jar))
        if library.maven:
            yield DependencyProblem('not-yet-implemented', 'Maven library is not yet implemented')
        if library.file:
            yield DependencyProblem('not-yet-implemented', 'File library is not yet implemented')

    def _register_notebook_path(self, graph: DependencyGraph, notebook_path: str) -> Iterable[DependencyProblem]:
        try:
            # Notebooks can't be on DBFS.
            path = self._cache.get_workspace_path(notebook_path)
        except InvalidPath as e:
            yield DependencyProblem('cannot-load-notebook', str(e))
            return
        # the notebook is the root of the graph, so there's no context to inherit
        yield from graph.register_notebook(path, inherit_context=False)

    def _register_existing_cluster_id(self, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        if not self._task.existing_cluster_id:
            return
        try:
            # load libraries installed on the referred cluster
            library_full_status_list = self._ws.libraries.cluster_status(self._task.existing_cluster_id)
            for library_full_status in library_full_status_list:
                if library_full_status.library:
                    yield from self._register_library(graph, library_full_status.library)
        except (ResourceDoesNotExist, InvalidParameterValue):
            yield DependencyProblem('cluster-not-found', f'Could not find cluster: {self._task.existing_cluster_id}')

    def _register_spark_submit_task(self, _) -> Iterable[DependencyProblem]:
        if not self._task.spark_submit_task:
            return
        yield DependencyProblem('not-yet-implemented', 'Spark submit task is not yet implemented')

    def _register_cluster_info(self) -> Iterable[DependencyProblem]:
        if self._task.existing_cluster_id:
            try:
                cluster_info = self._ws.clusters.get(self._task.existing_cluster_id)
                return self._new_job_cluster_metadata(cluster_info)
            except (ResourceDoesNotExist, InvalidParameterValue):
                message = f'Could not find cluster: {self._task.existing_cluster_id}'
                yield DependencyProblem('cluster-not-found', message)
        if self._task.new_cluster:
            return self._new_job_cluster_metadata(self._task.new_cluster)
        if self._task.job_cluster_key:
            assert self._job.settings is not None, "Job settings are missing"
            assert self._job.settings.job_clusters is not None, "Job clusters are missing"
            for job_cluster in self._job.settings.job_clusters:
                if job_cluster.job_cluster_key != self._task.job_cluster_key:
                    continue
                return self._new_job_cluster_metadata(job_cluster.new_cluster)
        self._data_security_mode = compute.DataSecurityMode.USER_ISOLATION
        self._is_serverless = True
        return []

    def _new_job_cluster_metadata(self, new_cluster) -> Iterable[DependencyProblem]:
        self._spark_conf = new_cluster.spark_conf
        self._spark_version = new_cluster.spark_version
        self._data_security_mode = new_cluster.data_security_mode
        return []
