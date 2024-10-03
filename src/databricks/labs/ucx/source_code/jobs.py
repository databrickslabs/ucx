import dataclasses
import functools
import logging
import shutil
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import TypeVar
from urllib import parse

from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.blueprint.paths import DBFSPath
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.assessment.crawlers import runtime_version_tuple
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.mixins.cached_workspace_path import WorkspaceCache
from databricks.labs.ucx.source_code.base import (
    CurrentSessionState,
    LocatedAdvice,
    is_a_notebook,
    file_language,
    guess_encoding,
    SourceInfo,
    UsedTable,
    LineageAtom,
    PythonSequentialLinter,
)
from databricks.labs.ucx.source_code.directfs_access import (
    DirectFsAccessCrawler,
    DirectFsAccess,
)
from databricks.labs.ucx.source_code.graph import (
    Dependency,
    DependencyGraph,
    DependencyProblem,
    DependencyResolver,
    SourceContainer,
    WrappingLoader,
    DependencyGraphWalker,
)
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage
from databricks.labs.ucx.source_code.python.python_ast import Tree
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter, Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler

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
        self._data_security_mode = None
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
                return self._cache.get_path(path)
            case "dbfs":
                return DBFSPath(self._ws, parsed_path.path)
            case other:
                msg = f"Unsupported schema: {other} (only DBFS or Workspace paths are allowed)"
                raise ValueError(msg)

    @classmethod
    @contextmanager
    def _temporary_copy(cls, path: Path) -> Generator[Path, None, None]:
        with tempfile.TemporaryDirectory() as directory:
            temporary_path = Path(directory) / path.name
            with path.open("rb") as src, temporary_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            yield temporary_path

    def _register_library(self, graph: DependencyGraph, library: compute.Library) -> Iterable[DependencyProblem]:
        if library.pypi:
            problems = graph.register_library(library.pypi.package)
            if problems:
                yield from problems
        if library.egg:
            yield from self._register_egg(graph, library)
        if library.whl:
            wheel_path = self._as_path(library.whl)
            with self._temporary_copy(wheel_path) as local_file:
                yield from graph.register_library(local_file.as_posix())
        if library.requirements:  # https://pip.pypa.io/en/stable/reference/requirements-file-format/
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
        if library.jar:
            # TODO: https://github.com/databrickslabs/ucx/issues/1641
            yield DependencyProblem('not-yet-implemented', 'Jar library is not yet implemented')

    def _register_egg(self, graph, library):
        if self.runtime_version > (14, 0):
            yield DependencyProblem(
                code='not-supported',
                message='Installing eggs is no longer supported on Databricks 14.0 or higher',
            )
        logger.info(f"Registering library from {library.egg}")
        egg_path = self._as_path(library.egg)
        with self._temporary_copy(egg_path) as local_file:
            yield from graph.register_library(local_file.as_posix())

    def _register_notebook(self, graph: DependencyGraph) -> Iterable[DependencyProblem]:
        if not self._task.notebook_task:
            return []
        self._named_parameters = self._task.notebook_task.base_parameters
        notebook_path = self._task.notebook_task.notebook_path
        logger.info(f'Discovering {self._task.task_key} entrypoint: {notebook_path}')
        # Notebooks can't be on DBFS.
        path = self._cache.get_path(notebook_path)
        return graph.register_notebook(path, False)

    def _register_spark_python_task(self, graph: DependencyGraph):
        if not self._task.spark_python_task:
            return []
        self._parameters = self._task.spark_python_task.parameters
        python_file = self._task.spark_python_task.python_file
        logger.info(f'Discovering {self._task.task_key} entrypoint: {python_file}')
        path = self._as_path(python_file)
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

    def _register_spark_jar_task(self, graph: DependencyGraph):  # pylint: disable=unused-argument
        if not self._task.spark_jar_task:
            return
        # TODO: https://github.com/databrickslabs/ucx/issues/1641
        self._parameters = self._task.spark_jar_task.parameters
        yield DependencyProblem('not-yet-implemented', 'Spark Jar task is not yet implemented')

    def _register_run_job_task(self, graph: DependencyGraph):  # pylint: disable=unused-argument
        if not self._task.run_job_task:
            return
        # TODO: it's not clear how to terminate the graph
        yield DependencyProblem('not-yet-implemented', 'Run job task is not yet implemented')

    def _register_pipeline_task(self, graph: DependencyGraph):
        if not self._task.pipeline_task:
            return

        pipeline = self._ws.pipelines.get(self._task.pipeline_task.pipeline_id)
        if not pipeline.spec:
            return
        if not pipeline.spec.libraries:
            return

        pipeline_libraries = pipeline.spec.libraries
        for library in pipeline_libraries:
            if not library.notebook:
                return
            if library.notebook.path:
                notebook_path = library.notebook.path
                # Notebooks can't be on DBFS.
                path = self._cache.get_path(notebook_path)
                # the notebook is the root of the graph, so there's no context to inherit
                yield from graph.register_notebook(path, inherit_context=False)
            if library.jar:
                yield from self._register_library(graph, compute.Library(jar=library.jar))
            if library.maven:
                yield DependencyProblem('not-yet-implemented', 'Maven library is not yet implemented')
            if library.file:
                yield DependencyProblem('not-yet-implemented', 'File library is not yet implemented')

    def _register_existing_cluster_id(self, graph: DependencyGraph):
        if not self._task.existing_cluster_id:
            return

        # load libraries installed on the referred cluster
        library_full_status_list = self._ws.libraries.cluster_status(self._task.existing_cluster_id)

        for library_full_status in library_full_status_list:
            if library_full_status.library:
                yield from self._register_library(graph, library_full_status.library)

    def _register_spark_submit_task(self, graph: DependencyGraph):  # pylint: disable=unused-argument
        if not self._task.spark_submit_task:
            return
        yield DependencyProblem('not-yet-implemented', 'Spark submit task is not yet implemented')

    def _register_cluster_info(self):
        if self._task.existing_cluster_id:
            cluster_info = self._ws.clusters.get(self._task.existing_cluster_id)
            return self._new_job_cluster_metadata(cluster_info)
        if self._task.new_cluster:
            return self._new_job_cluster_metadata(self._task.new_cluster)
        if self._task.job_cluster_key:
            for job_cluster in self._job.settings.job_clusters:
                if job_cluster.job_cluster_key != self._task.job_cluster_key:
                    continue
                return self._new_job_cluster_metadata(job_cluster.new_cluster)
        self._data_security_mode = compute.DataSecurityMode.USER_ISOLATION
        self._is_serverless = True
        return []

    def _new_job_cluster_metadata(self, new_cluster):
        self._spark_conf = new_cluster.spark_conf
        self._spark_version = new_cluster.spark_version
        self._data_security_mode = new_cluster.data_security_mode
        return []


class WorkflowLinter:
    def __init__(
        self,
        ws: WorkspaceClient,
        resolver: DependencyResolver,
        path_lookup: PathLookup,
        migration_index: TableMigrationIndex,
        directfs_crawler: DirectFsAccessCrawler,
        used_tables_crawler: UsedTablesCrawler,
        include_job_ids: list[int] | None = None,
    ):
        self._ws = ws
        self._resolver = resolver
        self._path_lookup = path_lookup
        self._migration_index = migration_index
        self._directfs_crawler = directfs_crawler
        self._used_tables_crawler = used_tables_crawler
        self._include_job_ids = include_job_ids

    def refresh_report(self, sql_backend: SqlBackend, inventory_database: str):
        tasks = []
        all_jobs = list(self._ws.jobs.list())
        logger.info(f"Preparing {len(all_jobs)} linting tasks...")
        for job in all_jobs:
            if self._include_job_ids and job.job_id not in self._include_job_ids:
                logger.info(f"Skipping job {job.job_id}...")
                continue
            tasks.append(functools.partial(self.lint_job, job.job_id))
        logger.info(f"Running {tasks} linting tasks in parallel...")
        job_results, errors = Threads.gather('linting workflows', tasks)
        job_problems: list[JobProblem] = []
        job_dfsas: list[DirectFsAccess] = []
        job_tables: list[UsedTable] = []
        for problems, dfsas, tables in job_results:
            job_problems.extend(problems)
            job_dfsas.extend(dfsas)
            job_tables.extend(tables)
        logger.info(f"Saving {len(job_problems)} linting problems...")
        sql_backend.save_table(
            f'{inventory_database}.workflow_problems',
            job_problems,
            JobProblem,
            mode='overwrite',
        )
        self._directfs_crawler.dump_all(job_dfsas)
        self._used_tables_crawler.dump_all(job_tables)
        if len(errors) > 0:
            raise ManyError(errors)

    def lint_job(self, job_id: int) -> tuple[list[JobProblem], list[DirectFsAccess], list[UsedTable]]:
        try:
            job = self._ws.jobs.get(job_id)
        except NotFound:
            logger.warning(f'Could not find job: {job_id}')
            return ([], [], [])

        problems, dfsas, tables = self._lint_job(job)
        if len(problems) > 0:
            problem_messages = "\n".join([problem.as_message() for problem in problems])
            logger.warning(f"Found job problems:\n{problem_messages}")
        return problems, dfsas, tables

    _UNKNOWN = Path('<UNKNOWN>')

    def _lint_job(self, job: jobs.Job) -> tuple[list[JobProblem], list[DirectFsAccess], list[UsedTable]]:
        problems: list[JobProblem] = []
        dfsas: list[DirectFsAccess] = []
        table_infos: list[UsedTable] = []

        assert job.job_id is not None
        assert job.settings is not None
        assert job.settings.name is not None
        assert job.settings.tasks is not None
        linted_paths: set[Path] = set()
        for task in job.settings.tasks:
            graph, advices, session_state = self._build_task_dependency_graph(task, job)
            if not advices:
                advices = self._lint_task(task, graph, session_state, linted_paths)
            for advice in advices:
                absolute_path = advice.path.absolute().as_posix() if advice.path != self._UNKNOWN else 'UNKNOWN'
                job_problem = JobProblem(
                    job_id=job.job_id,
                    job_name=job.settings.name,
                    task_key=task.task_key,
                    path=absolute_path,
                    code=advice.advice.code,
                    message=advice.advice.message,
                    start_line=advice.advice.start_line,
                    start_col=advice.advice.start_col,
                    end_line=advice.advice.end_line,
                    end_col=advice.advice.end_col,
                )
                problems.append(job_problem)
            assessment_start = datetime.now(timezone.utc)
            task_dfsas = self._collect_task_dfsas(job, task, graph, session_state)
            assessment_end = datetime.now(timezone.utc)
            for dfsa in task_dfsas:
                dfsa = dfsa.replace_assessment_infos(assessment_start=assessment_start, assessment_end=assessment_end)
                dfsas.append(dfsa)
            assessment_start = datetime.now(timezone.utc)
            task_tables = self._collect_task_tables(job, task, graph, session_state)
            assessment_end = datetime.now(timezone.utc)
            for table_info in task_tables:
                table_info = table_info.replace_assessment_infos(
                    assessment_start=assessment_start, assessment_end=assessment_end
                )
                table_infos.append(table_info)

        return problems, dfsas, table_infos

    def _build_task_dependency_graph(
        self, task: jobs.Task, job: jobs.Job
    ) -> tuple[DependencyGraph, Iterable[LocatedAdvice], CurrentSessionState]:
        root_dependency: Dependency = WorkflowTask(self._ws, task, job)
        # we can load it without further preparation since the WorkflowTask is merely a wrapper
        container = root_dependency.load(self._path_lookup)
        assert isinstance(container, WorkflowTaskContainer)
        session_state = CurrentSessionState(
            data_security_mode=container.data_security_mode,
            named_parameters=container.named_parameters,
            spark_conf=container.spark_conf,
            dbr_version=container.runtime_version,
        )
        graph = DependencyGraph(root_dependency, None, self._resolver, self._path_lookup, session_state)
        problems = container.build_dependency_graph(graph)
        located_advices: list[LocatedAdvice] = []
        for problem in problems:
            source_path = self._UNKNOWN if problem.is_path_missing() else problem.source_path
            located_advices.append(LocatedAdvice(problem.as_advisory(), source_path))
        return graph, located_advices, session_state

    def _lint_task(
        self,
        task: jobs.Task,
        graph: DependencyGraph,
        session_state: CurrentSessionState,
        linted_paths: set[Path],
    ) -> Iterable[LocatedAdvice]:
        walker = LintingWalker(
            graph, linted_paths, self._path_lookup, task.task_key, session_state, self._migration_index
        )
        yield from walker

    def _collect_task_dfsas(
        self, job: jobs.Job, task: jobs.Task, graph: DependencyGraph, session_state: CurrentSessionState
    ) -> Iterable[DirectFsAccess]:
        # need to add lineage for job/task because walker doesn't register them
        job_id = str(job.job_id)
        job_name = job.settings.name if job.settings and job.settings.name else "<anonymous>"
        for dfsa in DfsaCollectorWalker(graph, set(), self._path_lookup, session_state, self._migration_index):
            atoms = [
                LineageAtom(object_type="WORKFLOW", object_id=job_id, other={"name": job_name}),
                LineageAtom(object_type="TASK", object_id=f"{job_id}/{task.task_key}"),
            ]
            yield dataclasses.replace(dfsa, source_lineage=atoms + dfsa.source_lineage)

    def _collect_task_tables(
        self, job: jobs.Job, task: jobs.Task, graph: DependencyGraph, session_state: CurrentSessionState
    ) -> Iterable[UsedTable]:
        # need to add lineage for job/task because walker doesn't register them
        job_id = str(job.job_id)
        job_name = job.settings.name if job.settings and job.settings.name else "<anonymous>"
        for dfsa in TablesCollectorWalker(graph, set(), self._path_lookup, session_state, self._migration_index):
            atoms = [
                LineageAtom(object_type="WORKFLOW", object_id=job_id, other={"name": job_name}),
                LineageAtom(object_type="TASK", object_id=f"{job_id}/{task.task_key}"),
            ]
            yield dataclasses.replace(dfsa, source_lineage=atoms + dfsa.source_lineage)


class LintingWalker(DependencyGraphWalker[LocatedAdvice]):

    def __init__(
        self,
        graph: DependencyGraph,
        walked_paths: set[Path],
        path_lookup: PathLookup,
        key: str,
        session_state: CurrentSessionState,
        migration_index: TableMigrationIndex,
    ):
        super().__init__(graph, walked_paths, path_lookup)
        self._key = key
        self._session_state = session_state
        self._linter_context = LinterContext(migration_index, session_state)

    def _log_walk_one(self, dependency: Dependency):
        logger.info(f'Linting {self._key} dependency: {dependency}')

    def _process_dependency(
        self, dependency: Dependency, path_lookup: PathLookup, inherited_tree: Tree | None
    ) -> Iterable[LocatedAdvice]:
        # FileLinter determines which file/notebook linter to use
        linter = FileLinter(self._linter_context, path_lookup, self._session_state, dependency.path, inherited_tree)
        for advice in linter.lint():
            yield LocatedAdvice(advice, dependency.path)


T = TypeVar("T", bound=SourceInfo)


class _CollectorWalker(DependencyGraphWalker[T], ABC):

    def __init__(
        self,
        graph: DependencyGraph,
        walked_paths: set[Path],
        path_lookup: PathLookup,
        session_state: CurrentSessionState,
        migration_index: TableMigrationIndex,
    ):
        super().__init__(graph, walked_paths, path_lookup)
        self._session_state = session_state
        self._linter_context = LinterContext(migration_index, session_state)

    def _process_dependency(
        self, dependency: Dependency, path_lookup: PathLookup, inherited_tree: Tree | None
    ) -> Iterable[T]:
        language = file_language(dependency.path)
        if not language:
            logger.warning(f"Unknown language for {dependency.path}")
            return
        cell_language = CellLanguage.of_language(language)
        source = dependency.path.read_text(guess_encoding(dependency.path))
        if is_a_notebook(dependency.path):
            yield from self._collect_from_notebook(source, cell_language, dependency.path, inherited_tree)
        elif dependency.path.is_file():
            yield from self._collect_from_source(source, cell_language, dependency.path, inherited_tree)

    def _collect_from_notebook(
        self, source: str, language: CellLanguage, path: Path, inherited_tree: Tree | None
    ) -> Iterable[T]:
        notebook = Notebook.parse(path, source, language.language)
        src_timestamp = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        src_id = str(path)
        for cell in notebook.cells:
            for item in self._collect_from_source(cell.original_code, cell.language, path, inherited_tree):
                yield item.replace_source(source_id=src_id, source_lineage=self.lineage, source_timestamp=src_timestamp)
            if cell.language is CellLanguage.PYTHON:
                if inherited_tree is None:
                    inherited_tree = Tree.new_module()
                tree = Tree.normalize_and_parse(cell.original_code)
                inherited_tree.append_tree(tree)

    def _collect_from_source(
        self, source: str, language: CellLanguage, path: Path, inherited_tree: Tree | None
    ) -> Iterable[T]:
        iterable: Iterable[T] | None = None
        if language is CellLanguage.SQL:
            iterable = self._collect_from_sql(source)
        if language is CellLanguage.PYTHON:
            iterable = self._collect_from_python(source, inherited_tree)
        if iterable is None:
            logger.warning(f"Language {language.name} not supported yet!")
            return
        src_timestamp = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        src_id = str(path)
        for item in iterable:
            yield item.replace_source(source_id=src_id, source_lineage=self.lineage, source_timestamp=src_timestamp)

    @abstractmethod
    def _collect_from_python(self, source: str, inherited_tree: Tree | None) -> Iterable[T]: ...

    @abstractmethod
    def _collect_from_sql(self, source: str) -> Iterable[T]: ...


class DfsaCollectorWalker(_CollectorWalker[DirectFsAccess]):

    def _collect_from_python(self, source: str, inherited_tree: Tree | None) -> Iterable[DirectFsAccess]:
        collector = self._linter_context.dfsa_collector(Language.PYTHON)
        yield from collector.collect_dfsas(source)

    def _collect_from_sql(self, source: str) -> Iterable[DirectFsAccess]:
        collector = self._linter_context.dfsa_collector(Language.SQL)
        yield from collector.collect_dfsas(source)


class TablesCollectorWalker(_CollectorWalker[UsedTable]):

    def _collect_from_python(self, source: str, inherited_tree: Tree | None) -> Iterable[UsedTable]:
        collector = self._linter_context.tables_collector(Language.PYTHON)
        assert isinstance(collector, PythonSequentialLinter)
        yield from collector.collect_tables(source)

    def _collect_from_sql(self, source: str) -> Iterable[UsedTable]:
        collector = self._linter_context.tables_collector(Language.SQL)
        yield from collector.collect_tables(source)
