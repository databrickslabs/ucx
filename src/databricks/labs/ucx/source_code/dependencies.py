from __future__ import annotations

import abc
import ast
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from databricks.sdk.service.workspace import ObjectType, ObjectInfo, ExportFormat
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.source_code.python_linter import ASTLinter, PythonLinter
from databricks.labs.ucx.source_code.site_packages import SitePackages, SitePackage
from databricks.labs.ucx.source_code.whitelist import Whitelist, UCCompatibility


MISSING_SOURCE_PATH = "<MISSING_SOURCE_PATH>"


@dataclass
class DependencyProblem:
    code: str
    message: str
    source_path: Path = Path(MISSING_SOURCE_PATH)
    start_line: int = -1
    start_col: int = -1
    end_line: int = -1
    end_col: int = -1

    def replace(
        self,
        code: str | None = None,
        message: str | None = None,
        source_path: Path | None = None,
        start_line: int | None = None,
        start_col: int | None = None,
        end_line: int | None = None,
        end_col: int | None = None,
    ) -> DependencyProblem:
        return DependencyProblem(
            code if code is not None else self.code,
            message if message is not None else self.message,
            source_path if source_path is not None else self.source_path,
            start_line if start_line is not None else self.start_line,
            start_col if start_col is not None else self.start_col,
            end_line if end_line is not None else self.end_line,
            end_col if end_col is not None else self.end_col,
        )


class Dependency(abc.ABC):

    def __init__(self, loader: DependencyLoader, path: Path):
        self._loader = loader
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.path == other.path

    def load(self) -> SourceContainer | None:
        return self._loader.load_dependency(self)


class SourceContainer(abc.ABC):

    @abc.abstractmethod
    def build_dependency_graph(self, parent: DependencyGraph) -> None:
        raise NotImplementedError()


class DependencyLoader(abc.ABC):

    @abc.abstractmethod
    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        raise NotImplementedError()

    @abc.abstractmethod
    def is_notebook(self, path: Path) -> bool:
        raise NotImplementedError()


# a DependencyLoader that simply wraps a pre-existing SourceContainer
class WrappingLoader(DependencyLoader):

    def __init__(self, source_container: SourceContainer):
        self._source_container = source_container

    def is_file(self, path: Path) -> bool:
        raise NotImplementedError()  # should never happen

    def is_notebook(self, path: Path) -> bool:
        raise NotImplementedError()  # should never happen

    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        return self._source_container


class LocalFileLoader(DependencyLoader):
    # see https://github.com/databrickslabs/ucx/issues/1499
    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        raise NotImplementedError()

    def is_file(self, path: Path) -> bool:
        raise NotImplementedError()

    def is_notebook(self, path: Path) -> bool:
        raise NotImplementedError()


class NotebookLoader(DependencyLoader, abc.ABC):
    pass


class LocalNotebookLoader(NotebookLoader, LocalFileLoader):
    # see https://github.com/databrickslabs/ucx/issues/1499
    pass


class SitePackageContainer(SourceContainer):

    def __init__(self, file_loader: LocalFileLoader, site_package: SitePackage):
        self._file_loader = file_loader
        self._site_package = site_package

    def build_dependency_graph(self, parent: DependencyGraph) -> None:
        for module_path in self._site_package.module_paths:
            parent.register_dependency(Dependency(self._file_loader, module_path))


class WorkspaceNotebookLoader(NotebookLoader):

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def is_notebook(self, path: Path):
        object_info = self._ws.workspace.get_status(str(path))
        # TODO check error conditions, see https://github.com/databrickslabs/ucx/issues/1361
        return object_info is not None and object_info.object_type is ObjectType.NOTEBOOK

    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        object_info = self._ws.workspace.get_status(str(dependency.path))
        # TODO check error conditions, see https://github.com/databrickslabs/ucx/issues/1361
        return self._load_notebook(object_info)

    def _load_notebook(self, object_info: ObjectInfo) -> SourceContainer:
        # local import to avoid cyclic dependency
        # pylint: disable=import-outside-toplevel, cyclic-import
        from databricks.labs.ucx.source_code.notebook import Notebook

        assert object_info.path is not None
        assert object_info.language is not None
        source = self._load_source(object_info)
        return Notebook.parse(object_info.path, source, object_info.language)

    def _load_source(self, object_info: ObjectInfo) -> str:
        assert object_info.path is not None
        with self._ws.workspace.download(object_info.path, format=ExportFormat.SOURCE) as f:
            return f.read().decode("utf-8")


class DependencyResolver:
    def __init__(
        self,
        whitelist: Whitelist,
        site_packages: SitePackages,
        file_loader: LocalFileLoader,
        notebook_loader: NotebookLoader,
    ):
        self._whitelist = whitelist
        self._site_packages = site_packages
        self._file_loader = file_loader
        self._notebook_loader = notebook_loader
        self._problems: list[DependencyProblem] = []

    @property
    def problems(self):
        return self._problems

    def add_problems(self, problems: list[DependencyProblem]):
        self._problems.extend(problems)

    # TODO problem_collector is tactical, pending https://github.com/databrickslabs/ucx/issues/1421
    def resolve_notebook(
        self, path: Path, problem_collector: Callable[[DependencyProblem], None] | None = None
    ) -> Dependency | None:
        if self._notebook_loader.is_notebook(path):
            return Dependency(self._notebook_loader, path)
        problem = DependencyProblem('dependency-check', f"Notebook not found: {path.as_posix()}")
        if problem_collector:
            problem_collector(problem)
        else:
            self._problems.append(problem)
        return None

    # TODO problem_collector is tactical, pending https://github.com/databrickslabs/ucx/issues/1421
    def resolve_local_file(
        self, path: Path, problem_collector: Callable[[DependencyProblem], None] | None = None
    ) -> Dependency | None:
        if self._file_loader.is_file(path) and not self._file_loader.is_notebook(path):
            return Dependency(self._file_loader, path)
        problem = DependencyProblem('dependency-check', f"File not found: {path.as_posix()}")
        if problem_collector:
            problem_collector(problem)
        else:
            self._problems.append(problem)
        return None

    # TODO problem_collector is tactical, pending https://github.com/databrickslabs/ucx/issues/1421
    def resolve_import(self, name: str, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        if self._is_whitelisted(name):
            return None
        if self._file_loader.is_file(Path(name)):
            return Dependency(self._file_loader, Path(name))
        site_package = self._site_packages[name]
        if site_package is None:
            problem = DependencyProblem(code='dependency-check', message=f"Could not locate import: {name}")
            problem_collector(problem)
            return None
        container = SitePackageContainer(self._file_loader, site_package)
        return Dependency(WrappingLoader(container), Path(name))

    def _is_whitelisted(self, name: str) -> bool:
        compatibility = self._whitelist.compatibility(name)
        # TODO attach compatibility to dependency, see https://github.com/databrickslabs/ucx/issues/1382
        if compatibility is None:
            return False
        if compatibility == UCCompatibility.NONE:
            # TODO move to linter, see https://github.com/databrickslabs/ucx/issues/1527
            self._problems.append(
                DependencyProblem(
                    code="dependency-check",
                    message=f"Use of dependency {name} is deprecated",
                    start_line=0,
                    start_col=0,
                    end_line=0,
                    end_col=0,
                )
            )
        return True


class DependencyGraph:

    def __init__(
        self,
        dependency: Dependency,
        parent: DependencyGraph | None,
        resolver: DependencyResolver,
    ):
        self._dependency = dependency
        self._parent = parent
        self._resolver = resolver
        self._dependencies: dict[Dependency, DependencyGraph] = {}

    @property
    def dependency(self):
        return self._dependency

    @property
    def path(self):
        return self._dependency.path

    def add_problems(self, problems: list[DependencyProblem]):
        problems = [problem.replace(source_path=self.dependency.path) for problem in problems]
        self._resolver.add_problems(problems)

    # TODO problem_collector is tactical, pending https://github.com/databrickslabs/ucx/issues/1421
    def register_notebook(
        self, path: Path, problem_collector: Callable[[DependencyProblem], None]
    ) -> DependencyGraph | None:
        resolved = self._resolver.resolve_notebook(path, problem_collector)
        if resolved is None:
            return None
        return self.register_dependency(resolved)

    # TODO problem_collector is tactical, pending https://github.com/databrickslabs/ucx/issues/1421
    def register_import(
        self, name: str, problem_collector: Callable[[DependencyProblem], None]
    ) -> DependencyGraph | None:
        resolved = self._resolver.resolve_import(name, problem_collector)
        if resolved is None:
            return None
        return self.register_dependency(resolved)

    def register_dependency(self, dependency: Dependency):
        # already registered ?
        child_graph = self._locate_dependency(dependency)
        if child_graph is not None:
            self._dependencies[dependency] = child_graph
            return child_graph
        # nay, create the child graph and populate it
        child_graph = DependencyGraph(dependency, self, self._resolver)
        self._dependencies[dependency] = child_graph
        container = dependency.load()
        if not container:
            return None
        container.build_dependency_graph(child_graph)
        return child_graph

    def _locate_dependency(self, dependency: Dependency) -> DependencyGraph | None:
        return self.locate_dependency(dependency.path)

    def locate_dependency(self, path: Path) -> DependencyGraph | None:
        # need a list since unlike JS, Python won't let you assign closure variables
        found: list[DependencyGraph] = []
        # TODO https://github.com/databrickslabs/ucx/issues/1287
        posix_path = path.as_posix()
        posix_path = posix_path[2:] if posix_path.startswith('./') else posix_path

        def check_registered_dependency(graph):
            # TODO https://github.com/databrickslabs/ucx/issues/1287
            graph_posix_path = graph.path.as_posix()
            if graph_posix_path.startswith('./'):
                graph_posix_path = graph_posix_path[2:]
            if graph_posix_path == posix_path:
                found.append(graph)
                return True
            # TODO remove HORRIBLE hack until we implement https://github.com/databrickslabs/ucx/issues/1421
            if "site-packages" in graph_posix_path and graph_posix_path.endswith(posix_path):
                found.append(graph)
                return True
            return False

        self.root.visit(check_registered_dependency, set())
        return found[0] if len(found) > 0 else None

    @property
    def root(self):
        return self if self._parent is None else self._parent.root

    @property
    def dependencies(self) -> set[Dependency]:
        dependencies: set[Dependency] = set()

        def add_to_dependencies(graph: DependencyGraph) -> bool:
            if graph.dependency in dependencies:
                return True
            dependencies.add(graph.dependency)
            return False

        self.visit(add_to_dependencies, set())
        return dependencies

    @property
    def paths(self) -> set[Path]:
        return {d.path for d in self.dependencies}

    # when visit_node returns True it interrupts the visit
    def visit(self, visit_node: Callable[[DependencyGraph], bool | None], visited: set[Path]) -> bool | None:
        if self.path in visited:
            return False
        visited.add(self.path)
        if visit_node(self):
            return True
        for dependency in self._dependencies.values():
            if dependency.visit(visit_node, visited):
                return True
        return False

    def build_graph_from_python_source(self, python_code: str, problem_collector: Callable[[DependencyProblem], None]):
        linter = ASTLinter.parse(python_code)
        calls = linter.locate(ast.Call, [("run", ast.Attribute), ("notebook", ast.Attribute), ("dbutils", ast.Name)])
        for call in calls:
            assert isinstance(call, ast.Call)
            path = PythonLinter.get_dbutils_notebook_run_path_arg(call)
            if isinstance(path, ast.Constant):
                path = path.value.strip().strip("'").strip('"')
                call_problems: list[DependencyProblem] = []
                self.register_notebook(Path(path), call_problems.append)
                for problem in call_problems:
                    problem = problem.replace(
                        start_line=call.lineno,
                        start_col=call.col_offset,
                        end_line=call.end_lineno or 0,
                        end_col=call.end_col_offset or 0,
                    )
                    problem_collector(problem)
            else:
                problem = DependencyProblem(
                    code='dependency-check',
                    message="Can't check dependency not provided as a constant",
                    start_line=call.lineno,
                    start_col=call.col_offset,
                    end_line=call.end_lineno or 0,
                    end_col=call.end_col_offset or 0,
                )
                problem_collector(problem)
        for pair in PythonLinter.list_import_sources(linter):
            import_problems: list[DependencyProblem] = []
            self.register_import(pair[0], import_problems.append)
            node = pair[1]
            for problem in import_problems:
                problem = problem.replace(
                    start_line=node.lineno,
                    start_col=node.col_offset,
                    end_line=node.end_lineno or 0,
                    end_col=node.end_col_offset or 0,
                )
                problem_collector(problem)


class DependencyGraphBuilder:

    def __init__(self, resolver: DependencyResolver):
        self._resolver = resolver

    @property
    def problems(self):
        return self._resolver.problems

    def build_local_file_dependency_graph(self, path: Path) -> DependencyGraph | None:
        dependency = self._resolver.resolve_local_file(path)
        if dependency is None:
            return None
        graph = DependencyGraph(dependency, None, self._resolver)
        container = dependency.load()
        if container is not None:
            container.build_dependency_graph(graph)
        return graph

    def build_notebook_dependency_graph(self, path: Path) -> DependencyGraph | None:
        dependency = self._resolver.resolve_notebook(path)
        if dependency is None:
            return None
        graph = DependencyGraph(dependency, None, self._resolver)
        container = dependency.load()
        if container is not None:
            container.build_dependency_graph(graph)
        return graph
