from __future__ import annotations

import abc
import ast
import sys
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable

from databricks.labs.ucx.source_code.base import Advisory
from databricks.labs.ucx.source_code.python_linter import ASTLinter, PythonLinter
from databricks.labs.ucx.source_code.path_lookup import PathLookup


class DependencyGraph:

    def __init__(
        self,
        dependency: Dependency,
        parent: DependencyGraph | None,
        resolver: DependencyResolver,
        path_lookup: PathLookup,
    ):
        self._dependency = dependency
        self._parent = parent
        self._resolver = resolver
        self._path_lookup = path_lookup.change_directory(dependency.path.parent)
        self._dependencies: dict[Dependency, DependencyGraph] = {}

    @property
    def path_lookup(self):
        return self._path_lookup

    @property
    def dependency(self):
        return self._dependency

    @property
    def path(self):
        return self._dependency.path

    def register_notebook(self, path: Path) -> MaybeGraph:
        maybe = self._resolver.resolve_notebook(self.path_lookup, path)
        if not maybe.dependency:
            return MaybeGraph(None, maybe.problems)
        return self.register_dependency(maybe.dependency)

    def register_import(self, name: str) -> MaybeGraph:
        maybe = self._resolver.resolve_import(self.path_lookup, name)
        if not maybe.dependency:
            return MaybeGraph(None, maybe.problems)
        return self.register_dependency(maybe.dependency)

    def register_dependency(self, dependency: Dependency) -> MaybeGraph:
        # already registered ?
        maybe = self.locate_dependency(dependency.path)
        if maybe.graph is not None:
            self._dependencies[dependency] = maybe.graph
            return maybe
        # nay, create the child graph and populate it
        child_graph = DependencyGraph(dependency, self, self._resolver, self._path_lookup)
        self._dependencies[dependency] = child_graph
        container = dependency.load(self.path_lookup)
        if not container:
            problem = DependencyProblem('dependency-register-failed', 'Failed to register dependency', dependency.path)
            return MaybeGraph(child_graph, [problem])
        problems = container.build_dependency_graph(child_graph)
        return MaybeGraph(child_graph, problems)

    def locate_dependency(self, path: Path) -> MaybeGraph:
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
            return False

        self.root.visit(check_registered_dependency, set())
        if not found:
            return MaybeGraph(None, [DependencyProblem('dependency-not-found', 'Dependency not found')])
        return MaybeGraph(found[0], [])

    @property
    def root(self):
        return self if self._parent is None else self._parent.root

    @property
    def all_dependencies(self) -> set[Dependency]:
        dependencies: set[Dependency] = set()

        def add_to_dependencies(graph: DependencyGraph) -> bool:
            if graph.dependency in dependencies:
                return True
            dependencies.add(graph.dependency)
            return False

        self.visit(add_to_dependencies, set())
        return dependencies

    @property
    def local_dependencies(self) -> set[Dependency]:
        return {child.dependency for child in self._dependencies.values()}

    @property
    def all_paths(self) -> set[Path]:
        # TODO: remove this public method, as it'll throw false positives
        # for package imports, like certifi.
        return {d.path for d in self.all_dependencies}

    # when visit_node returns True it interrupts the visit
    def visit(self, visit_node: Callable[[DependencyGraph], bool | None], visited: set[Path]) -> bool:
        if self.path in visited:
            return False
        visited.add(self.path)
        if visit_node(self):
            return True
        for dependency in self._dependencies.values():
            if dependency.visit(visit_node, visited):
                return True
        return False

    def build_graph_from_python_source(self, python_code: str) -> MaybeGraph:
        linter = ASTLinter.parse(python_code)
        problems: list[DependencyProblem] = []
        run_notebook_calls = PythonLinter.list_dbutils_notebook_run_calls(linter)
        for call in run_notebook_calls:
            assert isinstance(call, ast.Call)
            notebook_path_arg = PythonLinter.get_dbutils_notebook_run_path_arg(call)
            if isinstance(notebook_path_arg, ast.Constant):
                notebook_path = notebook_path_arg.value
                maybe = self.register_notebook(Path(notebook_path))
                for problem in maybe.problems:
                    problem = problem.replace(
                        start_line=call.lineno,
                        start_col=call.col_offset,
                        end_line=call.end_lineno or 0,
                        end_col=call.end_col_offset or 0,
                    )
                    problems.append(problem)
                continue
            problem = DependencyProblem(
                code='dependency-not-constant',
                message="Can't check dependency not provided as a constant",
                start_line=call.lineno,
                start_col=call.col_offset,
                end_line=call.end_lineno or 0,
                end_col=call.end_col_offset or 0,
            )
            problems.append(problem)
        for import_name, node in PythonLinter.list_import_sources(linter):
            if import_name.split('.')[0] in sys.stdlib_module_names:
                # we don't need to register stdlib imports
                continue
            maybe = self.register_import(import_name)
            for problem in maybe.problems:
                problem = problem.replace(
                    start_line=node.lineno,
                    start_col=node.col_offset,
                    end_line=node.end_lineno or 0,
                    end_col=node.end_col_offset or 0,
                )
                problems.append(problem)
        return MaybeGraph(self, problems)

    def __repr__(self):
        return f"<DependencyGraph {self.path}>"


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

    def load(self, path_lookup: PathLookup) -> SourceContainer | None:
        return self._loader.load_dependency(path_lookup, self)

    def __repr__(self):
        return f"Dependency<{self.path}>"


class SourceContainer(abc.ABC):

    @abc.abstractmethod
    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        raise NotImplementedError()


class DependencyLoader(abc.ABC):
    @abc.abstractmethod
    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        raise NotImplementedError()


class WrappingLoader(DependencyLoader):

    def __init__(self, source_container: SourceContainer):
        self._source_container = source_container

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        return self._source_container

    def __repr__(self):
        return f"<WrappingLoader source_container={self._source_container}>"


class BaseDependencyResolver(abc.ABC):

    def __init__(self, next_resolver: BaseDependencyResolver | None):
        self._next_resolver = next_resolver

    @abc.abstractmethod
    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        raise NotImplementedError()

    @property
    def next_resolver(self):
        return self._next_resolver

    def resolve_notebook(self, path_lookup: PathLookup, path: Path) -> MaybeDependency:
        # TODO: remove StubResolver and return MaybeDependency(None, [...])
        assert self._next_resolver is not None
        return self._next_resolver.resolve_notebook(path_lookup, path)

    def resolve_local_file(self, path: Path) -> MaybeDependency:
        assert self._next_resolver is not None
        return self._next_resolver.resolve_local_file(path)

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        # TODO: remove StubResolver and return MaybeDependency(None, [...])
        assert self._next_resolver is not None
        return self._next_resolver.resolve_import(path_lookup, name)


class StubResolver(BaseDependencyResolver):

    def __init__(self):
        super().__init__(None)

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        raise NotImplementedError("Should never happen!")

    def resolve_notebook(self, path_lookup: PathLookup, path: Path) -> MaybeDependency:
        return self._fail('notebook-not-found', f"Notebook not found: {path.as_posix()}")

    def resolve_local_file(self, path: Path) -> MaybeDependency:
        return self._fail('file-not-found', f"File not found: {path.as_posix()}")

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        return self._fail('import-not-found', f"Could not locate import: {name}")

    @staticmethod
    def _fail(code: str, message: str):
        return MaybeDependency(None, [DependencyProblem(code, message)])


@dataclass
class MaybeDependency:
    dependency: Dependency | None
    problems: list[DependencyProblem]


class DependencyResolver:
    def __init__(self, resolvers: list[BaseDependencyResolver]):
        previous: BaseDependencyResolver = StubResolver()
        for resolver in resolvers:
            resolver = resolver.with_next_resolver(previous)
            previous = resolver
        self._resolver: BaseDependencyResolver = previous

    def resolve_notebook(self, path_lookup: PathLookup, path: Path) -> MaybeDependency:
        return self._resolver.resolve_notebook(path_lookup, path)

    def resolve_local_file(self, path: Path) -> MaybeDependency:
        return self._resolver.resolve_local_file(path)

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        return self._resolver.resolve_import(path_lookup, name)

    def __repr__(self):
        return f"<DependencyResolver {self._resolver}>"


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

    def as_advisory(self) -> 'Advisory':
        return Advisory(
            code=self.code,
            message=self.message,
            start_line=self.start_line,
            start_col=self.start_col,
            end_line=self.end_line,
            end_col=self.end_col,
        )


@dataclass
class MaybeGraph:
    graph: DependencyGraph | None
    problems: list[DependencyProblem]

    @property
    def failed(self):
        return len(self.problems) > 0


class DependencyGraphBuilder:

    def __init__(self, resolver: DependencyResolver, path_lookup: PathLookup):
        self._resolver = resolver
        self._path_lookup = path_lookup

    def build_local_file_dependency_graph(self, path: Path) -> MaybeGraph:
        maybe = self._resolver.resolve_local_file(path)
        if not maybe.dependency:
            return MaybeGraph(None, maybe.problems)
        graph = DependencyGraph(maybe.dependency, None, self._resolver, self._path_lookup)
        container = maybe.dependency.load(graph.path_lookup)
        if container is not None:
            problems = container.build_dependency_graph(graph)
            if problems:
                return MaybeGraph(None, problems)
        return MaybeGraph(graph, [])

    def build_notebook_dependency_graph(self, path: Path) -> MaybeGraph:
        maybe = self._resolver.resolve_notebook(self._path_lookup, path)
        if not maybe.dependency:
            return MaybeGraph(None, maybe.problems)
        graph = DependencyGraph(maybe.dependency, None, self._resolver, self._path_lookup)
        container = maybe.dependency.load(graph.path_lookup)
        if container is not None:
            problems = container.build_dependency_graph(graph)
            if problems:
                return MaybeGraph(None, problems)
        return MaybeGraph(graph, [])

    def __repr__(self):
        return f"<DependencyGraphBuilder resolver={self._resolver}, path_lookup={self._path_lookup}>"
