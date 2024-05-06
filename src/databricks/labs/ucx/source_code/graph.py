from __future__ import annotations

import abc
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable, Iterable

from databricks.labs.ucx.source_code.python_linter import (
    ASTLinter,
    PythonLinter,
    SysPathChange,
    NotebookRunCall,
    ImportSource,
)
from databricks.labs.ucx.source_code.syspath_lookup import SysPathLookup


class DependencyGraph:

    def __init__(
        self,
        dependency: Dependency,
        parent: DependencyGraph | None,
        resolver: DependencyResolver,
        syspath_lookup: SysPathLookup,
    ):
        self._dependency = dependency
        self._parent = parent
        self._resolver = resolver
        self._path_lookup = syspath_lookup
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

    def register_notebook(self, path: Path) -> MaybeGraph:
        maybe = self._resolver.resolve_notebook(path)
        if maybe.dependency is None:
            return MaybeGraph(None, maybe.problems)
        return self.register_dependency(maybe.dependency)

    def register_import(self, name: str) -> MaybeGraph:
        maybe = self._resolver.resolve_import(name)
        if maybe.dependency is None:
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
        container = dependency.load()
        if not container:
            problem = DependencyProblem('dependency-register-failed', 'Failed to analyze dependency', dependency.path)
            return MaybeGraph(None, [problem])
        problems = container.build_dependency_graph(child_graph, self._path_lookup)
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
        graph = found[0] if len(found) > 0 else None
        return MaybeGraph(graph, [])

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
        return {d.path for d in self.all_dependencies}

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

    def build_graph_from_python_source(self, python_code: str, syspath_lookup: SysPathLookup) -> MaybeGraph:
        problems: list[DependencyProblem] = []
        linter = ASTLinter.parse(python_code)
        syspath_changes = PythonLinter.list_sys_path_changes(linter)
        run_calls = PythonLinter.list_dbutils_notebook_run_calls(linter)
        import_sources = PythonLinter.list_import_sources(linter)
        # need to execute things in intertwined sequence so concat and sort
        nodes = syspath_changes + run_calls + import_sources
        nodes.sort(key=lambda node: node.node.lineno * 10000 + node.node.col_offset)
        for base_node in nodes:
            if isinstance(base_node, SysPathChange):
                path = Path(base_node.path)
                if not path.is_absolute():
                    path = syspath_lookup.cwd / path
                if base_node.is_append:
                    syspath_lookup.append_path(path)
                    continue
                syspath_lookup.prepend_path(path)
                continue
            if isinstance(base_node, NotebookRunCall):
                strpath = base_node.get_constant_path()
                if strpath is None:
                    problem = DependencyProblem(
                        code='dependency-not-constant',
                        message="Can't check dependency not provided as a constant",
                        start_line=base_node.node.lineno,
                        start_col=base_node.node.col_offset,
                        end_line=base_node.node.end_lineno or 0,
                        end_col=base_node.node.end_col_offset or 0,
                    )
                    problems.append(problem)
                    continue
                maybe = self.register_notebook(Path(strpath))
                for problem in maybe.problems:
                    problem = problem.replace(
                        start_line=base_node.node.lineno,
                        start_col=base_node.node.col_offset,
                        end_line=base_node.node.end_lineno or 0,
                        end_col=base_node.node.end_col_offset or 0,
                    )
                    problems.append(problem)
                    continue
            if isinstance(base_node, ImportSource):
                maybe = self.register_import(base_node.name)
                for problem in maybe.problems:
                    problem = problem.replace(
                        start_line=base_node.node.lineno,
                        start_col=base_node.node.col_offset,
                        end_line=base_node.node.end_lineno or 0,
                        end_col=base_node.node.end_col_offset or 0,
                    )
                    problems.append(problem)
                    continue
        return MaybeGraph(self, problems)


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
    def build_dependency_graph(self, parent: DependencyGraph, syspath_lookup: SysPathLookup) -> list[DependencyProblem]:
        raise NotImplementedError()


class DependencyLoader(abc.ABC):

    @abc.abstractmethod
    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        raise NotImplementedError()

    @abc.abstractmethod
    def is_notebook(self, path: Path) -> bool:
        raise NotImplementedError()


class WrappingLoader(DependencyLoader):

    def __init__(self, source_container: SourceContainer):
        self._source_container = source_container

    def is_notebook(self, path: Path) -> bool:
        raise NotImplementedError()  # should never happen

    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        return self._source_container


class BaseDependencyResolver(abc.ABC):

    def __init__(self, next_resolver: BaseDependencyResolver | None):
        self._next_resolver = next_resolver
        self._problems: list[DependencyProblem] = []

    @abc.abstractmethod
    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        raise NotImplementedError()

    @property
    def problems(self):
        return self._problems

    def add_problems(self, problems: list[DependencyProblem]):
        self._problems.extend(problems)

    @property
    def next_resolver(self):
        return self._next_resolver

    def resolve_notebook(self, path: Path) -> MaybeDependency:
        assert self._next_resolver is not None
        return self._next_resolver.resolve_notebook(path)

    def resolve_local_file(self, path: Path) -> MaybeDependency:
        assert self._next_resolver is not None
        return self._next_resolver.resolve_local_file(path)

    def resolve_import(self, name: str) -> MaybeDependency:
        assert self._next_resolver is not None
        return self._next_resolver.resolve_import(name)


class StubResolver(BaseDependencyResolver):

    def __init__(self):
        super().__init__(None)

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        raise NotImplementedError("Should never happen!")

    def resolve_notebook(self, path: Path) -> MaybeDependency:
        return MaybeDependency(None, [])

    def resolve_local_file(self, path: Path) -> MaybeDependency:
        return MaybeDependency(None, [])

    def resolve_import(self, name: str) -> MaybeDependency:
        return MaybeDependency(None, [])


@dataclass
class MaybeGraph:
    graph: DependencyGraph | None
    problems: list[DependencyProblem]


@dataclass
class MaybeDependency:
    dependency: Dependency | None
    problems: list[DependencyProblem]


class DependencyResolver:

    def __init__(self, resolvers: list[BaseDependencyResolver], syspath_lookup: SysPathLookup):
        previous: BaseDependencyResolver = StubResolver()
        for resolver in resolvers:
            resolver = resolver.with_next_resolver(previous)
            previous = resolver
        self._resolver: BaseDependencyResolver = previous
        self._syspath_lookup = syspath_lookup

    def build_local_file_dependency_graph(self, path: Path) -> MaybeGraph:
        maybe = self.resolve_local_file(path)
        if maybe.dependency is None:
            return MaybeGraph(None, maybe.problems)
        problems = maybe.problems
        graph = DependencyGraph(maybe.dependency, None, self, self._syspath_lookup)
        container = maybe.dependency.load()
        if container is not None:
            more_problems = container.build_dependency_graph(graph, self._syspath_lookup)
            problems.extend(more_problems)
        return MaybeGraph(graph, problems)

    def build_notebook_dependency_graph(self, path: Path) -> MaybeGraph:
        maybe = self.resolve_notebook(path)
        if maybe.dependency is None:
            return MaybeGraph(None, maybe.problems)
        problems = maybe.problems
        graph = DependencyGraph(maybe.dependency, None, self, self._syspath_lookup)
        container = maybe.dependency.load()
        if container is not None:
            more_problems = container.build_dependency_graph(graph, self._syspath_lookup)
            problems.extend(more_problems)
        return MaybeGraph(graph, problems)

    def resolve_notebook(self, path: Path) -> MaybeDependency:
        maybe = self._resolver.resolve_notebook(path)
        if maybe.dependency is None:
            problem = DependencyProblem('notebook-not-found', f"Notebook not found: {path.as_posix()}")
            return MaybeDependency(None, maybe.problems + [problem])
        return maybe

    def resolve_local_file(self, path: Path) -> MaybeDependency:
        maybe = self._resolver.resolve_local_file(path)
        if maybe.dependency is None:
            problem = DependencyProblem('file-not-found', f"File not found: {path.as_posix()}")
            return MaybeDependency(None, maybe.problems + [problem])
        return maybe

    def resolve_import(
        self,
        name: str,
    ) -> MaybeDependency:
        maybe = self._resolver.resolve_import(name)
        if maybe.dependency is None:
            problem = DependencyProblem('import-not-found', f"Could not locate import: {name}")
            return MaybeDependency(None, maybe.problems + [problem])
        return maybe

    @property
    def problems(self) -> Iterable[DependencyProblem]:
        resolver = self._resolver
        while resolver is not None:
            yield from resolver.problems
            resolver = resolver.next_resolver

    def add_problems(self, problems: list[DependencyProblem]):
        self._resolver.add_problems(problems)


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

    def is_path_missing(self):
        return self.source_path == Path(MISSING_SOURCE_PATH)

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
