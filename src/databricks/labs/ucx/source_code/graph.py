from __future__ import annotations

import abc
import ast
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable
from typing import cast

from databricks.labs.ucx.source_code.base import Advisory
from databricks.labs.ucx.source_code.python_linter import (
    ASTLinter,
    PythonLinter,
    SysPathChange,
    NotebookRunCall,
    ImportSource,
    NodeBase,
)
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

    def register_library(self, library: str) -> list[DependencyProblem]:
        # TODO: https://github.com/databrickslabs/ucx/issues/1643
        # TODO: https://github.com/databrickslabs/ucx/issues/1640
        maybe = self._resolver.resolve_library(self.path_lookup, library)
        if not maybe.dependency:
            return maybe.problems
        maybe_graph = self.register_dependency(maybe.dependency)
        return maybe_graph.problems

    def register_notebook(self, path: Path) -> list[DependencyProblem]:
        maybe = self._resolver.resolve_notebook(self.path_lookup, path)
        if not maybe.dependency:
            return maybe.problems
        maybe_graph = self.register_dependency(maybe.dependency)
        return maybe_graph.problems

    def register_import(self, name: str) -> list[DependencyProblem]:
        if not name:
            return [DependencyProblem('import-empty', 'Empty import name')]
        maybe = self._resolver.resolve_import(self.path_lookup, name)
        if not maybe.dependency:
            return maybe.problems
        maybe_graph = self.register_dependency(maybe.dependency)
        return maybe_graph.problems

    def register_dependency(self, dependency: Dependency) -> MaybeGraph:
        # TODO: this has to be a private method, because we don't want to allow free-form dependencies.
        # the only case we have for this method to be used outside of this class is for DistInfoPackage
        # See databricks.labs.ucx.source_code.python_libraries.DistInfoContainer.build_dependency_graph for reference
        maybe = self.locate_dependency(dependency.path)
        if maybe.graph is not None:
            self._dependencies[dependency] = maybe.graph
            return maybe
        # nay, create the child graph and populate it
        child_graph = DependencyGraph(dependency, self, self._resolver, self._path_lookup)
        self._dependencies[dependency] = child_graph
        container = dependency.load(self.path_lookup)
        # TODO: Return either (child) graph OR problems
        if not container:
            problem = DependencyProblem('dependency-register-failed', 'Failed to register dependency', dependency.path)
            return MaybeGraph(child_graph, [problem])
        problems = container.build_dependency_graph(child_graph)
        return MaybeGraph(
            child_graph,
            [
                problem.replace(
                    source_path=dependency.path if problem.is_path_missing() else problem.source_path,
                )
                for problem in problems
            ],
        )

    def locate_dependency(self, path: Path) -> MaybeGraph:
        # need a list since unlike JS, Python won't let you assign closure variables
        found: list[DependencyGraph] = []

        def check_registered_dependency(graph):
            if graph.path == path:
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
        # for package imports, like certifi. a WorkflowTask is also a dependency,
        # but it does not exist on a filesyste
        return {d.path for d in self.all_dependencies}

    def all_relative_names(self) -> set[str]:
        """This method is intended to simplify testing"""
        return {d.path.relative_to(self._path_lookup.cwd).as_posix() for d in self.all_dependencies}

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

    def build_graph_from_python_source(self, python_code: str) -> list[DependencyProblem]:
        problems: list[DependencyProblem] = []
        linter = ASTLinter.parse(python_code)
        syspath_changes = PythonLinter.list_sys_path_changes(linter)
        run_calls = PythonLinter.list_dbutils_notebook_run_calls(linter)
        import_sources, import_problems = PythonLinter.list_import_sources(linter, DependencyProblem)
        problems.extend(cast(list[DependencyProblem], import_problems))
        nodes = syspath_changes + run_calls + import_sources
        # need to execute things in intertwined sequence so concat and sort
        for base_node in sorted(nodes, key=lambda node: node.node.lineno * 10000 + node.node.col_offset):
            for problem in self._process_node(base_node):
                problem = problem.replace(
                    start_line=base_node.node.lineno,
                    start_col=base_node.node.col_offset,
                    end_line=base_node.node.end_lineno or 0,
                    end_col=base_node.node.end_col_offset or 0,
                )
                problems.append(problem)
        return problems

    def _process_node(self, base_node: NodeBase):
        if isinstance(base_node, SysPathChange):
            self._mutate_path_lookup(base_node)
        if isinstance(base_node, NotebookRunCall):
            strpath = base_node.get_constant_path()
            if strpath is None:
                yield DependencyProblem('dependency-not-constant', "Can't check dependency not provided as a constant")
            else:
                yield from self.register_notebook(Path(strpath))
        if isinstance(base_node, ImportSource):
            prefix = ("." * base_node.node.level) if isinstance(base_node.node, ast.ImportFrom) else ""
            name = base_node.name or ""
            yield from self.register_import(prefix + name)

    def _mutate_path_lookup(self, change: SysPathChange):
        path = Path(change.path)
        if not path.is_absolute():
            path = self._path_lookup.cwd / path
        if change.is_append:
            self._path_lookup.append_path(path)
            return
        self._path_lookup.prepend_path(path)

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
        """builds a dependency graph from the contents of this container"""


class DependencyLoader(abc.ABC):
    @abc.abstractmethod
    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        """loads a dependency"""


class WrappingLoader(DependencyLoader):

    def __init__(self, source_container: SourceContainer):
        self._source_container = source_container

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        return self._source_container

    def __repr__(self):
        return f"<WrappingLoader source_container={self._source_container}>"


class BaseLibraryResolver(abc.ABC):

    def __init__(self, next_resolver: BaseLibraryResolver | None):
        self._next_resolver = next_resolver

    @abc.abstractmethod
    def with_next_resolver(self, resolver: BaseLibraryResolver) -> BaseLibraryResolver:
        """required to create a linked list of resolvers"""

    def resolve_library(self, path_lookup: PathLookup, library: Path) -> MaybeDependency:
        assert self._next_resolver is not None
        return self._next_resolver.resolve_library(path_lookup, library)


class BaseNotebookResolver(abc.ABC):

    @abc.abstractmethod
    def resolve_notebook(self, path_lookup: PathLookup, path: Path) -> MaybeDependency:
        """locates a notebook"""

    @staticmethod
    def _fail(code: str, message: str) -> MaybeDependency:
        return MaybeDependency(None, [DependencyProblem(code, message)])


class BaseImportResolver(abc.ABC):

    @abc.abstractmethod
    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        """resolve a simple or composite import name"""


class BaseFileResolver(abc.ABC):

    @abc.abstractmethod
    def resolve_local_file(self, path_lookup, path: Path) -> MaybeDependency:
        """locates a file"""


class StubLibraryResolver(BaseLibraryResolver):

    def __init__(self):
        super().__init__(None)

    def with_next_resolver(self, resolver: BaseLibraryResolver) -> BaseLibraryResolver:
        raise NotImplementedError("Should never happen!")

    def resolve_library(self, path_lookup: PathLookup, library: Path) -> MaybeDependency:
        return self._fail('library-not-found', f"Could not resolve library: {library.as_posix()}")

    @staticmethod
    def _fail(code: str, message: str):
        return MaybeDependency(None, [DependencyProblem(code, message)])


@dataclass
class MaybeDependency:
    dependency: Dependency | None
    problems: list[DependencyProblem]


class DependencyResolver:
    """the DependencyResolver is responsible for locating "stuff", mimicking the Databricks runtime behavior.
    There are specific resolution algorithms for import and for Notebooks (executed via %run or dbutils.notebook.run)
    so we're using 2 distinct resolvers for notebooks (instance) and imports (linked list of specialized sub-resolvers)
    resolving imports is affected by cwd and sys.paths, the latter being itself influenced by Python code
    we therefore need a PathLookup to convey these during import resolution
    """

    def __init__(
        self,
        library_resolvers: list[BaseLibraryResolver],
        notebook_resolver: BaseNotebookResolver,
        import_resolver: BaseImportResolver,
        path_lookup: PathLookup,
    ):
        self._library_resolver = self._chain_library_resolvers(library_resolvers)
        self._notebook_resolver = notebook_resolver
        self._import_resolver = import_resolver
        self._path_lookup = path_lookup

    @staticmethod
    def _chain_library_resolvers(library_resolvers: list[BaseLibraryResolver]) -> BaseLibraryResolver:
        previous: BaseLibraryResolver = StubLibraryResolver()
        for resolver in library_resolvers:
            resolver = resolver.with_next_resolver(previous)
            previous = resolver
        return previous

    def resolve_notebook(self, path_lookup: PathLookup, path: Path) -> MaybeDependency:
        return self._notebook_resolver.resolve_notebook(path_lookup, path)

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        return self._import_resolver.resolve_import(path_lookup, name)

    def resolve_library(self, path_lookup: PathLookup, library: str) -> MaybeDependency:
        return self._library_resolver.resolve_library(path_lookup, Path(library))

    def build_local_file_dependency_graph(self, path: Path) -> MaybeGraph:
        """Builds a dependency graph starting from a file. This method is mainly intended for testing purposes.
        In case of problems, the paths in the problems will be relative to the starting path lookup."""
        resolver = self._local_file_resolver
        if not resolver:
            problem = DependencyProblem("missing-file-resolver", "Missing resolver for local files")
            return MaybeGraph(None, [problem])
        maybe = resolver.resolve_local_file(self._path_lookup, path)
        if not maybe.dependency:
            return MaybeGraph(None, self._make_relative_paths(maybe.problems, path))
        graph = DependencyGraph(maybe.dependency, None, self, self._path_lookup)
        container = maybe.dependency.load(graph.path_lookup)
        if container is None:
            problem = DependencyProblem('cannot-load-file', f"Could not load file {path}")
            return MaybeGraph(None, [problem])
        problems = container.build_dependency_graph(graph)
        if problems:
            problems = self._make_relative_paths(problems, path)
        return MaybeGraph(graph, problems)

    @property
    def _local_file_resolver(self) -> BaseFileResolver | None:
        if isinstance(self._import_resolver, BaseFileResolver):
            return self._import_resolver
        return None

    def build_notebook_dependency_graph(self, path: Path) -> MaybeGraph:
        """Builds a dependency graph starting from a notebook. This method is mainly intended for testing purposes.
        In case of problems, the paths in the problems will be relative to the starting path lookup."""
        maybe = self._notebook_resolver.resolve_notebook(self._path_lookup, path)
        if not maybe.dependency:
            return MaybeGraph(None, self._make_relative_paths(maybe.problems, path))
        graph = DependencyGraph(maybe.dependency, None, self, self._path_lookup)
        container = maybe.dependency.load(graph.path_lookup)
        if container is None:
            problem = DependencyProblem('cannot-load-notebook', f"Could not load notebook {path}")
            return MaybeGraph(None, [problem])
        problems = container.build_dependency_graph(graph)
        if problems:
            problems = self._make_relative_paths(problems, path)
        return MaybeGraph(graph, problems)

    def _make_relative_paths(self, problems: list[DependencyProblem], path: Path) -> list[DependencyProblem]:
        adjusted_problems = []
        for problem in problems:
            out_path = path if problem.is_path_missing() else problem.source_path
            if out_path.is_absolute() and out_path.is_relative_to(self._path_lookup.cwd):
                out_path = out_path.relative_to(self._path_lookup.cwd)
            adjusted_problems.append(problem.replace(source_path=out_path))
        return adjusted_problems

    def build_library_dependency_graph(self, path: Path):
        """Builds a dependency graph starting from a library. This method is mainly intended for testing purposes.
        In case of problems, the paths in the problems will be relative to the starting path lookup."""
        maybe = self._library_resolver.resolve_library(self._path_lookup, path)
        if not maybe.dependency:
            return MaybeGraph(None, self._make_relative_paths(maybe.problems, path))
        graph = DependencyGraph(maybe.dependency, None, self, self._path_lookup)
        container = maybe.dependency.load(graph.path_lookup)
        if container is None:
            problem = DependencyProblem('cannot-load-library', f"Could not load library {path}")
            return MaybeGraph(None, [problem])
        problems = container.build_dependency_graph(graph)
        if problems:
            problems = self._make_relative_paths(problems, path)
        return MaybeGraph(graph, problems)

    def __repr__(self):
        return f"<DependencyResolver {self._notebook_resolver} {self._import_resolver} {self._path_lookup}>"


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
