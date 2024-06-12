from __future__ import annotations

import abc
import logging
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable

from astroid import ImportFrom, NodeNG  # type: ignore

from databricks.labs.ucx.source_code.base import Advisory
from databricks.labs.ucx.source_code.linters.imports import (
    DbutilsLinter,
    ImportSource,
    NotebookRunCall,
    SysPathChange,
    UnresolvedPath,
)
from databricks.labs.ucx.source_code.linters.python_ast import Tree, NodeBase
from databricks.labs.ucx.source_code.path_lookup import PathLookup

logger = logging.Logger(__name__)


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

    def register_library(self, *libraries: str) -> list[DependencyProblem]:
        return self._resolver.register_library(self.path_lookup, *libraries)

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
        logger.debug(f"Registering dependency {dependency}")
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
        all_names = set[str]()
        dependencies = self.all_dependencies
        for library_root in self._path_lookup.library_roots:
            for dependency in dependencies:
                if not dependency.path.is_relative_to(library_root):
                    continue
                relative_path = dependency.path.relative_to(library_root).as_posix()
                if relative_path == ".":
                    continue
                all_names.add(relative_path)
        return all_names

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
        """Check python code for dependency-related problems.

        Returns:
            A list of dependency problems; position information is relative to the python code itself.
        """
        problems: list[DependencyProblem] = []
        try:
            tree = Tree.parse(python_code)
        except Exception as e:  # pylint: disable=broad-except
            problems.append(DependencyProblem('parse-error', f"Could not parse Python code: {e}"))
            return problems
        syspath_changes = SysPathChange.extract_from_tree(tree)
        run_calls = DbutilsLinter.list_dbutils_notebook_run_calls(tree)
        import_sources: list[ImportSource]
        import_problems: list[DependencyProblem]
        import_sources, import_problems = ImportSource.extract_from_tree(tree, DependencyProblem.from_node)
        problems.extend(import_problems)
        nodes = syspath_changes + run_calls + import_sources
        # need to execute things in intertwined sequence so concat and sort
        for base_node in sorted(nodes, key=lambda node: (node.node.lineno, node.node.col_offset)):
            for problem in self._process_node(base_node):
                # Astroid line numbers are 1-based.
                problem = problem.replace(
                    start_line=base_node.node.lineno - 1,
                    start_col=base_node.node.col_offset,
                    end_line=(base_node.node.end_lineno or 1) - 1,
                    end_col=base_node.node.end_col_offset or 0,
                )
                problems.append(problem)
        return problems

    def _process_node(self, base_node: NodeBase):
        if isinstance(base_node, SysPathChange):
            yield from self._mutate_path_lookup(base_node)
        elif isinstance(base_node, NotebookRunCall):
            yield from self._register_notebook(base_node)
        elif isinstance(base_node, ImportSource):
            yield from self._register_import(base_node)
        else:
            logger.warning(f"Can't process {NodeBase.__name__} of type {type(base_node).__name__}")

    def _register_import(self, base_node: ImportSource):
        prefix = ""
        if isinstance(base_node.node, ImportFrom) and base_node.node.level is not None:
            prefix = "." * base_node.node.level
        name = base_node.name or ""
        yield from self.register_import(prefix + name)

    def _register_notebook(self, base_node: NotebookRunCall):
        has_unresolved, paths = base_node.get_notebook_paths()
        if has_unresolved:
            yield DependencyProblem(
                'dependency-cannot-compute',
                f"Can't check dependency from {base_node.node.as_string()} because the expression cannot be computed",
            )
        for path in paths:
            yield from self.register_notebook(Path(path))

    def _mutate_path_lookup(self, change: SysPathChange):
        if isinstance(change, UnresolvedPath):
            yield DependencyProblem(
                'sys-path-cannot-compute',
                f"Can't update sys.path from {change.node.as_string()} because the expression cannot be computed",
            )
            return
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


class LibraryResolver(abc.ABC):
    @abc.abstractmethod
    def register_library(self, path_lookup: PathLookup, *libraries: str) -> list[DependencyProblem]:
        pass


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
        library_resolver: LibraryResolver,
        notebook_resolver: BaseNotebookResolver,
        import_resolver: BaseImportResolver,
        path_lookup: PathLookup,
    ):
        self._library_resolver = library_resolver
        self._notebook_resolver = notebook_resolver
        self._import_resolver = import_resolver
        self._path_lookup = path_lookup

    def resolve_notebook(self, path_lookup: PathLookup, path: Path) -> MaybeDependency:
        return self._notebook_resolver.resolve_notebook(path_lookup, path)

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        return self._import_resolver.resolve_import(path_lookup, name)

    def register_library(self, path_lookup: PathLookup, *libraries: str) -> list[DependencyProblem]:
        return self._library_resolver.register_library(path_lookup, *libraries)

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

    def __repr__(self):
        return f"<DependencyResolver {self._notebook_resolver} {self._import_resolver} {self._path_lookup}>"


MISSING_SOURCE_PATH = "<MISSING_SOURCE_PATH>"


@dataclass
class DependencyProblem:
    code: str
    message: str
    source_path: Path = Path(MISSING_SOURCE_PATH)
    # Lines and colums are both 0-based: the first line is line 0.
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

    @staticmethod
    def from_node(code: str, message: str, node: NodeNG) -> DependencyProblem:
        # Astroid line numbers are 1-based.
        return DependencyProblem(
            code=code,
            message=message,
            start_line=(node.lineno or 1) - 1,
            start_col=node.col_offset,
            end_line=(node.end_lineno or 1) - 1,
            end_col=(node.end_col_offset),
        )


@dataclass
class MaybeGraph:
    graph: DependencyGraph | None
    problems: list[DependencyProblem]

    @property
    def failed(self):
        return len(self.problems) > 0
