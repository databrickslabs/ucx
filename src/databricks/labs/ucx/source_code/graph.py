from __future__ import annotations

import abc
import dataclasses
import itertools
import logging
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable, Iterable, Iterator
from typing import TypeVar, Generic

from astroid import (  # type: ignore
    NodeNG,
)
from databricks.labs.ucx.source_code.base import Advisory, CurrentSessionState, is_a_notebook, LineageAtom
from databricks.labs.ucx.source_code.python.python_ast import Tree
from databricks.labs.ucx.source_code.path_lookup import PathLookup

logger = logging.Logger(__name__)


class DependencyGraph:

    def __init__(
        self,
        dependency: Dependency,
        parent: DependencyGraph | None,
        resolver: DependencyResolver,
        path_lookup: PathLookup,
        session_state: CurrentSessionState,
    ):
        self._dependency = dependency
        self._parent = parent
        self._resolver = resolver
        self._path_lookup = path_lookup.change_directory(dependency.path.parent)
        self._session_state = session_state
        self._dependencies: dict[Dependency, DependencyGraph] = {}

    @property
    def path_lookup(self):
        return self._path_lookup

    @property
    def dependency(self):
        return self._dependency

    @property
    def dependencies(self):
        return self._dependencies

    def register_library(self, *libraries: str) -> list[DependencyProblem]:
        return self._resolver.register_library(self.path_lookup, *libraries)

    def register_notebook(self, path: Path, inherit_context: bool) -> list[DependencyProblem]:
        maybe = self._resolver.resolve_notebook(self.path_lookup, path, inherit_context)
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

    def register_file(self, path: Path) -> list[DependencyProblem]:
        maybe = self._resolver.resolve_file(self.path_lookup, path)
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
        child_graph = DependencyGraph(dependency, self, self._resolver, self._path_lookup, self._session_state)
        self._dependencies[dependency] = child_graph
        container = dependency.load(self.path_lookup)
        # TODO: Return either (child) graph OR problems
        if not container:
            problem = DependencyProblem('dependency-register-failed', 'Failed to register dependency', dependency.path)
            return MaybeGraph(child_graph, [problem])
        problems = []
        for problem in container.build_dependency_graph(child_graph):
            if problem.is_path_missing():
                problem = dataclasses.replace(problem, source_path=dependency.path)
            problems.append(problem)
        return MaybeGraph(child_graph, problems)

    def locate_dependency(self, path: Path) -> MaybeGraph:
        # need a list since unlike JS, Python won't let you assign closure variables
        found: list[DependencyGraph] = []

        def check_registered_dependency(graph):
            if graph.dependency.path == path:
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
    def parent(self):
        return self._parent

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
    def root_dependencies(self) -> set[Dependency]:
        roots: set[Dependency] = set()
        children: set[Dependency] = set()

        def add_to_dependencies(graph: DependencyGraph) -> bool:
            dependency = graph.dependency
            # if already encountered we're done
            if dependency in children:
                return False
            # if it's not a real file, then it's not a root
            if not dependency.path.is_file() and not is_a_notebook(dependency.path):
                children.add(dependency)
                return False
            # if it appears more than once then it can't be a root
            if dependency in roots:
                roots.remove(dependency)
                children.add(dependency)
                return False
            # if it has a 'real' parent, it's a child
            parent_graph = graph.parent
            while parent_graph is not None:
                dep = parent_graph.dependency
                if dep.path.is_file() or is_a_notebook(dep.path):
                    children.add(dependency)
                    return False
                parent_graph = parent_graph.parent
            # ok, it's a root
            roots.add(dependency)
            return False

        self.visit(add_to_dependencies, None)
        return roots

    @property
    def local_dependencies(self) -> set[Dependency]:
        return {child.dependency for child in self._dependencies.values()}

    def all_relative_names(self) -> set[str]:
        """This method is intended to simplify testing"""
        return self._relative_names(self.all_dependencies)

    def _relative_names(self, dependencies: set[Dependency]) -> set[str]:
        """This method is intended to simplify testing"""
        names = set[str]()
        for dependency in dependencies:
            for library_root in self._path_lookup.library_roots:
                if not dependency.path.is_relative_to(library_root):
                    continue
                relative_path = dependency.path.relative_to(library_root).as_posix()
                if relative_path == ".":
                    continue
                names.add(relative_path)
        return names

    def root_paths(self) -> set[Path]:
        return {d.path for d in self.root_dependencies}

    def root_relative_names(self) -> set[str]:
        """This method is intended to simplify testing"""
        return self._relative_names(self.root_dependencies)

    def visit(self, visit_node: Callable[[DependencyGraph], bool | None], visited: set[Path] | None) -> bool:
        """ "
        when visit_node returns True it interrupts the visit
        provide visited set if you want to ensure nodes are only visited once
        """
        visitor = DependencyGraphVisitor(visit_node, visited)
        return visitor.visit(self)

    def new_dependency_graph_context(self):
        return DependencyGraphContext(
            parent=self, path_lookup=self._path_lookup, resolver=self._resolver, session_state=self._session_state
        )

    def _compute_route(self, root: Path, leaf: Path, visited: set[Path]) -> list[Dependency]:
        """given 2 files or notebooks root and leaf, compute the list of dependencies that must be traversed
        in order to lint the leaf in the context of its parents"""
        try:
            route = self._do_compute_route(root, leaf, visited)
            return self._trim_route(route)
        except ValueError:
            # we only compute routes on graphs so _compute_route can't fail
            # but we return an empty route in case it does
            return []

    def _do_compute_route(self, root: Path, leaf: Path, visited: set[Path]) -> list[Dependency]:
        maybe = self.locate_dependency(root)
        if not maybe.graph:
            logger.warning(f"Could not compute route because dependency {root} cannot be located")
            raise ValueError()
        route: list[Dependency] = []

        def do_compute_route(graph: DependencyGraph) -> bool:
            route.append(graph.dependency)
            for dependency in graph.local_dependencies:
                if dependency.path == leaf:
                    route.append(dependency)
                    return True
            for dependency in graph.local_dependencies:
                sub_route = self._do_compute_route(dependency.path, leaf, visited)
                if len(sub_route) > 0:
                    route.extend(sub_route)
                    return True
            route.pop()
            return False

        maybe.graph.visit(do_compute_route, visited)
        return route

    def _trim_route(self, dependencies: list[Dependency]) -> list[Dependency]:
        """don't inherit context if dependency is not a file/notebook, or it is loaded via dbutils.notebook.run or via import"""
        # filter out intermediate containers
        deps_with_source: list[Dependency] = []
        for dependency in dependencies:
            if dependency.path.is_file() or is_a_notebook(dependency.path):
                deps_with_source.append(dependency)
        # restart when not inheriting context
        for i, dependency in enumerate(deps_with_source):
            if dependency.inherits_context:
                continue
            return [dependency] + self._trim_route(deps_with_source[i + 1 :])
        return deps_with_source

    def build_inherited_tree(self, root: Path, leaf: Path) -> Tree | None:
        return self._build_inherited_context(root, leaf).tree

    def _build_inherited_context(self, root: Path, leaf: Path) -> InheritedContext:
        route = self._compute_route(root, leaf, set())
        return InheritedContext.from_route(self, self.path_lookup, route)

    def __repr__(self):
        return f"<DependencyGraph {self.dependency.path}>"


class DependencyGraphVisitor:

    def __init__(self, visit_node: Callable[[DependencyGraph], bool | None], visited: set[Path] | None):
        self._visit_node = visit_node
        self._visited = visited
        self._visited_pairs: set[tuple[Path, Path]] = set()

    def visit(self, graph: DependencyGraph) -> bool:
        path = graph.dependency.path
        if self._visited is not None:
            if path in self._visited:
                return False
            self._visited.add(path)
        if self._visit_node(graph):
            return True
        for dependency_graph in graph.dependencies.values():
            pair = (path, dependency_graph.dependency.path)
            if pair in self._visited_pairs:
                continue
            self._visited_pairs.add(pair)
            if self.visit(dependency_graph):
                return True
        return False


@dataclass
class DependencyGraphContext:
    parent: DependencyGraph
    path_lookup: PathLookup
    resolver: DependencyResolver
    session_state: CurrentSessionState


class Dependency:

    def __init__(self, loader: DependencyLoader, path: Path, inherits_context=True):
        self._loader = loader
        self._path = path
        self._inherits_context = inherits_context

    @property
    def path(self) -> Path:
        return self._path

    @property
    def inherits_context(self):
        return self._inherits_context

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.path == other.path

    def load(self, path_lookup: PathLookup) -> SourceContainer | None:
        return self._loader.load_dependency(path_lookup, self)

    def __repr__(self):
        return f"Dependency<{self.path}>"

    @property
    def lineage(self) -> list[LineageAtom]:
        object_type = "NOTEBOOK" if is_a_notebook(self.path) else "FILE"
        return [LineageAtom(object_type=object_type, object_id=str(self.path))]


class SourceContainer(abc.ABC):

    @abc.abstractmethod
    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]: ...

    def build_inherited_context(self, graph: DependencyGraph, child_path: Path) -> InheritedContext:
        raise ValueError(f"Building an inherited context from {type(self).__name__} is not supported!")


class DependencyLoader(abc.ABC):
    @abc.abstractmethod
    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None: ...


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
    def resolve_notebook(self, path_lookup: PathLookup, path: Path, inherit_context: bool) -> MaybeDependency: ...

    @staticmethod
    def _fail(code: str, message: str) -> MaybeDependency:
        return MaybeDependency(None, [DependencyProblem(code, message)])


class BaseImportResolver(abc.ABC):

    @abc.abstractmethod
    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        """resolve a simple or composite import name"""


class BaseFileResolver(abc.ABC):

    @abc.abstractmethod
    def resolve_file(self, path_lookup, path: Path) -> MaybeDependency:
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
        file_resolver: BaseFileResolver,
        path_lookup: PathLookup,
    ):
        self._library_resolver = library_resolver
        self._notebook_resolver = notebook_resolver
        self._import_resolver = import_resolver
        self._file_resolver = file_resolver
        self._path_lookup = path_lookup

    def resolve_notebook(self, path_lookup: PathLookup, path: Path, inherit_context: bool) -> MaybeDependency:
        return self._notebook_resolver.resolve_notebook(path_lookup, path, inherit_context)

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        return self._import_resolver.resolve_import(path_lookup, name)

    def resolve_file(self, path_lookup: PathLookup, path: Path) -> MaybeDependency:
        return self._file_resolver.resolve_file(path_lookup, path)

    def register_library(self, path_lookup: PathLookup, *libraries: str) -> list[DependencyProblem]:
        return self._library_resolver.register_library(path_lookup, *libraries)

    def build_local_file_dependency_graph(self, path: Path, session_state: CurrentSessionState) -> MaybeGraph:
        """Builds a dependency graph starting from a file. This method is mainly intended for testing purposes.
        In case of problems, the paths in the problems will be relative to the starting path lookup."""
        maybe = self._file_resolver.resolve_file(self._path_lookup, path)
        if not maybe.dependency:
            return MaybeGraph(None, self._make_relative_paths(maybe.problems, path))
        graph = DependencyGraph(maybe.dependency, None, self, self._path_lookup, session_state)
        container = maybe.dependency.load(graph.path_lookup)
        if container is None:
            problem = DependencyProblem('cannot-load-file', f"Could not load file {path}")
            return MaybeGraph(None, [problem])
        problems = container.build_dependency_graph(graph)
        if problems:
            problems = self._make_relative_paths(problems, path)
        return MaybeGraph(graph, problems)

    def build_notebook_dependency_graph(self, path: Path, session_state: CurrentSessionState) -> MaybeGraph:
        """Builds a dependency graph starting from a notebook. This method is mainly intended for testing purposes.
        In case of problems, the paths in the problems will be relative to the starting path lookup."""
        # the notebook is the root of the graph, so there's no context to inherit
        maybe = self._notebook_resolver.resolve_notebook(self._path_lookup, path, inherit_context=False)
        if not maybe.dependency:
            return MaybeGraph(None, self._make_relative_paths(maybe.problems, path))
        graph = DependencyGraph(maybe.dependency, None, self, self._path_lookup, session_state)
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
            adjusted_problems.append(dataclasses.replace(problem, source_path=out_path))
        return adjusted_problems

    def __repr__(self):
        return f"<DependencyResolver {self._notebook_resolver} {self._import_resolver} {self._file_resolver}, {self._path_lookup}>"


MISSING_SOURCE_PATH = "<MISSING_SOURCE_PATH>"


@dataclass
class DependencyProblem:
    code: str
    message: str
    source_path: Path = Path(MISSING_SOURCE_PATH)
    # Lines and columns are both 0-based: the first line is line 0.
    start_line: int = -1
    start_col: int = -1
    end_line: int = -1
    end_col: int = -1

    def is_path_missing(self):
        return self.source_path == Path(MISSING_SOURCE_PATH)

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
            end_col=(node.end_col_offset or 0),
        )


@dataclass
class MaybeGraph:
    graph: DependencyGraph | None
    problems: list[DependencyProblem]

    @property
    def failed(self):
        return len(self.problems) > 0


class InheritedContext:

    @classmethod
    def from_route(cls, graph: DependencyGraph, path_lookup: PathLookup, route: list[Dependency]) -> InheritedContext:
        context = InheritedContext(None, False)
        for i, dependency in enumerate(route):
            if i >= len(route) - 1:
                break
            next_path = route[i + 1].path
            container = dependency.load(path_lookup)
            if container is None:
                logger.warning(f"Could not load content of {dependency.path}")
                return context
            local = container.build_inherited_context(graph, next_path)
            # only copy 'found' flag if this is the last parent
            context = context.append(local, i == len(route) - 2)
        return context.finalize()

    def __init__(self, tree: Tree | None, found: bool):
        self._tree = tree
        self._found = found

    @property
    def tree(self):
        return self._tree

    @property
    def found(self):
        return self._found

    def append(self, context: InheritedContext, copy_found: bool) -> InheritedContext:
        # we should never append to a found context
        if self.found:
            raise ValueError("Appending to an already resolved InheritedContext is illegal!")
        tree = context.tree
        found = copy_found and context.found
        if tree is None:
            return InheritedContext(self._tree, found)
        if self._tree is None:
            self._tree = Tree.new_module()
        self._tree.append_tree(context.tree)
        return InheritedContext(self._tree, found)

    def finalize(self) -> InheritedContext:
        # hacky stuff for aligning with Astroid's inference engine behavior
        # the engine checks line numbers to skip variables that are not in scope of the current frame
        # see https://github.com/pylint-dev/astroid/blob/5b665e7e760a7181625a24b3635e9fec7b174d87/astroid/filter_statements.py#L113
        # this is problematic when linting code fragments that refer to inherited code with unrelated line numbers
        # here we fool the engine by pretending that all nodes from context have negative line numbers
        if self._tree is None:
            return self
        tree = self._tree.renumber(-1)
        return InheritedContext(tree, self.found)


T = TypeVar("T")


class DependencyGraphWalker(abc.ABC, Generic[T]):

    def __init__(self, graph: DependencyGraph, walked_paths: set[Path], path_lookup: PathLookup):
        self._graph = graph
        self._walked_paths = walked_paths
        self._path_lookup = path_lookup
        self._lineage: list[Dependency] = []

    def __iter__(self) -> Iterator[T]:
        for dependency in self._graph.root_dependencies:
            # the dependency is a root, so its path is the one to use
            # for computing lineage and building python global context
            root_path = dependency.path
            yield from self._iter_one(dependency, self._graph, root_path)

    def _iter_one(self, dependency: Dependency, graph: DependencyGraph, root_path: Path) -> Iterable[T]:
        if dependency.path in self._walked_paths:
            return
        self._lineage.append(dependency)
        self._walked_paths.add(dependency.path)
        self._log_walk_one(dependency)
        if dependency.path.is_file() or is_a_notebook(dependency.path):
            inherited_tree = graph.root.build_inherited_tree(root_path, dependency.path)
            path_lookup = self._path_lookup.change_directory(dependency.path.parent)
            yield from self._process_dependency(dependency, path_lookup, inherited_tree)
            maybe_graph = graph.locate_dependency(dependency.path)
            # missing graph problems have already been reported while building the graph
            if maybe_graph.graph:
                child_graph = maybe_graph.graph
                for child_dependency in child_graph.local_dependencies:
                    yield from self._iter_one(child_dependency, child_graph, root_path)
        self._lineage.pop()

    def _log_walk_one(self, dependency: Dependency):
        logger.debug(f'Analyzing dependency: {dependency}')

    @abc.abstractmethod
    def _process_dependency(
        self, dependency: Dependency, path_lookup: PathLookup, inherited_tree: Tree | None
    ) -> Iterable[T]: ...

    @property
    def lineage(self) -> list[LineageAtom]:
        lists: list[list[LineageAtom]] = [dependency.lineage for dependency in self._lineage]
        return list(itertools.chain(*lists))
