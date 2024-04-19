from __future__ import annotations

import abc
from collections.abc import Callable, Iterable

from databricks.sdk.service.workspace import ObjectType, ObjectInfo, ExportFormat, Language
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.source_code.base import Advice, Deprecation
from databricks.labs.ucx.source_code.whitelist import Whitelist, UCCompatibility


class Dependency:

    @staticmethod
    def from_object_info(object_info: ObjectInfo):
        assert object_info.object_type is not None
        assert object_info.path is not None
        return Dependency(object_info.object_type, object_info.path)

    def __init__(self, object_type: ObjectType | None, path: str):
        self._type = object_type
        self._path = path

    @property
    def type(self) -> ObjectType | None:
        return self._type

    @property
    def path(self) -> str:
        return self._path

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        return isinstance(other, Dependency) and self.path == other.path


class SourceContainer(abc.ABC):

    @property
    @abc.abstractmethod
    def object_type(self) -> ObjectType:
        raise NotImplementedError()

    @abc.abstractmethod
    def build_dependency_graph(self, graph) -> None:
        raise NotImplementedError()


class DependencyLoader:

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        object_info = self._load_object(dependency)
        if object_info.object_type is ObjectType.NOTEBOOK:
            return self._load_notebook(object_info)
        if object_info.object_type is ObjectType.FILE:
            return self._load_file(object_info)
        if object_info.object_type in [ObjectType.LIBRARY, ObjectType.DIRECTORY, ObjectType.DASHBOARD, ObjectType.REPO]:
            return None
        raise NotImplementedError(str(object_info.object_type))

    def _load_object(self, dependency: Dependency) -> ObjectInfo:
        object_info = self._ws.workspace.get_status(dependency.path)
        # TODO check error conditions, see https://github.com/databrickslabs/ucx/issues/1361
        if object_info is None or object_info.object_type is None:
            raise ValueError(f"Could not locate object at '{dependency.path}'")
        if dependency.type is not None and object_info.object_type is not dependency.type:
            raise ValueError(
                f"Invalid object at '{dependency.path}', expected a {str(dependency.type)}, got a {str(object_info.object_type)}"
            )
        return object_info

    def _load_notebook(self, object_info: ObjectInfo) -> SourceContainer:
        # local import to avoid cyclic dependency
        # pylint: disable=import-outside-toplevel, cyclic-import
        from databricks.labs.ucx.source_code.notebook import Notebook

        assert object_info.path is not None
        assert object_info.language is not None
        source = self._load_source(object_info)
        return Notebook.parse(object_info.path, source, object_info.language)

    def _load_file(self, object_info: ObjectInfo) -> SourceContainer:
        # local import to avoid cyclic dependency
        # pylint: disable=import-outside-toplevel, cyclic-import
        from databricks.labs.ucx.source_code.files import WorkspaceFile

        assert object_info.path is not None
        # TODO https://github.com/databrickslabs/ucx/issues/1363
        # the below assumes that the dependency was discovered whilst processing a Python notebook or cell
        # which is safe since Python is the only language supported as of writing
        language = Language.PYTHON if object_info.language is None else object_info.language
        source = self._load_source(object_info)
        return WorkspaceFile(object_info.path, source, language)

    def _load_source(self, object_info: ObjectInfo) -> str:
        if not object_info.path:
            raise ValueError(f"Invalid ObjectInfo: {object_info}")
        with self._ws.workspace.download(object_info.path, format=ExportFormat.SOURCE) as f:
            return f.read().decode("utf-8")


class DependencyResolver:
    def __init__(self, whitelist: Whitelist | None = None):
        self._whitelist = Whitelist() if whitelist is None else whitelist
        self._advices: list[Advice] = []

    def resolve(self, dependency: Dependency) -> Dependency | None:
        if dependency.type == ObjectType.NOTEBOOK:
            return dependency
        compatibility = self._whitelist.compatibility(dependency.path)
        # TODO attach compatibility to dependency, see https://github.com/databrickslabs/ucx/issues/1382
        if compatibility is not None:
            if compatibility == UCCompatibility.NONE:
                self._advices.append(
                    Deprecation(
                        code="dependency-check",
                        message=f"Use of dependency {dependency.path} is deprecated",
                        start_line=0,
                        start_col=0,
                        end_line=0,
                        end_col=0,
                    )
                )
            return None
        return dependency

    def get_advices(self) -> Iterable[Advice]:
        yield from self._advices


class DependencyGraph:

    def __init__(
        self,
        dependency: Dependency,
        parent: DependencyGraph | None,
        loader: DependencyLoader,
        resolver: DependencyResolver | None = None,
    ):
        self._dependency = dependency
        self._parent = parent
        self._loader = loader
        self._resolver = resolver or DependencyResolver()
        self._dependencies: dict[Dependency, DependencyGraph] = {}

    @property
    def dependency(self):
        return self._dependency

    @property
    def path(self):
        return self._dependency.path

    def register_dependency(self, dependency: Dependency) -> DependencyGraph | None:
        resolved = self._resolver.resolve(dependency)
        if resolved is None:
            return None
        # already registered ?
        child_graph = self.locate_dependency(resolved)
        if child_graph is not None:
            self._dependencies[resolved] = child_graph
            return child_graph
        # nay, create the child graph and populate it
        child_graph = DependencyGraph(resolved, self, self._loader, self._resolver)
        self._dependencies[resolved] = child_graph
        container = self._loader.load_dependency(resolved)
        if not container:
            return None
        container.build_dependency_graph(child_graph)
        return child_graph

    def locate_dependency(self, dependency: Dependency) -> DependencyGraph | None:
        # need a list since unlike JS, Python won't let you assign closure variables
        found: list[DependencyGraph] = []
        path = dependency.path
        # TODO https://github.com/databrickslabs/ucx/issues/1287
        path = path[2:] if path.startswith('./') else path

        def check_registered_dependency(graph):
            # TODO https://github.com/databrickslabs/ucx/issues/1287
            graph_path = graph.path[2:] if graph.path.startswith('./') else graph.path
            if graph_path == path:
                found.append(graph)
                return True
            return False

        self.root.visit(check_registered_dependency)
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

        self.visit(add_to_dependencies)
        return dependencies

    @property
    def paths(self) -> set[str]:
        return {d.path for d in self.dependencies}

    # when visit_node returns True it interrupts the visit
    def visit(self, visit_node: Callable[[DependencyGraph], bool | None]) -> bool | None:
        if visit_node(self):
            return True
        for dependency in self._dependencies.values():
            if dependency.visit(visit_node):
                return True
        return False
