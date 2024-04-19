from __future__ import annotations

import abc
import ast
import typing
from collections.abc import Callable, Iterable

from databricks.sdk.service.workspace import ObjectType, ObjectInfo, ExportFormat, Language
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.source_code.base import Advice, Deprecation
from databricks.labs.ucx.source_code.python_linter import ASTLinter, PythonLinter
from databricks.labs.ucx.source_code.whitelist import Whitelist, UCCompatibility


class Dependency(abc.ABC):

    def __init__(self, path: str):
        self._path = path

    @property
    def path(self) -> str:
        return self._path

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.path == other.path


class UnresolvedDependency(Dependency):
    pass


class ResolvedDependency(Dependency, abc.ABC):

    def __init__(self, loader: DependencyLoader, path: str):
        super().__init__(path)
        self._loader = loader

    def load(self) -> SourceContainer | None:
        return self._loader.load_dependency(self)


class LocalFileDependency(ResolvedDependency):
    pass


class NotebookDependency(ResolvedDependency, abc.ABC):
    pass


class WorkspaceNotebookDependency(NotebookDependency):
    pass


class LocalNotebookDependency(NotebookDependency):
    pass


class SourceContainer(abc.ABC):

    @abc.abstractmethod
    def build_dependency_graph(self, parent: DependencyGraph) -> None:
        raise NotImplementedError()


class DependencyLoader(abc.ABC):

    @abc.abstractmethod
    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        raise NotImplementedError()


class LocalLoader(DependencyLoader):
    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        raise NotImplementedError()

    def is_file(self, path: str) -> bool:
        raise NotImplementedError()


class WorkspaceLoader(DependencyLoader):

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def get_object_info(self, path: str):
        return self._ws.workspace.get_status(path)

    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        object_info = self._load_object(dependency)
        return self._load_notebook(object_info)

    def _load_object(self, dependency: Dependency) -> ObjectInfo:
        object_info = self._ws.workspace.get_status(dependency.path)
        # TODO check error conditions, see https://github.com/databrickslabs/ucx/issues/1361
        if object_info is None or object_info.object_type is None:
            raise ValueError(f"Could not locate object at '{dependency.path}'")
        if object_info.object_type is not ObjectType.NOTEBOOK:
            raise ValueError(
                f"Invalid object at '{dependency.path}', expected a {ObjectType.NOTEBOOK.name}, got a {str(object_info.object_type)}"
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

    def _load_source(self, object_info: ObjectInfo) -> str:
        if not object_info.path:
            raise ValueError(f"Invalid ObjectInfo: {object_info}")
        with self._ws.workspace.download(object_info.path, format=ExportFormat.SOURCE) as f:
            return f.read().decode("utf-8")


class DependencyResolver:
    def __init__(
        self,
        whitelist: Whitelist,
        local_loader: LocalLoader,
        ws: WorkspaceClient | None,
    ):
        self._whitelist = Whitelist() if whitelist is None else whitelist
        self._local_loader = local_loader
        self._workspace_loader = None if ws is None else WorkspaceLoader(ws)
        self._advices: list[Advice] = []

    def resolve_object_info(self, object_info: ObjectInfo) -> ResolvedDependency | None:
        assert self._workspace_loader is not None
        assert object_info.path is not None
        if object_info.object_type is None:
            raise ValueError(f"Invalid ObjectInfo (missing 'object_type'): {object_info}")
        if object_info.object_type is ObjectType.NOTEBOOK:
            return WorkspaceNotebookDependency(self._workspace_loader, object_info.path)
        if object_info.object_type is ObjectType.FILE:
            return LocalFileDependency(self._local_loader, object_info.path)
        if object_info.object_type in [
            ObjectType.FILE,
            ObjectType.LIBRARY,
            ObjectType.REPO,
            ObjectType.DASHBOARD,
            ObjectType.DIRECTORY,
        ]:
            return None
        raise NotImplementedError(str(object_info.object_type))

    def resolve_dependency(self, dependency: Dependency) -> ResolvedDependency | None:
        if isinstance(dependency, ResolvedDependency):
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
        if self._local_loader.is_file(dependency.path):
            return LocalFileDependency(self._local_loader, dependency.path)
        if self._workspace_loader is not None:
            object_info = self._workspace_loader.get_object_info(dependency.path)
            if object_info is not None:
                return self.resolve_object_info(object_info)
        raise ValueError(f"Could not locate {dependency.path}")

    def get_advices(self) -> Iterable[Advice]:
        yield from self._advices


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

    def register_dependency(self, dependency: Dependency) -> DependencyGraph | None:
        resolved = self._resolver.resolve_dependency(dependency)
        if resolved is None:
            return None
        # already registered ?
        child_graph = self.locate_dependency(resolved)
        if child_graph is not None:
            self._dependencies[resolved] = child_graph
            return child_graph
        # nay, create the child graph and populate it
        child_graph = DependencyGraph(resolved, self, self._resolver)
        self._dependencies[resolved] = child_graph
        container = resolved.load()
        if not container:
            return None
        container.build_dependency_graph(child_graph)
        return child_graph

    def locate_dependency(self, dependency: Dependency) -> DependencyGraph | None:
        return self.locate_dependency_with_path(dependency.path)

    def locate_dependency_with_path(self, path: str) -> DependencyGraph | None:
        # need a list since unlike JS, Python won't let you assign closure variables
        found: list[DependencyGraph] = []
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

    def build_graph_from_python_source(self, python_code: str, register_dependency: Callable[[str], typing.Any]):
        linter = ASTLinter.parse(python_code)
        calls = linter.locate(ast.Call, [("run", ast.Attribute), ("notebook", ast.Attribute), ("dbutils", ast.Name)])
        for call in calls:
            assert isinstance(call, ast.Call)
            path = PythonLinter.get_dbutils_notebook_run_path_arg(call)
            if isinstance(path, ast.Constant):
                path = path.value.strip().strip("'").strip('"')
                object_info = ObjectInfo(object_type=ObjectType.NOTEBOOK, path=path)
                dependency = self._resolver.resolve_object_info(object_info)
                if dependency is not None:
                    self.register_dependency(dependency)
                else:
                    # TODO raise Advice, see https://github.com/databrickslabs/ucx/issues/1439
                    raise ValueError(f"Invalid notebook path in dbutils.notebook.run command: {path}")
        names = PythonLinter.list_import_sources(linter)
        for name in names:
            register_dependency(name)

    def resolve_object_info(self, object_info: ObjectInfo):
        return self._resolver.resolve_object_info(object_info)
