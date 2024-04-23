from __future__ import annotations

import abc
import ast
from collections.abc import Callable, Iterable
from pathlib import Path

from databricks.sdk.service.workspace import ObjectType, ObjectInfo, ExportFormat
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.source_code.base import Advice, Deprecation
from databricks.labs.ucx.source_code.python_linter import ASTLinter, PythonLinter
from databricks.labs.ucx.source_code.site_packages import SitePackages
from databricks.labs.ucx.source_code.whitelist import Whitelist, UCCompatibility


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


class WorkspaceNotebookLoader(NotebookLoader):

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def is_notebook(self, path: Path):
        object_info = self._ws.workspace.get_status(str(path))
        # TODO check error conditions, see https://github.com/databrickslabs/ucx/issues/1361
        return object_info is not None and object_info.object_type is ObjectType.NOTEBOOK

    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        object_info = self._load_object(dependency)
        return self._load_notebook(object_info)

    def _load_object(self, dependency: Dependency) -> ObjectInfo:
        object_info = self._ws.workspace.get_status(str(dependency.path))
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
        site_packages: SitePackages,
        file_loader: LocalFileLoader,
        notebook_loader: NotebookLoader,
    ):
        self._whitelist = whitelist
        self._site_packages = site_packages
        self._file_loader = file_loader
        self._notebook_loader = notebook_loader
        self._advices: list[Advice] = []

    def resolve_notebook(self, path: Path) -> Dependency | None:
        if self._notebook_loader.is_notebook(path):
            return Dependency(self._notebook_loader, path)
        return None

    def resolve_local_file(self, path: Path) -> Dependency | None:
        if self._file_loader.is_file(path) and not self._file_loader.is_notebook(path):
            return Dependency(self._file_loader, path)
        return None

    def resolve_import(self, name: str) -> Dependency | None:
        compatibility = self._whitelist.compatibility(name)
        # TODO attach compatibility to dependency, see https://github.com/databrickslabs/ucx/issues/1382
        if compatibility is not None:
            if compatibility == UCCompatibility.NONE:
                self._advices.append(
                    Deprecation(
                        code="dependency-check",
                        message=f"Use of dependency {name} is deprecated",
                        start_line=0,
                        start_col=0,
                        end_line=0,
                        end_col=0,
                    )
                )
            return None
        if self._file_loader.is_file(Path(name)):
            return Dependency(self._file_loader, Path(name))
        raise ValueError(f"Could not locate {name}")

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

    def register_notebook(self, path: Path) -> DependencyGraph | None:
        resolved = self._resolver.resolve_notebook(path)
        if resolved is None:
            return None
        return self.register_dependency(resolved)

    def register_import(self, name: str) -> DependencyGraph | None:
        resolved = self._resolver.resolve_import(name)
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
        path_str = str(path)
        path_str = path_str[2:] if path_str.startswith('./') else path_str

        def check_registered_dependency(graph):
            # TODO https://github.com/databrickslabs/ucx/issues/1287
            graph_path = str(graph.path)
            if graph_path.startswith('./'):
                graph_path = graph_path[2:]
            if graph_path == path_str:
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
    def paths(self) -> set[Path]:
        return {d.path for d in self.dependencies}

    # when visit_node returns True it interrupts the visit
    def visit(self, visit_node: Callable[[DependencyGraph], bool | None]) -> bool | None:
        if visit_node(self):
            return True
        for dependency in self._dependencies.values():
            if dependency.visit(visit_node):
                return True
        return False

    def build_graph_from_python_source(self, python_code: str):
        linter = ASTLinter.parse(python_code)
        calls = linter.locate(ast.Call, [("run", ast.Attribute), ("notebook", ast.Attribute), ("dbutils", ast.Name)])
        for call in calls:
            assert isinstance(call, ast.Call)
            path = PythonLinter.get_dbutils_notebook_run_path_arg(call)
            if isinstance(path, ast.Constant):
                path = path.value.strip().strip("'").strip('"')
                dependency = self.register_notebook(path)
                if dependency is None:
                    # TODO raise Advice, see https://github.com/databrickslabs/ucx/issues/1439
                    raise ValueError(f"Invalid notebook path in dbutils.notebook.run command: {path}")
            else:
                # TODO raise Advice, see https://github.com/databrickslabs/ucx/issues/1439
                pass
        for name in PythonLinter.list_import_sources(linter):
            self.register_import(name)


class DependencyGraphBuilder:

    def __init__(self, resolver: DependencyResolver):
        self._resolver = resolver

    def build_local_file_dependency_graph(self, path: Path) -> DependencyGraph:
        dependency = self._resolver.resolve_local_file(path)
        assert dependency is not None
        graph = DependencyGraph(dependency, None, self._resolver)
        container = dependency.load()
        if container is not None:
            container.build_dependency_graph(graph)
        return graph

    def build_notebook_dependency_graph(self, path: Path) -> DependencyGraph:
        dependency = self._resolver.resolve_notebook(path)
        assert dependency is not None
        graph = DependencyGraph(dependency, None, self._resolver)
        container = dependency.load()
        if container is not None:
            container.build_dependency_graph(graph)
        return graph
