from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ObjectInfo, ObjectType

from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook import Notebook, RunCell
from databricks.labs.ucx.source_code.dependencies import DependencyGraph, Dependency, DependencyLoader


class NotebookMigrator:
    def __init__(self, ws: WorkspaceClient, languages: Languages):
        self._ws = ws
        self._languages = languages

    def revert(self, object_info: ObjectInfo):
        if not object_info.path:
            return False
        with self._ws.workspace.download(object_info.path + ".bak", format=ExportFormat.SOURCE) as f:
            code = f.read().decode("utf-8")
            self._ws.workspace.upload(object_info.path, code.encode("utf-8"))
        return True

    def apply(self, object_info: ObjectInfo) -> bool:
        if not object_info.path or not object_info.language or object_info.object_type is not ObjectType.NOTEBOOK:
            return False
        notebook = self._load_notebook_from_object_info(object_info)
        return self._apply(notebook)

    def build_dependency_graph(self, object_info: ObjectInfo, loader: DependencyLoader) -> DependencyGraph:
        if not object_info.path or not object_info.language or object_info.object_type is not ObjectType.NOTEBOOK:
            raise ValueError("Not a valid Notebook")
        dependency = Dependency.from_object_info(object_info)
        graph = DependencyGraph(dependency, None, loader)
        container = loader.load_dependency(dependency)
        if container is not None:
            container.build_dependency_graph(graph)
        return graph

    def _apply(self, notebook: Notebook) -> bool:
        changed = False
        for cell in notebook.cells:
            # %run is not a supported language, so this needs to come first
            if isinstance(cell, RunCell):
                # TODO migration data ?
                if cell.migrate_notebook_path():
                    changed = True
                continue
            if not self._languages.is_supported(cell.language.language):
                continue
            migrated_code = self._languages.apply_fixes(cell.language.language, cell.original_code)
            if migrated_code != cell.original_code:
                cell.migrated_code = migrated_code
                changed = True
        if changed:
            self._ws.workspace.upload(notebook.path + ".bak", notebook.original_code.encode("utf-8"))
            self._ws.workspace.upload(notebook.path, notebook.to_migrated_code().encode("utf-8"))
            # TODO https://github.com/databrickslabs/ucx/issues/1327 store 'migrated' status
        return changed

    def _load_notebook_from_path(self, path: str) -> Notebook:
        object_info = self._load_object(path)
        if object_info.object_type is not ObjectType.NOTEBOOK:
            raise ValueError(f"Not a Notebook: {path}")
        return self._load_notebook_from_object_info(object_info)

    def _load_object(self, path: str) -> ObjectInfo:
        result = self._ws.workspace.list(path)
        object_info = next((oi for oi in result), None)
        if object_info is None:
            raise ValueError(f"Could not locate object at '{path}'")
        return object_info

    def _load_notebook_from_object_info(self, object_info: ObjectInfo) -> Notebook:
        assert object_info is not None and object_info.path is not None and object_info.language is not None
        source = self._load_source(object_info)
        return Notebook.parse(object_info.path, source, object_info.language)

    def _load_source(self, object_info: ObjectInfo) -> str:
        if not object_info.language or not object_info.path:
            raise ValueError(f"Invalid ObjectInfo: {object_info}")
        with self._ws.workspace.download(object_info.path, format=ExportFormat.SOURCE) as f:
            return f.read().decode("utf-8")
