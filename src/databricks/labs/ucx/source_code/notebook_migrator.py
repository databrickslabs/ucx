from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ObjectInfo, ObjectType

from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook import Notebook, RunCell
from databricks.labs.ucx.source_code.dependencies import DependencyGraph, Dependency, DependencyLoader


class NotebookMigrator:
    def __init__(self, ws: WorkspaceClient, languages: Languages, loader: DependencyLoader):
        self._ws = ws
        self._languages = languages
        self._loader = loader

    def build_dependency_graph(self, object_info: ObjectInfo) -> DependencyGraph:
        if not object_info.path or not object_info.language or object_info.object_type is not ObjectType.NOTEBOOK:
            raise ValueError("Not a valid Notebook")
        dependency = Dependency.from_object_info(object_info)
        graph = DependencyGraph(dependency, None, self._loader)
        container = self._loader.load_dependency(dependency)
        if container is not None:
            container.build_dependency_graph(graph)
        return graph

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
        notebook = self._loader.load_dependency(Dependency.from_object_info(object_info))
        assert isinstance(notebook, Notebook)
        return self._apply(notebook)

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
