
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ObjectInfo, ObjectType

from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook import DependencyGraph, Notebook, RunCell


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
        notebook = self._load_notebook(object_info)
        changed = self._apply(notebook)
        # now discover dependencies and migrate them too
        dependencies = DependencyGraph(object_info.path, None, self._load_notebook_from_path)
        notebook.build_dependency_graph(dependencies)
        for path in dependencies.paths:
            # don't process twice
            if path == object_info.path:
                continue
            object_info = self._load_object(path)
            notebook = self._load_notebook(object_info)
            changed_ = self._apply(notebook)
            # TODO what to do with changed_ ?
        return changed

    def _apply(self, notebook: Notebook) -> bool:
        changed = False
        for cell in notebook.cells:
            if isinstance(cell, RunCell):
                # TODO data on what to change to ?
                if cell.migrate_notebook_path():
                    changed = True
                continue
            elif not self._languages.is_supported(cell.language.language):
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

    def _load_notebook_from_path(self, path: str) -> Notebook | None:
        object_info = self._load_object(path)
        if object_info is None or object_info.object_type is not ObjectType.NOTEBOOK:
            return None
        return self._load_notebook(object_info)

    def _load_object(self, path: str) -> ObjectInfo | None:
        result = self._ws.workspace.list(path)
        if isinstance(result, ObjectInfo):
            return result
        return next((oi for oi in result), None)

    def _load_notebook(self, object_info: ObjectInfo) -> Notebook:
        source = self._loadSource(object_info)
        return Notebook.parse(object_info.path, source, object_info.language)

    def _loadSource(self, object_info: ObjectInfo) -> str:
        if not object_info.language or not object_info.path:
            raise ValueError(f"Invalid ObjectInfo: {object_info}")
        with self._ws.workspace.download(object_info.path, format=ExportFormat.SOURCE) as f:
            return f.read().decode("utf-8")
