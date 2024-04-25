from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ObjectInfo, ObjectType

from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook import Notebook, RunCell
from databricks.labs.ucx.source_code.dependencies import (
    Dependency,
)
from databricks.labs.ucx.source_code.dependency_loaders import WorkspaceNotebookLoader


class NotebookMigrator:
    def __init__(
        self,
        ws: WorkspaceClient,
        languages: Languages,
    ):
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
        dependency = Dependency(WorkspaceNotebookLoader(self._ws), Path(object_info.path))
        container = dependency.load()
        assert isinstance(container, Notebook)
        return self._apply(container)

    def _apply(self, notebook: Notebook) -> bool:
        changed = False
        for cell in notebook.cells:
            # %run is not a supported language, so this needs to come first
            if isinstance(cell, RunCell):
                # TODO migration data, see https://github.com/databrickslabs/ucx/issues/1327
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
