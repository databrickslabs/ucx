from __future__ import annotations

from pathlib import Path

from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.cells import RunCell
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup


class NotebookMigrator:
    def __init__(self, languages: LinterContext):
        # TODO: move languages to `apply`
        self._languages = languages

    def revert(self, path: Path):
        backup_path = path.with_suffix(".bak")
        if not backup_path.exists():
            return False
        return path.write_text(backup_path.read_text()) > 0

    def apply(self, path: Path) -> bool:
        if not path.exists():
            return False
        dependency = Dependency(NotebookLoader(), path)
        # TODO: the interface for this method has to be changed
        lookup = PathLookup.from_sys_path(Path.cwd())
        container = dependency.load(lookup)
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
            # TODO https://github.com/databrickslabs/ucx/issues/1327 store 'migrated' status
            notebook.path.replace(notebook.path.with_suffix(".bak"))
            notebook.path.write_text(notebook.to_migrated_code())
        return changed
