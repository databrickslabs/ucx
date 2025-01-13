from __future__ import annotations

from pathlib import Path

from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.cells import RunCell
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup


class NotebookMigrator:
    def __init__(self, context: LinterContext):
        self._context = context

    def revert(self, path: Path) -> bool:
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
                continue
            if not self._context.is_supported(cell.language.language):
                continue
            migrated_code = self._context.apply_fixes(cell.language.language, cell.original_code)
            if migrated_code != cell.original_code:
                cell.migrated_code = migrated_code
                changed = True
        if changed:
            notebook.path.replace(notebook.path.with_suffix(".bak"))
            notebook.path.write_text(notebook.to_migrated_code())
        return changed
