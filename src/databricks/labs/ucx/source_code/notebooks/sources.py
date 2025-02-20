from __future__ import annotations

import logging
from pathlib import Path


from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import back_up_path, safe_write_text, revert_back_up_path
from databricks.labs.ucx.source_code.graph import (
    SourceContainer,
    DependencyGraph,
    DependencyProblem,
    InheritedContext,
)
from databricks.labs.ucx.source_code.notebooks.cells import (
    CellLanguage,
    Cell,
    CELL_SEPARATOR,
    NOTEBOOK_HEADER,
)

logger = logging.getLogger(__name__)


class Notebook(SourceContainer):
    """A notebook source code container.

    TODO:
        Let `Notebook` inherit from `LocalFile`
    """

    @classmethod
    def parse(cls, path: Path, source: str, default_language: Language) -> Notebook:
        default_cell_language = CellLanguage.of_language(default_language)
        cells = default_cell_language.extract_cells(source)
        if cells is None:
            raise ValueError(f"Could not parse Notebook: {path}")
        return cls(path, source, default_language, cells, source.endswith('\n'))

    def __init__(self, path: Path, source: str, language: Language, cells: list[Cell], ends_with_lf: bool):
        self._path = path
        self._original_code = source
        self._language = language
        self._cells = cells
        self._ends_with_lf = ends_with_lf

    @property
    def path(self) -> Path:
        return self._path

    @property
    def cells(self) -> list[Cell]:
        return self._cells

    @property
    def original_code(self) -> str:
        return self._original_code

    @property
    def migrated_code(self) -> str:
        """Format the migrated code by chaining the migrated cells."""
        default_language = CellLanguage.of_language(self._language)
        header = f"{default_language.comment_prefix} {NOTEBOOK_HEADER}"
        sources = [header]
        for i, cell in enumerate(self._cells):
            migrated_code = cell.migrated_code
            if cell.language is not default_language:
                migrated_code = default_language.wrap_with_magic(migrated_code, cell.language)
            sources.append(migrated_code)
            if i < len(self._cells) - 1:
                sources.append('')
                sources.append(f'{default_language.comment_prefix} {CELL_SEPARATOR}')
                sources.append('')
        if self._ends_with_lf:
            sources.append('')  # following join will append lf
        return '\n'.join(sources)

    def _safe_write_text(self, contents: str) -> int | None:
        """Write content to the local file."""
        return safe_write_text(self._path, contents)

    def _back_up_path(self) -> Path | None:
        """Back up the original file."""
        return back_up_path(self._path)

    def back_up_original_and_flush_migrated_code(self) -> int | None:
        """Back up the original notebook and flush the migrated code to the file.

        This is a single method to avoid overwriting the original file without a backup.

        Returns :
            int : The number of characters written. If None, nothing is written to the file.

        TODO:
            Let `Notebook` inherit from `LocalFile` and reuse implementation of
            `back_up_original_and_flush_migrated_code`.
        """
        if self.original_code == self.migrated_code:
            # Avoiding unnecessary back up and flush
            return len(self.migrated_code)
        backed_up_path = self._back_up_path()
        if not backed_up_path:
            # Failed to back up the original file, avoid overwriting existing file
            return None
        number_of_characters_written = self._safe_write_text(self.migrated_code)
        if number_of_characters_written is None:
            # Failed to overwrite original file, clean up by reverting backup
            revert_back_up_path(self._path)
        return number_of_characters_written

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        """Check for any problems with dependencies of the cells in this notebook.

        Returns:
            A list of found dependency problems; position information for problems is relative to the notebook source.
        """
        problems: list[DependencyProblem] = []
        for cell in self._cells:
            cell_problems = cell.build_dependency_graph(parent)
            problems.extend(cell_problems)
        return problems

    def build_inherited_context(self, graph: DependencyGraph, child_path: Path) -> InheritedContext:
        problems: list[DependencyProblem] = []
        context = InheritedContext(None, False, problems)
        for cell in self._cells:
            child_context = cell.build_inherited_context(graph, child_path)
            context = context.append(child_context, True)
            if context.found:
                return context
        return context

    def __repr__(self):
        return f"<Notebook {self._path}>"
