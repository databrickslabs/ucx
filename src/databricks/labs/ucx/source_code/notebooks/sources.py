from __future__ import annotations

import logging
from pathlib import Path


from databricks.sdk.service.workspace import Language

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

    @staticmethod
    def parse(path: Path, source: str, default_language: Language) -> Notebook:
        default_cell_language = CellLanguage.of_language(default_language)
        cells = default_cell_language.extract_cells(source)
        if cells is None:
            raise ValueError(f"Could not parse Notebook: {path}")
        return Notebook(path, source, default_language, cells, source.endswith('\n'))

    def __init__(self, path: Path, source: str, language: Language, cells: list[Cell], ends_with_lf: bool):
        self._path = path
        self._source = source
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
        return self._source

    def to_migrated_code(self) -> str:
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
