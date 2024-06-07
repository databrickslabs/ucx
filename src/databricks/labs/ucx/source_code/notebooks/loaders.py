from __future__ import annotations

import abc
import logging
from pathlib import Path

from databricks.sdk.errors import NotFound

from databricks.labs.ucx.source_code.graph import (
    BaseNotebookResolver,
    Dependency,
    DependencyLoader,
    MaybeDependency,
    SourceContainer,
)
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage
from databricks.labs.ucx.source_code.notebooks.sources import Notebook, SUPPORTED_EXTENSION_LANGUAGES
from databricks.labs.ucx.source_code.path_lookup import PathLookup

logger = logging.getLogger(__name__)


class NotebookResolver(BaseNotebookResolver):

    def __init__(self, notebook_loader: NotebookLoader):
        super().__init__()
        self._notebook_loader = notebook_loader

    def resolve_notebook(self, path_lookup: PathLookup, path: Path) -> MaybeDependency:
        absolute_path = self._notebook_loader.resolve(path_lookup, path)
        if not absolute_path:
            return self._fail('notebook-not-found', f"Notebook not found: {path.as_posix()}")
        dependency = Dependency(self._notebook_loader, absolute_path)
        return MaybeDependency(dependency, [])


class NotebookLoader(DependencyLoader, abc.ABC):
    def resolve(self, path_lookup: PathLookup, path: Path) -> Path | None:
        """When exported through Git, notebooks are saved with a .py extension. If the path is a notebook, return the
        path to the notebook. If the path is a Python file, return the path to the Python file. If the path is neither,
        return None."""
        for candidate in (path, self._adjust_path(path)):
            absolute_path = path_lookup.resolve(candidate)
            if not absolute_path:
                continue
            return absolute_path
        return None

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        absolute_path = self.resolve(path_lookup, dependency.path)
        if not absolute_path:
            return None
        try:
            content = absolute_path.read_text("utf-8")
        except NotFound:
            logger.warning(f"Could not read notebook from workspace: {absolute_path}")
            return None
        language = self._detect_language(absolute_path, content)
        if not language:
            logger.warning(f"Could not detect language for {absolute_path}")
            return None
        return Notebook.parse(absolute_path, content, language)

    @staticmethod
    def _detect_language(path: Path, content: str):
        language = SUPPORTED_EXTENSION_LANGUAGES.get(path.suffix, None)
        if language:
            return language
        for cell_language in CellLanguage:
            if content.startswith(cell_language.file_magic_header):
                return cell_language.language
        return None

    @staticmethod
    def _adjust_path(path: Path):
        if path.suffix == ".py":
            return path
        return Path(path.as_posix() + ".py")

    def __repr__(self):
        return "NotebookLoader()"
