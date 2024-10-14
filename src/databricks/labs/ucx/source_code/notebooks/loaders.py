from __future__ import annotations

import abc
import logging
from pathlib import Path
from typing import TypeVar

from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import is_a_notebook, file_language
from databricks.labs.ucx.source_code.graph import (
    BaseNotebookResolver,
    Dependency,
    DependencyLoader,
    MaybeDependency,
    SourceContainer,
)
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup

logger = logging.getLogger(__name__)


PathT = TypeVar("PathT", bound=Path)


class NotebookResolver(BaseNotebookResolver):

    def __init__(self, notebook_loader: NotebookLoader):
        super().__init__()
        self._notebook_loader = notebook_loader

    def resolve_notebook(self, path_lookup: PathLookup, path: Path, inherit_context: bool) -> MaybeDependency:
        absolute_path = self._notebook_loader.resolve(path_lookup, path)
        if not absolute_path:
            return self._fail('notebook-not-found', f"Notebook not found: {path.as_posix()}")
        dependency = Dependency(self._notebook_loader, absolute_path, inherit_context)
        return MaybeDependency(dependency, [])


class NotebookLoader(DependencyLoader, abc.ABC):
    def resolve(self, path_lookup: PathLookup, path: Path) -> Path | None:
        """If the path is a Python file, return the path to the Python file. If the path is neither,
        return None."""
        # check current working directory first
        absolute_path = path_lookup.cwd / path
        absolute_path = absolute_path.resolve()
        if is_a_notebook(absolute_path):
            return absolute_path
        # When exported through Git, notebooks are saved with a .py extension. So check with and without extension
        candidates = (path, self._adjust_path(path)) if not path.suffix else (path,)
        for candidate in candidates:
            a_path = path_lookup.resolve(candidate)
            if not a_path:
                continue
            return a_path
        return None

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        absolute_path = self.resolve(path_lookup, dependency.path)
        if not absolute_path:
            return None
        try:
            content = absolute_path.read_text("utf-8")
        except NotFound:
            logger.warning(f"Path not found trying to read notebook from workspace: {absolute_path}")
            return None
        except PermissionError:
            logger.warning(
                f"Permission error while reading notebook from workspace: {absolute_path}",
                exc_info=True,
            )
            return None
        language = self._detect_language(absolute_path, content)
        if not language:
            logger.warning(f"Could not detect language for {absolute_path}")
            return None
        return Notebook.parse(absolute_path, content, language)

    @staticmethod
    def _detect_language(path: Path, content: str) -> Language | None:
        language = file_language(path)
        if language:
            return language
        for cell_language in CellLanguage:
            if content.startswith(cell_language.file_magic_header):
                return cell_language.language
        return None

    @staticmethod
    def _adjust_path(path: PathT) -> PathT:
        existing_suffix = path.suffix
        if existing_suffix == ".py":
            return path
        # Ensure we append instead of replacing an existing suffix.
        new_suffix = existing_suffix + ".py"
        return path.with_suffix(new_suffix)

    def __repr__(self):
        return "NotebookLoader()"
