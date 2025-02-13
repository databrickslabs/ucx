from __future__ import annotations

import logging
from pathlib import Path
from typing import TypeVar

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import infer_file_language_if_supported, is_a_notebook, safe_read_text
from databricks.labs.ucx.source_code.graph import (
    BaseNotebookResolver,
    Dependency,
    DependencyLoader,
    MaybeDependency,
    StubContainer,
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


class NotebookLoader(DependencyLoader):
    """Load a notebook.

    Args:
        exclude_paths (set[Path] | None) : A set of paths to load as
            class:`StubContainer`. If None, no paths are excluded.

            Note: The exclude paths are loaded as `StubContainer` to
            signal that the path is found, however, it should not be
            processed.

    TODO:
        Let `NotebookLoader` inherit from `FileLoader` and reuse the
        implementation of `load_dependency` to first load the file, then
        convert it to a `Notebook` if it is a notebook source.
    """

    def __init__(self, *, exclude_paths: set[Path] | None = None):
        self._exclude_paths = exclude_paths or set[Path]()

    def resolve(self, path_lookup: PathLookup, path: Path) -> Path | None:
        """Resolve the notebook path.

        If the path is a Python file, return the path to the Python file. If the path is neither,
        return None.
        """
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

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> Notebook | StubContainer | None:
        """Load the notebook dependency."""
        resolved_path = self.resolve(path_lookup, dependency.path)
        if not resolved_path:
            return None
        if resolved_path in self._exclude_paths:
            # Paths are excluded from further processing by loading them as stub container.
            # Note we don't return `None`, as the path is found.
            return StubContainer(resolved_path)
        content = safe_read_text(resolved_path)
        if content is None:
            return None
        language = self._detect_language(resolved_path, content)
        if not language:
            logger.warning(f"Could not detect language for {resolved_path}")
            return None
        try:
            return Notebook.parse(resolved_path, content, language)
        except ValueError as e:
            logger.warning(f"Could not parse notebook for {resolved_path}", exc_info=e)
            return None

    @staticmethod
    def _detect_language(path: Path, content: str) -> Language | None:
        language = infer_file_language_if_supported(path)
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
