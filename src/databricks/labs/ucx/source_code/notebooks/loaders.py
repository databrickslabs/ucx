from __future__ import annotations

import abc
import logging
from pathlib import Path

from databricks.sdk.errors import NotFound

from databricks.labs.ucx.source_code.graph import (
    BaseDependencyResolver,
    Dependency,
    DependencyLoader,
    SourceContainer,
    MaybeDependency,
)
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup

logger = logging.getLogger(__name__)


class NotebookResolver(BaseDependencyResolver):

    def __init__(self, notebook_loader: NotebookLoader, next_resolver: BaseDependencyResolver | None = None):
        super().__init__(next_resolver)
        self._notebook_loader = notebook_loader

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        return NotebookResolver(self._notebook_loader, resolver)

    def resolve_notebook(self, path_lookup: PathLookup, path: Path) -> MaybeDependency:
        absolute_path = self._notebook_loader.resolve(path_lookup, path)
        if not absolute_path:
            return super().resolve_notebook(path_lookup, path)
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
        # TODO: catch exceptions, and create MaybeContainer to follow the pattern - OSError and NotFound are common
        absolute_path = self.resolve(path_lookup, dependency.path)
        if not absolute_path:
            return None
        try:
            content = absolute_path.read_text("utf-8")
        except NotFound:
            logger.warning(f"Could not read notebook from workspace: {absolute_path}")
            return None
        language = self._detect_language(content)
        if not language:
            logger.warning(f"Could not detect language for {absolute_path}")
            return None
        return Notebook.parse(absolute_path, content, language)

    @staticmethod
    def _detect_language(content: str):
        for language in CellLanguage:
            if content.startswith(language.file_magic_header):
                return language.language
        return None

    @staticmethod
    def _adjust_path(path: Path):
        if path.suffix == ".py":
            return path
        return Path(path.as_posix() + ".py")

    def __repr__(self):
        return "NotebookLoader()"
