from __future__ import annotations  # for type hints

import logging
from pathlib import Path
from collections.abc import Callable

from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage
from databricks.labs.ucx.source_code.notebooks.base import NOTEBOOK_HEADER
from databricks.labs.ucx.source_code.graph import (
    DependencyGraph,
    SourceContainer,
    DependencyProblem,
    DependencyLoader,
    Dependency,
    BaseDependencyResolver,
)

logger = logging.getLogger(__name__)


class LocalFile(SourceContainer):

    def __init__(self, path: Path, source: str, language: Language):
        self._path = path
        self._original_code = source
        # using CellLanguage so we can reuse the facilities it provides
        self._language = CellLanguage.of_language(language)

    @property
    def path(self):
        return self._path

    def build_dependency_graph(self, parent: DependencyGraph, path_lookup: PathLookup) -> None:
        if self._language is not CellLanguage.PYTHON:
            logger.warning(f"Unsupported language: {self._language.language}")
            return
        path_lookup.push_cwd(self.path.parent)
        problems = parent.build_graph_from_python_source(self._original_code, path_lookup)
        parent.add_problems(problems)
        path_lookup.pop_cwd()


class LocalFileMigrator:
    """The LocalFileMigrator class is responsible for fixing code files based on their language."""

    def __init__(self, languages: Languages):
        self._languages = languages
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}

    def apply(self, path: Path) -> bool:
        if path.is_dir():
            for child_path in path.iterdir():
                self.apply(child_path)
            return True
        return self._apply_file_fix(path)

    def _apply_file_fix(self, path):
        """
        The fix method reads a file, lints it, applies fixes, and writes the fixed code back to the file.
        """
        # Check if the file extension is in the list of supported extensions
        if path.suffix not in self._extensions:
            return False
        # Get the language corresponding to the file extension
        language = self._extensions[path.suffix]
        # If the language is not supported, return
        if not language:
            return False
        logger.info(f"Analysing {path}")
        # Get the linter for the language
        linter = self._languages.linter(language)
        # Open the file and read the code
        with path.open("r") as f:
            code = f.read()
            applied = False
            # Lint the code and apply fixes
            for advice in linter.lint(code):
                logger.info(f"Found: {advice}")
                fixer = self._languages.fixer(language, advice.code)
                if not fixer:
                    continue
                logger.info(f"Applying fix for {advice}")
                code = fixer.apply(code)
                applied = True
            if not applied:
                return False
            # Write the fixed code back to the file
            with path.open("w") as f:
                logger.info(f"Overwriting {path}")
                f.write(code)
                return True


class FileLoader(DependencyLoader):

    def __init__(self, path_lookup: PathLookup):
        self._path_lookup = path_lookup

    def load_dependency(self, dependency: Dependency) -> SourceContainer | None:
        fullpath = self.full_path(dependency.path)
        assert fullpath is not None
        return LocalFile(fullpath, fullpath.read_text("utf-8"), Language.PYTHON)

    def exists(self, path: Path) -> bool:
        return self.full_path(path) is not None

    def full_path(self, path: Path) -> Path | None:
        if path.is_file():
            return path
        for parent in self._path_lookup.paths:
            child = Path(parent, path)
            if child.is_file():
                return child
        return None

    def is_notebook(self, path: Path) -> bool:
        fullpath = self.full_path(path)
        if fullpath is None:
            return False
        with fullpath.open(mode="r", encoding="utf-8") as stream:
            line = stream.readline()
            return NOTEBOOK_HEADER in line


class LocalFileResolver(BaseDependencyResolver):

    def __init__(self, file_loader: FileLoader, next_resolver: BaseDependencyResolver | None = None):
        super().__init__(next_resolver)
        self._file_loader = file_loader

    def with_next_resolver(self, resolver: BaseDependencyResolver) -> BaseDependencyResolver:
        return LocalFileResolver(self._file_loader, resolver)

    # TODO problem_collector is tactical, pending https://github.com/databrickslabs/ucx/issues/1559
    def resolve_local_file(
        self, path: Path, problem_collector: Callable[[DependencyProblem], None]
    ) -> Dependency | None:
        if self._file_loader.exists(path) and not self._file_loader.is_notebook(path):
            return Dependency(self._file_loader, path)
        return super().resolve_local_file(path, problem_collector)

    def resolve_import(self, name: str, problem_collector: Callable[[DependencyProblem], None]) -> Dependency | None:
        fullpath = self._file_loader.full_path(Path(f"{name}.py"))
        if fullpath is not None:
            return Dependency(self._file_loader, fullpath)
        return super().resolve_import(name, problem_collector)
