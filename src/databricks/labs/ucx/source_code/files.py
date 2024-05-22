from __future__ import annotations  # for type hints

import logging
from pathlib import Path

from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.whitelist import Whitelist, WhitelistResolver
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage
from databricks.labs.ucx.source_code.graph import (
    BaseImportResolver,
    BaseFileResolver,
    Dependency,
    DependencyGraph,
    DependencyLoader,
    DependencyProblem,
    MaybeDependency,
    SourceContainer,
)

logger = logging.getLogger(__name__)


class LocalFile(SourceContainer):

    def __init__(self, path: Path, source: str, language: Language):
        self._path = path
        self._original_code = source
        # using CellLanguage so we can reuse the facilities it provides
        self._language = CellLanguage.of_language(language)

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        if self._language is not CellLanguage.PYTHON:
            logger.warning(f"Unsupported language: {self._language.language}")
            return []
        return parent.build_graph_from_python_source(self._original_code)

    @property
    def path(self) -> Path:
        return self._path

    def __repr__(self):
        return f"<LocalFile {self._path}>"


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
    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        absolute_path = path_lookup.resolve(dependency.path)
        if not absolute_path:
            return None
        return LocalFile(absolute_path, absolute_path.read_text("utf-8"), Language.PYTHON)

    def exists(self, path: Path) -> bool:
        return path.exists()

    def __repr__(self):
        return "FileLoader()"


class LocalFileResolver(BaseImportResolver, BaseFileResolver):

    def __init__(self, file_loader: FileLoader, whitelist: Whitelist):
        super().__init__()
        self._whitelist_resolver = WhitelistResolver(whitelist)
        self._file_loader = file_loader

    def resolve_local_file(self, path_lookup, path: Path) -> MaybeDependency:
        absolute_path = path_lookup.resolve(path)
        if absolute_path:
            return MaybeDependency(Dependency(self._file_loader, absolute_path), [])
        problem = DependencyProblem("file-not-found", f"File not found: {path.as_posix()}")
        return MaybeDependency(None, [problem])

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        maybe = self._whitelist_resolver.resolve_import(path_lookup, name)
        if maybe.dependency is not None or len(maybe.problems) > 0:
            return maybe
        maybe = self._resolve_import(path_lookup, name)
        if maybe.dependency is not None or len(maybe.problems) > 0:
            return maybe
        return self._fail('import-not-found', f"Could not locate import: {name}")

    def _resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        if not name:
            return MaybeDependency(None, [DependencyProblem("ucx-bug", "Import name is empty")])
        parts = []
        # Relative imports use leading dots. A single leading dot indicates a relative import, starting with
        # the current package. Two or more leading dots indicate a relative import to the parent(s) of the current
        # package, one level per dot after the first.
        # see https://docs.python.org/3/reference/import.html#package-relative-imports
        for i, rune in enumerate(name):
            if not i and rune == '.':  # leading single dot
                parts.append(path_lookup.cwd.as_posix())
                continue
            if rune != '.':
                parts.append(name[i:].replace('.', '/'))
                break
            parts.append("..")
        for candidate in (f'{"/".join(parts)}.py', f'{"/".join(parts)}/__init__.py'):
            relative_path = Path(candidate)
            absolute_path = path_lookup.resolve(relative_path)
            if not absolute_path:
                continue
            dependency = Dependency(self._file_loader, absolute_path)
            return MaybeDependency(dependency, [])
        return MaybeDependency(None, [])

    @staticmethod
    def _fail(code: str, message: str):
        return MaybeDependency(None, [DependencyProblem(code, message)])

    def __repr__(self):
        return "LocalFileResolver()"
