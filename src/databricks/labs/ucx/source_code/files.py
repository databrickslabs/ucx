from __future__ import annotations  # for type hints

import logging
from collections.abc import Iterable, Callable
from pathlib import Path

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import LocatedAdvice
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter
from databricks.labs.ucx.source_code.path_lookup import PathLookup
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
    DependencyResolver,
    MISSING_SOURCE_PATH,
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


class LocalDirectory(SourceContainer):
    def __init__(self, path: Path, file_loader: FileLoader, dir_loader: DirectoryLoader):
        self._path = path
        self._file_loader = file_loader
        self._dir_loader = dir_loader

    @property
    def path(self) -> Path:
        return self._path

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        return list(self._build_dependency_graph(parent))

    def _build_dependency_graph(self, parent: DependencyGraph) -> Iterable[DependencyProblem]:
        for child_path in self._path.iterdir():
            if child_path.is_dir():
                dependency = Dependency(self._dir_loader, child_path)
                yield from parent.register_dependency(dependency).problems
                continue
            dependency = Dependency(self._file_loader, child_path)
            yield from parent.register_dependency(dependency).problems

    def __repr__(self):
        return f"<LocalDirectory {self._path}>"


class LocalFilesLinter:

    def __init__(
        self,
        languages: Languages,
        file_loader: FileLoader,
        dir_loader: DirectoryLoader,
        path_lookup: PathLookup,
        dependency_resolver: DependencyResolver,
    ) -> None:
        self._languages = languages
        self._file_loader = file_loader
        self._dir_loader = dir_loader
        self._path_lookup = path_lookup
        self._dependency_resolver = dependency_resolver
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}

    def lint(self, path: Path) -> list[LocatedAdvice]:
        return list(self._lint(path))

    def _lint(self, path: Path) -> Iterable[LocatedAdvice]:
        loader = self._dir_loader if path.is_dir() else self._file_loader
        dependency = Dependency(loader, path)
        graph = DependencyGraph(dependency, None, self._dependency_resolver, self._path_lookup)
        container = dependency.load(self._path_lookup)
        assert container is not None  # because we just created it
        dependency_problems = container.build_dependency_graph(graph)
        for dependency_problem in dependency_problems:
            advice = self._located_advice_from_dependency_problem(dependency_problem)
            yield advice
        for child_path in graph.all_paths:
            yield from self._lint_one(child_path)

    @staticmethod
    def _located_advice_from_dependency_problem(problem: DependencyProblem):
        return LocatedAdvice(
            path=problem.source_path.absolute() if problem.source_path.name != MISSING_SOURCE_PATH else Path('UNKNOWN'),
            code=problem.code,
            message=problem.message,
            start_line=problem.start_line,
            start_col=problem.start_col,
            end_line=problem.end_line,
            end_col=problem.end_col,
        )

    def _lint_one(self, path: Path) -> Iterable[LocatedAdvice]:
        if path.is_dir():
            return []
        linter = FileLinter(self._languages, path)
        advices = linter.lint()
        return [advice.for_path(path) for advice in advices]


class LocalFileMigrator:
    """The LocalFileMigrator class is responsible for fixing code files based on their language."""

    def __init__(
        self, migration_index: MigrationIndex, languages_factory: Callable[[MigrationIndex], Languages] = Languages
    ):
        self._migration_index = migration_index
        self._languages_factory = languages_factory
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
        languages = self._languages_factory(self._migration_index)
        linter = languages.linter(language)
        # Open the file and read the code
        with path.open("r") as f:
            code = f.read()
            applied = False
            # Lint the code and apply fixes
            for advice in linter.lint(code):
                logger.info(f"Found: {advice}")
                fixer = languages.fixer(language, advice.code)
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
        # for now we only support python
        if not absolute_path.as_posix().endswith(".py"):
            return None
        for encoding in ("utf-8", "ascii"):
            try:
                code = absolute_path.read_text(encoding)
                return LocalFile(absolute_path, code, Language.PYTHON)
            except UnicodeDecodeError:
                pass
        return None

    def exists(self, path: Path) -> bool:
        return path.exists()

    def __repr__(self):
        return "FileLoader()"


class DirectoryLoader(FileLoader):

    def __init__(self, file_loader: FileLoader):
        self._file_loader = file_loader

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        absolute_path = path_lookup.resolve(dependency.path)
        if not absolute_path:
            return None
        return LocalDirectory(absolute_path, self._file_loader, self)


class LocalFileResolver(BaseImportResolver, BaseFileResolver):

    def __init__(self, file_loader: FileLoader, next_resolver: BaseImportResolver | None = None):
        super().__init__(next_resolver)
        self._file_loader = file_loader

    def with_next_resolver(self, resolver: BaseImportResolver) -> BaseImportResolver:
        return LocalFileResolver(self._file_loader, resolver)

    def resolve_local_file(self, path_lookup, path: Path) -> MaybeDependency:
        absolute_path = path_lookup.resolve(path)
        if absolute_path:
            return MaybeDependency(Dependency(self._file_loader, absolute_path), [])
        problem = DependencyProblem("file-not-found", f"File not found: {path.as_posix()}")
        return MaybeDependency(None, [problem])

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
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
        return super().resolve_import(path_lookup, name)

    def __repr__(self):
        return "LocalFileResolver()"
