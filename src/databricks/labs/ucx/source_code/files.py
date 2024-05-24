from __future__ import annotations  # for type hints

import logging
from collections.abc import Iterable, Callable
from pathlib import Path
import sys
from typing import TextIO

from databricks.labs.ucx.source_code.base import LocatedAdvice
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter, SUPPORTED_EXTENSION_LANGUAGES
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.whitelist import Whitelist, UCCompatibility
from databricks.sdk.service.workspace import Language
from databricks.labs.blueprint.tui import Prompts

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
)

logger = logging.getLogger(__name__)


class LocalFile(SourceContainer):

    def __init__(self, path: Path, source: str, language: Language):
        self._path = path
        self._original_code = source
        # using CellLanguage so we can reuse the facilities it provides
        self._language = CellLanguage.of_language(language)

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        if self._language is CellLanguage.PYTHON:
            return parent.build_graph_from_python_source(self._original_code)
        # supported language that does not generate dependencies
        if self._language is CellLanguage.SQL:
            return []
        logger.warning(f"Unsupported language: {self._language.language}")
        return []

    @property
    def path(self) -> Path:
        return self._path

    def __repr__(self):
        return f"<LocalFile {self._path}>"


class Folder(SourceContainer):
    def __init__(self, path: Path, file_loader: FileLoader, folder_loader: FolderLoader):
        self._path = path
        self._file_loader = file_loader
        self._folder_loader = folder_loader

    @property
    def path(self) -> Path:
        return self._path

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        # don't directly scan non-source directories, let it be done for relevant imports only
        if self._path.name in {"__pycache__", ".git", ".github", ".venv", ".mypy_cache", "site-packages"}:
            return []
        return list(self._build_dependency_graph(parent))

    def _build_dependency_graph(self, parent: DependencyGraph) -> Iterable[DependencyProblem]:
        for child_path in self._path.iterdir():
            loader = self._folder_loader if child_path.is_dir() else self._file_loader
            dependency = Dependency(loader, child_path)
            yield from parent.register_dependency(dependency).problems

    def __repr__(self):
        return f"<Folder {self._path}>"


class LocalCodeLinter:

    def __init__(
        self,
        file_loader: FileLoader,
        folder_loader: FolderLoader,
        path_lookup: PathLookup,
        dependency_resolver: DependencyResolver,
        languages_factory: Callable[[], Languages],
    ) -> None:
        self._file_loader = file_loader
        self._folder_loader = folder_loader
        self._path_lookup = path_lookup
        self._dependency_resolver = dependency_resolver
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}
        self._languages_factory = languages_factory

    def lint(self, prompts: Prompts, path: Path | None, stdout: TextIO = sys.stdout) -> list[LocatedAdvice]:
        """Lint local code files looking for problems in notebooks and python files."""
        if path is None:
            response = prompts.question(
                "Which file or directory do you want to lint?",
                default=Path.cwd().as_posix(),
                validate=lambda p_: Path(p_).exists(),
            )
            path = Path(response)
        located_advices = list(self._lint(path))
        for located in located_advices:
            advice_path = located.path.relative_to(path)
            advice = located.advice
            message = (
                f"{advice_path.as_posix()}:{advice.start_line}:{advice.start_col}: [{advice.code}] {advice.message}\n"
            )
            stdout.write(message)
        return located_advices

    def _lint(self, path: Path) -> Iterable[LocatedAdvice]:
        loader = self._folder_loader if path.is_dir() else self._file_loader
        dependency = Dependency(loader, path)
        graph = DependencyGraph(dependency, None, self._dependency_resolver, self._path_lookup)
        container = dependency.load(self._path_lookup)
        assert container is not None  # because we just created it
        problems = container.build_dependency_graph(graph)
        for problem in problems:
            problem_path = Path('UNKNOWN') if problem.is_path_missing() else problem.source_path.absolute()
            yield problem.as_advisory().for_path(problem_path)
        for child_path in graph.all_paths:
            yield from self._lint_one(child_path)

    def _lint_one(self, path: Path) -> Iterable[LocatedAdvice]:
        if path.is_dir():
            return []
        languages = self._languages_factory()
        linter = FileLinter(languages, path)
        return [advice.for_path(path) for advice in linter.lint()]


class LocalFileMigrator:
    """The LocalFileMigrator class is responsible for fixing code files based on their language."""

    def __init__(self, languages_factory: Callable[[], Languages]):
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}
        self._languages_factory = languages_factory

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
        languages = self._languages_factory()
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


class StubContainer(SourceContainer):

    def __init__(self, path: Path):
        super().__init__()
        self._path = path

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        return []


class FileLoader(DependencyLoader):
    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        absolute_path = path_lookup.resolve(dependency.path)
        if not absolute_path:
            return None
        language = SUPPORTED_EXTENSION_LANGUAGES.get(absolute_path.suffix.lower())
        if not language:
            return StubContainer(absolute_path)
        for encoding in ("utf-8", "ascii"):
            try:
                code = absolute_path.read_text(encoding)
                return LocalFile(absolute_path, code, language)
            except UnicodeDecodeError:
                pass
        return None

    def exists(self, path: Path) -> bool:
        return path.exists()

    def __repr__(self):
        return "FileLoader()"


class FolderLoader(FileLoader):

    def __init__(self, file_loader: FileLoader):
        self._file_loader = file_loader

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        absolute_path = path_lookup.resolve(dependency.path)
        if not absolute_path:
            return None
        return Folder(absolute_path, self._file_loader, self)


class ImportFileResolver(BaseImportResolver, BaseFileResolver):

    def __init__(self, file_loader: FileLoader, whitelist: Whitelist):
        super().__init__()
        self._whitelist = whitelist
        self._file_loader = file_loader

    def resolve_local_file(self, path_lookup, path: Path) -> MaybeDependency:
        absolute_path = path_lookup.resolve(path)
        if absolute_path:
            return MaybeDependency(Dependency(self._file_loader, absolute_path), [])
        problem = DependencyProblem("file-not-found", f"File not found: {path.as_posix()}")
        return MaybeDependency(None, [problem])

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        maybe = self._resolve_whitelist(name)
        if maybe is not None:
            return maybe
        maybe = self._resolve_import(path_lookup, name)
        if maybe is not None:
            return maybe
        return self._fail('import-not-found', f"Could not locate import: {name}")

    def _resolve_whitelist(self, name: str) -> MaybeDependency | None:
        # TODO attach compatibility to dependency, see https://github.com/databrickslabs/ucx/issues/1382
        compatibility = self._whitelist.compatibility(name)
        if compatibility == UCCompatibility.FULL:
            return MaybeDependency(None, [])
        if compatibility == UCCompatibility.NONE:
            # TODO move to linter, see https://github.com/databrickslabs/ucx/issues/1527
            problem = DependencyProblem("dependency-check", f"Use of dependency {name} is deprecated")
            return MaybeDependency(None, [problem])
        if compatibility == UCCompatibility.PARTIAL:
            problem = DependencyProblem("dependency-check", f"Package {name} is only partially supported by UC")
            return MaybeDependency(None, [problem])
        return None

    def _resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency | None:
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
        return None

    @staticmethod
    def _fail(code: str, message: str):
        return MaybeDependency(None, [DependencyProblem(code, message)])

    def __repr__(self):
        return "ImportFileResolver()"
