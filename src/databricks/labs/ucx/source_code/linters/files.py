from __future__ import annotations  # for type hints

import logging
from collections.abc import Iterable, Callable
from pathlib import Path
import sys
from typing import TextIO

from databricks.labs.ucx.source_code.base import LocatedAdvice, CurrentSessionState, file_language, is_a_notebook
from databricks.labs.ucx.source_code.python.python_ast import Tree
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.known import KnownList
from databricks.sdk.service.workspace import Language
from databricks.labs.blueprint.tui import Prompts

from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage, PythonCodeAnalyzer
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
    InheritedContext,
    DependencyGraphWalker,
)

logger = logging.getLogger(__name__)


class LocalFile(SourceContainer):

    def __init__(self, path: Path, source: str, language: Language):
        self._path = path
        self._original_code = source
        # using CellLanguage so we can reuse the facilities it provides
        self._language = CellLanguage.of_language(language)

    @property
    def path(self) -> Path:
        return self._path

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        if self._language is CellLanguage.PYTHON:
            context = parent.new_dependency_graph_context()
            analyzer = PythonCodeAnalyzer(context, self._original_code)
            return analyzer.build_graph()
        # supported language that does not generate dependencies
        if self._language is CellLanguage.SQL:
            return []
        logger.warning(f"Unsupported language: {self._language.language}")
        return []

    def build_inherited_context(self, graph: DependencyGraph, child_path: Path) -> InheritedContext:
        if self._language is CellLanguage.PYTHON:
            context = graph.new_dependency_graph_context()
            analyzer = PythonCodeAnalyzer(context, self._original_code)
            return analyzer.build_inherited_context(child_path)
        return InheritedContext(None, False)

    def __repr__(self):
        return f"<LocalFile {self._path}>"


class Folder(SourceContainer):

    def __init__(
        self, path: Path, notebook_loader: NotebookLoader, file_loader: FileLoader, folder_loader: FolderLoader
    ):
        self._path = path
        self._notebook_loader = notebook_loader
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
            is_file = child_path.is_file()
            is_notebook = is_a_notebook(child_path)
            loader = self._notebook_loader if is_notebook else self._file_loader if is_file else self._folder_loader
            dependency = Dependency(loader, child_path, inherits_context=is_notebook)
            yield from parent.register_dependency(dependency).problems

    def __repr__(self):
        return f"<Folder {self._path}>"


class LocalCodeLinter:

    def __init__(
        self,
        notebook_loader: NotebookLoader,
        file_loader: FileLoader,
        folder_loader: FolderLoader,
        path_lookup: PathLookup,
        session_state: CurrentSessionState,
        dependency_resolver: DependencyResolver,
        context_factory: Callable[[], LinterContext],
    ) -> None:
        self._notebook_loader = notebook_loader
        self._file_loader = file_loader
        self._folder_loader = folder_loader
        self._path_lookup = path_lookup
        self._session_state = session_state
        self._dependency_resolver = dependency_resolver
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}
        self._context_factory = context_factory

    def lint(
        self,
        prompts: Prompts,
        path: Path | None,
        stdout: TextIO = sys.stdout,
    ) -> list[LocatedAdvice]:
        """Lint local code files looking for problems in notebooks and python files."""
        if path is None:
            response = prompts.question(
                "Which file or directory do you want to lint?",
                default=Path.cwd().as_posix(),
                validate=lambda p_: Path(p_).exists(),
            )
            path = Path(response)
        located_advices = list(self.lint_path(path))
        for located in located_advices:
            message = located.message_relative_to(path)
            stdout.write(f"{message}\n")
        return located_advices

    def lint_path(self, path: Path, linted_paths: set[Path] | None = None) -> Iterable[LocatedAdvice]:
        is_dir = path.is_dir()
        loader: DependencyLoader
        if is_a_notebook(path):
            loader = self._notebook_loader
        elif path.is_dir():
            loader = self._folder_loader
        else:
            loader = self._file_loader
        path_lookup = self._path_lookup.change_directory(path if is_dir else path.parent)
        root_dependency = Dependency(loader, path, not is_dir)  # don't inherit context when traversing folders
        graph = DependencyGraph(root_dependency, None, self._dependency_resolver, path_lookup, self._session_state)
        container = root_dependency.load(path_lookup)
        assert container is not None  # because we just created it
        problems = container.build_dependency_graph(graph)
        for problem in problems:
            problem_path = Path('UNKNOWN') if problem.is_path_missing() else problem.source_path.absolute()
            yield problem.as_advisory().for_path(problem_path)
        context_factory = self._context_factory
        session_state = self._session_state

        class LintingWalker(DependencyGraphWalker[LocatedAdvice]):

            def _process_dependency(
                self, dependency: Dependency, path_lookup: PathLookup, inherited_tree: Tree | None
            ) -> Iterable[LocatedAdvice]:
                ctx = context_factory()
                # FileLinter will determine which file/notebook linter to use
                linter = FileLinter(ctx, path_lookup, session_state, dependency.path, inherited_tree)
                for advice in linter.lint():
                    yield LocatedAdvice(advice, dependency.path)

        if linted_paths is None:
            linted_paths = set()
        walker = LintingWalker(graph, linted_paths, self._path_lookup)
        yield from walker


class LocalFileMigrator:
    """The LocalFileMigrator class is responsible for fixing code files based on their language."""

    def __init__(self, context_factory: Callable[[], LinterContext]):
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}
        self._context_factory = context_factory

    def apply(self, path: Path) -> bool:
        if path.is_dir():
            for child_path in path.iterdir():
                self.apply(child_path)
            return True
        return self._apply_file_fix(path)

    def _apply_file_fix(self, path: Path):
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
        context = self._context_factory()
        linter = context.linter(language)
        # Open the file and read the code
        with path.open("r") as f:
            try:
                code = f.read()
            except UnicodeDecodeError as e:
                logger.error(f"Could not decode file {path}: {e}")
                return False
            applied = False
            # Lint the code and apply fixes
            for advice in linter.lint(code):
                logger.info(f"Found: {advice}")
                fixer = context.fixer(language, advice.code)
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
        language = file_language(absolute_path)
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

    def __init__(self, notebook_loader: NotebookLoader, file_loader: FileLoader):
        self._notebook_loader = notebook_loader
        self._file_loader = file_loader

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        absolute_path = path_lookup.resolve(dependency.path)
        if not absolute_path:
            return None
        return Folder(absolute_path, self._notebook_loader, self._file_loader, self)


class ImportFileResolver(BaseImportResolver, BaseFileResolver):

    def __init__(self, file_loader: FileLoader, allow_list: KnownList):
        super().__init__()
        self._allow_list = allow_list
        self._file_loader = file_loader

    def resolve_file(self, path_lookup, path: Path) -> MaybeDependency:
        absolute_path = path_lookup.resolve(path)
        if absolute_path:
            return MaybeDependency(Dependency(self._file_loader, absolute_path), [])
        problem = DependencyProblem("file-not-found", f"File not found: {path.as_posix()}")
        return MaybeDependency(None, [problem])

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        maybe = self._resolve_allow_list(name)
        if maybe is not None:
            return maybe
        maybe = self._resolve_import(path_lookup, name)
        if maybe is not None:
            return maybe
        return self._fail('import-not-found', f"Could not locate import: {name}")

    def _resolve_allow_list(self, name: str) -> MaybeDependency | None:
        compatibility = self._allow_list.module_compatibility(name)
        if not compatibility.known:
            logger.debug(f"Resolving unknown import: {name}")
            return None
        if not compatibility.problems:
            return MaybeDependency(None, [])
        # TODO move to linter, see https://github.com/databrickslabs/ucx/issues/1527
        return MaybeDependency(None, compatibility.problems)

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
            dependency = Dependency(self._file_loader, absolute_path, False)
            return MaybeDependency(dependency, [])
        return None

    @staticmethod
    def _fail(code: str, message: str):
        return MaybeDependency(None, [DependencyProblem(code, message)])

    def __repr__(self):
        return "ImportFileResolver()"
