from __future__ import annotations  # for type hints

import dataclasses
import logging
from collections.abc import Iterable, Callable
from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import (
    CurrentSessionState,
    LocatedAdvice,
    file_language,
    is_a_notebook,
)
from databricks.labs.ucx.source_code.graph import (
    BaseImportResolver,
    BaseFileResolver,
    Dependency,
    DependencyGraph,
    DependencyLoader,
    DependencyProblem,
    DependencyResolver,
    InheritedContext,
    MaybeDependency,
    MaybeGraph,
    SourceContainer,
)
from databricks.labs.ucx.source_code.graph_walkers import DependencyGraphWalker, LintingWalker
from databricks.labs.ucx.source_code.known import KnownList
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage, PythonCodeAnalyzer
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python.python_ast import Tree

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
            problems = analyzer.build_graph()
            for idx, problem in enumerate(problems):
                if problem.is_path_missing():
                    problems[idx] = dataclasses.replace(problem, source_path=self._path)
            return problems
        # supported language that does not generate dependencies
        if self._language is CellLanguage.SQL:
            return []
        logger.warning(f"Unsupported language: {self._language.language}")
        return []

    def build_inherited_context(self, graph: DependencyGraph, child_path: Path) -> InheritedContext:
        if self._language is CellLanguage.PYTHON:
            context = graph.new_dependency_graph_context()
            analyzer = PythonCodeAnalyzer(context, self._original_code)
            inherited = analyzer.build_inherited_context(child_path)
            problems = list(inherited.problems)
            for idx, problem in enumerate(problems):
                if problem.is_path_missing():
                    problems[idx] = dataclasses.replace(problem, source_path=self._path)
            return dataclasses.replace(inherited, problems=problems)
        return InheritedContext(None, False, [])

    def __repr__(self):
        return f"<LocalFile {self._path}>"


class Folder(SourceContainer):

    def __init__(
        self,
        path: Path,
        notebook_loader: NotebookLoader,
        file_loader: FileLoader,
        folder_loader: FolderLoader,
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

    def lint(self, path: Path) -> Iterable[LocatedAdvice]:
        """Lint local code generating advices on becoming Unity Catalog compatible.

        Parameters :
            path (Path) : The path to the resource(s) to lint. If the path is a directory, then all files within the
                directory and subdirectories are linted.
        """
        maybe_graph = self._build_dependency_graph_from_path(path)
        if maybe_graph.problems:
            for problem in maybe_graph.problems:
                yield problem.as_located_advice()
            return
        assert maybe_graph.graph
        walker = LintingWalker(maybe_graph.graph, self._path_lookup, self._context_factory)
        yield from walker

    def apply(self, path: Path) -> Iterable[LocatedAdvice]:
        """Apply local code fixes to become Unity Catalog compatible.

        Parameters :
            path (Path) : The path to the resource(s) to lint. If the path is a directory, then all files within the
                directory and subdirectories are linted.
        """
        maybe_graph = self._build_dependency_graph_from_path(path)
        if maybe_graph.problems:
            for problem in maybe_graph.problems:
                yield problem.as_located_advice()
            return
        assert maybe_graph.graph

        context_factory = self._context_factory

        class FixerWalker(DependencyGraphWalker[LocatedAdvice]):

            def _process_dependency(
                self, dependency: Dependency, path_lookup: PathLookup, inherited_tree: Tree | None
            ) -> Iterable[LocatedAdvice]:
                ctx = context_factory()
                # FileLinter will determine which file/notebook linter to use
                linter = FileLinter(ctx, path_lookup, dependency.path, inherited_tree)
                for advice in linter.apply():
                    yield LocatedAdvice(advice, dependency.path)

        yield from FixerWalker(maybe_graph.graph, self._path_lookup)

    def _build_dependency_graph_from_path(self, path: Path) -> MaybeGraph:
        """Build a dependency graph from the path.

        It tries to load the path as a directory, file or notebook.

        Returns :
            MaybeGraph : If the loading fails, the returned maybe graph contains a problem. Otherwise, returned maybe
            graph contains the graph.
        """
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
        if container is None:
            problem = DependencyProblem("dependency-not-found", "Dependency not found", source_path=path)
            return MaybeGraph(None, [problem])
        problems = list(container.build_dependency_graph(graph))
        if problems:
            return MaybeGraph(None, problems)
        return MaybeGraph(graph)


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
    def _fail(code: str, message: str) -> MaybeDependency:
        return MaybeDependency(None, [DependencyProblem(code, message)])

    def __repr__(self):
        return "ImportFileResolver()"
