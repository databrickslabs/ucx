from __future__ import annotations

import dataclasses
import logging
from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import file_language
from databricks.labs.ucx.source_code.graph import (
    SourceContainer,
    DependencyGraph,
    DependencyProblem,
    InheritedContext,
    DependencyLoader,
    Dependency,
    BaseImportResolver,
    BaseFileResolver,
    MaybeDependency,
)
from databricks.labs.ucx.source_code.known import KnownList
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.linters.python import PythonCodeAnalyzer


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
                if problem.has_missing_path():
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
                if problem.has_missing_path():
                    problems[idx] = dataclasses.replace(problem, source_path=self._path)
            return dataclasses.replace(inherited, problems=problems)
        return InheritedContext(None, False, [])

    def __repr__(self):
        return f"<LocalFile {self._path}>"


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


class KnownContainer(SourceContainer):
    """A container for known libraries."""

    def __init__(self, path: Path, problems: list[DependencyProblem]):
        super().__init__()
        self._path = path
        self._problems = problems

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        """Return the known problems."""
        _ = parent
        return self._problems


class KnownLoader(DependencyLoader):
    """Always load as `KnownContainer`.

    This loader is used in combination with the KnownList to load known dependencies and their known problems.
    """

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> KnownContainer:
        """Load the dependency."""
        _ = path_lookup
        if not isinstance(dependency, KnownDependency):
            raise RuntimeError("Only KnownDependency is supported")
        # Known library paths do not need to be resolved
        return KnownContainer(dependency.path, dependency.problems)


class KnownDependency(Dependency):
    """A dependency for known libraries, see :class:KnownList."""

    def __init__(self, module_name: str, problems: list[DependencyProblem]):
        known_url = "https://github.com/databrickslabs/ucx/blob/main/src/databricks/labs/ucx/source_code/known.json"
        # Note that Github does not support navigating JSON files, hence the #<module_name> does nothing.
        # https://docs.github.com/en/repositories/working-with-files/using-files/navigating-code-on-github
        super().__init__(KnownLoader(), Path(f"{known_url}#{module_name}"), inherits_context=False)
        self._module_name = module_name
        self.problems = problems


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
        dependency = KnownDependency(name, compatibility.problems)
        return MaybeDependency(dependency, [])

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
