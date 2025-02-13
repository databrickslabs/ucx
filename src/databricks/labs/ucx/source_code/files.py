from __future__ import annotations

import dataclasses
import logging
from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import (
    back_up_path,
    infer_file_language_if_supported,
    revert_back_up_path,
    safe_read_text,
    safe_write_text,
)
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
    StubContainer,
)
from databricks.labs.ucx.source_code.known import KnownDependency, KnownList
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.linters.python import PythonCodeAnalyzer


logger = logging.getLogger(__name__)


class LocalFile(SourceContainer):
    """A container for accessing local files."""

    def __init__(self, path: Path, source: str, language: Language):
        self._path = path
        self._original_code = source
        self._migrated_code = source
        self.language = language

    @property
    def original_code(self) -> str:
        """The source code when creating the container."""
        return self._original_code

    @property
    def migrated_code(self) -> str:
        """The source code after fixing with a linter."""
        return self._migrated_code

    @migrated_code.setter
    def migrated_code(self, source: str) -> None:
        """Set the source code after fixing with a linter."""
        self._migrated_code = source

    def _safe_write_text(self, contents: str) -> int | None:
        """Write content to the local file."""
        return safe_write_text(self._path, contents)

    def _back_up_path(self) -> Path | None:
        """Back up the original file."""
        return back_up_path(self._path)

    def back_up_original_and_flush_migrated_code(self) -> int | None:
        """Back up the original file and flush the migrated code to the file.

        This is a single method to avoid overwriting the original file without a backup.

        Returns :
            int : The number of characters written. If None, nothing is written to the file.
        """
        if self._original_code == self._migrated_code:
            # Avoiding unnecessary back up and flush
            return len(self._migrated_code)
        backed_up_path = self._back_up_path()
        if not backed_up_path:
            # Failed to back up the original file, avoid overwriting existing file
            return None
        number_of_characters_written = self._safe_write_text(self._migrated_code)
        if number_of_characters_written is None:
            # Failed to overwrite original file, clean up by reverting backup
            revert_back_up_path(self._path)
        return number_of_characters_written

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        """The dependency graph for the local file."""
        if self.language == Language.PYTHON:
            context = parent.new_dependency_graph_context()
            analyzer = PythonCodeAnalyzer(context, self._original_code)
            problems = analyzer.build_graph()
            for idx, problem in enumerate(problems):
                if problem.has_missing_path():
                    problems[idx] = dataclasses.replace(problem, source_path=self._path)
            return problems
        if self.language == Language.SQL:  # SQL cannot refer other dependencies
            return []
        logger.warning(f"Unsupported language: {self.language}")
        return []

    def build_inherited_context(self, graph: DependencyGraph, child_path: Path) -> InheritedContext:
        if self.language == Language.PYTHON:
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


class FileLoader(DependencyLoader):
    """Loader for a file dependency.

    Args:
        exclude_paths (set[Path] | None) : A set of paths to load as
            class:`StubContainer`. If None, no paths are excluded.

            Note: The exclude paths are loaded as `StubContainer` to
            signal that the path is found, however, it should not be
            processed.
    """

    def __init__(self, *, exclude_paths: set[Path] | None = None):
        self._exclude_paths = exclude_paths or set[Path]()

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> LocalFile | StubContainer | None:
        """Load the dependency."""
        resolved_path = path_lookup.resolve(dependency.path)
        if not resolved_path:
            return None
        if resolved_path in self._exclude_paths:
            # Paths are excluded from further processing by loading them as stub container.
            # Note we don't return `None`, as the path is found.
            return StubContainer(resolved_path)
        language = infer_file_language_if_supported(resolved_path)
        if not language:
            return StubContainer(resolved_path)
        content = safe_read_text(resolved_path)
        if content is None:
            return None
        return LocalFile(resolved_path, content, language)

    def __repr__(self):
        return "FileLoader()"


class ImportFileResolver(BaseImportResolver, BaseFileResolver):

    def __init__(self, file_loader: FileLoader, *, allow_list: KnownList | None = None):
        super().__init__()
        self._file_loader = file_loader
        self._allow_list = allow_list or KnownList()

    def resolve_file(self, path_lookup, path: Path) -> MaybeDependency:
        absolute_path = path_lookup.resolve(path)
        if absolute_path:
            return MaybeDependency(Dependency(self._file_loader, absolute_path), [])
        problem = DependencyProblem("file-not-found", f"File not found: {path.as_posix()}")
        return MaybeDependency(None, [problem])

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        """Resolve an import by name.

        1. Check the known modules.
        2. Check the import on the path lookup.
        """
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
