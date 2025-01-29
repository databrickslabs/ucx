from __future__ import annotations

import dataclasses
import logging
from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import file_language, safe_read_text
from databricks.labs.ucx.source_code.graph import (
    SourceContainer,
    DependencyGraph,
    DependencyProblem,
    InheritedContext,
    DependencyLoader,
    Dependency,
)
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python.python_analyzer import PythonCodeAnalyzer


logger = logging.getLogger(__name__)


class LocalFile(SourceContainer):

    def __init__(self, path: Path, source: str, language: Language):
        self._path = path
        self._original_code = source
        self.language = language

    @property
    def content(self) -> str:
        return self._original_code

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        if self.language == Language.PYTHON:
            context = parent.new_dependency_graph_context()
            analyzer = PythonCodeAnalyzer(context, self._original_code)
            problems = analyzer.build_graph()
            for idx, problem in enumerate(problems):
                if problem.has_missing_path():
                    problems[idx] = dataclasses.replace(problem, source_path=self._path)
            return problems
        # supported language that does not generate dependencies
        if self.language == Language.SQL:
            return []
        logger.warning(f"Unsupported language: {self.language}")
        return []

    def build_inherited_context(self, graph: DependencyGraph, child_path: Path) -> InheritedContext:
        if self.language == CellLanguage.PYTHON:
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
    """Loader for a file dependency."""

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> LocalFile | StubContainer | None:
        """Load the dependency."""
        resolved_path = path_lookup.resolve(dependency.path)
        if not resolved_path:
            return None
        language = file_language(resolved_path)
        if not language:
            return StubContainer(resolved_path)
        content = safe_read_text(resolved_path)
        if not content:
            return None
        return LocalFile(resolved_path, content, language)

    def exists(self, path: Path) -> bool:
        return path.exists()

    def __repr__(self):
        return "FileLoader()"
