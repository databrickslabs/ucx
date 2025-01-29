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
)
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python.python_analyzer import PythonCodeAnalyzer


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
