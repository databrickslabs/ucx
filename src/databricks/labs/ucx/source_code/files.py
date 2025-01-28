from __future__ import annotations

import dataclasses
from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import file_language, safe_read_text
from databricks.labs.ucx.source_code.graph import (
    SourceContainer,
    DependencyGraph,
    DependencyProblem,
    logger,
    InheritedContext,
    DependencyLoader,
    Dependency,
)
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python.python_analyzer import PythonCodeAnalyzer


class LocalFile(SourceContainer):

    def __init__(self, path: Path, source: str, language: Language):
        self.path = path
        self.content = source
        self.language = language

    def write_text(self, content: str) -> None:
        """Write text to the local file."""
        if self.content == content:  # Avoid unnecessary write
            return
        self.path.write_text(content)
        self.content = content

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        if self.language == Language.PYTHON:
            context = parent.new_dependency_graph_context()
            analyzer = PythonCodeAnalyzer(context, self.content)
            problems = analyzer.build_graph()
            for idx, problem in enumerate(problems):
                if problem.is_path_missing():
                    problems[idx] = dataclasses.replace(problem, source_path=self.path)
            return problems
        # supported language that does not generate dependencies
        if self.language == Language.SQL:
            return []
        logger.warning(f"Unsupported language: {self.language}")
        return []

    def build_inherited_context(self, graph: DependencyGraph, child_path: Path) -> InheritedContext:
        if self.language == Language.PYTHON:
            context = graph.new_dependency_graph_context()
            analyzer = PythonCodeAnalyzer(context, self.content)
            inherited = analyzer.build_inherited_context(child_path)
            problems = list(inherited.problems)
            for idx, problem in enumerate(problems):
                if problem.is_path_missing():
                    problems[idx] = dataclasses.replace(problem, source_path=self.path)
            return dataclasses.replace(inherited, problems=problems)
        return InheritedContext(None, False, [])

    def __repr__(self):
        return f"<LocalFile {self.path}>"


class FileLoader(DependencyLoader):
    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
        absolute_path = path_lookup.resolve(dependency.path)
        if not absolute_path:
            return None
        language = file_language(absolute_path)
        if not language:
            return StubContainer(absolute_path)
        content = safe_read_text(absolute_path)
        if content is None:
            return None
        return LocalFile(absolute_path, content, language)

    def exists(self, path: Path) -> bool:
        return path.exists()

    def __repr__(self):
        return "FileLoader()"


class StubContainer(SourceContainer):

    def __init__(self, path: Path):
        super().__init__()
        self._path = path

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        return []
