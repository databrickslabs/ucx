import logging
from collections.abc import Iterable, Callable
from dataclasses import dataclass
from pathlib import Path

from sqlglot import Expression as SqlExpression, parse as parse_sql, ParseError as SqlParseError
from sqlglot.expressions import Identifier, Literal

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import (
    is_a_notebook,
    CurrentSessionState,
    file_language,
    guess_encoding,
    DIRECT_FS_REFS,
)
from databricks.labs.ucx.source_code.graph import DependencyGraph, Dependency
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.queries import FromTable


logger = logging.getLogger(__name__)


@dataclass
class DFSA:
    """A DFSA is a record describing a Direct File System Access"""

    path: str


class DfsaCollector:
    """DfsaCollector is responsible for collecting and storing DFSAs i.e. Direct File System Access records"""

    def __init__(
        self, path_lookup: PathLookup, session_state: CurrentSessionState, context_factory: Callable[[], LinterContext]
    ):
        self._path_lookup = path_lookup
        self._session_state = session_state
        self._context_factory = context_factory

    def collect(self, graph: DependencyGraph) -> Iterable[DFSA]:
        collected_paths: set[Path] = set()
        for dependency in graph.root_dependencies:
            root = dependency.path  # since it's a root
            yield from self._collect_from_dependency(dependency, graph, root, collected_paths)

    def _collect_from_dependency(
        self, dependency: Dependency, graph: DependencyGraph, root_path: Path, collected_paths: set[Path]
    ) -> Iterable[DFSA]:
        if dependency.path in collected_paths:
            return
        collected_paths.add(dependency.path)
        language = file_language(dependency.path)
        if not language:
            logger.warning(f"Unknown language for {dependency.path}")
            return
        source = dependency.path.read_text(guess_encoding(dependency.path))
        if is_a_notebook(dependency.path):
            yield from self._collect_from_notebook(dependency.path, source, language)
        elif dependency.path.is_file():
            yield from self._collect_from_source(dependency.path, source, language)
        maybe_graph = graph.locate_dependency(dependency.path)
        # dependency problems have already been reported while building the graph
        if maybe_graph.graph:
            child_graph = maybe_graph.graph
            for child_dependency in child_graph.local_dependencies:
                yield from self._collect_from_dependency(child_dependency, child_graph, root_path, collected_paths)

    def _collect_from_notebook(self, path: Path, source: str, language: Language) -> Iterable[DFSA]:
        notebook = Notebook.parse(path, source, language)
        for cell in notebook.cells:
            yield from self._collect_from_source(path, cell.original_code, cell.language.language)

    @classmethod
    def _collect_from_source(cls, path: Path, source: str, language: Language) -> Iterable[DFSA]:
        if language is Language.SQL:
            yield from cls._collect_from_sql(path, source)
            return
        logger.warning(f"Language {language.name} not supported yet!")

    @classmethod
    def _collect_from_sql(cls, _path: Path, source: str) -> Iterable[DFSA]:
        try:
            sqls = parse_sql(source, read='databricks')
            for sql in sqls:
                if not sql:
                    continue
                yield from cls._collect_from_literals(sql)
                yield from cls._collect_from_identifiers(sql)
        except SqlParseError as e:
            logger.debug(f"Failed to parse SQL: {source}", exc_info=e)
            yield FromTable.sql_parse_failure(source)

    @classmethod
    def _collect_from_literals(cls, expression: SqlExpression) -> Iterable[DFSA]:
        for literal in expression.find_all(Literal):
            if not isinstance(literal.this, str):
                logger.warning(f"Can't interpret {type(literal.this).__name__}")
            fs_path: str = literal.this
            for fs_ref in DIRECT_FS_REFS:
                if not fs_path.startswith(fs_ref):
                    continue
                yield DFSA(path=fs_path)
                break

    @classmethod
    def _collect_from_identifiers(cls, expression: SqlExpression) -> Iterable[DFSA]:
        for identifier in expression.find_all(Identifier):
            if not isinstance(identifier.this, str):
                logger.warning(f"Can't interpret {type(identifier.this).__name__}")
            fs_path: str = identifier.this
            for fs_ref in DIRECT_FS_REFS:
                if not fs_path.startswith(fs_ref):
                    continue
                yield DFSA(path=fs_path)
                break
