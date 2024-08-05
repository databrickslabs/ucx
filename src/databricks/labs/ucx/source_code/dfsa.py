import logging
from collections.abc import Iterable, Callable
from dataclasses import dataclass
from pathlib import Path

from databricks.sdk.service.workspace import Language

from sqlglot import Expression as SqlExpression, parse as parse_sql, ParseError as SqlParseError
from sqlglot.expressions import Literal, LocationProperty, Table as SqlTable

from databricks.labs.ucx.source_code.base import is_a_notebook, CurrentSessionState, file_language, guess_encoding, \
    DIRECT_FS_REFS
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
        if is_a_notebook(dependency.path):
            yield from self._collect_from_notebook(dependency.path)
        elif dependency.path.is_file():
            yield from self._collect_from_file(dependency.path)
        maybe_graph = graph.locate_dependency(dependency.path)
        # dependency problems have already been reported while building the graph
        if maybe_graph.graph:
            child_graph = maybe_graph.graph
            for child_dependency in child_graph.local_dependencies:
                yield from self._collect_from_dependency(child_dependency, child_graph, root_path, collected_paths)

    def _collect_from_file(self, path: Path) -> Iterable[DFSA]:
        language = file_language(path)
        source = path.read_text(guess_encoding(path))
        yield from self._collect_from_source(path, source, language)

    def _collect_from_notebook(self, path: Path) -> Iterable[DFSA]:
        language = file_language(path)
        source = path.read_text(guess_encoding(path))
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
    def _collect_from_sql(cls, path: Path, source: str) -> Iterable[DFSA]:
        try:
            sqls = parse_sql(source, read='databricks')
            for sql in sqls:
                if not sql:
                    continue
                yield from cls._collect_from_sql_expression(sql)
        except SqlParseError as e:
            logger.debug(f"Failed to parse SQL: {source}", exc_info=e)
            yield FromTable.sql_parse_failure(source)

    @classmethod
    def _collect_from_sql_expression(cls, expression: SqlExpression) -> Iterable[DFSA]:
        for property in expression.find_all(LocationProperty):
            if not isinstance(property.this, Literal):
                logger.warning(f"Can't interpret {type(property.this).__name__}")
            literal: Literal = property.this
            if not isinstance(literal.this, str):
                logger.warning(f"Can't interpret {type(literal.this).__name__}")
            fs_path: str = literal.this
            for fsref in DIRECT_FS_REFS:
                if not fs_path.startswith(fsref):
                    continue
                yield DFSA(path=fs_path)
                break
