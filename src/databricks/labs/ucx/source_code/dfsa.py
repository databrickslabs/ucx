import logging
from collections.abc import Iterable
from functools import partial
from pathlib import Path

from sqlglot import Expression as SqlExpression, parse as parse_sql, ParseError as SqlParseError
from sqlglot.expressions import AlterTable, Create, Delete, Drop, Identifier, Insert, Literal, Select

from databricks.sdk.service.workspace import Language
from databricks.sdk.service.sql import Query

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.source_code.base import (
    is_a_notebook,
    CurrentSessionState,
    file_language,
    guess_encoding,
    DIRECT_FS_REFS,
    DFSA,
)
from databricks.labs.ucx.source_code.graph import DependencyGraph, Dependency
from databricks.labs.ucx.source_code.python.python_ast import Tree
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.python.python_analyzer import PythonCodeAnalyzer
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.queries import FromTable


logger = logging.getLogger(__name__)


class DfsaCrawler(CrawlerBase):

    def __init__(self, backend: SqlBackend, schema: str):
        """
        Initializes a DFSACrawler instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, "direct_file_system_access", DFSA)

    def append(self, dfsa: DFSA):
        self._append_records([dfsa])

    def snapshot(self) -> Iterable[DFSA]:
        return self._snapshot(partial(self._try_load), lambda: [])

    def _try_load(self) -> Iterable[DFSA]:
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield DFSA(*row)


class DfsaCollector:
    """DfsaCollector is responsible for collecting and storing DFSAs i.e. Direct File System Access records"""

    def __init__(self, crawler: DfsaCrawler, path_lookup: PathLookup, session_state: CurrentSessionState):
        self._crawler = crawler
        self._path_lookup = path_lookup
        self._session_state = session_state

    def collect_from_workspace_queries(self, ws) -> Iterable[DFSA]:
        for query in ws.queries.list():
            yield from self.collect_from_query(query)

    def collect_from_query(self, query: Query) -> Iterable[DFSA]:
        if query.query is None:
            return
        name: str = query.name or "<anonymous>"
        source_path = Path(name) if query.parent is None else Path(query.parent) / name
        for dfsa in self._collect_from_sql(query.query):
            dfsa = DFSA(
                source_type="QUERY",
                source_id=str(source_path),
                path=dfsa.path,
                is_read=dfsa.is_read,
                is_write=dfsa.is_write,
            )
            self._crawler.append(dfsa)
            yield dfsa

    def collect_from_graph(self, graph: DependencyGraph) -> Iterable[DFSA]:
        collected_paths: set[Path] = set()
        for dependency in graph.root_dependencies:
            root = dependency.path  # since it's a root
            for dfsa in self._collect_from_dependency(dependency, graph, root, collected_paths):
                self._crawler.append(dfsa)
                yield dfsa

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
        inherited_tree = graph.build_inherited_tree(root_path, dependency.path)
        source = dependency.path.read_text(guess_encoding(dependency.path))
        if is_a_notebook(dependency.path):
            yield from self._collect_from_notebook(dependency.path, source, language, graph, inherited_tree)
        elif dependency.path.is_file():
            yield from self._collect_from_source(dependency.path, source, language, graph, inherited_tree)
        maybe_graph = graph.locate_dependency(dependency.path)
        # dependency problems have already been reported while building the graph
        if maybe_graph.graph:
            child_graph = maybe_graph.graph
            for child_dependency in child_graph.local_dependencies:
                yield from self._collect_from_dependency(child_dependency, child_graph, root_path, collected_paths)

    def _collect_from_notebook(
        self, path: Path, source: str, language: Language, graph: DependencyGraph, inherited_tree: Tree | None
    ) -> Iterable[DFSA]:
        notebook = Notebook.parse(path, source, language)
        for cell in notebook.cells:
            for dfsa in self._collect_from_source(
                path, cell.original_code, cell.language.language, graph, inherited_tree
            ):
                yield DFSA(
                    source_type="NOTEBOOK",
                    source_id=str(path),
                    path=dfsa.path,
                    is_read=dfsa.is_read,
                    is_write=dfsa.is_write,
                )
            if cell.language.language is Language.PYTHON:
                if inherited_tree is None:
                    inherited_tree = Tree.new_module()
                tree = Tree.normalize_and_parse(cell.original_code)
                inherited_tree.append_tree(tree)

    @classmethod
    def _collect_from_source(
        cls, path: Path, source: str, language: Language, graph: DependencyGraph, inherited_tree: Tree | None
    ) -> Iterable[DFSA]:
        iterable: Iterable[DFSA] | None = None
        if language is Language.SQL:
            iterable = cls._collect_from_sql(source)
        if language is Language.PYTHON:
            iterable = cls._collect_from_python(source, graph, inherited_tree)
        if iterable is None:
            logger.warning(f"Language {language.name} not supported yet!")
            return
        for dfsa in iterable:
            yield DFSA(
                source_type="FILE", source_id=str(path), path=dfsa.path, is_read=dfsa.is_read, is_write=dfsa.is_write
            )

    @classmethod
    def _collect_from_python(cls, source: str, graph: DependencyGraph, inherited_tree: Tree | None) -> Iterable[DFSA]:
        analyzer = PythonCodeAnalyzer(graph.new_dependency_graph_context(), source)
        yield from analyzer.collect_dfsas(inherited_tree)

    @classmethod
    def _collect_from_sql(cls, source: str) -> Iterable[DFSA]:
        try:
            sqls = parse_sql(source, read='databricks')
            for sql in sqls:
                if not sql:
                    continue
                yield from cls._collect_from_sql_literals(sql)
                yield from cls._collect_from_sql_identifiers(sql)
        except SqlParseError as e:
            logger.debug(f"Failed to parse SQL: {source}", exc_info=e)
            yield FromTable.sql_parse_failure(source)

    @classmethod
    def _collect_from_sql_literals(cls, expression: SqlExpression) -> Iterable[DFSA]:
        for literal in expression.find_all(Literal):
            if not isinstance(literal.this, str):
                logger.warning(f"Can't interpret {type(literal.this).__name__}")
            fs_path: str = literal.this
            if any(fs_path.startswith(fs_ref) for fs_ref in DIRECT_FS_REFS):
                is_read = cls._is_read(literal)
                is_write = cls._is_write(literal)
                yield DFSA(
                    source_type=DFSA.UNKNOWN, source_id=DFSA.UNKNOWN, path=fs_path, is_read=is_read, is_write=is_write
                )

    @classmethod
    def _collect_from_sql_identifiers(cls, expression: SqlExpression) -> Iterable[DFSA]:
        for identifier in expression.find_all(Identifier):
            if not isinstance(identifier.this, str):
                logger.warning(f"Can't interpret {type(identifier.this).__name__}")
            fs_path: str = identifier.this
            if any(fs_path.startswith(fs_ref) for fs_ref in DIRECT_FS_REFS):
                is_read = cls._is_read(identifier)
                is_write = cls._is_write(identifier)
                yield DFSA(
                    source_type=DFSA.UNKNOWN, source_id=DFSA.UNKNOWN, path=fs_path, is_read=is_read, is_write=is_write
                )

    @classmethod
    def _is_read(cls, expression: SqlExpression | None) -> bool:
        expression = cls._walk_up(expression)
        return isinstance(expression, Select)

    @classmethod
    def _is_write(cls, expression: SqlExpression | None) -> bool:
        expression = cls._walk_up(expression)
        return isinstance(expression, (Create, AlterTable, Drop, Insert, Delete))

    @classmethod
    def _walk_up(cls, expression: SqlExpression | None) -> SqlExpression | None:
        if expression is None:
            return None
        if isinstance(expression, (Create, AlterTable, Drop, Insert, Delete, Select)):
            return expression
        return cls._walk_up(expression.parent)
