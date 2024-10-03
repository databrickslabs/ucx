import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import TypeVar

from astroid import Attribute, Call, Const, Name, NodeNG  # type: ignore
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import (
    Advice,
    Advisory,
    Deprecation,
    CurrentSessionState,
    PythonLinter,
    SqlLinter,
    Fixer,
    UsedTable,
    TableInfoNode,
    TablePyCollector,
    TableSqlCollector,
    DfsaPyCollector,
    DfsaSqlCollector,
)
from databricks.labs.ucx.source_code.linters.directfs import DIRECT_FS_ACCESS_PATTERNS, DirectFsAccessNode
from databricks.labs.ucx.source_code.python.python_infer import InferredValue
from databricks.labs.ucx.source_code.linters.from_table import FromTableSqlLinter
from databricks.labs.ucx.source_code.python.python_ast import Tree, TreeHelper, MatchingVisitor

logger = logging.getLogger(__name__)


@dataclass
class _TableNameMatcher(ABC):
    method_name: str
    min_args: int
    max_args: int
    table_arg_index: int
    table_arg_name: str | None = None
    call_context: dict[str, set[str]] | None = None
    session_state: CurrentSessionState | None = None
    is_read: bool | None = None
    is_write: bool | None = None

    def matches(self, node: NodeNG):
        return (
            isinstance(node, Call)
            and self._get_table_arg(node) is not None
            and isinstance(node.func, Attribute)
            and Tree(node.func.expr).is_from_module("spark")
        )

    @abstractmethod
    def lint(
        self, from_table: FromTableSqlLinter, index: TableMigrationIndex, session_state: CurrentSessionState, node: Call
    ) -> Iterable[Advice]: ...

    @abstractmethod
    def apply(self, from_table: FromTableSqlLinter, index: TableMigrationIndex, node: Call) -> None: ...

    @abstractmethod
    def collect_tables(
        self, from_table: FromTableSqlLinter, index: TableMigrationIndex, session_state: CurrentSessionState, node: Call
    ) -> Iterable[UsedTable]: ...

    def _get_table_arg(self, node: Call):
        node_argc = len(node.args)
        if self.min_args <= node_argc <= self.max_args and self.table_arg_index < node_argc:
            return node.args[self.table_arg_index]
        if not self.table_arg_name:
            return None
        if not node.keywords:
            return None
        for keyword in node.keywords:
            if keyword.arg == self.table_arg_name:
                return keyword.value
        return None

    def _check_call_context(self, node: Call) -> bool:
        assert isinstance(node.func, Attribute)  # Avoid linter warning
        func_name = node.func.attrname
        qualified_name = TreeHelper.get_full_function_name(node)

        # Check if the call_context is None as that means all calls are checked
        if self.call_context is None:
            return True

        # Get the qualified names from the call_context dictionary
        qualified_names = self.call_context.get(func_name)

        # Check if the qualified name is in the set of qualified names that are allowed
        return qualified_name in qualified_names if qualified_names else False


@dataclass
class SparkCallMatcher(_TableNameMatcher):

    def collect_tables(
        self, from_table: FromTableSqlLinter, index: TableMigrationIndex, session_state: CurrentSessionState, node: Call
    ) -> Iterable[UsedTable]:
        for used_table in self._collect_tables(from_table, session_state, node):
            if not used_table:
                continue
            yield used_table[1]

    def _collect_tables(
        self, from_table: FromTableSqlLinter, session_state: CurrentSessionState, node: Call
    ) -> Iterable[tuple[str, UsedTable] | None]:
        table_arg = self._get_table_arg(node)
        if table_arg is None:
            return
        for inferred in InferredValue.infer_from_node(table_arg, session_state):
            if not inferred.is_inferred():
                yield None
                continue
            table_name = inferred.as_string().strip("'").strip('"')
            info = UsedTable.parse(table_name, from_table.schema)
            yield table_name, info

    def lint(
        self, from_table: FromTableSqlLinter, index: TableMigrationIndex, session_state: CurrentSessionState, node: Call
    ) -> Iterable[Advice]:
        for used_table in self._collect_tables(from_table, session_state, node):
            if not used_table:
                yield Advisory.from_node(
                    code='cannot-autofix-table-reference',
                    message=f"Can't migrate '{node.as_string()}' because its table name argument cannot be computed",
                    node=node,
                )
                continue
            dst = self._find_dest(index, used_table[1])
            if dst is None:
                continue
            yield Deprecation.from_node(
                code='table-migrated-to-uc',
                message=f"Table {used_table[0]} is migrated to {dst.destination()} in Unity Catalog",
                # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
                node=node,
            )

    def apply(self, from_table: FromTableSqlLinter, index: TableMigrationIndex, node: Call) -> None:
        table_arg = self._get_table_arg(node)
        assert isinstance(table_arg, Const)
        # TODO locate constant when value is inferred
        info = UsedTable.parse(table_arg.value, from_table.schema)
        dst = self._find_dest(index, info)
        if dst is not None:
            table_arg.value = dst.destination()

    @classmethod
    def _find_dest(cls, index: TableMigrationIndex, table: UsedTable):
        if table.catalog_name != "hive_metastore":
            return None
        return index.get(table.schema_name, table.table_name)


@dataclass
class ReturnValueMatcher(_TableNameMatcher):

    def matches(self, node: NodeNG):
        return (
            isinstance(node, Call) and isinstance(node.func, Attribute) and Tree(node.func.expr).is_from_module("spark")
        )

    def lint(
        self, from_table: FromTableSqlLinter, index: TableMigrationIndex, session_state: CurrentSessionState, node: Call
    ) -> Iterator[Advice]:
        assert isinstance(node.func, Attribute)  # always true, avoids a pylint warning
        yield Advisory.from_node(
            code='changed-result-format-in-uc',
            message=f"Call to '{node.func.attrname}' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.",
            node=node,
        )

    def apply(self, from_table: FromTableSqlLinter, index: TableMigrationIndex, node: Call) -> None:
        # No transformations to apply
        return

    def collect_tables(
        self, from_table: FromTableSqlLinter, index: TableMigrationIndex, session_state: CurrentSessionState, node: Call
    ) -> Iterable[UsedTable]:
        return []


T = TypeVar("T")


@dataclass
class DirectFilesystemAccessMatcher(_TableNameMatcher):

    def matches(self, node: NodeNG):
        return (
            isinstance(node, Call)
            and self._get_table_arg(node) is not None
            and isinstance(node.func, Attribute)
            and (Tree(node.func.expr).is_from_module("spark") or Tree(node.func.expr).is_from_module("dbutils"))
        )

    def lint(
        self,
        from_table: FromTableSqlLinter,
        index: TableMigrationIndex,
        session_state: CurrentSessionState,
        node: NodeNG,
    ) -> Iterator[Advice]:
        table_arg = self._get_table_arg(node)
        for inferred in InferredValue.infer_from_node(table_arg):
            if not inferred.is_inferred():
                logger.debug(f"Could not infer value of {table_arg.as_string()}")
                continue
            value = inferred.as_string()
            if any(pattern.matches(value) for pattern in DIRECT_FS_ACCESS_PATTERNS):
                yield Deprecation.from_node(
                    code='direct-filesystem-access',
                    message=f"The use of direct filesystem references is deprecated: {value}",
                    node=node,
                )

    def apply(self, from_table: FromTableSqlLinter, index: TableMigrationIndex, node: Call) -> None:
        # No transformations to apply
        return

    def collect_tables(
        self, from_table: FromTableSqlLinter, index: TableMigrationIndex, session_state: CurrentSessionState, node: Call
    ) -> Iterable[UsedTable]:
        return []  # we don't collect tables through this matcher


class SparkTableNameMatchers:

    def __init__(self, dfsa_matchers_only: bool):

        spark_dfsa_matchers: list[_TableNameMatcher] = [
            DirectFilesystemAccessMatcher(
                "ls", 1, 1, 0, call_context={"ls": {"dbutils.fs.ls"}}, is_read=True, is_write=False
            ),
            DirectFilesystemAccessMatcher(
                "cp", 1, 2, 0, call_context={"cp": {"dbutils.fs.cp"}}, is_read=True, is_write=True
            ),
            DirectFilesystemAccessMatcher("rm", 1, 1, 0, call_context={"rm": {"dbutils.fs.rm"}}, is_write=True),
            DirectFilesystemAccessMatcher(
                "head", 1, 1, 0, call_context={"head": {"dbutils.fs.head"}}, is_read=True, is_write=False
            ),
            DirectFilesystemAccessMatcher(
                "put", 1, 2, 0, call_context={"put": {"dbutils.fs.put"}}, is_read=False, is_write=True
            ),
            DirectFilesystemAccessMatcher(
                "mkdirs", 1, 1, 0, call_context={"mkdirs": {"dbutils.fs.mkdirs"}}, is_read=False, is_write=True
            ),
            DirectFilesystemAccessMatcher(
                "mv", 1, 2, 0, call_context={"mv": {"dbutils.fs.mv"}}, is_read=False, is_write=True
            ),
            DirectFilesystemAccessMatcher("text", 1, 3, 0, is_read=True, is_write=False),
            DirectFilesystemAccessMatcher("csv", 1, 1000, 0, is_read=True, is_write=False),
            DirectFilesystemAccessMatcher("json", 1, 1000, 0, is_read=True, is_write=False),
            DirectFilesystemAccessMatcher("orc", 1, 1000, 0, is_read=True, is_write=False),
            DirectFilesystemAccessMatcher("parquet", 1, 1000, 0, is_read=True, is_write=False),
            DirectFilesystemAccessMatcher("save", 0, 1000, 0, "path", is_read=False, is_write=True),
            DirectFilesystemAccessMatcher("load", 0, 1000, 0, "path", is_read=True, is_write=False),
            DirectFilesystemAccessMatcher(
                "option", 1, 1000, 1, is_read=True, is_write=False
            ),  # Only .option("path", "xxx://bucket/path") will hit
            DirectFilesystemAccessMatcher("addFile", 1, 3, 0, is_read=False, is_write=True),
            DirectFilesystemAccessMatcher("binaryFiles", 1, 2, 0, is_read=True, is_write=False),
            DirectFilesystemAccessMatcher("binaryRecords", 1, 2, 0, is_read=True, is_write=False),
            DirectFilesystemAccessMatcher("dump_profiles", 1, 1, 0, is_read=True, is_write=False),
            DirectFilesystemAccessMatcher("hadoopFile", 1, 8, 0, is_read=True, is_write=False),
            DirectFilesystemAccessMatcher("newAPIHadoopFile", 1, 8, 0, is_read=True, is_write=False),
            DirectFilesystemAccessMatcher("pickleFile", 1, 3, 0, is_read=True, is_write=False),
            DirectFilesystemAccessMatcher("saveAsHadoopFile", 1, 8, 0, is_read=False, is_write=True),
            DirectFilesystemAccessMatcher("saveAsNewAPIHadoopFile", 1, 7, 0, is_read=False, is_write=True),
            DirectFilesystemAccessMatcher("saveAsPickleFile", 1, 2, 0, is_read=False, is_write=True),
            DirectFilesystemAccessMatcher("saveAsSequenceFile", 1, 2, 0, is_read=False, is_write=True),
            DirectFilesystemAccessMatcher("saveAsTextFile", 1, 2, 0, is_read=False, is_write=True),
            DirectFilesystemAccessMatcher("load_from_path", 1, 1, 0, is_read=True, is_write=False),
        ]
        if dfsa_matchers_only:
            self._make_matchers(spark_dfsa_matchers)
            return

        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html
        # spark.sql is handled by a dedicated linter
        spark_session_matchers: list[_TableNameMatcher] = [SparkCallMatcher("table", 1, 1, 0)]

        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.html
        spark_catalog_matchers: list[_TableNameMatcher] = [
            SparkCallMatcher("cacheTable", 1, 2, 0, "tableName"),
            SparkCallMatcher("createTable", 1, 1000, 0, "tableName"),
            SparkCallMatcher("createExternalTable", 1, 1000, 0, "tableName"),
            SparkCallMatcher("getTable", 1, 1, 0),
            SparkCallMatcher("isCached", 1, 1, 0),
            SparkCallMatcher("listColumns", 1, 2, 0, "tableName"),
            SparkCallMatcher("tableExists", 1, 2, 0, "tableName"),
            SparkCallMatcher("recoverPartitions", 1, 1, 0),
            SparkCallMatcher("refreshTable", 1, 1, 0),
            SparkCallMatcher("uncacheTable", 1, 1, 0),
            ReturnValueMatcher("listTables", 0, 2, 0),
        ]

        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html
        spark_dataframe_matchers: list[_TableNameMatcher] = [
            SparkCallMatcher("writeTo", 1, 1, 0),
        ]

        # nothing to migrate in Column, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.html
        # nothing to migrate in Observation, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Observation.html
        # nothing to migrate in Row, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Row.html
        # nothing to migrate in GroupedData, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.html
        # nothing to migrate in PandasCogroupedOps, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.PandasCogroupedOps.html
        # nothing to migrate in DataFrameNaFunctions, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html
        # nothing to migrate in DataFrameStatFunctions, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameStatFunctions.html
        # nothing to migrate in Window, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html

        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html
        spark_dataframereader_matchers: list[_TableNameMatcher] = [
            SparkCallMatcher("table", 1, 1, 0),  # TODO good example of collision, see spark_session_calls
        ]

        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html
        spark_dataframewriter_matchers: list[_TableNameMatcher] = [
            SparkCallMatcher("insertInto", 1, 2, 0, "tableName"),
            # TODO jdbc: could the url be a databricks url, raise warning ?
            SparkCallMatcher("saveAsTable", 1, 4, 0, "name"),
        ]

        # nothing to migrate in DataFrameWriterV2, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriterV2.html
        # nothing to migrate in UDFRegistration, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UDFRegistration.html

        # nothing to migrate in UserDefinedFunction, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UserDefinedFunction.html
        # nothing to migrate in UserDefinedTableFunction, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UserDefinedTableFunction.html
        self._make_matchers(
            spark_dfsa_matchers
            + spark_session_matchers
            + spark_catalog_matchers
            + spark_dataframe_matchers
            + spark_dataframereader_matchers
            + spark_dataframewriter_matchers
        )

    def _make_matchers(self, matchers: list[_TableNameMatcher]):
        self._matchers = {}
        for matcher in matchers:
            self._matchers[matcher.method_name] = matcher

    @property
    def matchers(self):
        return self._matchers


class SparkTableNamePyLinter(PythonLinter, Fixer, TablePyCollector):

    def __init__(self, from_table: FromTableSqlLinter, index: TableMigrationIndex, session_state):
        self._from_table = from_table
        self._index = index
        self._session_state = session_state
        self._spark_matchers = SparkTableNameMatchers(False).matchers

    @property
    def name(self) -> str:
        # this is the same fixer, just in a different language context
        return self._from_table.name

    def lint_tree(self, tree: Tree) -> Iterable[Advice]:
        for node in tree.walk():
            matcher = self._find_matcher(node)
            if matcher is None:
                continue
            assert isinstance(node, Call)
            yield from matcher.lint(self._from_table, self._index, self._session_state, node)

    def apply(self, code: str) -> str:
        tree = Tree.parse(code)
        # we won't be doing it like this in production, but for the sake of the example
        for node in tree.walk():
            matcher = self._find_matcher(node)
            if matcher is None:
                continue
            assert isinstance(node, Call)
            matcher.apply(self._from_table, self._index, node)
        return tree.node.as_string()

    def _find_matcher(self, node: NodeNG):
        if not isinstance(node, Call):
            return None
        if not isinstance(node.func, Attribute):
            return None
        matcher = self._spark_matchers.get(node.func.attrname, None)
        if matcher is None:
            return None
        return matcher if matcher.matches(node) else None

    def collect_tables_from_tree(self, tree: Tree) -> Iterable[TableInfoNode]:
        for node in tree.walk():
            matcher = self._find_matcher(node)
            if matcher is None:
                continue
            assert isinstance(node, Call)
            yield from matcher.collect_tables(self._from_table, self._index, self._session_state, node)


class _SparkSqlAnalyzer:

    @classmethod
    def _visit_call_nodes(cls, tree: Tree) -> Iterable[tuple[Call, NodeNG]]:
        visitor = MatchingVisitor(Call, [("sql", Attribute), ("spark", Name)])
        visitor.visit(tree.node)
        for call_node in visitor.matched_nodes:
            query = TreeHelper.get_arg(call_node, arg_index=0, arg_name="sqlQuery")
            if query is None:
                continue
            yield call_node, query


class SparkSqlPyLinter(_SparkSqlAnalyzer, PythonLinter, Fixer):

    def __init__(self, sql_linter: SqlLinter, sql_fixer: Fixer | None):
        self._sql_linter = sql_linter
        self._sql_fixer = sql_fixer

    @property
    def name(self) -> str:
        return "<none>" if self._sql_fixer is None else self._sql_fixer.name

    def lint_tree(self, tree: Tree) -> Iterable[Advice]:
        for call_node, query in self._visit_call_nodes(tree):
            for value in InferredValue.infer_from_node(query):
                if not value.is_inferred():
                    yield Advisory.from_node(
                        code="cannot-autofix-table-reference",
                        message=f"Can't migrate table_name argument in '{query.as_string()}' because its value cannot be computed",
                        node=call_node,
                    )
                    continue
                for advice in self._sql_linter.lint(value.as_string()):
                    yield advice.replace_from_node(call_node)

    def apply(self, code: str) -> str:
        if not self._sql_fixer:
            return code
        tree = Tree.normalize_and_parse(code)
        for _call_node, query in self._visit_call_nodes(tree):
            if not isinstance(query, Const) or not isinstance(query.value, str):
                continue
            # TODO avoid applying same fix multiple times
            # this requires changing 'apply' API in order to check advice fragment location
            new_query = self._sql_fixer.apply(query.value)
            query.value = new_query
        return tree.node.as_string()


class SparkSqlDfsaPyCollector(_SparkSqlAnalyzer, DfsaPyCollector):

    def __init__(self, sql_collector: DfsaSqlCollector):
        self._sql_collector = sql_collector

    def collect_dfsas_from_tree(self, tree: Tree) -> Iterable[DirectFsAccessNode]:
        assert self._sql_collector
        for call_node, query in self._visit_call_nodes(tree):
            for value in InferredValue.infer_from_node(query):
                if not value.is_inferred():
                    continue  # TODO error handling strategy
                for dfsa in self._sql_collector.collect_dfsas(value.as_string()):
                    yield DirectFsAccessNode(dfsa, call_node)


class SparkSqlTablePyCollector(_SparkSqlAnalyzer, TablePyCollector):

    def __init__(self, sql_collector: TableSqlCollector):
        self._sql_collector = sql_collector

    def collect_tables_from_tree(self, tree: Tree) -> Iterable[TableInfoNode]:
        assert self._sql_collector
        for call_node, query in self._visit_call_nodes(tree):
            for value in InferredValue.infer_from_node(query):
                if not value.is_inferred():
                    continue  # TODO error handling strategy
                for table in self._sql_collector.collect_tables(value.as_string()):
                    yield TableInfoNode(table, call_node)
