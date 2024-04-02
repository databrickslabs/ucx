import ast
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from databricks.labs.ucx.hive_metastore.table_migrate import MigrationIndex
from databricks.labs.ucx.source_code.base import Advice, Deprecation, Fixer, Linter
from databricks.labs.ucx.source_code.queries import FromTable


@dataclass
class Matcher(ABC):
    method_name: str
    min_args: int
    max_args: int
    table_arg_index: int

    def matches(self, node: ast.AST):
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and self.min_args <= len(node.args) <= self.max_args
            and isinstance(node.args[self.table_arg_index], ast.Constant)
        )

    @abstractmethod
    def lint(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> Iterator[Advice]:
        raise NotImplementedError()

    @abstractmethod
    def apply(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> None:
        raise NotImplementedError()


@dataclass
class QueryMatcher(Matcher):

    def lint(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> Iterator[Advice]:
        table_arg = node.args[self.table_arg_index]
        assert isinstance(table_arg, ast.Constant)
        for advice in from_table.lint(table_arg.value):
            yield advice.replace(
                start_line=node.lineno,
                start_col=node.col_offset,
                end_line=node.end_lineno,
                end_col=node.end_col_offset,
            )

    def apply(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> None:
        table_arg = node.args[self.table_arg_index]
        assert isinstance(table_arg, ast.Constant)
        new_query = from_table.apply(table_arg.value)
        table_arg.value = new_query


@dataclass
class TableNameMatcher(Matcher):

    def lint(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> Iterator[Advice]:
        table_arg = node.args[self.table_arg_index]
        assert isinstance(table_arg, ast.Constant)
        dst = self._find_dest(index, table_arg.value)
        if dst is not None:
            yield Deprecation(
                code='table-migrate',
                message=f"Table {table_arg.value} is migrated to {dst.destination()} in Unity Catalog",
                # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
                start_line=node.lineno,
                start_col=node.col_offset,
                end_line=node.end_lineno or 0,
                end_col=node.end_col_offset or 0,
            )

    def apply(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> None:
        table_arg = node.args[self.table_arg_index]
        assert isinstance(table_arg, ast.Constant)
        dst = self._find_dest(index, table_arg.value)
        if dst is not None:
            table_arg.value = dst.destination()

    @staticmethod
    def _find_dest(index: MigrationIndex, value: str):
        parts = value.split(".")
        return None if len(parts) != 2 else index.get(parts[0], parts[1])


class SparkMatchers:

    def __init__(self):
        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html
        spark_session_matchers = [QueryMatcher("sql", 1, 1000, 0), TableNameMatcher("table", 1, 1, 0)]

        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.html
        spark_catalog_matchers = [
            TableNameMatcher("cacheTable", 1, 2, 0),
            TableNameMatcher("createTable", 1, 1000, 0),
            TableNameMatcher("createExternalTable", 1, 1000, 0),
            TableNameMatcher("getTable", 1, 1, 0),
            TableNameMatcher("isCached", 1, 1, 0),
            TableNameMatcher("listColumns", 1, 2, 0),
            TableNameMatcher("tableExists", 1, 2, 0),
            TableNameMatcher("recoverPartitions", 1, 1, 0),
            TableNameMatcher("refreshTable", 1, 1, 0),
            TableNameMatcher("uncacheTable", 1, 1, 0),
            # TODO listTables: raise warning ?
        ]

        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html
        spark_dataframe_matchers = [
            TableNameMatcher("writeTo", 1, 1, 0),
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
        spark_dataframereader_matchers = [
            TableNameMatcher("table", 1, 1, 0),  # TODO good example of collision, see SPARK_SESSION_CALLS
        ]

        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html
        spark_dataframewriter_matchers = [
            TableNameMatcher("insertInto", 1, 2, 0),
            # TODO jdbc: could the url be a databricks url, raise warning ?
            TableNameMatcher("saveAsTable", 1, 4, 0),
        ]

        # nothing to migrate in DataFrameWriterV2, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriterV2.html
        # nothing to migrate in UDFRegistration, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UDFRegistration.html

        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UDTFRegistration.html
        spark_udtfregistration_matchers = [
            TableNameMatcher("register", 1, 2, 0),
        ]

        # nothing to migrate in UserDefinedFunction, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UserDefinedFunction.html
        # nothing to migrate in UserDefinedTableFunction, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UserDefinedTableFunction.html
        self._matchers = {}
        for matcher in (
            spark_session_matchers
            + spark_catalog_matchers
            + spark_dataframe_matchers
            + spark_dataframereader_matchers
            + spark_dataframewriter_matchers
            + spark_udtfregistration_matchers
        ):
            self._matchers[matcher.method_name] = matcher

    @property
    def matchers(self):
        return self._matchers


class SparkSql(Linter, Fixer):

    _spark_matchers = SparkMatchers()

    def __init__(self, from_table: FromTable, index: MigrationIndex):
        self._from_table = from_table
        self._index = index

    def name(self) -> str:
        # this is the same fixer, just in a different language context
        return self._from_table.name()

    def lint(self, code: str) -> Iterable[Advice]:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            matcher = self._find_matcher(node)
            if matcher is None:
                continue
            assert isinstance(node, ast.Call)
            yield from matcher.lint(self._from_table, self._index, node)

    def apply(self, code: str) -> str:
        tree = ast.parse(code)
        # we won't be doing it like this in production, but for the sake of the example
        for node in ast.walk(tree):
            matcher = self._find_matcher(node)
            if matcher is None:
                continue
            assert isinstance(node, ast.Call)
            matcher.apply(self._from_table, self._index, node)
        return ast.unparse(tree)

    def _find_matcher(self, node: ast.AST):
        if not isinstance(node, ast.Call):
            return None
        if not isinstance(node.func, ast.Attribute):
            return None
        matcher = self._spark_matchers.matchers.get(node.func.attr, None)
        if matcher is None:
            return None
        return matcher if matcher.matches(node) else None
