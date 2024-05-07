import ast
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import (
    Advice,
    Advisory,
    Deprecation,
    Fixer,
    Linter,
)
from databricks.labs.ucx.source_code.queries import FromTable
from databricks.labs.ucx.source_code.ast_helpers import AstHelper


@dataclass
class Matcher(ABC):
    method_name: str
    min_args: int
    max_args: int
    table_arg_index: int
    table_arg_name: str | None = None
    call_context: dict[str, set[str]] | None = None

    def matches(self, node: ast.AST):
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and self._get_table_arg(node) is not None
        )

    @abstractmethod
    def lint(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> Iterator[Advice]:
        raise NotImplementedError()

    @abstractmethod
    def apply(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> None:
        raise NotImplementedError()

    def _get_table_arg(self, node: ast.Call):
        if len(node.args) > 0:
            return node.args[self.table_arg_index] if self.min_args <= len(node.args) <= self.max_args else None
        assert self.table_arg_name is not None
        if not node.keywords:
            return None
        arg = next(kw for kw in node.keywords if kw.arg == self.table_arg_name)
        return arg.value if arg is not None else None

    def _check_call_context(self, node: ast.Call) -> bool:
        assert isinstance(node.func, ast.Attribute)  # Avoid linter warning
        func_name = node.func.attr
        qualified_name = AstHelper.get_full_function_name(node)

        # Check if the call_context is None as that means all calls are checked
        if self.call_context is None:
            return True

        # Get the qualified names from the call_context dictionary
        qualified_names = self.call_context.get(func_name)

        # Check if the qualified name is in the set of qualified names that are allowed
        return qualified_name in qualified_names if qualified_names else False


@dataclass
class QueryMatcher(Matcher):

    def lint(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> Iterator[Advice]:
        table_arg = self._get_table_arg(node)
        if isinstance(table_arg, ast.Constant):
            for advice in from_table.lint(table_arg.value):
                yield advice.replace(
                    start_line=node.lineno,
                    start_col=node.col_offset,
                    end_line=node.end_lineno,
                    end_col=node.end_col_offset,
                )
        else:
            yield Advisory(
                code='table-migrate',
                message=f"Can't migrate '{node}' because its table name argument is not a constant",
                start_line=node.lineno,
                start_col=node.col_offset,
                end_line=node.end_lineno or 0,
                end_col=node.end_col_offset or 0,
            )

    def apply(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> None:
        table_arg = self._get_table_arg(node)
        assert isinstance(table_arg, ast.Constant)
        new_query = from_table.apply(table_arg.value)
        table_arg.value = new_query


@dataclass
class TableNameMatcher(Matcher):

    def lint(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> Iterator[Advice]:
        table_arg = self._get_table_arg(node)

        if not isinstance(table_arg, ast.Constant):
            assert isinstance(node.func, ast.Attribute)  # always true, avoids a pylint warning
            yield Advisory(
                code='table-migrate',
                message=f"Can't migrate '{node.func.attr}' because its table name argument is not a constant",
                start_line=node.lineno,
                start_col=node.col_offset,
                end_line=node.end_lineno or 0,
                end_col=node.end_col_offset or 0,
            )
            return

        dst = self._find_dest(index, table_arg.value, from_table.schema)
        if dst is None:
            return

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
        table_arg = self._get_table_arg(node)
        assert isinstance(table_arg, ast.Constant)
        dst = self._find_dest(index, table_arg.value, from_table.schema)
        if dst is not None:
            table_arg.value = dst.destination()

    @staticmethod
    def _find_dest(index: MigrationIndex, value: str, schema: str):
        parts = value.split(".")
        # Ensure that unqualified table references use the current schema
        if len(parts) == 1:
            return index.get(schema, parts[0])
        return None if len(parts) != 2 else index.get(parts[0], parts[1])


@dataclass
class ReturnValueMatcher(Matcher):

    def matches(self, node: ast.AST):
        return isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)

    def lint(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> Iterator[Advice]:
        assert isinstance(node.func, ast.Attribute)  # always true, avoids a pylint warning
        yield Advisory(
            code='table-migrate',
            message=f"Call to '{node.func.attr}' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.",
            start_line=node.lineno,
            start_col=node.col_offset,
            end_line=node.end_lineno or 0,
            end_col=node.end_col_offset or 0,
        )

    def apply(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> None:
        # No transformations to apply
        return


@dataclass
class DirectFilesystemAccessMatcher(Matcher):
    _DIRECT_FS_REFS = {
        "s3a://",
        "s3n://",
        "s3://",
        "wasb://",
        "wasbs://",
        "abfs://",
        "abfss://",
        "dbfs:/",
        "hdfs://",
        "file:/",
    }

    def matches(self, node: ast.AST):
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and self._get_table_arg(node) is not None
        )

    def lint(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> Iterator[Advice]:
        table_arg = self._get_table_arg(node)

        if not isinstance(table_arg, ast.Constant):
            return

        if any(table_arg.value.startswith(prefix) for prefix in self._DIRECT_FS_REFS):
            yield Deprecation(
                code='direct-filesystem-access',
                message=f"The use of direct filesystem references is deprecated: {table_arg.value}",
                start_line=node.lineno,
                start_col=node.col_offset,
                end_line=node.end_lineno or 0,
                end_col=node.end_col_offset or 0,
            )
            return

        if table_arg.value.startswith("/") and self._check_call_context(node):
            yield Deprecation(
                code='direct-filesystem-access',
                message=f"The use of default dbfs: references is deprecated: {table_arg.value}",
                start_line=node.lineno,
                start_col=node.col_offset,
                end_line=node.end_lineno or 0,
                end_col=node.end_col_offset or 0,
            )

    def apply(self, from_table: FromTable, index: MigrationIndex, node: ast.Call) -> None:
        # No transformations to apply
        return


class SparkMatchers:

    def __init__(self):
        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html
        spark_session_matchers = [QueryMatcher("sql", 1, 1000, 0, "sqlQuery"), TableNameMatcher("table", 1, 1, 0)]

        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.html
        spark_catalog_matchers = [
            TableNameMatcher("cacheTable", 1, 2, 0, "tableName"),
            TableNameMatcher("createTable", 1, 1000, 0, "tableName"),
            TableNameMatcher("createExternalTable", 1, 1000, 0, "tableName"),
            TableNameMatcher("getTable", 1, 1, 0),
            TableNameMatcher("table", 1, 1, 0),
            TableNameMatcher("isCached", 1, 1, 0),
            TableNameMatcher("listColumns", 1, 2, 0, "tableName"),
            TableNameMatcher("tableExists", 1, 2, 0, "tableName"),
            TableNameMatcher("recoverPartitions", 1, 1, 0),
            TableNameMatcher("refreshTable", 1, 1, 0),
            TableNameMatcher("uncacheTable", 1, 1, 0),
            ReturnValueMatcher("listTables", 0, 2, -1),
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
            TableNameMatcher("table", 1, 1, 0),  # TODO good example of collision, see spark_session_calls
        ]

        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html
        spark_dataframewriter_matchers = [
            TableNameMatcher("insertInto", 1, 2, 0, "tableName"),
            # TODO jdbc: could the url be a databricks url, raise warning ?
            TableNameMatcher("saveAsTable", 1, 4, 0, "name"),
        ]

        # nothing to migrate in DataFrameWriterV2, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriterV2.html
        # nothing to migrate in UDFRegistration, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UDFRegistration.html

        # see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UDTFRegistration.html
        spark_udtfregistration_matchers = [
            TableNameMatcher("register", 1, 2, 0, "name"),
        ]

        direct_fs_access_matchers = [
            DirectFilesystemAccessMatcher("ls", 1, 1, 0, call_context={"ls": {"dbutils.fs.ls"}}),
            DirectFilesystemAccessMatcher("cp", 1, 2, 0, call_context={"cp": {"dbutils.fs.cp"}}),
            DirectFilesystemAccessMatcher("rm", 1, 1, 0, call_context={"rm": {"dbutils.fs.rm"}}),
            DirectFilesystemAccessMatcher("head", 1, 1, 0, call_context={"head": {"dbutils.fs.head"}}),
            DirectFilesystemAccessMatcher("put", 1, 2, 0, call_context={"put": {"dbutils.fs.put"}}),
            DirectFilesystemAccessMatcher("mkdirs", 1, 1, 0, call_context={"mkdirs": {"dbutils.fs.mkdirs"}}),
            DirectFilesystemAccessMatcher("mv", 1, 2, 0, call_context={"mv": {"dbutils.fs.mv"}}),
            DirectFilesystemAccessMatcher("text", 1, 3, 0),
            DirectFilesystemAccessMatcher("csv", 1, 1000, 0),
            DirectFilesystemAccessMatcher("json", 1, 1000, 0),
            DirectFilesystemAccessMatcher("orc", 1, 1000, 0),
            DirectFilesystemAccessMatcher("parquet", 1, 1000, 0),
            DirectFilesystemAccessMatcher("save", 0, 1000, -1, "path"),
            DirectFilesystemAccessMatcher("load", 0, 1000, -1, "path"),
            DirectFilesystemAccessMatcher("option", 1, 1000, 1),  # Only .option("path", "xxx://bucket/path") will hit
            DirectFilesystemAccessMatcher("addFile", 1, 3, 0),
            DirectFilesystemAccessMatcher("binaryFiles", 1, 2, 0),
            DirectFilesystemAccessMatcher("binaryRecords", 1, 2, 0),
            DirectFilesystemAccessMatcher("dump_profiles", 1, 1, 0),
            DirectFilesystemAccessMatcher("hadoopFile", 1, 8, 0),
            DirectFilesystemAccessMatcher("newAPIHadoopFile", 1, 8, 0),
            DirectFilesystemAccessMatcher("pickleFile", 1, 3, 0),
            DirectFilesystemAccessMatcher("saveAsHadoopFile", 1, 8, 0),
            DirectFilesystemAccessMatcher("saveAsNewAPIHadoopFile", 1, 7, 0),
            DirectFilesystemAccessMatcher("saveAsPickleFile", 1, 2, 0),
            DirectFilesystemAccessMatcher("saveAsSequenceFile", 1, 2, 0),
            DirectFilesystemAccessMatcher("saveAsTextFile", 1, 2, 0),
            DirectFilesystemAccessMatcher("load_from_path", 1, 1, 0),
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
            + direct_fs_access_matchers
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
