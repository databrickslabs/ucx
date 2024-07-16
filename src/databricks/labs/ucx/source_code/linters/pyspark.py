from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from astroid import Attribute, Call, Const, InferenceError, NodeNG  # type: ignore
from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import (
    Advice,
    Advisory,
    Deprecation,
    Fixer,
    CurrentSessionState,
    PythonLinter,
)
from databricks.labs.ucx.source_code.linters.python_infer import InferredValue
from databricks.labs.ucx.source_code.queries import FromTable
from databricks.labs.ucx.source_code.linters.python_ast import Tree


@dataclass
class Matcher(ABC):
    method_name: str
    min_args: int
    max_args: int
    table_arg_index: int
    table_arg_name: str | None = None
    call_context: dict[str, set[str]] | None = None
    session_state: CurrentSessionState | None = None

    def matches(self, node: NodeNG):
        return (
            isinstance(node, Call)
            and self._get_table_arg(node) is not None
            and isinstance(node.func, Attribute)
            and Tree(node.func.expr).is_from_module("spark")
        )

    @abstractmethod
    def lint(
        self, from_table: FromTable, index: MigrationIndex, session_state: CurrentSessionState, node: Call
    ) -> Iterator[Advice]:
        """raises Advices by linting the code"""

    @abstractmethod
    def apply(self, from_table: FromTable, index: MigrationIndex, node: Call) -> None:
        """applies recommendations"""

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
        qualified_name = Tree.get_full_function_name(node)

        # Check if the call_context is None as that means all calls are checked
        if self.call_context is None:
            return True

        # Get the qualified names from the call_context dictionary
        qualified_names = self.call_context.get(func_name)

        # Check if the qualified name is in the set of qualified names that are allowed
        return qualified_name in qualified_names if qualified_names else False


@dataclass
class QueryMatcher(Matcher):

    def lint(
        self, from_table: FromTable, index: MigrationIndex, session_state: CurrentSessionState, node: Call
    ) -> Iterator[Advice]:
        table_arg = self._get_table_arg(node)
        if table_arg:
            try:
                for inferred in InferredValue.infer_from_node(table_arg, self.session_state):
                    yield from self._lint_table_arg(from_table, node, inferred)
            except InferenceError:
                yield Advisory.from_node(
                    code='cannot-autofix-table-reference',
                    message=f"Can't migrate table_name argument in '{node.as_string()}' because its value cannot be computed",
                    node=node,
                )

    @classmethod
    def _lint_table_arg(cls, from_table: FromTable, call_node: NodeNG, inferred: InferredValue):
        if inferred.is_inferred():
            for advice in from_table.lint(inferred.as_string()):
                yield advice.replace_from_node(call_node)
        else:
            yield Advisory.from_node(
                code='cannot-autofix-table-reference',
                message=f"Can't migrate table_name argument in '{call_node.as_string()}' because its value cannot be computed",
                node=call_node,
            )

    def apply(self, from_table: FromTable, index: MigrationIndex, node: Call) -> None:
        table_arg = self._get_table_arg(node)
        assert isinstance(table_arg, Const)
        new_query = from_table.apply(table_arg.value)
        table_arg.value = new_query


@dataclass
class TableNameMatcher(Matcher):

    def lint(
        self, from_table: FromTable, index: MigrationIndex, session_state: CurrentSessionState, node: Call
    ) -> Iterator[Advice]:
        table_arg = self._get_table_arg(node)
        table_name = table_arg.as_string().strip("'").strip('"')
        for inferred in InferredValue.infer_from_node(table_arg, session_state):
            if not inferred.is_inferred():
                yield Advisory.from_node(
                    code='cannot-autofix-table-reference',
                    message=f"Can't migrate '{node.as_string()}' because its table name argument cannot be computed",
                    node=node,
                )
                continue
            dst = self._find_dest(index, inferred.as_string(), from_table.schema)
            if dst is None:
                continue
            yield Deprecation.from_node(
                code='table-migrated-to-uc',
                message=f"Table {table_name} is migrated to {dst.destination()} in Unity Catalog",
                # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
                node=node,
            )

    def apply(self, from_table: FromTable, index: MigrationIndex, node: Call) -> None:
        table_arg = self._get_table_arg(node)
        assert isinstance(table_arg, Const)
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

    def matches(self, node: NodeNG):
        return isinstance(node, Call) and isinstance(node.func, Attribute)

    def lint(
        self, from_table: FromTable, index: MigrationIndex, session_state: CurrentSessionState, node: Call
    ) -> Iterator[Advice]:
        assert isinstance(node.func, Attribute)  # always true, avoids a pylint warning
        yield Advisory.from_node(
            code='changed-result-format-in-uc',
            message=f"Call to '{node.func.attrname}' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.",
            node=node,
        )

    def apply(self, from_table: FromTable, index: MigrationIndex, node: Call) -> None:
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

    def matches(self, node: NodeNG):
        return isinstance(node, Call) and isinstance(node.func, Attribute) and self._get_table_arg(node) is not None

    def lint(
        self, from_table: FromTable, index: MigrationIndex, session_state: CurrentSessionState, node: NodeNG
    ) -> Iterator[Advice]:
        table_arg = self._get_table_arg(node)
        if not isinstance(table_arg, Const):
            return
        if not table_arg.value:
            return
        if not isinstance(table_arg.value, str):
            return
        if any(table_arg.value.startswith(prefix) for prefix in self._DIRECT_FS_REFS):
            yield Deprecation.from_node(
                code='direct-filesystem-access',
                message=f"The use of direct filesystem references is deprecated: {table_arg.value}",
                node=node,
            )
            return
        if table_arg.value.startswith("/") and self._check_call_context(node):
            yield Deprecation.from_node(
                code='implicit-dbfs-usage',
                message=f"The use of default dbfs: references is deprecated: {table_arg.value}",
                node=node,
            )

    def apply(self, from_table: FromTable, index: MigrationIndex, node: Call) -> None:
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
            TableNameMatcher("isCached", 1, 1, 0),
            TableNameMatcher("listColumns", 1, 2, 0, "tableName"),
            TableNameMatcher("tableExists", 1, 2, 0, "tableName"),
            TableNameMatcher("recoverPartitions", 1, 1, 0),
            TableNameMatcher("refreshTable", 1, 1, 0),
            TableNameMatcher("uncacheTable", 1, 1, 0),
            ReturnValueMatcher("listTables", 0, 2, 0),
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
            DirectFilesystemAccessMatcher("save", 0, 1000, 0, "path"),
            DirectFilesystemAccessMatcher("load", 0, 1000, 0, "path"),
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
            + direct_fs_access_matchers
        ):
            self._matchers[matcher.method_name] = matcher

    @property
    def matchers(self):
        return self._matchers


class SparkSql(PythonLinter, Fixer):

    _spark_matchers = SparkMatchers()

    def __init__(self, from_table: FromTable, index: MigrationIndex, session_state):
        self._from_table = from_table
        self._index = index
        self._session_state = session_state

    def name(self) -> str:
        # this is the same fixer, just in a different language context
        return self._from_table.name()

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
        matcher = self._spark_matchers.matchers.get(node.func.attrname, None)
        if matcher is None:
            return None
        return matcher if matcher.matches(node) else None
