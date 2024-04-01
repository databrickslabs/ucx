import ast
from _ast import AST
from collections.abc import Iterable

from databricks.labs.ucx.source_code.base import Advice, Fixer, Linter, Deprecation
from databricks.labs.ucx.source_code.queries import FromTable


# see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html
SPARK_SESSION_CALLS = [
    ("sql", 1, 1000, 0, True),
    ("table", 1, 1, 0)]

# see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.html
SPARK_CATALOG_CALLS = [
    ("cacheTable", 1, 2, 0),
    ("createTable", 1, 1000, 0),
    ("createExternalTable", 1, 1000, 0),
    ("getTable", 1, 1, 0),
    ("isCached", 1, 1, 0),
    ("listColumns", 1, 2, 0),
    ("tableExists", 1, 2, 0),
    ("recoverPartitions", 1, 1, 0),
    ("refreshTable", 1, 1, 0),
    ("uncacheTable", 1, 1, 0),
    # TODO listTables: raise warning ?
]

# see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html
SPARK_DATAFRAME_CALLS = [
    ("writeTo", 1, 1, 0),
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
SPARK_DATAFRAMEREADER_CALLS = [
    ("table", 1, 1, 0), # TODO good example of collision, see SPARK_SESSION_CALLS
]

# see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html
SPARK_DATAFRAMEWRITER_CALLS = [
    ("insertInto", 1, 2, 0),
    # TODO jdbc: could the url be a databricks url, raise warning ?
    ("saveAsTable", 1, 4, 0),
]

# nothing to migrate in DataFrameWriterV2, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriterV2.html
# nothing to migrate in UDFRegistration, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UDFRegistration.html

# see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UDTFRegistration.html
SPARK_UDTFREGISTRATION_CALLS = [
    ("register", 1, 2, 0),
]

# nothing to migrate in UserDefinedFunction, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UserDefinedFunction.html
# nothing to migrate in UserDefinedTableFunction, see https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.UserDefinedTableFunction.html

SPARK_CALLS = {}
for call in SPARK_SESSION_CALLS + SPARK_CATALOG_CALLS + SPARK_DATAFRAME_CALLS + SPARK_DATAFRAMEREADER_CALLS + SPARK_DATAFRAMEWRITER_CALLS + SPARK_UDTFREGISTRATION_CALLS:
    SPARK_CALLS[call[0]] = call


class SparkSql(Linter, Fixer):
    def __init__(self, from_table: FromTable):
        self._from_table = from_table


    def name(self) -> str:
        # this is the same fixer, just in a different language context
        return self._from_table.name()


    def lint(self, code: str) -> Iterable[Advice]:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            table_arg, is_query = SparkSql._find_table_arg_if_eligible(node)
            if table_arg is None:
                continue
            if is_query:
                query = table_arg.value
                for advice in self._from_table.lint(query):
                    yield advice.replace(
                        start_line=node.lineno,
                        start_col=node.col_offset,
                        end_line=node.end_lineno,
                        end_col=node.end_col_offset,
                    )
            else:
                dst = self._find_dest(table_arg.value)
                if dst is None:
                    continue
                yield Deprecation(
                    code='table-migrate',
                    message=f"Table {table_arg.value} is migrated to {dst.destination()} in Unity Catalog",
                    # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
                    start_line=node.lineno,
                    start_col=node.col_offset,
                    end_line=node.end_lineno,
                    end_col=node.end_col_offset,
                )


    def apply(self, code: str) -> str:
        tree = ast.parse(code)
        # we won't be doing it like this in production, but for the sake of the example
        for node in ast.walk(tree):
            table_arg, is_query = SparkSql._find_table_arg_if_eligible(node)
            if table_arg is None:
                continue
            if is_query:
                query = table_arg.value
                new_query = self._from_table.apply(query)
                table_arg.value = new_query
            else:
                dst = self._find_dest(table_arg.value)
                if dst is None:
                    continue
                table_arg.value = dst.destination()
        return ast.unparse(tree)


    @staticmethod
    def _find_table_arg_if_eligible(node: AST):
        NoneResult = None, None
        if not isinstance(node, ast.Call):
            return NoneResult
        if not isinstance(node.func, ast.Attribute):
            return NoneResult
        call = SPARK_CALLS.get(node.func.attr, None)
        if call is None or len(node.args) < call[1] or len(node.args) > call[2]:
            return NoneResult
        table_arg = node.args[call[3]]
        if not isinstance(table_arg, ast.Constant):
            # `astroid` library supports inference and parent node lookup,
            # which makes traversing the AST a bit easier.
            return NoneResult
        is_query = call[4] if len(call) > 4 else False
        return table_arg, is_query

    def _find_dest(self, table_value: str):
        parts = table_value.split(".")
        if len(parts) != 2:
            return None
        return self._from_table.index().get(parts[0], parts[1])
