import ast
from abc import abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass

from databricks.labs.ucx.source_code.base import (
    Advice,
    Failure,
    Linter,
)
from databricks.labs.ucx.source_code.ast_helpers import AstHelper


@dataclass
class SharedClusterMatcher:
    is_serverless: bool

    def _cluster_type_str(self) -> str:
        return 'UC Shared Clusters' if not self.is_serverless else 'Serverless Compute'

    @abstractmethod
    def lint(self, node: ast.AST) -> Iterator[Advice]:
        pass


class JvmAccessMatcher(SharedClusterMatcher):
    _FIELDS = [
        "_jvm",
        "_jcol",
        "_jdf",
        "_jspark",
        "_jsparkSession",
    ]

    def lint(self, node: ast.AST) -> Iterator[Advice]:
        if not isinstance(node, ast.Attribute):
            return
        if node.attr not in JvmAccessMatcher._FIELDS:
            return
        yield Failure(
            code='jvm-access-in-shared-clusters',
            message=f'Cannot access Spark Driver JVM on {self._cluster_type_str()}',
            start_line=node.lineno,
            start_col=node.col_offset,
            end_line=node.end_lineno or 0,
            end_col=node.end_col_offset or 0,
        )


class RDDApiMatcher(SharedClusterMatcher):
    _SC_METHODS = [
        # RDD creation
        "emptyRDD",
        "parallelize",
        "range",
        "makeRDD",
        # file read methods
        "binaryFiles",
        "binaryRecords",
        "hadoopFile",
        "hadoopRDD",
        "newAPIHadoopFile",
        "newAPIHadoopRDD",
        "objectFile",
        "sequenceFile",
        "textFile",
        "wholeTextFiles",
    ]

    def lint(self, node: ast.AST) -> Iterator[Advice]:
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            return
        if node.func.attr not in self._SC_METHODS:
            return
        function_name = AstHelper.get_full_function_name(node)
        if not function_name or not function_name.endswith(f"sc.{node.func.attr}"):
            return
        yield Failure(
            code='rdd-in-shared-clusters',
            message=f'RDD APIs are not supported on {self._cluster_type_str()}. Rewrite it using DataFrame API',
            start_line=node.lineno,
            start_col=node.col_offset,
            end_line=node.end_lineno or 0,
            end_col=node.end_col_offset or 0,
        )


class SparkSqlContextMatcher(SharedClusterMatcher):
    _ATTRIBUTES = ["sc", "sqlContext", "sparkContext"]

    def lint(self, node: ast.AST) -> Iterator[Advice]:
        if not isinstance(node, ast.Attribute):
            return
        if not isinstance(node.value, ast.Name) or node.value.id not in SparkSqlContextMatcher._ATTRIBUTES:
            return
        yield Failure(
            code='legacy-context-in-shared-clusters',
            message=f'{node.value.id} is not supported on {self._cluster_type_str()}. Rewrite it using spark',
            start_line=node.lineno,
            start_col=node.col_offset,
            end_line=node.end_lineno or 0,
            end_col=node.end_col_offset or 0,
        )


class LoggingMatcher(SharedClusterMatcher):
    def lint(self, node: ast.AST) -> Iterator[Advice]:
        yield from self._match_sc_set_log_level(node)
        yield from self._match_jvm_log(node)

    def _match_sc_set_log_level(self, node: ast.AST) -> Iterator[Advice]:
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            return
        if node.func.attr != 'setLogLevel':
            return
        function_name = AstHelper.get_full_function_name(node)
        if not function_name or not function_name.endswith('sc.setLogLevel'):
            return

        yield Failure(
            code='spark-logging-in-shared-clusters',
            message=f'Cannot set Spark log level directly from code on {self._cluster_type_str()}. '
            f'Remove the call and set the cluster spark conf \'spark.log.level\' instead',
            start_line=node.lineno,
            start_col=node.col_offset,
            end_line=node.end_lineno or 0,
            end_col=node.end_col_offset or 0,
        )

    def _match_jvm_log(self, node: ast.AST) -> Iterator[Advice]:
        if not isinstance(node, ast.Attribute):
            return
        attribute_name = AstHelper.get_full_attribute_name(node)
        if attribute_name and attribute_name.endswith('org.apache.log4j'):
            yield Failure(
                code='spark-logging-in-shared-clusters',
                message=f'Cannot access Spark Driver JVM logger on {self._cluster_type_str()}. '
                f'Use logging.getLogger() instead',
                start_line=node.lineno,
                start_col=node.col_offset,
                end_line=node.end_lineno or 0,
                end_col=node.end_col_offset or 0,
            )


class SparkConnectLinter(Linter):
    def __init__(self, is_serverless: bool = False):
        self._matchers = [
            JvmAccessMatcher(is_serverless=is_serverless),
            RDDApiMatcher(is_serverless=is_serverless),
            SparkSqlContextMatcher(is_serverless=is_serverless),
            LoggingMatcher(is_serverless=is_serverless),
        ]

    def lint(self, code: str) -> Iterator[Advice]:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            for matcher in self._matchers:
                yield from matcher.lint(node)
