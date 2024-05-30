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
        yield from self._lint_sc(node)
        yield from self._lint_rdd_use(node)

    def _lint_rdd_use(self, node: ast.AST) -> Iterator[Advice]:
        if isinstance(node, ast.Attribute):
            if node.attr == 'rdd':
                yield self._rdd_failure(node)
                return
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'mapPartitions':
            yield Failure(
                code='rdd-in-shared-clusters',
                message=f'RDD APIs are not supported on {self._cluster_type_str()}. '
                f'Use mapInArrow() or Pandas UDFs instead',
                start_line=node.lineno,
                start_col=node.col_offset,
                end_line=node.end_lineno or 0,
                end_col=node.end_col_offset or 0,
            )

    def _lint_sc(self, node: ast.AST) -> Iterator[Advice]:
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            return
        if node.func.attr not in self._SC_METHODS:
            return
        function_name = AstHelper.get_full_function_name(node)
        if not function_name or not function_name.endswith(f"sc.{node.func.attr}"):
            return
        yield self._rdd_failure(node)

    def _rdd_failure(self, node: ast.AST) -> Advice:
        return Failure(
            code='rdd-in-shared-clusters',
            message=f'RDD APIs are not supported on {self._cluster_type_str()}. Rewrite it using DataFrame API',
            start_line=node.lineno,
            start_col=node.col_offset,
            end_line=node.end_lineno or 0,
            end_col=node.end_col_offset or 0,
        )


class SparkSqlContextMatcher(SharedClusterMatcher):
    _ATTRIBUTES = ["sc", "sqlContext", "sparkContext"]
    _KNOWN_REPLACEMENTS = {"getConf": "conf", "_conf": "conf"}

    def lint(self, node: ast.AST) -> Iterator[Advice]:
        if not isinstance(node, ast.Attribute):
            return

        if isinstance(node.value, ast.Name) and node.value.id in SparkSqlContextMatcher._ATTRIBUTES:
            yield self._get_advice(node, node.value.id)
        # sparkContext can be an attribute as in df.sparkContext.getConf()
        if isinstance(node.value, ast.Attribute) and node.value.attr == 'sparkContext':
            yield self._get_advice(node, node.value.attr)

    def _get_advice(self, node: ast.Attribute, name: str) -> Advice:
        if node.attr in SparkSqlContextMatcher._KNOWN_REPLACEMENTS:
            replacement = SparkSqlContextMatcher._KNOWN_REPLACEMENTS[node.attr]
            return Failure(
                code='legacy-context-in-shared-clusters',
                message=f'{name} and {node.attr} are not supported on {self._cluster_type_str()}. '
                f'Rewrite it using spark.{replacement}',
                start_line=node.lineno,
                start_col=node.col_offset,
                end_line=node.end_lineno or 0,
                end_col=node.end_col_offset or 0,
            )
        return Failure(
            code='legacy-context-in-shared-clusters',
            message=f'{name} is not supported on {self._cluster_type_str()}. Rewrite it using spark',
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
