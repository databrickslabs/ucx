from abc import abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass

from astroid import Attribute, Call, Name, NodeNG  # type: ignore
from databricks.labs.ucx.source_code.base import (
    Advice,
    Failure,
    Linter,
)
from databricks.labs.ucx.source_code.linters.python_ast import AstHelper
from databricks.labs.ucx.source_code.linters.imports import ASTLinter, TreeWalker


@dataclass
class SharedClusterMatcher:
    is_serverless: bool

    def _cluster_type_str(self) -> str:
        return 'UC Shared Clusters' if not self.is_serverless else 'Serverless Compute'

    @abstractmethod
    def lint(self, node: NodeNG) -> Iterator[Advice]:
        pass

    def lint_tree(self, tree: NodeNG) -> Iterator[Advice]:
        reported_locations = set()
        for node in TreeWalker.walk(tree):
            for advice in self.lint(node):
                loc = (advice.start_line, advice.start_col)
                if loc not in reported_locations:
                    reported_locations.add(loc)
                    yield advice


class JvmAccessMatcher(SharedClusterMatcher):
    _FIELDS = [
        "_jvm",
        "_jcol",
        "_jdf",
        "_jspark",
        "_jsparkSession",
    ]

    def lint(self, node: NodeNG) -> Iterator[Advice]:
        if not isinstance(node, Attribute):
            return
        if node.attrname not in JvmAccessMatcher._FIELDS:
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

    def lint(self, node: NodeNG) -> Iterator[Advice]:
        yield from self._lint_sc(node)
        yield from self._lint_rdd_use(node)

    def _lint_rdd_use(self, node: NodeNG) -> Iterator[Advice]:
        if isinstance(node, Attribute):
            if node.attrname == 'rdd':
                yield self._rdd_failure(node)
                return
        if isinstance(node, Call) and isinstance(node.func, Attribute) and node.func.attrname == 'mapPartitions':
            yield Failure(
                code='rdd-in-shared-clusters',
                message=f'RDD APIs are not supported on {self._cluster_type_str()}. '
                f'Use mapInArrow() or Pandas UDFs instead',
                start_line=node.lineno,
                start_col=node.col_offset,
                end_line=node.end_lineno or 0,
                end_col=node.end_col_offset or 0,
            )

    def _lint_sc(self, node: NodeNG) -> Iterator[Advice]:
        if not isinstance(node, Call) or not isinstance(node.func, Attribute):
            return
        if node.func.attrname not in self._SC_METHODS:
            return
        function_name = AstHelper.get_full_function_name(node)
        if not function_name or not function_name.endswith(f"sc.{node.func.attrname}"):
            return
        yield self._rdd_failure(node)

    def _rdd_failure(self, node: NodeNG) -> Advice:
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

    def lint(self, node: NodeNG) -> Iterator[Advice]:
        if not isinstance(node, Attribute):
            return

        if isinstance(node.expr, Name) and node.expr.name in SparkSqlContextMatcher._ATTRIBUTES:
            yield self._get_advice(node, node.expr.name)
        # sparkContext can be an attribute as in df.sparkContext.getConf()
        if isinstance(node.expr, Attribute) and node.expr.attrname == 'sparkContext':
            yield self._get_advice(node, node.expr.attrname)

    def _get_advice(self, node: Attribute, name: str) -> Advice:
        if node.attrname in self._KNOWN_REPLACEMENTS:
            replacement = self._KNOWN_REPLACEMENTS[node.attrname]
            return Failure(
                code='legacy-context-in-shared-clusters',
                message=f'{name} and {node.attrname} are not supported on {self._cluster_type_str()}. '
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
    def lint(self, node: NodeNG) -> Iterator[Advice]:
        yield from self._match_sc_set_log_level(node)
        yield from self._match_jvm_log(node)

    def _match_sc_set_log_level(self, node: NodeNG) -> Iterator[Advice]:
        if not isinstance(node, Call) or not isinstance(node.func, Attribute):
            return
        if node.func.attrname != 'setLogLevel':
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

    def _match_jvm_log(self, node: NodeNG) -> Iterator[Advice]:
        if not isinstance(node, Attribute):
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
        linter = ASTLinter.parse(code)
        for matcher in self._matchers:
            yield from matcher.lint_tree(linter.root)
