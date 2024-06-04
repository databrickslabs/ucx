from abc import abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass

from astroid import Attribute, Call, Name, NodeNG  # type: ignore
from databricks.labs.ucx.source_code.base import (
    Advice,
    Failure,
    Linter,
)
from databricks.labs.ucx.source_code.ast_helpers import AstHelper
from databricks.labs.ucx.source_code.python_linter import TreeWalker, ASTLinter


@dataclass
class SharedClusterMatcher:
    is_serverless: bool

    def _cluster_type_str(self) -> str:
        return 'UC Shared Clusters' if not self.is_serverless else 'Serverless Compute'

    @abstractmethod
    def lint(self, node: NodeNG) -> Iterator[Advice]:
        pass


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
        if not isinstance(node, Call):
            return
        assert isinstance(node.func, Attribute)  # Avoid linter warning
        if node.func.attrname not in self._SC_METHODS:
            return
        function_name = AstHelper.get_full_function_name(node)
        if not function_name or not function_name.endswith(f"sc.{node.func.attrname}"):
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

    def lint(self, node: NodeNG) -> Iterator[Advice]:
        if not isinstance(node, Attribute):
            return
        if not isinstance(node.expr, Name) or node.expr.name not in SparkSqlContextMatcher._ATTRIBUTES:
            return
        yield Failure(
            code='legacy-context-in-shared-clusters',
            message=f'{node.expr.name} is not supported on {self._cluster_type_str()}. Rewrite it using spark',
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
        ]

    def lint(self, code: str) -> Iterator[Advice]:
        linter = ASTLinter.parse(code)
        for node in TreeWalker.walk(linter.root):
            for matcher in self._matchers:
                yield from matcher.lint(node)
