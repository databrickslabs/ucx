import ast
from abc import abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass

from databricks.labs.ucx.source_code.base import (
    Advice,
    Linter,
)
from databricks.sdk.service.compute import DataSecurityMode
from databricks.labs.ucx.source_code.ast import AstHelper


@dataclass
class SharedClusterMatcher:
    data_security_mode: DataSecurityMode

    @abstractmethod
    def _lint(self, node: ast.AST) -> Iterator[Advice]:
        pass

    def lint(self, node: ast.AST) -> Iterator[Advice]:
        for advice in self._lint(node):
            if self.data_security_mode == DataSecurityMode.USER_ISOLATION:
                yield advice.as_failure()
            else:
                yield advice.as_advisory()


class JvmAccessMatcher(SharedClusterMatcher):
    _FIELDS = [
        "_jvm",
        "_jcol",
        "_jdf",
        "_jspark",
        "_jsparkSession",
    ]

    def _lint(self, node: ast.AST) -> bool:
        if not isinstance(node, ast.Attribute):
            return
        if node.attr not in JvmAccessMatcher._FIELDS:
            return
        print(node.attr, node.col_offset, node.end_col_offset)
        yield Advice(
            code='shared-clusters',
            message='Cannot access Spark Driver JVM on UC Shared Clusters',
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

    def _lint(self, node: ast.AST) -> bool:
        if not isinstance(node, ast.Call):
            return
        assert isinstance(node.func, ast.Attribute)  # Avoid linter warning
        if node.func.attr not in RDDApiMatcher._SC_METHODS:
            return
        if not AstHelper.get_full_function_name(node).endswith(f"sc.{node.func.attr}"):
            return
        yield Advice(
            code='shared-clusters',
            message='RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            start_line=node.lineno,
            start_col=node.col_offset,
            end_line=node.end_lineno or 0,
            end_col=node.end_col_offset or 0,
        )


class SparkSqlContextMatcher(SharedClusterMatcher):
    _ATTRIBUTES = [
        "sc",
        "sqlContext",
        "sparkContext"
    ]

    def _lint(self, node: ast.AST) -> bool:
        if not isinstance(node, ast.Attribute):
            return
        if node.attr not in SparkSqlContextMatcher._ATTRIBUTES:
            return
        yield Advice(
            code='shared-clusters',
            message=f'{node.attr} is not supported on UC Shared Clusters. Rewrite it using spark',
            start_line=node.lineno,
            start_col=node.col_offset,
            end_line=node.end_lineno or 0,
            end_col=node.end_col_offset or 0,
        )
        

class SparkConnectLinter(Linter):
    def __init__(self, data_security_mode: DataSecurityMode | None = None):
        self._matchers = [
            JvmAccessMatcher(data_security_mode=data_security_mode),
            RDDApiMatcher(data_security_mode=data_security_mode),
            SparkSqlContextMatcher(data_security_mode=data_security_mode),
        ]

    def lint(self, code: str) -> Iterator[Advice]:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            for matcher in self._matchers:
                yield from matcher.lint(node)
