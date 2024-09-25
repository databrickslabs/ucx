from abc import abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass

from astroid import Attribute, Call, Const, Name, NodeNG  # type: ignore
from databricks.labs.ucx.source_code.base import (
    Advice,
    Failure,
    PythonLinter,
    CurrentSessionState,
)
from databricks.sdk.service.compute import DataSecurityMode

from databricks.labs.ucx.source_code.python.python_ast import Tree, TreeHelper


@dataclass
class SharedClusterMatcher:
    session_state: CurrentSessionState

    def _cluster_type_str(self) -> str:
        return (
            'Unity Catalog clusters in Shared access mode'
            if not self.session_state.is_serverless
            else 'Serverless Compute'
        )

    @abstractmethod
    def lint(self, node: NodeNG) -> Iterator[Advice]:
        pass

    def lint_tree(self, root: NodeNG) -> Iterator[Advice]:
        reported_locations = set()
        for node in Tree(root).walk():
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
        yield Failure.from_node(
            code='jvm-access-in-shared-clusters',
            message=f'Cannot access Spark Driver JVM on {self._cluster_type_str()}',
            node=node,
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
            yield Failure.from_node(
                code='rdd-in-shared-clusters',
                message=f'RDD APIs are not supported on {self._cluster_type_str()}. '
                f'Use mapInArrow() or Pandas UDFs instead',
                node=node,
            )

    def _lint_sc(self, node: NodeNG) -> Iterator[Advice]:
        if not isinstance(node, Call) or not isinstance(node.func, Attribute):
            return
        if node.func.attrname not in self._SC_METHODS:
            return
        function_name = TreeHelper.get_full_function_name(node)
        if not function_name or not function_name.endswith(f"sc.{node.func.attrname}"):
            return
        yield self._rdd_failure(node)

    def _rdd_failure(self, node: NodeNG) -> Advice:
        return Failure.from_node(
            code='rdd-in-shared-clusters',
            message=f'RDD APIs are not supported on {self._cluster_type_str()}. Rewrite it using DataFrame API',
            node=node,
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
            return Failure.from_node(
                code='legacy-context-in-shared-clusters',
                message=f'{name} and {node.attrname} are not supported on {self._cluster_type_str()}. '
                f'Rewrite it using spark.{replacement}',
                node=node,
            )
        return Failure.from_node(
            code='legacy-context-in-shared-clusters',
            message=f'{name} is not supported on {self._cluster_type_str()}. Rewrite it using spark',
            node=node,
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
        function_name = TreeHelper.get_full_function_name(node)
        if not function_name or not function_name.endswith('sc.setLogLevel'):
            return

        yield Failure.from_node(
            code='spark-logging-in-shared-clusters',
            message=f'Cannot set Spark log level directly from code on {self._cluster_type_str()}. '
            f'Remove the call and set the cluster spark conf \'spark.log.level\' instead',
            node=node,
        )

    def _match_jvm_log(self, node: NodeNG) -> Iterator[Advice]:
        if not isinstance(node, Attribute):
            return
        attribute_name = TreeHelper.get_full_attribute_name(node)
        if attribute_name and attribute_name.endswith('org.apache.log4j'):
            yield Failure.from_node(
                code='spark-logging-in-shared-clusters',
                message=f'Cannot access Spark Driver JVM logger on {self._cluster_type_str()}. '
                f'Use logging.getLogger() instead',
                node=node,
            )


class UDFMatcher(SharedClusterMatcher):
    _DBR_14_2_BELOW_NOT_SUPPORTED = ["applyInPandas", "mapInPandas", "applyInPandasWithState", "udtf", "pandas_udf"]

    def lint(self, node: NodeNG) -> Iterator[Advice]:
        if not isinstance(node, Call):
            return
        function_name = TreeHelper.get_function_name(node)

        if function_name == 'registerJavaFunction':
            yield Failure.from_node(
                code='python-udf-in-shared-clusters',
                message=f'Cannot register Java UDF from Python code on {self._cluster_type_str()}. '
                f'Use a %scala cell to register the Scala UDF using spark.udf.register.',
                node=node,
            )

        if (
            function_name in UDFMatcher._DBR_14_2_BELOW_NOT_SUPPORTED
            and self.session_state.dbr_version
            and self.session_state.dbr_version < (14, 3)
        ):
            yield Failure.from_node(
                code='python-udf-in-shared-clusters',
                message=f'{function_name} require DBR 14.3 LTS or above on {self._cluster_type_str()}',
                node=node,
            )

        if function_name == 'udf' and self.session_state.dbr_version and self.session_state.dbr_version < (14, 3):
            for keyword in node.keywords:
                if keyword.arg == 'useArrow' and isinstance(keyword.value, Const) and keyword.value.value:
                    yield Failure.from_node(
                        code='python-udf-in-shared-clusters',
                        message=f'Arrow UDFs require DBR 14.3 LTS or above on {self._cluster_type_str()}',
                        node=node,
                    )


class CatalogApiMatcher(SharedClusterMatcher):
    def lint(self, node: NodeNG) -> Iterator[Advice]:
        if not isinstance(node, Attribute):
            return
        if node.attrname == 'catalog' and TreeHelper.get_full_attribute_name(node).endswith('spark.catalog'):
            yield Failure.from_node(
                code='catalog-api-in-shared-clusters',
                message=f'spark.catalog functions require DBR 14.3 LTS or above on {self._cluster_type_str()}',
                node=node,
            )


class CommandContextMatcher(SharedClusterMatcher):
    def lint(self, node: NodeNG) -> Iterator[Advice]:
        if not isinstance(node, Call):
            return
        function_name = TreeHelper.get_full_function_name(node)
        if function_name and function_name.endswith('getContext.toJson'):
            yield Failure.from_node(
                code='to-json-in-shared-clusters',
                message=f'toJson() is not available on {self._cluster_type_str()}. '
                f'Use toSafeJson() on DBR 13.3 LTS or above to get a subset of command context information.',
                node=node,
            )


class SparkConnectPyLinter(PythonLinter):
    def __init__(self, session_state: CurrentSessionState):
        if session_state.data_security_mode != DataSecurityMode.USER_ISOLATION:
            self._matchers = []
            return

        self._matchers = [
            JvmAccessMatcher(session_state=session_state),
            RDDApiMatcher(session_state=session_state),
            SparkSqlContextMatcher(session_state=session_state),
            LoggingMatcher(session_state=session_state),
            UDFMatcher(session_state=session_state),
            CommandContextMatcher(session_state=session_state),
        ]
        if session_state.dbr_version and session_state.dbr_version < (14, 3):
            self._matchers.append(CatalogApiMatcher(session_state=session_state))

    def lint_tree(self, tree: Tree) -> Iterator[Advice]:
        for matcher in self._matchers:
            yield from matcher.lint_tree(tree.node)
