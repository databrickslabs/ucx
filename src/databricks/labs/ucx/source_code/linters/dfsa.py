from dataclasses import dataclass
import logging
from abc import ABC
from collections.abc import Iterable

from astroid import Call, Const, InferenceError, NodeNG  # type: ignore
from sqlglot import Expression
from sqlglot.expressions import Table

from databricks.labs.ucx.source_code.base import (
    Advice,
    Deprecation,
    CurrentSessionState,
    PythonLinter,
    SqlLinter,
)
from databricks.labs.ucx.source_code.linters.python_ast import Tree, TreeVisitor
from databricks.labs.ucx.source_code.linters.python_infer import InferredValue

logger = logging.getLogger(__name__)


class DFSAPattern(ABC):

    def __init__(self, prefix: str, allowed_roots: list[str]):
        self._prefix = prefix
        self._allowed_roots = allowed_roots

    def matches(self, value: str) -> bool:
        return value.startswith(self._prefix) and not self._matches_allowed_root(value)

    def _matches_allowed_root(self, value: str):
        return any(value.startswith(f"{self._prefix}/{root}") for root in self._allowed_roots)


class RootPattern(DFSAPattern):

    def _matches_allowed_root(self, value: str):
        return any(value.startswith(f"/{root}") for root in self._allowed_roots)


# the below aims to implement https://docs.databricks.com/en/files/index.html
DFSA_PATTERNS = [
    DFSAPattern("dbfs:/", []),
    DFSAPattern("file:/", ["Workspace/", "tmp/"]),
    DFSAPattern("s3:/", []),
    DFSAPattern("s3n:/", []),
    DFSAPattern("s3a:/", []),
    DFSAPattern("wasb:/", []),
    DFSAPattern("wasbs:/", []),
    DFSAPattern("abfs:/", []),
    DFSAPattern("abfss:/", []),
    DFSAPattern("hdfs:/", []),
    DFSAPattern("/mnt/", []),
    RootPattern("/", ["Volumes/", "Workspace/", "tmp/"]),
]


@dataclass
class DFSA:
    """A DFSA is a record describing a Direct File System Access"""

    path: str


@dataclass
class DFSANode:
    dfsa: DFSA
    node: NodeNG


class _DetectDfsaVisitor(TreeVisitor):
    """
    Visitor that detects file system paths in Python code and checks them
    against a list of known deprecated paths.
    """

    def __init__(self, session_state: CurrentSessionState, allow_spark_duplicates: bool) -> None:
        self._session_state = session_state
        self._dfsa_nodes: list[DFSANode] = []
        self._reported_locations: set[tuple[int, int]] = set()
        self._allow_spark_duplicates = allow_spark_duplicates

    def visit_call(self, node: Call):
        for arg in node.args:
            self._visit_arg(arg)

    def _visit_arg(self, arg: NodeNG):
        try:
            for inferred in InferredValue.infer_from_node(arg, self._session_state):
                if not inferred.is_inferred():
                    logger.debug(f"Could not infer value of {arg.as_string()}")
                    continue
                self._check_str_constant(arg, inferred)
        except InferenceError as e:
            logger.debug(f"Could not infer value of {arg.as_string()}", exc_info=e)

    def visit_const(self, node: Const):
        # Constant strings yield Advisories
        if isinstance(node.value, str):
            self._check_str_constant(node, InferredValue([node]))

    def _check_str_constant(self, source_node, inferred: InferredValue):
        if self._already_reported(source_node, inferred):
            return
        # avoid duplicate advices that are reported by SparkSqlPyLinter
        if Tree(source_node).is_from_module("spark") and not self._allow_spark_duplicates:
            return
        value = inferred.as_string()
        if any(pattern.matches(value) for pattern in DFSA_PATTERNS):
            self._dfsa_nodes.append(DFSANode(DFSA(value), source_node))
            self._reported_locations.add((source_node.lineno, source_node.col_offset))

    def _already_reported(self, source_node: NodeNG, inferred: InferredValue):
        all_nodes = [source_node] + inferred.nodes
        return any((node.lineno, node.col_offset) in self._reported_locations for node in all_nodes)

    @property
    def dfsa_nodes(self):
        return self._dfsa_nodes


class DfsaPyLinter(PythonLinter):

    def __init__(self, session_state: CurrentSessionState, allow_spark_duplicates=False):
        self._session_state = session_state
        self._allow_spark_duplicates = allow_spark_duplicates

    @staticmethod
    def name() -> str:
        """
        Returns the name of the linter, for reporting etc
        """
        return 'dfsa-usage'

    def lint_tree(self, tree: Tree) -> Iterable[Advice]:
        """
        Lints the code looking for file system paths that are deprecated
        """
        visitor = _DetectDfsaVisitor(self._session_state, self._allow_spark_duplicates)
        visitor.visit(tree.node)
        for dfsa_node in visitor.dfsa_nodes:
            advisory = Deprecation.from_node(
                code='direct-filesystem-access',
                message=f"The use of direct filesystem references is deprecated: {dfsa_node.dfsa.path}",
                node=dfsa_node.node,
            )
            yield advisory


class DfsaSqlLinter(SqlLinter):

    @staticmethod
    def name() -> str:
        return 'dfsa-query'

    def lint_expression(self, expression: Expression):
        for table in expression.find_all(Table):
            # Check table names for direct file system access
            yield from self._check_dfsa(table)

    def _check_dfsa(self, table: Table) -> Iterable[Advice]:
        """
        Check if the table is a DBFS table or reference in some way
        and yield a deprecation message if it is
        """
        if any(pattern.matches(table.name) for pattern in DFSA_PATTERNS):
            yield Deprecation(
                code='direct-filesystem-access-in-sql-query',
                message=f"The use of direct filesystem references is deprecated: {table.name}",
                # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
                start_line=0,
                start_col=0,
                end_line=0,
                end_col=1024,
            )
