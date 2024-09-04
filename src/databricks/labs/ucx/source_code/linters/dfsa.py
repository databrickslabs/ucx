from dataclasses import dataclass
import logging
from abc import ABC
from collections.abc import Iterable

from astroid import Call, Const, InferenceError, NodeNG  # type: ignore
from sqlglot import Expression as SqlExpression, parse as parse_sql, ParseError as SqlParseError
from sqlglot.expressions import Alter, Create, Delete, Drop, Identifier, Insert, Literal, Select

from databricks.labs.ucx.source_code.base import (
    Advice,
    Deprecation,
    CurrentSessionState,
    PythonLinter,
    SqlLinter,
    DFSA,
)
from databricks.labs.ucx.source_code.python.python_ast import Tree, TreeVisitor
from databricks.labs.ucx.source_code.python.python_infer import InferredValue

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
    # "/mnt/" is detected by the below pattern,
    RootPattern("/", ["Volumes/", "Workspace/", "tmp/"]),
]


@dataclass
class DFSANode:
    dfsa: DFSA
    node: NodeNG


class _DetectDfsaVisitor(TreeVisitor):
    """
    Visitor that detects file system paths in Python code and checks them
    against a list of known deprecated paths.
    """

    def __init__(self, session_state: CurrentSessionState, prevent_spark_duplicates: bool) -> None:
        self._session_state = session_state
        self._dfsa_nodes: list[DFSANode] = []
        self._reported_locations: set[tuple[int, int]] = set()
        self._prevent_spark_duplicates = prevent_spark_duplicates

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
        if self._prevent_spark_duplicates and Tree(source_node).is_from_module("spark"):
            return
        value = inferred.as_string()
        for pattern in DFSA_PATTERNS:
            if not pattern.matches(value):
                continue
            # since we're normally filtering out spark calls, we're dealing with dfsas we know little about
            # notable we don't know is_read or is_write
            dfsa = DFSA(source_type=DFSA.UNKNOWN, source_id=DFSA.UNKNOWN, path=value, is_read=True, is_write=False)
            self._dfsa_nodes.append(DFSANode(dfsa, source_node))
            self._reported_locations.add((source_node.lineno, source_node.col_offset))

    def _already_reported(self, source_node: NodeNG, inferred: InferredValue):
        all_nodes = [source_node] + inferred.nodes
        return any((node.lineno, node.col_offset) in self._reported_locations for node in all_nodes)

    @property
    def dfsa_nodes(self):
        return self._dfsa_nodes


class DfsaPyLinter(PythonLinter):

    def __init__(self, session_state: CurrentSessionState, prevent_spark_duplicates=True):
        self._session_state = session_state
        self._prevent_spark_duplicates = prevent_spark_duplicates

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
        visitor = _DetectDfsaVisitor(self._session_state, self._prevent_spark_duplicates)
        visitor.visit(tree.node)
        for dfsa_node in visitor.dfsa_nodes:
            advisory = Deprecation.from_node(
                code='direct-filesystem-access',
                message=f"The use of direct filesystem references is deprecated: {dfsa_node.dfsa.path}",
                node=dfsa_node.node,
            )
            yield advisory

    def collect_dfsas(self, python_code: str, inherited_tree: Tree | None) -> Iterable[DFSANode]:
        tree = Tree.new_module()
        if inherited_tree:
            tree.append_tree(inherited_tree)
        tree.append_tree(Tree.normalize_and_parse(python_code))
        visitor = _DetectDfsaVisitor(self._session_state, self._prevent_spark_duplicates)
        visitor.visit(tree.node)
        yield from visitor.dfsa_nodes


class DfsaSqlLinter(SqlLinter):

    @staticmethod
    def name() -> str:
        return 'dfsa-query'

    def lint_expression(self, expression: SqlExpression):
        for dfsa in self._collect_dfsas(expression):
            yield Deprecation(
                code='direct-filesystem-access-in-sql-query',
                message=f"The use of direct filesystem references is deprecated: {dfsa.path}",
                # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
                start_line=0,
                start_col=0,
                end_line=0,
                end_col=1024,
            )

    def collect_dfsas(self, sql_code: str):
        try:
            expressions = parse_sql(sql_code, read='databricks')
            for expression in expressions:
                if not expression:
                    continue
                yield from self._collect_dfsas(expression)
        except SqlParseError as e:
            logger.debug(f"Failed to parse SQL: {sql_code}", exc_info=e)

    @classmethod
    def _collect_dfsas(cls, expression: SqlExpression) -> Iterable[DFSA]:
        yield from cls._collect_dfsas_from_literals(expression)
        yield from cls._collect_dfsas_from_identifiers(expression)

    @classmethod
    def _collect_dfsas_from_literals(cls, expression: SqlExpression) -> Iterable[DFSA]:
        for literal in expression.find_all(Literal):
            if not isinstance(literal.this, str):
                logger.warning(f"Can't interpret {type(literal.this).__name__}")
            yield from cls._collect_dfsa_from_node(literal, literal.this)

    @classmethod
    def _collect_dfsas_from_identifiers(cls, expression: SqlExpression) -> Iterable[DFSA]:
        for identifier in expression.find_all(Identifier):
            if not isinstance(identifier.this, str):
                logger.warning(f"Can't interpret {type(identifier.this).__name__}")
            yield from cls._collect_dfsa_from_node(identifier, identifier.this)

    @classmethod
    def _collect_dfsa_from_node(cls, expression: SqlExpression, path: str) -> Iterable[DFSA]:
        if any(pattern.matches(path) for pattern in DFSA_PATTERNS):
            is_read = cls._is_read(expression)
            is_write = cls._is_write(expression)
            yield DFSA(source_type=DFSA.UNKNOWN, source_id=DFSA.UNKNOWN, path=path, is_read=is_read, is_write=is_write)

    @classmethod
    def _is_read(cls, expression: SqlExpression | None) -> bool:
        expression = cls._walk_up(expression)
        return isinstance(expression, Select)

    @classmethod
    def _is_write(cls, expression: SqlExpression | None) -> bool:
        expression = cls._walk_up(expression)
        return isinstance(expression, (Create, Alter, Drop, Insert, Delete))

    @classmethod
    def _walk_up(cls, expression: SqlExpression | None) -> SqlExpression | None:
        if expression is None:
            return None
        if isinstance(expression, (Create, Alter, Drop, Insert, Delete, Select)):
            return expression
        return cls._walk_up(expression.parent)
