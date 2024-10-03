import logging
from abc import ABC
from collections.abc import Iterable

from astroid import Call, InferenceError, NodeNG  # type: ignore
from sqlglot.expressions import Alter, Create, Delete, Drop, Expression, Identifier, Insert, Literal, Select

from databricks.labs.ucx.source_code.base import (
    Advice,
    Deprecation,
    CurrentSessionState,
    PythonLinter,
    SqlLinter,
    DfsaPyCollector,
    DirectFsAccessNode,
    DfsaSqlCollector,
    DirectFsAccess,
)
from databricks.labs.ucx.source_code.python.python_ast import Tree, TreeVisitor, TreeHelper
from databricks.labs.ucx.source_code.python.python_infer import InferredValue
from databricks.labs.ucx.source_code.sql.sql_parser import SqlParser, SqlExpression

logger = logging.getLogger(__name__)


class DirectFsAccessPattern(ABC):

    def __init__(self, prefix: str, allowed_roots: list[str]):
        self._prefix = prefix
        self._allowed_roots = allowed_roots

    def matches(self, value: str) -> bool:
        return value.startswith(self._prefix) and not self._matches_allowed_root(value)

    def _matches_allowed_root(self, value: str):
        return any(value.startswith(f"{self._prefix}/{root}") for root in self._allowed_roots)


class RootPattern(DirectFsAccessPattern):

    def _matches_allowed_root(self, value: str):
        return any(value.startswith(f"/{root}") for root in self._allowed_roots)


# the below aims to implement https://docs.databricks.com/en/files/index.html
DIRECT_FS_ACCESS_PATTERNS = [
    DirectFsAccessPattern("dbfs:/", []),
    DirectFsAccessPattern("file:/", ["Workspace/"]),
    DirectFsAccessPattern("s3:/", []),
    DirectFsAccessPattern("s3n:/", []),
    DirectFsAccessPattern("s3a:/", []),
    DirectFsAccessPattern("wasb:/", []),
    DirectFsAccessPattern("wasbs:/", []),
    DirectFsAccessPattern("abfs:/", []),
    DirectFsAccessPattern("abfss:/", []),
    DirectFsAccessPattern("hdfs:/", []),
    # "/mnt/" is detected by the below pattern,
    RootPattern("/", ["Volumes/", "Workspace/"]),
]


class _DetectDirectFsAccessVisitor(TreeVisitor):
    """
    Visitor that detects file system paths in Python code and checks them
    against a list of known deprecated paths.
    """

    def __init__(self, session_state: CurrentSessionState, prevent_spark_duplicates: bool) -> None:
        self._session_state = session_state
        self._directfs_nodes: list[DirectFsAccessNode] = []
        self._prevent_spark_duplicates = prevent_spark_duplicates

    def visit_call(self, node: Call):
        for arg in node.args:
            self._visit_arg(node, arg)

    def _visit_arg(self, call: Call, arg: NodeNG):
        try:
            for inferred in InferredValue.infer_from_node(arg, self._session_state):
                if not inferred.is_inferred():
                    logger.debug(f"Could not infer value of {arg.as_string()}")
                    continue
                self._check_str_arg(call, arg, inferred)
        except InferenceError as e:
            logger.debug(f"Could not infer value of {arg.as_string()}", exc_info=e)

    def _check_str_arg(self, call_node: Call, arg_node: NodeNG, inferred: InferredValue):
        value = inferred.as_string()
        for pattern in DIRECT_FS_ACCESS_PATTERNS:
            if not pattern.matches(value):
                continue
            # only capture 'open' calls or calls originating from spark or dbutils
            # because there is no other known way to manipulate data directly from file system
            tree = Tree(call_node)
            is_open = TreeHelper.get_call_name(call_node) == "open" and tree.is_builtin()
            is_from_db_utils = False if is_open else tree.is_from_module("dbutils")
            is_from_spark = False if is_open or is_from_db_utils else tree.is_from_module("spark")
            if not (is_open or is_from_db_utils or is_from_spark):
                return
            # avoid duplicate advices that are reported by SparkSqlPyLinter
            if self._prevent_spark_duplicates and is_from_spark:
                return
            # since we're normally filtering out spark calls, we're dealing with dfsas we know little about
            # notably we don't know is_read or is_write
            dfsa = DirectFsAccess(
                path=value,
                is_read=True,
                is_write=False,
            )
            self._directfs_nodes.append(DirectFsAccessNode(dfsa, arg_node))
            return

    @property
    def directfs_nodes(self):
        return self._directfs_nodes


class DirectFsAccessPyLinter(PythonLinter, DfsaPyCollector):

    def __init__(self, session_state: CurrentSessionState, prevent_spark_duplicates=True):
        self._session_state = session_state
        self._prevent_spark_duplicates = prevent_spark_duplicates

    def lint_tree(self, tree: Tree) -> Iterable[Advice]:
        """
        Lints the code looking for file system paths that are deprecated
        """
        visitor = _DetectDirectFsAccessVisitor(self._session_state, self._prevent_spark_duplicates)
        visitor.visit(tree.node)
        for directfs_node in visitor.directfs_nodes:
            advisory = Deprecation.from_node(
                code='direct-filesystem-access',
                message=f"The use of direct filesystem references is deprecated: {directfs_node.dfsa.path}",
                node=directfs_node.node,
            )
            yield advisory

    def collect_dfsas_from_tree(self, tree: Tree) -> Iterable[DirectFsAccessNode]:
        visitor = _DetectDirectFsAccessVisitor(self._session_state, self._prevent_spark_duplicates)
        visitor.visit(tree.node)
        yield from visitor.directfs_nodes


class DirectFsAccessSqlLinter(SqlLinter, DfsaSqlCollector):

    def lint_expression(self, expression: Expression):
        for dfsa in self._collect_dfsas(SqlExpression(expression)):
            yield Deprecation(
                code='direct-filesystem-access-in-sql-query',
                message=f"The use of direct filesystem references is deprecated: {dfsa.path}",
                # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
                start_line=0,
                start_col=0,
                end_line=0,
                end_col=1024,
            )

    def collect_dfsas(self, source_code: str) -> Iterable[DirectFsAccess]:
        yield from SqlParser.walk_expressions(source_code, self._collect_dfsas)

    @classmethod
    def _collect_dfsas(cls, expression: SqlExpression) -> Iterable[DirectFsAccess]:
        yield from cls._collect_dfsas_from_literals(expression)
        yield from cls._collect_dfsas_from_identifiers(expression)

    @classmethod
    def _collect_dfsas_from_literals(cls, expression: SqlExpression) -> Iterable[DirectFsAccess]:
        for literal in expression.find_all(Literal):
            if not isinstance(literal.this, str):
                logger.warning(f"Can't interpret {type(literal.this).__name__}")
            yield from cls._collect_dfsa_from_node(literal, literal.this)

    @classmethod
    def _collect_dfsas_from_identifiers(cls, expression: SqlExpression) -> Iterable[DirectFsAccess]:
        for identifier in expression.find_all(Identifier):
            if not isinstance(identifier.this, str):
                logger.warning(f"Can't interpret {type(identifier.this).__name__}")
            yield from cls._collect_dfsa_from_node(identifier, identifier.this)

    @classmethod
    def _collect_dfsa_from_node(cls, expression: Expression, path: str) -> Iterable[DirectFsAccess]:
        if any(pattern.matches(path) for pattern in DIRECT_FS_ACCESS_PATTERNS):
            is_read = cls._is_read(expression)
            is_write = cls._is_write(expression)
            yield DirectFsAccess(
                path=path,
                is_read=is_read,
                is_write=is_write,
            )

    @classmethod
    def _is_read(cls, expression: Expression | None) -> bool:
        expression = cls._walk_up(expression)
        return isinstance(expression, Select)

    @classmethod
    def _is_write(cls, expression: Expression | None) -> bool:
        expression = cls._walk_up(expression)
        return isinstance(expression, (Create, Alter, Drop, Insert, Delete))

    @classmethod
    def _walk_up(cls, expression: Expression | None) -> Expression | None:
        if expression is None:
            return None
        if isinstance(expression, (Create, Alter, Drop, Insert, Delete, Select)):
            return expression
        return cls._walk_up(expression.parent)
