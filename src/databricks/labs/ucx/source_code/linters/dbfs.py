import logging
from collections.abc import Iterable

from astroid import Call, Const, InferenceError, NodeNG  # type: ignore
from sqlglot import Expression, parse as parse_sql, ParseError as SqlParseError
from sqlglot.expressions import Table

from databricks.labs.ucx.source_code.base import Advice, Linter, Deprecation, CurrentSessionState, PythonLinter
from databricks.labs.ucx.source_code.linters.python_ast import Tree, TreeVisitor
from databricks.labs.ucx.source_code.linters.python_infer import InferredValue
from databricks.labs.ucx.source_code.queries import FromTable

logger = logging.getLogger(__name__)


class DetectDbfsVisitor(TreeVisitor):
    """
    Visitor that detects file system paths in Python code and checks them
    against a list of known deprecated paths.
    """

    def __init__(self, session_state: CurrentSessionState) -> None:
        self._session_state = session_state
        self._advices: list[Advice] = []
        self._fs_prefixes = ["/dbfs/mnt", "dbfs:/", "/mnt/"]
        self._reported_locations: set[tuple[int, int]] = set()  # Set to store reported locations; astroid coordinates!

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
        value = inferred.as_string()
        if any(value.startswith(prefix) for prefix in self._fs_prefixes):
            advisory = Deprecation.from_node(
                code='dbfs-usage',
                message=f"Deprecated file system path: {value}",
                node=source_node,
            )
            self._advices.append(advisory)

    def _already_reported(self, source_node: NodeNG, inferred: InferredValue):
        all_nodes = [source_node]
        all_nodes.extend(inferred.nodes)
        reported = any((node.lineno, node.col_offset) in self._reported_locations for node in all_nodes)
        for node in all_nodes:
            self._reported_locations.add((node.lineno, node.col_offset))
        return reported

    def get_advices(self) -> Iterable[Advice]:
        yield from self._advices


class DBFSUsageLinter(PythonLinter):

    def __init__(self, session_state: CurrentSessionState):
        self._session_state = session_state

    @staticmethod
    def name() -> str:
        """
        Returns the name of the linter, for reporting etc
        """
        return 'dbfs-usage'

    def lint_tree(self, tree: Tree) -> Iterable[Advice]:
        """
        Lints the code looking for file system paths that are deprecated
        """
        visitor = DetectDbfsVisitor(self._session_state)
        visitor.visit(tree.node)
        yield from visitor.get_advices()


class FromDbfsFolder(Linter):
    def __init__(self):
        self._dbfs_prefixes = ["/dbfs/mnt", "dbfs:/", "/mnt/", "/dbfs/", "/"]

    @staticmethod
    def name() -> str:
        return 'dbfs-query'

    def lint(self, code: str) -> Iterable[Advice]:
        try:
            queries = parse_sql(code, read='databricks')
            for query in queries:
                if not query:
                    continue
                yield from self._lint_query(query)
        except SqlParseError as e:
            logger.debug(f"Failed to parse SQL: {code}", exc_info=e)
            yield FromTable.sql_parse_failure(code)

    def _lint_query(self, query: Expression):
        for table in query.find_all(Table):
            # Check table names for deprecated DBFS table names
            yield from self._check_dbfs_folder(table)

    def _check_dbfs_folder(self, table: Table) -> Iterable[Advice]:
        """
        Check if the table is a DBFS table or reference in some way
        and yield a deprecation message if it is
        """
        if any(table.name.startswith(prefix) for prefix in self._dbfs_prefixes):
            yield Deprecation(
                code='dbfs-read-from-sql-query',
                message=f"The use of DBFS is deprecated: {table.name}",
                # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
                start_line=0,
                start_col=0,
                end_line=0,
                end_col=1024,
            )
