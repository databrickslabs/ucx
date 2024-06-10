import logging
from collections.abc import Iterable

from astroid import Call, Const, InferenceError, NodeNG  # type: ignore
import sqlglot
from sqlglot.expressions import Table

from databricks.labs.ucx.source_code.base import Advice, Linter, Deprecation
from databricks.labs.ucx.source_code.linters.python_ast import Tree, TreeVisitor


logger = logging.getLogger(__file__)


class DetectDbfsVisitor(TreeVisitor):
    """
    Visitor that detects file system paths in Python code and checks them
    against a list of known deprecated paths.
    """

    def __init__(self):
        self._advices: list[Advice] = []
        self._fs_prefixes = ["/dbfs/mnt", "dbfs:/", "/mnt/"]
        self._reported_locations = set()  # Set to store reported locations; astroid coordinates!

    def visit_call(self, node: Call):
        for arg in node.args:
            self._visit_arg(arg)

    def _visit_arg(self, arg: NodeNG):
        try:
            for inferred in arg.inferred():
                if isinstance(inferred, Const) and isinstance(inferred.value, str):
                    self._check_str_constant(arg, inferred)
        except InferenceError:
            logger.debug(f"Could not infer value of {str(arg)}")

    def visit_const(self, node: Const):
        # Constant strings yield Advisories
        if isinstance(node.value, str):
            self._check_str_constant(node, node)

    def _check_str_constant(self, source_node, const_node: Const):
        if self._already_reported(source_node, const_node):
            return
        value = const_node.value
        if any(value.startswith(prefix) for prefix in self._fs_prefixes):
            advisory = Deprecation.from_node(
                code='dbfs-usage', message=f"Deprecated file system path: {value}", node=source_node
            )
            self._advices.append(advisory)

    def _already_reported(self, *nodes: NodeNG):
        reported = any((node.lineno, node.col_offset) in self._reported_locations for node in nodes)
        for node in nodes:
            self._reported_locations.add((node.lineno, node.col_offset))
        return reported

    def get_advices(self) -> Iterable[Advice]:
        yield from self._advices


class DBFSUsageLinter(Linter):
    def __init__(self):
        pass

    @staticmethod
    def name() -> str:
        """
        Returns the name of the linter, for reporting etc
        """
        return 'dbfs-usage'

    def lint(self, code: str) -> Iterable[Advice]:
        """
        Lints the code looking for file system paths that are deprecated
        """
        tree = Tree.parse(code)
        visitor = DetectDbfsVisitor()
        visitor.visit(tree.root)
        yield from visitor.get_advices()


class FromDbfsFolder(Linter):
    def __init__(self):
        self._dbfs_prefixes = ["/dbfs/mnt", "dbfs:/", "/mnt/", "/dbfs/", "/"]

    @staticmethod
    def name() -> str:
        return 'dbfs-query'

    def lint(self, code: str) -> Iterable[Advice]:
        for statement in sqlglot.parse(code, read='databricks'):
            if not statement:
                continue
            for table in statement.find_all(Table):
                # Check table names for deprecated DBFS table names
                yield from self._check_dbfs_folder(table)

    def _check_dbfs_folder(self, table: Table) -> Iterable[Advice]:
        """
        Check if the table is a DBFS table or reference in some way
        and yield a deprecation message if it is
        """
        if any(table.name.startswith(prefix) for prefix in self._dbfs_prefixes):
            yield Deprecation(
                code='dbfs-query',
                message=f"The use of DBFS is deprecated: {table.name}",
                # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
                start_line=0,
                start_col=0,
                end_line=0,
                end_col=1024,
            )
