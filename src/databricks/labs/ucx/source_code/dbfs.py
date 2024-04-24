import ast
from collections.abc import Iterable

import sqlglot
from sqlglot.expressions import Table

from databricks.labs.ucx.source_code.base import Advice, Linter, Advisory, Deprecation


class DetectDbfsVisitor(ast.NodeVisitor):
    """
    Visitor that detects file system paths in Python code and checks them
    against a list of known deprecated paths.
    """

    def __init__(self):
        self._advices: list[Advice] = []
        self._fs_prefixes = ["/dbfs/mnt", "dbfs:/", "/mnt/"]
        self._reported_locations = set()  # Set to store reported locations

    def visit_Call(self, node):
        for arg in node.args:
            if isinstance(arg, (ast.Str, ast.Constant)) and isinstance(arg.s, str):
                if any(arg.s.startswith(prefix) for prefix in self._fs_prefixes):
                    self._advices.append(
                        Deprecation(
                            code='dbfs-usage',
                            message=f"Deprecated file system path in call to: {arg.s}",
                            start_line=arg.lineno,
                            start_col=arg.col_offset,
                            end_line=arg.lineno,
                            end_col=arg.col_offset + len(arg.s),
                        )
                    )
                    # Record the location of the reported constant, so we do not double report
                    self._reported_locations.add((arg.lineno, arg.col_offset))
        self.generic_visit(node)

    def visit_Constant(self, node):
        # Constant strings yield Advisories
        if isinstance(node.value, str):
            self._check_str_constant(node)

    def _check_str_constant(self, node):
        # Check if the location has been reported before
        if (node.lineno, node.col_offset) not in self._reported_locations:
            if any(node.s.startswith(prefix) for prefix in self._fs_prefixes):
                self._advices.append(
                    Advisory(
                        code='dbfs-usage',
                        message=f"Possible deprecated file system path: {node.s}",
                        start_line=node.lineno,
                        start_col=node.col_offset,
                        end_line=node.lineno,
                        end_col=node.col_offset + len(node.s),
                    )
                )
        self.generic_visit(node)

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
        tree = ast.parse(code)
        visitor = DetectDbfsVisitor()
        visitor.visit(tree)
        return visitor.get_advices()


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
