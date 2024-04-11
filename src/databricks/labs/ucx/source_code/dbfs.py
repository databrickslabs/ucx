import ast
from collections.abc import Iterable

from databricks.labs.ucx.source_code.base import Advice, Linter, Advisory, Deprecation


class DetectDbfsVisitor(ast.NodeVisitor):
    """
    Visitor that detects file system paths in Python code and checks them
    against a list of known deprecated paths.
    """

    def __init__(self):
        self._advices: list[Advice] = []
        self._fs_prefixes = ["/dbfs/mnt", "dbfs:/", "/mnt/"]

    def visit_Call(self, node):  # pylint: disable=invalid-name
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
        self.generic_visit(node)

    def visit_Constant(self, node):
        # Constant strings yield Advisories
        if isinstance(node.value, str):
            self._check_str_constant(node)

    def _check_str_constant(self, node):
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
