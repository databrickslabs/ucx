from __future__ import annotations

import ast
import logging
from collections.abc import Iterable

from databricks.labs.ucx.source_code.base import Linter, Advice, Advisory


logger = logging.getLogger(__name__)


class MatchingVisitor(ast.NodeVisitor):

    def __init__(self, node_type: type, match_nodes: list[tuple[str, type]]):
        self._matched_nodes: list[ast.AST] = []
        self._node_type = node_type
        self._match_nodes = match_nodes

    @property
    def matched_nodes(self):
        return self._matched_nodes

    # pylint: disable=invalid-name
    def visit_Call(self, node: ast.Call):
        if self._node_type is not ast.Call:
            return
        try:
            if self._matches(node.func, 0):
                self._matched_nodes.append(node)
        except NotImplementedError as e:
            logger.warning(f"Missing implementation: {e.args[0]}")

    def _matches(self, node: ast.AST, depth: int):
        if depth >= len(self._match_nodes):
            return False
        pair = self._match_nodes[depth]
        if not isinstance(node, pair[1]):
            return False
        next_node: ast.AST | None = None
        if isinstance(node, ast.Attribute):
            if node.attr != pair[0]:
                return False
            next_node = node.value
        elif isinstance(node, ast.Name):
            if node.id != pair[0]:
                return False
        else:
            raise NotImplementedError(str(type(node)))
        if next_node is None:
            # is this the last node to match ?
            return len(self._match_nodes) - 1 == depth
        return self._matches(next_node, depth + 1)


# disclaimer this class is NOT thread-safe
class ASTLinter:

    @staticmethod
    def parse(code: str):
        root = ast.parse(code)
        return ASTLinter(root)

    def __init__(self, root: ast.AST):
        self._root: ast.AST = root

    def locate(self, node_type: type, match_nodes: list[tuple[str, type]]) -> list[ast.AST]:
        visitor = MatchingVisitor(node_type, match_nodes)
        visitor.visit(self._root)
        return visitor.matched_nodes


class PythonLinter(Linter):

    def lint(self, code: str) -> Iterable[Advice]:
        linter = ASTLinter.parse(code)
        nodes = linter.locate(ast.Call, [("run", ast.Attribute), ("notebook", ast.Attribute), ("dbutils", ast.Name)])
        return [self._convert_dbutils_notebook_run_to_advice(node) for node in nodes]

    @classmethod
    def _convert_dbutils_notebook_run_to_advice(cls, node: ast.AST) -> Advisory:
        assert isinstance(node, ast.Call)
        path = cls.get_dbutils_notebook_run_path_arg(node)
        if isinstance(path, ast.Constant):
            return Advisory(
                'notebook-auto-migrate',
                "Call to 'dbutils.notebook.run' will be migrated automatically",
                node.lineno,
                node.col_offset,
                node.end_lineno or 0,
                node.end_col_offset or 0,
            )
        return Advisory(
            'notebook-manual-migrate',
            "Path for 'dbutils.notebook.run' is not a constant and requires adjusting the notebook path",
            node.lineno,
            node.col_offset,
            node.end_lineno or 0,
            node.end_col_offset or 0,
        )

    @staticmethod
    def get_dbutils_notebook_run_path_arg(node: ast.Call):
        if len(node.args) > 0:
            return node.args[0]
        arg = next(kw for kw in node.keywords if kw.arg == "path")
        return arg.value if arg is not None else None
