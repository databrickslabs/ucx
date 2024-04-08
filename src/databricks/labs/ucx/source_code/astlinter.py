import ast
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Segment:
    start_line: int
    start_col: int
    end_line: int
    end_col: int


class LocatingVisitor(ast.NodeVisitor):

    def __init__(self, node_type: type, match_nodes: list[tuple[str, type]]):
        self.locations: list[Segment] = []
        self._node_type = node_type
        self._match_nodes = match_nodes

    # pylint: disable=invalid-name
    def visit_Call(self, node: ast.Call):
        if self._node_type is not ast.Call:
            return
        try:
            if self._matches(node.func, 0):
                self.locations.append(
                    Segment(node.lineno, node.col_offset, node.end_lineno or 0, node.end_col_offset or 0)
                )
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


class ASTLinter:

    def __init__(self):
        self._module: ast.Module | None = None

    def parse(self, code: str):
        self._module = ast.parse(code)

    def locate(self, node_type: type, match_nodes: list[tuple[str, type]]) -> list[Segment]:
        assert self._module is not None
        visitor = LocatingVisitor(node_type, match_nodes)
        visitor.visit(self._module)
        return visitor.locations
