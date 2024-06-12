from __future__ import annotations

import abc
import logging
from collections.abc import Iterable, Iterator
from typing import TypeVar

from astroid import Assign, Attribute, Call, Const, FormattedValue, Import, ImportFrom, JoinedStr, Module, Name, NodeNG, parse, Uninferable  # type: ignore

logger = logging.getLogger(__name__)

missing_handlers: set[str] = set()


T = TypeVar("T", bound=NodeNG)


class Tree:

    @staticmethod
    def parse(code: str):
        root = parse(code)
        return Tree(root)

    def __init__(self, root: NodeNG):
        self._root: NodeNG = root

    @property
    def root(self):
        return self._root

    def walk(self) -> Iterable[NodeNG]:
        yield from self._walk(self._root)

    @classmethod
    def _walk(cls, node: NodeNG) -> Iterable[NodeNG]:
        yield node
        for child in node.get_children():
            yield from cls._walk(child)

    def locate(self, node_type: type[T], match_nodes: list[tuple[str, type]]) -> list[T]:
        visitor = MatchingVisitor(node_type, match_nodes)
        visitor.visit(self._root)
        return visitor.matched_nodes

    def first_statement(self):
        if isinstance(self._root, Module):
            return self._root.body[0]
        return None

    @classmethod
    def extract_call_by_name(cls, call: Call, name: str) -> Call | None:
        """Given a call-chain, extract its sub-call by method name (if it has one)"""
        assert isinstance(call, Call)
        node = call
        while True:
            func = node.func
            if not isinstance(func, Attribute):
                return None
            if func.attrname == name:
                return node
            if not isinstance(func.expr, Call):
                return None
            node = func.expr

    @classmethod
    def args_count(cls, node: Call) -> int:
        """Count the number of arguments (positionals + keywords)"""
        assert isinstance(node, Call)
        return len(node.args) + len(node.keywords)

    @classmethod
    def get_arg(
        cls,
        node: Call,
        arg_index: int | None,
        arg_name: str | None,
    ) -> NodeNG | None:
        """Extract the call argument identified by an optional position or name (if it has one)"""
        assert isinstance(node, Call)
        if arg_index is not None and len(node.args) > arg_index:
            return node.args[arg_index]
        if arg_name is not None:
            arg = [kw.value for kw in node.keywords if kw.arg == arg_name]
            if len(arg) == 1:
                return arg[0]
        return None

    @classmethod
    def is_none(cls, node: NodeNG) -> bool:
        """Check if the given AST expression is the None constant"""
        if not isinstance(node, Const):
            return False
        return node.value is None

    def __repr__(self):
        truncate_after = 32
        code = repr(self._root)
        if len(code) > truncate_after:
            code = code[0:truncate_after] + "..."
        return f"<Tree: {code}>"

    @classmethod
    def get_full_attribute_name(cls, node: Attribute) -> str:
        return cls._get_attribute_value(node)

    @classmethod
    def get_full_function_name(cls, node: Call) -> str | None:
        if not isinstance(node, Call):
            return None
        if isinstance(node.func, Attribute):
            return cls._get_attribute_value(node.func)
        if isinstance(node.func, Name):
            return node.func.name
        return None

    @classmethod
    def _get_attribute_value(cls, node: Attribute):
        if isinstance(node.expr, Name):
            return node.expr.name + '.' + node.attrname
        if isinstance(node.expr, Attribute):
            parent = cls._get_attribute_value(node.expr)
            return node.attrname if parent is None else parent + '.' + node.attrname
        if isinstance(node.expr, Call):
            name = cls.get_full_function_name(node.expr)
            return node.attrname if name is None else name + '.' + node.attrname
        name = type(node.expr).__name__
        if name not in missing_handlers:
            missing_handlers.add(name)
            logger.debug(f"Missing handler for {name}")
        return None

    def infer_values(self) -> Iterable[InferredValue]:
        for inferred_atoms in self._infer_values():
            yield InferredValue(inferred_atoms)

    def _infer_values(self) -> Iterator[Iterable[NodeNG]]:
        # deal with node types that don't implement 'inferred()'
        if self._root is Uninferable or isinstance(self._root, Const):
            yield [self._root]
        elif isinstance(self._root, JoinedStr):
            yield from self._infer_values_from_joined_string()
        elif isinstance(self._root, FormattedValue):
            yield from _LocalTree(self._root.value).do_infer_values()
        else:
            for inferred in self._root.inferred():
                # work around infinite recursion of empty lists
                if inferred == self._root:
                    continue
                yield from _LocalTree(inferred).do_infer_values()

    def _infer_values_from_joined_string(self) -> Iterator[Iterable[NodeNG]]:
        assert isinstance(self._root, JoinedStr)
        yield from self._infer_values_from_joined_values(self._root.values)

    @classmethod
    def _infer_values_from_joined_values(cls, nodes: list[NodeNG]) -> Iterator[Iterable[NodeNG]]:
        if len(nodes) == 1:
            yield from _LocalTree(nodes[0]).do_infer_values()
            return
        for firsts in _LocalTree(nodes[0]).do_infer_values():
            for remains in cls._infer_values_from_joined_values(nodes[1:]):
                yield list(firsts) + list(remains)


class _LocalTree(Tree):
    """class that avoids pylint W0212 protected-access warning"""

    def do_infer_values(self):
        return self._infer_values()


class InferredValue:
    """Represents 1 or more nodes that together represent the value.
    The list of nodes typically holds one Const element, but for f-strings it
    can hold multiple ones, including Uninferable nodes."""

    def __init__(self, atoms: Iterable[NodeNG]):
        self._atoms = list(atoms)

    def nodes(self):
        return self._atoms

    def is_inferred(self):
        return all(atom is not Uninferable for atom in self._atoms)

    def as_string(self):
        strings = [str(const.value) for const in filter(lambda atom: isinstance(atom, Const), self._atoms)]
        return "".join(strings)


class TreeVisitor:

    def visit(self, node: NodeNG):
        self._visit_specific(node)
        for child in node.get_children():
            self.visit(child)

    def _visit_specific(self, node: NodeNG):
        method_name = "visit_" + type(node).__name__.lower()
        method_slot = getattr(self, method_name, None)
        if callable(method_slot):
            method_slot(node)
            return
        self.visit_nodeng(node)

    def visit_nodeng(self, node: NodeNG):
        pass


class MatchingVisitor(TreeVisitor):

    def __init__(self, node_type: type, match_nodes: list[tuple[str, type]]):
        super()
        self._matched_nodes: list[NodeNG] = []
        self._node_type = node_type
        self._match_nodes = match_nodes

    @property
    def matched_nodes(self):
        return self._matched_nodes

    def visit_assign(self, node: Assign):
        if self._node_type is not Assign:
            return
        self._matched_nodes.append(node)

    def visit_call(self, node: Call):
        if self._node_type is not Call:
            return
        try:
            if self._matches(node.func, 0):
                self._matched_nodes.append(node)
        except NotImplementedError as e:
            logger.warning(f"Missing implementation: {e.args[0]}")

    def visit_import(self, node: Import):
        if self._node_type is not Import:
            return
        self._matched_nodes.append(node)

    def visit_importfrom(self, node: ImportFrom):
        if self._node_type is not ImportFrom:
            return
        self._matched_nodes.append(node)

    def _matches(self, node: NodeNG, depth: int):
        if depth >= len(self._match_nodes):
            return False
        name, match_node = self._match_nodes[depth]
        if not isinstance(node, match_node):
            return False
        next_node: NodeNG | None = None
        if isinstance(node, Attribute):
            if node.attrname != name:
                return False
            next_node = node.expr
        elif isinstance(node, Name):
            if node.name != name:
                return False
        else:
            raise NotImplementedError(str(type(node)))
        if next_node is None:
            # is this the last node to match ?
            return len(self._match_nodes) - 1 == depth
        return self._matches(next_node, depth + 1)


class NodeBase(abc.ABC):

    def __init__(self, node: NodeNG):
        self._node = node

    @property
    def node(self):
        return self._node

    def __repr__(self):
        return f"<{self.__class__.__name__}: {repr(self._node)}>"
