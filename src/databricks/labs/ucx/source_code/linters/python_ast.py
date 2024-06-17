from __future__ import annotations

from abc import ABC
import logging
from collections.abc import Iterable, Iterator, Generator
from typing import Any, TypeVar

from astroid import Assign, Attribute, Call, Const, decorators, Dict, FormattedValue, Import, ImportFrom, JoinedStr, Module, Name, NodeNG, parse, Uninferable  # type: ignore
from astroid.context import InferenceContext, InferenceResult, CallContext  # type: ignore
from astroid.typing import InferenceErrorInfo  # type: ignore
from astroid.exceptions import InferenceError  # type: ignore

from databricks.labs.ucx.source_code.base import CurrentSessionState

logger = logging.getLogger(__name__)

missing_handlers: set[str] = set()


T = TypeVar("T", bound=NodeNG)


class Tree:

    @staticmethod
    def parse(code: str):
        root = parse(code)
        return Tree(root)

    @classmethod
    def convert_magic_lines_to_magic_commands(cls, python_code: str):
        lines = python_code.split("\n")
        for i, line in enumerate(lines):
            if not line.startswith("%"):
                continue
            lines[i] = f"magic_command({line.encode()!r})"
        return "\n".join(lines)

    def __init__(self, node: NodeNG):
        self._node: NodeNG = node

    @property
    def node(self):
        return self._node

    @property
    def root(self):
        node = self._node
        while node.parent:
            node = node.parent
        return node

    def walk(self) -> Iterable[NodeNG]:
        yield from self._walk(self._node)

    @classmethod
    def _walk(cls, node: NodeNG) -> Iterable[NodeNG]:
        yield node
        for child in node.get_children():
            yield from cls._walk(child)

    def locate(self, node_type: type[T], match_nodes: list[tuple[str, type]]) -> list[T]:
        visitor = MatchingVisitor(node_type, match_nodes)
        visitor.visit(self._node)
        return visitor.matched_nodes

    def first_statement(self):
        if isinstance(self._node, Module):
            if len(self._node.body) > 0:
                return self._node.body[0]
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
        code = repr(self._node)
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

    def infer_values(self, state: CurrentSessionState | None = None) -> Iterable[InferredValue]:
        self._contextualize(state)
        for inferred_atoms in self._infer_values():
            yield InferredValue(inferred_atoms)

    def _contextualize(self, state: CurrentSessionState | None):
        if state is None or state.named_parameters is None or len(state.named_parameters) == 0:
            return
        self._contextualize_dbutils_widgets_get(state)
        self._contextualize_dbutils_widgets_get_all(state)

    def _contextualize_dbutils_widgets_get(self, state: CurrentSessionState):
        calls = Tree(self.root).locate(Call, [("get", Attribute), ("widgets", Attribute), ("dbutils", Name)])
        for call in calls:
            call.func = _DbUtilsWidgetsGetCall(state, call)

    def _contextualize_dbutils_widgets_get_all(self, state: CurrentSessionState):
        calls = Tree(self.root).locate(Call, [("getAll", Attribute), ("widgets", Attribute), ("dbutils", Name)])
        for call in calls:
            call.func = _DbUtilsWidgetsGetAllCall(state, call)

    def _infer_values(self) -> Iterator[Iterable[NodeNG]]:
        # deal with node types that don't implement 'inferred()'
        if self._node is Uninferable or isinstance(self._node, Const):
            yield [self._node]
        elif isinstance(self._node, JoinedStr):
            yield from self._infer_values_from_joined_string()
        elif isinstance(self._node, FormattedValue):
            yield from _LocalTree(self._node.value).do_infer_values()
        else:
            yield from self._infer_internal()

    def _infer_internal(self):
        try:
            for inferred in self._node.inferred():
                # work around infinite recursion of empty lists
                if inferred == self._node:
                    continue
                yield from _LocalTree(inferred).do_infer_values()
        except InferenceError as e:
            logger.debug(f"When inferring {self._node}", exc_info=e)
            yield [Uninferable]

    def _infer_values_from_joined_string(self) -> Iterator[Iterable[NodeNG]]:
        assert isinstance(self._node, JoinedStr)
        yield from self._infer_values_from_joined_values(self._node.values)

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


class _DbUtilsWidgetsGetCall(NodeNG):

    def __init__(self, session_state: CurrentSessionState, node: NodeNG):
        super().__init__(
            lineno=node.lineno,
            col_offset=node.col_offset,
            end_lineno=node.end_lineno,
            end_col_offset=node.end_col_offset,
            parent=node.parent,
        )
        self._session_state = session_state

    @decorators.raise_if_nothing_inferred
    def _infer(
        self, context: InferenceContext | None = None, **kwargs: Any
    ) -> Generator[InferenceResult, None, InferenceErrorInfo | None]:
        yield self
        return InferenceErrorInfo(node=self, context=context)

    def infer_call_result(self, context: InferenceContext | None = None, **_):  # caller needs unused kwargs
        call_context = getattr(context, "callcontext", None)
        if not isinstance(call_context, CallContext):
            yield Uninferable
            return
        arg = call_context.args[0]
        for inferred in Tree(arg).infer_values(self._session_state):
            if not inferred.is_inferred():
                yield Uninferable
                continue
            name = inferred.as_string()
            named_parameters = self._session_state.named_parameters
            if not named_parameters or name not in named_parameters:
                yield Uninferable
                continue
            value = named_parameters[name]
            yield Const(
                value,
                lineno=self.lineno,
                col_offset=self.col_offset,
                end_lineno=self.end_lineno,
                end_col_offset=self.end_col_offset,
                parent=self,
            )


class _DbUtilsWidgetsGetAllCall(NodeNG):

    def __init__(self, session_state: CurrentSessionState, node: NodeNG):
        super().__init__(
            lineno=node.lineno,
            col_offset=node.col_offset,
            end_lineno=node.end_lineno,
            end_col_offset=node.end_col_offset,
            parent=node.parent,
        )
        self._session_state = session_state

    @decorators.raise_if_nothing_inferred
    def _infer(
        self, context: InferenceContext | None = None, **kwargs: Any
    ) -> Generator[InferenceResult, None, InferenceErrorInfo | None]:
        yield self
        return InferenceErrorInfo(node=self, context=context)

    def infer_call_result(self, **_):  # caller needs unused kwargs
        named_parameters = self._session_state.named_parameters
        if not named_parameters:
            yield Uninferable
            return
        items = self._populate_items(named_parameters)
        result = Dict(
            lineno=self.lineno,
            col_offset=self.col_offset,
            end_lineno=self.end_lineno,
            end_col_offset=self.end_col_offset,
            parent=self,
        )
        result.postinit(items)
        yield result

    def _populate_items(self, values: dict[str, str]):
        items: list[tuple[InferenceResult, InferenceResult]] = []
        for key, value in values.items():
            item_key = Const(
                key,
                lineno=self.lineno,
                col_offset=self.col_offset,
                end_lineno=self.end_lineno,
                end_col_offset=self.end_col_offset,
                parent=self,
            )
            item_value = Const(
                value,
                lineno=self.lineno,
                col_offset=self.col_offset,
                end_lineno=self.end_lineno,
                end_col_offset=self.end_col_offset,
                parent=self,
            )
            items.append((item_key, item_value))
        return items


class InferredValue:
    """Represents 1 or more nodes that together represent the value.
    The list of nodes typically holds one Const element, but for f-strings it
    can hold multiple ones, including Uninferable nodes."""

    def __init__(self, atoms: Iterable[NodeNG]):
        self._atoms = list(atoms)

    @property
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


class NodeBase(ABC):

    def __init__(self, node: NodeNG):
        self._node = node

    @property
    def node(self):
        return self._node

    def __repr__(self):
        return f"<{self.__class__.__name__}: {repr(self._node)}>"
