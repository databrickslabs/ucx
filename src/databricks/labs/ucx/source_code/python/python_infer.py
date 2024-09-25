from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator, Generator
from typing import Any

from astroid import (  # type: ignore
    Attribute,
    Call,
    Const,
    decorators,
    Dict,
    Instance,
    Name,
    NodeNG,
    Uninferable,
)
from astroid.context import InferenceContext, InferenceResult, CallContext  # type: ignore
from astroid.typing import InferenceErrorInfo  # type: ignore
from astroid.exceptions import InferenceError  # type: ignore

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.python.python_ast import Tree

logger = logging.getLogger(__name__)


class InferredValue:
    """Represents 1 or more nodes that together represent the value.
    The list of nodes typically holds one Const element, but for f-strings it
    can hold multiple ones, including Uninferable nodes."""

    @classmethod
    def infer_from_node(cls, node: NodeNG, state: CurrentSessionState | None = None) -> Iterable[InferredValue]:
        cls._contextualize(node, state)
        for inferred_atoms in cls._infer_values(node):
            yield InferredValue(inferred_atoms)

    @classmethod
    def _contextualize(cls, node: NodeNG, state: CurrentSessionState | None):
        if state is None or state.named_parameters is None or len(state.named_parameters) == 0:
            return
        cls._contextualize_dbutils_widgets_get(node, state)
        cls._contextualize_dbutils_widgets_get_all(node, state)

    @classmethod
    def _contextualize_dbutils_widgets_get(cls, node: NodeNG, state: CurrentSessionState):
        root = Tree(node).root
        calls = Tree(root).locate(Call, [("get", Attribute), ("widgets", Attribute), ("dbutils", Name)])
        for call in calls:
            call.func = _DbUtilsWidgetsGetCall(state, call)

    @classmethod
    def _contextualize_dbutils_widgets_get_all(cls, node: NodeNG, state: CurrentSessionState):
        root = Tree(node).root
        calls = Tree(root).locate(Call, [("getAll", Attribute), ("widgets", Attribute), ("dbutils", Name)])
        for call in calls:
            call.func = _DbUtilsWidgetsGetAllCall(state, call)

    @classmethod
    def _infer_values(cls, node: NodeNG) -> Iterator[Iterable[NodeNG]]:
        # deal with node types that don't implement 'inferred()'
        if node is Uninferable or isinstance(node, Const):
            yield [node]
        else:
            yield from cls._safe_infer_internal(node)

    @classmethod
    def _safe_infer_internal(cls, node: NodeNG):
        try:
            yield from cls._unsafe_infer_internal(node)
        except InferenceError as e:
            logger.debug(f"When inferring {node}", exc_info=e)
            yield [Uninferable]

    @classmethod
    def _unsafe_infer_internal(cls, node: NodeNG):
        all_inferred = node.inferred()
        if len(all_inferred) == 0 and isinstance(node, Instance):
            yield [node]
            return
        for inferred in all_inferred:
            # work around infinite recursion of empty lists
            if inferred == node:
                continue
            yield from _LocalInferredValue.do_infer_values(inferred)

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
        for inferred in InferredValue.infer_from_node(arg, self._session_state):
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


class _LocalInferredValue(InferredValue):

    @classmethod
    def do_infer_values(cls, node: NodeNG):
        yield from cls._infer_values(node)


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
