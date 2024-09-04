from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

from astroid import NodeNG, Call, Name, Const  # type: ignore

from databricks.labs.ucx.source_code.graph import (
    DependencyGraph,
    DependencyProblem,
    DependencyGraphContext,
    InheritedContext,
)
from databricks.labs.ucx.source_code.python.python_ast import NodeBase, Tree


logger = logging.getLogger(__name__)

T = TypeVar("T")


class MagicLine(NodeBase):

    @classmethod
    def extract_from_tree(
        cls, tree: Tree, problem_factory: Callable[[str, str, NodeNG], T]
    ) -> tuple[list[MagicLine], list[T]]:
        problems: list[T] = []
        commands: list[MagicLine] = []
        try:
            nodes = tree.locate(Call, [("magic_command", Name)])
            for command in cls._make_commands_for_magic_command_call_nodes(nodes):
                commands.append(command)
        except Exception as e:  # pylint: disable=broad-except
            logger.debug(f"Internal error while checking magic commands in tree: {tree.root}", exc_info=True)
            problem = problem_factory('internal-error', f"While checking magic commands: {e}", tree.root)
            problems.append(problem)
        return commands, problems

    @classmethod
    def _make_commands_for_magic_command_call_nodes(cls, nodes: list[Call]):
        for node in nodes:
            arg = node.args[0]
            if isinstance(arg, Const):
                yield MagicLine(node, arg.value)

    def __init__(self, node: NodeNG, command: bytes):
        super().__init__(node)
        self._command = command.decode()

    def as_magic(self) -> MagicCommand | None:
        for factory in _FACTORIES:
            command = factory(self._command, self.node)
            if command is not None:
                return command
        return None

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        magic = self.as_magic()
        if magic is not None:
            return magic.build_dependency_graph(parent)
        problem = DependencyProblem.from_node(
            code='unsupported-magic-line', message=f"magic line '{self._command}' is not supported yet", node=self.node
        )
        return [problem]

    def build_inherited_context(self, context: DependencyGraphContext, child_path: Path) -> InheritedContext:
        magic = self.as_magic()
        if magic is not None:
            return magic.build_inherited_context(context, child_path)
        return InheritedContext(None, False)


class MagicNode(NodeNG):
    pass


class MagicCommand(ABC):

    def __init__(self, node: NodeNG, code: str):
        self._node = node
        self._code = code

    @abstractmethod
    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]: ...

    def build_inherited_context(self, _context: DependencyGraphContext, _child_path: Path) -> InheritedContext:
        return InheritedContext(None, False)


_FACTORIES: list[Callable[[str, NodeNG], MagicCommand | None]] = []


def magic_command_factory(func: Callable[[str, NodeNG], MagicCommand | None]):
    _FACTORIES.append(func)

    def inner(command: str, node: NodeNG) -> MagicCommand | None:
        return func(command, node)

    return inner
