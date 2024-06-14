from __future__ import annotations

import re
import shlex

from astroid import Call, Name, Const, NodeNG  # type: ignore

from databricks.labs.ucx.source_code.graph import DependencyGraph, DependencyProblem
from databricks.labs.ucx.source_code.linters.imports import ProblemFactory
from databricks.labs.ucx.source_code.linters.python_ast import NodeBase, Tree


class MagicCommand(NodeBase):

    @classmethod
    def convert_magic_lines_to_magic_commands(cls, python_code: str):
        lines = python_code.split("\n")
        for i, line in enumerate(lines):
            if not line.startswith("%"):
                continue
            lines[i] = f"magic_command({line.encode()!r})"
        return "\n".join(lines)

    @classmethod
    def extract_from_tree(
        cls, tree: Tree, problem_factory: ProblemFactory
    ) -> tuple[list[MagicCommand], list[DependencyProblem]]:
        problems: list[DependencyProblem] = []
        commands: list[MagicCommand] = []
        try:
            nodes = tree.locate(Call, [("magic_command", Name)])
            for command in cls._make_commands_for_magic_command_call_nodes(nodes):
                commands.append(command)
            return commands, problems
        except Exception as e:  # pylint: disable=broad-except
            problem = problem_factory('internal-error', f"While checking magic commands: {e}", tree.root)
            problems.append(problem)
            return [], problems

    @classmethod
    def _make_commands_for_magic_command_call_nodes(cls, nodes: list[Call]):
        for node in nodes:
            arg = node.args[0]
            if isinstance(arg, Const):
                yield MagicCommand(node, arg.value)

    def __init__(self, node: NodeNG, command: bytes):
        super().__init__(node)
        self._command = command.decode()

    def build_dependency_graph(self, graph: DependencyGraph) -> list[DependencyProblem]:
        if self._command.startswith("%pip"):
            cmd = PipMagic(self._command)
            return cmd.build_dependency_graph(graph)
        problem = DependencyProblem.from_node(
            code='unsupported-magic-line',
            message=f"magic line '{self._command}' is not supported yet",
            node=self.node
        )
        return [problem]


class PipMagic:

    def __init__(self, code: str):
        self._code = code

    def build_dependency_graph(self, graph: DependencyGraph) -> list[DependencyProblem]:
        argv = self._split(self._code)
        if len(argv) == 1:
            return [DependencyProblem("library-install-failed", "Missing command after '%pip'")]
        if argv[1] != "install":
            return [DependencyProblem("library-install-failed", f"Unsupported %pip command: {argv[1]}")]
        if len(argv) == 2:
            return [DependencyProblem("library-install-failed", "Missing arguments after '%pip install'")]
        return graph.register_library(*argv[2:])  # Skipping %pip install

    @staticmethod
    def _split(code) -> list[str]:
        """Split pip cell code into multiple arguments

        Note:
            PipCell should be a pip command, i.e. single line possible spanning multilines escaped with backslashes.

        Sources:
            https://docs.databricks.com/en/libraries/notebooks-python-libraries.html#manage-libraries-with-pip-commands
        """
        match = re.search(r"(?<!\\)\n", code)
        if match:
            code = code[: match.start()]  # Remove code after non-escaped newline
        code = code.replace("\\\n", " ")
        lexer = shlex.split(code, posix=True)
        return list(lexer)
