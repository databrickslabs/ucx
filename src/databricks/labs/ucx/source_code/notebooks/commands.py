import re
import shlex
from typing import TypeVar, cast

from databricks.labs.ucx.source_code.graph import DependencyGraph, DependencyProblem

T = TypeVar("T")


class PipCommand:

    def __init__(self, code: str):
        self._code = code

    def execute(self, _: T, **kwargs) -> T:
        command = kwargs["command"]
        if command == "build_dependency_graph" and "graph" in kwargs:
            graph = kwargs["graph"]
            assert isinstance(graph, DependencyGraph)
            return cast(T, self.build_dependency_graph(graph))
        return cast(T, None)

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
