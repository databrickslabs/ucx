from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import DependencyProblem, DependencyGraph, Dependency
from databricks.labs.ucx.source_code.linters.files import FileLoader
from databricks.labs.ucx.source_code.linters.python_ast import Tree
from databricks.labs.ucx.source_code.notebooks.magic import PipMagic, MagicCommand


@pytest.mark.parametrize(
    "code,split",
    [
        ("%pip install foo", ["%pip", "install", "foo"]),
        ("%pip install", ["%pip", "install"]),
        ("%pip installl foo", ["%pip", "installl", "foo"]),
        ("%pip install foo --index-url bar", ["%pip", "install", "foo", "--index-url", "bar"]),
        ("%pip install foo --index-url bar", ["%pip", "install", "foo", "--index-url", "bar"]),
        ("%pip install foo --index-url \\\n bar", ["%pip", "install", "foo", "--index-url", "bar"]),
        ("%pip install foo --index-url bar\nmore code", ["%pip", "install", "foo", "--index-url", "bar"]),
        (
            "%pip install foo --index-url bar\\\n -t /tmp/",
            ["%pip", "install", "foo", "--index-url", "bar", "-t", "/tmp/"],
        ),
        ("%pip install foo --index-url \\\n bar", ["%pip", "install", "foo", "--index-url", "bar"]),
        (
            "%pip install ./distribution/dist/thingy-0.0.1-py2.py3-none-any.whl",
            ["%pip", "install", "./distribution/dist/thingy-0.0.1-py2.py3-none-any.whl"],
        ),
        (
            "%pip install distribution/dist/thingy-0.0.1-py2.py3-none-any.whl",
            ["%pip", "install", "distribution/dist/thingy-0.0.1-py2.py3-none-any.whl"],
        ),
    ],
)
def test_pip_command_split(code, split):
    assert PipMagic._split(code) == split  # pylint: disable=protected-access


def test_unsupported_magic_raises_problem(simple_dependency_resolver, mock_path_lookup):
    source = """
%unsupported stuff '"%#@!
"""
    converted = MagicCommand.convert_magic_lines_to_magic_commands(source)
    tree = Tree.parse(converted)
    commands, _ = MagicCommand.extract_from_tree(tree, DependencyProblem.from_node)
    dependency = Dependency(FileLoader(), Path(""))
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    problems = commands[0].build_dependency_graph(graph)
    assert problems[0].code == "unsupported-magic-line"
