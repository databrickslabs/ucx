from pathlib import Path

import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.files import FileLoader, LocalFile
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyProblem


def test_local_file_path_is_accessible() -> None:
    local_file = LocalFile(Path("test.py"), "print(1)", Language.PYTHON)
    assert local_file.path == Path("test.py")


@pytest.mark.parametrize("language", [Language.SQL, Language.SCALA, Language.R])
def test_local_file_builds_dependency_graph_without_problems_independent_from_source(
    simple_dependency_resolver, mock_path_lookup, language: Language
) -> None:
    """Unsupported language and SQL builds a dependency graph without problems"""
    dependency = Dependency(FileLoader(), Path("test.py"))
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    local_file = LocalFile(Path("test.py"), "does not matter", language)
    assert not local_file.build_dependency_graph(graph)


def test_local_file_builds_dependency_graph_without_problems_for_python(
    simple_dependency_resolver, mock_path_lookup
) -> None:
    """No problems should be yielded for the python source code"""
    dependency = Dependency(FileLoader(), Path("test.py"))
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    local_file = LocalFile(Path("test.py"), "print(1)", Language.PYTHON)
    assert not local_file.build_dependency_graph(graph)


def test_local_file_builds_dependency_graph_with_problems_for_python(
    simple_dependency_resolver, mock_path_lookup
) -> None:
    """Problems should be yielded for the python source code"""
    dependency = Dependency(FileLoader(), Path("test.py"))
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    local_file = LocalFile(Path("test.py"), "print(1", Language.PYTHON)  # Missing parenthesis is on purpose
    assert local_file.build_dependency_graph(graph) == [
        DependencyProblem(
            "python-parse-error",
            "Failed to parse code due to invalid syntax: print(1",
            Path("test.py"),
        )
    ]


@pytest.mark.parametrize("language", [Language.SQL, Language.SCALA, Language.R])
def test_local_file_builds_inherited_context_without_tree_found_and_problems_independent_from_source(
    simple_dependency_resolver, mock_path_lookup, language: Language
) -> None:
    """Unsupported language and SQL builds an inherited context without a tree, found flag and problems"""
    dependency = Dependency(FileLoader(), Path("test"))
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    local_file = LocalFile(Path("test"), "does not matter", language)
    inherited_context = local_file.build_inherited_context(graph, Path("child"))
    assert not inherited_context.tree
    assert not inherited_context.found
    assert not inherited_context.problems


def test_local_file_builds_inherited_context_with_tree_without_found_and_problems(
    simple_dependency_resolver, mock_path_lookup
) -> None:
    """A tree should be yielded, but the child nor problems are found."""
    dependency = Dependency(FileLoader(), Path("test.py"))
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    local_file = LocalFile(Path("test.py"), "print(1)", Language.PYTHON)
    inherited_context = local_file.build_inherited_context(graph, Path("child.py"))
    assert inherited_context.tree
    assert not inherited_context.found
    assert not inherited_context.problems


def test_local_file_builds_inherited_context_with_python_parse_error_problem(
    simple_dependency_resolver, mock_path_lookup
) -> None:
    """Problems should be yielded for the python source code"""
    dependency = Dependency(FileLoader(), Path("test.py"))
    graph = DependencyGraph(dependency, None, simple_dependency_resolver, mock_path_lookup, CurrentSessionState())
    local_file = LocalFile(Path("test.py"), "print(1", Language.PYTHON)  # Missing parenthesis is on purpose
    inherited_context = local_file.build_inherited_context(graph, Path("child.py"))
    assert not inherited_context.tree
    assert not inherited_context.found
    assert inherited_context.problems == [
        DependencyProblem(
            "python-parse-error",
            "Failed to parse code due to invalid syntax: print(1",
            Path("test.py"),
        )
    ]
