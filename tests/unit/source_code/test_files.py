import codecs
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyProblem
from databricks.labs.ucx.source_code.files import FileLoader, LocalFile, StubContainer
from databricks.labs.ucx.source_code.path_lookup import PathLookup


def test_local_file_content_is_accessible() -> None:
    local_file = LocalFile(Path("test.py"), "print(1)", Language.PYTHON)
    assert local_file.content == "print(1)"


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


def test_file_loader_loads_file_without_permission() -> None:
    path = create_autospec(Path)
    path.suffix = ".py"
    path.open.side_effect = PermissionError("Permission denied")
    dependency = Dependency(FileLoader(), path)
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path

    local_file = dependency.load(path_lookup)

    # TODO: Test specific error while loading: https://github.com/databrickslabs/ucx/issues/3584
    assert local_file is None
    path.open.assert_called_once()
    path_lookup.resolve.assert_called_once_with(path)


def test_file_loader_loads_non_ascii_file(mock_path_lookup) -> None:
    dependency = Dependency(FileLoader(), Path("nonascii.py"))

    local_file = dependency.load(mock_path_lookup)

    # TODO: Test specific error while loading: https://github.com/databrickslabs/ucx/issues/3584
    assert local_file is None
    assert Path("nonascii.py") in mock_path_lookup.successfully_resolved_paths


def test_file_loader_loads_non_existing_file() -> None:
    path = create_autospec(Path)
    path.suffix = ".py"
    path.open.side_effect = FileNotFoundError("No such file or directory: 'test.py'")
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path

    dependency = Dependency(FileLoader(), path)
    local_file = dependency.load(path_lookup)

    assert local_file is None
    path.open.assert_called_once()
    path_lookup.resolve.assert_called_once_with(path)


@pytest.mark.parametrize(
    "bom, encoding",
    [
        (codecs.BOM_UTF8, "utf-8"),
        (codecs.BOM_UTF16_LE, "utf-16-le"),
        (codecs.BOM_UTF16_BE, "utf-16-be"),
        (codecs.BOM_UTF32_LE, "utf-32-le"),
        (codecs.BOM_UTF32_BE, "utf-32-be"),
    ],
)
def test_file_loader_loads_file_with_bom(tmp_path, bom, encoding) -> None:
    path = tmp_path / "file.py"
    path.write_bytes(bom + "a = 12".encode(encoding))
    dependency = Dependency(FileLoader(), path)
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path

    local_file = dependency.load(path_lookup)

    # TODO: Test specific error while loading: https://github.com/databrickslabs/ucx/issues/3584
    assert isinstance(local_file, LocalFile)
    assert local_file.content == "a = 12"
    path_lookup.resolve.assert_called_once_with(path)


def test_file_loader_loads_empty_file(tmp_path) -> None:
    path = tmp_path / "empty.py"
    path.write_text("")
    dependency = Dependency(FileLoader(), path)
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path

    local_file = dependency.load(path_lookup)

    # TODO: Test specific error while loading: https://github.com/databrickslabs/ucx/issues/3584
    assert local_file
    path_lookup.resolve.assert_called_once_with(path)


def test_file_loader_ignores_path_by_loading_it_as_stub_container(tmp_path) -> None:
    path = tmp_path / "path.py"
    dependency = Dependency(FileLoader(exclude_paths={path}), path)
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path

    stub = dependency.load(path_lookup)

    assert isinstance(stub, StubContainer)
    path_lookup.resolve.assert_called_once_with(path)
