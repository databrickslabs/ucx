from unittest.mock import create_autospec
import re

import pytest
from databricks.sdk.service.workspace import Language, ObjectType, ObjectInfo
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.source_code.notebook import WorkspaceNotebook
from databricks.labs.ucx.source_code.dependencies import (
    DependencyGraph,
    SourceContainer,
    DependencyResolver,
)
from tests.unit import _load_sources, _download_side_effect, whitelist_mock, site_packages_mock

# fmt: off
# the following samples are real samples from https://github.com/databricks-industry-solutions
# please keep them untouched, we want our unit tests to run against genuinely representative data
PYTHON_NOTEBOOK_SAMPLE = (
    "00_var_context.py.txt",
    Language.PYTHON,
    ['md', 'md', 'md', 'python', 'python', 'python', 'md', 'python', 'md'],
)
PYTHON_NOTEBOOK_WITH_RUN_SAMPLE = (
    "01_var_market_etl.py.txt",
    Language.PYTHON,
    ['md', 'run', 'md', 'python', 'md', 'python', 'python', 'python', 'python', 'md', 'python', 'python',
     'md', 'python', 'python', 'python', 'md', 'python', 'md', 'python', 'python', 'md', 'python'],
)
SCALA_NOTEBOOK_SAMPLE = (
    "01_HL7Streaming.scala.txt",
    Language.SCALA,
    ['md', 'md', 'scala', 'sql', 'md', 'scala', 'scala', 'md', 'md', 'scala', 'md', 'scala', 'sql', 'sql',
     'md', 'scala', 'md', 'scala', 'sql', 'sql', 'sql', 'sql', 'sql', 'sql', 'sql', 'md', 'scala', 'md', 'md'],
)
R_NOTEBOOK_SAMPLE = (
    "3_SparkR_Fine Grained Demand Forecasting.r.txt",
    Language.R,
    ['md', 'md', 'md', 'r', 'r', 'md', 'run', 'r', 'md', 'sql', 'md', 'sql', 'md', 'sql', 'md', 'md', 'r', 'md',
     'r', 'md', 'r', 'md', 'r', 'md', 'r', 'md', 'md', 'r', 'md', 'md', 'r', 'md', 'r', 'md', 'r', 'md', 'md',
     'r', 'md', 'r', 'md', 'r', 'md', 'r', 'md', 'sql', 'md', 'sql', 'md'],
)
SQL_NOTEBOOK_SAMPLE = (
    "chf-pqi-scoring.sql.txt",
    Language.SQL,
    ['md', 'sql', 'sql', 'md', 'sql', 'python', 'sql', 'sql', 'sql', 'md', 'sql', 'sql', 'md', 'sql', 'sql', 'md', 'sql'],
)
SHELL_NOTEBOOK_SAMPLE = (
    "notebook-with-shell-cell.py.txt",
    Language.PYTHON,
    ['python', 'sh'],
)
PIP_NOTEBOOK_SAMPLE = (
    "notebook-with-pip-cell.py.txt",
    Language.PYTHON,
    ['python', 'pip'],
)
# fmt: on


@pytest.mark.parametrize(
    "source",
    [
        PYTHON_NOTEBOOK_SAMPLE,
        PYTHON_NOTEBOOK_WITH_RUN_SAMPLE,
        SCALA_NOTEBOOK_SAMPLE,
        R_NOTEBOOK_SAMPLE,
        SQL_NOTEBOOK_SAMPLE,
        SHELL_NOTEBOOK_SAMPLE,
        PIP_NOTEBOOK_SAMPLE,
    ],
)
def test_notebook_splits_source_into_cells(source: tuple[str, Language, list[str]]):
    path = source[0]
    sources: list[str] = _load_sources(SourceContainer, path)
    assert len(sources) == 1
    notebook = WorkspaceNotebook.parse(path, sources[0], source[1])
    assert notebook is not None
    languages = [cell.language.magic_name for cell in notebook.cells]
    assert languages == source[2]


@pytest.mark.parametrize(
    "source",
    [
        PYTHON_NOTEBOOK_SAMPLE,
        PYTHON_NOTEBOOK_WITH_RUN_SAMPLE,
        SCALA_NOTEBOOK_SAMPLE,
        R_NOTEBOOK_SAMPLE,
        SQL_NOTEBOOK_SAMPLE,
    ],
)
def test_notebook_rebuilds_same_code(source: tuple[str, Language, list[str]]):
    path = source[0]
    sources: list[str] = _load_sources(SourceContainer, path)
    assert len(sources) == 1
    notebook = WorkspaceNotebook.parse(path, sources[0], source[1])
    assert notebook is not None
    new_source = notebook.to_migrated_code()
    # ignore trailing whitespaces
    actual_purified = re.sub(r'\s+$', '', new_source, flags=re.MULTILINE)
    expected_purified = re.sub(r'\s+$', '', sources[0], flags=re.MULTILINE)
    assert actual_purified == expected_purified


@pytest.mark.parametrize(
    "source",
    [
        PYTHON_NOTEBOOK_SAMPLE,
        PYTHON_NOTEBOOK_WITH_RUN_SAMPLE,
        SCALA_NOTEBOOK_SAMPLE,
        R_NOTEBOOK_SAMPLE,
        SQL_NOTEBOOK_SAMPLE,
    ],
)
def test_notebook_generates_runnable_cells(source: tuple[str, Language, list[str]]):
    path = source[0]
    sources: list[str] = _load_sources(SourceContainer, path)
    assert len(sources) == 1
    notebook = WorkspaceNotebook.parse(path, sources[0], source[1])
    assert notebook is not None
    for cell in notebook.cells:
        assert cell.is_runnable()


def test_notebook_builds_leaf_dependency_graph():
    paths = ["leaf1.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.return_value = ObjectInfo(
        object_type=ObjectType.NOTEBOOK, path="leaf1.py.txt", language=Language.PYTHON
    )
    resolver = DependencyResolver(ws, whitelist_mock(), site_packages_mock())
    dependency = resolver.resolve_object_info(
        ObjectInfo(object_type=ObjectType.NOTEBOOK, path=paths[0], language=Language.PYTHON), lambda advice: None
    )
    graph = DependencyGraph(dependency, None, resolver)
    container = dependency.load()
    container.build_dependency_graph(graph)
    assert graph.paths == {"leaf1.py.txt"}


def get_status_side_effect(*args):
    path = args[0]
    return ObjectInfo(path=path, object_type=ObjectType.NOTEBOOK, language=Language.PYTHON)


def test_notebook_builds_depth1_dependency_graph():
    paths = ["root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    resolver = DependencyResolver(ws, whitelist_mock(), site_packages_mock())
    dependency = resolver.resolve_object_info(
        ObjectInfo(object_type=ObjectType.NOTEBOOK, path=paths[0], language=Language.PYTHON), lambda advice: None
    )
    graph = DependencyGraph(dependency, None, resolver)
    container = dependency.load()
    container.build_dependency_graph(graph)
    actual = {path[2:] if path.startswith('./') else path for path in graph.paths}
    assert actual == set(paths)


def test_notebook_builds_depth2_dependency_graph():
    paths = ["root2.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    resolver = DependencyResolver(ws, whitelist_mock(), site_packages_mock())
    dependency = resolver.resolve_object_info(
        ObjectInfo(object_type=ObjectType.NOTEBOOK, path=paths[0], language=Language.PYTHON), lambda advice: None
    )
    graph = DependencyGraph(dependency, None, resolver)
    container = dependency.load()
    container.build_dependency_graph(graph)
    actual = {path[2:] if path.startswith('./') else path for path in graph.paths}
    assert actual == set(paths)


def test_notebook_builds_dependency_graph_avoiding_duplicates():
    paths = ["root3.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited = {}
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    resolver = DependencyResolver(ws, whitelist_mock(), site_packages_mock())
    dependency = resolver.resolve_object_info(
        ObjectInfo(object_type=ObjectType.NOTEBOOK, path=paths[0], language=Language.PYTHON), lambda advice: None
    )
    graph = DependencyGraph(dependency, None, resolver)
    container = dependency.load()
    container.build_dependency_graph(graph)
    # if visited once only, set and list will have same len
    assert len(set(visited)) == len(visited)


def test_notebook_builds_cyclical_dependency_graph():
    paths = ["cyclical1.run.py.txt", "cyclical2.run.py.txt"]
    sources: list[str] = _load_sources(SourceContainer, *paths)
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    resolver = DependencyResolver(ws, whitelist_mock(), site_packages_mock())
    dependency = resolver.resolve_object_info(
        ObjectInfo(object_type=ObjectType.NOTEBOOK, path=paths[0], language=Language.PYTHON), lambda advice: None
    )
    graph = DependencyGraph(dependency, None, resolver)
    container = dependency.load()
    container.build_dependency_graph(graph)
    actual = {path[2:] if path.startswith('./') else path for path in graph.paths}
    assert actual == set(paths)


def test_notebook_builds_python_dependency_graph():
    paths = ["root4.py.txt", "leaf3.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    resolver = DependencyResolver(ws, whitelist_mock(), site_packages_mock())
    dependency = resolver.resolve_object_info(
        ObjectInfo(object_type=ObjectType.NOTEBOOK, path=paths[0], language=Language.PYTHON), lambda advice: None
    )
    graph = DependencyGraph(dependency, None, resolver)
    container = dependency.load()
    container.build_dependency_graph(graph)
    actual = {path[2:] if path.startswith('./') else path for path in graph.paths}
    assert actual == set(paths)
