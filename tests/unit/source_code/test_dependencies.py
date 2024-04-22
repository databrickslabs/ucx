from pathlib import Path
from typing import BinaryIO
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo, Language, ObjectType

from databricks.labs.ucx.source_code.dependencies import (
    SourceContainer,
    DependencyResolver,
    LocalLoader,
    DependencyGraphBuilder,
    WorkspaceLoader,
)
from databricks.labs.ucx.source_code.whitelist import Whitelist
from tests.unit import _load_sources, _download_side_effect, whitelist_mock, _load_dependency_side_effect


def test_dependency_graph_builder_visits_workspace_notebook_dependencies():
    paths = ["root3.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path, language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    whi = whitelist_mock()
    loader = create_autospec(LocalLoader)
    loader.is_notebook.return_value = False
    builder = DependencyGraphBuilder(DependencyResolver(whi, loader, WorkspaceLoader(ws)))
    builder.build_notebook_dependency_graph(Path("root3.run.py.txt"))
    assert len(visited) == len(paths)


def test_dependency_graph_builder_visits_local_notebook_dependencies():
    paths = ["root4.py.txt", "leaf3.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    whi = whitelist_mock()
    loader = create_autospec(LocalLoader)
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    loader.is_notebook.return_value = True
    loader.is_file.return_value = True
    builder = DependencyGraphBuilder(DependencyResolver(whi, loader, None))
    builder.build_notebook_dependency_graph(Path("root4.py.txt"))
    assert len(visited) == len(paths)


def test_dependency_graph_builder_visits_workspace_file_dependencies():
    paths = ["root8.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}

    def get_status_side_effect(*args):
        path = args[0]
        return (
            ObjectInfo(path=path, object_type=ObjectType.FILE)
            if path.startswith("leaf")
            else ObjectInfo(path=path, language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
        )

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    whi = whitelist_mock()
    loader = create_autospec(LocalLoader)
    loader.is_notebook.return_value = False
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    builder = DependencyGraphBuilder(DependencyResolver(whi, loader, ws))
    builder.build_local_file_dependency_graph(Path("root8.py.txt"))
    assert len(visited) == len(paths)


def test_dependency_graph_builder_fails_with_unfound_dependency():
    paths = ["root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))

    # can't remove **kwargs because it receives format=xxx
    # pylint: disable=unused-argument
    def download_side_effect(*args, **kwargs):
        filename = args[0]
        if filename.startswith('./'):
            filename = filename[2:]
        result = create_autospec(BinaryIO)
        result.__enter__.return_value.read.return_value = sources[filename].encode("utf-8")
        return result

    def get_status_side_effect(*args):
        path = args[0]
        if "leaf2" in path:
            return None
        return ObjectInfo(object_type=ObjectType.NOTEBOOK, language=Language.PYTHON, path=path)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.is_notebook.return_value = True
    ws.workspace.download.side_effect = download_side_effect
    ws.workspace.get_status.side_effect = get_status_side_effect
    whi = whitelist_mock()
    loader = create_autospec(LocalLoader)
    loader.is_notebook.return_value = False
    builder = DependencyGraphBuilder(DependencyResolver(whi, loader, WorkspaceLoader(ws)))
    with pytest.raises(ValueError):
        builder.build_notebook_dependency_graph(Path("root1.run.py.txt"))


def test_dependency_graph_builder_visits_file_dependencies():
    paths = ["root5.py.txt", "leaf4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    whi = whitelist_mock()
    loader = create_autospec(LocalLoader)
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    builder = DependencyGraphBuilder(DependencyResolver(whi, loader, None))
    builder.build_local_file_dependency_graph(Path("root5.py.txt"))
    assert len(visited) == len(paths)


def test_dependency_graph_builder_visits_recursive_file_dependencies():
    paths = ["root6.py.txt", "root5.py.txt", "leaf4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    whi = whitelist_mock()
    loader = create_autospec(LocalLoader)
    loader.is_file.return_value = True
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    builder = DependencyGraphBuilder(DependencyResolver(whi, loader, None))
    builder.build_local_file_dependency_graph(Path("root6.py.txt"))
    assert len(visited) == len(paths)


def test_dependency_graph_builder_throws_with_invalid_dependencies():
    paths = ["root7.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    whi = whitelist_mock()

    def is_file_side_effect(*args):
        filename = args[0]
        return filename in paths

    loader = create_autospec(LocalLoader)
    loader.is_file.side_effect = is_file_side_effect
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    builder = DependencyGraphBuilder(DependencyResolver(whi, loader, None))
    with pytest.raises(ValueError):
        builder.build_local_file_dependency_graph(Path("root7.py.txt"))


def test_file_build_dependency_graph_ignores_builtin_dependencies():
    paths = ["python_builtins.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    whi = Whitelist()

    def is_file_side_effect(*args):
        filename = args[0]
        return filename in paths

    loader = create_autospec(LocalLoader)
    loader.is_file.side_effect = is_file_side_effect
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, {}, *args)
    builder = DependencyGraphBuilder(DependencyResolver(whi, loader, None))
    graph = builder.build_local_file_dependency_graph(Path("python_builtins.py.txt"))
    assert not graph.locate_dependency_with_path("os")
    assert not graph.locate_dependency_with_path("path")


def test_dependency_graph_builder_ignores_known_dependencies():
    paths = ["python_builtins.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    datas = _load_sources(SourceContainer, "sample-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])

    def is_file_side_effect(*args):
        filename = args[0]
        return filename in paths

    loader = create_autospec(LocalLoader)
    loader.is_file.side_effect = is_file_side_effect
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, {}, *args)
    builder = DependencyGraphBuilder(DependencyResolver(whitelist, loader, None))
    graph = builder.build_local_file_dependency_graph(Path("python_builtins.py.txt"))
    assert not graph.locate_dependency_with_path("databricks")
