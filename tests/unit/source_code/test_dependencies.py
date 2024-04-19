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
)
from databricks.labs.ucx.source_code.files import LocalFileMigrator
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook_migrator import NotebookMigrator
from databricks.labs.ucx.source_code.whitelist import Whitelist
from tests.unit import _load_sources, _download_side_effect, whitelist_mock, _load_dependency_side_effect


def test_notebook_build_dependency_graph_visits_notebook_notebook_dependencies():
    paths = ["root3.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path, language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    languages = create_autospec(Languages)
    whi = whitelist_mock()
    migrator = NotebookMigrator(ws, languages, DependencyResolver(whi, LocalLoader(), ws))
    object_info = ObjectInfo(path="root3.run.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_notebook_build_dependency_graph_visits_notebook_file_dependencies():
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
    languages = create_autospec(Languages)
    whi = whitelist_mock()
    loader = create_autospec(LocalLoader)
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    migrator = NotebookMigrator(ws, languages, DependencyResolver(whi, loader, ws))
    object_info = ObjectInfo(path="root8.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_notebook_build_dependency_graph_fails_with_unfound_dependency():
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

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = download_side_effect
    ws.workspace.get_status.return_value = None
    languages = create_autospec(Languages)
    whi = whitelist_mock()
    migrator = NotebookMigrator(ws, languages, DependencyResolver(whi, LocalLoader(), ws))
    object_info = ObjectInfo(path="root1.run.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    with pytest.raises(ValueError):
        migrator.build_dependency_graph(object_info)


def test_notebook_build_dependency_graph_visits_file_dependencies():
    paths = ["root5.py.txt", "leaf4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path, object_type=ObjectType.FILE)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    languages = create_autospec(Languages)
    whi = whitelist_mock()
    loader = create_autospec(LocalLoader)
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    migrator = NotebookMigrator(ws, languages, DependencyResolver(whi, loader, ws))
    object_info = ObjectInfo(path="root5.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_file_build_dependency_graph_visits_file_dependencies():
    paths = ["root5.py.txt", "leaf4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    languages = create_autospec(Languages)
    whi = whitelist_mock()
    loader = create_autospec(LocalLoader)
    loader.is_file.return_value = True
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    migrator = LocalFileMigrator(languages, DependencyResolver(whi, loader, None))
    migrator.build_dependency_graph(Path("root5.py.txt"))
    assert len(visited) == len(paths)


def test_notebook_build_dependency_graph_visits_recursive_file_dependencies():
    paths = ["root6.py.txt", "root5.py.txt", "leaf4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path, object_type=ObjectType.FILE)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    languages = create_autospec(Languages)
    whi = whitelist_mock()
    loader = create_autospec(LocalLoader)
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    migrator = NotebookMigrator(ws, languages, DependencyResolver(whi, loader, ws))
    object_info = ObjectInfo(path="root6.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_file_build_dependency_graph_visits_recursive_file_dependencies():
    paths = ["root6.py.txt", "root5.py.txt", "leaf4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    languages = create_autospec(Languages)
    whi = whitelist_mock()
    loader = create_autospec(LocalLoader)
    loader.is_file.return_value = True
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    migrator = LocalFileMigrator(languages, DependencyResolver(whi, loader, None))
    migrator.build_dependency_graph(Path("root6.py.txt"))
    assert len(visited) == len(paths)


def test_notebook_build_dependency_graph_safely_visits_non_file_dependencies():
    paths = ["root7.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}

    def get_status_side_effect(*args):
        path = args[0]
        return (
            ObjectInfo(path=path, object_type=ObjectType.LIBRARY)
            if path == "some_library"
            else ObjectInfo(path=path, object_type=ObjectType.FILE)
        )

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    languages = create_autospec(Languages)
    whi = whitelist_mock()

    def is_file_side_effect(*args):
        path = args[0]
        return path in paths

    loader = create_autospec(LocalLoader)
    loader.is_file.side_effect = is_file_side_effect
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    migrator = NotebookMigrator(ws, languages, DependencyResolver(whi, loader, ws))
    object_info = ObjectInfo(path="root7.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_notebook_build_dependency_graph_throws_with_invalid_dependencies():
    paths = ["root7.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path) if path == "some_library" else ObjectInfo(path=path, object_type=ObjectType.FILE)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    languages = create_autospec(Languages)
    whi = whitelist_mock()

    def is_file_side_effect(*args):
        path = args[0]
        return path in paths

    loader = create_autospec(LocalLoader)
    loader.is_file.side_effect = is_file_side_effect
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    migrator = NotebookMigrator(ws, languages, DependencyResolver(whi, loader, ws))
    object_info = ObjectInfo(path="root7.py.txt", language=Language.PYTHON, object_type=ObjectType.FILE)
    with pytest.raises(ValueError):
        migrator.build_dependency_graph(object_info)


def test_file_build_dependency_graph_throws_with_invalid_dependencies():
    paths = ["root7.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    languages = create_autospec(Languages)
    whi = whitelist_mock()

    def is_file_side_effect(*args):
        filename = args[0]
        return filename in paths

    loader = create_autospec(LocalLoader)
    loader.is_file.side_effect = is_file_side_effect
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, visited, *args)
    migrator = LocalFileMigrator(languages, DependencyResolver(whi, loader, None))
    with pytest.raises(ValueError):
        migrator.build_dependency_graph(Path("root7.py.txt"))


def test_notebook_build_dependency_graph_ignores_builtin_dependencies():
    paths = ["python_builtins.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.return_value = ObjectInfo(path="python_builtins.py.txt", object_type=ObjectType.FILE)
    languages = create_autospec(Languages)
    whi = Whitelist()
    loader = create_autospec(LocalLoader)
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, {}, *args)
    migrator = NotebookMigrator(ws, languages, DependencyResolver(whi, loader, ws))
    object_info = ObjectInfo(path="python_builtins.py.txt", language=Language.PYTHON, object_type=ObjectType.FILE)
    graph = migrator.build_dependency_graph(object_info)
    assert not graph.locate_dependency_with_path("os")
    assert not graph.locate_dependency_with_path("path")


def test_file_build_dependency_graph_ignores_builtin_dependencies():
    paths = ["python_builtins.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    languages = create_autospec(Languages)
    whi = Whitelist()

    def is_file_side_effect(*args):
        filename = args[0]
        return filename in paths

    loader = create_autospec(LocalLoader)
    loader.is_file.side_effect = is_file_side_effect
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, {}, *args)
    migrator = LocalFileMigrator(languages, DependencyResolver(whi, loader, None))
    graph = migrator.build_dependency_graph(Path("python_builtins.py.txt"))
    assert not graph.locate_dependency_with_path("os")
    assert not graph.locate_dependency_with_path("path")


def test_notebook_build_dependency_graph_ignores_known_dependencies():
    paths = ["python_builtins.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    datas = _load_sources(SourceContainer, "sample-python-compatibility-catalog.yml")
    languages = create_autospec(Languages)
    whitelist = Whitelist.parse(datas[0])
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.return_value = ObjectInfo(path="python_builtins.py.txt", object_type=ObjectType.FILE)
    loader = create_autospec(LocalLoader)
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, {}, *args)
    migrator = NotebookMigrator(ws, languages, DependencyResolver(whitelist, loader, ws))
    object_info = ObjectInfo(path="python_builtins.py.txt", language=Language.PYTHON, object_type=ObjectType.FILE)
    graph = migrator.build_dependency_graph(object_info)
    assert not graph.locate_dependency_with_path("databricks")


def test_file_build_dependency_graph_ignores_known_dependencies():
    paths = ["python_builtins.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    datas = _load_sources(SourceContainer, "sample-python-compatibility-catalog.yml")
    languages = create_autospec(Languages)
    whitelist = Whitelist.parse(datas[0])

    def is_file_side_effect(*args):
        filename = args[0]
        return filename in paths

    loader = create_autospec(LocalLoader)
    loader.is_file.side_effect = is_file_side_effect
    loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(sources, {}, *args)
    migrator = LocalFileMigrator(languages, DependencyResolver(whitelist, loader, None))
    graph = migrator.build_dependency_graph(Path("python_builtins.py.txt"))
    assert not graph.locate_dependency_with_path("databricks")
