from typing import BinaryIO
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo, Language, ObjectType

from databricks.labs.ucx.source_code.dependencies import DependencyLoader
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook import Notebook
from databricks.labs.ucx.source_code.notebook_migrator import NotebookMigrator
from tests.unit import _load_sources


def test_build_dependency_graph_visits_notebook_notebook_dependencies(empty_index):
    paths = ["root3.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(Notebook, *paths)))
    visited: dict[str, bool] = {}

    # can't remove **kwargs because it receives format=xxx
    # pylint: disable=unused-argument
    def download_side_effect(*args, **kwargs):
        filename = args[0]
        if filename.startswith('./'):
            filename = filename[2:]
        visited[filename] = True
        result = create_autospec(BinaryIO)
        result.__enter__.return_value.read.return_value = sources[filename].encode("utf-8")
        return result

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path, language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = download_side_effect
    ws.workspace.get_status.side_effect = get_status_side_effect
    migrator = NotebookMigrator(ws, Languages(empty_index), DependencyLoader(ws))
    object_info = ObjectInfo(path="root3.run.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_build_dependency_graph_visits_notebook_file_dependencies(empty_index):
    paths = ["root8.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(Notebook, *paths)))
    visited: dict[str, bool] = {}

    # can't remove **kwargs because it receives format=xxx
    # pylint: disable=unused-argument
    def download_side_effect(*args, **kwargs):
        filename = args[0]
        if filename.startswith('./'):
            filename = filename[2:]
        visited[filename] = True
        if filename.find(".py") < 0:
            filename = filename + ".py"
        if filename.find(".txt") < 0:
            filename = filename + ".txt"
        result = create_autospec(BinaryIO)
        result.__enter__.return_value.read.return_value = sources[filename].encode("utf-8")
        return result

    def get_status_side_effect(*args):
        path = args[0]
        return (
            ObjectInfo(path=path, object_type=ObjectType.FILE)
            if path.startswith("leaf")
            else ObjectInfo(path=path, language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
        )

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = download_side_effect
    ws.workspace.get_status.side_effect = get_status_side_effect
    migrator = NotebookMigrator(ws, Languages(empty_index), DependencyLoader(ws))
    object_info = ObjectInfo(path="root8.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_build_dependency_graph_fails_with_unfound_dependency(empty_index):
    paths = ["root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(Notebook, *paths)))

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
    ws.workspace.list.return_value = []
    migrator = NotebookMigrator(ws, Languages(empty_index), DependencyLoader(ws))
    object_info = ObjectInfo(path="root1.run.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    with pytest.raises(ValueError):
        migrator.build_dependency_graph(object_info)


def test_build_dependency_graph_visits_file_dependencies(empty_index):
    paths = ["root5.py.txt", "leaf4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(Notebook, *paths)))
    visited: dict[str, bool] = {}

    # can't remove **kwargs because it receives format=xxx
    # pylint: disable=unused-argument
    def download_side_effect(*args, **kwargs):
        filename = args[0]
        if filename.startswith('./'):
            filename = filename[2:]
        visited[filename] = True
        if filename.find(".py") < 0:
            filename = filename + ".py"
        if filename.find(".txt") < 0:
            filename = filename + ".txt"
        result = create_autospec(BinaryIO)
        result.__enter__.return_value.read.return_value = sources[filename].encode("utf-8")
        return result

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path, object_type=ObjectType.FILE)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = download_side_effect
    ws.workspace.get_status.side_effect = get_status_side_effect
    migrator = NotebookMigrator(ws, Languages(empty_index), DependencyLoader(ws))
    object_info = ObjectInfo(path="root5.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_build_dependency_graph_visits_recursive_file_dependencies(empty_index):
    paths = ["root6.py.txt", "root5.py.txt", "leaf4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(Notebook, *paths)))
    visited: dict[str, bool] = {}

    # can't remove **kwargs because it receives format=xxx
    # pylint: disable=unused-argument
    def download_side_effect(*args, **kwargs):
        filename = args[0]
        if filename.startswith('./'):
            filename = filename[2:]
        visited[filename] = True
        if filename.find(".py") < 0:
            filename = filename + ".py"
        if filename.find(".txt") < 0:
            filename = filename + ".txt"
        result = create_autospec(BinaryIO)
        result.__enter__.return_value.read.return_value = sources[filename].encode("utf-8")
        return result

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path, object_type=ObjectType.FILE)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = download_side_effect
    ws.workspace.get_status.side_effect = get_status_side_effect
    migrator = NotebookMigrator(ws, Languages(empty_index), DependencyLoader(ws))
    object_info = ObjectInfo(path="root6.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_build_dependency_graph_safely_visits_non_file_dependencies(empty_index):
    paths = ["root7.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(Notebook, *paths)))
    visited: dict[str, bool] = {}

    # can't remove **kwargs because it receives format=xxx
    # pylint: disable=unused-argument
    def download_side_effect(*args, **kwargs):
        filename = args[0]
        if filename.startswith('./'):
            filename = filename[2:]
        visited[filename] = True
        if filename.find(".py") < 0:
            filename = filename + ".py"
        if filename.find(".txt") < 0:
            filename = filename + ".txt"
        result = create_autospec(BinaryIO)
        result.__enter__.return_value.read.return_value = sources[filename].encode("utf-8")
        return result

    def get_status_side_effect(*args):
        path = args[0]
        return (
            ObjectInfo(path=path, object_type=ObjectType.LIBRARY)
            if path == "some_library"
            else ObjectInfo(path=path, object_type=ObjectType.FILE)
        )

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = download_side_effect
    ws.workspace.get_status.side_effect = get_status_side_effect
    migrator = NotebookMigrator(ws, Languages(empty_index), DependencyLoader(ws))
    object_info = ObjectInfo(path="root7.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_build_dependency_graph_throws_with_invalid_dependencies(empty_index):
    paths = ["root7.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(Notebook, *paths)))
    visited: dict[str, bool] = {}

    # can't remove **kwargs because it receives format=xxx
    # pylint: disable=unused-argument
    def download_side_effect(*args, **kwargs):
        filename = args[0]
        if filename.startswith('./'):
            filename = filename[2:]
        visited[filename] = True
        if filename.find(".py") < 0:
            filename = filename + ".py"
        if filename.find(".txt") < 0:
            filename = filename + ".txt"
        result = create_autospec(BinaryIO)
        result.__enter__.return_value.read.return_value = sources[filename].encode("utf-8")
        return result

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path) if path == "some_library" else ObjectInfo(path=path, object_type=ObjectType.FILE)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = download_side_effect
    ws.workspace.get_status.side_effect = get_status_side_effect
    migrator = NotebookMigrator(ws, Languages(empty_index), DependencyLoader(ws))
    object_info = ObjectInfo(path="root7.py.txt", language=Language.PYTHON, object_type=ObjectType.FILE)
    with pytest.raises(ValueError):
        migrator.build_dependency_graph(object_info)
