from typing import BinaryIO
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo, Language, ObjectType

from databricks.labs.ucx.source_code.dependencies import (
    SourceContainer,
    DependencyResolver,
)
from databricks.labs.ucx.source_code.notebook_migrator import NotebookMigrator
from databricks.labs.ucx.source_code.site_packages import SitePackages
from databricks.labs.ucx.source_code.whitelist import Whitelist
from tests.unit import _load_sources, _download_side_effect, locate_site_packages, site_packages_mock, whitelist_mock


def test_build_dependency_graph_visits_notebook_notebook_dependencies(empty_index):
    paths = ["root3.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path, language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    sps = site_packages_mock()
    whi = whitelist_mock()
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(whi, sps, ws))
    object_info = ObjectInfo(path="root3.run.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_build_dependency_graph_visits_notebook_file_dependencies(empty_index):
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
    sps = site_packages_mock()
    whi = whitelist_mock()
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(whi, sps, ws))
    object_info = ObjectInfo(path="root8.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_build_dependency_graph_fails_with_unfound_dependency(empty_index):
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
    sps = site_packages_mock()
    whi = whitelist_mock()
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(whi, sps, ws))
    object_info = ObjectInfo(path="root1.run.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    with pytest.raises(ValueError):
        migrator.build_dependency_graph(object_info)


def test_build_dependency_graph_visits_file_dependencies(empty_index):
    paths = ["root5.py.txt", "leaf4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path, object_type=ObjectType.FILE)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    sps = site_packages_mock()
    whi = whitelist_mock()
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(whi, sps, ws))
    object_info = ObjectInfo(path="root5.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_build_dependency_graph_visits_recursive_file_dependencies(empty_index):
    paths = ["root6.py.txt", "root5.py.txt", "leaf4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path, object_type=ObjectType.FILE)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    sps = site_packages_mock()
    whi = whitelist_mock()
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(whi, sps, ws))
    object_info = ObjectInfo(path="root6.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_build_dependency_graph_safely_visits_non_file_dependencies(empty_index):
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
    sps = site_packages_mock()
    whi = whitelist_mock()
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(whi, sps, ws))
    object_info = ObjectInfo(path="root7.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info)
    assert len(visited) == len(paths)


def test_build_dependency_graph_throws_with_invalid_dependencies(empty_index):
    paths = ["root7.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path) if path == "some_library" else ObjectInfo(path=path, object_type=ObjectType.FILE)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, visited, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    sps = site_packages_mock()
    whi = whitelist_mock()
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(whi, sps, ws))
    object_info = ObjectInfo(path="root7.py.txt", language=Language.PYTHON, object_type=ObjectType.FILE)
    with pytest.raises(ValueError):
        migrator.build_dependency_graph(object_info)


def test_build_dependency_graph_ignores_builtin_dependencies(empty_index):
    paths = ["builtins.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.return_value = ObjectInfo(path="builtins.py.txt", object_type=ObjectType.FILE)
    sps = site_packages_mock()
    whi = Whitelist()
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(whi, sps, ws))
    object_info = ObjectInfo(path="builtins.py.txt", language=Language.PYTHON, object_type=ObjectType.FILE)
    graph = migrator.build_dependency_graph(object_info)
    assert not graph.locate_dependency_with_path("os")
    assert not graph.locate_dependency_with_path("path")


def test_build_dependency_graph_ignores_known_dependencies(empty_index):
    paths = ["builtins.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    datas = _load_sources(SourceContainer, "sample-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.return_value = ObjectInfo(path="builtins.py.txt", object_type=ObjectType.FILE)
    sps = site_packages_mock()
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(whitelist, sps, ws))
    object_info = ObjectInfo(path="builtins.py.txt", language=Language.PYTHON, object_type=ObjectType.FILE)
    graph = migrator.build_dependency_graph(object_info)
    assert not graph.locate_dependency_with_path("databricks")


def test_build_dependency_graph_visits_site_packages(empty_index):
    paths = ["import-site-package.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    datas = _load_sources(SourceContainer, "sample-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)

    def get_status_side_effect(*args):
        path = args[0]
        return ObjectInfo(path=path, object_type=ObjectType.FILE) if path == "import-site-package.py.txt" else None

    ws.workspace.get_status.side_effect = get_status_side_effect
    site_packages_path = locate_site_packages()
    site_packages = SitePackages.parse(str(site_packages_path))
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(whitelist, site_packages, ws))
    object_info = ObjectInfo(path="import-site-package.py.txt", language=Language.PYTHON, object_type=ObjectType.FILE)
    graph = migrator.build_dependency_graph(object_info)
    assert graph.locate_dependency_with_path("certifi/core.py")
