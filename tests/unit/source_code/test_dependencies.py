from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo, Language, ObjectType

from databricks.labs.ucx.source_code.base import Advice, Failure
from databricks.labs.ucx.source_code.dependencies import (
    SourceContainer,
    DependencyResolver,
    DependencyType,
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
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(ws, whi, sps))
    object_info = ObjectInfo(path="root3.run.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    migrator.build_dependency_graph(object_info, lambda advice: None)
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
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(ws, whi, sps))
    object_info = ObjectInfo(path="root8.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    migrator.build_dependency_graph(object_info, lambda advice: None)
    assert len(visited) == len(paths)


def test_build_dependency_graph_raises_advice_with_unfound_root(empty_index):
    sources: dict[str, str] = {}
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.return_value = None
    sps = site_packages_mock()
    whi = whitelist_mock()
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(ws, whi, sps))
    object_info = ObjectInfo(path="root1.run.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    advices: list[Advice] = []
    migrator.build_dependency_graph(object_info, advices.append)
    assert advices == [
        # pylint: disable=duplicate-code
        Failure(
            'dependency-check',
            'Object not found: root1.run.py.txt',
            Advice.MISSING_SOURCE_TYPE,
            Advice.MISSING_SOURCE_PATH,
            -1,
            -1,
            -1,
            -1,
        )
    ]


def test_build_dependency_graph_raises_advice_with_unfound_dependency(empty_index):
    paths = ["root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))

    def get_status_side_effect(*args):
        path = args[0]
        if "leaf2" in path:
            return None
        if "leaf1" in path:
            return ObjectInfo(path=path, object_type=ObjectType.FILE)
        return ObjectInfo(path=path, language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    sps = site_packages_mock()
    whi = whitelist_mock()
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(ws, whi, sps))
    object_info = ObjectInfo(path="root1.run.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    advices: list[Advice] = []
    migrator.build_dependency_graph(object_info, advices.append)
    assert advices == [
        # pylint: disable=duplicate-code
        Failure(
            'dependency-check',
            'Object not found: ./leaf2.py.txt',
            DependencyType.WORKSPACE_NOTEBOOK.value,
            object_info.path,
            1,
            0,
            1,
            21,
        )
    ]


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
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(ws, whi, sps))
    object_info = ObjectInfo(path="root5.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info, lambda advice: None)
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
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(ws, whi, sps))
    object_info = ObjectInfo(path="root6.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info, lambda advice: None)
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
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(ws, whi, sps))
    object_info = ObjectInfo(path="root7.py.txt", object_type=ObjectType.FILE)
    migrator.build_dependency_graph(object_info, lambda advice: None)
    assert len(visited) == len(paths)


def test_build_dependency_graph_creates_advice_with_invalid_dependencies(empty_index):
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
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(ws, whi, sps))
    object_info = ObjectInfo(path="root7.py.txt", language=Language.PYTHON, object_type=ObjectType.FILE)
    advices: list[Advice] = []
    migrator.build_dependency_graph(object_info, advices.append)
    # ignore message details
    advice = advices[0].replace(message='failure')
    assert advice == Failure('dependency-check', 'failure', 'WORKSPACE_FILE', 'root7.py.txt', 1, 0, 1, 19)


def test_build_dependency_graph_ignores_builtin_dependencies(empty_index):
    paths = ["builtins.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.return_value = ObjectInfo(path="builtins.py.txt", object_type=ObjectType.FILE)
    sps = site_packages_mock()
    whi = Whitelist()
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(ws, whi, sps))
    object_info = ObjectInfo(path="builtins.py.txt", language=Language.PYTHON, object_type=ObjectType.FILE)
    graph = migrator.build_dependency_graph(object_info, lambda advice: None)
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
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(ws, whitelist, sps))
    object_info = ObjectInfo(path="builtins.py.txt", language=Language.PYTHON, object_type=ObjectType.FILE)
    graph = migrator.build_dependency_graph(object_info, lambda advice: None)
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
    migrator = NotebookMigrator(ws, empty_index, DependencyResolver(ws, whitelist, site_packages))
    object_info = ObjectInfo(path="import-site-package.py.txt", language=Language.PYTHON, object_type=ObjectType.FILE)
    graph = migrator.build_dependency_graph(object_info, lambda advice: None)
    assert graph.locate_dependency_with_path("certifi/core.py")
