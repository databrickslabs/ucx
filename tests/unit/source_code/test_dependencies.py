from pathlib import Path
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo, Language, ObjectType

from databricks.labs.ucx.source_code.graph import (
    SourceContainer,
    DependencyResolver,
    DependencyProblem,
    DependencyGraphBuilder,
)
from databricks.labs.ucx.source_code.notebooks.loaders import (
    NotebookResolver,
    WorkspaceNotebookLoader,
    LocalNotebookLoader,
)
from databricks.labs.ucx.source_code.files import FileLoader, LocalFileResolver
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.whitelist import WhitelistResolver, Whitelist
from databricks.labs.ucx.source_code.site_packages import SitePackagesResolver, SitePackages
from tests.unit import (
    _load_sources,
    _download_side_effect,
    whitelist_mock,
    _load_dependency_side_effect,
    locate_site_packages,
    TestFileLoader,
    _samples_path,
    _local_loader_with_side_effects,
)


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
    workspace_notebook_loader = WorkspaceNotebookLoader(ws)
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(workspace_notebook_loader),
        ]
    )
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_notebook_dependency_graph(Path("root3.run.py.txt"))
    assert len(visited) == len(paths)


def test_dependency_graph_builder_visits_local_notebook_dependencies():
    paths = ["root4.py.txt", "leaf3.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    whi = whitelist_mock()
    file_loader = create_autospec(FileLoader)
    file_loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(
        sources, visited, *args
    )
    file_loader.is_notebook.return_value = True
    file_loader.exists.return_value = True
    notebook_loader = create_autospec(LocalNotebookLoader)
    notebook_loader.load_dependency.side_effect = lambda *args, **kwargs: _load_dependency_side_effect(
        sources, visited, *args
    )
    notebook_loader.is_notebook.return_value = True
    notebook_loader.exists.return_value = True
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_notebook_dependency_graph(Path("root4.py.txt"))
    assert len(visited) == len(paths)


def test_dependency_graph_builder_visits_workspace_file_dependencies():
    paths = ["root8.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    file_loader = _local_loader_with_side_effects(FileLoader, sources, visited)
    whi = whitelist_mock()
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    notebook_loader = LocalNotebookLoader(provider)
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_local_file_dependency_graph(Path(paths[0]))
    assert len(visited) == len(paths)


def test_dependency_graph_builder_raises_problem_with_unfound_workspace_notebook_dependency():
    paths = ["root1.run.py.txt", "leaf1.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))

    def get_status_side_effect(*args):
        path = args[0]
        if "leaf2" in path:
            return None
        return ObjectInfo(object_type=ObjectType.NOTEBOOK, language=Language.PYTHON, path=path)

    ws = create_autospec(WorkspaceClient)
    ws.workspace.is_notebook.return_value = True
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.side_effect = get_status_side_effect
    whi = whitelist_mock()
    file_loader = create_autospec(FileLoader)
    file_loader.is_notebook.return_value = False
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    notebook_loader = WorkspaceNotebookLoader(ws)
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_notebook_dependency_graph(Path("root1.run.py.txt"))
    assert list(builder.problems) == [
        DependencyProblem(
            'notebook-not-found', 'Notebook not found: leaf2.py.txt', Path("root1.run.py.txt"), 19, 0, 19, 21
        )
    ]


def test_dependency_graph_builder_raises_problem_with_unfound_local_notebook_dependency():
    paths = ["root4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    whi = whitelist_mock()

    def is_file_side_effect(*args):
        path = args[0]
        return path.as_posix() in paths

    file_loader = create_autospec(FileLoader)
    file_loader.exists.side_effect = is_file_side_effect
    file_loader.is_notebook.side_effect = is_file_side_effect
    file_loader.load_dependency.side_effect = lambda *args: _load_dependency_side_effect(sources, {}, *args)
    notebook_loader = create_autospec(LocalNotebookLoader)
    notebook_loader.exists.side_effect = is_file_side_effect
    notebook_loader.is_notebook.side_effect = is_file_side_effect
    notebook_loader.load_dependency.side_effect = lambda *args: _load_dependency_side_effect(sources, {}, *args)
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_notebook_dependency_graph(Path(paths[0]))
    assert list(builder.problems) == [
        DependencyProblem('notebook-not-found', 'Notebook not found: leaf3.py.txt', Path(paths[0]), 1, 0, 1, 38)
    ]


def test_dependency_graph_builder_raises_problem_with_non_constant_local_notebook_dependency():
    paths = ["root10.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    whi = whitelist_mock()

    def is_file_side_effect(*args):
        path = args[0]
        return path.as_posix() in paths

    file_loader = create_autospec(FileLoader)
    file_loader.exists.side_effect = is_file_side_effect
    file_loader.is_notebook.side_effect = is_file_side_effect
    file_loader.load_dependency.side_effect = lambda *args: _load_dependency_side_effect(sources, {}, *args)
    notebook_loader = create_autospec(LocalNotebookLoader)
    notebook_loader.exists.side_effect = is_file_side_effect
    notebook_loader.is_notebook.side_effect = is_file_side_effect
    notebook_loader.load_dependency.side_effect = lambda *args: _load_dependency_side_effect(sources, {}, *args)
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_notebook_dependency_graph(Path(paths[0]))
    assert list(builder.problems) == [
        DependencyProblem(
            'dependency-not-constant', "Can't check dependency not provided as a constant", Path(paths[0]), 2, 0, 2, 35
        )
    ]


def test_dependency_graph_builder_raises_problem_with_invalid_run_cell():
    paths = ["leaf6.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.return_value = ObjectInfo(
        object_type=ObjectType.NOTEBOOK, language=Language.PYTHON, path=paths[0]
    )
    whi = whitelist_mock()
    file_loader = create_autospec(FileLoader)
    file_loader.is_notebook.return_value = False
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    notebook_loader = WorkspaceNotebookLoader(ws)
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_notebook_dependency_graph(Path(paths[0]))
    assert list(builder.problems) == [
        DependencyProblem('invalid-run-cell', 'Missing notebook path in %run command', Path("leaf6.py.txt"), 5, 0, 5, 4)
    ]


def test_dependency_graph_builder_visits_recursive_file_dependencies():
    paths = ["root6.py.txt", "root5.py.txt", "leaf4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    whi = whitelist_mock()
    file_loader = _local_loader_with_side_effects(FileLoader, sources, visited)
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    notebook_loader = LocalNotebookLoader(provider)
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_local_file_dependency_graph(Path("root6.py.txt"))
    assert len(visited) == len(paths)


def test_dependency_graph_builder_raises_problem_with_unresolved_import():
    paths = ["root7.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    whi = whitelist_mock()
    file_loader = _local_loader_with_side_effects(FileLoader, sources, visited)
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    notebook_loader = LocalNotebookLoader(provider)
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_local_file_dependency_graph(Path(paths[0]))
    assert list(builder.problems) == [
        DependencyProblem(
            'import-not-found', 'Could not locate import: some_library', Path("root7.py.txt"), 1, 0, 1, 19
        )
    ]


def test_dependency_graph_builder_raises_problem_with_non_constant_notebook_argument():
    paths = ["run_notebooks.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    whi = Whitelist()
    file_loader = _local_loader_with_side_effects(FileLoader, sources, visited)
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    notebook_loader = LocalNotebookLoader(provider)
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_local_file_dependency_graph(Path(paths[0]))
    assert list(builder.problems) == [
        DependencyProblem(
            'dependency-not-constant',
            "Can't check dependency not provided as a constant",
            Path(paths[0]),
            14,
            13,
            14,
            50,
        )
    ]


def test_dependency_graph_builder_visits_file_dependencies():
    paths = ["root5.py.txt", "leaf4.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    visited: dict[str, bool] = {}
    whi = whitelist_mock()
    file_loader = _local_loader_with_side_effects(FileLoader, sources, visited)
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    notebook_loader = LocalNotebookLoader(provider)
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_local_file_dependency_graph(Path("root5.py.txt"))
    assert len(visited) == len(paths)


def test_dependency_graph_builder_skips_builtin_dependencies():
    paths = ["python_builtins.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    whi = Whitelist()
    file_loader = _local_loader_with_side_effects(FileLoader, sources, {})
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    notebook_loader = LocalNotebookLoader(provider)
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    graph = builder.build_local_file_dependency_graph(Path("python_builtins.py.txt"))
    child = graph.locate_dependency(Path("os"))
    assert child
    assert len(child.local_dependencies) == 0
    child = graph.locate_dependency(Path("pathlib"))
    assert child
    assert len(child.local_dependencies) == 0


def test_dependency_graph_builder_ignores_known_dependencies():
    paths = ["python_builtins.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    datas = _load_sources(SourceContainer, "sample-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    file_loader = _local_loader_with_side_effects(FileLoader, sources, {})
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    notebook_loader = LocalNotebookLoader(provider)
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whitelist),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    graph = builder.build_local_file_dependency_graph(Path("python_builtins.py.txt"))
    assert not graph.locate_dependency(Path("databricks"))


def test_dependency_graph_builder_visits_site_packages(empty_index):
    datas = _load_sources(SourceContainer, "sample-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    paths = ["import-site-package.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    provider = PathLookup.from_pathlike_string(Path.cwd(), _samples_path(SourceContainer))
    file_loader = TestFileLoader(provider, sources)
    site_packages_path = locate_site_packages()
    site_packages = SitePackages.parse(site_packages_path)
    notebook_loader = LocalNotebookLoader(provider)
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whitelist),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    graph = builder.build_local_file_dependency_graph(Path("import-site-package.py.txt"))
    assert graph.locate_dependency(Path(site_packages_path, "certifi/core.py"))
    assert not graph.locate_dependency(Path("core.py"))
    assert not graph.locate_dependency(Path("core"))


def test_dependency_graph_builder_raises_problem_with_unfound_root_file(empty_index):
    site_packages = SitePackages.parse(locate_site_packages())
    whi = whitelist_mock()
    file_loader = create_autospec(FileLoader)
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    notebook_loader = LocalNotebookLoader(provider)
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_local_file_dependency_graph(Path("root1.run.py.txt"))
    assert list(builder.problems) == [DependencyProblem('file-not-found', 'File not found: root1.run.py.txt')]
    file_loader.load_dependency.assert_not_called()


def test_dependency_graph_builder_raises_problem_with_unfound_root_notebook(empty_index):
    sources: dict[str, str] = {}
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = lambda *args, **kwargs: _download_side_effect(sources, {}, *args, **kwargs)
    ws.workspace.get_status.return_value = None
    site_packages = SitePackages.parse(locate_site_packages())
    whi = whitelist_mock()
    file_loader = create_autospec(FileLoader)
    notebook_loader = create_autospec(LocalNotebookLoader)
    notebook_loader.is_notebook.return_value = False
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    builder.build_notebook_dependency_graph(Path("root2.run.py.txt"))
    assert list(builder.problems) == [DependencyProblem('notebook-not-found', 'Notebook not found: root2.run.py.txt')]
    file_loader.load_dependency.assert_not_called()
