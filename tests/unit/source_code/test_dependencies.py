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
    _local_loader_with_side_effects, MockPathLookup,
)


def test_dependency_graph_builder_visits_workspace_notebook_dependencies():
    lookup = MockPathLookup()
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader)])
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_notebook_dependency_graph(Path("root3.run.py"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root3.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"}


def test_dependency_graph_builder_visits_local_notebook_dependencies():
    lookup = MockPathLookup()
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader)])
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_notebook_dependency_graph(Path("root4.py.txt"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root4.py.txt", "leaf3.py.txt"}


def test_dependency_graph_builder_visits_workspace_file_dependencies():
    whi = whitelist_mock()
    site_packages = SitePackages.parse(locate_site_packages())
    file_loader = FileLoader()
    lookup = MockPathLookup()
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, lookup),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_local_file_dependency_graph(Path('./root8.py'))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {'leaf1.py.txt', 'leaf2.py.txt', 'root8.py.txt'}


def test_dependency_graph_builder_raises_problem_with_unfound_workspace_notebook_dependency():
    whi = whitelist_mock()
    site_packages = SitePackages.parse(locate_site_packages())
    file_loader = FileLoader()
    lookup = MockPathLookup()
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, lookup),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_notebook_dependency_graph(Path("./root1-no-leaf.run.py"))
    assert list(maybe.problems) == [
        DependencyProblem(
            'notebook-not-found', 'Notebook not found: __NOT_FOUND__', lookup.cwd / 'root1-no-leaf.run.py.txt', 19, 0, 19, 22
        )
    ]


def test_dependency_graph_builder_raises_problem_with_unfound_local_notebook_dependency():
    lookup = MockPathLookup()
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader)])
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_notebook_dependency_graph(Path("./root4-no-leaf.py"))
    assert list(maybe.problems) == [
        DependencyProblem('notebook-not-found', 'Notebook not found: __NO_LEAF__', lookup.cwd / 'root4-no-leaf.py.txt', 1, 0, 1, 37)
    ]


def test_dependency_graph_builder_raises_problem_with_non_constant_local_notebook_dependency():
    lookup = MockPathLookup()
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader)])
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_notebook_dependency_graph(Path('./root10.py.txt'))
    assert list(maybe.problems) == [
        DependencyProblem(
            'dependency-not-constant', "Can't check dependency not provided as a constant", lookup.cwd / 'root10.py.txt', 2, 0, 2, 35
        )
    ]


def test_dependency_graph_builder_raises_problem_with_invalid_run_cell():
    lookup = MockPathLookup()
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader)])
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_notebook_dependency_graph(Path('leaf6.py.txt'))
    assert list(maybe.problems) == [
        DependencyProblem('invalid-run-cell', 'Missing notebook path in %run command', lookup.cwd / 'leaf6.py.txt', 5, 0, 5, 4)
    ]


def test_dependency_graph_builder_visits_recursive_file_dependencies():
    lookup = MockPathLookup()
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader), LocalFileResolver(FileLoader())])
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_local_file_dependency_graph(Path("root6.py.txt"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root6.py.txt", "root5.py.txt", "leaf4.py.txt"}


def test_dependency_graph_builder_raises_problem_with_unresolved_import():
    lookup = MockPathLookup()
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader), LocalFileResolver(FileLoader())])
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_local_file_dependency_graph(Path('root7.py.txt'))
    assert list(maybe.problems) == [
        DependencyProblem(
            'import-not-found', 'Could not locate import: some_library', lookup.cwd / "root7.py.txt", 1, 0, 1, 19
        )
    ]


def test_dependency_graph_builder_raises_problem_with_non_constant_notebook_argument():
    lookup = MockPathLookup()
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader), LocalFileResolver(FileLoader())])
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_local_file_dependency_graph(Path("run_notebooks.py.txt"))
    assert list(maybe.problems) == [
        DependencyProblem(
            'dependency-not-constant',
            "Can't check dependency not provided as a constant",
            lookup.cwd / "run_notebooks.py.txt",
            14,
            13,
            14,
            50,
        )
    ]


def test_dependency_graph_builder_visits_file_dependencies():
    lookup = MockPathLookup()
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader), LocalFileResolver(FileLoader())])
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_local_file_dependency_graph(Path("root5.py.txt"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root5.py.txt", "leaf4.py.txt"}


def test_dependency_graph_builder_skips_builtin_dependencies():
    lookup = MockPathLookup()
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader), LocalFileResolver(FileLoader())])
    builder = DependencyGraphBuilder(dependency_resolver, lookup)
    maybe = builder.build_local_file_dependency_graph(Path("python_builtins.py.txt"))
    assert not maybe.failed
    graph = maybe.graph
    maybe = graph.locate_dependency(Path("os"))
    assert maybe.failed
    maybe = graph.locate_dependency(Path("pathlib"))
    assert maybe.failed


def test_dependency_graph_builder_ignores_known_dependencies():
    paths = ["python_builtins.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    datas = _load_sources(SourceContainer, "sample-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    file_loader = _local_loader_with_side_effects(FileLoader, sources, {})
    site_packages = SitePackages.parse(locate_site_packages())
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whitelist),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    maybe = builder.build_local_file_dependency_graph(Path("python_builtins.py.txt"))
    assert maybe.graph
    graph = maybe.graph
    maybe_graph = graph.locate_dependency(Path("databricks"))
    assert not maybe_graph.graph


def test_dependency_graph_builder_visits_site_packages(empty_index):
    datas = _load_sources(SourceContainer, "sample-python-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    paths = ["import-site-package.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(SourceContainer, *paths)))
    provider = PathLookup.from_pathlike_string(Path.cwd(), _samples_path(SourceContainer))
    file_loader = TestFileLoader(sources)
    site_packages_path = locate_site_packages()
    site_packages = SitePackages.parse(site_packages_path)
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whitelist),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    maybe = builder.build_local_file_dependency_graph(Path("import-site-package.py.txt"))
    assert not maybe.failed
    graph = maybe.graph
    maybe = graph.locate_dependency(Path(site_packages_path, "certifi/core.py"))
    assert not maybe.failed
    maybe = graph.locate_dependency(Path("core.py"))
    assert maybe.failed


def test_dependency_graph_builder_raises_problem_with_unfound_root_file(empty_index):
    site_packages = SitePackages.parse(locate_site_packages())
    whi = whitelist_mock()
    file_loader = create_autospec(FileLoader)
    provider = PathLookup.from_pathlike_string(Path.cwd(), "")
    notebook_loader = LocalNotebookLoader()
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackagesResolver(site_packages, file_loader, provider),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ]
    )
    builder = DependencyGraphBuilder(dependency_resolver, provider)
    maybe = builder.build_local_file_dependency_graph(Path("root1.run.py.txt"))
    assert list(maybe.problems) == [DependencyProblem('file-not-found', 'File not found: root1.run.py.txt')]
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
    maybe = builder.build_notebook_dependency_graph(Path("root2.run.py.txt"))
    assert list(maybe.problems) == [DependencyProblem('notebook-not-found', 'Notebook not found: root2.run.py.txt')]
    file_loader.load_dependency.assert_not_called()
