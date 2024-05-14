from pathlib import Path

from databricks.labs.ucx.source_code.graph import (
    SourceContainer,
    DependencyResolver,
    DependencyProblem,
    Dependency,
)
from databricks.labs.ucx.source_code.notebooks.loaders import (
    NotebookResolver,
    NotebookLoader,
)
from databricks.labs.ucx.source_code.files import FileLoader, LocalFileResolver, SitePackageResolver
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.whitelist import WhitelistResolver, Whitelist
from tests.unit import (
    locate_site_packages,
    _samples_path,
    MockPathLookup,
    _load_sources,
)


def test_dependency_graph_builder_visits_workspace_notebook_dependencies():
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader)], lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path("root3.run.py"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root3.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"}


def test_dependency_graph_builder_visits_local_notebook_dependencies():
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader)], lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path("root4.py.txt"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root4.py.txt", "leaf3.py.txt"}


def test_dependency_graph_builder_visits_workspace_file_dependencies():
    whi = Whitelist()
    file_loader = FileLoader()
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackageResolver(file_loader),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ],
        lookup,
    )
    maybe = dependency_resolver.build_local_file_dependency_graph(Path('./root8.py'))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {'leaf1.py.txt', 'leaf2.py.txt', 'root8.py.txt'}


def test_dependency_graph_builder_raises_problem_with_unfound_workspace_notebook_dependency():
    whi = Whitelist()
    file_loader = FileLoader()
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    dependency_resolver = DependencyResolver(
        [
            NotebookResolver(notebook_loader),
            SitePackageResolver(file_loader),
            WhitelistResolver(whi),
            LocalFileResolver(file_loader),
        ],
        lookup,
    )
    maybe = dependency_resolver.build_notebook_dependency_graph(Path("root1-no-leaf.run.py"))
    assert list(maybe.problems) == [
        DependencyProblem(
            'notebook-not-found',
            'Notebook not found: __NOT_FOUND__',
            Path('root1-no-leaf.run.py'),
            19,
            0,
            19,
            22,
        )
    ]


def test_dependency_graph_builder_raises_problem_with_unfound_local_notebook_dependency():
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader)], lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path("root4-no-leaf.py"))
    assert list(maybe.problems) == [
        DependencyProblem(
            'notebook-not-found', 'Notebook not found: __NO_LEAF__', Path('root4-no-leaf.py'), 1, 0, 1, 37
        )
    ]


def test_dependency_graph_builder_raises_problem_with_non_constant_local_notebook_dependency():
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader)], lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path('root10.py.txt'))
    assert list(maybe.problems) == [
        DependencyProblem(
            'dependency-not-constant',
            "Can't check dependency not provided as a constant",
            Path('root10.py.txt'),
            2,
            0,
            2,
            35,
        )
    ]


def test_dependency_graph_builder_raises_problem_with_invalid_run_cell():
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader)], lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path('leaf6.py.txt'))
    assert list(maybe.problems) == [
        DependencyProblem('invalid-run-cell', 'Missing notebook path in %run command', Path('leaf6.py.txt'), 5, 0, 5, 4)
    ]


def test_dependency_graph_builder_visits_recursive_file_dependencies():
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    dependency_resolver = DependencyResolver(
        [NotebookResolver(notebook_loader), LocalFileResolver(FileLoader())], lookup
    )
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("root6.py.txt"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root6.py.txt", "root5.py.txt", "leaf4.py.txt"}


def test_dependency_graph_builder_raises_problem_with_unresolved_import():
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    dependency_resolver = DependencyResolver(
        [NotebookResolver(notebook_loader), LocalFileResolver(FileLoader())], lookup
    )
    maybe = dependency_resolver.build_local_file_dependency_graph(Path('root7.py.txt'))
    assert list(maybe.problems) == [
        DependencyProblem(
            'import-not-found', 'Could not locate import: some_library', Path("root7.py.txt"), 1, 0, 1, 19
        )
    ]


def test_dependency_graph_builder_raises_problem_with_non_constant_notebook_argument():
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    resolvers = [NotebookResolver(notebook_loader), WhitelistResolver(Whitelist()), LocalFileResolver(FileLoader())]
    dependency_resolver = DependencyResolver(resolvers, lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("run_notebooks.py.txt"))
    assert list(maybe.problems) == [
        DependencyProblem(
            'dependency-not-constant',
            "Can't check dependency not provided as a constant",
            Path("run_notebooks.py.txt"),
            14,
            13,
            14,
            50,
        )
    ]


def test_dependency_graph_builder_visits_file_dependencies():
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    dependency_resolver = DependencyResolver(
        [NotebookResolver(notebook_loader), LocalFileResolver(FileLoader())], lookup
    )
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("root5.py.txt"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root5.py.txt", "leaf4.py.txt"}


def test_dependency_graph_builder_skips_builtin_dependencies():
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    resolvers = [NotebookResolver(notebook_loader), WhitelistResolver(Whitelist()), LocalFileResolver(FileLoader())]
    dependency_resolver = DependencyResolver(resolvers, lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("python_builtins.py.txt"))
    assert not maybe.failed
    graph = maybe.graph
    maybe = graph.locate_dependency(Path("os"))
    assert maybe.failed
    maybe = graph.locate_dependency(Path("pathlib"))
    assert maybe.failed


def test_dependency_graph_builder_ignores_known_dependencies():
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    resolvers = [NotebookResolver(notebook_loader), WhitelistResolver(Whitelist()), LocalFileResolver(FileLoader())]
    dependency_resolver = DependencyResolver(resolvers, lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("python_builtins.py.txt"))
    assert maybe.graph
    graph = maybe.graph
    maybe_graph = graph.locate_dependency(Path("databricks"))
    assert not maybe_graph.graph


def test_dependency_graph_builder_visits_site_packages():
    file_loader = FileLoader()
    site_packages_path = locate_site_packages()
    lookup = PathLookup.from_pathlike_string(Path.cwd(), _samples_path(SourceContainer))
    lookup.append_path(site_packages_path)
    resolvers = [
        SitePackageResolver(file_loader),
        WhitelistResolver(Whitelist()),
        LocalFileResolver(file_loader),
    ]
    dependency_resolver = DependencyResolver(resolvers, lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("import-site-package.py.txt"))
    assert not maybe.failed
    graph = maybe.graph
    maybe = graph.locate_dependency(Path(site_packages_path, "certifi", "core.py"))
    assert not maybe.failed
    maybe = graph.locate_dependency(Path("core.py"))
    assert maybe.failed


def test_dependency_graph_builder_resolves_sub_site_package():
    # need a custom whitelist to avoid filtering out databricks
    datas = _load_sources(SourceContainer, "minimal-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0])
    site_packages_path = locate_site_packages()
    lookup = PathLookup.from_pathlike_string(Path.cwd(), _samples_path(SourceContainer))
    lookup.append_path(site_packages_path)
    file_loader = FileLoader()
    notebook_loader = NotebookLoader()
    resolvers = [
        NotebookResolver(notebook_loader),
        SitePackageResolver(file_loader),
        LocalFileResolver(file_loader),
        WhitelistResolver(whitelist),
    ]
    dependency_resolver = DependencyResolver(resolvers, lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("import-sub-site-package.py.txt"))
    assert maybe.graph
    maybe = maybe.graph.locate_dependency(Path(site_packages_path, "databricks", "labs", "lsql", "core.py"))
    assert maybe.graph


def test_dependency_graph_builder_raises_problem_with_unfound_root_file():
    lookup = MockPathLookup()
    dependency_resolver = DependencyResolver([LocalFileResolver(FileLoader())], lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("non-existing.py.txt"))
    assert list(maybe.problems) == [
        DependencyProblem('file-not-found', 'File not found: non-existing.py.txt', Path("non-existing.py.txt"))
    ]


def test_dependency_graph_builder_raises_problem_with_unfound_root_notebook():
    lookup = MockPathLookup()
    notebook_loader = NotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader)], lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path("unknown_notebook"))
    assert list(maybe.problems) == [
        DependencyProblem('notebook-not-found', 'Notebook not found: unknown_notebook', Path("unknown_notebook"))
    ]


def test_dependency_graph_builder_raises_problem_with_unloadable_root_file():
    lookup = MockPathLookup()

    class FailingFileLoader(FileLoader):
        def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
            return None

    file_loader = FailingFileLoader()
    dependency_resolver = DependencyResolver([LocalFileResolver(file_loader)], lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("import-sub-site-package.py.txt"))
    assert list(maybe.problems) == [
        DependencyProblem(
            'cannot-load-file', 'Could not load file import-sub-site-package.py.txt', Path('<MISSING_SOURCE_PATH>')
        )
    ]


def test_dependency_graph_builder_raises_problem_with_unloadable_root_notebook():
    lookup = MockPathLookup()

    class FailingNotebookLoader(NotebookLoader):
        def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
            return None

    notebook_loader = FailingNotebookLoader()
    dependency_resolver = DependencyResolver([NotebookResolver(notebook_loader)], lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path("root5.py.txt"))
    assert list(maybe.problems) == [
        DependencyProblem('cannot-load-notebook', 'Could not load notebook root5.py.txt', Path('<MISSING_SOURCE_PATH>'))
    ]
