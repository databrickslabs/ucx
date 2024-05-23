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
from databricks.labs.ucx.source_code.files import FileLoader, ImportFileResolver
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python_libraries import PipResolver
from databricks.labs.ucx.source_code.whitelist import Whitelist
from tests.unit import (
    locate_site_packages,
    _samples_path,
    _load_sources,
)


def test_dependency_resolver_repr(mock_notebook_resolver, mock_path_lookup):
    """for improving test coverage"""
    dependency_resolver = DependencyResolver([], mock_notebook_resolver, [], mock_path_lookup)
    assert len(repr(dependency_resolver)) > 0


def test_dependency_resolver_visits_workspace_notebook_dependencies(mock_path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver([], notebook_resolver, [], mock_path_lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path("root3.run.py"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root3.run.py", "root1.run.py", "leaf1.py", "leaf2.py"}


def test_dependency_resolver_visits_local_notebook_dependencies(mock_path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver([], notebook_resolver, [], mock_path_lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path("root4.py"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root4.py", "leaf3.py"}


def test_dependency_resolver_visits_workspace_file_dependencies(mock_path_lookup):
    whitelist = Whitelist()
    file_loader = FileLoader()
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    import_resolver = ImportFileResolver(file_loader, whitelist)
    dependency_resolver = DependencyResolver([], notebook_resolver, import_resolver, mock_path_lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path('./root8.py'))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {'leaf1.py', 'leaf2.py', 'root8.py'}


def test_dependency_resolver_raises_problem_with_unfound_workspace_notebook_dependency(mock_path_lookup):
    file_loader = FileLoader()
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    import_resolver = ImportFileResolver(file_loader, Whitelist())
    dependency_resolver = DependencyResolver([], notebook_resolver, import_resolver, mock_path_lookup)
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


def test_dependency_resolver_raises_problem_with_unfound_local_notebook_dependency(mock_path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver([], notebook_resolver, [], mock_path_lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path("root4-no-leaf.py"))
    assert list(maybe.problems) == [
        DependencyProblem(
            'notebook-not-found', 'Notebook not found: __NO_LEAF__', Path('root4-no-leaf.py'), 1, 0, 1, 37
        )
    ]


def test_dependency_resolver_raises_problem_with_non_constant_local_notebook_dependency(mock_path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver([], notebook_resolver, [], mock_path_lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path('root10.py'))
    assert list(maybe.problems) == [
        DependencyProblem(
            'dependency-not-constant',
            "Can't check dependency not provided as a constant",
            Path('root10.py'),
            2,
            0,
            2,
            35,
        )
    ]


def test_dependency_resolver_raises_problem_with_invalid_run_cell(mock_path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver([], notebook_resolver, [], mock_path_lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path('leaf6.py'))
    assert list(maybe.problems) == [
        DependencyProblem('invalid-run-cell', 'Missing notebook path in %run command', Path('leaf6.py'), 5, 0, 5, 4)
    ]


def test_dependency_resolver_visits_recursive_file_dependencies(mock_path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    import_resolver = ImportFileResolver(FileLoader(), Whitelist())
    dependency_resolver = DependencyResolver([], notebook_resolver, import_resolver, mock_path_lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("root6.py"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root6.py", "root5.py", "leaf4.py"}


def test_dependency_resolver_raises_problem_with_unresolved_import(mock_path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    import_resolver = ImportFileResolver(FileLoader(), Whitelist())
    dependency_resolver = DependencyResolver([], notebook_resolver, import_resolver, mock_path_lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path('root7.py'))
    assert list(maybe.problems) == [
        DependencyProblem('import-not-found', 'Could not locate import: some_library', Path("root7.py"), 1, 0, 1, 19)
    ]


def test_dependency_resolver_raises_problem_with_non_constant_notebook_argument(mock_path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    import_resolver = ImportFileResolver(FileLoader(), Whitelist())
    dependency_resolver = DependencyResolver([], notebook_resolver, import_resolver, mock_path_lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("run_notebooks.py"))
    assert list(maybe.problems) == [
        DependencyProblem(
            'dependency-not-constant',
            "Can't check dependency not provided as a constant",
            Path("run_notebooks.py"),
            14,
            13,
            14,
            50,
        )
    ]


def test_dependency_resolver_visits_file_dependencies(mock_path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    import_resolver = ImportFileResolver(FileLoader(), Whitelist())
    dependency_resolver = DependencyResolver([], notebook_resolver, import_resolver, mock_path_lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("root5.py"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root5.py", "leaf4.py"}


def test_dependency_resolver_skips_builtin_dependencies(mock_path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    import_resolver = ImportFileResolver(FileLoader(), Whitelist())
    dependency_resolver = DependencyResolver([], notebook_resolver, import_resolver, mock_path_lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("python_builtins.py"))
    assert not maybe.failed
    graph = maybe.graph
    maybe = graph.locate_dependency(Path("os"))
    assert maybe.failed
    maybe = graph.locate_dependency(Path("pathlib"))
    assert maybe.failed


def test_dependency_resolver_ignores_known_dependencies(mock_path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    import_resolver = ImportFileResolver(FileLoader(), Whitelist())
    dependency_resolver = DependencyResolver([], notebook_resolver, import_resolver, mock_path_lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("python_builtins.py"))
    assert maybe.graph
    graph = maybe.graph
    maybe_graph = graph.locate_dependency(Path("databricks"))
    assert not maybe_graph.graph


def test_dependency_resolver_visits_site_packages(empty_index, mock_notebook_resolver):
    site_packages_path = locate_site_packages()
    lookup = PathLookup.from_pathlike_string(Path.cwd(), _samples_path(SourceContainer))
    lookup.append_path(site_packages_path)
    file_loader = FileLoader()
    import_resolver = ImportFileResolver(file_loader, Whitelist())
    dependency_resolver = DependencyResolver([], mock_notebook_resolver, import_resolver, lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("import-site-package.py"))
    assert not maybe.failed
    graph = maybe.graph
    maybe = graph.locate_dependency(Path(site_packages_path, "certifi", "core.py"))
    assert not maybe.failed
    maybe = graph.locate_dependency(Path("core.py"))
    assert maybe.failed


def test_dependency_resolver_resolves_sub_site_package():
    # need a custom whitelist to avoid filtering out databricks
    datas = _load_sources(SourceContainer, "minimal-compatibility-catalog.yml")
    whitelist = Whitelist.parse(datas[0], False)
    site_packages_path = locate_site_packages()
    lookup = PathLookup.from_pathlike_string(Path.cwd(), _samples_path(SourceContainer))
    lookup.append_path(site_packages_path)
    file_loader = FileLoader()
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    import_resolver = ImportFileResolver(file_loader, whitelist)
    dependency_resolver = DependencyResolver([], notebook_resolver, import_resolver, lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("import-sub-site-package.py"))
    assert maybe.graph
    maybe = maybe.graph.locate_dependency(Path(site_packages_path, "databricks", "labs", "lsql", "core.py"))
    assert maybe.graph


def test_dependency_resolver_raises_problem_with_unfound_root_file(mock_path_lookup, mock_notebook_resolver):
    import_resolver = ImportFileResolver(FileLoader(), Whitelist())
    dependency_resolver = DependencyResolver([], mock_notebook_resolver, import_resolver, mock_path_lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("non-existing.py"))
    assert list(maybe.problems) == [
        DependencyProblem('file-not-found', 'File not found: non-existing.py', Path("non-existing.py"))
    ]


def test_dependency_resolver_raises_problem_with_unfound_root_notebook(mock_path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver([], notebook_resolver, [], mock_path_lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path("unknown_notebook"))
    assert list(maybe.problems) == [
        DependencyProblem('notebook-not-found', 'Notebook not found: unknown_notebook', Path("unknown_notebook"))
    ]


def test_dependency_resolver_raises_problem_with_unloadable_root_file(mock_path_lookup, mock_notebook_resolver):

    class FailingFileLoader(FileLoader):
        def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
            return None

    file_loader = FailingFileLoader()
    import_resolver = ImportFileResolver(file_loader, Whitelist())
    dependency_resolver = DependencyResolver([], mock_notebook_resolver, import_resolver, mock_path_lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("import-sub-site-package.py"))
    assert list(maybe.problems) == [
        DependencyProblem(
            'cannot-load-file', 'Could not load file import-sub-site-package.py', Path('<MISSING_SOURCE_PATH>')
        )
    ]


def test_dependency_resolver_raises_problem_with_unloadable_root_notebook(mock_path_lookup):

    class FailingNotebookLoader(NotebookLoader):
        def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
            return None

    notebook_loader = FailingNotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver([], notebook_resolver, [], mock_path_lookup)
    maybe = dependency_resolver.build_notebook_dependency_graph(Path("root5.py"))
    assert list(maybe.problems) == [
        DependencyProblem('cannot-load-notebook', 'Could not load notebook root5.py', Path('<MISSING_SOURCE_PATH>'))
    ]


def test_dependency_resolver_raises_problem_with_missing_file_loader(mock_notebook_resolver, mock_path_lookup):
    dependency_resolver = DependencyResolver([], mock_notebook_resolver, [], mock_path_lookup)
    maybe = dependency_resolver.build_local_file_dependency_graph(Path("import-sub-site-package.py"))
    assert list(maybe.problems) == [
        DependencyProblem('missing-file-resolver', 'Missing resolver for local files', Path('<MISSING_SOURCE_PATH>'))
    ]


def test_dependency_resolver_resolves_already_installed_library_dependency(mock_notebook_resolver):
    path_lookup = PathLookup.from_sys_path(Path.cwd())
    file_loader = FileLoader()
    whitelist = Whitelist()
    dependency_resolver = DependencyResolver(
        [PipResolver(file_loader, whitelist)],
        mock_notebook_resolver,
        ImportFileResolver(file_loader, whitelist),
        path_lookup,
    )
    maybe = dependency_resolver.build_library_dependency_graph(Path("astroid"))
    library_graph = maybe.graph
    assert library_graph is not None
    maybe = library_graph.locate_dependency(path_lookup.resolve(Path("astroid", "builder.py")))
    assert maybe.graph is not None


def test_dependency_resolver_resolves_newly_installed_library_dependency(mock_notebook_resolver):
    path_lookup = PathLookup.from_sys_path(Path.cwd())
    dependency_resolver = DependencyResolver(
        [PipResolver(FileLoader(), Whitelist())], mock_notebook_resolver, [], path_lookup
    )
    maybe = dependency_resolver.build_library_dependency_graph(Path("demo-egg"))
    library_graph = maybe.graph
    assert library_graph is not None
    maybe = library_graph.locate_dependency(path_lookup.resolve(Path("pkgdir", "__init__.py")))
    assert maybe.graph is not None


def test_dependency_resolver_fails_to_load_unknown_library(mock_notebook_resolver):
    path_lookup = PathLookup.from_sys_path(Path.cwd())
    dependency_resolver = DependencyResolver(
        [PipResolver(FileLoader(), Whitelist())], mock_notebook_resolver, [], path_lookup
    )
    maybe = dependency_resolver.build_library_dependency_graph(Path("some-unknown-library"))
    assert maybe.graph is None
