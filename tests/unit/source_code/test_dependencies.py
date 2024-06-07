from pathlib import Path
from unittest.mock import create_autospec

from databricks.labs.ucx.source_code.graph import (
    SourceContainer,
    DependencyResolver,
    DependencyProblem,
    Dependency,
    BaseImportResolver,
)
from databricks.labs.ucx.source_code.linters.files import FileLoader, ImportFileResolver
from databricks.labs.ucx.source_code.notebooks.loaders import (
    NotebookResolver,
    NotebookLoader,
)
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.known import Whitelist
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from tests.unit import (
    locate_site_packages,
)
from tests.unit.conftest import MockPathLookup


def dependency_resolver(notebook_resolver, path_lookup):
    library_resolver = PythonLibraryResolver(Whitelist())
    import_resolver = ImportFileResolver(FileLoader(), Whitelist())
    return DependencyResolver(library_resolver, notebook_resolver, import_resolver, path_lookup)


def test_dependency_resolver_repr(mock_notebook_resolver, mock_path_lookup):
    """for improving test coverage"""
    resolver = dependency_resolver(mock_notebook_resolver, mock_path_lookup)
    assert len(repr(resolver)) > 0


def test_dependency_resolver_visits_workspace_notebook_dependencies(mock_path_lookup):
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_notebook_dependency_graph(Path("root3.run.py"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root3.run.py", "root1.run.py", "leaf1.py", "leaf2.py"}


def test_dependency_resolver_visits_local_notebook_dependencies(mock_path_lookup):
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_notebook_dependency_graph(Path("root4.py"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root4.py", "leaf3.py"}


def test_dependency_resolver_visits_workspace_file_dependencies(mock_path_lookup):
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_local_file_dependency_graph(Path('./root8.py'))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {'leaf1.py', 'leaf2.py', 'root8.py'}


def test_dependency_resolver_raises_problem_with_unfound_workspace_notebook_dependency(mock_path_lookup):
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_notebook_dependency_graph(Path("root1-no-leaf.run.py"))
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
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_notebook_dependency_graph(Path("root4-no-leaf.py"))
    assert list(maybe.problems) == [
        DependencyProblem(
            'notebook-not-found', 'Notebook not found: __NO_LEAF__', Path('root4-no-leaf.py'), 1, 0, 1, 37
        )
    ]


def test_dependency_resolver_raises_problem_with_invalid_run_cell(mock_path_lookup):
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_notebook_dependency_graph(Path('leaf6.py'))
    assert list(maybe.problems) == [
        DependencyProblem('invalid-run-cell', 'Missing notebook path in %run command', Path('leaf6.py'), 5, 0, 5, 4)
    ]


def test_dependency_resolver_visits_recursive_file_dependencies(mock_path_lookup):
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_local_file_dependency_graph(Path("root6.py"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root6.py", "root5.py", "leaf4.py"}


def test_dependency_resolver_raises_problem_with_unresolved_import(mock_path_lookup):
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_local_file_dependency_graph(Path('root7.py'))
    assert list(maybe.problems) == [
        DependencyProblem('import-not-found', 'Could not locate import: some_library', Path("root7.py"), 1, 0, 1, 19)
    ]


def test_dependency_resolver_visits_file_dependencies(mock_path_lookup):
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_local_file_dependency_graph(Path("root5.py"))
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root5.py", "leaf4.py"}


def test_dependency_resolver_skips_builtin_dependencies(mock_path_lookup):
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_local_file_dependency_graph(Path("python_builtins.py"))
    assert not maybe.failed
    graph = maybe.graph
    maybe = graph.locate_dependency(Path("os"))
    assert maybe.failed
    maybe = graph.locate_dependency(Path("pathlib"))
    assert maybe.failed


def test_dependency_resolver_ignores_known_dependencies(mock_path_lookup):
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_local_file_dependency_graph(Path("python_builtins.py"))
    assert maybe.graph
    graph = maybe.graph
    maybe_graph = graph.locate_dependency(Path("databricks"))
    assert not maybe_graph.graph


def test_dependency_resolver_terminates_at_known_libraries(empty_index, mock_notebook_resolver, mock_path_lookup):
    lookup = MockPathLookup()
    site_packages_path = locate_site_packages()
    lookup.append_path(site_packages_path)
    file_loader = FileLoader()
    import_resolver = ImportFileResolver(file_loader, Whitelist())
    library_resolver = PythonLibraryResolver(Whitelist())
    resolver = DependencyResolver(library_resolver, mock_notebook_resolver, import_resolver, lookup)
    maybe = resolver.build_local_file_dependency_graph(Path("import-site-package.py"))
    assert not maybe.failed
    graph = maybe.graph
    maybe = graph.locate_dependency(Path(site_packages_path, "certifi", "core.py"))
    # we terminate graph resolution for known packages to save on CPU cycles
    assert maybe.failed


def test_dependency_resolver_raises_problem_with_unfound_root_file(mock_path_lookup, mock_notebook_resolver):
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_local_file_dependency_graph(Path("non-existing.py"))
    assert list(maybe.problems) == [
        DependencyProblem('file-not-found', 'File not found: non-existing.py', Path("non-existing.py"))
    ]


def test_dependency_resolver_raises_problem_with_unfound_root_notebook(mock_path_lookup):
    notebook_resolver = NotebookResolver(NotebookLoader())
    resolver = dependency_resolver(notebook_resolver, mock_path_lookup)
    maybe = resolver.build_notebook_dependency_graph(Path("unknown_notebook"))
    assert list(maybe.problems) == [
        DependencyProblem('notebook-not-found', 'Notebook not found: unknown_notebook', Path("unknown_notebook"))
    ]


def test_dependency_resolver_raises_problem_with_unloadable_root_file(mock_path_lookup, mock_notebook_resolver):

    class FailingFileLoader(FileLoader):
        def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
            return None

    file_loader = FailingFileLoader()
    whitelist = Whitelist()
    import_resolver = ImportFileResolver(file_loader, whitelist)
    pip_resolver = PythonLibraryResolver(whitelist)
    resolver = DependencyResolver(pip_resolver, mock_notebook_resolver, import_resolver, mock_path_lookup)
    maybe = resolver.build_local_file_dependency_graph(Path("import-sub-site-package.py"))
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
    pip_resolver = PythonLibraryResolver(Whitelist())
    resolver = DependencyResolver(pip_resolver, notebook_resolver, [], mock_path_lookup)
    maybe = resolver.build_notebook_dependency_graph(Path("root5.py"))
    assert list(maybe.problems) == [
        DependencyProblem('cannot-load-notebook', 'Could not load notebook root5.py', Path('<MISSING_SOURCE_PATH>'))
    ]


def test_dependency_resolver_raises_problem_with_missing_file_loader(mock_notebook_resolver, mock_path_lookup):
    library_resolver = PythonLibraryResolver(Whitelist())
    import_resolver = create_autospec(BaseImportResolver)
    import_resolver.resolve_import.return_value = None
    resolver = DependencyResolver(library_resolver, mock_notebook_resolver, import_resolver, mock_path_lookup)
    maybe = resolver.build_local_file_dependency_graph(Path("import-sub-site-package.py"))
    assert list(maybe.problems) == [
        DependencyProblem('missing-file-resolver', 'Missing resolver for local files', Path('<MISSING_SOURCE_PATH>'))
    ]


def test_dependency_resolver_raises_problem_for_non_inferable_sys_path(mock_notebook_resolver, mock_path_lookup):
    resolver = dependency_resolver(mock_notebook_resolver, mock_path_lookup)
    maybe = resolver.build_local_file_dependency_graph(Path("sys-path-with-fstring.py"))
    assert list(maybe.problems) == [
        DependencyProblem(
            'sys-path-cannot-compute',
            "Can't update sys.path from f'{name}' because the expression cannot be computed",
            Path('sys-path-with-fstring.py'),
            3,
            16,
            3,
            25,
        )
    ]
