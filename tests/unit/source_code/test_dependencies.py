from pathlib import Path
from unittest.mock import create_autospec

from databricks.labs.ucx.source_code.base import CurrentSessionState
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
from databricks.labs.ucx.source_code.known import KnownList
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from tests.unit import (
    locate_site_packages,
)
from tests.unit.conftest import MockPathLookup


def test_dependency_resolver_repr(simple_dependency_resolver):
    """for improving test coverage"""
    assert len(repr(simple_dependency_resolver)) > 0


def test_dependency_resolver_visits_workspace_notebook_dependencies(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_notebook_dependency_graph(Path("root3.run.py"), CurrentSessionState())
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root3.run.py", "root1.run.py", "leaf1.py", "leaf2.py"}


def test_dependency_resolver_locates_root_dependencies(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_notebook_dependency_graph(Path("root3.run.py"), CurrentSessionState())
    assert not maybe.failed
    assert maybe.graph.root_relative_names() == {"root3.run.py"}


def test_dependency_resolver_visits_local_notebook_dependencies(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_notebook_dependency_graph(Path("root4.py"), CurrentSessionState())
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root4.py", "leaf3.py"}


def test_dependency_resolver_visits_local_notebook_dependencies_in_magic_run(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_notebook_dependency_graph(Path("root1.magic.py"), CurrentSessionState())
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root1.magic.py", "leaf1.py"}


def test_dependency_resolver_visits_workspace_file_dependencies(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_local_file_dependency_graph(Path('./root8.py'), CurrentSessionState())
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {'leaf1.py', 'leaf2.py', 'root8.py'}


def test_dependency_resolver_raises_problem_with_unresolved_workspace_notebook_dependency(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_notebook_dependency_graph(
        Path("root1-no-leaf.run.py"), CurrentSessionState()
    )
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


def test_dependency_resolver_raises_problem_with_unresolved_local_notebook_dependency(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_notebook_dependency_graph(Path("root4-no-leaf.py"), CurrentSessionState())
    assert list(maybe.problems) == [
        DependencyProblem(
            'notebook-not-found', 'Notebook not found: __NO_LEAF__', Path('root4-no-leaf.py'), 1, 0, 1, 37
        )
    ]


def test_dependency_resolver_raises_problem_with_invalid_run_cell(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_notebook_dependency_graph(Path('leaf6.py'), CurrentSessionState())
    assert list(maybe.problems) == [
        DependencyProblem('invalid-run-cell', 'Missing notebook path in %run command', Path('leaf6.py'), 5, 0, 5, 4)
    ]


def test_dependency_resolver_visits_recursive_file_dependencies(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_local_file_dependency_graph(Path("root6.py"), CurrentSessionState())
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root6.py", "root5.py", "leaf4.py"}


def test_dependency_resolver_raises_problem_with_unresolved_import(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_local_file_dependency_graph(Path('root7.py'), CurrentSessionState())
    assert list(maybe.problems) == [
        DependencyProblem('import-not-found', 'Could not locate import: some_library', Path("root7.py"), 0, 0, 0, 19)
    ]


def test_dependency_resolver_visits_file_dependencies(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_local_file_dependency_graph(Path("root5.py"), CurrentSessionState())
    assert not maybe.failed
    assert maybe.graph.all_relative_names() == {"root5.py", "leaf4.py"}


def test_dependency_resolver_skips_builtin_dependencies(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_local_file_dependency_graph(
        Path("python_builtins.py"), CurrentSessionState()
    )
    assert not maybe.failed
    graph = maybe.graph
    maybe = graph.locate_dependency(Path("os"))
    assert maybe.failed
    maybe = graph.locate_dependency(Path("pathlib"))
    assert maybe.failed


def test_dependency_resolver_ignores_known_dependencies(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_local_file_dependency_graph(
        Path("python_builtins.py"), CurrentSessionState()
    )
    assert maybe.graph
    graph = maybe.graph
    maybe_graph = graph.locate_dependency(Path("databricks"))
    assert not maybe_graph.graph


def test_dependency_resolver_terminates_at_known_libraries(empty_index, mock_notebook_resolver, mock_path_lookup):
    lookup = MockPathLookup()
    site_packages_path = locate_site_packages()
    lookup.append_path(site_packages_path)
    file_loader = FileLoader()
    import_resolver = ImportFileResolver(file_loader, KnownList())
    library_resolver = PythonLibraryResolver(KnownList())
    resolver = DependencyResolver(library_resolver, mock_notebook_resolver, import_resolver, lookup)
    maybe = resolver.build_local_file_dependency_graph(Path("import-site-package.py"), CurrentSessionState())
    assert not maybe.failed
    graph = maybe.graph
    maybe = graph.locate_dependency(Path(site_packages_path, "certifi", "core.py"))
    # we terminate graph resolution for known packages to save on CPU cycles
    assert maybe.failed


def test_dependency_resolver_raises_problem_with_unresolved_root_file(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_local_file_dependency_graph(Path("non-existing.py"), CurrentSessionState())
    assert list(maybe.problems) == [
        DependencyProblem('file-not-found', 'File not found: non-existing.py', Path("non-existing.py"))
    ]


def test_dependency_resolver_raises_problem_with_unresolved_root_notebook(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_notebook_dependency_graph(Path("unknown_notebook"), CurrentSessionState())
    assert list(maybe.problems) == [
        DependencyProblem('notebook-not-found', 'Notebook not found: unknown_notebook', Path("unknown_notebook"))
    ]


def test_dependency_resolver_raises_problem_with_unloadable_root_file(mock_path_lookup, mock_notebook_resolver):

    class FailingFileLoader(FileLoader):
        def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> SourceContainer | None:
            return None

    file_loader = FailingFileLoader()
    allow_list = KnownList()
    import_resolver = ImportFileResolver(file_loader, allow_list)
    pip_resolver = PythonLibraryResolver(allow_list)
    resolver = DependencyResolver(pip_resolver, mock_notebook_resolver, import_resolver, mock_path_lookup)
    maybe = resolver.build_local_file_dependency_graph(Path("import-sub-site-package.py"), CurrentSessionState())
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
    known_list = KnownList()
    pip_resolver = PythonLibraryResolver(known_list)
    import_resolver = ImportFileResolver(FileLoader(), known_list)
    resolver = DependencyResolver(pip_resolver, notebook_resolver, import_resolver, mock_path_lookup)
    maybe = resolver.build_notebook_dependency_graph(Path("root5.py"), CurrentSessionState())
    assert list(maybe.problems) == [
        DependencyProblem('cannot-load-notebook', 'Could not load notebook root5.py', Path('<MISSING_SOURCE_PATH>'))
    ]


def test_dependency_resolver_raises_problem_with_missing_file_loader(mock_notebook_resolver, mock_path_lookup):
    library_resolver = PythonLibraryResolver(KnownList())
    import_resolver = create_autospec(BaseImportResolver)
    import_resolver.resolve_import.return_value = None
    resolver = DependencyResolver(library_resolver, mock_notebook_resolver, import_resolver, mock_path_lookup)
    maybe = resolver.build_local_file_dependency_graph(Path("import-sub-site-package.py"), CurrentSessionState())
    assert list(maybe.problems) == [
        DependencyProblem('missing-file-resolver', 'Missing resolver for local files', Path('<MISSING_SOURCE_PATH>'))
    ]


def test_dependency_resolver_raises_problem_for_non_inferable_sys_path(simple_dependency_resolver):
    maybe = simple_dependency_resolver.build_local_file_dependency_graph(
        Path("sys-path-with-fstring.py"), CurrentSessionState()
    )
    assert list(maybe.problems) == [
        DependencyProblem(
            code='sys-path-cannot-compute-value',
            message="Can't update sys.path from f'{name_2}' because the expression cannot be computed",
            source_path=Path('sys-path-with-fstring.py'),
            start_line=4,
            start_col=16,
            end_line=4,
            end_col=27,
        ),
        DependencyProblem(
            code='sys-path-cannot-compute-value',
            message="Can't update sys.path from name because the expression cannot be computed",
            source_path=Path('sys-path-with-fstring.py'),
            start_line=7,
            start_col=20,
            end_line=7,
            end_col=24,
        ),
    ]
