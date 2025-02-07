from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import CurrentSessionState, Failure
from databricks.labs.ucx.source_code.files import FileLoader, LocalFile, ImportFileResolver
from databricks.labs.ucx.source_code.folders import Folder, FolderLoader
from databricks.labs.ucx.source_code.graph import Dependency, DependencyResolver, SourceContainer
from databricks.labs.ucx.source_code.known import KnownList
from databricks.labs.ucx.source_code.linters.base import PythonLinter
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.files import FileLinter, NotebookMigrator
from databricks.labs.ucx.source_code.linters.folders import LocalCodeLinter
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader, NotebookResolver
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver

from tests.unit import locate_site_packages, _samples_path


@pytest.mark.parametrize("extension", [".json", ".md"])
def test_file_linter_lint_ignores_file_with_extension(extension: str) -> None:
    dependency = create_autospec(Dependency)
    dependency.path.suffix.lower.return_value = extension
    path_lookup = create_autospec(PathLookup)
    context = create_autospec(LinterContext)
    linter = FileLinter(dependency, path_lookup, context)

    advices = list(linter.lint())

    assert not advices
    dependency.path.suffix.lower.assert_called_once()
    path_lookup.assert_not_called()
    context.assert_not_called()


@pytest.mark.parametrize("name", [".ds_store", "metadata"])
def test_file_linter_lint_ignores_file_with_name(name: str) -> None:
    dependency = create_autospec(Dependency)
    dependency.path.name.lower.return_value = name
    path_lookup = create_autospec(PathLookup)
    context = create_autospec(LinterContext)
    linter = FileLinter(dependency, path_lookup, context)

    advices = list(linter.lint())

    assert not advices
    dependency.path.name.lower.assert_called_once()
    path_lookup.assert_not_called()
    context.assert_not_called()


def test_file_linter_lints_file() -> None:
    local_file = create_autospec(LocalFile)
    local_file.language = Language.PYTHON
    local_file.content = "print(1)"
    dependency = create_autospec(Dependency)
    dependency.path.suffix.lower.return_value = ".py"
    dependency.load.return_value = local_file
    path_lookup = create_autospec(PathLookup)
    python_linter = create_autospec(PythonLinter)
    context = create_autospec(LinterContext)
    context.linter.return_value = python_linter
    linter = FileLinter(dependency, path_lookup, context)

    advices = list(linter.lint())

    assert not advices
    local_file.assert_not_called()
    dependency.load.assert_called_once_with(path_lookup)
    path_lookup.assert_not_called()  # not used as the `load` method is mocked
    context.linter.assert_called_once_with(Language.PYTHON)
    python_linter.lint.assert_called_once_with("print(1)")


def test_file_linter_lints_notebook() -> None:
    notebook = create_autospec(Notebook)
    dependency = create_autospec(Dependency)
    dependency.path.suffix.lower.return_value = ".py"
    dependency.load.return_value = notebook
    path_lookup = create_autospec(PathLookup)
    context = create_autospec(LinterContext)
    linter = FileLinter(dependency, path_lookup, context)

    advices = list(linter.lint())

    assert not advices
    notebook.assert_not_called()
    dependency.load.assert_called_once_with(path_lookup)
    path_lookup.assert_not_called()  # not used as the `load` method is mocked
    context.assert_not_called()


def test_file_linter_lints_unsupported_file() -> None:
    source_container = create_autospec(SourceContainer)
    dependency = create_autospec(Dependency)
    dependency.path.suffix.lower.return_value = ".py"
    dependency.load.return_value = source_container
    path_lookup = create_autospec(PathLookup)
    context = create_autospec(LinterContext)
    linter = FileLinter(dependency, path_lookup, context)

    advices = list(linter.lint())

    assert advices == [Failure("unsupported-file", "Unsupported file", -1, -1, -1, -1)]
    source_container.assert_not_called()
    dependency.load.assert_called_once_with(path_lookup)
    path_lookup.assert_not_called()  # not used as the `load` method is mocked
    context.assert_not_called()


def test_file_linter_lints_this_file(mock_path_lookup) -> None:
    """This test does not mock to test closer to reality."""
    dependency = Dependency(FileLoader(), Path(__file__), inherits_context=False)
    context = LinterContext()
    linter = FileLinter(dependency, mock_path_lookup, context)

    advices = list(linter.lint())

    assert not advices


def test_file_linter_applies_unsupported_file() -> None:
    source_container = create_autospec(SourceContainer)
    dependency = create_autospec(Dependency)
    dependency.path.suffix.lower.return_value = ".py"
    dependency.load.return_value = source_container
    path_lookup = create_autospec(PathLookup)
    context = create_autospec(LinterContext)
    linter = FileLinter(dependency, path_lookup, context)

    linter.apply()

    source_container.assert_not_called()
    dependency.load.assert_called_once_with(path_lookup)
    path_lookup.assert_not_called()  # not used as the `load` method is mocked
    context.assert_not_called()


def test_file_linter_applies_migrated(tmp_path, mock_path_lookup, migration_index) -> None:
    """This test does not mock to test closer to reality."""
    path = tmp_path / "file.py"
    path.write_text("df = spark.read.table('old.things')")
    dependency = Dependency(FileLoader(), path, inherits_context=False)
    context = LinterContext(migration_index)
    linter = FileLinter(dependency, mock_path_lookup, context)

    linter.apply()

    # The .rstrip() is to remove the trailing newlines added by the fixer
    assert path.read_text().rstrip() == "df = spark.read.table('brand.new.stuff')"


def test_notebook_migrator_ignores_unsupported_extensions() -> None:
    languages = LinterContext(TableMigrationIndex([]))
    migrator = NotebookMigrator(languages)
    path = Path('unsupported.ext')
    assert not migrator.apply(path)


def test_notebook_migrator_supported_language_no_diagnostics(mock_path_lookup) -> None:
    languages = LinterContext(TableMigrationIndex([]))
    migrator = NotebookMigrator(languages)
    path = mock_path_lookup.resolve(Path("root1.run.py"))
    assert not migrator.apply(path)


@pytest.fixture()
def local_code_linter(mock_path_lookup, migration_index):
    notebook_loader = NotebookLoader()
    file_loader = FileLoader()
    folder_loader = FolderLoader(notebook_loader, file_loader)
    allow_list = KnownList()
    pip_resolver = PythonLibraryResolver(allow_list)
    session_state = CurrentSessionState()
    import_file_resolver = ImportFileResolver(file_loader, allow_list)
    resolver = DependencyResolver(
        pip_resolver,
        NotebookResolver(NotebookLoader()),
        import_file_resolver,
        import_file_resolver,
        mock_path_lookup,
    )
    return LocalCodeLinter(
        notebook_loader,
        file_loader,
        folder_loader,
        mock_path_lookup,
        session_state,
        resolver,
        lambda: LinterContext(migration_index),
    )


def test_linter_walks_directory(mock_path_lookup, local_code_linter) -> None:
    mock_path_lookup.append_path(Path(_samples_path(SourceContainer)))
    path = Path(__file__).parent / "../samples" / "simulate-sys-path"
    paths: set[Path] = set()
    advices = list(local_code_linter.lint_path(path, paths))
    assert len(paths) > 10
    assert not advices


def test_linter_lints_children_in_context(mock_path_lookup, local_code_linter) -> None:
    mock_path_lookup.append_path(Path(_samples_path(SourceContainer)))
    path = Path(__file__).parent.parent / "samples" / "parent-child-context"
    paths: set[Path] = set()
    advices = list(local_code_linter.lint_path(path, paths))
    assert len(paths) == 3
    assert not advices


def test_triple_dot_import() -> None:
    file_resolver = ImportFileResolver(FileLoader(), KnownList())
    path_lookup = create_autospec(PathLookup)
    path_lookup.cwd.as_posix.return_value = '/some/path/to/folder'
    path_lookup.resolve.return_value = Path('/some/path/foo.py')

    maybe = file_resolver.resolve_import(path_lookup, "...foo")
    assert not maybe.problems
    assert maybe.dependency is not None
    assert maybe.dependency.path == Path('/some/path/foo.py')
    path_lookup.resolve.assert_called_once_with(Path('/some/path/to/folder/../../foo.py'))


def test_single_dot_import() -> None:
    file_resolver = ImportFileResolver(FileLoader(), KnownList())
    path_lookup = create_autospec(PathLookup)
    path_lookup.cwd.as_posix.return_value = '/some/path/to/folder'
    path_lookup.resolve.return_value = Path('/some/path/to/folder/foo.py')

    maybe = file_resolver.resolve_import(path_lookup, ".foo")
    assert not maybe.problems
    assert maybe.dependency is not None
    assert maybe.dependency.path == Path('/some/path/to/folder/foo.py')
    path_lookup.resolve.assert_called_once_with(Path('/some/path/to/folder/foo.py'))


def test_folder_has_repr() -> None:
    notebook_loader = NotebookLoader()
    file_loader = FileLoader()
    folder = Folder(Path("test"), notebook_loader, file_loader, FolderLoader(notebook_loader, file_loader))
    assert len(repr(folder)) > 0


site_packages = locate_site_packages()


@pytest.mark.skip("Manual testing for troubleshooting")
@pytest.mark.parametrize(
    "path", [Path("/Users/eric.vergnaud/development/ucx/.venv/lib/python3.10/site-packages/spacy/pipe_analysis.py")]
)
def test_known_issues(path: Path, migration_index) -> None:
    notebook_loader = NotebookLoader()
    file_loader = FileLoader()
    notebook_loader = NotebookLoader()
    folder_loader = FolderLoader(notebook_loader, file_loader)
    path_lookup = PathLookup.from_sys_path(Path.cwd())
    session_state = CurrentSessionState()
    allow_list = KnownList()
    notebook_resolver = NotebookResolver(NotebookLoader())
    import_resolver = ImportFileResolver(file_loader, allow_list)
    pip_resolver = PythonLibraryResolver(allow_list)
    resolver = DependencyResolver(pip_resolver, notebook_resolver, import_resolver, import_resolver, path_lookup)
    linter = LocalCodeLinter(
        notebook_loader,
        file_loader,
        folder_loader,
        path_lookup,
        session_state,
        resolver,
        lambda: LinterContext(migration_index, session_state),
    )
    advices = linter.lint(MockPrompts({}), path)
    for advice in advices:
        print(repr(advice))
