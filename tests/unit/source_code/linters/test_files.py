from pathlib import Path
from unittest.mock import Mock, create_autospec

import pytest
from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.ucx.source_code.base import CurrentSessionState, LocatedAdvice, Advice
from databricks.labs.ucx.source_code.graph import DependencyResolver, SourceContainer
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, NotebookLoader
from databricks.labs.ucx.source_code.notebooks.migrator import NotebookMigrator
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from databricks.labs.ucx.source_code.known import KnownList

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.linters.files import (
    LocalFileMigrator,
    FileLoader,
    LocalCodeLinter,
    FolderLoader,
    ImportFileResolver,
    Folder,
)
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from tests.unit import locate_site_packages, _samples_path


def test_notebook_migrator_ignores_unsupported_extensions():
    languages = LinterContext(MigrationIndex([]))
    migrator = NotebookMigrator(languages)
    path = Path('unsupported.ext')
    assert not migrator.apply(path)


def test_file_migrator_fix_ignores_unsupported_extensions():
    languages = LinterContext(MigrationIndex([]))
    migrator = LocalFileMigrator(lambda: languages)
    path = Path('unsupported.ext')
    assert not migrator.apply(path)


def test_file_migrator_fix_ignores_unsupported_language():
    languages = LinterContext(MigrationIndex([]))
    migrator = LocalFileMigrator(lambda: languages)
    migrator._extensions[".py"] = None  # pylint: disable=protected-access
    path = Path('unsupported.py')
    assert not migrator.apply(path)


def test_file_migrator_fix_reads_supported_extensions(migration_index):
    languages = LinterContext(migration_index)
    migrator = LocalFileMigrator(lambda: languages)
    path = Path(__file__)
    assert not migrator.apply(path)


def test_file_migrator_supported_language_no_diagnostics():
    languages = create_autospec(LinterContext)
    languages.linter(Language.PYTHON).lint.return_value = []
    migrator = LocalFileMigrator(lambda: languages)
    path = Path(__file__)
    migrator.apply(path)
    languages.fixer.assert_not_called()


def test_notebook_migrator_supported_language_no_diagnostics(mock_path_lookup):
    languages = LinterContext(MigrationIndex([]))
    migrator = NotebookMigrator(languages)
    path = mock_path_lookup.resolve(Path("root1.run.py"))
    assert not migrator.apply(path)


def test_migrator_supported_language_no_fixer():
    languages = create_autospec(LinterContext)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer.return_value = None
    migrator = LocalFileMigrator(lambda: languages)
    path = Path(__file__)
    migrator.apply(path)
    languages.fixer.assert_called_once_with(Language.PYTHON, 'some-code')


def test_migrator_supported_language_with_fixer(tmpdir):
    languages = create_autospec(LinterContext)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer(Language.PYTHON, 'some-code').apply.return_value = "Hi there!"
    migrator = LocalFileMigrator(lambda: languages)
    path = Path(tmpdir, 'any.py')
    path.write_text("import tempfile", encoding='utf-8')
    migrator.apply(path)
    assert path.read_text("utf-8") == "Hi there!"


def test_migrator_walks_directory():
    languages = create_autospec(LinterContext)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer.return_value = None
    migrator = LocalFileMigrator(lambda: languages)
    path = Path(__file__).parent
    migrator.apply(path)
    languages.fixer.assert_called_with(Language.PYTHON, 'some-code')
    assert languages.fixer.call_count > 1


@pytest.fixture()
def local_code_linter(mock_path_lookup, migration_index):
    file_loader = FileLoader()
    folder_loader = FolderLoader(file_loader)
    allow_list = KnownList()
    pip_resolver = PythonLibraryResolver(allow_list)
    session_state = CurrentSessionState()
    resolver = DependencyResolver(
        pip_resolver,
        NotebookResolver(NotebookLoader()),
        ImportFileResolver(file_loader, allow_list),
        mock_path_lookup,
    )
    return LocalCodeLinter(
        file_loader, folder_loader, mock_path_lookup, session_state, resolver, lambda: LinterContext(migration_index)
    )


def test_linter_walks_directory(mock_path_lookup, local_code_linter):
    mock_path_lookup.append_path(Path(_samples_path(SourceContainer)))
    path = Path(__file__).parent / "../samples" / "simulate-sys-path"
    paths: set[Path] = set()
    advices = list(local_code_linter.lint_path(path, paths))
    assert len(paths) > 10
    assert not advices


def test_linter_lints_children_in_context(mock_path_lookup, local_code_linter):
    mock_path_lookup.append_path(Path(_samples_path(SourceContainer)))
    path = Path(__file__).parent.parent / "samples" / "parent-child-context"
    paths: set[Path] = set()
    advices = list(local_code_linter.lint_path(path, paths))
    assert len(paths) == 3
    assert advices == [
        LocatedAdvice(
            advice=Advice(
                code='default-format-changed-in-dbr8',
                message='The default format changed in Databricks Runtime 8.0, from Parquet to Delta',
                start_line=3,
                start_col=0,
                end_line=3,
                end_col=33,
            ),
            path=path / "child.py",
        )
    ]


def test_triple_dot_import():
    file_resolver = ImportFileResolver(FileLoader(), KnownList())
    path_lookup = create_autospec(PathLookup)
    path_lookup.cwd.as_posix.return_value = '/some/path/to/folder'
    path_lookup.resolve.return_value = Path('/some/path/foo.py')

    maybe = file_resolver.resolve_import(path_lookup, "...foo")
    assert not maybe.problems
    assert maybe.dependency.path == Path('/some/path/foo.py')
    path_lookup.resolve.assert_called_once_with(Path('/some/path/to/folder/../../foo.py'))


def test_single_dot_import():
    file_resolver = ImportFileResolver(FileLoader(), KnownList())
    path_lookup = create_autospec(PathLookup)
    path_lookup.cwd.as_posix.return_value = '/some/path/to/folder'
    path_lookup.resolve.return_value = Path('/some/path/to/folder/foo.py')

    maybe = file_resolver.resolve_import(path_lookup, ".foo")
    assert not maybe.problems
    assert maybe.dependency.path == Path('/some/path/to/folder/foo.py')
    path_lookup.resolve.assert_called_once_with(Path('/some/path/to/folder/foo.py'))


def test_folder_has_repr():
    file_loader = FileLoader()
    folder = Folder(Path("test"), file_loader, FolderLoader(file_loader))
    assert len(repr(folder)) > 0


site_packages = locate_site_packages()


@pytest.mark.skip("Manual testing for troubleshooting")
@pytest.mark.parametrize(
    "path", [Path("/Users/eric.vergnaud/development/ucx/.venv/lib/python3.10/site-packages/spacy/pipe_analysis.py")]
)
def test_known_issues(path: Path, migration_index):
    file_loader = FileLoader()
    folder_loader = FolderLoader(file_loader)
    path_lookup = PathLookup.from_sys_path(Path.cwd())
    session_state = CurrentSessionState()
    allow_list = KnownList()
    notebook_resolver = NotebookResolver(NotebookLoader())
    import_resolver = ImportFileResolver(file_loader, allow_list)
    pip_resolver = PythonLibraryResolver(allow_list)
    resolver = DependencyResolver(pip_resolver, notebook_resolver, import_resolver, path_lookup)
    linter = LocalCodeLinter(
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
