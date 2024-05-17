import tempfile
from pathlib import Path
from unittest.mock import Mock, create_autospec

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.files import LocalFilesMigrator, LocalFileResolver, FileLoader
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.path_lookup import PathLookup


def test_files_fix_ignores_unsupported_extensions():
    languages = Languages(MigrationIndex([]))
    files = LocalFilesMigrator(languages)
    path = Path('unsupported.ext')
    assert not files.apply(path)


def test_files_fix_ignores_unsupported_language():
    languages = Languages(MigrationIndex([]))
    files = LocalFilesMigrator(languages)
    files._extensions[".py"] = None  # pylint: disable=protected-access
    path = Path('unsupported.py')
    assert not files.apply(path)


def test_files_fix_reads_supported_extensions():
    languages = Languages(MigrationIndex([]))
    files = LocalFilesMigrator(languages)
    path = Path(__file__)
    assert not files.apply(path)


def test_files_supported_language_no_diagnostics():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = []
    files = LocalFilesMigrator(languages)
    path = Path(__file__)
    files.apply(path)
    languages.fixer.assert_not_called()


def test_files_supported_language_no_fixer():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer.return_value = None
    files = LocalFilesMigrator(languages)
    path = Path(__file__)
    files.apply(path)
    languages.fixer.assert_called_once_with(Language.PYTHON, 'some-code')


def test_files_supported_language_with_fixer():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer(Language.PYTHON, 'some-code').apply.return_value = "Hi there!"
    files = LocalFilesMigrator(languages)
    with tempfile.NamedTemporaryFile(mode="w+t", suffix=".py") as file:
        file.writelines(["import tempfile"])
        path = Path(file.name)
        files.apply(path)
        assert file.readline() == "Hi there!"


def test_files_walks_directory():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer.return_value = None
    files = LocalFilesMigrator(languages)
    path = Path(__file__).parent
    files.apply(path)
    languages.fixer.assert_called_with(Language.PYTHON, 'some-code')
    assert languages.fixer.call_count > 1


def test_triple_dot_import():
    file_loader = FileLoader()
    file_resolver = LocalFileResolver(file_loader)
    path_lookup = create_autospec(PathLookup)
    path_lookup.cwd.as_posix.return_value = '/some/path/to/folder'
    path_lookup.resolve.return_value = Path('/some/path/foo.py')

    maybe = file_resolver.resolve_import(path_lookup, "...foo")
    assert not maybe.problems
    assert maybe.dependency.path == Path('/some/path/foo.py')
    path_lookup.resolve.assert_called_once_with(Path('/some/path/to/folder/../../foo.py'))


def test_single_dot_import():
    file_loader = FileLoader()
    file_resolver = LocalFileResolver(file_loader)
    path_lookup = create_autospec(PathLookup)
    path_lookup.cwd.as_posix.return_value = '/some/path/to/folder'
    path_lookup.resolve.return_value = Path('/some/path/to/folder/foo.py')

    maybe = file_resolver.resolve_import(path_lookup, ".foo")
    assert not maybe.problems
    assert maybe.dependency.path == Path('/some/path/to/folder/foo.py')
    path_lookup.resolve.assert_called_once_with(Path('/some/path/to/folder/foo.py'))
