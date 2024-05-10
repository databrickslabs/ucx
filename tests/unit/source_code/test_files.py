import tempfile
from pathlib import Path
from unittest.mock import Mock, create_autospec

import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.files import LocalFile, LocalFileMigrator, LocalFileResolver, FileLoader
from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.path_lookup import PathLookup


def test_local_file_path():
    """Return the right local file path"""
    local_file = LocalFile(Path("test"), "code", Language.PYTHON)
    assert local_file.path == Path("test")


def test_local_file_equal_to_itself():
    """Local file equals itself"""
    local_file = LocalFile(Path("test"), "code", Language.PYTHON)
    assert local_file == local_file


def test_local_file_equal_to_its_path():
    """Local file equals itself"""
    local_file = LocalFile(Path("test"), "code", Language.PYTHON)
    assert local_file == Path("test")


def test_files_fix_ignores_unsupported_extensions():
    languages = Languages(MigrationIndex([]))
    files = LocalFileMigrator(languages)
    path = Path('unsupported.ext')
    assert not files.apply(path)


def test_files_fix_ignores_unsupported_language():
    languages = Languages(MigrationIndex([]))
    files = LocalFileMigrator(languages)
    files._extensions[".py"] = None  # pylint: disable=protected-access
    path = Path('unsupported.py')
    assert not files.apply(path)


def test_files_fix_reads_supported_extensions():
    languages = Languages(MigrationIndex([]))
    files = LocalFileMigrator(languages)
    path = Path(__file__)
    assert not files.apply(path)


def test_files_supported_language_no_diagnostics():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = []
    files = LocalFileMigrator(languages)
    path = Path(__file__)
    files.apply(path)
    languages.fixer.assert_not_called()


def test_files_supported_language_no_fixer():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer.return_value = None
    files = LocalFileMigrator(languages)
    path = Path(__file__)
    files.apply(path)
    languages.fixer.assert_called_once_with(Language.PYTHON, 'some-code')


def test_files_supported_language_with_fixer():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer(Language.PYTHON, 'some-code').apply.return_value = "Hi there!"
    files = LocalFileMigrator(languages)
    with tempfile.NamedTemporaryFile(mode="w+t", suffix=".py") as file:
        file.writelines(["import tempfile"])
        path = Path(file.name)
        files.apply(path)
        assert file.readline() == "Hi there!"


def test_files_walks_directory():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer.return_value = None
    files = LocalFileMigrator(languages)
    path = Path(__file__).parent
    files.apply(path)
    languages.fixer.assert_called_with(Language.PYTHON, 'some-code')
    assert languages.fixer.call_count > 1


def test_file_loader_exists():
    """File exists"""
    file_loader = FileLoader()
    assert file_loader.exists(Path(__file__))


def test_file_loader_not_exists():
    """File does not exists"""
    file_loader = FileLoader()
    assert not file_loader.exists(Path("/some/path/that/does/not/exist"))


@pytest.mark.parametrize("path", [Path("/some/path/that/does/not/exists"), Path("lib.py")])
def test_file_loader_load_dependency_calls_dependency_load(mock_path_lookup, path):
    """The file loader load dependency points to the dependency load"""
    file_loader = FileLoader()
    dependency = Dependency(file_loader, path)
    # TODO: Clarify if this is the right way to use it.
    # It reads a bit convoluted to pass the file_loader to the dependency first and then the dependency to the file.
    assert file_loader.load_dependency(mock_path_lookup, dependency) == dependency.load(mock_path_lookup)


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
