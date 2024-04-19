import tempfile
from pathlib import Path
from unittest.mock import Mock, create_autospec

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.dependencies import DependencyResolver
from databricks.labs.ucx.source_code.files import LocalFileMigrator
from databricks.labs.ucx.source_code.languages import Languages


def test_files_fix_ignores_unsupported_extensions():
    languages = create_autospec(Languages)
    resolver = create_autospec(DependencyResolver)
    files = LocalFileMigrator(languages, resolver)
    path = Path('unsupported.ext')
    assert not files.apply(path)


def test_files_fix_ignores_unsupported_language():
    languages = create_autospec(Languages)
    resolver = create_autospec(DependencyResolver)
    files = LocalFileMigrator(languages, resolver)
    files._extensions[".py"] = None  # pylint: disable=protected-access
    path = Path('unsupported.py')
    assert not files.apply(path)


def test_files_fix_reads_supported_extensions():
    languages = create_autospec(Languages)
    resolver = create_autospec(DependencyResolver)
    files = LocalFileMigrator(languages, resolver)
    path = Path(__file__)
    assert not files.apply(path)


def test_files_supported_language_no_diagnostics():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = []
    resolver = create_autospec(DependencyResolver)
    files = LocalFileMigrator(languages, resolver)
    path = Path(__file__)
    files.apply(path)
    languages.fixer.assert_not_called()


def test_files_supported_language_no_fixer():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer.return_value = None
    resolver = create_autospec(DependencyResolver)
    files = LocalFileMigrator(languages, resolver)
    path = Path(__file__)
    files.apply(path)
    languages.fixer.assert_called_once_with(Language.PYTHON, 'some-code')


def test_files_supported_language_with_fixer():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer(Language.PYTHON, 'some-code').apply.return_value = "Hi there!"
    resolver = create_autospec(DependencyResolver)
    files = LocalFileMigrator(languages, resolver)
    with tempfile.NamedTemporaryFile(mode="w+t", suffix=".py") as file:
        file.writelines(["import tempfile"])
        path = Path(file.name)
        files.apply(path)
        assert file.readline() == "Hi there!"


def test_files_walks_directory():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer.return_value = None
    resolver = create_autospec(DependencyResolver)
    files = LocalFileMigrator(languages, resolver)
    path = Path(__file__).parent
    files.apply(path)
    languages.fixer.assert_called_with(Language.PYTHON, 'some-code')
    assert languages.fixer.call_count > 1
