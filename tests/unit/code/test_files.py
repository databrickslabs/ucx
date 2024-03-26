from pathlib import Path
from unittest.mock import Mock, create_autospec

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.code.files import Files
from databricks.labs.ucx.code.languages import Languages


def test_files_fix_ignores_unsupported_extensions():
    languages = create_autospec(Languages)
    files = Files(languages)
    path = Path('unsupported.ext')
    assert not files.apply(path)


def test_files_fix_ignores_unsupported_language():
    languages = create_autospec(Languages)
    files = Files(languages)
    files._extensions[".py"] = None
    path = Path('unsupported.py')
    assert not files.apply(path)


def test_files_fix_reads_supported_extensions():
    languages = create_autospec(Languages)
    files = Files(languages)
    path = Path(__file__)
    assert not files.apply(path)


def test_files_supported_language_no_diagnostics():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = []
    files = Files(languages)
    path = Path(__file__)
    files.apply(path)
    languages.fixer.assert_not_called()


def test_files_supported_language_no_fixer():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer.return_value = None
    files = Files(languages)
    path = Path(__file__)
    files.apply(path)
    languages.fixer.assert_called_once_with(Language.PYTHON, 'some-code')


def test_files_walks_directory():
    languages = create_autospec(Languages)
    languages.linter(Language.PYTHON).lint.return_value = [Mock(code='some-code')]
    languages.fixer.return_value = None
    files = Files(languages)
    path = Path(__file__).parent
    files.apply(path)
    languages.fixer.assert_called_with(Language.PYTHON, 'some-code')
    assert languages.fixer.call_count > 1
