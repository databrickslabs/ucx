from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, Language, ObjectInfo

from databricks.labs.ucx.code.languages import Languages
from databricks.labs.ucx.code.notebooks import Notebooks


def test_notebooks_revert_restores_original_code():
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b'original_code'
    languages = create_autospec(Languages)
    notebooks = Notebooks(ws, languages)
    object_info = ObjectInfo(path='path', language=Language.PYTHON)
    notebooks.revert(object_info)
    ws.workspace.download.assert_called_with('path.bak', format=ExportFormat.SOURCE)
    ws.workspace.upload.assert_called_with('path', b'original_code')


def test_apply_returns_false_when_language_not_supported():
    ws = create_autospec(WorkspaceClient)
    languages = create_autospec(Languages)
    languages.is_supported.return_value = False
    notebooks = Notebooks(ws, languages)
    object_info = ObjectInfo(path='path', language=Language.R)
    result = notebooks.apply(object_info)
    assert not result


def test_apply_returns_false_when_no_fixes_applied():
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b'original_code'
    languages = create_autospec(Languages)
    languages.is_supported.return_value = True
    languages.apply_fixes.return_value = 'original_code'
    notebooks = Notebooks(ws, languages)
    object_info = ObjectInfo(path='path', language=Language.PYTHON)
    assert not notebooks.apply(object_info)


def test_apply_returns_true_and_changes_code_when_fixes_applied():
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b'original_code'
    languages = create_autospec(Languages)
    languages.is_supported.return_value = True
    languages.apply_fixes.return_value = 'new_code'
    notebooks = Notebooks(ws, languages)
    object_info = ObjectInfo(path='path', language=Language.PYTHON)
    assert notebooks.apply(object_info)
    ws.workspace.upload.assert_any_call('path.bak', 'original_code'.encode("utf-8"))
    ws.workspace.upload.assert_any_call('path', 'new_code'.encode("utf-8"))
