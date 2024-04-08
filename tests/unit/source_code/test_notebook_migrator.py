from typing import BinaryIO
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, Language, ObjectInfo

from databricks.labs.ucx.hive_metastore.table_migrate import MigrationIndex
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook import Notebook
from databricks.labs.ucx.source_code.notebook_migrator import NotebookMigrator
from tests.unit import _load_sources


def test_revert_restores_original_code():
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b'original_code'
    languages = create_autospec(Languages)
    notebooks = NotebookMigrator(ws, languages)
    object_info = ObjectInfo(path='path', language=Language.PYTHON)
    notebooks.revert(object_info)
    ws.workspace.download.assert_called_with('path.bak', format=ExportFormat.SOURCE)
    ws.workspace.upload.assert_called_with('path', b'original_code')


def test_apply_returns_false_when_language_not_supported():
    ws = create_autospec(WorkspaceClient)
    languages = create_autospec(Languages)
    languages.is_supported.return_value = False
    notebooks = NotebookMigrator(ws, languages)
    object_info = ObjectInfo(path='path', language=Language.R)
    result = notebooks.apply(object_info)
    assert not result


def test_apply_returns_false_when_no_fixes_applied():
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b'original_code'
    languages = create_autospec(Languages)
    languages.is_supported.return_value = True
    languages.apply_fixes.return_value = 'original_code'
    notebooks = NotebookMigrator(ws, languages)
    object_info = ObjectInfo(path='path', language=Language.PYTHON)
    assert not notebooks.apply(object_info)


def test_apply_returns_true_and_changes_code_when_fixes_applied():
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b'original_code'
    languages = create_autospec(Languages)
    languages.is_supported.return_value = True
    languages.apply_fixes.return_value = 'new_code'
    migrator = NotebookMigrator(ws, languages)
    object_info = ObjectInfo(path='path', language=Language.PYTHON)
    assert migrator.apply(object_info)
    ws.workspace.upload.assert_any_call('path.bak', 'original_code'.encode("utf-8"))
    ws.workspace.upload.assert_any_call('path', 'new_code'.encode("utf-8"))


def test_apply_visits_dependencies():
    paths = ["root3.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = {k: v for k,v in zip(paths, _load_sources(Notebook, *paths))}
    visited: dict[str, bool] = {}

    def side_effect(*args, **kwargs):
        filename = args[0]
        visited[filename] = True
        result = create_autospec(BinaryIO)
        result.__enter__.return_value.read.return_value = sources[filename]
        return result
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = side_effect
    migrator = NotebookMigrator(ws, Languages(create_autospec(MigrationIndex)))
    object_info = ObjectInfo(path="root3.run.py.txt", language=Language.PYTHON)
    migrator.apply(object_info)
    assert len(visited) == len(paths)
