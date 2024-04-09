from typing import BinaryIO
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, Language, ObjectInfo, ObjectType

from databricks.labs.ucx.hive_metastore.table_migrate import MigrationIndex
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook import Notebook
from databricks.labs.ucx.source_code.notebook_migrator import NotebookMigrator
from tests.unit import _load_sources


def test_apply_invalid_object_fails():
    ws = create_autospec(WorkspaceClient)
    languages = create_autospec(Languages)
    migrator = NotebookMigrator(ws, languages)
    object_info = ObjectInfo(language=Language.PYTHON)
    assert not migrator.apply(object_info)


def test_revert_invalid_object_fails():
    ws = create_autospec(WorkspaceClient)
    languages = create_autospec(Languages)
    migrator = NotebookMigrator(ws, languages)
    object_info = ObjectInfo(language=Language.PYTHON)
    assert not migrator.revert(object_info)


def test_revert_restores_original_code():
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b'original_code'
    languages = create_autospec(Languages)
    migrator = NotebookMigrator(ws, languages)
    object_info = ObjectInfo(path='path', language=Language.PYTHON)
    migrator.revert(object_info)
    ws.workspace.download.assert_called_with('path.bak', format=ExportFormat.SOURCE)
    ws.workspace.upload.assert_called_with('path', b'original_code')


def test_apply_returns_false_when_language_not_supported():
    notebook_code = """# Databricks notebook source
# MAGIC %r
# // original code
"""
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = notebook_code.encode("utf-8")
    languages = create_autospec(Languages)
    languages.is_supported.return_value = False
    migrator = NotebookMigrator(ws, languages)
    object_info = ObjectInfo(path='path', language=Language.R, object_type=ObjectType.NOTEBOOK)
    result = migrator.apply(object_info)
    assert not result


def test_apply_returns_false_when_no_fixes_applied():
    notebook_code = """# Databricks notebook source
# original code
"""
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = notebook_code.encode("utf-8")
    languages = create_autospec(Languages)
    languages.is_supported.return_value = True
    languages.apply_fixes.return_value = "# original code"  # cell code
    migrator = NotebookMigrator(ws, languages)
    object_info = ObjectInfo(path='path', language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    assert not migrator.apply(object_info)


def test_apply_returns_true_and_changes_code_when_fixes_applied():
    original_code = """# Databricks notebook source
# original code
"""
    migrated_cell_code = '# migrated code'
    migrated_code = """# Databricks notebook source
# migrated code
"""
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = original_code.encode("utf-8")
    languages = create_autospec(Languages)
    languages.is_supported.return_value = True
    languages.apply_fixes.return_value = migrated_cell_code
    migrator = NotebookMigrator(ws, languages)
    object_info = ObjectInfo(path='path', language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    assert migrator.apply(object_info)
    ws.workspace.upload.assert_any_call('path.bak', original_code.encode("utf-8"))
    ws.workspace.upload.assert_any_call('path', migrated_code.encode("utf-8"))


def test_apply_visits_dependencies():
    paths = ["root3.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(Notebook, *paths)))
    visited: dict[str, bool] = {}

    # can't remove **kwargs because it receives format=xxx
    # pylint: disable=unused-argument
    def download_side_effect(*args, **kwargs):
        filename = args[0]
        if filename.startswith('./'):
            filename = filename[2:]
        visited[filename] = True
        result = create_autospec(BinaryIO)
        result.__enter__.return_value.read.return_value = sources[filename].encode("utf-8")
        return result

    def list_side_effect(*args):
        path = args[0]
        return [ObjectInfo(path=path, language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)]

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = download_side_effect
    ws.workspace.list.side_effect = list_side_effect
    migrator = NotebookMigrator(ws, Languages(create_autospec(MigrationIndex)))
    object_info = ObjectInfo(path="root3.run.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    migrator.apply(object_info)
    assert len(visited) == len(paths)


def test_apply_fails_with_unfound_dependency():
    paths = ["root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: dict[str, str] = dict(zip(paths, _load_sources(Notebook, *paths)))

    # can't remove **kwargs because it receives format=xxx
    # pylint: disable=unused-argument
    def download_side_effect(*args, **kwargs):
        filename = args[0]
        if filename.startswith('./'):
            filename = filename[2:]
        result = create_autospec(BinaryIO)
        result.__enter__.return_value.read.return_value = sources[filename].encode("utf-8")
        return result

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.side_effect = download_side_effect
    ws.workspace.list.return_value = []
    migrator = NotebookMigrator(ws, Languages(create_autospec(MigrationIndex)))
    object_info = ObjectInfo(path="root1.run.py.txt", language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    with pytest.raises(ValueError):
        migrator.apply(object_info)
