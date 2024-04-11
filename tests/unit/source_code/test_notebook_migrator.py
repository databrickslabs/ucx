from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, Language, ObjectInfo, ObjectType

from databricks.labs.ucx.source_code.dependencies import DependencyLoader
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook import Notebook
from databricks.labs.ucx.source_code.notebook_migrator import NotebookMigrator


def test_apply_invalid_object_fails():
    ws = create_autospec(WorkspaceClient)
    languages = create_autospec(Languages)
    migrator = NotebookMigrator(ws, languages, DependencyLoader(ws))
    object_info = ObjectInfo(language=Language.PYTHON)
    assert not migrator.apply(object_info)


def test_revert_invalid_object_fails():
    ws = create_autospec(WorkspaceClient)
    languages = create_autospec(Languages)
    migrator = NotebookMigrator(ws, languages, DependencyLoader(ws))
    object_info = ObjectInfo(language=Language.PYTHON)
    assert not migrator.revert(object_info)


def test_revert_restores_original_code():
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value.__enter__.return_value.read.return_value = b'original_code'
    languages = create_autospec(Languages)
    migrator = NotebookMigrator(ws, languages, DependencyLoader(ws))
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
    loader = create_autospec(DependencyLoader)
    loader.load_dependency.return_value = Notebook.parse('path', notebook_code, Language.R)
    migrator = NotebookMigrator(ws, languages, loader)
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
    loader = create_autospec(DependencyLoader)
    loader.load_dependency.return_value = Notebook.parse('path', notebook_code, Language.R)
    migrator = NotebookMigrator(ws, languages, loader)
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
    loader = create_autospec(DependencyLoader)
    loader.load_dependency.return_value = Notebook.parse('path', original_code, Language.R)
    migrator = NotebookMigrator(ws, languages, loader)
    object_info = ObjectInfo(path='path', language=Language.PYTHON, object_type=ObjectType.NOTEBOOK)
    assert migrator.apply(object_info)
    ws.workspace.upload.assert_any_call('path.bak', original_code.encode("utf-8"))
    ws.workspace.upload.assert_any_call('path', migrated_code.encode("utf-8"))
