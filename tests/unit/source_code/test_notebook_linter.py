from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.notebooks.sources import NotebookLinter

index = TableMigrationIndex([])


def test_notebook_linter_name(mock_path_lookup) -> None:
    source = """-- Databricks notebook source"""
    linter = NotebookLinter.from_source(index, mock_path_lookup, CurrentSessionState(), source, Language.SQL)
    assert linter.name() == "notebook-linter"
