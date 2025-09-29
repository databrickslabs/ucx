"""Tests for workspace table scanning functionality."""

import pytest
from unittest.mock import Mock, patch
from databricks.sdk.service.workspace import ObjectType, Language

from databricks.labs.ucx.source_code.linters.workspace import WorkspaceTablesLinter
from databricks.labs.ucx.workspace_access.generic import WorkspaceObjectInfo
from databricks.labs.ucx.source_code.base import UsedTable, LineageAtom


@pytest.fixture
def workspace_linter():
    """Create a WorkspaceTablesLinter instance for testing."""
    ws = Mock()
    sql_backend = Mock()
    inventory_database = "test_inventory"
    path_lookup = Mock()
    used_tables_crawler = Mock()

    return WorkspaceTablesLinter(
        ws=ws,
        sql_backend=sql_backend,
        inventory_database=inventory_database,
        path_lookup=path_lookup,
        used_tables_crawler=used_tables_crawler,
        max_workers=2,
    )


def test_get_language_from_path(workspace_linter):
    """Test language detection from file paths."""
    assert workspace_linter._get_language_from_path("/test/file.py") == Language.PYTHON
    assert workspace_linter._get_language_from_path("/test/query.sql") == Language.SQL
    assert workspace_linter._get_language_from_path("/test/script.scala") == Language.SCALA
    assert workspace_linter._get_language_from_path("/test/analysis.r") == Language.R
    assert workspace_linter._get_language_from_path("/test/readme.md") is None


def test_error_handling(workspace_linter):
    """Test error handling during workspace scanning."""
    obj = WorkspaceObjectInfo(object_type="NOTEBOOK", object_id="123", path="/error/notebook.py", language="python")

    source_lineage = [LineageAtom(object_type="WORKSPACE_OBJECT", object_id="/error/notebook.py")]

    # Mock a download error
    with patch.object(workspace_linter._ws.workspace, 'download') as mock_download:
        mock_download.side_effect = Exception("Download failed")

        # Should handle error gracefully and return empty list
        tables = workspace_linter._extract_tables_from_notebook(obj, source_lineage)
        assert tables == []


def test_parallel_processing(workspace_linter):
    """Test that parallel processing is used for multiple objects."""
    objects = [
        WorkspaceObjectInfo(object_type="NOTEBOOK", object_id="1", path="/test1.py"),
        WorkspaceObjectInfo(object_type="NOTEBOOK", object_id="2", path="/test2.py"),
        WorkspaceObjectInfo(object_type="FILE", object_id="3", path="/test3.sql"),
    ]

    with patch('databricks.labs.blueprint.parallel.Threads.gather') as mock_gather:
        mock_gather.return_value = ([], [])  # No results, no errors

        workspace_linter._extract_tables_from_objects(objects)

        # Verify parallel execution was called
        mock_gather.assert_called_once()
        call_args = mock_gather.call_args
        assert call_args[0][0] == 'extracting tables from workspace objects'
        assert len(call_args[0][1]) == 3  # Three tasks for three objects
