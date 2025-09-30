"""Unit tests for WorkspaceTablesLinter."""

from unittest.mock import create_autospec
from databricks.sdk.service.workspace import Language, ObjectType
from databricks.labs.ucx.source_code.linters.workspace import WorkspaceTablesLinter
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler
from databricks.labs.ucx.workspace_access.generic import WorkspaceObjectInfo

from databricks.labs.ucx.workspace_access.listing import WorkspaceListing


class TestWorkspaceTablesLinter:
    """Test cases for WorkspaceTablesLinter."""

    def test_scan_workspace_for_tables_empty_and_none_paths(self, ws, tmp_path, mock_path_lookup, mock_backend):
        """Test successful workspace scanning with table detection."""
        # Create mock dependencies
        mock_used_tables_crawler = create_autospec(UsedTablesCrawler)
        mock_workspace_listing = create_autospec(WorkspaceListing)

        # Mock the WorkspaceListing to return empty results
        mock_workspace_listing.walk.return_value = []  # Empty workspace

        # Create the linter instance
        linter = WorkspaceTablesLinter(
            ws=ws,
            sql_backend=mock_backend,
            inventory_database="test_db",
            path_lookup=mock_path_lookup,
            used_tables_crawler=mock_used_tables_crawler,
        )

        # Call the method under test with tmp_path
        linter.scan_workspace_for_tables([str(tmp_path)])
        # Call the method under test with empty paths
        linter.scan_workspace_for_tables([])
        # Call the method under test with None paths
        linter.scan_workspace_for_tables(None)

        mock_used_tables_crawler.dump_all.assert_not_called()

    def test_scan_workspace_for_python_file(self, ws, tmp_path, mock_path_lookup, mock_backend):
        """Test successful workspace scanning with table detection."""
        # Create mock dependencies
        mock_used_tables_crawler = create_autospec(UsedTablesCrawler)
        mock_workspace_listing = create_autospec(WorkspaceListing)

        # Create a mock Python file in the temporary directory
        python_file_path = tmp_path / "test_script.py"
        python_file_path.write_text(
            """import pandas as pd
            import sqlite3
            conn = sqlite3.connect('example.db')
            df = pd.read_sql_query('SELECT * FROM test_table', conn)
            """
        )

        # Mock the WorkspaceListing to return the mock Python file
        mock_workspace_listing.walk.return_value = [
            WorkspaceObjectInfo(
                object_id="1",
                object_type=ObjectType.NOTEBOOK,
                language=Language.PYTHON,
                path=str(python_file_path),
            )
        ]

        # Create the linter instance
        linter = WorkspaceTablesLinter(
            ws=ws,
            sql_backend=mock_backend,
            inventory_database="test_db",
            path_lookup=mock_path_lookup,
            used_tables_crawler=mock_used_tables_crawler,
        )

        linter.scan_workspace_for_tables([str(python_file_path)])
        mock_used_tables_crawler.dump_all.assert_not_called()
