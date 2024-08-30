from unittest.mock import MagicMock, patch, mock_open
from pathlib import Path
from databricks.labs.ucx.assessment.export import Exporter
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext

from .. import mock_workspace_client

def test_get_ucx_main_queries():
    # Set up the mock workspace client
    ws = mock_workspace_client()
    sql_backend = MockBackend()

    # Mock the configuration and installation methods that might be invoked
    ctx = MagicMock(spec=WorkspaceContext)
    ctx.inventory_database = "test_db"

    # Initialize the Exporter with the mocked context
    exporter = Exporter(ctx)

    # Patch the Path.iterdir method to simulate the presence of SQL files
    with patch.object(Path, 'iterdir', return_value=[Path("01_inventory.sql"), Path("02_count_inventory.sql")]), \
        patch('pathlib.Path.read_text', return_value="SELECT * FROM inventory;"):
        queries = exporter._get_ucx_main_queries()

    # Assertions to verify the expected queries
    assert len(queries) == 4, "Should contain 3 hardcoded queries and 1 additional SQL file"
    assert queries[0]['name'] == "01_1_permissions"
    assert "inventory" not in queries[1]['query'], "The word 'inventory' should be replaced in the query"
    assert queries[-1]['name'] == "01_inventory", "The query name should match the file name without the extension"


def test_execute_query():
    # Set up the mock workspace client
    ws = MagicMock(spec=WorkspaceClient)
    ws.sql_backend = MockBackend()
    ws.inventory_database = "test_db"

    # Mock the configuration and installation methods that might be invoked
    ctx = MagicMock(spec=WorkspaceContext)
    ctx.sql_backend.fetch.return_value = [MagicMock(asDict=lambda: {"column1": "value1"})]
    ctx.inventory_database = "test_db"

    # Initialize the Exporter with the mocked context
    exporter = Exporter(ctx)

    # Define a mock query result
    mock_result = {
        "name": "01_1_permissions",
        "query": "SELECT * FROM test_db.permissions"
    }

    # Patch open to mock file writing and mock the fetch method
    with patch('builtins.open', new_callable=mock_open) as mock_file, \
        patch.object(exporter, '_add_to_zip') as mock_add_to_zip:
        # Call _execute_query
        exporter._execute_query(Path("/fake/dir"), mock_result)

        # Assertions to verify the expected behavior
        mock_file.assert_called_once_with('/fake/dir/permissions.csv', mode='w', newline='', encoding='utf-8')
        mock_add_to_zip.assert_called_once()
