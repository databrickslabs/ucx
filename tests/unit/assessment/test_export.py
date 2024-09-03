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

    _UCX_MAIN_QUERIES_PATH = "src/databricks/labs/ucx/queries/assessment/main"
    exporter._UCX_MAIN_QUERIES_PATH = _UCX_MAIN_QUERIES_PATH
    # Patch the Path.iterdir method to simulate the presence of SQL files
    #with patch.object(Path, 'iterdir', return_value=[Path("01_inventory.sql"), Path("02_count_inventory.sql")]), \
    #    patch('pathlib.Path.read_text', return_value="SELECT * FROM inventory;"):
    queries = exporter._get_ucx_main_queries()

    # Assertions to verify the expected queries - test for total queries in addition to 3 xamples
    # Define the expected names and query
    expected_names = {
        "00_1_count_table_failures",
        "40_1_pipelines",
        "05_0_object_readiness"
    }

    expected_query = "/* --title 'Table Types' --filter name --width 6 */\nSELECT\n  CONCAT(tables.`database`, '.', tables.name) AS name,\n  object_type AS type,\n  table_format AS format,\n  CASE\n    WHEN STARTSWITH(location, 'dbfs:/mnt')\n    THEN 'DBFS MOUNT'\n    WHEN STARTSWITH(location, '/dbfs/mnt')\n    THEN 'DBFS MOUNT'\n    WHEN STARTSWITH(location, 'dbfs:/databricks-datasets')\n    THEN 'Databricks Demo Dataset'\n    WHEN STARTSWITH(location, '/dbfs/databricks-datasets')\n    THEN 'Databricks Demo Dataset'\n    WHEN STARTSWITH(location, 'dbfs:/')\n    THEN 'DBFS ROOT'\n    WHEN STARTSWITH(location, '/dbfs/')\n    THEN 'DBFS ROOT'\n    WHEN STARTSWITH(location, 'wasb')\n    THEN 'UNSUPPORTED'\n    WHEN STARTSWITH(location, 'adl')\n    THEN 'UNSUPPORTED'\n    ELSE 'EXTERNAL'\n  END AS storage,\n  IF(table_format = 'DELTA', 'Yes', 'No') AS is_delta,\n  location,\n  CASE\n    WHEN size_in_bytes IS NULL\n    THEN 'Non DBFS Root'\n    WHEN size_in_bytes > 10000000000000000\n    THEN 'SIZE OUT OF RANGE'\n    WHEN size_in_bytes < 100\n    THEN CONCAT(CAST(size_in_bytes AS STRING), ' Bytes')\n    WHEN size_in_bytes < 100000\n    THEN CONCAT(CAST(ROUND(size_in_bytes / 1024, 2) AS STRING), 'KB')\n    WHEN size_in_bytes < 100000000\n    THEN CONCAT(CAST(ROUND(size_in_bytes / 1024 / 1024, 2) AS STRING), 'MB')\n    WHEN size_in_bytes < 100000000000\n    THEN CONCAT(CAST(ROUND(size_in_bytes / 1024 / 1024 / 1024, 2) AS STRING), 'GB')\n    ELSE CONCAT(CAST(ROUND(size_in_bytes / 1024 / 1024 / 1024 / 1024, 2) AS STRING), 'TB')\n  END AS table_size\nFROM test_db.tables AS tables\nLEFT OUTER JOIN test_db.table_size AS table_size\n  ON tables.catalog = table_size.catalog AND tables.database = table_size.database AND tables.name = table_size.name"

    # Initialize flags to check if all conditions are met
    name_flags = {name: False for name in expected_names}
    query_flag = False

    # Check the length of the list first
    assert len(queries) == 28, "Should contain 28 queries for counts and content"

    # Loop through the queries only once
    for query in queries:
        if query['name'] in name_flags:
            name_flags[query['name']] = True
        if query['query'] == expected_query:
            query_flag = True
        # If all flags are set, no need to continue the loop
        if all(name_flags.values()) and query_flag:
            break

    # Assert all name flags are True
    for name, flag in name_flags.items():
        assert flag, f"'name' with value '{name}' not found in queries"

    # Assert the query flag is True
    assert query_flag, "'query' with the expected value not found in queries"

    #assert "inventory" not in queries[1]['query'], "The word 'inventory' should be replaced in the query"


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
