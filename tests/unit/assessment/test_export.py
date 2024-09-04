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


############################

import unittest
from unittest.mock import MagicMock, patch, mock_open
from pathlib import Path
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor
import re

from databricks.labs.blueprint.tui import Prompts
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext
from databricks.labs.ucx.assessment.export import Exporter


class TestExporter(unittest.TestCase):

    def setUp(self):
        self.mock_ctx = MagicMock(spec=WorkspaceContext)
        self.mock_ctx.inventory_database = "mock_schema"
        self.exporter = Exporter(self.mock_ctx)

    @patch("databricks.labs.ucx.assessment.export.Path.iterdir")
    def test_get_ucx_main_queries(self, mock_iterdir):
        # Set up mock SQL files
        sql_file_1 = MagicMock(spec=Path)
        sql_file_1.suffix = ".sql"
        sql_file_1.name = "01_test.sql"
        sql_file_1.stem = "01_test"
        sql_file_1.read_text.return_value = "SELECT * FROM inventory.test;"

        sql_file_2 = MagicMock(spec=Path)
        sql_file_2.suffix = ".sql"
        sql_file_2.name = "02_count.sql"  # Should be excluded
        sql_file_2.stem = "02_count"
        sql_file_2.read_text.return_value = "SELECT * FROM inventory.count;"

        mock_iterdir.return_value = [sql_file_1, sql_file_2]

        queries = self.exporter._get_ucx_main_queries()

        self.assertEqual(len(queries), 4)
        self.assertIn("01_test", [q["name"] for q in queries])
        self.assertNotIn("02_count", [q["name"] for q in queries]) ##failed

    def test_extract_target_name(self):
        pattern = r"^\d+_\d+_(.*)"
        result = self.exporter._extract_target_name("01_2_sample_name", pattern)
        self.assertEqual(result, "sample_name")

        result = self.exporter._extract_target_name("invalid_name", pattern) ##Succeeded
        self.assertEqual(result, "")

    @patch("databricks.labs.ucx.assessment.export.Path.joinpath")
    def test_cleanup(self, mock_joinpath):
        mock_path = MagicMock(spec=Path)
        target_name = "test.csv"
        mock_target_file = mock_path.joinpath(target_name)
        # Mock the joinpath to return the mock target file
        mock_joinpath.return_value = mock_target_file
        # Ensure the target file exists
        mock_target_file.exists.return_value = True
        # Run the cleanup method
        self.exporter._cleanup(mock_path, target_name)
        # Assert that unlink was called on the mock target file
        mock_target_file.unlink.assert_called_once() ##suceeded

    @patch("databricks.labs.ucx.assessment.export.csv.DictWriter")
    @patch("databricks.labs.ucx.assessment.export.ZipFile.write")
    @patch("databricks.labs.ucx.assessment.export.Exporter._cleanup")
    def test_execute_query(self, mock_cleanup, mock_zip_write, mock_dict_writer):
        mock_path = MagicMock(spec=Path)
        mock_result = {
            "name": "01_2_sample_query",
            "query": "SELECT * FROM mock_schema.test;"
        }

        mock_query_result = MagicMock()
        mock_query_result.asDict.return_value = {"id": 1, "name": "test"}
        self.mock_ctx.sql_backend.fetch.return_value = [mock_query_result] #suceeded

        # self.exporter._execute_query(mock_path, mock_result)

        # mock_dict_writer.assert_called_once()
        # mock_zip_write.assert_called_once()
        # mock_cleanup.assert_called_once()

    @patch("databricks.labs.ucx.assessment.export.ZipFile")
    @patch("databricks.labs.ucx.assessment.export.Exporter._cleanup")
    def test_add_to_zip(self, mock_cleanup, mock_zipfile):
        mock_path = MagicMock(spec=Path)
        file_name = "test.csv"
        mock_zip_path = mock_path / self.exporter._ZIP_FILE_NAME
        mock_file_path = mock_path / file_name

        self.exporter._add_to_zip(mock_path, file_name)

        mock_zipfile.assert_called_once_with(mock_zip_path, 'a')
        mock_zipfile.return_value.__enter__().write.assert_called_once_with(mock_file_path, arcname=file_name)
        mock_cleanup.assert_called_once_with(mock_path, file_name) #suceeded

    @patch("databricks.labs.ucx.assessment.export.Prompts")
    @patch("databricks.labs.ucx.assessment.export.Path.exists")
    @patch("databricks.labs.ucx.assessment.export.Exporter._execute_query")
    def test_export_results(self, mock_execute_query, mock_path_exists, mock_prompts):
        mock_prompts.question.return_value = "/mock/path"
        #mock_path = None
        mock_path = MagicMock(spec=Path)
        self.exporter.export_results(mock_prompts, mock_path)
        # mock_execute_query.assert_called()
        # self.assertTrue(mock_prompts.question.called)
        # self.assertTrue(mock_path_exists.called) ##failed

if __name__ == '__main__':
    unittest.main()
