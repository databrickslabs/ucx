import os
from pathlib import Path
import sys
import threading
from unittest.mock import patch, create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.tables import FasterTableScanCrawler
from databricks.labs.ucx.source_code.graph import BaseNotebookResolver, DependencyResolver
from databricks.labs.ucx.source_code.known import KnownList
from databricks.labs.ucx.source_code.linters.files import ImportFileResolver, FileLoader
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, NotebookLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.sdk import AccountClient
from databricks.sdk.config import Config

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver

from . import mock_workspace_client

pytest.register_assert_rewrite('databricks.labs.blueprint.installation')

# Lock to prevent concurrent execution of tests that patch the environment
_lock = threading.Lock()


@pytest.fixture()
def mock_installation() -> MockInstallation:
    return MockInstallation(
        {
            'config.yml': {
                'connect': {
                    'host': 'adb-9999999999999999.14.azuredatabricks.net',
                    'token': '...',
                },
                'inventory_database': 'ucx',
                'warehouse_id': 'abc',
            },
            'mapping.csv': [
                {
                    'catalog_name': 'catalog',
                    'dst_schema': 'schema',
                    'dst_table': 'table',
                    'src_schema': 'schema',
                    'src_table': 'table',
                    'workspace_name': 'workspace',
                },
            ],
            'state.json': {'resources': {'jobs': {'test': '123', 'assessment': '456'}}},
        }
    )


class CustomIterator:
    def __init__(self, values):
        self._values = iter(values)
        self._has_next = True
        self._next_value = None

    # pylint: disable=invalid-name
    def hasNext(self):
        try:
            self._next_value = next(self._values)
            self._has_next = True
        except StopIteration:
            self._has_next = False
        return self._has_next

    def next(self):
        if self._has_next:
            return self._next_value
        raise StopIteration


@pytest.fixture
def spark_table_crawl_mocker(mocker):
    def create_product_element_mock(key, value):
        def product_element_side_effect(index):
            if index == 0:
                return key
            if index == 1:
                return value
            raise IndexError(f"Invalid index: {index}")

        mock = mocker.Mock()
        mock.productElement.side_effect = product_element_side_effect
        return mock

    mock_list_databases_iterator = mocker.Mock()
    mock_list_databases_iterator.iterator.return_value = CustomIterator(["default", "test_database"])
    mock_list_tables_iterator = mocker.Mock()
    mock_list_tables_iterator.iterator.return_value = CustomIterator(["table1"])

    mock_property_1 = create_product_element_mock("delta.appendOnly", "true")
    mock_property_2 = create_product_element_mock("delta.autoOptimize", "false")
    mock_property_pat = create_product_element_mock("personalAccessToken", "e32kfkasdas")
    mock_property_password = create_product_element_mock("password", "very_secret")

    mock_storage_properties_list = [
        mock_property_1,
        mock_property_2,
        mock_property_pat,
        mock_property_password,
    ]
    mock_properties_iterator = mocker.Mock()
    mock_properties_iterator.iterator.return_value = CustomIterator(mock_storage_properties_list)

    mock_partition_col_iterator = mocker.Mock()
    mock_partition_col_iterator.iterator.return_value = CustomIterator(["age", "name"])

    get_table_mock = mocker.Mock()
    get_table_mock.provider().isDefined.return_value = True
    get_table_mock.provider().get.return_value = "delta"
    get_table_mock.storage().locationUri().isDefined.return_value = False

    get_table_mock.viewText().isDefined.return_value = True
    get_table_mock.viewText().get.return_value = "mock table text"
    get_table_mock.properties.return_value = mock_properties_iterator
    get_table_mock.partitionColumnNames.return_value = mock_partition_col_iterator

    return mock_list_databases_iterator, mock_list_tables_iterator, get_table_mock


@pytest.fixture
def run_workflow(mocker, mock_installation, ws, spark_table_crawl_mocker):
    def inner(cb, **replace) -> RuntimeContext:
        with _lock, patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
            pyspark_sql_session = mocker.Mock()
            sys.modules["pyspark.sql.session"] = pyspark_sql_session
            if 'installation' not in replace:
                replace['installation'] = mock_installation
            if 'workspace_client' not in replace:
                replace['workspace_client'] = ws
            if 'sql_backend' not in replace:
                replace['sql_backend'] = MockBackend()
            if 'config' not in replace:
                replace['config'] = mock_installation.load(WorkspaceConfig)
            if 'tables_crawler' not in replace:
                replace['tables_crawler'] = TablesCrawler(replace['sql_backend'], replace['config'].inventory_database)

            module = __import__(cb.__module__, fromlist=[cb.__name__])
            klass, method = cb.__qualname__.split('.', 1)
            workflow = getattr(module, klass)()
            current_task = getattr(workflow, method)

            ctx = RuntimeContext().replace(**replace)
            if isinstance(ctx.tables_crawler, FasterTableScanCrawler):
                mock_list_databases_iterator, mock_list_tables_iterator, get_table_mock = spark_table_crawl_mocker
                # pylint: disable=protected-access
                ctx.tables_crawler._spark._jsparkSession.sharedState().externalCatalog().listDatabases.return_value = (
                    mock_list_databases_iterator
                )
                # pylint: disable=protected-access
                ctx.tables_crawler._spark._jsparkSession.sharedState().externalCatalog().listTables.return_value = (
                    mock_list_tables_iterator
                )
                # pylint: disable=protected-access
                ctx.tables_crawler._spark._jsparkSession.sharedState().externalCatalog().getTable.return_value = (
                    get_table_mock
                )
                # pylint: enable=protected-access
            current_task(ctx)
            return ctx

    yield inner


@pytest.fixture
def acc_client():
    acc = create_autospec(AccountClient)
    acc.config = Config(host="https://accounts.cloud.databricks.com", account_id="123", token="123")
    acc.assert_not_called()
    return acc


class MockPathLookup(PathLookup):
    def __init__(self, cwd='source_code/samples', sys_paths: list[Path] | None = None):
        super().__init__(Path(__file__).parent / cwd, sys_paths or [])

    def change_directory(self, new_working_directory: Path) -> 'MockPathLookup':
        return MockPathLookup(new_working_directory, self._sys_paths)

    def __repr__(self):
        return f"<MockPathLookup {self._cwd}, sys.path: {self._sys_paths}>"


@pytest.fixture
def mock_path_lookup() -> PathLookup:
    return MockPathLookup()


@pytest.fixture
def mock_notebook_resolver():
    resolver = create_autospec(BaseNotebookResolver)
    resolver.resolve_notebook.return_value = None
    return resolver


@pytest.fixture
def mock_backend() -> MockBackend:
    return MockBackend()


@pytest.fixture
def ws():
    return mock_workspace_client()


@pytest.fixture
def simple_dependency_resolver(mock_path_lookup: PathLookup) -> DependencyResolver:
    allow_list = KnownList()
    library_resolver = PythonLibraryResolver(allow_list)
    notebook_resolver = NotebookResolver(NotebookLoader())
    import_resolver = ImportFileResolver(FileLoader(), allow_list)
    return DependencyResolver(library_resolver, notebook_resolver, import_resolver, import_resolver, mock_path_lookup)
