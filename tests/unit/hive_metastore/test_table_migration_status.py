from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest, DatabricksError, NotFound
from databricks.sdk.service.catalog import CatalogInfoSecurableKind, CatalogInfo, SchemaInfo, TableInfo

from databricks.labs.ucx.hive_metastore.tables import TablesCrawler
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatusRefresher


def test_table_migration_status_refresher_get_seen_tables_handles_errors_on_catalogs_list(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.catalogs.list.side_effect = BadRequest()
    tables_crawler = create_autospec(TablesCrawler)

    refresher = TableMigrationStatusRefresher(ws, mock_backend, "test", tables_crawler)

    seen_tables = refresher.get_seen_tables()

    assert not seen_tables
    ws.catalogs.list.assert_called_once()
    ws.schemas.list.assert_not_called()
    ws.tables.list.assert_not_called()
    tables_crawler.snapshot.assert_not_called()


@pytest.mark.parametrize("error", [BadRequest(), NotFound()])
def test_table_migration_status_refresher_get_seen_tables_handles_errors_on_schemas_list(
    mock_backend, error: DatabricksError
) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.catalogs.list.return_value = [CatalogInfo(name="test")]
    ws.schemas.list.side_effect = error
    tables_crawler = create_autospec(TablesCrawler)

    refresher = TableMigrationStatusRefresher(ws, mock_backend, "test", tables_crawler)

    seen_tables = refresher.get_seen_tables()

    assert not seen_tables
    ws.catalogs.list.assert_called_once()
    ws.schemas.list.assert_called_once()
    ws.tables.list.assert_not_called()
    tables_crawler.snapshot.assert_not_called()


@pytest.mark.parametrize("error", [BadRequest(), NotFound()])
def test_table_migration_status_refresher_get_seen_tables_handles_errors_on_tables_list(
    mock_backend, error: DatabricksError
) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.catalogs.list.return_value = [CatalogInfo(name="test")]
    ws.schemas.list.return_value = [SchemaInfo(catalog_name="test", name="test")]
    ws.tables.list.side_effect = error
    tables_crawler = create_autospec(TablesCrawler)

    refresher = TableMigrationStatusRefresher(ws, mock_backend, "test", tables_crawler)

    seen_tables = refresher.get_seen_tables()

    assert not seen_tables
    ws.catalogs.list.assert_called_once()
    ws.schemas.list.assert_called_once()
    ws.tables.list.assert_called_once()
    tables_crawler.snapshot.assert_not_called()


@pytest.mark.parametrize(
    "securable_kind",
    [
        CatalogInfoSecurableKind.CATALOG_INTERNAL,
        CatalogInfoSecurableKind.CATALOG_SYSTEM,
    ],
)
def test_table_migration_status_refresher_get_seen_tables_skips_builtin_catalog(
    mock_backend, securable_kind: CatalogInfoSecurableKind
) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.catalogs.list.return_value = [
        CatalogInfo(name="test"),
        CatalogInfo(name="system", securable_kind=securable_kind),
    ]
    ws.schemas.list.return_value = [
        SchemaInfo(catalog_name="test", name="test"),
        SchemaInfo(catalog_name="system", name="access"),
    ]
    ws.tables.list.return_value = [
        TableInfo(
            full_name="test.test.test",
            catalog_name="test",
            schema_name="test",
            name="test",
            properties={"upgraded_from": "test"},
        ),
        TableInfo(catalog_name="system", schema_name="access", name="audit"),
    ]
    tables_crawler = create_autospec(TablesCrawler)
    refresher = TableMigrationStatusRefresher(ws, mock_backend, "test", tables_crawler)

    seen_tables = refresher.get_seen_tables()

    assert seen_tables == {"test.test.test": "test"}
    ws.catalogs.list.assert_called_once()
    ws.schemas.list.assert_called_once_with(catalog_name="test")  # System is NOT called
    ws.tables.list.assert_called()
    tables_crawler.snapshot.assert_not_called()
