import logging
from unittest.mock import call, create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest, NotFound
from databricks.sdk.service.catalog import CatalogInfo, ExternalLocationInfo, SchemaInfo

from databricks.labs.ucx.hive_metastore.catalog_schema import CatalogSchema
from databricks.labs.ucx.hive_metastore.grants import Grant, MigrateGrants
from databricks.labs.ucx.hive_metastore.mapping import TableMapping
from databricks.labs.ucx.workspace_access.groups import GroupManager


def prepare_test(ws, backend: MockBackend | None = None) -> CatalogSchema:  # pylint: disable=too-complex
    """Prepare tests with the following setup:

    Existing HIVE metastore resources:
    - Schemas: `schema1`, `schema2`, `schema3`
    - Tables: Irrelevant for creating catalogs and schemas.
    - Legacy ACLS:
      - Principal `principal1`:
        - DENY on `schema2`
        - USAGE on `schema3`
      - Users:
        - `user1` has USE on `hive_metastore`
        - `user2` has USAGE on `hive_metastore.schema2`
        - `user3` has USAGE on `hive_metastore.schema3`
        - `user4` has DENY on `hive_metastore.schema3`
        - `user5` has SELECT on a table and view (Irrelevant for creating catalogs and schemas)

    Existing UC resources:
    - Catalog `catalog1`
    - Schema `catalog1.schema1`
    - External locations (can be referenced when creating catalogs):
      - `"s3://foo/bar"`
      - `"abfss://container@storageaccount.dfs.core.windows.net"`

    To be created UC resources inferred from the mapping.csv
    - Catalogs `catalog1`, `catalog2`, `catalog3` and `catalog4`
    - Schemas:
      - `catalog1.schema2`
      - `catalog1.schema3`
      - `catalog2.schema2`
      - `catalog2.schema3`
      - `catalog3.schema3`
      - `catalog4.schema4`
    """
    backend = backend or MockBackend()

    def get_catalog(catalog_name: str) -> CatalogInfo:
        if catalog_name == "catalog1":
            return CatalogInfo(name="catalog1")
        raise NotFound(f"Catalog: {catalog_name}")

    def raise_catalog1_exists(catalog: str, *_, **__) -> None:
        if catalog == "catalog1":
            raise BadRequest("Catalog 'catalog1' already exists")

    def get_schema(full_name: str) -> SchemaInfo:
        if full_name == "catalog1.schema1":
            return SchemaInfo(catalog_name="catalog1", name="schema", full_name="catalog1.schema1")
        raise NotFound(f"Schema: {full_name}")

    def raise_catalog1_schema1_exists(schema: str, catalog: str, *_, **__) -> None:
        if catalog == "catalog1" and schema == "schema1":
            raise BadRequest("Schema 'catalog1.schema1' already exists")

    ws.catalogs.list.return_value = [CatalogInfo(name="catalog1")]
    ws.catalogs.get.side_effect = get_catalog
    ws.catalogs.create.side_effect = raise_catalog1_exists
    ws.schemas.list.return_value = [SchemaInfo(catalog_name="catalog1", name="schema1")]
    ws.schemas.get.side_effect = get_schema
    ws.schemas.create.side_effect = raise_catalog1_schema1_exists
    ws.external_locations.list.return_value = [
        ExternalLocationInfo(url="s3://foo/bar"),
        ExternalLocationInfo(url="abfss://container@storageaccount.dfs.core.windows.net"),
    ]
    installation = MockInstallation(
        {
            "mapping.csv": [
                {
                    "catalog_name": "catalog1",
                    "dst_schema": "schema1",
                    "dst_table": "table1",
                    "src_schema": "schema1",
                    "src_table": "table1",
                    "workspace_name": "workspace",
                },
                {
                    "catalog_name": "catalog1",
                    "dst_schema": "schema2",
                    "dst_table": "table1",
                    "src_schema": "schema1",
                    "src_table": "abfss://container@msft/path/dest1",
                    "workspace_name": "workspace",
                },
                {
                    "catalog_name": "catalog1",
                    "dst_schema": "schema3",
                    "dst_table": "table1",
                    "src_schema": "schema3",
                    "src_table": "table",
                    "workspace_name": "workspace",
                },
                {
                    "catalog_name": "catalog2",
                    "dst_schema": "schema2",
                    "dst_table": "table1",
                    "src_schema": "schema2",
                    "src_table": "table",
                    "workspace_name": "workspace",
                },
                {
                    "catalog_name": "catalog2",
                    "dst_schema": "schema2",
                    "dst_table": "table2",
                    "src_schema": "schema2",
                    "src_table": "abfss://container@msft/path/dest2",
                    "workspace_name": "workspace",
                },
                {
                    "catalog_name": "catalog2",
                    "dst_schema": "schema3",
                    "dst_table": "table1",
                    "src_schema": "schema2",
                    "src_table": "table2",
                    "workspace_name": "workspace",
                },
                {
                    "catalog_name": "catalog3",
                    "dst_schema": "schema3",
                    "dst_table": "table1",
                    "src_schema": "schema1",
                    "src_table": "abfss://container@msft/path/dest3",
                    "workspace_name": "workspace",
                },
                {
                    "catalog_name": "catalog4",
                    "dst_schema": "schema4",
                    "dst_table": "table1",
                    "src_schema": "schema1",
                    "src_table": "abfss://container@msft/path/dest4",
                    "workspace_name": "workspace",
                },
            ]
        }
    )
    table_mapping = TableMapping(installation, ws, backend)

    def interactive_cluster_grants_loader() -> list[Grant]:
        return [
            Grant("principal1", "DENY", "hive_metastore", "schema2"),
            Grant("principal1", "USAGE", "hive_metastore", "schema3"),
        ]

    def hive_grants_loader() -> list[Grant]:
        return [
            Grant("user1", "USE", "hive_metastore"),
            Grant("user2", "USAGE", "hive_metastore", "schema2"),
            Grant("user3", "USAGE", "hive_metastore", "schema3"),
            Grant("user4", "DENY", "hive_metastore", "schema3"),
            Grant("user5", "SELECT", "hive_metastore", "schema2", table="table"),
            Grant("user5", "SELECT", "hive_metastore", "schema2", view="view"),
        ]

    group_manager = create_autospec(GroupManager)
    group_manager.snapshot.return_value = []
    migrate_grants = MigrateGrants(backend, group_manager, [interactive_cluster_grants_loader, hive_grants_loader])

    return CatalogSchema(ws, table_mapping, migrate_grants, "ucx")


def test_create_ucx_catalog_creates_ucx_catalog() -> None:
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: ucx": "metastore"})

    catalog_schema = prepare_test(ws)
    catalog_schema.create_ucx_catalog(mock_prompts)

    ws.catalogs.create.assert_called_with("ucx", comment="Created by UCX", properties=None)


def test_create_ucx_catalog_skips_when_ucx_catalogs_exists(caplog) -> None:
    ws = create_autospec(WorkspaceClient)
    catalog_schema = prepare_test(ws)
    ws.catalogs.get.side_effect = lambda catalog_name: CatalogInfo(name=catalog_name)

    def raise_catalog_exists(catalog: str, *_, **__) -> None:
        if catalog == "ucx":
            raise BadRequest("Catalog 'ucx' already exists")

    ws.catalogs.create.side_effect = raise_catalog_exists

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.hive_metastore.catalog_schema"):
        catalog_schema.create_ucx_catalog(MockPrompts({}))
    assert "Skipping already existing catalog: ucx" in caplog.text


@pytest.mark.parametrize(
    "location",
    [
        "s3://foo/bar",
        "s3://foo/bar/test",
        "s3://foo/bar/test/baz",
        "abfss://container@storageaccount.dfs.core.windows.net",
    ],
)
def test_create_all_catalogs_schemas_creates_catalogs(location: str) -> None:
    """Catalog 2-4 should be created; catalog 1 already exists."""
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": location})

    catalog_schema = prepare_test(ws)
    catalog_schema.create_all_catalogs_schemas(mock_prompts)

    calls = [
        call("catalog2", storage_root=location, comment="Created by UCX", properties=None),
        call("catalog3", storage_root=location, comment="Created by UCX", properties=None),
        call("catalog4", storage_root=location, comment="Created by UCX", properties=None),
    ]
    ws.catalogs.create.assert_has_calls(calls, any_order=True)


def test_create_all_catalogs_schemas_creates_catalogs_with_different_locations() -> None:
    """Catalog 2-4 should be created; catalog 1 already exists."""
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts(
        {
            "Please provide storage location url for catalog: catalog2": "s3://foo/bar",
            "Please provide storage location url for catalog: catalog3": "s3://foo/bar/test",
            "Please provide storage location url for catalog: catalog4": "s3://foo/bar/test/baz",
        }
    )

    catalog_schema = prepare_test(ws)
    catalog_schema.create_all_catalogs_schemas(mock_prompts)

    calls = [
        call("catalog2", storage_root="s3://foo/bar", comment="Created by UCX", properties=None),
        call("catalog3", storage_root="s3://foo/bar/test", comment="Created by UCX", properties=None),
        call("catalog4", storage_root="s3://foo/bar/test/baz", comment="Created by UCX", properties=None),
    ]
    ws.catalogs.create.assert_has_calls(calls, any_order=True)


@pytest.mark.parametrize(
    "catalog,schema",
    [("catalog1", "schema2"), ("catalog1", "schema3"), ("catalog2", "schema2"), ("catalog3", "schema3")],
)
def test_create_all_catalogs_schemas_creates_schemas(catalog: str, schema: str) -> None:
    """Non-existing schemas should be created."""
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": "metastore"})

    catalog_schema = prepare_test(ws)
    catalog_schema.create_all_catalogs_schemas(mock_prompts)

    ws.schemas.create.assert_any_call(schema, catalog, comment="Created by UCX")


def test_create_catalogs_and_schemas_with_invalid_storage_location() -> None:
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": "s3://foo/fail"})
    catalog_schema = prepare_test(ws)

    with pytest.raises(NotFound):
        catalog_schema.create_all_catalogs_schemas(mock_prompts)
    # `catalog3` and `catalog4` are not reached as the logic breaks when the users fails to supply a valid location
    calls = [call("catalog1"), call("catalog2")]
    ws.catalogs.get.assert_has_calls(calls)
    ws.catalogs.create.assert_not_called()
    ws.schemas.create.assert_not_called()


def test_no_catalog_storage() -> None:
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})

    catalog_schema = prepare_test(ws)
    catalog_schema.create_all_catalogs_schemas(mock_prompts)

    calls = [
        call("catalog2", comment="Created by UCX", properties=None),
        call("catalog3", comment="Created by UCX", properties=None),
        call("catalog4", comment="Created by UCX", properties=None),
    ]
    ws.catalogs.create.assert_has_calls(calls, any_order=True)


def test_catalog_schema_acl() -> None:
    ws = create_autospec(WorkspaceClient)
    backend = MockBackend()
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})
    catalog_schema = prepare_test(ws, backend)

    catalog_schema.create_all_catalogs_schemas(mock_prompts)

    calls = [
        call("catalog2", comment="Created by UCX", properties=None),
        call("catalog3", comment="Created by UCX", properties=None),
        call("catalog4", comment="Created by UCX", properties=None),
    ]
    ws.catalogs.create.assert_has_calls(calls, any_order=True)
    ws.schemas.create.assert_any_call("schema2", "catalog2", comment="Created by UCX")
    queries = [
        'GRANT USE CATALOG ON CATALOG `catalog1` TO `principal1`',
        'GRANT USE CATALOG ON CATALOG `catalog1` TO `user3`',
        'GRANT USE CATALOG ON CATALOG `catalog2` TO `user2`',
        'GRANT USE SCHEMA ON DATABASE `catalog1`.`schema3` TO `principal1`',
        'GRANT USE SCHEMA ON DATABASE `catalog1`.`schema3` TO `user3`',
        'GRANT USE SCHEMA ON DATABASE `catalog2`.`schema2` TO `user2`',
        'GRANT USE SCHEMA ON DATABASE `catalog2`.`schema3` TO `user2`',
    ]

    assert not set(backend.queries) - set(queries), f"Additional queries {set(backend.queries) - set(queries)}"
    assert not set(queries) - set(backend.queries), f"Missing queries {set(queries) - set(backend.queries)}"


def test_create_all_catalogs_schemas_logs_untranslatable_grant(caplog) -> None:
    ws = create_autospec(WorkspaceClient)
    backend = MockBackend()
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})
    catalog_schema = prepare_test(ws, backend)

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.hive_metastore.catalog_schema"):
        catalog_schema.create_all_catalogs_schemas(mock_prompts)
    message_prefix = "failed-to-migrate: Hive metastore grant 'DENY' cannot be mapped to UC grant for"
    assert f"{message_prefix} DATABASE 'catalog1.schema3'. Skipping." in caplog.messages
    assert f"{message_prefix} CATALOG 'catalog2'. Skipping." in caplog.messages
    assert f"{message_prefix} DATABASE 'catalog2.schema2'. Skipping." in caplog.messages
    assert f"{message_prefix} DATABASE 'catalog2.schema3'. Skipping." in caplog.messages
    ws.assert_not_called()


def test_create_catalogs_and_schemas_logs_skipping_already_existing_unity_catalog_resources(caplog) -> None:
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})
    catalog_schema = prepare_test(ws)

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.hive_metastore.catalog_schema"):
        catalog_schema.create_all_catalogs_schemas(mock_prompts)
    assert "Skipping already existing catalog: catalog1" in caplog.text
    assert "Skipping already existing schema: catalog1.schema1" in caplog.text
    ws.assert_not_called()
