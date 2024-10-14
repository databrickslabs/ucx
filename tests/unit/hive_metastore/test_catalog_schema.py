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


def prepare_test(ws, backend: MockBackend | None = None) -> CatalogSchema:
    ws.catalogs.list.return_value = [CatalogInfo(name="catalog1")]

    def get_catalog(catalog_name: str) -> CatalogInfo:
        if catalog_name == "catalog1":
            return CatalogInfo(name="catalog1")
        raise NotFound(f"Catalog: {catalog_name}")

    ws.catalogs.get.side_effect = get_catalog

    def raise_catalog_exists(catalog: str, *_, **__) -> None:
        if catalog == "catalog1":
            raise BadRequest("Catalog 'catalog1' already exists")

    ws.catalogs.create.side_effect = raise_catalog_exists
    ws.schemas.list.return_value = [SchemaInfo(catalog_name="catalog1", name="schema1")]
    ws.external_locations.list.return_value = [
        ExternalLocationInfo(url="s3://foo/bar"),
        ExternalLocationInfo(url="abfss://container@storageaccount.dfs.core.windows.net"),
    ]
    if backend is None:
        backend = MockBackend()
    installation = MockInstallation(
        {
            'mapping.csv': [
                {
                    'catalog_name': 'catalog1',
                    'dst_schema': 'schema3',
                    'dst_table': 'table',
                    'src_schema': 'schema3',
                    'src_table': 'table',
                    'workspace_name': 'workspace',
                },
                {
                    'catalog_name': 'catalog2',
                    'dst_schema': 'schema2',
                    'dst_table': 'table',
                    'src_schema': 'schema2',
                    'src_table': 'table',
                    'workspace_name': 'workspace',
                },
                {
                    'catalog_name': 'catalog2',
                    'dst_schema': 'schema3',
                    'dst_table': 'table2',
                    'src_schema': 'schema2',
                    'src_table': 'table2',
                    'workspace_name': 'workspace',
                },
                {
                    'catalog_name': 'catalog1',
                    'dst_schema': 'schema2',
                    'dst_table': 'table3',
                    'src_schema': 'schema1',
                    'src_table': 'abfss://container@msft/path/dest1',
                    'workspace_name': 'workspace',
                },
                {
                    'catalog_name': 'catalog2',
                    'dst_schema': 'schema2',
                    'dst_table': 'table1',
                    'src_schema': 'schema2',
                    'src_table': 'abfss://container@msft/path/dest2',
                    'workspace_name': 'workspace',
                },
                {
                    'catalog_name': 'catalog3',
                    'dst_schema': 'schema3',
                    'dst_table': 'table1',
                    'src_schema': 'schema1',
                    'src_table': 'abfss://container@msft/path/dest3',
                    'workspace_name': 'workspace',
                },
                {
                    'catalog_name': 'catalog4',
                    'dst_schema': 'schema4',
                    'dst_table': 'table1',
                    'src_schema': 'schema1',
                    'src_table': 'abfss://container@msft/path/dest4',
                    'workspace_name': 'workspace',
                },
            ]
        }
    )
    table_mapping = TableMapping(installation, ws, backend)

    def interactive_cluster_grants_loader() -> list[Grant]:
        return [
            Grant('princ1', 'SELECT', 'catalog1', 'schema3', 'table'),
            Grant('princ1', 'MODIFY', 'catalog2', 'schema2', 'table'),
            Grant('princ1', 'SELECT', 'catalog2', 'schema3', 'table2'),
            Grant('princ1', 'USAGE', 'hive_metastore', 'schema3'),
            Grant('princ1', 'DENY', 'hive_metastore', 'schema2'),
        ]

    def hive_grants_loader() -> list[Grant]:
        return [
            Grant(principal="user1", catalog="hive_metastore", action_type="USE"),
            Grant(principal="user2", catalog="hive_metastore", database="schema3", action_type="USAGE"),
            Grant(
                principal="user3",
                catalog="hive_metastore",
                database="database_one",
                view="table_one",
                action_type="SELECT",
            ),
            Grant(principal="user4", catalog="hive_metastore", database="schema3", action_type="DENY"),
            Grant(
                principal="user5",
                catalog="hive_metastore",
                database="schema2",
                action_type="USAGE",
            ),
        ]

    group_manager = create_autospec(GroupManager)
    group_manager.snapshot.return_value = []
    migrate_grants = MigrateGrants(backend, group_manager, [interactive_cluster_grants_loader, hive_grants_loader])

    return CatalogSchema(ws, table_mapping, migrate_grants, backend, "ucx")


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


def test_create_bad_location() -> None:
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": "s3://foo/fail"})
    catalog_schema = prepare_test(ws)
    with pytest.raises(NotFound):
        catalog_schema.create_all_catalogs_schemas(mock_prompts)
    ws.catalogs.get.assert_called_once_with("catalog2")
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
    ]
    ws.catalogs.create.assert_has_calls(calls, any_order=True)
    ws.schemas.create.assert_any_call("schema2", "catalog2", comment="Created by UCX")
    queries = [
        'GRANT USE SCHEMA ON DATABASE `catalog1`.`schema3` TO `princ1`',
        'GRANT USE CATALOG ON CATALOG `catalog1` TO `princ1`',
        'GRANT USE CATALOG ON CATALOG `catalog1` TO `user2`',
        'GRANT USE SCHEMA ON DATABASE `catalog1`.`schema3` TO `user2`',
        'GRANT USE SCHEMA ON DATABASE `catalog2`.`schema2` TO `user5`',
        'GRANT USE SCHEMA ON DATABASE `catalog2`.`schema3` TO `user5`',
        'GRANT USE CATALOG ON CATALOG `catalog2` TO `user5`',
    ]

    outer = set(backend.queries) ^ set(queries)
    assert not outer, f"Additional queries {set(backend.queries) - set(queries)}"
    assert not outer, f"Missing queries {set(queries) - set(backend.queries)}"


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
