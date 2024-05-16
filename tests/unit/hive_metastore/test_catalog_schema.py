from unittest.mock import call, create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import CatalogInfo, ExternalLocationInfo, SchemaInfo

from databricks.labs.ucx.hive_metastore.catalog_schema import CatalogSchema
from databricks.labs.ucx.hive_metastore.grants import PrincipalACL, Grant
from databricks.labs.ucx.hive_metastore.mapping import TableMapping


def prepare_test(ws, backend: MockBackend | None = None) -> CatalogSchema:
    ws.catalogs.list.return_value = [CatalogInfo(name="catalog1")]
    ws.schemas.list.return_value = [SchemaInfo(name="schema1")]
    ws.external_locations.list.return_value = [ExternalLocationInfo(url="s3://foo/bar")]
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
    principal_acl = create_autospec(PrincipalACL)
    grants = [
        Grant('user1', 'SELECT', 'catalog1', 'schema3', 'table'),
        Grant('user1', 'MODIFY', 'catalog2', 'schema2', 'table'),
        Grant('user1', 'SELECT', 'catalog2', 'schema3', 'table2'),
        Grant('user1', 'USAGE', 'hive_metastore', 'schema3'),
        Grant('user1', 'USAGE', 'hive_metastore', 'schema2'),
    ]
    principal_acl.get_interactive_cluster_grants.return_value = grants
    return CatalogSchema(ws, table_mapping, principal_acl, backend)


@pytest.mark.parametrize("location", ["s3://foo/bar", "s3://foo/bar/test", "s3://foo/bar/test/baz"])
def test_create_all_catalogs_schemas_creates_catalogs(location: str):
    """Catalog 2-4 should be created; catalog 1 already exists."""
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": location})

    catalog_schema = prepare_test(ws)
    catalog_schema.create_all_catalogs_schemas(mock_prompts)

    calls = [
        call("catalog2", storage_root=location, comment="Created by UCX"),
        call("catalog3", storage_root=location, comment="Created by UCX"),
        call("catalog4", storage_root=location, comment="Created by UCX"),
    ]
    ws.catalogs.create.assert_has_calls(calls, any_order=True)


@pytest.mark.parametrize(
    "catalog,schema",
    [("catalog1", "schema2"), ("catalog1", "schema3"), ("catalog2", "schema2"), ("catalog3", "schema3")],
)
def test_create_all_catalogs_schemas_creates_schemas(catalog: str, schema: str):
    """Non-existing schemas should be created."""
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": "metastore"})

    catalog_schema = prepare_test(ws)
    catalog_schema.create_all_catalogs_schemas(mock_prompts)

    ws.schemas.create.assert_any_call(schema, catalog, comment="Created by UCX")


def test_create_bad_location():
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": "s3://foo/fail"})
    catalog_schema = prepare_test(ws)
    with pytest.raises(NotFound):
        catalog_schema.create_all_catalogs_schemas(mock_prompts)
    ws.catalogs.create.assert_not_called()
    ws.catalogs.list.assert_called_once()
    ws.schemas.create.assert_not_called()


def test_no_catalog_storage():
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})

    catalog_schema = prepare_test(ws)
    catalog_schema.create_all_catalogs_schemas(mock_prompts)

    calls = [
        call("catalog2", comment="Created by UCX"),
        call("catalog3", comment="Created by UCX"),
    ]
    ws.catalogs.create.assert_has_calls(calls, any_order=True)


def test_catalog_schema_acl():
    ws = create_autospec(WorkspaceClient)
    backend = MockBackend()
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})

    catalog_schema = prepare_test(ws, backend)
    catalog_schema.create_all_catalogs_schemas(mock_prompts)

    calls = [
        call("catalog2", comment="Created by UCX"),
        call("catalog3", comment="Created by UCX"),
    ]
    ws.catalogs.create.assert_has_calls(calls, any_order=True)
    ws.schemas.create.assert_any_call("schema2", "catalog2", comment="Created by UCX")
    queries = [
        'GRANT USE SCHEMA ON DATABASE catalog1.schema3 TO `user1`',
        'GRANT USE SCHEMA ON DATABASE catalog2.schema2 TO `user1`',
        'GRANT USE SCHEMA ON DATABASE catalog2.schema3 TO `user1`',
        'GRANT USE CATALOG ON CATALOG catalog1 TO `user1`',
        'GRANT USE CATALOG ON CATALOG catalog2 TO `user1`',
    ]
    assert len(backend.queries) == len(queries)
    for query in queries:
        assert query in backend.queries
