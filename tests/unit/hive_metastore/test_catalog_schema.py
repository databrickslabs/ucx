from unittest.mock import create_autospec

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
                    'dst_schema': 'schema2',
                    'dst_table': 'table2',
                    'src_schema': 'schema2',
                    'src_table': 'table2',
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
        Grant('user1', 'SELECY', 'catalog2', 'schema2', 'table2'),
        Grant('user1', 'USAGE', 'hive_metastore', 'schema3'),
        Grant('user1', 'USAGE', 'hive_metastore', 'schema2'),
    ]
    principal_acl.get_interactive_cluster_grants.return_value = grants
    return CatalogSchema(ws, table_mapping, principal_acl, backend)


def test_create():
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": "s3://foo/bar"})

    catalog_schema = prepare_test(ws)
    catalog_schema.create_all_catalogs_schemas(
        mock_prompts,
    )
    ws.catalogs.create.assert_called_once_with("catalog2", storage_root="s3://foo/bar", comment="Created by UCX")
    ws.schemas.create.assert_any_call("schema2", "catalog2", comment="Created by UCX")
    ws.schemas.create.assert_any_call("schema3", "catalog1", comment="Created by UCX")


def test_create_sub_location():
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": "s3://foo/bar/test"})

    catalog_schema = prepare_test(ws)
    catalog_schema.create_all_catalogs_schemas(mock_prompts)
    ws.catalogs.create.assert_called_once_with("catalog2", storage_root="s3://foo/bar/test", comment="Created by UCX")
    ws.schemas.create.assert_any_call("schema2", "catalog2", comment="Created by UCX")
    ws.schemas.create.assert_any_call("schema3", "catalog1", comment="Created by UCX")


def test_create_bad_location():
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": "s3://foo/fail"})
    catalog_schema = prepare_test(ws)
    with pytest.raises(NotFound):
        catalog_schema.create_all_catalogs_schemas(mock_prompts)


def test_no_catalog_storage():
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})

    catalog_schema = prepare_test(ws)
    catalog_schema.create_all_catalogs_schemas(mock_prompts)
    ws.catalogs.create.assert_called_once_with("catalog2", comment="Created by UCX")


def test_catalog_schema_acl():
    ws = create_autospec(WorkspaceClient)
    backend = MockBackend()
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})
    catalog_schema = prepare_test(ws, backend)
    catalog_schema.create_all_catalogs_schemas(
        mock_prompts,
    )
    queries = [
        'GRANT USE SCHEMA ON DATABASE catalog1.schema3 TO `user1`',
        'GRANT USE SCHEMA ON DATABASE catalog2.schema2 TO `user1`',
        'GRANT USE CATALOG ON CATALOG catalog1 TO `user1`',
        'GRANT USE CATALOG ON CATALOG catalog2 TO `user1`',
    ]
    assert len(backend.queries) == 4
    for query in queries:
        assert query in backend.queries
