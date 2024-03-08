from unittest.mock import create_autospec

from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo

from databricks.labs.ucx.hive_metastore.catalog_schema import CatalogSchema
from databricks.labs.ucx.hive_metastore.mapping import TableMapping

from ..framework.mocks import MockBackend


def prepare_test(ws, mock_prompts) -> CatalogSchema:
    ws.catalogs.list.return_value = [CatalogInfo(name="catalog1")]
    ws.schemas.list.return_value = [SchemaInfo(name="schema1")]
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

    return CatalogSchema(ws, table_mapping, mock_prompts)


def test_create():
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": "s3://foo/bar"})

    catalog_schema = prepare_test(ws, mock_prompts)
    catalog_schema.create_catalog_schema()
    ws.catalogs.create.assert_called_once_with("catalog2", storage_root="s3://foo/bar", comment="Created by UCX")
    ws.schemas.create.assert_any_call("schema2", "catalog2", comment="Created by UCX")
    ws.schemas.create.assert_any_call("schema3", "catalog1", comment="Created by UCX")


def test_no_catalog_storage():
    ws = create_autospec(WorkspaceClient)
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})

    catalog_schema = prepare_test(ws, mock_prompts)
    catalog_schema.create_catalog_schema()
    ws.catalogs.create.assert_called_once_with("catalog2", comment="Created by UCX")


def test_for_cli():
    ws = create_autospec(WorkspaceClient)
    installation = MockInstallation(
        {
            "config.yml": {
                'version': 2,
                'inventory_database': 'test',
                'connect': {
                    'host': 'test',
                    'token': 'test',
                },
            }
        }
    )
    prompts = MockPrompts({"hello": "world"})
    catalog_schema = CatalogSchema.for_cli(ws, installation, prompts)
    assert isinstance(catalog_schema, CatalogSchema)
