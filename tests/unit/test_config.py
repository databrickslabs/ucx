import sqlglot
from databricks.labs.blueprint.installation import MockInstallation

from databricks.labs.ucx.config import WorkspaceConfig


def test_v1_migrate_zeroconf():
    installation = MockInstallation(
        {'config.yml': {'inventory_database': 'x', 'groups': {}, 'connect': {'host': 'a', 'token': 'b'}}}
    )

    workspace_config = installation.load(WorkspaceConfig)

    assert workspace_config.renamed_group_prefix == 'db-temp-'


def test_v1_migrate_some_conf():
    installation = MockInstallation(
        {
            'config.yml': {
                'inventory_database': 'x',
                'groups': {'selected': ['foo', 'bar'], 'backup_group_prefix': 'some-'},
                'connect': {'host': 'a', 'token': 'b'},
            }
        }
    )

    workspace_config = installation.load(WorkspaceConfig)

    assert workspace_config.renamed_group_prefix == 'some-'
    assert workspace_config.include_group_names == ['foo', 'bar']


def test_workspace_config_transforms_inventory_database_in_query():
    installation = MockInstallation({"config.yml": {"inventory_database": "test"}})
    workspace_config = installation.load(WorkspaceConfig)

    query_transformed_expected = "SELECT a, b FROM hive_metastore.test.table"
    query = sqlglot.parse_one("SELECT a, b FROM inventory.table")

    query_transformed = (
        query
        .transform(workspace_config.transform_inventory_database)
        .sql(dialect=sqlglot.dialects.Databricks)
    )

    assert query_transformed == query_transformed_expected
