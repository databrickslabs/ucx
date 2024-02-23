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
