import pytest

from databricks.labs.ucx.config import MigrationConfig, ConnectConfig, GroupsConfig
from databricks.labs.ucx.providers.logger import logger
from databricks.labs.ucx.toolkits.group_migration import GroupMigrationToolkit


def factory(name, create, remove):
    cleanup = []
    def inner(**kwargs):
        x = create(**kwargs)
        logger.debug(f"added {name} fixture: {x}")
        cleanup.append(x)
        return x
    yield inner
    logger.debug(f"clearing {len(cleanup)} {name} fixtures")
    for x in cleanup:
        logger.debug(f"removing {name} fixture: {x}")
        remove(x)

@pytest.fixture
def make_account_user(acc, make_random):
    return factory('account user',
                   lambda: acc.users.create(user_name=f'ucx-{make_random(4)}@example.com'),
                   lambda user: acc.users.delete(user.id))


@pytest.fixture
def make_secret_scope(ws, make_random):
    def create(*, manage_by_group=None):
        name = f'ucx_{make_random(4)}'
        ws.secrets.create_scope(name, initial_manage_principal=manage_by_group)
        return name
    return factory('secret scope', create, lambda scope: ws.secrets.delete_scope(scope))


def test_secret_scopes(ws, inventory_config, make_account_group, make_secret_scope):
    acc_group = make_account_group()

    scope = make_secret_scope(manage_by_group=acc_group.display_name)

    config = MigrationConfig(
        connect=ConnectConfig.from_databricks_config(ws.config),
        with_table_acls=False,
        inventory=inventory_config,
        groups=GroupsConfig(selected=[acc_group.display_name]),
        auth=None,
        log_level="TRACE",
    )
    toolkit = GroupMigrationToolkit(config)

    toolkit._crawlers.secret_scopes.save(toolkit._workspace_inventory)
    toolkit.apply_permissions_to_backup_groups()

    # for _objects, id_attribute, request_object_type in verifiable_objects:
    #     _verify_group_permissions(_objects, id_attribute, request_object_type, ws, toolkit, "backup")
    #
    # _verify_roles_and_entitlements(toolkit.group_manager.migration_groups_provider, ws, "backup")
    #
    # toolkit.replace_workspace_groups_with_account_groups()
    #
    # new_groups = list(ws.groups.list(filter=f"displayName sw '{env.test_uid}'", attributes="displayName,meta"))
    # assert len(new_groups) == len(toolkit.group_manager.migration_groups_provider.groups)
    # assert all(g.meta.resource_type == "Group" for g in new_groups)
    #
    # toolkit.apply_permissions_to_account_groups()
    #
    # for _objects, id_attribute, request_object_type in verifiable_objects:
    #     _verify_group_permissions(_objects, id_attribute, request_object_type, ws, toolkit, "account")
    #
    # _verify_roles_and_entitlements(toolkit.group_manager.migration_groups_provider, ws, "account")
    #
    # toolkit.delete_backup_groups()
    #
    # backup_groups = list(
    #     ws.groups.list(
    #         filter=f"displayName sw '{config.groups.backup_group_prefix}{env.test_uid}'", attributes="displayName,meta"
    #     )
    # )
    # assert len(backup_groups) == 0
    #
    # toolkit.cleanup_inventory_table()