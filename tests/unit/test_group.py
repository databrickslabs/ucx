from unittest.mock import Mock

import pytest
from databricks.sdk.service.iam import Group, ResourceMeta

from databricks.labs.ucx.config import GroupsConfig
from databricks.labs.ucx.managers.group import GroupLevel, GroupManager


@pytest.fixture
def workspace_client():
    client = Mock()
    return client


@pytest.fixture
def group_manager(workspace_client):
    config = GroupsConfig(auto=True)
    return GroupManager(ws=workspace_client, groups=config)


def test_group_manager_find_eligible_groups(group_manager):
    group_manager._ws.groups.list.return_value = []
    groups = group_manager._find_eligible_groups()
    assert len(groups) == 0

    group_manager._ws.groups.list.return_value = [
        Group(display_name="grp1", meta=ResourceMeta(resource_type="rm1")),
        Group(display_name="grp2", meta=ResourceMeta(resource_type="rm2")),
    ]
    groups = group_manager._find_eligible_groups()
    assert len(groups) == 0

    group_manager._ws.groups.list.return_value = [
        Group(display_name="grp1", meta=ResourceMeta(resource_type="WorkspaceGroup")),
        Group(display_name="grp2", meta=ResourceMeta(resource_type="WorkspaceGroup")),
    ]
    groups = group_manager._find_eligible_groups()
    assert len(groups) == 2


def test_group_manager_list_account_level_groups(group_manager):
    group_manager._ws.api_client.do.return_value = {
        "totalResults": 0,
        "startIndex": 1,
        "itemsPerPage": 0,
        "Resources": [
            {
                "id": "id1",
                "displayName": "dn1",
            }
        ],
    }
    groups = group_manager._list_account_level_groups(filter="")
    assert len(groups) == 1
    assert groups[0].id == "id1"


def test_group_manager_get_group(group_manager):
    # Test with no groups
    group_manager._ws.api_client.do.return_value = {}
    group_manager._ws.groups.list.return_value = []
    group = group_manager._get_group(group_name="grp1", level=GroupLevel.WORKSPACE)
    assert group is None

    # Test account level groups
    group_manager._ws.api_client.do.return_value = {
        "totalResults": 0,
        "startIndex": 1,
        "itemsPerPage": 0,
        "Resources": [
            {
                "id": "id1",
                "displayName": "dn1",
            }
        ],
    }
    group = group_manager._get_group(group_name="grp1", level=GroupLevel.ACCOUNT)
    assert group.display_name == "dn1"

    # Test with workspace groups
    group_manager._ws.groups.list.return_value = [
        Group(display_name="grp1", meta=ResourceMeta(resource_type="rm1")),
        Group(display_name="grp2", meta=ResourceMeta(resource_type="rm2")),
    ]
    group = group_manager._get_group(group_name="grp1", level=GroupLevel.WORKSPACE)
    assert group.display_name == "grp1"


def test_group_manager_get_or_create_backup_group(group_manager):
    prefix = group_manager.config.backup_group_prefix
    prefixed_name = f"{prefix}grp1"

    def create_group(display_name, meta, entitlements, roles, members):
        return Group(display_name=display_name, meta=meta, entitlements=entitlements, roles=roles, members=members)

    # Test with backup not present
    group_manager._ws.groups.list.return_value = []
    group_manager._ws.groups.create.side_effect = create_group
    source_group = Group(display_name="grp1", meta=ResourceMeta(resource_type="rm1"))
    group = group_manager._get_or_create_backup_group(source_group_name="grp1", source_group=source_group)
    assert group.display_name == prefixed_name

    # Test with backup already present
    group_manager._ws.groups.list.return_value = [
        Group(display_name=prefixed_name, meta=ResourceMeta(resource_type="rm1")),
    ]
    source_group = Group(display_name="grp1", meta=ResourceMeta(resource_type="rm1"))
    group = group_manager._get_or_create_backup_group(source_group_name="grp1", source_group=source_group)
    assert group.display_name == prefixed_name
