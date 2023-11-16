from unittest.mock import MagicMock

from databricks.sdk.service import iam
from databricks.sdk.service.iam import ComplexValue, Group, ResourceMeta

from databricks.labs.ucx.workspace_access.groups import GroupManager, MigratedGroup
from tests.unit.framework.mocks import MockBackend


def test_snapshot_with_group_created_in_account_console_should_be_considered():
    backend = MockBackend()
    wsclient = MagicMock()
    group = Group(
        id="1",
        display_name="de",
        meta=ResourceMeta(resource_type="WorkspaceGroup"),
        members=[ComplexValue(display="test-user-1", value="20"), ComplexValue(display="test-user-2", value="21")],
        roles=[
            ComplexValue(value="arn:aws:iam::123456789098:instance-profile/ip1"),
            ComplexValue(value="arn:aws:iam::123456789098:instance-profile/ip2"),
        ],
        entitlements=[ComplexValue(value="allow-cluster-create"), ComplexValue(value="allow-instance-pool-create")],
    )
    wsclient.groups.list.return_value = [group]
    account_admins_group = Group(id="1234", display_name="de")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_admins_group]],
    }
    res = GroupManager(backend, wsclient, inventory_database="inv").snapshot()
    assert res == [
        MigratedGroup(
            id_in_workspace="1",
            name_in_workspace="de",
            name_in_account="de",
            temporary_name="ucx-renamed-de",
            members='[{"display": "test-user-1", "value": "20"}, {"display": "test-user-2", "value": "21"}]',
            external_id="1234",
            roles='[{"value": "arn:aws:iam::123456789098:instance-profile/ip1"}, {"value": "arn:aws:iam::123456789098:instance-profile/ip2"}]',
            entitlements='[{"value": "allow-cluster-create"}, {"value": "allow-instance-pool-create"}]',
        )
    ]


def test_snapshot_with_group_not_created_in_account_console_should_be_filtered():
    backend = MockBackend()
    wsclient = MagicMock()
    group = Group(
        id="1",
        display_name="de",
        meta=ResourceMeta(resource_type="WorkspaceGroup"),
        members=[ComplexValue(display="test-user-1", value="20"), ComplexValue(display="test-user-2", value="21")],
        roles=[
            ComplexValue(value="arn:aws:iam::123456789098:instance-profile/ip1"),
            ComplexValue(value="arn:aws:iam::123456789098:instance-profile/ip2"),
        ],
        entitlements=[ComplexValue(value="allow-cluster-create"), ComplexValue(value="allow-instance-pool-create")],
    )
    wsclient.groups.list.return_value = [group]
    account_admins_group = Group(id="1234", display_name="ds")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_admins_group]],
    }
    res = GroupManager(backend, wsclient, inventory_database="inv").snapshot()
    assert res == []


def test_snapshot_with_group_already_migrated_should_be_filtered():
    backend = MockBackend()
    wsclient = MagicMock()
    group = Group(
        id="1",
        display_name="de",
        meta=ResourceMeta(resource_type="Group"),
        members=[ComplexValue(display="test-user-1", value="20"), ComplexValue(display="test-user-2", value="21")],
        roles=[
            ComplexValue(value="arn:aws:iam::123456789098:instance-profile/ip1"),
            ComplexValue(value="arn:aws:iam::123456789098:instance-profile/ip2"),
        ],
        entitlements=[ComplexValue(value="allow-cluster-create"), ComplexValue(value="allow-instance-pool-create")],
    )
    wsclient.groups.list.return_value = [group]
    account_admins_group = Group(id="1234", display_name="de")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_admins_group]],
    }
    res = GroupManager(backend, wsclient, inventory_database="inv").snapshot()
    assert res == []


def test_snapshot_should_filter_account_system_groups():
    backend = MockBackend()
    wsclient = MagicMock()
    group = Group(
        id="1",
        display_name="de",
        meta=ResourceMeta(resource_type="WorkspaceGroup"),
        members=[ComplexValue(display="test-user-1", value="20"), ComplexValue(display="test-user-2", value="21")],
        roles=[
            ComplexValue(value="arn:aws:iam::123456789098:instance-profile/ip1"),
            ComplexValue(value="arn:aws:iam::123456789098:instance-profile/ip2"),
        ],
        entitlements=[ComplexValue(value="allow-cluster-create"), ComplexValue(value="allow-instance-pool-create")],
    )
    wsclient.groups.list.return_value = [group]
    account_admins_group = Group(id="1234", display_name="account users")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_admins_group]],
    }
    res = GroupManager(backend, wsclient, inventory_database="inv").snapshot()
    assert res == []


def test_snapshot_should_filter_workspace_system_groups():
    backend = MockBackend()
    wsclient = MagicMock()
    group = Group(
        id="1",
        display_name="admins",
        meta=ResourceMeta(resource_type="WorkspaceGroup"),
        members=[ComplexValue(display="test-user-1", value="20"), ComplexValue(display="test-user-2", value="21")],
        roles=[
            ComplexValue(value="arn:aws:iam::123456789098:instance-profile/ip1"),
            ComplexValue(value="arn:aws:iam::123456789098:instance-profile/ip2"),
        ],
        entitlements=[ComplexValue(value="allow-cluster-create"), ComplexValue(value="allow-instance-pool-create")],
    )
    wsclient.groups.list.return_value = [group]
    account_admins_group = Group(id="1234", display_name="de")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_admins_group]],
    }
    res = GroupManager(backend, wsclient, inventory_database="inv").snapshot()
    assert res == []


def test_snapshot_should_consider_groups_defined_in_conf():
    backend = MockBackend()
    wsclient = MagicMock()
    group1 = Group(id="1", display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    group2 = Group(id="2", display_name="ds", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [group1, group2]
    account_admins_group_1 = Group(id="11", display_name="de")
    account_admins_group_2 = Group(id="12", display_name="ds")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_admins_group_1, account_admins_group_2]],
    }
    res = GroupManager(backend, wsclient, inventory_database="inv", include_group_names=["de"]).snapshot()
    assert res == [
        MigratedGroup(
            id_in_workspace="1",
            name_in_workspace="de",
            name_in_account="de",
            temporary_name="ucx-renamed-de",
            members=None,
            external_id="11",
            roles=None,
            entitlements=None,
        )
    ]


def test_snapshot_should_rename_groups_defined_in_conf():
    backend = MockBackend()
    wsclient = MagicMock()
    group1 = Group(id="1", display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    group2 = Group(id="2", display_name="ds", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [group1, group2]
    account_admins_group_1 = Group(id="11", display_name="de")
    account_admins_group_2 = Group(id="12", display_name="ds")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_admins_group_1, account_admins_group_2]],
    }
    res = GroupManager(backend, wsclient, inventory_database="inv", renamed_group_prefix="test-group-").snapshot()
    assert res == [
        MigratedGroup(
            id_in_workspace="1",
            name_in_workspace="de",
            name_in_account="de",
            temporary_name="test-group-de",
            members=None,
            external_id="11",
            roles=None,
            entitlements=None,
        ),
        MigratedGroup(
            id_in_workspace="2",
            name_in_workspace="ds",
            name_in_account="ds",
            temporary_name="test-group-ds",
            members=None,
            external_id="12",
            roles=None,
            entitlements=None,
        ),
    ]


def test_rename_groups_should_patch_eligible_groups():
    backend = MockBackend()
    wsclient = MagicMock()
    group1 = Group(id="1", display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [
        group1,
    ]
    account_admins_group_1 = Group(id="11", display_name="de")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_admins_group_1]],
    }
    GroupManager(backend, wsclient, inventory_database="inv", renamed_group_prefix="test-group-").rename_groups()
    wsclient.groups.patch.assert_called_with(
        "1",
        operations=[iam.Patch(iam.PatchOp.REPLACE, "displayName", "test-group-de")],
    )


def test_rename_groups_should_filter_account_groups_in_workspace():
    backend = MockBackend()
    wsclient = MagicMock()
    group1 = Group(id="1", display_name="de", meta=ResourceMeta(resource_type="Group"))
    wsclient.groups.list.return_value = [group1]
    account_group1 = Group(id="11", display_name="de")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_group1]],
    }
    GroupManager(backend, wsclient, inventory_database="inv").rename_groups()
    wsclient.groups.patch.assert_not_called()


def test_rename_groups_should_filter_already_renamed_groups():
    backend = MockBackend(rows={"SELECT": [("1", "de", "de", "test-group-de", "", "", "", "")]})
    wsclient = MagicMock()
    group1 = Group(id="1", display_name="test-group-de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [group1]

    GroupManager(backend, wsclient, inventory_database="inv", renamed_group_prefix="test-group-").rename_groups()
    wsclient.groups.patch.assert_not_called()


def test_reflect_account_groups_on_workspace_should_be_called_for_eligible_groups():
    backend = MockBackend(rows={"SELECT": [("1", "de", "de", "test-group-de", "", "", "", "")]})
    wsclient = MagicMock()
    account_group = Group(id="1", display_name="de")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_group]],
    }

    group1 = Group(id="1", display_name="test-dfd-de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [group1]

    (GroupManager(backend, wsclient, inventory_database="inv").reflect_account_groups_on_workspace())

    wsclient.api_client.do.assert_called_with(
        "PUT", "/api/2.0/preview/permissionassignments/principals/1", data='{"permissions": ["USER"]}'
    )
