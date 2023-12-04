from unittest.mock import MagicMock

import pytest
from _pytest.outcomes import fail
from databricks.sdk.errors import DatabricksError
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
            roles='[{"value": "arn:aws:iam::123456789098:instance-profile/ip1"}, '
            '{"value": "arn:aws:iam::123456789098:instance-profile/ip2"}]',
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
    group = Group(id="1", display_name="admins", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [group]
    acc_group = Group(id="1234", display_name="de")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [acc_group]],
    }
    res = GroupManager(backend, wsclient, inventory_database="inv").snapshot()
    assert res == []


def test_snapshot_should_consider_groups_defined_in_conf():
    backend = MockBackend()
    wsclient = MagicMock()
    group1 = Group(id="1", display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    group2 = Group(id="2", display_name="ds", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [group1, group2]
    acc_group_1 = Group(id="11", display_name="de")
    acc_group_2 = Group(id="12", display_name="ds")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [acc_group_1, acc_group_2]],
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


def test_snapshot_should_filter_system_groups_defined_in_conf():
    backend = MockBackend()
    wsclient = MagicMock()
    group1 = Group(id="1", display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [group1]
    acc_group_1 = Group(id="11", display_name="de")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [acc_group_1]],
    }
    res = GroupManager(backend, wsclient, inventory_database="inv", include_group_names=["admins"]).snapshot()
    assert res == []


def test_snapshot_should_filter_groups_defined_in_conf_not_present_in_workspace():
    backend = MockBackend()
    wsclient = MagicMock()
    group1 = Group(id="1", display_name="ds", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [group1]
    acc_group_1 = Group(id="11", display_name="de")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [acc_group_1]],
    }
    res = GroupManager(backend, wsclient, inventory_database="inv", include_group_names=["de"]).snapshot()
    assert res == []


def test_snapshot_should_filter_groups_defined_in_conf_not_present_in_account():
    backend = MockBackend()
    wsclient = MagicMock()
    group1 = Group(id="1", display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [group1]
    acc_group_1 = Group(id="11", display_name="ds")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [acc_group_1]],
    }
    res = GroupManager(backend, wsclient, inventory_database="inv", include_group_names=["de"]).snapshot()
    assert res == []


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
    backend = MockBackend(rows={"SELECT": [("1", "de", "de", "test-group-de", "", "", "", "")]})
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


def test_rename_groups_should_fail_if_error_is_thrown():
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
    wsclient.groups.patch.side_effect = RuntimeError("Something bad")
    gm = GroupManager(backend, wsclient, inventory_database="inv", renamed_group_prefix="test-group-")
    with pytest.raises(RuntimeWarning) as e:
        gm.rename_groups()
    assert e.value.args[0] == "During rename of workspace groups got 1 errors. See debug logs"


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


def test_reflect_account_groups_on_workspace_should_filter_account_groups_in_workspace():
    backend = MockBackend(rows={"SELECT": [("1", "de", "de", "test-group-de", "", "", "", "")]})
    wsclient = MagicMock()
    group1 = Group(id="1", display_name="de", meta=ResourceMeta(resource_type="Group"))
    wsclient.groups.list.return_value = [group1]
    account_group1 = Group(id="11", display_name="de")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_group1]],
    }
    GroupManager(backend, wsclient, inventory_database="inv").reflect_account_groups_on_workspace()

    with pytest.raises(AssertionError):
        wsclient.api_client.do.assert_called_with("PUT")


def test_reflect_account_groups_on_workspace_should_filter_account_groups_not_in_account():
    backend = MockBackend(rows={"SELECT": [("1", "de", "de", "test-group-de", "", "", "", "")]})
    wsclient = MagicMock()
    group1 = Group(id="1", display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [group1]
    account_group1 = Group(id="11", display_name="ds")
    wsclient.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_group1]],
    }
    GroupManager(backend, wsclient, inventory_database="inv").reflect_account_groups_on_workspace()

    with pytest.raises(AssertionError):
        wsclient.api_client.do.assert_called_with("PUT")


def test_reflect_account_should_fail_if_error_is_thrown():
    backend = MockBackend(rows={"SELECT": [("1", "de", "de", "test-group-de", "", "", "", "")]})
    wsclient = MagicMock()
    account_group = Group(id="1", display_name="de")

    def do_side_effect(*args, **kwargs):
        if args[0] == "get":
            return {"Resources": [g.as_dict() for g in [account_group]]}
        else:
            raise RuntimeError()

    wsclient.api_client.do.side_effect = do_side_effect

    group1 = Group(id="1", display_name="test-dfd-de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [group1]
    gm = GroupManager(backend, wsclient, inventory_database="inv")

    with pytest.raises(RuntimeWarning):
        gm.reflect_account_groups_on_workspace()


def test_delete_original_workspace_groups_should_delete_relected_acc_groups_in_workspace():
    account_id = "11"
    ws_id = "1"
    backend = MockBackend(rows={"SELECT": [(ws_id, "de", "de", "test-group-de", account_id, "", "", "")]})
    wsclient = MagicMock()

    temp_group = Group(id=ws_id, display_name="test-group-de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    reflected_group = Group(id=account_id, display_name="de", meta=ResourceMeta(resource_type="Group"))
    wsclient.groups.list.return_value = [temp_group, reflected_group]

    GroupManager(backend, wsclient, inventory_database="inv").delete_original_workspace_groups()
    wsclient.groups.delete.assert_called_with(id=ws_id)


def test_delete_original_workspace_groups_should_not_delete_groups_not_renamed():
    account_id = "11"
    ws_id = "1"
    backend = MockBackend(rows={"SELECT": [(ws_id, "de", "de", "test-group-de", account_id, "", "", "")]})
    wsclient = MagicMock()

    temp_group = Group(id=ws_id, display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    reflected_group = Group(id=account_id, display_name="de", meta=ResourceMeta(resource_type="Group"))
    wsclient.groups.list.return_value = [temp_group, reflected_group]

    GroupManager(backend, wsclient, inventory_database="inv").delete_original_workspace_groups()
    wsclient.groups.delete.assert_not_called()


def test_delete_original_workspace_groups_should_not_delete_groups_not_reflected_to_workspace():
    account_id = "11"
    ws_id = "1"
    backend = MockBackend(rows={"SELECT": [(ws_id, "de", "de", "test-group-de", account_id, "", "", "")]})
    wsclient = MagicMock()

    temp_group = Group(id=ws_id, display_name="test-group-de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    wsclient.groups.list.return_value = [temp_group]

    GroupManager(backend, wsclient, inventory_database="inv").delete_original_workspace_groups()
    wsclient.groups.delete.assert_not_called()


def test_delete_original_workspace_groups_should_not_fail_if_target_group_doesnt_exist():
    account_id = "11"
    ws_id = "1"
    backend = MockBackend(rows={"SELECT": [(ws_id, "de", "de", "test-group-de", account_id, "", "", "")]})
    wsclient = MagicMock()

    temp_group = Group(id=ws_id, display_name="test-group-de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    reflected_group = Group(id=account_id, display_name="de", meta=ResourceMeta(resource_type="Group"))
    wsclient.groups.list.return_value = [temp_group, reflected_group]

    wsclient.groups.delete.side_effect = DatabricksError(message="None Group with id 100 not found")
    gm = GroupManager(backend, wsclient, inventory_database="inv")

    try:
        gm.delete_original_workspace_groups()
    except DatabricksError:
        fail("delete_original_workspace_groups() raised DatabricksError unexpectedly!")


def test_delete_original_workspace_groups_should_fail_if_delete_does_not_work():
    account_id = "11"
    ws_id = "1"
    backend = MockBackend(rows={"SELECT": [(ws_id, "de", "de", "test-group-de", account_id, "", "", "")]})
    wsclient = MagicMock()

    temp_group = Group(id=ws_id, display_name="test-group-de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    reflected_group = Group(id=account_id, display_name="de", meta=ResourceMeta(resource_type="Group"))
    wsclient.groups.list.return_value = [temp_group, reflected_group]

    wsclient.groups.delete.side_effect = RuntimeError("Something bad")
    gm = GroupManager(backend, wsclient, inventory_database="inv")

    with pytest.raises(RuntimeWarning):
        gm.delete_original_workspace_groups()


@pytest.mark.parametrize('status_code,include_retry_after',
                         ((429, False),
                          (429, True),
                          (503, False),
                          (503, True)))
def test_blah(status_code, include_retry_after):
    print(f'{status_code}, {include_retry_after}')
    assert False