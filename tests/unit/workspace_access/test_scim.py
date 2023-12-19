from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from databricks.sdk.core import DatabricksError
from databricks.sdk.errors import InternalError, PermissionDenied
from databricks.sdk.service import iam
from databricks.sdk.service.iam import Group, PatchOp, PatchSchema, ResourceMeta

from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.scim import ScimSupport


def test_applier_task_should_return_true_if_roles_are_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(id="1", roles=[iam.ComplexValue(value="role1"), iam.ComplexValue(value="role2")])
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))

    result = sup._applier_task(group_id="1", value=[iam.ComplexValue(value="role1")], property_name="roles")
    assert result


def test_applier_task_should_return_true_if_entitlements_are_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(
        id="1", roles=[iam.ComplexValue(value="role1")], entitlements=[iam.ComplexValue(value="allow-cluster-create")]
    )
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))

    result = sup._applier_task(
        group_id="1", value=[iam.ComplexValue(value="allow-cluster-create")], property_name="entitlements"
    )
    assert result


def test_applier_task_should_return_false_if_roles_are_not_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(id="1", roles=[iam.ComplexValue(value="role2")])
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))

    with pytest.raises(TimeoutError) as e:
        sup._applier_task(group_id="1", value=[iam.ComplexValue(value="role1")], property_name="roles")
    assert "Timed out after" in str(e.value)


def test_applier_task_should_return_false_if_entitlements_are_not_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(id="1", entitlements=[iam.ComplexValue(value="allow-cluster-create")])
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))

    with pytest.raises(TimeoutError) as e:
        sup._applier_task(
            group_id="1", value=[iam.ComplexValue(value="forbidden-cluster-create")], property_name="entitlements"
        )
    assert "Timed out after" in str(e.value)


def test_applier_task_when_get_error_retriable():
    ws = MagicMock()
    ws.groups.get.side_effect = InternalError(error_code="INTERNAL_SERVER_ERROR")
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))
    group_id = "1"
    with pytest.raises(TimeoutError) as e:
        sup._applier_task(
            group_id=group_id, value=[iam.ComplexValue(value="forbidden-cluster-create")], property_name="entitlements"
        )
    assert "Timed out" in str(e.value)


def test_applier_task_when_get_error_non_retriable():
    ws = MagicMock()
    ws.groups.get.side_effect = PermissionDenied(...)
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))
    group_id = "1"
    result = sup._applier_task(
        group_id=group_id, value=[iam.ComplexValue(value="forbidden-cluster-create")], property_name="entitlements"
    )
    assert result is False


def test_safe_patch_group_when_error_non_retriable():
    ws = MagicMock()
    ws.groups.patch.side_effect = PermissionDenied(...)
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))
    operations = [
        iam.Patch(op=iam.PatchOp.ADD, path="roles", value=[e.as_dict() for e in [iam.ComplexValue(value="role1")]])
    ]
    schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
    result = sup._safe_patch_group(group_id="1", operations=operations, schemas=schemas)
    assert result is None


def test_safe_patch_group_when_error_retriable():
    ws = MagicMock()
    ws.groups.patch.side_effect = InternalError(...)
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))
    operations = [
        iam.Patch(op=iam.PatchOp.ADD, path="roles", value=[e.as_dict() for e in [iam.ComplexValue(value="role1")]])
    ]
    schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
    with pytest.raises(DatabricksError) as e:
        sup._safe_patch_group(group_id="1", operations=operations, schemas=schemas)
    assert e.type == InternalError


def test_safe_get_group_when_error_non_retriable():
    ws = MagicMock()
    ws.groups.get.side_effect = PermissionDenied(...)
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))
    result = sup._safe_get_group(group_id="1")
    assert result is None


def test_safe_get_group_when_error_retriable():
    ws = MagicMock()
    ws.groups.get.side_effect = InternalError(...)
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))
    with pytest.raises(DatabricksError) as e:
        sup._safe_get_group(group_id="1")
    assert e.type == InternalError


def test_get_crawler_task_with_roles_and_entitlements_should_be_crawled():
    ws = MagicMock()
    ws.groups.list.return_value = [
        Group(
            id="1",
            display_name="de",
            roles=[iam.ComplexValue(value="role1"), iam.ComplexValue(value="role2")],
            entitlements=[iam.ComplexValue(value="forbidden-cluster-create")],
            meta=ResourceMeta(resource_type="WorkspaceGroup"),
        )
    ]
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))

    result = list(sup.get_crawler_tasks())
    assert len(result) == 2
    assert result[0]() == Permissions(
        object_id="1", object_type="roles", raw='[{"value": "role1"}, {"value": "role2"}]'
    )
    assert result[1]() == Permissions(
        object_id="1", object_type="entitlements", raw='[{"value": "forbidden-cluster-create"}]'
    )


def test_groups_without_roles_and_entitlements_should_be_ignored():
    ws = MagicMock()
    ws.groups.list.return_value = [Group(id="1", display_name="de")]
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))

    result = list(sup.get_crawler_tasks())
    assert len(result) == 0


def test_get_apply_task_should_call_patch_on_group_external_id():
    ws = MagicMock()
    ws.groups.list.return_value = [
        Group(
            id="1",
            display_name="de",
            entitlements=[iam.ComplexValue(value="forbidden-cluster-create")],
            meta=ResourceMeta(resource_type="WorkspaceGroup"),
        ),
        Group(id="12", display_name="ANOTHER", meta=ResourceMeta(resource_type="Group")),
    ]
    ws.groups.get.return_value = Group(
        id="1", display_name="de", entitlements=[iam.ComplexValue(value="forbidden-cluster-create")]
    )
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))

    item = Permissions(object_id="1", object_type="entitlements", raw='[{"value": "forbidden-cluster-create"}]')
    mggrp = MigratedGroup(
        id_in_workspace="1",
        name_in_workspace="de",
        name_in_account="ANOTHER",
        temporary_name="ucx-temp-de",
        external_id="12",
    )
    appliers = sup.get_apply_task(item, MigrationState([mggrp]))
    appliers()

    ws.groups.patch.assert_called_once_with(
        "12",
        operations=[iam.Patch(op=PatchOp.ADD, path="entitlements", value=[{"value": "forbidden-cluster-create"}])],
        schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )


def test_get_apply_task_should_ignore_groups_not_in_migration_state():
    ws = MagicMock()
    ws.groups.get.return_value = Group(
        id="1", display_name="de", entitlements=[iam.ComplexValue(value="forbidden-cluster-create")]
    )
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))

    item = Permissions(object_id="1", object_type="entitlements", raw='[{"value": "forbidden-cluster-create"}]')
    mggrp = MigratedGroup(
        id_in_workspace="2",
        name_in_workspace="de",
        name_in_account="de",
        temporary_name="ucx-temp-de",
        external_id="12",
    )
    assert sup.get_apply_task(item, MigrationState([mggrp])) is None
