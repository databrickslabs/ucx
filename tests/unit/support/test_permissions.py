import json
from unittest.mock import MagicMock

import pytest
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import compute, iam

from databricks.labs.ucx.inventory.types import (
    PermissionsInventoryItem,
    RequestObjectType,
)
from databricks.labs.ucx.support.permissions import (
    GenericPermissionsSupport,
    listing_wrapper,
)


def test_crawler():
    ws = MagicMock()
    ws.clusters.list.return_value = [
        compute.ClusterDetails(
            cluster_id="test",
        )
    ]

    sample_permission = iam.ObjectPermissions(
        object_id="test",
        object_type=str(RequestObjectType.CLUSTERS),
        access_control_list=[
            iam.AccessControlResponse(
                group_name="test",
                all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
            )
        ],
    )

    ws.permissions.get.return_value = sample_permission

    sup = GenericPermissionsSupport(
        ws=ws,
        listings=[
            listing_wrapper(ws.clusters.list, "cluster_id", RequestObjectType.CLUSTERS),
        ],
    )

    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 1
    ws.clusters.list.assert_called_once()
    _task = tasks[0]
    item = _task()
    ws.permissions.get.assert_called_once()
    assert item.object_id == "test"
    assert item.support == "clusters"
    assert json.loads(item.raw_object_permissions) == sample_permission.as_dict()


def test_apply(migration_state):
    ws = MagicMock()
    sup = GenericPermissionsSupport(ws=ws, listings=[])  # no listings since only apply is tested

    item = PermissionsInventoryItem(
        object_id="test",
        support="clusters",
        raw_object_permissions=json.dumps(
            iam.ObjectPermissions(
                object_id="test",
                object_type=str(RequestObjectType.CLUSTERS),
                access_control_list=[
                    iam.AccessControlResponse(
                        group_name="test",
                        all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
                    ),
                    iam.AccessControlResponse(
                        group_name="irrelevant",
                        all_permissions=[
                            iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_MANAGE)
                        ],
                    ),
                ],
            ).as_dict()
        ),
    )

    _task = sup.get_apply_task(item, migration_state, "backup")
    _task()
    ws.permissions.update.assert_called_once()

    expected_acl_payload = [
        iam.AccessControlRequest(
            group_name="db-temp-test",
            permission_level=iam.PermissionLevel.CAN_USE,
        )
    ]

    ws.permissions.update.assert_called_with(RequestObjectType.CLUSTERS, "test", expected_acl_payload)


def test_relevance():
    sup = GenericPermissionsSupport(ws=MagicMock(), listings=[])  # no listings since only apply is tested
    result = sup.is_item_relevant(
        item=PermissionsInventoryItem(object_id="passwords", support="passwords", raw_object_permissions="some-stuff"),
        migration_state=MagicMock(),
    )
    assert result is True


def test_safe_get():
    ws = MagicMock()
    ws.permissions.get.side_effect = DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")
    sup = GenericPermissionsSupport(ws=ws, listings=[])
    result = sup._safe_get_permissions(ws, RequestObjectType.CLUSTERS, "test")
    assert result is None

    ws.permissions.get.side_effect = DatabricksError(error_code="SOMETHING_UNEXPECTED")
    with pytest.raises(DatabricksError):
        sup._safe_get_permissions(ws, RequestObjectType.CLUSTERS, "test")


def test_no_permissions():
    ws = MagicMock()
    ws.clusters.list.return_value = [
        compute.ClusterDetails(
            cluster_id="test",
        )
    ]
    ws.permissions.get.side_effect = DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")
    sup = GenericPermissionsSupport(
        ws=ws,
        listings=[
            listing_wrapper(ws.clusters.list, "cluster_id", RequestObjectType.CLUSTERS),
        ],
    )
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 1
    ws.clusters.list.assert_called_once()
    _task = tasks[0]
    item = _task()
    assert item is None
