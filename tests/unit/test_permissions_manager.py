import json
from unittest import mock
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service import iam

from databricks.labs.ucx.inventory.permissions import PermissionManager
from databricks.labs.ucx.inventory.permissions_inventory import (
    PermissionsInventoryTable,
)
from databricks.labs.ucx.inventory.types import PermissionsInventoryItem
from databricks.labs.ucx.support.impl import SupportsProvider


@pytest.fixture(scope="function")
def spark_mixin():
    with mock.patch("databricks.labs.ucx.providers.spark.SparkMixin._initialize_spark", MagicMock()):
        yield


def test_manager_inventorize(spark_mixin):
    sup = SupportsProvider(ws=MagicMock(), num_threads=1, workspace_start_path="/")
    pm = PermissionManager(
        ws=MagicMock(), permissions_inventory=PermissionsInventoryTable("test", MagicMock()), supports_provider=sup
    )

    with mock.patch("databricks.labs.ucx.inventory.permissions.ThreadedExecution.run", MagicMock()) as run_mock:
        pm.inventorize_permissions()
        run_mock.assert_called_once()


def test_manager_apply(spark_mixin):
    sup = SupportsProvider(ws=MagicMock(), num_threads=1, workspace_start_path="/")
    inventory = MagicMock(spec=PermissionsInventoryTable)
    inventory.load_all.return_value = [
        PermissionsInventoryItem(
            object_id="test",
            support="clusters",
            raw_object_permissions=json.dumps(
                iam.ObjectPermissions(
                    object_id="test",
                    object_type="clusters",
                    access_control_list=[
                        iam.AccessControlResponse(
                            group_name="test",
                            all_permissions=[
                                iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)
                            ],
                        )
                    ],
                ).as_dict()
            ),
        ),
        PermissionsInventoryItem(
            object_id="test2",
            support="cluster-policies",
            raw_object_permissions=json.dumps(
                iam.ObjectPermissions(
                    object_id="test",
                    object_type="cluster-policies",
                    access_control_list=[
                        iam.AccessControlResponse(
                            group_name="test",
                            all_permissions=[
                                iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)
                            ],
                        )
                    ],
                ).as_dict()
            ),
        ),
    ]
    pm = PermissionManager(ws=MagicMock(), permissions_inventory=inventory, supports_provider=sup)
    with mock.patch("databricks.labs.ucx.inventory.permissions.ThreadedExecution.run", MagicMock()) as run_mock:
        pm.apply_group_permissions(migration_state=MagicMock(), destination="backup")
        run_mock.assert_called_once()


def test_unregistered_support():
    sup = SupportsProvider(ws=MagicMock(), num_threads=1, workspace_start_path="/")
    inventory = MagicMock(spec=PermissionsInventoryTable)
    inventory.load_all.return_value = [
        PermissionsInventoryItem(object_id="test", support="SOME_NON_EXISTENT", raw_object_permissions="")
    ]
    pm = PermissionManager(ws=MagicMock(), permissions_inventory=inventory, supports_provider=sup)
    with pytest.raises(ValueError):
        pm.apply_group_permissions(migration_state=MagicMock(), destination="backup")
