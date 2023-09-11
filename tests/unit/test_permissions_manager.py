from unittest import mock
from unittest.mock import MagicMock

import pytest

from databricks.labs.ucx.inventory.permissions import PermissionManager
from databricks.labs.ucx.inventory.permissions_inventory import (
    PermissionsInventoryTable,
)
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
    pm = PermissionManager(
        ws=MagicMock(), permissions_inventory=PermissionsInventoryTable("test", MagicMock()), supports_provider=sup
    )
    with mock.patch("databricks.labs.ucx.inventory.permissions.ThreadedExecution.run", MagicMock()) as run_mock:
        pm.apply_group_permissions(migration_state=MagicMock(), destination="backup")
        run_mock.assert_called_once()
