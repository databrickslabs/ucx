from unittest import mock
from unittest.mock import MagicMock

import pytest

from databricks.labs.ucx.inventory.permissions import PermissionManager
from databricks.labs.ucx.inventory.permissions_inventory import (
    PermissionsInventoryTable,
)
from databricks.labs.ucx.supports.impl import get_supports


@pytest.fixture(scope="function")
def spark_mixin():
    with mock.patch("databricks.labs.ucx.providers.spark.SparkMixin._initialize_spark", MagicMock()):
        yield


def test_manager_init(spark_mixin):
    pm = PermissionManager(ws=MagicMock(), permissions_inventory=PermissionsInventoryTable("test", MagicMock()))
    assert pm.supports == {}


def test_manager_set_supports(spark_mixin):
    pm = PermissionManager(ws=MagicMock(), permissions_inventory=PermissionsInventoryTable("test", MagicMock()))
    supports = get_supports(ws=MagicMock(), workspace_start_path="/", num_threads=1)
    pm.set_supports(supports)
    assert pm.supports == supports
