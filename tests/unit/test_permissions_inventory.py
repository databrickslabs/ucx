from unittest.mock import Mock

import pandas as pd
import pytest
from pyspark.sql.types import StringType, StructField, StructType

from databricks.labs.ucx.inventory.permissions_inventory import (
    PermissionsInventoryTable,
)
from databricks.labs.ucx.inventory.types import (
    LogicalObjectType,
    PermissionsInventoryItem,
    RequestObjectType,
)


@pytest.fixture
def workspace_client():
    client = Mock()
    return client


perm_items = [PermissionsInventoryItem("object1", LogicalObjectType.CLUSTER, RequestObjectType.CLUSTERS, "test acl")]


@pytest.fixture
def permissions_inventory(workspace_client, mocker):
    mocker.patch("databricks.labs.ucx.providers.spark.SparkMixin._initialize_spark", Mock())
    return PermissionsInventoryTable("test_database", workspace_client)


def test_inventory_table_manager_init(permissions_inventory):
    assert str(permissions_inventory._table) == "hive_metastore.test_database.permissions"


def test_table_schema(permissions_inventory):
    schema = StructType(
        [
            StructField("object_id", StringType(), True),
            StructField("crawler", StringType(), True),
            StructField("raw_object_permissions", StringType(), True),
            StructField("raw_extras", StringType(), True),
        ]
    )
    assert permissions_inventory._table_schema == schema


def test_table(permissions_inventory):
    assert permissions_inventory._df == permissions_inventory.spark.table("test_catalog.test_database.permissions")


def test_cleanup(permissions_inventory):
    permissions_inventory.cleanup()
    permissions_inventory.spark.sql.assert_called_with("DROP TABLE IF EXISTS hive_metastore.test_database.permissions")


def test_save(permissions_inventory):
    permissions_inventory.save(perm_items)
    permissions_inventory.spark.createDataFrame.assert_called_once()


def test_load_all(permissions_inventory):
    items = pd.DataFrame(
        {
            "object_id": ["object1"],
            "crawler": ["clusters"],
            "raw_object_permissions": ["test acl"],
        }
    )
    permissions_inventory._df.toPandas.return_value = items
    output = permissions_inventory.load_all()
    assert output[0] == PermissionsInventoryItem("object1", crawler="clusters", raw_object_permissions="test acl")
