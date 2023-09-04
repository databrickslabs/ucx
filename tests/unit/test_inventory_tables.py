import json
from unittest.mock import Mock

import pandas as pd
import pytest
from databricks.sdk.service.iam import AccessControlResponse, ObjectPermissions
from pyspark.sql.types import StringType, StructField, StructType

from databricks.labs.ucx.config import InventoryConfig, InventoryTable
from databricks.labs.ucx.inventory.table import InventoryTableManager
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
def inventory_table_manager(workspace_client, mocker):
    mocker.patch("databricks.labs.ucx.providers.spark.SparkMixin._initialize_spark", Mock())
    config = InventoryConfig(
        InventoryTable(catalog="test_catalog", database="test_database", name="test_inventory_table")
    )
    return InventoryTableManager(config, workspace_client)


def test_inventory_table_manager_init(inventory_table_manager):
    assert str(inventory_table_manager.config.table) == "test_catalog.test_database.test_inventory_table"


def test_table_schema(inventory_table_manager):
    schema = StructType(
        [
            StructField("object_id", StringType(), True),
            StructField("logical_object_type", StringType(), True),
            StructField("request_object_type", StringType(), True),
            StructField("raw_object_permissions", StringType(), True),
        ]
    )
    assert inventory_table_manager._table_schema == schema


def test_table(inventory_table_manager):
    assert inventory_table_manager._table == inventory_table_manager.spark.table(
        "test_catalog.test_database.test_inventory_table"
    )


def test_cleanup(inventory_table_manager):
    inventory_table_manager.cleanup()
    inventory_table_manager.spark.sql.assert_called_with(
        "DROP TABLE IF EXISTS test_catalog.test_database.test_inventory_table"
    )


def test_save(inventory_table_manager):
    inventory_table_manager.save(perm_items)
    inventory_table_manager.spark.createDataFrame.assert_called_once()


def test_load_all(inventory_table_manager):
    items = pd.DataFrame(
        {
            "object_id": ["object1"],
            "logical_object_type": ["CLUSTER"],
            "request_object_type": ["clusters"],
            "raw_object_permissions": ["test acl"],
        }
    )
    inventory_table_manager._table.toPandas.return_value = items
    output = inventory_table_manager.load_all()
    assert output[0] == PermissionsInventoryItem(
        "object1", LogicalObjectType.CLUSTER, RequestObjectType.CLUSTERS, "test acl"
    )


@pytest.mark.parametrize(
    "items,groups,status",
    [
        (
            PermissionsInventoryItem(
                object_id="group1",
                logical_object_type=LogicalObjectType.CLUSTER,
                request_object_type=RequestObjectType.CLUSTERS,
                raw_object_permissions=json.dumps(
                    ObjectPermissions(
                        object_id="clusterid1",
                        object_type="clusters",
                        access_control_list=[
                            AccessControlResponse.from_dict(
                                {"all_permissions": [{"permission_level": "CAN_MANAGE"}], "group_name": "group1"}
                            ),
                            AccessControlResponse.from_dict(
                                {"all_permissions": [{"permission_level": "CAN_MANAGE"}], "group_name": "admin"}
                            ),
                        ],
                    ).as_dict()
                ),
            ),
            ["group1", "group2"],
            True,
        ),
        (
            PermissionsInventoryItem(
                object_id="group1",
                logical_object_type=LogicalObjectType.ROLES,
                request_object_type=None,
                raw_object_permissions=json.dumps(
                    {
                        "roles": [
                            {"value": "arn:aws:iam::123456789:instance-profile/test-uc-role"},
                            {"value": "arn:aws:iam::123456789:instance-profile/test-uc-role2"},
                        ],
                        "entitlements": [{"value": "workspace-access"}],
                    }
                ),
            ),
            ["group1", "group2"],
            True,
        ),
        (
            PermissionsInventoryItem(
                object_id="scope-1",
                logical_object_type=LogicalObjectType.SECRET_SCOPE,
                request_object_type=None,
                raw_object_permissions="""{"acls": [
                    {"principal": "g1", "permission": "READ"},
                    {"principal": "unrelated-group", "permission": "READ"},
                    {"principal": "admins", "permission": "MANAGE"}
                ]}""",
            ),
            ["group1", "group2"],
            False,
        ),
        (
            PermissionsInventoryItem(
                object_id="scope-1",
                logical_object_type=LogicalObjectType.SECRET_SCOPE,
                request_object_type=None,
                raw_object_permissions="""{"acls": [
                    {"principal": "g1", "permission": "READ"},
                    {"principal": "unrelated-group", "permission": "READ"},
                    {"principal": "admins", "permission": "MANAGE"}
                ]}""",
            ),
            ["g1", "group2"],
            True,
        ),
    ],
)
def test_is_item_relevant_to_groups(inventory_table_manager, items, groups, status):
    assert inventory_table_manager._is_item_relevant_to_groups(items, groups) is status


def test_is_item_relevant_to_groups_exception(inventory_table_manager):
    item = PermissionsInventoryItem("object1", "FOO", "BAR", "test acl")
    with pytest.raises(NotImplementedError):
        inventory_table_manager._is_item_relevant_to_groups(item, ["g1"])


def test_load_for_groups(inventory_table_manager):
    items = pd.DataFrame(
        {
            "object_id": ["group1"],
            "logical_object_type": ["CLUSTER"],
            "request_object_type": ["clusters"],
            "raw_object_permissions": json.dumps(
                ObjectPermissions(
                    object_id="clusterid1",
                    object_type="clusters",
                    access_control_list=[
                        AccessControlResponse.from_dict(
                            {"all_permissions": [{"permission_level": "CAN_MANAGE"}], "group_name": "group1"}
                        )
                    ],
                ).as_dict()
            ),
        }
    )
    groups = ["group1", "group2"]
    inventory_table_manager._table.toPandas.return_value = items
    output = inventory_table_manager.load_for_groups(groups)
    assert output[0] == PermissionsInventoryItem(
        object_id="group1",
        logical_object_type=LogicalObjectType.CLUSTER,
        request_object_type=RequestObjectType.CLUSTERS,
        raw_object_permissions=json.dumps(
            ObjectPermissions(
                object_id="clusterid1",
                object_type="clusters",
                access_control_list=[
                    AccessControlResponse.from_dict(
                        {"all_permissions": [{"permission_level": "CAN_MANAGE"}], "group_name": "group1"}
                    )
                ],
            ).as_dict()
        ),
    )
    assert len(output) == 1
