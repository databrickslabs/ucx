import json
from unittest.mock import Mock

import pandas as pd
import pytest
from databricks.sdk.service.iam import AccessControlResponse, ObjectPermissions
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
            StructField("logical_object_type", StringType(), True),
            StructField("request_object_type", StringType(), True),
            StructField("raw_object_permissions", StringType(), True),
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
            "logical_object_type": ["CLUSTER"],
            "request_object_type": ["clusters"],
            "raw_object_permissions": ["test acl"],
        }
    )
    permissions_inventory._df.toPandas.return_value = items
    output = permissions_inventory.load_all()
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
def test_is_item_relevant_to_groups(permissions_inventory, items, groups, status):
    assert permissions_inventory._is_item_relevant_to_groups(items, groups) is status


def test_is_item_relevant_to_groups_exception(permissions_inventory):
    item = PermissionsInventoryItem("object1", "FOO", "BAR", "test acl")
    with pytest.raises(NotImplementedError):
        permissions_inventory._is_item_relevant_to_groups(item, ["g1"])


def test_load_for_groups(permissions_inventory):
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
    permissions_inventory._df.toPandas.return_value = items
    output = permissions_inventory.load_for_groups(groups)
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
