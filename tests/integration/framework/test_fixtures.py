import logging

from databricks.sdk.errors import InvalidParameterValue
from databricks.sdk.service import iam


logger = logging.getLogger(__name__)
_SPARK_CONF = {
    "spark.databricks.cluster.profile": "singleNode",
}


def test_dbfs_fixture(make_mounted_location):
    logger.info(f"Created new dbfs data copy:{make_mounted_location}")


def test_creating_lakeview_dashboard_permissions(
    make_lakeview_dashboard,
    # The `_permissions` fixtures are generated following a pattern resulting in an argument with too many characters
    make_lakeview_dashboard_permissions,  # pylint: disable=invalid-name
    make_group,
):
    # Only the last permission in the list is visible in the Databricks UI
    permissions = [
        iam.PermissionLevel.CAN_EDIT,
        iam.PermissionLevel.CAN_RUN,
        iam.PermissionLevel.CAN_MANAGE,
        iam.PermissionLevel.CAN_READ,
    ]
    dashboard = make_lakeview_dashboard()
    group = make_group()
    for permission in permissions:
        try:
            make_lakeview_dashboard_permissions(
                object_id=dashboard.dashboard_id,
                permission_level=permission,
                group_name=group.display_name,
            )
        except InvalidParameterValue as e:
            assert False, f"Could not create {permission} permission for lakeview dashboard: {e}"
    assert True, "Could create all fixtures"
