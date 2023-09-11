import json
from unittest.mock import MagicMock

import pytest
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import sql

from databricks.labs.ucx.inventory.types import PermissionsInventoryItem
from databricks.labs.ucx.support.sql import SqlPermissionsSupport, listing_wrapper


def test_crawlers():
    ws = MagicMock()

    ws.alerts.list.return_value = [
        sql.Alert(
            id="test",
        )
    ]
    ws.queries.list.return_value = [
        sql.Query(
            id="test",
        )
    ]
    ws.dashboards.list.return_value = [sql.Dashboard(id="test")]

    sample_acl = [
        sql.AccessControl(
            group_name="test",
            permission_level=sql.PermissionLevel.CAN_MANAGE,
        )
    ]

    ws.dbsql_permissions.get.side_effect = [
        sql.GetResponse(object_type=ot, object_id="test", access_control_list=sample_acl)
        for ot in [sql.ObjectType.ALERT, sql.ObjectType.QUERY, sql.ObjectType.DASHBOARD]
    ]

    sup = SqlPermissionsSupport(
        ws=ws,
        listings=[
            listing_wrapper(ws.alerts.list, sql.ObjectTypePlural.ALERTS),
            listing_wrapper(ws.dashboards.list, sql.ObjectTypePlural.DASHBOARDS),
            listing_wrapper(ws.queries.list, sql.ObjectTypePlural.QUERIES),
        ],
    )

    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 3
    ws.alerts.list.assert_called_once()
    ws.queries.list.assert_called_once()
    ws.dashboards.list.assert_called_once()
    for task in tasks:
        item = task()
        assert item.object_id == "test"
        assert item.support in ["alerts", "dashboards", "queries"]
        assert item.raw_object_permissions is not None


def test_apply(migration_state):
    ws = MagicMock()
    sup = SqlPermissionsSupport(ws=ws, listings=[])
    item = PermissionsInventoryItem(
        object_id="test",
        support="alerts",
        raw_object_permissions=json.dumps(
            sql.GetResponse(
                object_type=sql.ObjectType.ALERT,
                object_id="test",
                access_control_list=[
                    sql.AccessControl(
                        group_name="test",
                        permission_level=sql.PermissionLevel.CAN_MANAGE,
                    ),
                    sql.AccessControl(
                        group_name="irrelevant",
                        permission_level=sql.PermissionLevel.CAN_MANAGE,
                    ),
                ],
            ).as_dict()
        ),
    )
    task = sup.get_apply_task(item, migration_state, "backup")
    task()
    assert ws.dbsql_permissions.set.call_count == 1
    expected_payload = [
        sql.AccessControl(
            group_name="db-temp-test",
            permission_level=sql.PermissionLevel.CAN_MANAGE,
        ),
        sql.AccessControl(
            group_name="irrelevant",
            permission_level=sql.PermissionLevel.CAN_MANAGE,
        ),
    ]
    ws.dbsql_permissions.set.assert_called_once_with(
        object_type=sql.ObjectTypePlural.ALERTS, object_id="test", access_control_list=expected_payload
    )


def test_safe_getter_known():
    ws = MagicMock()
    ws.dbsql_permissions.get.side_effect = DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")
    sup = SqlPermissionsSupport(ws=ws, listings=[])
    assert sup._safe_get_dbsql_permissions(object_type=sql.ObjectTypePlural.ALERTS, object_id="test") is None


def test_safe_getter_unknown():
    ws = MagicMock()
    ws.dbsql_permissions.get.side_effect = DatabricksError(error_code="SOMETHING_NON_EXPECTED")
    sup = SqlPermissionsSupport(ws=ws, listings=[])
    with pytest.raises(DatabricksError):
        sup._safe_get_dbsql_permissions(object_type=sql.ObjectTypePlural.ALERTS, object_id="test")


def test_empty_permissions():
    ws = MagicMock()
    ws.dbsql_permissions.get.side_effect = DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")
    sup = SqlPermissionsSupport(ws=ws, listings=[])
    assert sup._crawler_task(object_id="test", object_type=sql.ObjectTypePlural.ALERTS) is None
