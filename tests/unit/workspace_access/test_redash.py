import json
from unittest.mock import MagicMock, call

import pytest
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import sql

from databricks.labs.ucx.workspace_access.redash import (
    Listing,
    Permissions,
    RedashPermissionsSupport,
)


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

    sup = RedashPermissionsSupport(
        ws=ws,
        listings=[
            Listing(ws.alerts.list, sql.ObjectTypePlural.ALERTS),
            Listing(ws.dashboards.list, sql.ObjectTypePlural.DASHBOARDS),
            Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES),
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
        assert item.object_type in ["alerts", "dashboards", "queries"]
        assert item.raw is not None


def test_apply(migration_state):
    ws = MagicMock()
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.ALERT,
        object_id="test",
        access_control_list=[
            sql.AccessControl(
                group_name="db-temp-test",
                permission_level=sql.PermissionLevel.CAN_MANAGE,
            ),
            sql.AccessControl(
                group_name="irrelevant",
                permission_level=sql.PermissionLevel.CAN_MANAGE,
            ),
        ],
    )
    sup = RedashPermissionsSupport(ws=ws, listings=[])
    item = Permissions(
        object_id="test",
        object_type="alerts",
        raw=json.dumps(
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
    sup = RedashPermissionsSupport(ws=ws, listings=[])
    assert sup._safe_get_dbsql_permissions(object_type=sql.ObjectTypePlural.ALERTS, object_id="test") is None


def test_safe_getter_unknown():
    ws = MagicMock()
    ws.dbsql_permissions.get.side_effect = DatabricksError(error_code="SOMETHING_NON_EXPECTED")
    sup = RedashPermissionsSupport(ws=ws, listings=[])
    with pytest.raises(DatabricksError):
        sup._safe_get_dbsql_permissions(object_type=sql.ObjectTypePlural.ALERTS, object_id="test")


def test_empty_permissions():
    ws = MagicMock()
    ws.dbsql_permissions.get.side_effect = DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")
    sup = RedashPermissionsSupport(ws=ws, listings=[])
    assert sup._crawler_task(object_id="test", object_type=sql.ObjectTypePlural.ALERTS) is None


def test_applier_task_should_return_true_if_permission_is_up_to_date():
    ws = MagicMock()
    acl_grp_1 = sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE)
    acl_grp_2 = sql.AccessControl(group_name="group_2", permission_level=sql.PermissionLevel.CAN_MANAGE)
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.QUERY,
        object_id="test",
        access_control_list=[acl_grp_1, acl_grp_2],
    )

    sup = RedashPermissionsSupport(ws=ws, listings=[])
    result = sup._applier_task(sql.ObjectTypePlural.QUERIES, "test", [acl_grp_1])
    assert result


def test_applier_task_should_return_true_if_permission_is_up_to_date_with_multiple_permissions():
    ws = MagicMock()
    acl_1_grp_1 = sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE)
    acl_2_grp_1 = sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_RUN)
    acl_3_grp_1 = sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_RUN)
    acl_grp_2 = sql.AccessControl(group_name="group_2", permission_level=sql.PermissionLevel.CAN_MANAGE)
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.QUERY,
        object_id="test",
        access_control_list=[acl_1_grp_1, acl_2_grp_1, acl_3_grp_1, acl_grp_2],
    )

    sup = RedashPermissionsSupport(ws=ws, listings=[])
    result = sup._applier_task(sql.ObjectTypePlural.QUERIES, "test", [acl_1_grp_1, acl_2_grp_1])
    assert result


def test_applier_task_should_return_false_if_permission_are_not_up_to_date():
    ws = MagicMock()
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.QUERY,
        object_id="test",
        access_control_list=[
            sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE),
            sql.AccessControl(group_name="group_2", permission_level=sql.PermissionLevel.CAN_RUN),
        ],
    )

    sup = RedashPermissionsSupport(ws=ws, listings=[])
    result = sup._applier_task(
        sql.ObjectTypePlural.QUERIES,
        "test",
        [sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_RUN)],
    )
    assert not result


def test_applier_task_should_return_false_if_all_permissions_are_not_up_to_date():
    ws = MagicMock()
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.QUERY,
        object_id="test",
        access_control_list=[
            sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE),
            sql.AccessControl(group_name="group_2", permission_level=sql.PermissionLevel.CAN_RUN),
        ],
    )

    sup = RedashPermissionsSupport(ws=ws, listings=[])
    result = sup._applier_task(
        sql.ObjectTypePlural.QUERIES,
        "test",
        [
            sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_RUN),
            sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE),
        ],
    )
    assert not result


def test_applier_task_should_be_called_three_times_if_permission_is_not_up_to_date():
    ws = MagicMock()
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.QUERY,
        object_id="test",
        access_control_list=[
            sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE),
            sql.AccessControl(group_name="group_2", permission_level=sql.PermissionLevel.CAN_RUN),
        ],
    )

    sup = RedashPermissionsSupport(ws=ws, listings=[])
    input_acl = sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_RUN)

    sup._applier_task(
        sql.ObjectTypePlural.QUERIES,
        "test",
        [input_acl],
    )

    assert len(ws.dbsql_permissions.set.mock_calls) == 1
    assert ws.dbsql_permissions.set.mock_calls == [
        call(object_type=sql.ObjectTypePlural.QUERIES, object_id="test", access_control_list=[input_acl]),
    ]
    assert len(ws.dbsql_permissions.get.mock_calls) == 1
    assert ws.dbsql_permissions.set.mock_calls == [
        call(object_type=sql.ObjectTypePlural.QUERIES, object_id="test", access_control_list=[input_acl]),
    ]
