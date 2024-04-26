import json
from datetime import timedelta
from unittest.mock import Mock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.errors import InternalError, NotFound, PermissionDenied
from databricks.sdk.service import sql

from databricks.labs.ucx.workspace_access import redash
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.redash import (
    Listing,
    Permissions,
    RedashPermissionsSupport,
)

# pylint: disable=protected-access


def test_crawlers():
    ws = create_autospec(WorkspaceClient)

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
        for ot in (sql.ObjectType.ALERT, sql.ObjectType.QUERY, sql.ObjectType.DASHBOARD)
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
        assert item.object_type in {"alerts", "dashboards", "queries"}
        assert item.raw is not None


def test_apply(migration_state):
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.ALERT,
        object_id="test",
        access_control_list=[
            sql.AccessControl(
                group_name="test",
                permission_level=sql.PermissionLevel.CAN_EDIT,
            ),
            sql.AccessControl(
                group_name="db-temp-test",
                permission_level=sql.PermissionLevel.CAN_EDIT,
            ),
            sql.AccessControl(
                group_name="irrelevant",
                permission_level=sql.PermissionLevel.CAN_MANAGE,
            ),
            sql.AccessControl(
                user_name="test-user",
                permission_level=sql.PermissionLevel.CAN_RUN,
            ),
            sql.AccessControl(
                group_name="no-corresponding-account-group",
                permission_level=sql.PermissionLevel.CAN_RUN,
            ),
        ],
    )
    ws.dbsql_permissions.set.return_value = sql.GetResponse(
        object_type=sql.ObjectType.ALERT,
        object_id="test",
        access_control_list=[
            sql.AccessControl(
                group_name="test",
                permission_level=sql.PermissionLevel.CAN_EDIT,
            ),
            sql.AccessControl(
                group_name="db-temp-test",
                permission_level=sql.PermissionLevel.CAN_EDIT,
            ),
            sql.AccessControl(
                group_name="irrelevant",
                permission_level=sql.PermissionLevel.CAN_MANAGE,
            ),
            sql.AccessControl(
                user_name="test-user",
                permission_level=sql.PermissionLevel.CAN_RUN,
            ),
            sql.AccessControl(
                group_name="no-corresponding-account-group",
                permission_level=sql.PermissionLevel.CAN_RUN,
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
                        permission_level=sql.PermissionLevel.CAN_EDIT,
                    ),
                    sql.AccessControl(
                        group_name="irrelevant",
                        permission_level=sql.PermissionLevel.CAN_MANAGE,
                    ),
                    sql.AccessControl(
                        user_name="test-user",
                        permission_level=sql.PermissionLevel.CAN_RUN,
                    ),
                    sql.AccessControl(
                        group_name="no-corresponding-account-group",
                        permission_level=sql.PermissionLevel.CAN_RUN,
                    ),
                ],
            ).as_dict()
        ),
    )
    task = sup.get_apply_task(item, migration_state)
    task()
    assert ws.dbsql_permissions.set.call_count == 1
    expected_payload = [
        sql.AccessControl(
            group_name="test",
            permission_level=sql.PermissionLevel.CAN_EDIT,
        ),
        sql.AccessControl(
            group_name="db-temp-test",
            permission_level=sql.PermissionLevel.CAN_EDIT,
        ),
        sql.AccessControl(
            group_name="irrelevant",
            permission_level=sql.PermissionLevel.CAN_MANAGE,
        ),
        sql.AccessControl(
            user_name="test-user",
            permission_level=sql.PermissionLevel.CAN_RUN,
        ),
        sql.AccessControl(
            group_name="no-corresponding-account-group",
            permission_level=sql.PermissionLevel.CAN_RUN,
        ),
    ]
    ws.dbsql_permissions.set.assert_called_once_with(
        object_type=sql.ObjectTypePlural.ALERTS, object_id="test", access_control_list=expected_payload
    )


def test_apply_permissions_not_applied(migration_state):
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.get.return_value = None
    ws.dbsql_permissions.set.return_value = sql.GetResponse(
        object_type=sql.ObjectType.ALERT,
        object_id="test",
        access_control_list=[
            sql.AccessControl(
                group_name="test",
                permission_level=sql.PermissionLevel.CAN_MANAGE,
            ),
            sql.AccessControl(
                group_name="db-temp-test",
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
                ],
            ).as_dict()
        ),
    )
    task = sup.get_apply_task(item, migration_state)
    task()
    assert ws.dbsql_permissions.set.call_count == 1


def test_apply_permissions_no_relevant_items(migration_state):
    ws = create_autospec(WorkspaceClient)
    sup = RedashPermissionsSupport(ws=ws, listings=[])
    item = Permissions(
        object_id="test",
        object_type=sql.ObjectTypePlural.ALERTS.value,
        raw=json.dumps(
            sql.GetResponse(
                object_type=sql.ObjectType.ALERT,
                object_id="test",
                access_control_list=[
                    sql.AccessControl(
                        group_name="irrelevant",
                        permission_level=sql.PermissionLevel.CAN_MANAGE,
                    ),
                ],
            ).as_dict()
        ),
    )
    task = sup.get_apply_task(item, migration_state)
    ws.dbsql_permissions.set.assert_not_called()
    assert not task


def test_apply_permissions_no_valid_groups():
    ws = create_autospec(WorkspaceClient)
    migration_state = MigrationState(
        [
            MigratedGroup(
                id_in_workspace="test",
                name_in_workspace="",
                name_in_account="",
                temporary_name="",
                members=None,
                entitlements=None,
                external_id=None,
                roles=None,
            ),
        ]
    )

    ws.dbsql_permissions.set.return_value = sql.GetResponse(
        object_type=sql.ObjectType.ALERT, object_id="test", access_control_list=[]
    )
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.ALERT, object_id="test", access_control_list=[]
    )
    sup = RedashPermissionsSupport(ws=ws, listings=[])
    item = Permissions(
        object_id="test",
        object_type=sql.ObjectTypePlural.ALERTS.value,
        raw=json.dumps(
            sql.GetResponse(
                object_type=sql.ObjectType.ALERT,
                object_id="test",
                access_control_list=[
                    sql.AccessControl(
                        group_name="",
                        permission_level=sql.PermissionLevel.CAN_MANAGE,
                    ),
                ],
            ).as_dict()
        ),
    )
    task = sup.get_apply_task(item, migration_state)
    task()
    assert sup._safe_get_dbsql_permissions(object_type=sql.ObjectTypePlural.ALERTS, object_id="test")
    assert sup._safe_set_permissions(object_type=sql.ObjectTypePlural.ALERTS, object_id="test", acl=[])


def test_safe_getter_known():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.get.side_effect = NotFound(...)
    sup = RedashPermissionsSupport(ws=ws, listings=[])
    assert sup._safe_get_dbsql_permissions(object_type=sql.ObjectTypePlural.ALERTS, object_id="test") is None


def test_safe_setter_known():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.set.side_effect = NotFound(...)
    sup = RedashPermissionsSupport(ws=ws, listings=[])
    assert sup._safe_set_permissions(object_type=sql.ObjectTypePlural.ALERTS, object_id="test", acl=[]) is None


def test_safe_getter_unknown():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.get.side_effect = InternalError(...)
    sup = RedashPermissionsSupport(ws=ws, listings=[])
    with pytest.raises(DatabricksError):
        sup._safe_get_dbsql_permissions(object_type=sql.ObjectTypePlural.ALERTS, object_id="test")


def test_empty_permissions():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.get.side_effect = NotFound(...)
    sup = RedashPermissionsSupport(ws=ws, listings=[])
    assert sup._crawler_task(object_id="test", object_type=sql.ObjectTypePlural.ALERTS) is None


def test_applier_task_should_return_true_if_permission_is_up_to_date():
    ws = create_autospec(WorkspaceClient)
    acl_grp_1 = sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE)
    acl_grp_2 = sql.AccessControl(group_name="group_2", permission_level=sql.PermissionLevel.CAN_MANAGE)
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.QUERY,
        object_id="test",
        access_control_list=[acl_grp_1, acl_grp_2],
    )
    ws.dbsql_permissions.set.return_value = sql.GetResponse(
        object_type=sql.ObjectType.QUERY,
        object_id="test",
        access_control_list=[acl_grp_1, acl_grp_2],
    )

    sup = RedashPermissionsSupport(ws=ws, listings=[])
    result = sup._applier_task(sql.ObjectTypePlural.QUERIES, "test", [acl_grp_1])
    assert result


def test_applier_task_should_return_true_if_permission_is_up_to_date_with_multiple_permissions():
    ws = create_autospec(WorkspaceClient)
    acl_1_grp_1 = sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE)
    acl_2_grp_1 = sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_RUN)
    acl_3_grp_1 = sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_RUN)
    acl_grp_2 = sql.AccessControl(group_name="group_2", permission_level=sql.PermissionLevel.CAN_MANAGE)
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.QUERY,
        object_id="test",
        access_control_list=[acl_1_grp_1, acl_2_grp_1, acl_3_grp_1, acl_grp_2],
    )
    ws.dbsql_permissions.set.return_value = sql.GetResponse(
        object_type=sql.ObjectType.QUERY,
        object_id="test",
        access_control_list=[acl_1_grp_1, acl_2_grp_1, acl_3_grp_1, acl_grp_2],
    )

    sup = RedashPermissionsSupport(ws=ws, listings=[])
    result = sup._applier_task(sql.ObjectTypePlural.QUERIES, "test", [acl_1_grp_1, acl_2_grp_1])
    assert result


def test_applier_task_failed():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.QUERY,
        object_id="test",
        access_control_list=[
            sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE),
            sql.AccessControl(group_name="group_2", permission_level=sql.PermissionLevel.CAN_RUN),
        ],
    )

    sup = RedashPermissionsSupport(
        ws=ws, listings=[], set_permissions_timeout=timedelta(seconds=1), verify_timeout=timedelta(seconds=1)
    )
    with pytest.raises(TimeoutError) as e:
        sup._applier_task(
            sql.ObjectTypePlural.QUERIES,
            "test",
            [sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_RUN)],
        )
    assert "Timed out after" in str(e.value)


def test_applier_task_failed_when_all_permissions_not_up_to_date():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.QUERY,
        object_id="test",
        access_control_list=[
            sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE),
            sql.AccessControl(group_name="group_2", permission_level=sql.PermissionLevel.CAN_RUN),
        ],
    )

    sup = RedashPermissionsSupport(
        ws=ws, listings=[], set_permissions_timeout=timedelta(seconds=1), verify_timeout=timedelta(seconds=1)
    )
    with pytest.raises(TimeoutError) as e:
        sup._applier_task(
            sql.ObjectTypePlural.QUERIES,
            "test",
            [
                sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_RUN),
                sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE),
            ],
        )
    assert "Timed out after" in str(e.value)


def test_applier_task_when_set_error_non_retriable():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.set.side_effect = PermissionDenied()

    sup = RedashPermissionsSupport(
        ws=ws, listings=[], set_permissions_timeout=timedelta(seconds=1), verify_timeout=timedelta(seconds=1)
    )
    with pytest.raises(TimeoutError) as e:
        sup._applier_task(
            sql.ObjectTypePlural.QUERIES,
            "test",
            [
                sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_RUN),
                sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE),
            ],
        )
    assert "Timed out after" in str(e.value)
    ws.dbsql_permissions.get.assert_called()


def test_applier_task_when_set_error_retriable():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.set.side_effect = InternalError()

    sup = RedashPermissionsSupport(
        ws=ws, listings=[], set_permissions_timeout=timedelta(seconds=1), verify_timeout=timedelta(seconds=1)
    )
    with pytest.raises(TimeoutError) as e:
        sup._applier_task(
            sql.ObjectTypePlural.QUERIES,
            "test",
            [
                sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_RUN),
                sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE),
            ],
        )
    assert "Timed out after" in str(e.value)
    ws.dbsql_permissions.get.assert_not_called()


def test_safe_set_permissions_when_error_non_retriable():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.set.side_effect = PermissionDenied(...)
    sup = RedashPermissionsSupport(
        ws=ws, listings=[], set_permissions_timeout=timedelta(seconds=1), verify_timeout=timedelta(seconds=1)
    )
    acl = [sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE)]
    result = sup._safe_set_permissions(sql.ObjectTypePlural.QUERIES, "test", acl)
    assert result is None


def test_safe_set_permissions_when_error_retriable():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.set.side_effect = InternalError(...)
    sup = RedashPermissionsSupport(
        ws=ws, listings=[], set_permissions_timeout=timedelta(seconds=1), verify_timeout=timedelta(seconds=1)
    )
    acl = [sql.AccessControl(group_name="group_1", permission_level=sql.PermissionLevel.CAN_MANAGE)]
    with pytest.raises(InternalError) as e:
        sup._safe_set_permissions(sql.ObjectTypePlural.QUERIES, "test", acl)
    assert e.type == InternalError


def test_load_as_dict():
    ws = create_autospec(WorkspaceClient)

    query_id = "query_test"
    group_name = "group_test"
    user_name = "user_test"

    ws.queries.list.return_value = [
        sql.QueryInfo(
            query_id=query_id,
        )
    ]

    sample_permission = sql.GetResponse(
        object_id=query_id,
        object_type=sql.ObjectType.QUERY,
        access_control_list=[
            sql.AccessControl(group_name=group_name, permission_level=sql.PermissionLevel.CAN_RUN),
            sql.AccessControl(user_name=user_name, permission_level=sql.PermissionLevel.CAN_MANAGE),
            sql.AccessControl(permission_level=sql.PermissionLevel.CAN_MANAGE),
        ],
    )

    ws.dbsql_permissions.get.return_value = sample_permission
    redash_permissions = RedashPermissionsSupport(
        ws,
        [redash.Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES)],
    )

    policy_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query_id)

    assert sql.PermissionLevel.CAN_RUN == policy_permissions[group_name]
    assert sql.PermissionLevel.CAN_MANAGE == policy_permissions[user_name]
    assert sql.PermissionLevel.CAN_MANAGE == policy_permissions["UNKNOWN"]


def test_load_as_dict_permissions_not_found():
    ws = create_autospec(WorkspaceClient)

    sup = RedashPermissionsSupport(
        ws=ws,
        listings=[],
    )

    ws.permissions.get.side_effect = Mock(side_effect=NotFound(...))

    policy_permissions = sup.load_as_dict(sql.ObjectTypePlural.QUERIES, "query_test")

    assert len(policy_permissions) == 0


def test_load_as_dict_no_acls():
    ws = create_autospec(WorkspaceClient)

    query_id = "query_test"

    ws.queries.list.return_value = [
        sql.QueryInfo(
            query_id=query_id,
        )
    ]

    sample_permission = sql.GetResponse(
        object_id=query_id,
        object_type=sql.ObjectType.QUERY,
        access_control_list=[],
    )

    ws.dbsql_permissions.get.return_value = sample_permission
    redash_permissions = RedashPermissionsSupport(
        ws,
        [redash.Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES)],
    )

    policy_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query_id)

    assert len(policy_permissions) == 0


def test_load_as_dict_handle_exception_when_getting_permissions():
    ws = create_autospec(WorkspaceClient)

    sup = RedashPermissionsSupport(
        ws=ws,
        listings=[],
    )

    ws.permissions.get.side_effect = Mock(side_effect=NotFound(...))

    policy_permissions = sup.load_as_dict(sql.ObjectTypePlural.QUERIES, "query_test")

    assert len(policy_permissions) == 0


def test_load_as_dict_no_permissions():
    ws = create_autospec(WorkspaceClient)

    sup = RedashPermissionsSupport(
        ws=ws,
        listings=[],
    )

    ws.dbsql_permissions.get.return_value = None

    policy_permissions = sup.load_as_dict(sql.ObjectTypePlural.QUERIES, "query_test")

    assert len(policy_permissions) == 0


def test_load_as_dict_no_permission_level():
    ws = create_autospec(WorkspaceClient)

    query_id = "query_test"
    group_name = "group_test"

    ws.queries.list.return_value = [
        sql.QueryInfo(
            query_id=query_id,
        )
    ]

    sample_permission = sql.GetResponse(
        object_id=query_id,
        object_type=sql.ObjectType.QUERY,
        access_control_list=[sql.AccessControl(group_name=group_name)],
    )

    ws.dbsql_permissions.get.return_value = sample_permission
    redash_permissions = RedashPermissionsSupport(
        ws,
        [redash.Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES)],
    )

    policy_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query_id)

    assert len(policy_permissions) == 0


def test_verify_task_should_return_true_if_permissions_applied():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.ALERT,
        object_id="test",
        access_control_list=[
            sql.AccessControl(
                group_name="test",
                permission_level=sql.PermissionLevel.CAN_EDIT,
            )
        ],
    )

    sup = RedashPermissionsSupport(ws=ws, listings=[])
    item = Permissions(
        object_id="test",
        object_type=sql.ObjectTypePlural.ALERTS.value,
        raw=json.dumps(
            sql.GetResponse(
                object_type=sql.ObjectType.ALERT,
                object_id="test",
                access_control_list=[
                    sql.AccessControl(
                        group_name="test",
                        permission_level=sql.PermissionLevel.CAN_EDIT,
                    )
                ],
            ).as_dict()
        ),
    )
    task = sup.get_verify_task(item)
    result = task()

    assert result


def test_verify_task_should_return_false_if_permissions_not_found():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.get.side_effect = NotFound(...)

    sup = RedashPermissionsSupport(ws=ws, listings=[])
    item = Permissions(
        object_id="test",
        object_type=sql.ObjectTypePlural.ALERTS.value,
        raw=json.dumps(
            sql.GetResponse(
                object_type=sql.ObjectType.ALERT,
                object_id="test",
                access_control_list=[
                    sql.AccessControl(
                        group_name="test",
                        permission_level=sql.PermissionLevel.CAN_EDIT,
                    )
                ],
            ).as_dict()
        ),
    )
    task = sup.get_verify_task(item)
    result = task()

    assert not result


def test_verify_task_should_fail_if_permissions_not_matching():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.ALERT,
        object_id="test",
        access_control_list=[
            sql.AccessControl(
                group_name="test",
                permission_level=sql.PermissionLevel.CAN_MANAGE,
            )
        ],
    )

    sup = RedashPermissionsSupport(ws=ws, listings=[])
    item = Permissions(
        object_id="test",
        object_type=sql.ObjectTypePlural.ALERTS.value,
        raw=json.dumps(
            sql.GetResponse(
                object_type=sql.ObjectType.ALERT,
                object_id="test",
                access_control_list=[
                    sql.AccessControl(
                        group_name="test",
                        permission_level=sql.PermissionLevel.CAN_EDIT,
                    )
                ],
            ).as_dict()
        ),
    )
    task = sup.get_verify_task(item)

    with pytest.raises(NotFound):
        task()


def test_verify_task_should_fail_if_acl_empty():
    ws = create_autospec(WorkspaceClient)

    sup = RedashPermissionsSupport(ws=ws, listings=[])
    item = Permissions(
        object_id="test",
        object_type=sql.ObjectTypePlural.ALERTS.value,
        raw=json.dumps(
            sql.GetResponse(object_type=sql.ObjectType.ALERT, object_id="test", access_control_list=[]).as_dict()
        ),
    )

    with pytest.raises(ValueError):
        sup.get_verify_task(item)
    ws.dbsql_permissions.set.assert_not_called()


def test_verify_task_should_fail_if_acl_missing():
    ws = create_autospec(WorkspaceClient)
    ws.dbsql_permissions.get.return_value = sql.GetResponse(
        object_type=sql.ObjectType.ALERT,
        object_id="test",
        access_control_list=None,
    )

    sup = RedashPermissionsSupport(ws=ws, listings=[])
    item = Permissions(
        object_id="test",
        object_type=sql.ObjectTypePlural.ALERTS.value,
        raw=json.dumps(
            sql.GetResponse(
                object_type=sql.ObjectType.ALERT,
                object_id="test",
                access_control_list=[
                    sql.AccessControl(
                        group_name="test",
                        permission_level=sql.PermissionLevel.CAN_EDIT,
                    )
                ],
            ).as_dict()
        ),
    )
    task = sup.get_verify_task(item)

    with pytest.raises(AssertionError):
        task()
