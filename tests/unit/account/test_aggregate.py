import logging
from unittest.mock import create_autospec
from databricks.sdk import Workspace, WorkspaceClient
from databricks.labs.ucx.account.aggregate import AccountAggregate
from databricks.labs.ucx.account.workspaces import AccountWorkspaces

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext
from databricks.sdk.service import sql



def test_basic_readiness_report_no_workspaces(acc_client, caplog):
    account_ws = AccountWorkspaces(acc_client)
    account_aggregate_obj = AccountAggregate(account_ws)

    with caplog.at_level(logging.INFO):
        account_aggregate_obj.readiness_report()

    assert 'UC compatibility' in caplog.text


def test_readiness_report_ucx_installed(acc_client, caplog):
    account_ws = AccountWorkspaces(acc_client)
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc")
    ]

    ws = create_autospec(WorkspaceClient)
    acc_client.get_workspace_client.return_value = ws
    ws.statement_execution.execute_statement.return_value = sql.ExecuteStatementResponse(
        status=sql.StatementStatus(state=sql.StatementState.SUCCEEDED),
        result=sql.ResultData(
            data_array=[
                [
                    "tables",
                    "32432",
                    """["cluster type not supported : LEGACY_TABLE_ACL", "cluster type not supported : LEGACY_SINGLE_USER"]""",
                ],
                [
                    "clusters",
                    "234234234",
                    """["cluster type not supported : LEGACY_TABLE_ACL", "cluster type not supported : LEGACY_SINGLE_USER"]""",
                ],
            ],
            row_count=2,
        ),
        manifest=sql.ResultManifest(
            schema=sql.ResultSchema(
                columns=[
                    sql.ColumnInfo(name="object_type", type_name=sql.ColumnInfoTypeName.STRING),
                    sql.ColumnInfo(name="object_id", type_name=sql.ColumnInfoTypeName.STRING),
                    sql.ColumnInfo(name="failures", type_name=sql.ColumnInfoTypeName.STRING),
                ],
                column_count=3,
            )
        ),
        statement_id='123',
    )

    ctx = WorkspaceContext(ws).replace(config=WorkspaceConfig(inventory_database="something", warehouse_id="1234"))
    account_aggregate_obj = AccountAggregate(account_ws, workspace_context_factory=lambda _: ctx)

    with caplog.at_level(logging.INFO):
        account_aggregate_obj.readiness_report()

    assert 'UC compatibility' in caplog.text
