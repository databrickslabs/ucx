import logging
from unittest.mock import create_autospec
from databricks.sdk import Workspace, WorkspaceClient
from databricks.labs.ucx.account.aggregate import AccountAggregate
from databricks.labs.ucx.account.workspaces import AccountWorkspaces
from databricks.labs.lsql.backends import MockBackend, SqlBackend, StatementExecutionBackend
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.ucx.contexts.cli_command import WorkspaceContext
from databricks.sdk.service import iam, sql, jobs



def test_basic_readiness_report_no_workspaces(acc_client, caplog):
    account_ws = AccountWorkspaces(acc_client)
    account_aggregate_obj = AccountAggregate(account_ws)

    with caplog.at_level(logging.INFO):
        account_aggregate_obj.readiness_report()

    assert 'UC compatibility' in caplog.text


def test_readiness_report_ucx_installed(acc_client, caplog):
    account_ws = AccountWorkspaces(acc_client)
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123, workspace_status_message="Running", deployment_name="abc")]

    account_aggregate_obj = AccountAggregate(account_ws)
    ws = create_autospec(WorkspaceClient)
    acc_client.get_workspace_client.return_value = ws
    ws.statement_execution.execute_statement.return_value \
        = sql.ExecuteStatementResponse(status=sql.StatementStatus(
            state=sql.StatementState.SUCCEEDED),
            result=sql.ResultData(data_array=[
                ["jobs", "123134", """["cluster type not supported : LEGACY_TABLE_ACL", "cluster type not supported : LEGACY_SINGLE_USER"]"]"""],
                ["clusters", "0325-3423-dfs", "[]"]]))

    with caplog.at_level(logging.INFO):
        account_aggregate_obj.readiness_report()

    assert 'UC compatibility' in caplog.text
