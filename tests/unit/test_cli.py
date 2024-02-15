import io
import json
import subprocess
from unittest.mock import create_autospec, patch

import pytest
import yaml
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam, sql

from databricks.labs.ucx.cli import (
    alias,
    create_table_mapping,
    ensure_assessment_run,
    installations,
    manual_workspace_info,
    move,
    open_remote_config,
    repair_run,
    revert_migrated_tables,
    save_storage_and_principal,
    skip,
    sync_workspace_info,
    validate_external_locations,
    validate_groups_membership,
    workflows,
)


@pytest.fixture
def ws():
    state = {
        "/Users/foo/.ucx/config.yml": yaml.dump(
            {
                'version': 2,
                'inventory_database': 'ucx',
                'warehouse_id': 'test',
                'connect': {
                    'host': 'foo',
                    'token': 'bar',
                },
            }
        ),
        '/Users/foo/.ucx/state.json': json.dumps({'resources': {'jobs': {'assessment': '123'}}}),
    }

    def download(path: str) -> io.StringIO:
        if path not in state:
            raise NotFound(path)
        return io.StringIO(state[path])

    ws = create_autospec(WorkspaceClient)
    ws.config.host = 'https://localhost'
    ws.current_user.me().user_name = "foo"
    ws.workspace.download = download
    ws.statement_execution.execute_statement.return_value = sql.ExecuteStatementResponse(
        status=sql.StatementStatus(state=sql.StatementState.SUCCEEDED),
        manifest=sql.ResultManifest(schema=sql.ResultSchema()),
    )
    return ws


def test_workflow(ws, caplog):
    workflows(ws)
    assert "Fetching deployed jobs..." in caplog.messages
    ws.jobs.list_runs.assert_called_once()


def test_open_remote_config(ws):
    with patch("webbrowser.open") as mock_webbrowser_open:
        open_remote_config(ws)
        mock_webbrowser_open.assert_called_with('https://localhost/#workspace/Users/foo/.ucx/config.yml')


def test_installations(ws, capsys):
    ws.users.list.return_value = [iam.User(user_name='foo')]
    installations(ws)
    assert '{"database": "ucx", "path": "/Users/foo/.ucx", "warehouse_id": "test"}' in capsys.readouterr().out


def test_skip_with_table(ws):
    skip(ws, "schema", "table")

    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id='test',
        statement="ALTER TABLE schema.table SET TBLPROPERTIES('databricks.labs.ucx.skip' = true)",
        byte_limit=None,
        catalog=None,
        schema=None,
        disposition=sql.Disposition.INLINE,
        format=sql.Format.JSON_ARRAY,
        wait_timeout=None,
    )


def test_skip_with_schema(ws):
    skip(ws, "schema", None)

    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id='test',
        statement="ALTER SCHEMA schema SET DBPROPERTIES('databricks.labs.ucx.skip' = true)",
        byte_limit=None,
        catalog=None,
        schema=None,
        disposition=sql.Disposition.INLINE,
        format=sql.Format.JSON_ARRAY,
        wait_timeout=None,
    )


def test_skip_no_schema(ws, caplog):
    skip(ws, schema=None, table="table")

    assert '--schema is a required parameter.' in caplog.messages


def test_sync_workspace_info():
    with (
        patch("databricks.labs.ucx.account.AccountWorkspaces.sync_workspace_info", return_value=None) as s,
        patch("databricks.labs.ucx.account.AccountWorkspaces.__init__", return_value=None),
    ):
        # TODO: rewrite with mocks, not patches
        a = create_autospec(AccountClient)
        sync_workspace_info(a)
        s.assert_called_once()


def test_manual_workspace_info(ws):
    with patch("databricks.labs.ucx.account.WorkspaceInfo.manual_workspace_info", return_value=None) as m:
        manual_workspace_info(ws)
        m.assert_called_once()


def test_create_table_mapping(ws):
    with (
        patch("databricks.labs.ucx.account.WorkspaceInfo._current_workspace_id", return_value=123),
        pytest.raises(ValueError, match='databricks labs ucx sync-workspace-info'),
    ):
        create_table_mapping(ws)


def test_validate_external_locations(ws):
    validate_external_locations(ws)

    ws.statement_execution.execute_statement.assert_called()


def test_ensure_assessment_run(ws):
    ensure_assessment_run(ws)

    ws.jobs.list_runs.assert_called_once()


def test_repair_run(ws):
    repair_run(ws, "assessment")

    ws.jobs.list_runs.assert_called_once()


def test_no_step_in_repair_run(ws):
    with pytest.raises(KeyError):
        repair_run(ws, "")


def test_revert_migrated_tables(ws, caplog):
    # test with no schema and no table, user confirm to not retry
    with patch("databricks.labs.blueprint.tui.Prompts.confirm", return_value=False):
        assert revert_migrated_tables(ws, schema=None, table=None) is None
    # test with no schema and no table, user confirm to retry, but no ucx installation found
    with patch("databricks.labs.blueprint.tui.Prompts.confirm", return_value=True):
        assert revert_migrated_tables(ws, schema=None, table=None) is None
        assert 'No migrated tables were found.' in caplog.messages


def test_move_no_catalog(ws, caplog):
    move(ws, "", "", "", "", "")

    assert 'Please enter from_catalog and to_catalog details' in caplog.messages


def test_move_same_schema(ws, caplog):
    move(ws, "SrcCat", "SrcS", "*", "SrcCat", "SrcS")

    assert 'please select a different schema or catalog to migrate to' in caplog.messages


def test_move_no_schema(ws, caplog):
    move(ws, "SrcCat", "", "*", "TgtCat", "")

    assert (
        'Please enter from_schema, to_schema and from_table (enter * for migrating all tables) details.'
        in caplog.messages
    )


def test_move(ws):
    with patch("databricks.labs.blueprint.tui.Prompts.confirm", return_value=True):
        move(ws, "SrcC", "SrcS", "*", "TgtC", "ToS")

    ws.tables.list.assert_called_once()


def test_alias_no_catalog(ws, caplog):
    alias(ws, "", "", "", "", "")

    assert "Please enter from_catalog and to_catalog details" in caplog.messages


def test_alias_same_schema(ws, caplog):
    alias(ws, "SrcCat", "SrcS", "*", "SrcCat", "SrcS")

    assert 'please select a different schema or catalog to migrate to' in caplog.messages


def test_alias_no_schema(ws, caplog):
    alias(ws, "SrcCat", "", "*", "TgtCat", "")

    assert (
        'Please enter from_schema, to_schema and from_table (enter * for migrating all tables) details.'
        in caplog.messages
    )


def test_alias(ws):
    with patch("databricks.labs.blueprint.tui.Prompts.confirm", return_value=True):
        alias(ws, "SrcC", "SrcS", "*", "TgtC", "ToS")

    ws.tables.list.assert_called_once()


def test_save_storage_and_principal_azure_no_azure_cli(ws, caplog):
    ws.config.auth_type = "azure_clis"
    ws.config.is_azure = True
    save_storage_and_principal(ws, "")

    assert 'In order to obtain AAD token, Please run azure cli to authenticate.' in caplog.messages


def test_save_storage_and_principal_azure_no_subscription_id(ws, caplog):
    ws.config.auth_type = "azure_cli"
    ws.config.is_azure = True

    save_storage_and_principal(ws, "")

    assert "Please enter subscription id to scan storage account in." in caplog.messages


def test_save_storage_and_principal_azure(ws, caplog, mocker):
    ws.config.auth_type = "azure_cli"
    ws.config.is_azure = True
    azure_resource = mocker.patch("databricks.labs.ucx.azure.access.AzureResourcePermissions.save_spn_permissions")
    save_storage_and_principal(ws, "test")
    azure_resource.assert_called_once()


def test_validate_groups_membership(ws):
    validate_groups_membership(ws)
    ws.groups.list.assert_called()


def test_save_storage_and_principal_aws_no_profile(ws, caplog, mocker):
    mocker.patch("shutil.which", return_value="/path/aws")
    ws.config.is_azure = False
    ws.config.is_aws = True
    save_storage_and_principal(ws)
    assert any({"AWS Profile is not specified." in message for message in caplog.messages})


def test_save_storage_and_principal_aws_no_connection(ws, mocker):
    mocker.patch("shutil.which", return_value="/path/aws")
    pop = create_autospec(subprocess.Popen)
    ws.config.is_azure = False
    ws.config.is_aws = True
    pop.communicate.return_value = (bytes("message", "utf-8"), bytes("error", "utf-8"))
    pop.returncode = 127
    mocker.patch("subprocess.Popen.__init__", return_value=None)
    mocker.patch("subprocess.Popen.__enter__", return_value=pop)
    mocker.patch("subprocess.Popen.__exit__", return_value=None)

    with pytest.raises(ResourceWarning, match="AWS CLI is not configured properly."):
        save_storage_and_principal(ws, aws_profile="profile")


def test_save_storage_and_principal_aws_no_cli(ws, mocker, caplog):
    mocker.patch("shutil.which", return_value=None)
    ws.config.is_azure = False
    ws.config.is_aws = True
    save_storage_and_principal(ws, aws_profile="profile")
    assert any({"Couldn't find AWS" in message for message in caplog.messages})


def test_save_storage_and_principal_aws(ws, mocker, caplog):
    mocker.patch("shutil.which", return_value=True)
    ws.config.is_azure = False
    ws.config.is_aws = True
    aws_resource = mocker.patch("databricks.labs.ucx.assessment.aws.AWSResourcePermissions.for_cli")
    save_storage_and_principal(ws, aws_profile="profile")
    aws_resource.assert_called_once()


def test_save_storage_and_principal_gcp(ws, caplog):
    ws.config.is_azure = False
    ws.config.is_aws = False
    ws.config.is_gcp = True

    save_storage_and_principal(
        ws,
    )

    assert "This cmd is only supported for azure and aws workspaces" in caplog.messages
