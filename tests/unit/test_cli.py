import io
import json
import subprocess
from unittest.mock import MagicMock, create_autospec, patch

import pytest
import yaml
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam, sql
from databricks.sdk.service.iam import User

from databricks.labs.ucx.cli import (
    CANT_FIND_UCX_MSG,
    alias,
    create_table_mapping,
    ensure_assessment_run,
    installations,
    manual_workspace_info,
    move,
    open_remote_config,
    repair_run,
    revert_migrated_tables,
    save_aws_iam_profiles,
    save_azure_storage_accounts,
    skip,
    sync_workspace_info,
    validate_external_locations,
    validate_groups_membership,
    workflows,
)
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.installer import InstallationUCX


@pytest.fixture
def ws():
    state = {
        "/Users/foo/.ucx/config.yml": yaml.dump(
            {
                '$version': 2,
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
        manifest=sql.ResultManifest(schema=sql.ResultSchema())
    )
    return ws


def test_workflow(ws, caplog):
    workflows(ws)
    assert caplog.messages == ["Fetching deployed jobs..."]
    ws.jobs.list_runs.assert_called_once()


def test_open_remote_config(ws):
    with patch("webbrowser.open") as mock_webbrowser_open:
        open_remote_config(ws)
        mock_webbrowser_open.assert_called_with('https://localhost/#workspace/Users/foo/.ucx/config.yml')


def test_installations(ws, capsys):
    ws.users.list.return_value = [iam.User(user_name='foo')]
    installations(ws)
    assert '{"user_name": "foo", "database": "ucx", "warehouse_id": "test"}' in capsys.readouterr().out


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
        wait_timeout=None
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
        wait_timeout=None
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
    with patch("databricks.labs.ucx.account.WorkspaceInfo._current_workspace_id", return_value=123), pytest.raises(ValueError, match='databricks labs ucx sync-workspace-info'):
        create_table_mapping(ws)


def test_validate_external_locations(mocker):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=MagicMock())
    # test save_as_terraform_definitions_on_workspace is called
    # also test if the saving tf scripts returns None
    with (
        patch(
            "databricks.labs.ucx.hive_metastore.locations.ExternalLocations.save_as_terraform_definitions_on_workspace",
            return_value=None,
        ) as s,
        patch("webbrowser.open") as w,
    ):
        validate_external_locations(w)
        s.assert_called_once()
        w.assert_not_called()
    # test when tf scripts is written and user confirmed to open it over browser
    path = "dummy/external_locations.tf"
    with (
        patch(
            "databricks.labs.ucx.hive_metastore.locations.ExternalLocations.save_as_terraform_definitions_on_workspace",
            return_value=path,
        ) as s,
        patch("webbrowser.open") as w,
        patch("databricks.labs.blueprint.tui.Prompts.confirm", return_value=True),
    ):
        validate_external_locations(w)
        s.assert_called_once()
        w.assert_called_with(f"{w.config.host}/#workspace{path}")
    # test when tf scripts is written but user did not confirm to open it over browser
    with (
        patch(
            "databricks.labs.ucx.hive_metastore.locations.ExternalLocations.save_as_terraform_definitions_on_workspace",
            return_value=path,
        ) as s,
        patch("webbrowser.open") as w,
        patch("databricks.labs.blueprint.tui.Prompts.confirm", return_value=False),
    ):
        validate_external_locations(w)
        s.assert_called_once()
        w.assert_not_called()


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

    assert 'Please enter from_schema, to_schema and from_table (enter * for migrating all tables) details.' in caplog.messages


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

    assert 'Please enter from_schema, to_schema and from_table (enter * for migrating all tables) details.' in caplog.messages


def test_alias(ws):
    with patch("databricks.labs.blueprint.tui.Prompts.confirm", return_value=True):
        alias(ws, "SrcC", "SrcS", "*", "TgtC", "ToS")

    ws.tables.list.assert_called_once()


def test_save_azure_storage_accounts_not_azure(ws, caplog):
    ws.config.is_azure = False

    save_azure_storage_accounts(ws, "")

    assert 'Workspace is not on azure, please run this command on azure databricks workspaces.' in caplog.messages


def test_save_azure_storage_accounts_no_azure_cli(ws, caplog):
    ws.config.auth_type = "azure_clis"

    save_azure_storage_accounts(ws, "")

    assert 'In order to obtain AAD token, Please run azure cli to authenticate.' in caplog.messages


def test_save_azure_storage_accounts_no_subscription_id(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.config.auth_type = "azure_cli"
    w.config.is_azure = True
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=w.current_user)
    save_azure_storage_accounts(w, "")
    assert [
        rec.message
        for rec in caplog.records
        if "Please enter subscription id to scan storage account in." in rec.message
    ]


def test_save_azure_storage_accounts(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.config.auth_type = "azure_cli"
    w.config.is_azure = True
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=w.current_user)
    with patch(
        "databricks.labs.ucx.assessment.azure.AzureResourcePermissions.save_spn_permissions", return_value=None
    ) as m:
        save_azure_storage_accounts(w, "test")
        m.assert_called_once()


def test_validate_groups_membership(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    inst_data = InstallationUCX(
        config=WorkspaceConfig(
            inventory_database="test_database",
            workspace_group_regex=None,
            workspace_group_replace=None,
            account_group_regex=None,
            group_match_by_external_id=False,
            include_group_names=None,
            renamed_group_prefix="db-temp-",
            warehouse_id="test_id",
            database_to_catalog_mapping=None,
            default_catalog="ucx_default",
        ),
        username="test_user",
        path="/Users/test_userd@databricks.com/.ucx",
    )
    w.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.__init__", return_value=None)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=inst_data)
    mocker.patch("databricks.labs.ucx.workspace_access.groups.GroupManager.__init__", return_value=None)
    mocker.patch(
        "databricks.labs.ucx.workspace_access.groups.GroupManager.validate_group_membership",
        return_value={"wf_group_name": "test_group", "ac_group_name": "test_group"},
    )
    validate_groups_membership(w)
    assert caplog.messages == ["Validating Groups which are having different memberships between account and workspace"]


def test_validate_group_no_ucx(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="test_user", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=None)
    validate_groups_membership(w)
    assert "Couldn't find UCX configuration" in caplog.messages[0]


def test_save_aws_iam_profiles_no_profile(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="test_user", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("shutil.which", return_value="/path/aws")
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=None)
    mocker.patch("os.getenv", return_value=None)
    save_aws_iam_profiles(w)
    assert "AWS Profile is not specified." in caplog.messages[0]


def test_save_aws_iam_profiles_no_connection(caplog, mocker):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="test_user", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("shutil.which", return_value="/path/aws")
    pop = create_autospec(subprocess.Popen)

    pop.communicate.return_value = (bytes("message", "utf-8"), bytes("error", "utf-8"))
    pop.returncode = 127
    mocker.patch("subprocess.Popen.__init__", return_value=None)
    mocker.patch("subprocess.Popen.__enter__", return_value=pop)
    mocker.patch("subprocess.Popen.__exit__", return_value=None)
    save_aws_iam_profiles(w, aws_profile="profile")
    assert "AWS CLI is not configured properly." in caplog.messages[len(caplog.messages) - 1]


def test_save_aws_iam_profiles_no_cli(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="test_user", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("shutil.which", return_value=None)
    save_aws_iam_profiles(w, aws_profile="profile")
    assert "Couldn't find AWS" in caplog.messages[0]
