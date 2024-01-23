import subprocess
from unittest.mock import MagicMock, create_autospec, patch

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam
from databricks.sdk.service.iam import User

from databricks.labs.ucx.cli import (
    CANT_FIND_UCX_MSG,
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
from databricks.labs.ucx.installer import Installation


def test_workflow(caplog):
    w = create_autospec(WorkspaceClient)
    with patch(
            "databricks.labs.ucx.install.WorkspaceInstaller.latest_job_status", return_value=[{"key": "dummy"}]
    ) as latest_job_status:
        workflows(w)
        assert caplog.messages == ["Fetching deployed jobs..."]
        latest_job_status.assert_called_once()


def test_open_remote_config(mocker):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    ws_file_url = "https://example.com/#workspace/Users/foo/.ucx/config.yml"
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.notebook_link", return_value=ws_file_url)
    with patch("webbrowser.open") as mock_webbrowser_open:
        open_remote_config(w)
        mock_webbrowser_open.assert_called_with(ws_file_url)


def test_installations(mocker, capsys):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    summary = {
        "user_name": "foo",
        "database": "ucx",
        "warehouse_id": "test",
    }
    installation = MagicMock()
    installation.as_summary.return_value = summary
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.user_installations", return_value=[installation])
    installations(w)
    assert '{"user_name": "foo", "database": "ucx", "warehouse_id": "test"}' in capsys.readouterr().out


def test_skip_with_table(mocker):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=MagicMock())
    with patch("databricks.labs.ucx.hive_metastore.mapping.TableMapping.skip_table", return_value=None) as s:
        skip(w, "schema", "table")
        s.assert_called_once()


def test_skip_with_schema(mocker):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=MagicMock())
    with patch("databricks.labs.ucx.hive_metastore.mapping.TableMapping.skip_schema", return_value=None) as s:
        skip(w, "schema", None)
        s.assert_called_once()


def test_skip_no_schema(caplog):
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.users.list.return_value = [User(user_name="foo")]
    ws.workspace.download.side_effect = NotFound(...)
    skip(ws, schema=None, table="table")
    assert [rec.message for rec in caplog.records if "schema" in rec.message.lower()]


def test_skip_no_ucx(caplog, mocker):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.__init__", return_value=None)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=None)
    skip(w, schema="schema", table="table")
    assert [rec.message for rec in caplog.records if "UCX configuration" in rec.message]


def test_sync_workspace_info():
    with patch("databricks.labs.ucx.account.AccountWorkspaces.sync_workspace_info", return_value=None) as s, patch(
            "databricks.labs.ucx.account.AccountWorkspaces.__init__", return_value=None
    ):
        sync_workspace_info(create_autospec(AccountClient))
        s.assert_called_once()


def test_manual_workspace_info():
    w = create_autospec(WorkspaceClient)
    with patch("databricks.labs.ucx.account.WorkspaceInfo.manual_workspace_info", return_value=None) as m:
        manual_workspace_info(w)
        m.assert_called_once()


def test_create_table_mapping(mocker):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=MagicMock())
    with patch("databricks.labs.ucx.hive_metastore.mapping.TableMapping.save", return_value=None) as s:
        create_table_mapping(w)
        s.assert_called_once()


def test_validate_external_locations(mocker):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=MagicMock())
    # test save_as_terraform_definitions_on_workspace is called
    # also test if the saving tf scripts returns None
    with patch(
            "databricks.labs.ucx.hive_metastore.locations.ExternalLocations.save_as_terraform_definitions_on_workspace",
            return_value=None,
    ) as s, patch("webbrowser.open") as w:
        validate_external_locations(w)
        s.assert_called_once()
        w.assert_not_called()
    # test when tf scripts is written and user confirmed to open it over browser
    path = "dummy/external_locations.tf"
    with patch(
            "databricks.labs.ucx.hive_metastore.locations.ExternalLocations.save_as_terraform_definitions_on_workspace",
            return_value=path,
    ) as s, patch("webbrowser.open") as w, patch("databricks.labs.blueprint.tui.Prompts.confirm", return_value=True):
        validate_external_locations(w)
        s.assert_called_once()
        w.assert_called_with(f"{w.config.host}/#workspace{path}")
    # test when tf scripts is written but user did not confirm to open it over browser
    with patch(
            "databricks.labs.ucx.hive_metastore.locations.ExternalLocations.save_as_terraform_definitions_on_workspace",
            return_value=path,
    ) as s, patch("webbrowser.open") as w, patch("databricks.labs.blueprint.tui.Prompts.confirm", return_value=False):
        validate_external_locations(w)
        s.assert_called_once()
        w.assert_not_called()


def test_ensure_assessment_run(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    with patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=None):
        assert ensure_assessment_run(w) is None
        assert caplog.messages == [CANT_FIND_UCX_MSG]

    with patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=MagicMock()), patch(
            "databricks.labs.ucx.install.WorkspaceInstaller.validate_and_run", return_value=MagicMock()
    ) as v:
        ensure_assessment_run(w)
        v.assert_called_with("assessment")


def test_repair_run(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.__init__", return_value=None)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.repair_run", return_value=None)
    repair_run(w, "assessment")
    assert caplog.messages == ["Repair Running assessment Job"]


def test_no_step_in_repair_run(mocker):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.__init__", return_value=None)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.repair_run", return_value=None)
    try:
        repair_run(w, "")
    except KeyError as e:
        assert e.args[0] == "You did not specify --step"


def test_revert_migrated_tables(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    # test with no schema and no table, user confirm to not retry
    with patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=MagicMock()), patch(
            "databricks.labs.blueprint.tui.Prompts.confirm", return_value=False
    ):
        assert revert_migrated_tables(w, schema=None, table=None) is None
    # test with no schema and no table, user confirm to retry, but no ucx installation found
    with patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=None), patch(
            "databricks.labs.blueprint.tui.Prompts.confirm", return_value=True
    ):
        assert revert_migrated_tables(w, schema=None, table=None) is None
        assert caplog.messages[0] == CANT_FIND_UCX_MSG
    # test with schema or table, but no ucx installation found
    with patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=None):
        assert revert_migrated_tables(w, schema="test", table=None) is None
        assert caplog.messages[1] == CANT_FIND_UCX_MSG
        assert revert_migrated_tables(w, schema=None, table="test") is None
        assert caplog.messages[2] == CANT_FIND_UCX_MSG
        assert revert_migrated_tables(w, schema="test", table="test") is None
        assert caplog.messages[3] == CANT_FIND_UCX_MSG
    # test revert_migrated_tables is executed when revert report print successfully and user confirm
    with patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=MagicMock()), patch(
            "databricks.labs.ucx.hive_metastore.table_migrate.TablesMigrate.print_revert_report", return_value=True
    ), patch("databricks.labs.blueprint.tui.Prompts.confirm", return_value=True), patch(
        "databricks.labs.ucx.hive_metastore.table_migrate.TablesMigrate.revert_migrated_tables", return_value=None
    ) as r:
        revert_migrated_tables(w, schema="test", table="test")
        r.assert_called_once()
    # test revert_migrated_tables is not executed when revert report print failed
    with patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=MagicMock()), patch(
            "databricks.labs.ucx.hive_metastore.table_migrate.TablesMigrate.print_revert_report", return_value=False
    ), patch("databricks.labs.blueprint.tui.Prompts.confirm", return_value=True), patch(
        "databricks.labs.ucx.hive_metastore.table_migrate.TablesMigrate.revert_migrated_tables", return_value=None
    ) as r:
        revert_migrated_tables(w, schema="test", table="test")
        r.assert_not_called()
    # test revert_migrated_tables is not executed when revert report print successfully but user does not confirm
    with patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=MagicMock()), patch(
            "databricks.labs.ucx.hive_metastore.table_migrate.TablesMigrate.print_revert_report", return_value=True
    ), patch("databricks.labs.blueprint.tui.Prompts.confirm", return_value=False), patch(
        "databricks.labs.ucx.hive_metastore.table_migrate.TablesMigrate.revert_migrated_tables", return_value=None
    ) as r:
        revert_migrated_tables(w, schema="test", table="test")
        r.assert_not_called()


def test_move_no_ucx(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=None)
    move(w, "", "", "", "", "")
    assert [rec.message for rec in caplog.records if "UCX configuration" in rec.message]


def test_move_no_catalog(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=w.current_user)
    move(w, "", "", "", "", "")
    assert (
            len([rec.message for rec in caplog.records if
                 "Please enter from_catalog and to_catalog" in rec.message]) == 1
    )


def test_move_same_schema(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=w.current_user)
    move(w, "SrcCat", "SrcS", "*", "SrcCat", "SrcS")
    assert len([rec.message for rec in caplog.records if "please select a different schema" in rec.message]) == 1


def test_move_no_schema(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=w.current_user)
    move(w, "SrcCat", "", "*", "TgtCat", "")
    assert len([rec.message for rec in caplog.records if "Please enter from_schema, to_schema" in rec.message]) == 1


def test_move(mocker, monkeypatch):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=w.current_user)
    monkeypatch.setattr("builtins.input", lambda _: "yes")

    with patch("databricks.labs.ucx.hive_metastore.table_migrate.TableMove.move_tables", return_value=None) as m:
        move(w, "SrcC", "SrcS", "*", "TgtC", "ToS")
        m.assert_called_once()


def test_save_azure_storage_accounts_no_ucx(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=None)
    save_azure_storage_accounts(w, "")
    assert [rec.message for rec in caplog.records if "UCX configuration" in rec.message]


def test_save_azure_storage_accounts_not_azure(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.config.is_azure = False
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=w.current_user)
    save_azure_storage_accounts(w, "")
    assert [rec.message for rec in caplog.records if "Workspace is not on azure" in rec.message]


def test_save_azure_storage_accounts_no_azure_cli(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.config.auth_type = "azure_clis"
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=w.current_user)
    save_azure_storage_accounts(w, "")
    assert [rec.message for rec in caplog.records if "In order to obtain AAD token" in rec.message]


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
    inst_data = Installation(
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
        user="test_user",
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
    mocker.patch("subprocess.Popen", return_value=pop)
    pop.communicate.return_value = (bytes("message", "utf-8"), bytes("error", "utf-8"))
    pop.returncode = 127
    save_aws_iam_profiles(w, aws_profile="profile")
    assert "AWS CLI is not configured properly." in caplog.messages[len(caplog.messages) - 1]


def test_save_aws_iam_profiles_no_cli(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="test_user", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("shutil.which", return_value=None)
    save_aws_iam_profiles(w, aws_profile="profile")
    assert "Couldn't find AWS" in caplog.messages[0]
