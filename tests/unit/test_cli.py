from unittest.mock import create_autospec, patch

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam
from databricks.sdk.service.iam import User

from databricks.labs.ucx.cli import move, repair_run, save_azure_storage_accounts, skip


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


def test_repair_run(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.__init__", return_value=None)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.repair_run", return_value=None)
    repair_run(w, "assessment")
    assert caplog.messages == ["Repair Running assessment Job"]


def test_no_step_in_repair_run(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.__init__", return_value=None)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.repair_run", return_value=None)
    try:
        repair_run(w, "")
    except KeyError as e:
        assert e.args[0] == "You did not specify --step"


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
        len([rec.message for rec in caplog.records if "Please enter from_catalog and to_catalog" in rec.message]) == 1
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


def test_move(mocker, caplog, monkeypatch):
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
