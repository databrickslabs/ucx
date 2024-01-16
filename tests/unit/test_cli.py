from unittest.mock import MagicMock, create_autospec, patch

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam
from databricks.sdk.service.iam import User

from databricks.labs.ucx.cli import move, repair_run, skip, workflows, open_remote_config, installations, sync_workspace_info, manual_workspace_info, create_table_mapping


def test_workflow(caplog):
    w = create_autospec(WorkspaceClient)
    with patch("databricks.labs.ucx.install.WorkspaceInstaller.latest_job_status", return_value=[{"key":"dummy"}]) as l:
        workflows(w)
        caplog.messages == ["Fetching deployed jobs..."]
        l.assert_called_once()

def test_open_remote_config(mocker):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    ws_file_url = f"https://example.com/#workspace/Users/foo/.ucx/config.yml"
    mocker.patch('databricks.labs.ucx.install.WorkspaceInstaller.notebook_link', return_value=ws_file_url)
    with patch('webbrowser.open') as mock_webbrowser_open:
        open_remote_config(w)
        mock_webbrowser_open.assert_called_with(ws_file_url)

def test_installations(mocker, caplog):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    summary = {
        "user_name": "foo",
        "database": "ucx",
        "warehouse_id": "test",
    }
    installation = MagicMock()
    installation.as_summary.return_value = summary
    mocker.patch('databricks.labs.ucx.installer.InstallationManager.user_installations', return_value=[installation])
    installations(w)
    caplog.messages == ['[{"user_name": "foo", "database": "ucx", "warehouse_id": "test"}]']

def test_skip_with_table(mocker):
    """
    Test the skip function with schema and table specified.
    :param mocker:
    :return:
    """
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=MagicMock())
    with patch("databricks.labs.ucx.hive_metastore.mapping.TableMapping.skip_table", return_value=None) as s:
        skip(w, "schema", "table")
        s.assert_called_once()


def test_skip_with_schema(mocker):
    """
    Test the skip function with schema specified.
    :param mocker:
    :return:
    """
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
    with patch("databricks.labs.ucx.account.AccountWorkspaces.sync_workspace_info", return_value=None) as s:
        sync_workspace_info(MagicMock())
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


def test_move(mocker, monkeypatch):
    w = create_autospec(WorkspaceClient)
    w.current_user.me = lambda: iam.User(user_name="foo", groups=[iam.ComplexValue(display="admins")])
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=w.current_user)
    monkeypatch.setattr("builtins.input", lambda _: "yes")

    with patch("databricks.labs.ucx.hive_metastore.table_migrate.TableMove.move_tables", return_value=None) as m:
        move(w, "SrcC", "SrcS", "*", "TgtC", "ToS")
        m.assert_called_once()
