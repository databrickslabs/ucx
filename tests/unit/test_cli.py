from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam
from databricks.sdk.service.iam import ComplexValue, User

from databricks.labs.ucx.cli import migrate_uc_to_uc, repair_run, skip


@pytest.fixture
def ws(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.return_value = None


def test_skip_no_schema(mocker, caplog):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    ws.users.list.return_value = [User(user_name="foo")]
    ws.workspace.download.side_effect = NotFound(...)
    skip(schema=None, table="table")
    assert [rec.message for rec in caplog.records if "schema" in rec.message.lower()]


def test_skip_no_ucx(caplog, mocker):
    mocker.patch("databricks.sdk.WorkspaceClient.__init__", return_value=None)
    current_user = MagicMock()
    current_user.me.return_value = User(user_name="foo", groups=[ComplexValue(display="admins")])
    current_user.return_value = None
    mocker.patch("databricks.sdk.WorkspaceClient.current_user", return_value=current_user)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.__init__", return_value=None)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=None)
    skip(schema="schema", table="table")
    assert [rec.message for rec in caplog.records if "UCX configuration" in rec.message]


def test_repair_run(mocker, caplog):
    mocker.patch("databricks.sdk.WorkspaceClient.__init__", return_value=None)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.__init__", return_value=None)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.repair_run", return_value=None)
    repair_run("assessment")
    assert caplog.messages == []


def test_no_step_in_repair_run(mocker, caplog):
    mocker.patch("databricks.sdk.WorkspaceClient.__init__", return_value=None)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.__init__", return_value=None)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.repair_run", return_value=None)
    try:
        repair_run("")
    except KeyError as e:
        assert e.args[0] == "You did not specify --step"


def test_uc_to_uc_no_catalog(mocker, caplog):
    mocker.patch("databricks.sdk.WorkspaceClient.__init__", return_value=None)
    current_user = MagicMock()
    current_user.me.return_value = User(user_name="foo", groups=[ComplexValue(display="admins")])
    mocker.patch("databricks.sdk.WorkspaceClient.current_user", return_value=current_user)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=current_user)
    mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__", return_value=None)
    migrate_uc_to_uc(from_catalog="", from_schema="", from_table="", to_catalog="", to_schema="")
    assert (
        len([rec.message for rec in caplog.records if "Please enter from_catalog and to_catalog" in rec.message]) == 1
    )


def test_uc_to_uc_no_schema(mocker, caplog):
    mocker.patch("databricks.sdk.WorkspaceClient.__init__", return_value=None)
    current_user = MagicMock()
    current_user.me.return_value = User(user_name="foo", groups=[ComplexValue(display="admins")])
    mocker.patch("databricks.sdk.WorkspaceClient.current_user", return_value=current_user)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=current_user)
    mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__", return_value=None)
    migrate_uc_to_uc(from_catalog="SrcCat", from_schema="", from_table=[], to_catalog="TgtCat", to_schema="")
    assert len([rec.message for rec in caplog.records if "Please enter from_schema, to_schema" in rec.message]) == 1


def test_uc_to_uc(mocker, caplog):
    mocker.patch("databricks.sdk.WorkspaceClient.__init__", return_value=None)
    current_user = MagicMock()
    current_user.me.return_value = User(user_name="foo", groups=[ComplexValue(display="admins")])
    mocker.patch("databricks.sdk.WorkspaceClient.current_user", return_value=current_user)
    mocker.patch("databricks.labs.ucx.installer.InstallationManager.for_user", return_value=current_user)
    mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__", return_value=None)
    with patch(
        "databricks.labs.ucx.hive_metastore.table_migrate.TablesMigrate.migrate_uc_tables", return_value=None
    ) as m:
        migrate_uc_to_uc(from_catalog="SrcC", from_schema="SrcS", from_table=["*"], to_catalog="TgtC", to_schema="ToS")
        m.assert_called_once()
