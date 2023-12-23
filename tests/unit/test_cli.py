import io
import sys
from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam
from databricks.sdk.service.catalog import MetastoreAssignment
from databricks.sdk.service.iam import ComplexValue, User

<<<<<<< HEAD
from databricks.labs.ucx.cli import repair_run, skip
=======
from databricks.labs.ucx.cli import skip, verify_metastore
>>>>>>> 2a2a1e7 (Added unit tests)


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

