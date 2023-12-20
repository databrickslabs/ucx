import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam
from databricks.sdk.service.iam import User

from databricks.labs.ucx.cli import skip


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
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller.__init__", return_value=None)
    mocker.patch("databricks.labs.ucx.install.WorkspaceInstaller._current_config", return_value="foo")
    mocker.patch(
        "databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__",
        return_value=None,
        side_effect=NotFound("..."),
    )
    skip(schema="schema", table="table")
    assert [rec.message for rec in caplog.records if "UCX configuration" in rec.message]
