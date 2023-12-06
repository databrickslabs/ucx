import io

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.iam import User

from databricks.labs.ucx.framework.parallel import ManyError
from databricks.labs.ucx.installer import InstallationManager


def test_happy_path(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")

    ws.users.list.return_value = [User(user_name="foo")]
    ws.workspace.download.return_value = io.StringIO("version: 2\ninventory_database: bar")

    installation_manager = InstallationManager(ws)
    user_installations = installation_manager.user_installations()
    assert len(user_installations) == 1

    assert user_installations[0].user.user_name == "foo"
    assert user_installations[0].config.inventory_database == "bar"


def test_config_not_found(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")

    ws.users.list.return_value = [User(user_name="foo")]
    ws.workspace.download.side_effect = NotFound(...)

    installation_manager = InstallationManager(ws)
    user_installations = installation_manager.user_installations()
    assert len(user_installations) == 0


def test_other_strange_errors(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")

    ws.users.list.return_value = [User(user_name="foo")]
    ws.workspace.download.side_effect = ValueError("nope")

    installation_manager = InstallationManager(ws)
    with pytest.raises(ManyError):
        installation_manager.user_installations()


def test_corrupt_config(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")

    ws.users.list.return_value = [User(user_name="foo")]
    ws.workspace.download.return_value = io.StringIO("version: 2\ntacl: true")

    installation_manager = InstallationManager(ws)
    user_installations = installation_manager.user_installations()
    assert len(user_installations) == 0
