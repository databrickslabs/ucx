import io
import os.path
from pathlib import Path

import yaml
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import iam
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.config import GroupsConfig, MigrationConfig, TaclConfig
from databricks.labs.ucx.install import Installer


def test_build_wheel(mocker, tmp_path):
    ws = mocker.Mock()
    install = Installer(ws)
    whl = install._build_wheel(str(tmp_path))
    assert os.path.exists(whl)


def test_save_config(mocker):
    def not_found(_):
        raise DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")

    mocker.patch("builtins.input", return_value="42")
    ws = mocker.Mock()
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.workspace.get_status = not_found

    install = Installer(ws)
    install._configure()

    ws.workspace.upload.assert_called_with(
        "/Users/me@example.com/.ucx/config.yml",
        b"""groups:
  backup_group_prefix: '42'
  selected:
  - '42'
inventory_database: '42'
log_level: '42'
num_threads: 42
tacl:
  auto: true
version: 1
workspace_start_path: /
""",
        format=ImportFormat.AUTO,
    )


def test_main_with_existing_conf_does_not_recreate_config(mocker):
    mocker.patch("builtins.input", return_value="yes")
    webbrowser_open = mocker.patch("webbrowser.open")
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")

    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.config.is_aws = True
    config_bytes = yaml.dump(
        MigrationConfig(inventory_database="a", groups=GroupsConfig(auto=True), tacl=TaclConfig(auto=True)).as_dict()
    ).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.workspace.get_status = lambda _: None

    install = Installer(ws)
    install._build_wheel = lambda _: Path(__file__)
    install.run()

    webbrowser_open.assert_called_with("https://foo/#workspace/Users/me@example.com/.ucx/README.py")
    # ws.workspace.mkdirs.assert_called_with("/Users/me@example.com/.ucx")
