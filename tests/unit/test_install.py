import os.path

from databricks.sdk.core import DatabricksError
from databricks.sdk.service import iam
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx import install


def test_build_wheel(tmp_path):
    whl = install.build_wheel(str(tmp_path))
    assert os.path.exists(whl)


def test_save_config(mocker):
    def not_found(_):
        raise DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")

    mocker.patch("builtins.input", return_value="42")
    ws = mocker.Mock()
    ws.config.host = "https://foo"
    ws.workspace.get_status = not_found

    install.save_config(ws, "abc")

    ws.workspace.upload.assert_called_with(
        "abc/config.yml",
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

    ws.current_user.me = lambda: iam.User(user_name="me@example.com")
    ws.config.host = "https://foo"
    ws.workspace.get_status = lambda _: None

    install.main(ws)

    webbrowser_open.assert_called_with("https://foo/#workspace/Users/me@example.com/.ucx/config.yml")
    ws.workspace.mkdirs.assert_called_with("/Users/me@example.com/.ucx")
