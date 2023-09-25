import io
import os.path
from pathlib import Path

import pytest
import yaml
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import iam
from databricks.sdk.service.sql import (
    Dashboard,
    DataSource,
    EndpointInfo,
    EndpointInfoWarehouseType,
    Query,
    State,
    Visualization,
    Widget,
)
from databricks.sdk.service.workspace import ImportFormat, ObjectInfo

from databricks.labs.ucx.config import GroupsConfig, WorkspaceConfig
from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.install import WorkspaceInstaller


def test_build_wheel(mocker, tmp_path):
    ws = mocker.Mock()
    install = WorkspaceInstaller(ws)
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
    ws.warehouses.list = lambda **_: [
        EndpointInfo(id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]

    install = WorkspaceInstaller(ws)
    install._choice = lambda _1, _2: "None (abc, PRO, RUNNING)"
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
version: 1
warehouse_id: abc
workspace_start_path: /
""",
        format=ImportFormat.AUTO,
    )


def test_save_config_with_error(mocker):
    def not_found(_):
        raise DatabricksError(error_code="RAISED_FOR_TESTING")

    mocker.patch("builtins.input", return_value="42")
    ws = mocker.Mock()
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.workspace.get_status = not_found

    install = WorkspaceInstaller(ws)
    with pytest.raises(DatabricksError) as e_info:
        install._configure()
    assert str(e_info.value.error_code) == "RAISED_FOR_TESTING"


def test_save_config_auto_groups(mocker):
    def not_found(_):
        raise DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")

    def mock_question(text: str, *, default: str | None = None) -> str:
        if "workspace group names" in text:
            return "<ALL>"
        else:
            return "42"

    mocker.patch("builtins.input", return_value="42")
    ws = mocker.Mock()
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.workspace.get_status = not_found
    ws.warehouses.list = lambda **_: [
        EndpointInfo(id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]

    install = WorkspaceInstaller(ws)
    install._question = mock_question
    install._choice = lambda _1, _2: "None (abc, PRO, RUNNING)"
    install._configure()

    ws.workspace.upload.assert_called_with(
        "/Users/me@example.com/.ucx/config.yml",
        b"""groups:
  auto: true
  backup_group_prefix: '42'
inventory_database: '42'
log_level: '42'
num_threads: 42
version: 1
warehouse_id: abc
workspace_start_path: /
""",
        format=ImportFormat.AUTO,
    )


def test_save_config_strip_group_names(mocker):
    def not_found(_):
        raise DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")

    def mock_question(text: str, *, default: str | None = None) -> str:
        if "workspace group names" in text:
            return "g1, g2, g99"
        else:
            return "42"

    mocker.patch("builtins.input", return_value="42")
    ws = mocker.Mock()
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.workspace.get_status = not_found
    ws.warehouses.list = lambda **_: [
        EndpointInfo(id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]

    install = WorkspaceInstaller(ws)
    install._question = mock_question
    install._choice = lambda _1, _2: "None (abc, PRO, RUNNING)"
    install._configure()

    ws.workspace.upload.assert_called_with(
        "/Users/me@example.com/.ucx/config.yml",
        b"""groups:
  backup_group_prefix: '42'
  selected:
  - g1
  - g2
  - g99
inventory_database: '42'
log_level: '42'
num_threads: 42
version: 1
warehouse_id: abc
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
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="a", groups=GroupsConfig(auto=True)).as_dict()).encode(
        "utf8"
    )
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.workspace.get_status = lambda _: ObjectInfo(object_id=123)
    ws.data_sources.list = lambda: [DataSource(id="bcd", warehouse_id="abc")]
    ws.warehouses.list = lambda **_: [EndpointInfo(id="abc", warehouse_type=EndpointInfoWarehouseType.PRO)]
    ws.dashboards.create.return_value = Dashboard(id="abc")
    ws.queries.create.return_value = Query(id="abc")
    ws.query_visualizations.create.return_value = Visualization(id="abc")
    ws.dashboard_widgets.create.return_value = Widget(id="abc")

    install = WorkspaceInstaller(ws)
    install._build_wheel = lambda _: Path(__file__)
    install.run()

    webbrowser_open.assert_called_with("https://foo/#workspace/Users/me@example.com/.ucx/README.py")
    # ws.workspace.mkdirs.assert_called_with("/Users/me@example.com/.ucx")


def test_query_metadata(mocker):
    ws = mocker.Mock()
    install = WorkspaceInstaller(ws)
    local_query_files = install._find_project_root() / "src/databricks/labs/ucx/assessment/queries"
    DashboardFromFiles(ws, local_query_files, "any", "any").validate()


def test_choices_out_of_range(mocker):
    ws = mocker.Mock()
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="42")
    with pytest.raises(ValueError):
        install._choice("foo", ["a", "b"])


def test_choices_not_a_number(mocker):
    ws = mocker.Mock()
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="two")
    with pytest.raises(ValueError):
        install._choice("foo", ["a", "b"])


def test_choices_happy(mocker):
    ws = mocker.Mock()
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="1")
    res = install._choice("foo", ["a", "b"])
    assert "b" == res
