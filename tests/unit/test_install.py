import io
import os.path
from pathlib import Path

import pytest
import yaml
from databricks.sdk.core import DatabricksError
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service import iam, jobs
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


def mock_ws(mocker):
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
    return ws


def test_replace_clusters_for_integration_tests(mocker):
    ws = mock_ws(mocker)
    return_value = WorkspaceInstaller.run_for_config(
        ws, WorkspaceConfig(inventory_database="a", groups=GroupsConfig(auto=True)), override_clusters={"main": "abc"}
    )
    assert return_value


def test_run_workflow_creates_proper_failure(mocker):
    def run_now(job_id):
        assert "bar" == job_id

        def result():
            raise OperationFailed(...)

        waiter = mocker.Mock()
        waiter.result = result
        waiter.run_id = "qux"
        return waiter

    ws = mock_ws(mocker)
    ws.jobs.run_now = run_now
    ws.jobs.get_run.return_value = jobs.Run(
        state=jobs.RunState(state_message="Stuff happens."),
        tasks=[
            jobs.RunTask(
                task_key="stuff",
                state=jobs.RunState(result_state=jobs.RunResultState.FAILED),
                run_id=123,
            )
        ],
    )
    ws.jobs.get_run_output.return_value = jobs.RunOutput(error="does not compute", error_trace="# goes to stderr")
    installer = WorkspaceInstaller(ws)
    installer._deployed_steps = {"foo": "bar"}

    with pytest.raises(OperationFailed) as failure:
        installer.run_workflow("foo")

    assert "Stuff happens: stuff: does not compute" == str(failure.value)


def test_install_database_happy(mocker, tmp_path):
    ws = mocker.Mock()
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="ucx")
    res = install._configure_inventory_database()
    assert "ucx" == res


def test_install_database_unhappy(mocker, tmp_path):
    ws = mocker.Mock()
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="main.ucx")

    with pytest.raises(SystemExit):
        install._configure_inventory_database()


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
        b"""default_catalog: ucx_default
groups:
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
        b"""default_catalog: ucx_default
groups:
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
        b"""default_catalog: ucx_default
groups:
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


def test_step_list(mocker):
    ws = mocker.Mock()
    from databricks.labs.ucx.framework.tasks import Task

    tasks = [
        Task(task_id=0, workflow="wl_1", name="n3", doc="d3", fn=lambda: None),
        Task(task_id=1, workflow="wl_2", name="n2", doc="d2", fn=lambda: None),
        Task(task_id=2, workflow="wl_1", name="n1", doc="d1", fn=lambda: None),
    ]

    with mocker.patch.object(WorkspaceInstaller, attribute="_sorted_tasks", return_value=tasks):
        install = WorkspaceInstaller(ws)
        steps = install._step_list()
    assert len(steps) == 2
    assert steps[0] == "wl_1" and steps[1] == "wl_2"


def test_create_readme(mocker):
    mocker.patch("builtins.input", return_value="yes")
    webbrowser_open = mocker.patch("webbrowser.open")
    ws = mocker.Mock()

    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="a", groups=GroupsConfig(auto=True)).as_dict()).encode(
        "utf8"
    )
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)

    from databricks.labs.ucx.framework.tasks import Task

    tasks = [
        Task(task_id=0, workflow="wl_1", name="n3", doc="d3", fn=lambda: None),
        Task(task_id=1, workflow="wl_2", name="n2", doc="d2", fn=lambda: None),
        Task(task_id=2, workflow="wl_1", name="n1", doc="d1", fn=lambda: None),
    ]

    with mocker.patch.object(WorkspaceInstaller, attribute="_sorted_tasks", return_value=tasks):
        install = WorkspaceInstaller(ws)
        install._deployed_steps = {"wl_1": 1, "wl_2": 2}
        install._create_readme()

    webbrowser_open.assert_called_with("https://foo/#workspace/Users/me@example.com/.ucx/README.py")

    _, args, kwargs = ws.mock_calls[0]
    assert args[0] == "/Users/me@example.com/.ucx/README.py"

    import re

    p = re.compile(".*wl_1.*n3.*n1.*wl_2.*n2.*")
    assert p.match(str(args[1]))


def test_replace_pydoc(mocker):
    ws = mocker.Mock()
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="a", groups=GroupsConfig(auto=True)).as_dict()).encode(
        "utf8"
    )
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)

    install = WorkspaceInstaller(ws)
    doc = install._current_config.remove_extra_indentation(
        """Test1
        Test2
    Test3"""
    )
    assert (
        doc
        == """Test1
    Test2
Test3"""
    )
