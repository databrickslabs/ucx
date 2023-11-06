import io
import os.path
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml
from databricks.sdk.core import DatabricksError
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service import iam, jobs
from databricks.sdk.service.compute import (
    GlobalInitScriptDetails,
    GlobalInitScriptDetailsWithContent,
    Policy,
)
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


def test_install_override_clusters(mocker, tmp_path):
    ws = mock_ws(mocker)
    install = WorkspaceInstaller(ws)
    cluster_id = "9999-999999-abcdefgh"
    mocker.patch("builtins.input", return_value=cluster_id)
    res = install._configure_override_clusters()
    assert res["main"] == cluster_id
    assert res["tacl"] == cluster_id

    install._configure()
    install.run()


def test_install_dbfs_write_protect(mocker, tmp_path):
    "Verify flag isn't in jobs create api call"
    ws = mock_ws(mocker)
    install = WorkspaceInstaller(ws)
    cluster_id = "9999-999999-abcdefgh"
    mocker.patch("builtins.input", return_value=cluster_id)
    res = install._configure_override_clusters()
    assert res["main"] == cluster_id
    assert res["tacl"] == cluster_id

    jobs_mock = MagicMock()

    def jobs_create(*args, **kwargs):
        assert "write_protected_dbfs" not in args, f"{args}"
        assert "write_protected_dbfs" not in kwargs, f"{kwargs}"
        return MagicMock(job_id="bar")

    jobs_mock.create = jobs_create
    ws.jobs = jobs_mock

    install._configure()
    install.run()


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
    ws.cluster_policies.list = lambda: []

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
    ws.cluster_policies.list = lambda: []

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
    ws.cluster_policies.list = lambda: []

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
    ws.cluster_policies.list = lambda: []

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


def test_save_config_with_glue(mocker):
    policy_def = b"""
{
  "aws_attributes.instance_profile_arn": {
    "type": "fixed",
    "value": "arn:aws:iam::111222333:instance-profile/foo-instance-profile",
    "hidden": false
  },
  "spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {
    "type": "fixed",
    "value": "true",
    "hidden": true
  }
}
            """

    def not_found(_):
        raise DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")

    def mock_question(text: str, *, default: str | None = None) -> str:
        if "external metastore" in text:
            return "yes"
        else:
            return "42"

    def mock_choice_from_dict(text: str, choices: dict[str, Any]) -> Any:
        if "Select a Cluster" in text:
            return policy_def
        if "warehouse" in text:
            return "abc"

    mocker.patch("builtins.input", return_value="42")
    ws = mocker.Mock()
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.workspace.get_status = not_found
    ws.warehouses.list = lambda **_: [
        EndpointInfo(id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]
    ws.cluster_policies.list = lambda: [Policy(definition=policy_def.decode("utf-8"))]

    install = WorkspaceInstaller(ws)
    install._question = mock_question
    install._choice_from_dict = mock_choice_from_dict
    install._configure()

    ws.workspace.upload.assert_called_with(
        "/Users/me@example.com/.ucx/config.yml",
        b"""default_catalog: ucx_default
groups:
  backup_group_prefix: '42'
  selected:
  - '42'
instance_profile: arn:aws:iam::111222333:instance-profile/foo-instance-profile
inventory_database: '42'
log_level: '42'
num_threads: 42
spark_conf:
  spark.databricks.hive.metastore.glueCatalog.enabled: 'true'
version: 1
warehouse_id: abc
workspace_start_path: /
""",
        format=ImportFormat.AUTO,
    )


def test_main_with_existing_conf_does_not_recreate_config(mocker):
    mocker.patch("builtins.input", return_value="yes")
    mock_file = MagicMock()
    mocker.patch("builtins.open", return_value=mock_file)
    mocker.patch("base64.b64encode")
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
    local_query_files = install._find_project_root() / "src/databricks/labs/ucx/queries"
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


def test_step_run(mocker):
    ws = mock_ws(mocker)
    from databricks.labs.ucx.framework.tasks import Task

    tasks = [
        Task(task_id=0, workflow="wl_1", name="n3", doc="d3", fn=lambda: None),
        Task(task_id=1, workflow="wl_2", name="n2", doc="d2", fn=lambda: None),
        Task(task_id=2, workflow="wl_1", name="n1", doc="d1", fn=lambda: None),
    ]
    mocker.patch("builtins.input", return_value="42")
    with mocker.patch.object(WorkspaceInstaller, attribute="_sorted_tasks", return_value=tasks):
        install = WorkspaceInstaller(ws)
        steps = install._step_list()
        assert len(steps) == 2
        assert steps[0] == "wl_1" and steps[1] == "wl_2"

        install._override_clusters = {"main": "clusterid1", "tacl": "clusterid_tacl"}
        install.run()


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
    from databricks.labs.ucx.framework.tasks import _remove_extra_indentation

    doc = _remove_extra_indentation(
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


def test_global_init_script_already_exists_enabled(mocker):
    ws = mocker.Mock()
    ginit_scripts = [
        GlobalInitScriptDetails(
            created_at=1695045723722,
            created_by="test@abc.com",
            enabled=True,
            name="test123",
            position=0,
            script_id="12345",
            updated_at=1695046359612,
            updated_by="test@abc.com",
        )
    ]
    ws.global_init_scripts.list.return_value = ginit_scripts
    ws.global_init_scripts.get.return_value = GlobalInitScriptDetailsWithContent(
        created_at=1695045723722,
        created_by="test@abc.com",
        enabled=True,
        name="test123",
        position=0,
        script="aWYgW1sgJERCX0lTX0RSSVZFUiA9ICJUUlVFIiB"
        "dXTsgdGhlbgogIGRyaXZlcl9jb25mPSR7REJfSE9NRX0"
        "vZHJpdmVyL2NvbmYvc3BhcmstYnJhbmNoLmNvbmYKICBpZ"
        "iBbICEgLWUgJGRyaXZlcl9jb25mIF0gOyB0aGVuCiAgICB0b"
        "3VjaCAkZHJpdmVyX2NvbmYKICBmaQpjYXQgPDwgRU9GID4+ICAkZ"
        "HJpdmVyX2NvbmYKICBbZHJpdmVyXSB7CiAgICJzcGFyay5kYXRhYnJpY2tzLm"
        "RhdGFMaW5lYWdlLmVuYWJsZWQiID0gdHJ1ZQogICB9CkVPRgpmaQ==",
        script_id="12C100F8BB38B002",
        updated_at=1695046359612,
        updated_by="test@abc.com",
    )
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="yes")
    install._install_spark_config_for_hms_lineage()


def test_global_init_script_already_exists_disabled(mocker):
    ws = mocker.Mock()
    ginit_scripts = [
        GlobalInitScriptDetails(
            created_at=1695045723722,
            created_by="test@abc.com",
            enabled=False,
            name="test123",
            position=0,
            script_id="12345",
            updated_at=1695046359612,
            updated_by="test@abc.com",
        )
    ]
    ws.global_init_scripts.list.return_value = ginit_scripts
    ws.global_init_scripts.get.return_value = GlobalInitScriptDetailsWithContent(
        created_at=1695045723722,
        created_by="test@abc.com",
        enabled=False,
        name="test123",
        position=0,
        script="aWYgW1sgJERCX0lTX0RSSVZFUiA9ICJUUlVFIiB"
        "dXTsgdGhlbgogIGRyaXZlcl9jb25mPSR7REJfSE9NRX0"
        "vZHJpdmVyL2NvbmYvc3BhcmstYnJhbmNoLmNvbmYKICBpZ"
        "iBbICEgLWUgJGRyaXZlcl9jb25mIF0gOyB0aGVuCiAgICB0b"
        "3VjaCAkZHJpdmVyX2NvbmYKICBmaQpjYXQgPDwgRU9GID4+ICAkZ"
        "HJpdmVyX2NvbmYKICBbZHJpdmVyXSB7CiAgICJzcGFyay5kYXRhYnJpY2tzLm"
        "RhdGFMaW5lYWdlLmVuYWJsZWQiID0gdHJ1ZQogICB9CkVPRgpmaQ==",
        script_id="12C100F8BB38B002",
        updated_at=1695046359612,
        updated_by="test@abc.com",
    )
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="yes")
    install._install_spark_config_for_hms_lineage()


def test_global_init_script_exists_disabled_not_enabled(mocker):
    ws = mocker.Mock()
    ginit_scripts = [
        GlobalInitScriptDetails(
            created_at=1695045723722,
            created_by="test@abc.com",
            enabled=False,
            name="test123",
            position=0,
            script_id="12345",
            updated_at=1695046359612,
            updated_by="test@abc.com",
        )
    ]
    ws.global_init_scripts.list.return_value = ginit_scripts
    ws.global_init_scripts.get.return_value = GlobalInitScriptDetailsWithContent(
        created_at=1695045723722,
        created_by="test@abc.com",
        enabled=False,
        name="test123",
        position=0,
        script="aWYgW1sgJERCX0lTX0RSSVZFUiA9ICJUUlVFIiB"
        "dXTsgdGhlbgogIGRyaXZlcl9jb25mPSR7REJfSE9NRX0"
        "vZHJpdmVyL2NvbmYvc3BhcmstYnJhbmNoLmNvbmYKICBpZ"
        "iBbICEgLWUgJGRyaXZlcl9jb25mIF0gOyB0aGVuCiAgICB0b"
        "3VjaCAkZHJpdmVyX2NvbmYKICBmaQpjYXQgPDwgRU9GID4+ICAkZ"
        "HJpdmVyX2NvbmYKICBbZHJpdmVyXSB7CiAgICJzcGFyay5kYXRhYnJpY2tzLm"
        "RhdGFMaW5lYWdlLmVuYWJsZWQiID0gdHJ1ZQogICB9CkVPRgpmaQ==",
        script_id="12C100F8BB38B002",
        updated_at=1695046359612,
        updated_by="test@abc.com",
    )
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="no")
    install._install_spark_config_for_hms_lineage()


@patch("builtins.open", new_callable=MagicMock)
@patch("base64.b64encode")
@patch("builtins.input", new_callable=MagicMock)
def test_global_init_script_create_new(mock_open, mocker, mock_input):
    expected_content = """if [[ $DB_IS_DRIVER = "TRUE" ]]; then
      driver_conf=${DB_HOME}/driver/conf/spark-branch.conf
      if [ ! -e $driver_conf ] ; then
        touch $driver_conf
      fi
    cat << EOF >>  $driver_conf
      [driver] {
       "spark.databricks.dataLineage.enabled" = true
       }
    EOF
    fi
        """
    mock_file = MagicMock()
    mock_file.read.return_value = expected_content
    mock_open.return_value = mock_file
    ws = mocker.Mock()
    install = WorkspaceInstaller(ws)
    mock_input.return_value = "yes"
    install._install_spark_config_for_hms_lineage()
