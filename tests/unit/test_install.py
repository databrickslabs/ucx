import io
import os.path
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml
from databricks.sdk.errors import (
    InvalidParameterValue,
    NotFound,
    OperationFailed,
    PermissionDenied,
)
from databricks.sdk.service import iam, jobs, sql
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

import databricks.labs.ucx.uninstall  # noqa
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.framework.install_state import InstallState
from databricks.labs.ucx.framework.tasks import Task
from databricks.labs.ucx.install import WorkspaceInstaller

from ..unit.framework.mocks import MockBackend

# test cluster id
cluster_id = "9999-999999-abcdefgh"


def mock_clusters():
    from databricks.sdk.service.compute import DataSecurityMode, State

    return [
        MagicMock(
            spark_version="13.3.x-dbrxxx",
            cluster_name="zero",
            data_security_mode=DataSecurityMode.USER_ISOLATION,
            state=State.RUNNING,
            cluster_id=cluster_id,
        ),
        MagicMock(
            spark_version="13.3.x-dbrxxx",
            cluster_name="one",
            data_security_mode=DataSecurityMode.NONE,
            state=State.RUNNING,
            cluster_id=cluster_id,
        ),
        MagicMock(
            spark_version="13.3.x-dbrxxx",
            cluster_name="two",
            data_security_mode=DataSecurityMode.LEGACY_TABLE_ACL,
            state=State.RUNNING,
            cluster_id=cluster_id,
        ),
    ]


@pytest.fixture
def ws(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")

    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.config.is_aws = True
    ws.workspace.get_status = lambda _: ObjectInfo(object_id=123)
    ws.data_sources.list = lambda: [DataSource(id="bcd", warehouse_id="abc")]
    ws.warehouses.list = lambda **_: [
        EndpointInfo(name="abc", id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]
    ws.dashboards.create.return_value = Dashboard(id="abc")
    ws.jobs.create.return_value = jobs.CreateResponse(job_id="abc")
    ws.queries.create.return_value = Query(id="abc")
    ws.query_visualizations.create.return_value = Visualization(id="abc")
    ws.dashboard_widgets.create.return_value = Widget(id="abc")
    ws.clusters.list.return_value = mock_clusters()
    return ws


def mock_override_choice_from_dict(text: str, choices: dict[str, Any]) -> Any:
    if "warehouse" in text:
        return "abc"
    if " cluster ID" in text:  # cluster override
        return cluster_id


def mock_question_cluster_override(text: str, *, default: str | None = None) -> str:
    if "workspace group names" in text:
        return "<ALL>"
    if "Open job overview in" in text:
        return "no"
    if " cluster ID" in text:
        return "1"
    return "42"


def mock_create_job(**args):
    """Intercept job.create api calls to validate"""
    assert args["job_clusters"] == [], "job_clusters argument should be empty list"
    for task in args["tasks"]:
        if isinstance(task, Task):
            assert task.libraries is None, task.libraries
            assert task.existing_cluster_id == cluster_id
    return jobs.CreateResponse(job_id="abc")


def mock_create_job_for_writable_dbfs(**args):
    """Intercept job.create api calls to validate"""
    assert args["job_clusters"] != [], "job_clusters argument should not be an empty list"
    for task in args["tasks"]:
        if isinstance(task, Task):
            assert task.libraries is not None, task.libraries
            assert task.existing_cluster_id is None
    return jobs.CreateResponse(job_id="abc")


def mock_dbfs(path, f, overwrite):
    """Write protected DBFS"""
    message = "403 error"
    raise PermissionDenied(message)


def test_install_cluster_override_jobs(ws, mocker, tmp_path):
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="a").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.jobs.create = mock_create_job
    install = WorkspaceInstaller(ws)
    install._question = mock_question_cluster_override
    install._current_config.override_clusters = {"main": cluster_id, "tacl": cluster_id}
    install._job_dashboard_task = MagicMock(name="_job_dashboard_task")  # disable problematic task
    install._create_jobs()


def test_write_protected_dbfs(ws, mocker, tmp_path):
    """Simulate write protected DBFS AND override clusters"""
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="a").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.jobs.create = mock_create_job
    ws.dbfs.upload = mock_dbfs
    mocker.patch("builtins.input", return_value="1")
    install = WorkspaceInstaller(ws)
    install._question = mock_question_cluster_override
    install._current_config.override_clusters = {"main": cluster_id, "tacl": cluster_id}
    install._job_dashboard_task = MagicMock(name="_job_dashboard_task")  # disable problematic task
    install._create_jobs()

    res = install._current_config.override_clusters
    assert res is not None
    assert res["main"] == cluster_id
    assert res["tacl"] == cluster_id


def test_writeable_dbfs(ws, mocker, tmp_path):
    """Ensure configure does not add cluster override for happy path of writable DBFS"""
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="a").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.jobs.create = mock_create_job_for_writable_dbfs

    mocker.patch("builtins.input", return_value="1")
    install = WorkspaceInstaller(ws)
    install._question = mock_question_cluster_override

    install._job_dashboard_task = MagicMock(name="_job_dashboard_task")  # disable problematic task
    install._create_jobs()

    res = install._current_config.override_clusters
    assert res is None


def test_replace_clusters_for_integration_tests(ws):
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="a").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    return_value = WorkspaceInstaller.run_for_config(
        ws,
        WorkspaceConfig(inventory_database="a"),
        override_clusters={"main": "abc"},
        sql_backend=MockBackend(),
    )
    assert return_value


def test_run_workflow_creates_proper_failure(ws, mocker):
    def run_now(job_id):
        assert "bar" == job_id

        def result():
            raise OperationFailed(...)

        waiter = mocker.Mock()
        waiter.result = result
        waiter.run_id = "qux"
        return waiter

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
    installer._state.jobs = {"foo": "bar"}
    with pytest.raises(OperationFailed) as failure:
        installer.run_workflow("foo")

    assert "Stuff happens: stuff: does not compute" == str(failure.value)


def test_install_database_happy(ws, mocker, tmp_path):
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="ucx")
    res = install._configure_inventory_database()
    assert "ucx" == res


def test_install_database_unhappy(ws, mocker, tmp_path):
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="main.ucx")

    with pytest.raises(SystemExit):
        install._configure_inventory_database()


def test_build_wheel(ws, tmp_path):
    install = WorkspaceInstaller(ws)
    whl = install._build_wheel(str(tmp_path))
    assert os.path.exists(whl)


def test_save_config(ws, mocker):
    def not_found(_):
        msg = "save_config"
        raise NotFound(msg)

    mocker.patch("builtins.input", return_value="42")

    ws.workspace.get_status = not_found
    ws.warehouses.list = lambda **_: [
        EndpointInfo(name="abc", id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]
    ws.cluster_policies.list = lambda: []
    ws.workspace.download = not_found

    install = WorkspaceInstaller(ws)
    install._choice = lambda _1, _2: "abc (abc, PRO, RUNNING)"
    install._configure()

    ws.workspace.upload.assert_called_with(
        "/Users/me@example.com/.ucx/config.yml",
        b"""default_catalog: ucx_default
include_group_names:
- '42'
inventory_database: '42'
log_level: '42'
num_threads: 42
renamed_group_prefix: '42'
version: 2
warehouse_id: abc
workspace_start_path: /
""",
        format=ImportFormat.AUTO,
        overwrite=False,
    )


def test_migrate_from_v1(ws, mocker):
    mocker.patch("builtins.input", return_value="yes")
    mock_file = MagicMock()
    mocker.patch("builtins.open", return_value=mock_file)
    mocker.patch("base64.b64encode")

    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.config.is_aws = True
    config_bytes = b"""default_catalog: ucx_default
groups:
  auto: true
  backup_group_prefix: db-temp-
inventory_database: ucx
log_level: INFO
num_threads: 8
version: 1
warehouse_id: abc
workspace_start_path: /
    """
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.workspace.get_status = lambda _: ObjectInfo(object_id=123)
    mocker.patch("builtins.input", return_value="42")

    ws.warehouses.list = lambda **_: [
        EndpointInfo(name="abc", id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]
    ws.cluster_policies.list = lambda: []

    install = WorkspaceInstaller(ws)
    install._choice = lambda _1, _2: "None (abc, PRO, RUNNING)"
    install._configure()

    ws.workspace.upload.assert_called_with(
        "/Users/me@example.com/.ucx/config.yml",
        b"""default_catalog: ucx_default
inventory_database: ucx
log_level: INFO
num_threads: 8
renamed_group_prefix: db-temp-
version: 2
warehouse_id: abc
workspace_start_path: /
""",
        format=ImportFormat.AUTO,
        overwrite=True,
    )


def test_migrate_from_v1_selected_groups(ws, mocker):
    mocker.patch("builtins.input", return_value="yes")
    mock_file = MagicMock()
    mocker.patch("builtins.open", return_value=mock_file)
    mocker.patch("base64.b64encode")

    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.config.is_aws = True
    config_bytes = b"""default_catalog: ucx_default
groups:
  backup_group_prefix: 'backup_baguette_prefix'
  selected:
  - '42'
  - '100'
inventory_database: '42'
log_level: '42'
num_threads: 42
version: 1
warehouse_id: abc
workspace_start_path: /
    """
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.workspace.get_status = lambda _: ObjectInfo(object_id=123)
    mocker.patch("builtins.input", return_value="42")

    ws.warehouses.list = lambda **_: [
        EndpointInfo(name="abc", id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]
    ws.cluster_policies.list = lambda: []

    install = WorkspaceInstaller(ws)
    install._choice = lambda _1, _2: "None (abc, PRO, RUNNING)"
    install._configure()

    ws.workspace.upload.assert_called_with(
        "/Users/me@example.com/.ucx/config.yml",
        b"""default_catalog: ucx_default
include_group_names:
- '42'
- '100'
inventory_database: '42'
log_level: '42'
num_threads: 42
renamed_group_prefix: backup_baguette_prefix
version: 2
warehouse_id: abc
workspace_start_path: /
""",
        format=ImportFormat.AUTO,
        overwrite=True,
    )


def test_save_config_auto_groups(ws, mocker):
    def not_found(_):
        raise NotFound(...)

    def mock_question(text: str, *, default: str | None = None) -> str:
        if "workspace group names" in text:
            return "<ALL>"
        else:
            return "42"

    mocker.patch("builtins.input", return_value="42")

    ws.workspace.get_status = not_found
    ws.warehouses.list = lambda **_: [
        EndpointInfo(name="abc", id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]
    ws.cluster_policies.list = lambda: []

    install = WorkspaceInstaller(ws)
    install._question = mock_question
    install._choice = lambda _1, _2: "None (abc, PRO, RUNNING)"
    install._choice_from_dict = mock_override_choice_from_dict
    install._configure()

    ws.workspace.upload.assert_called_with(
        "/Users/me@example.com/.ucx/config.yml",
        b"""default_catalog: ucx_default
inventory_database: '42'
log_level: '42'
num_threads: 42
renamed_group_prefix: '42'
version: 2
warehouse_id: abc
workspace_start_path: /
""",
        format=ImportFormat.AUTO,
        overwrite=False,
    )


def test_save_config_strip_group_names(ws, mocker):
    def not_found(_):
        raise NotFound(...)

    def mock_question(text: str, *, default: str | None = None) -> str:
        if "workspace group names" in text:
            return "g1, g2, g99"
        else:
            return "42"

    mocker.patch("builtins.input", return_value="42")

    ws.workspace.get_status = not_found
    ws.warehouses.list = lambda **_: [
        EndpointInfo(name="abc", id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]
    ws.cluster_policies.list = lambda: []

    install = WorkspaceInstaller(ws)
    install._question = mock_question
    install._choice = lambda _1, _2: "abc (abc, PRO, RUNNING)"
    install._configure()

    ws.workspace.upload.assert_called_with(
        "/Users/me@example.com/.ucx/config.yml",
        b"""default_catalog: ucx_default
include_group_names:
- g1
- g2
- g99
inventory_database: '42'
log_level: '42'
num_threads: 42
renamed_group_prefix: '42'
version: 2
warehouse_id: abc
workspace_start_path: /
""",
        format=ImportFormat.AUTO,
        overwrite=False,
    )


def test_save_config_with_glue(ws, mocker):
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
        raise NotFound(...)

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
        if "cluster ID" in text:  # cluster override
            return cluster_id

    mocker.patch("builtins.input", return_value="42")

    ws.workspace.get_status = not_found
    ws.warehouses.list = lambda **_: [
        EndpointInfo(name="abc", id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]
    ws.cluster_policies.list = lambda: [Policy(definition=policy_def.decode("utf-8"))]

    install = WorkspaceInstaller(ws)
    install._question = mock_question
    install._choice_from_dict = mock_choice_from_dict
    install._configure()

    ws.workspace.upload.assert_called_with(
        "/Users/me@example.com/.ucx/config.yml",
        b"""default_catalog: ucx_default
include_group_names:
- '42'
instance_profile: arn:aws:iam::111222333:instance-profile/foo-instance-profile
inventory_database: '42'
log_level: '42'
num_threads: 42
renamed_group_prefix: '42'
spark_conf:
  spark.databricks.hive.metastore.glueCatalog.enabled: 'true'
version: 2
warehouse_id: abc
workspace_start_path: /
""",
        format=ImportFormat.AUTO,
        overwrite=False,
    )


def test_main_with_existing_conf_does_not_recreate_config(ws, mocker):
    mocker.patch("builtins.input", return_value="yes")
    mock_file = MagicMock()
    mocker.patch("builtins.open", return_value=mock_file)
    mocker.patch("base64.b64encode")
    webbrowser_open = mocker.patch("webbrowser.open")

    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.config.is_aws = True
    config_bytes = b"""default_catalog: ucx_default
include_group_names:
- '42'
instance_profile: arn:aws:iam::111222333:instance-profile/foo-instance-profile
inventory_database: '42'
log_level: '42'
num_threads: 42
renamed_group_prefix: '42'
spark_conf:
  spark.databricks.hive.metastore.glueCatalog.enabled: 'true'
version: 2
warehouse_id: abc
workspace_start_path: /
"""

    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.workspace.get_status = lambda _: ObjectInfo(object_id=123)
    ws.data_sources.list = lambda: [DataSource(id="bcd", warehouse_id="abc")]
    ws.warehouses.list = lambda **_: [EndpointInfo(name="abc", id="abc", warehouse_type=EndpointInfoWarehouseType.PRO)]
    ws.dashboards.create.return_value = Dashboard(id="abc")
    ws.queries.create.return_value = Query(id="abc")
    ws.query_visualizations.create.return_value = Visualization(id="abc")
    ws.dashboard_widgets.create.return_value = Widget(id="abc")
    install = WorkspaceInstaller(ws, sql_backend=MockBackend())
    install._build_wheel = lambda _: Path(__file__)
    install.run()

    webbrowser_open.assert_called_with("https://foo/#workspace/Users/me@example.com/.ucx/README.py")
    # ws.workspace.mkdirs.assert_called_with("/Users/me@example.com/.ucx")


def test_query_metadata(ws, mocker):
    install = WorkspaceInstaller(ws)
    local_query_files = install._find_project_root() / "src/databricks/labs/ucx/queries"
    DashboardFromFiles(ws, InstallState(ws, "any"), local_query_files, "any", "any").validate()


def test_choices_out_of_range(ws, mocker):
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="42")
    with pytest.raises(ValueError):
        install._choice("foo", ["a", "b"])


def test_choices_not_a_number(ws, mocker):
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="two")
    with pytest.raises(ValueError):
        install._choice("foo", ["a", "b"])


def test_choices_happy(ws, mocker):
    install = WorkspaceInstaller(ws)
    mocker.patch("builtins.input", return_value="1")
    res = install._choice("foo", ["a", "b"])
    assert "b" == res


def test_step_list(ws, mocker):
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


def test_create_readme(ws, mocker):
    mocker.patch("builtins.input", return_value="yes")
    webbrowser_open = mocker.patch("webbrowser.open")

    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="a").as_dict()).encode("utf8")
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


def test_replace_pydoc():
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


def test_global_init_script_already_exists_enabled(ws, mocker):
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


def test_global_init_script_already_exists_disabled(ws, mocker):
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


def test_global_init_script_exists_disabled_not_enabled(ws, mocker):
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
@patch("builtins.input", new_callable=MagicMock)
def test_global_init_script_create_new(mock_open, mock_input, ws):
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
    mock_input.return_value = "yes"

    install = WorkspaceInstaller(ws)
    install._install_spark_config_for_hms_lineage()


def test_remove_database(ws, mocker):
    install = WorkspaceInstaller(ws, sql_backend=MockBackend())
    mocker.patch("builtins.input", return_value="yes")
    mocker.patch("databricks.labs.ucx.framework.crawlers.SqlBackend.execute", return_value=None)

    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="testdb", warehouse_id="123").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    install._remove_database()


def test_remove_jobs_no_state(ws, mocker):
    install = WorkspaceInstaller(ws)
    install._state.jobs = {}
    install._remove_jobs()


def test_remove_jobs_with_state_missing_job(ws, mocker):
    install = WorkspaceInstaller(ws)
    install._state.jobs = {"job1": "123"}
    ws.jobs.delete.side_effect = InvalidParameterValue("job id 123 not found")
    install._remove_jobs()


def test_remove_jobs_job(ws, mocker):
    install = WorkspaceInstaller(ws)
    install._state.jobs = {"job1": "123"}
    install._remove_jobs()


def test_remove_warehouse(ws, mocker):
    install = WorkspaceInstaller(ws)
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="testdb", warehouse_id="123").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.warehouses.get.return_value = sql.GetWarehouseResponse(id="123", name="Unity Catalog Migration 123456")
    ws.warehouses.delete.return_value = None
    install._remove_warehouse()


def test_remove_warehouse_not_exists(ws, mocker):
    install = WorkspaceInstaller(ws)
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="testdb", warehouse_id="123").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)

    ws.warehouses.get.return_value = sql.GetWarehouseResponse(id="123", name="Unity Catalog Migration 123456")
    ws.warehouses.delete.side_effect = InvalidParameterValue("warehouse id 123 not found")
    install._remove_warehouse()


def test_remove_install_folder(ws, mocker):
    install = WorkspaceInstaller(ws)
    ws.workspace.delete.return_value = None
    install._remove_install_folder()


def test_remove_install_folder_not_exists(ws, mocker):
    install = WorkspaceInstaller(ws)
    ws.workspace.delete.side_effect = InvalidParameterValue("folder not found")
    install._remove_install_folder()


def test_uninstall(ws, mocker):
    install = WorkspaceInstaller(ws, sql_backend=MockBackend())
    mocker.patch("builtins.input", return_value="yes")
    mocker.patch("databricks.labs.ucx.framework.crawlers.SqlBackend.execute", return_value=None)
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="testdb", warehouse_id="123").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    install._state.jobs = {"job1": "123"}
    ws.warehouses.get.return_value = sql.GetWarehouseResponse(id="123", name="Customer Warehouse 123456")
    ws.workspace.delete.return_value = None
    install.uninstall()


def test_uninstall_no_config_file(ws, mocker):
    def not_found(_):
        raise NotFound(...)

    install = WorkspaceInstaller(ws, sql_backend=MockBackend())
    mocker.patch("builtins.input", return_value="yes")
    mocker.patch("databricks.labs.ucx.framework.crawlers.SqlBackend.execute", return_value=None)
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="testdb", warehouse_id="123").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.workspace.get_status = not_found
    install.uninstall()
