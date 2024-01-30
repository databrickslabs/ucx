import io
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock, create_autospec, patch

import pytest
import yaml
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.parallel import ManyError
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import Wheels, WheelsV2, find_project_root
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    BadRequest,
    InvalidParameterValue,
    NotFound,
    OperationFailed,
    PermissionDenied,
    Unknown,
)
from databricks.sdk.service import iam, jobs, sql
from databricks.sdk.service.compute import (
    GlobalInitScriptDetails,
    GlobalInitScriptDetailsWithContent,
    Policy,
    State,
)
from databricks.sdk.service.jobs import BaseRun, RunResultState, RunState
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
from databricks.sdk.service.workspace import ObjectInfo

import databricks.labs.ucx.uninstall  # noqa
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.install import WorkspaceInstallation, WorkspaceInstaller

from ..unit.framework.mocks import MockBackend


def mock_clusters():
    from databricks.sdk.service.compute import ClusterDetails, DataSecurityMode, State

    return [
        ClusterDetails(
            spark_version="13.3.x-dbrxxx",
            cluster_name="zero",
            data_security_mode=DataSecurityMode.USER_ISOLATION,
            state=State.RUNNING,
            cluster_id="1111-999999-userisol",
        ),
        ClusterDetails(
            spark_version="13.3.x-dbrxxx",
            cluster_name="one",
            data_security_mode=DataSecurityMode.NONE,
            state=State.RUNNING,
            cluster_id='2222-999999-nosecuri',
        ),
        ClusterDetails(
            spark_version="13.3.x-dbrxxx",
            cluster_name="two",
            data_security_mode=DataSecurityMode.LEGACY_TABLE_ACL,
            state=State.RUNNING,
            cluster_id='3333-999999-legacytc',
        ),
    ]


@pytest.fixture
def ws():
    ws = create_autospec(WorkspaceClient)

    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.config.is_aws = True
    ws.config.is_azure = False
    ws.config.is_gcp = False
    ws.workspace.get_status = lambda _: ObjectInfo(object_id=123)
    ws.data_sources.list = lambda: [DataSource(id="bcd", warehouse_id="abc")]
    ws.warehouses.list = lambda **_: [
        EndpointInfo(name="abc", id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]
    ws.dashboards.create.return_value = Dashboard(id="abc")
    ws.jobs.create.return_value = jobs.CreateResponse(job_id=123)
    ws.queries.create.return_value = Query(id="abc")
    ws.query_visualizations.create.return_value = Visualization(id="abc")
    ws.dashboard_widgets.create.return_value = Widget(id="abc")
    ws.clusters.list.return_value = mock_clusters()
    ws.cluster_policies.list = lambda: []
    return ws


def created_job(ws: MagicMock, name: str):
    for call in ws.jobs.method_calls:
        if call.kwargs['name'] == name:
            return call.kwargs
    raise AssertionError(f'call not found: {name}')


def created_job_tasks(ws: MagicMock, name: str) -> dict[str, jobs.Task]:
    call = created_job(ws, name)
    return {_.task_key: _ for _ in call['tasks']}


@pytest.fixture
def mock_installation():
    return MockInstallation({'state.json': {'resources': {'dashboards': {'assessment_main': 'abc'}}}})


@pytest.fixture
def any_prompt():
    return MockPrompts({".*": ""})


def test_install_cluster_override_jobs(ws, mock_installation, any_prompt):
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx', override_clusters={"main": 'one', "tacl": 'two'}),
        mock_installation,
        sql_backend,
        wheels,
        ws,
        any_prompt,
        timedelta(seconds=1),
    )

    workspace_installation.create_jobs()

    tasks = created_job_tasks(ws, '[MOCK] assessment')
    assert 'one' == tasks['assess_jobs'].existing_cluster_id
    assert 'two' == tasks['crawl_grants'].existing_cluster_id


def test_write_protected_dbfs(ws, tmp_path, mock_installation):
    """Simulate write protected DBFS AND override clusters"""
    sql_backend = MockBackend()
    wheels = create_autospec(Wheels)
    wheels.upload_to_dbfs.side_effect = PermissionDenied(...)
    wheels.upload_to_wsfs.return_value = "/a/b/c"

    prompts = MockPrompts(
        {
            ".*pre-existing HMS Legacy cluster ID.*": "1",
            ".*pre-existing Table Access Control cluster ID.*": "1",
            ".*": "",
        }
    )

    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx'),
        mock_installation,
        sql_backend,
        wheels,
        ws,
        prompts,
        timedelta(seconds=1),
    )

    workspace_installation.create_jobs()

    tasks = created_job_tasks(ws, '[MOCK] assessment')
    assert "2222-999999-nosecuri" == tasks['assess_jobs'].existing_cluster_id
    assert '3333-999999-legacytc' == tasks['crawl_grants'].existing_cluster_id

    mock_installation.assert_file_written(
        'config.yml',
        {
            '$version': 2,
            'default_catalog': 'ucx_default',
            'inventory_database': 'ucx',
            'log_level': 'INFO',
            'num_threads': 10,
            'override_clusters': {'main': '2222-999999-nosecuri', 'tacl': '3333-999999-legacytc'},
            'renamed_group_prefix': 'ucx-renamed-',
            'workspace_start_path': '/',
        },
    )


def test_writeable_dbfs(ws, tmp_path, mock_installation, any_prompt):
    """Ensure configure does not add cluster override for happy path of writable DBFS"""
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx'),
        mock_installation,
        sql_backend,
        wheels,
        ws,
        any_prompt,
        timedelta(seconds=1),
    )

    workspace_installation.create_jobs()

    job = created_job(ws, '[MOCK] assessment')
    job_clusters = {_.job_cluster_key: _ for _ in job['job_clusters']}
    assert 'main' in job_clusters
    assert 'tacl' in job_clusters


def test_run_workflow_creates_proper_failure(ws, mocker, any_prompt):
    def run_now(job_id):
        assert 111 == job_id

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
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    installer = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx'),
        MockInstallation(
            {'state.json': {'resources': {'jobs': {"foo": "111"}, 'dashboards': {'assessment_main': 'abc'}}}}
        ),
        sql_backend,
        wheels,
        ws,
        any_prompt,
        timedelta(seconds=1),
    )
    with pytest.raises(Unknown) as failure:
        installer.run_workflow("foo")

    assert "stuff: does not compute" == str(failure.value)


def test_run_workflow_creates_failure_from_mapping(ws, mocker, mock_installation, any_prompt):
    def run_now(job_id):
        assert 111 == job_id

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
    ws.jobs.get_run_output.return_value = jobs.RunOutput(
        error="something: PermissionDenied: does not compute", error_trace="# goes to stderr"
    )
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    installer = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx'),
        MockInstallation(
            {'state.json': {'resources': {'jobs': {"foo": "111"}, 'dashboards': {'assessment_main': 'abc'}}}}
        ),
        sql_backend,
        wheels,
        ws,
        any_prompt,
        timedelta(seconds=1),
    )
    with pytest.raises(PermissionDenied) as failure:
        installer.run_workflow("foo")

    assert str(failure.value) == "does not compute"


def test_run_workflow_creates_failure_many_error(ws, mocker, any_prompt):
    def run_now(job_id):
        assert 111 == job_id

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
            ),
            jobs.RunTask(
                task_key="things",
                state=jobs.RunState(result_state=jobs.RunResultState.TIMEDOUT),
                run_id=124,
            ),
            jobs.RunTask(
                task_key="some",
                state=jobs.RunState(result_state=jobs.RunResultState.FAILED),
                run_id=125,
            ),
        ],
    )
    ws.jobs.get_run_output.return_value = jobs.RunOutput(
        error="something: DataLoss: does not compute", error_trace="# goes to stderr"
    )
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    installer = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx'),
        MockInstallation(
            {'state.json': {'resources': {'jobs': {"foo": "111"}, 'dashboards': {'assessment_main': 'abc'}}}}
        ),
        sql_backend,
        wheels,
        ws,
        any_prompt,
        timedelta(seconds=1),
    )
    with pytest.raises(ManyError) as failure:
        installer.run_workflow("foo")

    assert str(failure.value) == (
        "Detected 3 failures: "
        "DataLoss: does not compute, "
        "DeadlineExceeded: things: The run was stopped after reaching the timeout"
    )


def test_save_config(ws, mock_installation):
    def not_found(_):
        msg = "save_config"
        raise NotFound(msg)

    ws.workspace.get_status = not_found
    ws.warehouses.list = lambda **_: [
        EndpointInfo(name="abc", id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]
    ws.cluster_policies.list = lambda: []
    ws.workspace.download = not_found

    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",
            r".*": "",
        }
    )
    install = WorkspaceInstaller(prompts, mock_installation, ws)
    install.configure()

    mock_installation.assert_file_written(
        'config.yml',
        {
            '$version': 2,
            'default_catalog': 'ucx_default',
            'inventory_database': 'ucx',
            'log_level': 'INFO',
            'num_threads': 8,
            'renamed_group_prefix': 'db-temp-',
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
        },
    )


def test_save_config_strip_group_names(ws, mock_installation):
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",  # specify names
            r".*workspace group names.*": "g1, g2, g99",
            r".*": "",
        }
    )
    install = WorkspaceInstaller(prompts, mock_installation, ws)
    install.configure()

    mock_installation.assert_file_written(
        'config.yml',
        {
            '$version': 2,
            'default_catalog': 'ucx_default',
            'include_group_names': ['g1', 'g2', 'g99'],
            'inventory_database': 'ucx',
            'log_level': 'INFO',
            'num_threads': 8,
            'renamed_group_prefix': 'db-temp-',
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
        },
    )


def test_save_config_with_custom_policy(ws, mock_installation):
    policy_def = b"""{
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
    }"""

    ws.cluster_policies.list = lambda: [
        Policy(
            name="dummy",
            policy_id="0123456789ABCDEF",
            definition=policy_def.decode("utf-8"),
        )
    ]

    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r".*follow a policy.*": "yes",
            r"Choose how to map the workspace groups.*": "2",
            r".*Choose a cluster policy.*": "0",
            r".*": "",
        }
    )

    install = WorkspaceInstaller(prompts, mock_installation, ws)
    install.configure()

    mock_installation.assert_file_written(
        'config.yml',
        {
            '$version': 2,
            'custom_cluster_policy_id': '0123456789ABCDEF',
            'default_catalog': 'ucx_default',
            'inventory_database': 'ucx',
            'log_level': 'INFO',
            'num_threads': 8,
            'renamed_group_prefix': 'db-temp-',
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
        },
    )


def test_save_config_with_glue(ws, mock_installation):
    policy_def = b"""{
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
    }"""
    ws.cluster_policies.list = lambda: [
        Policy(
            name="dummy",
            policy_id="0123456789ABCDEF",
            definition=policy_def.decode("utf-8"),
        )
    ]

    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",
            r".*connect to the external metastore?.*": "yes",
            r".*Choose a cluster policy.*": "0",
            r".*": "",
        }
    )

    install = WorkspaceInstaller(prompts, mock_installation, ws)
    install.configure()

    mock_installation.assert_file_written(
        'config.yml',
        {
            '$version': 2,
            'default_catalog': 'ucx_default',
            'instance_profile': 'arn:aws:iam::111222333:instance-profile/foo-instance-profile',
            'inventory_database': 'ucx',
            'log_level': 'INFO',
            'num_threads': 8,
            'renamed_group_prefix': 'db-temp-',
            'spark_conf': {'spark.databricks.hive.metastore.glueCatalog.enabled': 'true'},
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
        },
    )


def test_main_with_existing_conf_does_not_recreate_config(ws, mocker):
    mock_file = MagicMock()
    mocker.patch("builtins.open", return_value=mock_file)
    mocker.patch("base64.b64encode")
    webbrowser_open = mocker.patch("webbrowser.open")

    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.config.is_aws = True
    ws.config.is_azure = False
    ws.config.is_gcp = False
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
    wheels = create_autospec(Wheels)
    install = WorkspaceInstaller(
        ws,
        sql_backend=MockBackend(),
        promtps=MockPrompts(
            {
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Open job overview.*": "yes",
                r".*": "",
            }
        ),
        wheels=wheels,
    )
    install._build_wheel = lambda _: Path(__file__)
    install.run()

    webbrowser_open.assert_called_with("https://foo/#workspace/Users/me@example.com/.ucx/README.py")


def test_query_metadata(ws):
    local_query_files = find_project_root(__file__) / "src/databricks/labs/ucx/queries"
    DashboardFromFiles(ws, InstallState(ws, "any"), local_query_files, "any", "any").validate()


def test_create_readme(ws, mocker):
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
        install = WorkspaceInstaller(
            ws,
            promtps=MockPrompts(
                {
                    r".*PRO or SERVERLESS SQL warehouse.*": "1",
                    r"Open job overview.*": "yes",
                    r".*": "",
                }
            ),
        )
        install._deployed_steps = {"wl_1": 1, "wl_2": 2}
        install._create_readme()

    webbrowser_open.assert_called_with("https://foo/#workspace/Users/me@example.com/.ucx/README.py")

    _, args, kwargs = ws.mock_calls[0]
    assert args[0] == "/Users/me@example.com/.ucx/README.py"




def test_global_init_script_already_exists_enabled(ws):
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
    install._install_spark_config_for_hms_lineage()


def test_global_init_script_already_exists_disabled(ws):
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
    install = WorkspaceInstaller(
        ws,
        promtps=MockPrompts(
            {
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Open job overview.*": "yes",
                r".*": "",
            }
        ),
    )
    install._install_spark_config_for_hms_lineage()


def test_global_init_script_exists_disabled_not_enabled(ws):
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
    install = WorkspaceInstaller(
        ws,
        promtps=MockPrompts(
            {
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Open job overview.*": "yes",
                r".*": "",
            }
        ),
    )
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

    install = WorkspaceInstaller(
        ws,
        promtps=MockPrompts(
            {
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Open job overview.*": "yes",
                r".*": "",
            }
        ),
    )
    install._install_spark_config_for_hms_lineage()


def test_remove_database(ws, mocker):
    install = WorkspaceInstaller(
        ws,
        sql_backend=MockBackend(),
        promtps=MockPrompts(
            {
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r".*": "",
            }
        ),
    )
    mocker.patch("databricks.labs.ucx.framework.crawlers.SqlBackend.execute", return_value=None)

    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="testdb", warehouse_id="123").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    install._remove_database()


def test_remove_jobs_no_state(ws):
    install = WorkspaceInstaller(ws)
    install._state.jobs = {}
    install._remove_jobs()


def test_remove_jobs_with_state_missing_job(ws):
    install = WorkspaceInstaller(ws)
    install._state.jobs = {"job1": "123"}
    ws.jobs.delete.side_effect = InvalidParameterValue("job id 123 not found")
    install._remove_jobs()


def test_remove_jobs_job(ws):
    install = WorkspaceInstaller(ws)
    install._state.jobs = {"job1": "123"}
    install._remove_jobs()


def test_remove_warehouse(ws):
    install = WorkspaceInstaller(ws)
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="testdb", warehouse_id="123").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.warehouses.get.return_value = sql.GetWarehouseResponse(id="123", name="Unity Catalog Migration 123456")
    ws.warehouses.delete.return_value = None
    install._remove_warehouse()


def test_remove_warehouse_not_exists(ws):
    install = WorkspaceInstaller(ws)
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="testdb", warehouse_id="123").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)

    ws.warehouses.get.return_value = sql.GetWarehouseResponse(id="123", name="Unity Catalog Migration 123456")
    ws.warehouses.delete.side_effect = InvalidParameterValue("warehouse id 123 not found")
    install._remove_warehouse()


def test_remove_install_folder(ws):
    install = WorkspaceInstaller(ws)
    ws.workspace.delete.return_value = None
    install._remove_install_folder()


def test_remove_install_folder_not_exists(ws):
    install = WorkspaceInstaller(ws)
    ws.workspace.delete.side_effect = InvalidParameterValue("folder not found")
    install._remove_install_folder()


def test_uninstall(ws, mocker):
    install = WorkspaceInstaller(
        ws,
        sql_backend=MockBackend(),
        promtps=MockPrompts(
            {
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r".*": "",
            }
        ),
    )
    mocker.patch("databricks.labs.ucx.framework.crawlers.SqlBackend.execute", return_value=None)
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="testdb", warehouse_id="123").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    install._state.jobs = {"job1": "123"}
    ws.warehouses.get.return_value = sql.GetWarehouseResponse(id="123", name="Customer Warehouse 123456")
    ws.workspace.delete.return_value = None
    install.uninstall()


def test_uninstall_no_config_file(ws, mocker):
    install = WorkspaceInstaller(
        ws,
        sql_backend=MockBackend(),
        promtps=MockPrompts(
            {
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r".*": "",
            }
        ),
    )
    mocker.patch("databricks.labs.ucx.framework.crawlers.SqlBackend.execute", return_value=None)
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="testdb", warehouse_id="123").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.workspace.get_status.side_effect = NotFound(...)
    install.uninstall()


def test_repair_run(ws, mocker):
    base = [
        BaseRun(
            job_clusters=None,
            job_id=677268692725050,
            job_parameters=None,
            number_in_job=725118654200173,
            run_id=725118654200173,
            run_name="[UCX] assessment",
            state=RunState(result_state=RunResultState.FAILED),
        )
    ]
    install = WorkspaceInstaller(ws, promtps=MockPrompts({".*": ""}))
    mocker.patch("webbrowser.open")
    install._state.jobs = {"assessment": "123"}
    ws.jobs.list_runs.return_value = base
    ws.jobs.list_runs.repair_run = None
    install.repair_run("assessment")


def test_repair_run_success(ws, caplog):
    base = [
        BaseRun(
            job_clusters=None,
            job_id=677268692725050,
            job_parameters=None,
            number_in_job=725118654200173,
            run_id=725118654200173,
            run_name="[UCX] assessment",
            state=RunState(result_state=RunResultState.SUCCESS),
        )
    ]
    install = WorkspaceInstaller(ws)
    install._state.jobs = {"assessment": "123"}
    ws.jobs.list_runs.return_value = base
    ws.jobs.list_runs.repair_run = None
    install.repair_run("assessment")
    assert "job is not in FAILED state" in caplog.text


def test_repair_run_no_job_id(ws):
    base = [
        BaseRun(
            job_clusters=None,
            job_id=677268692725050,
            job_parameters=None,
            number_in_job=725118654200173,
            run_id=725118654200173,
            run_name="[UCX] assessment",
            state=RunState(result_state=RunResultState.SUCCESS),
        )
    ]
    install = WorkspaceInstaller(ws)
    install._state.jobs = {"assessment": ""}
    ws.jobs.list_runs.return_value = base
    ws.jobs.list_runs.repair_run = None
    install.repair_run("workflow")


def test_repair_run_no_job_run(ws):
    install = WorkspaceInstaller(ws)
    install._state.jobs = {"assessment": "677268692725050"}
    ws.jobs.list_runs.return_value = ""
    ws.jobs.list_runs.repair_run = None
    install.repair_run("assessment")


def test_repair_run_exception(ws):
    install = WorkspaceInstaller(ws)
    install._state.jobs = {"assessment": "123"}
    ws.jobs.list_runs.side_effect = InvalidParameterValue("Workflow does not exists")
    install.repair_run("assessment")


def test_repair_run_result_state(ws, caplog):
    base = [
        BaseRun(
            job_clusters=None,
            job_id=677268692725050,
            job_parameters=None,
            number_in_job=725118654200173,
            run_id=725118654200173,
            run_name="[UCX] assessment",
            state=RunState(result_state=None),
        )
    ]
    install = WorkspaceInstaller(ws, verify_timeout=timedelta(seconds=1))
    install._state.jobs = {"assessment": "123"}
    ws.jobs.list_runs.return_value = base
    ws.jobs.list_runs.repair_run = None
    install.repair_run("assessment")
    assert "Please try after sometime" in caplog.text


def test_create_database(ws, mocker, caplog):
    install = WorkspaceInstaller(
        ws,
        sql_backend=None,
        promtps=MockPrompts(
            {
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r".*": "",
            }
        ),
    )
    mocker.patch(
        "databricks.labs.ucx.install.deploy_schema",
        side_effect=BadRequest(
            "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or "
            "function parameter with name `udf` cannot be resolved"
        ),
    )
    mocker.patch("databricks.labs.ucx.framework.crawlers.SqlBackend.execute", return_value=None)
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="testdb", warehouse_id="123").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    with pytest.raises(BadRequest) as failure:
        install._create_database()

    assert "Kindly uninstall and reinstall UCX" in str(failure.value)


def test_create_database_diff_error(ws, mocker, caplog):
    install = WorkspaceInstaller(
        ws,
        sql_backend=MockBackend(),
        promtps=MockPrompts(
            {
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r".*": "",
            }
        ),
    )
    mocker.patch("databricks.labs.ucx.install.deploy_schema", side_effect=BadRequest("Unknown Error"))
    mocker.patch("databricks.labs.ucx.framework.crawlers.SqlBackend.execute", return_value=None)
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="testdb", warehouse_id="123").as_dict()).encode("utf8")
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    with pytest.raises(BadRequest) as failure:
        install._create_database()

    assert "The UCX Installation Failed" in str(failure.value)
