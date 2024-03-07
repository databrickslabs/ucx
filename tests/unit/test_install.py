import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.parallel import ManyError
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import Wheels, WheelsV2, find_project_root
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    InvalidParameterValue,
    NotFound,
    OperationFailed,
    PermissionDenied,
    Unknown,
)
from databricks.sdk.service import compute, iam, jobs, sql
from databricks.sdk.service.compute import (
    ClusterDetails,
    CreatePolicyResponse,
    DataSecurityMode,
    Policy,
    State,
)
from databricks.sdk.service.jobs import (
    BaseRun,
    RunLifeCycleState,
    RunResultState,
    RunState,
)
from databricks.sdk.service.sql import (
    Dashboard,
    DataSource,
    EndpointInfo,
    EndpointInfoWarehouseType,
    Query,
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
    workspace_client = create_autospec(WorkspaceClient)

    workspace_client.current_user.me = lambda: iam.User(
        user_name="me@example.com", groups=[iam.ComplexValue(display="admins")]
    )
    workspace_client.config.host = "https://foo"
    workspace_client.config.is_aws = True
    workspace_client.config.is_azure = False
    workspace_client.config.is_gcp = False
    workspace_client.workspace.get_status = lambda _: ObjectInfo(object_id=123)
    workspace_client.data_sources.list = lambda: [DataSource(id="bcd", warehouse_id="abc")]
    workspace_client.warehouses.list = lambda **_: [
        EndpointInfo(name="abc", id="abc", warehouse_type=EndpointInfoWarehouseType.PRO, state=State.RUNNING)
    ]
    workspace_client.dashboards.create.return_value = Dashboard(id="abc")
    workspace_client.jobs.create.return_value = jobs.CreateResponse(job_id=123)
    workspace_client.queries.create.return_value = Query(id="abc")
    workspace_client.query_visualizations.create.return_value = Visualization(id="abc")
    workspace_client.dashboard_widgets.create.return_value = Widget(id="abc")
    workspace_client.clusters.list.return_value = mock_clusters()
    workspace_client.cluster_policies.create.return_value = CreatePolicyResponse(policy_id="foo")
    workspace_client.clusters.select_spark_version = lambda latest: "14.2.x-scala2.12"
    workspace_client.clusters.select_node_type = lambda local_disk: "Standard_F4s"

    return workspace_client


def created_job(workspace_client, name):
    for call in workspace_client.jobs.method_calls:
        if call.kwargs['name'] == name:
            return call.kwargs
    raise AssertionError(f'call not found: {name}')


def created_job_tasks(workspace_client: MagicMock, name: str) -> dict[str, jobs.Task]:
    call = created_job(workspace_client, name)
    return {_.task_key: _ for _ in call['tasks']}


@pytest.fixture
def mock_installation():
    return MockInstallation({'state.json': {'resources': {'dashboards': {'assessment_main': 'abc'}}}})


@pytest.fixture
def mock_installation_with_jobs():
    return MockInstallation(
        {'state.json': {'resources': {'jobs': {"assessment": "123"}, 'dashboards': {'assessment_main': 'abc'}}}}
    )


@pytest.fixture
def any_prompt():
    return MockPrompts({".*": ""})


def test_create_database(ws, caplog, mock_installation, any_prompt):
    sql_backend = MockBackend(
        fails_on_first={'CREATE TABLE': '[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable is incorrect'}
    )
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

    with pytest.raises(ManyError) as failure:
        workspace_installation.run()

    assert "Kindly uninstall and reinstall UCX" in str(failure.value)


def test_install_cluster_override_jobs(ws, mock_installation, any_prompt):
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx', override_clusters={"main": 'one', "tacl": 'two'}, policy_id='123'),
        mock_installation,
        sql_backend,
        wheels,
        ws,
        any_prompt,
        timedelta(seconds=1),
    )

    workspace_installation.create_jobs()

    tasks = created_job_tasks(ws, '[MOCK] assessment')
    assert tasks['assess_jobs'].existing_cluster_id == 'one'
    assert tasks['crawl_grants'].existing_cluster_id == 'two'


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
        WorkspaceConfig(inventory_database='ucx', policy_id='123'),
        mock_installation,
        sql_backend,
        wheels,
        ws,
        prompts,
        timedelta(seconds=1),
    )

    workspace_installation.create_jobs()

    tasks = created_job_tasks(ws, '[MOCK] assessment')
    assert tasks['assess_jobs'].existing_cluster_id == "2222-999999-nosecuri"
    assert tasks['crawl_grants'].existing_cluster_id == '3333-999999-legacytc'

    mock_installation.assert_file_written(
        'config.yml',
        {
            'version': 2,
            'default_catalog': 'ucx_default',
            'inventory_database': 'ucx',
            'log_level': 'INFO',
            'num_days_submit_runs_history': 30,
            'num_threads': 10,
            'override_clusters': {'main': '2222-999999-nosecuri', 'tacl': '3333-999999-legacytc'},
            'policy_id': '123',
            'renamed_group_prefix': 'ucx-renamed-',
            'workspace_start_path': '/',
        },
    )


def test_writeable_dbfs(ws, tmp_path, mock_installation, any_prompt):
    """Ensure configure does not add cluster override for happy path of writable DBFS"""
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx', policy_id='123'),
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


def test_run_workflow_creates_proper_failure(ws, mocker, any_prompt, mock_installation_with_jobs):
    def run_now(job_id):
        assert job_id == 123

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
        mock_installation_with_jobs,
        sql_backend,
        wheels,
        ws,
        any_prompt,
        timedelta(seconds=1),
    )
    with pytest.raises(Unknown) as failure:
        installer.run_workflow("assessment")

    assert str(failure.value) == "stuff: does not compute"


def test_run_workflow_creates_failure_from_mapping(
    ws, mocker, mock_installation, any_prompt, mock_installation_with_jobs
):
    def run_now(job_id):
        assert job_id == 123

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
        mock_installation_with_jobs,
        sql_backend,
        wheels,
        ws,
        any_prompt,
        timedelta(seconds=1),
    )
    with pytest.raises(PermissionDenied) as failure:
        installer.run_workflow("assessment")

    assert str(failure.value) == "does not compute"


def test_run_workflow_creates_failure_many_error(ws, mocker, any_prompt, mock_installation_with_jobs):
    def run_now(job_id):
        assert job_id == 123

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
        mock_installation_with_jobs,
        sql_backend,
        wheels,
        ws,
        any_prompt,
        timedelta(seconds=1),
    )
    with pytest.raises(ManyError) as failure:
        installer.run_workflow("assessment")

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
    ws.workspace.download = not_found

    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",
            r".*": "",
            r".*days to analyze submitted runs.*": "1",
        }
    )
    install = WorkspaceInstaller(prompts, mock_installation, ws)
    install.configure()

    mock_installation.assert_file_written(
        'config.yml',
        {
            'version': 2,
            'default_catalog': 'ucx_default',
            'inventory_database': 'ucx',
            'log_level': 'INFO',
            'num_days_submit_runs_history': 30,
            'num_threads': 8,
            'policy_id': 'foo',
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
            'version': 2,
            'default_catalog': 'ucx_default',
            'include_group_names': ['g1', 'g2', 'g99'],
            'inventory_database': 'ucx',
            'log_level': 'INFO',
            'num_days_submit_runs_history': 30,
            'num_threads': 8,
            'policy_id': 'foo',
            'renamed_group_prefix': 'db-temp-',
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
        },
    )


def test_cluster_policy_definition_present_reuse(ws, mock_installation):
    ws.config.is_aws = False
    ws.config.is_azure = True
    ws.config.is_gcp = False
    ws.cluster_policies.list.return_value = [
        Policy(
            policy_id="foo1",
            name="Unity Catalog Migration (ucx) (me@example.com)",
            definition=json.dumps({}),
            description="Custom cluster policy for Unity Catalog Migration (UCX)",
        )
    ]
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",  # specify names
            r".*workspace group names.*": "g1, g2, g99",
            r".*We have identified one or more cluster.*": "No",
            r".*Choose a cluster policy.*": "0",
            r".*": "",
        }
    )
    install = WorkspaceInstaller(prompts, mock_installation, ws)
    install.configure()
    mock_installation.assert_file_written(
        'config.yml',
        {
            'version': 2,
            'default_catalog': 'ucx_default',
            'include_group_names': ['g1', 'g2', 'g99'],
            'inventory_database': 'ucx',
            'log_level': 'INFO',
            'num_days_submit_runs_history': 30,
            'num_threads': 8,
            'policy_id': 'foo1',
            'renamed_group_prefix': 'db-temp-',
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
        },
    )


def test_cluster_policy_definition_azure_hms(ws, mock_installation):
    ws.config.is_aws = False
    ws.config.is_azure = True
    ws.config.is_gcp = False
    policy_definition = {
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL": {"value": "url"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionUserName": {"value": "user1"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionPassword": {"value": "pwd"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionDriverName": {"value": "SQLServerDriver"},
        "spark_conf.spark.sql.hive.metastore.version": {"value": "0.13"},
        "spark_conf.spark.sql.hive.metastore.jars": {"value": "jar1"},
        "aws_attributes.instance_profile_arn": {"value": "role_arn_1"},
    }

    ws.cluster_policies.list.return_value = [
        Policy(
            policy_id="id1",
            name="foo",
            definition=json.dumps(policy_definition),
            description="Custom cluster policy for Unity Catalog Migration (UCX)",
        )
    ]
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",  # specify names
            r".*workspace group names.*": "g1, g2, g99",
            r".*We have identified one or more cluster.*": "Yes",
            r".*Choose a cluster policy.*": "0",
            r".*": "",
        }
    )
    install = WorkspaceInstaller(prompts, mock_installation, ws)
    install.configure()
    policy_definition_actual = {
        "spark_version": {"type": "fixed", "value": "14.2.x-scala2.12"},
        "node_type_id": {"type": "fixed", "value": "Standard_F4s"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL": {"type": "fixed", "value": "url"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionUserName": {"type": "fixed", "value": "user1"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionPassword": {"type": "fixed", "value": "pwd"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionDriverName": {"type": "fixed", "value": "SQLServerDriver"},
        "spark_conf.spark.sql.hive.metastore.version": {"type": "fixed", "value": "0.13"},
        "spark_conf.spark.sql.hive.metastore.jars": {"type": "fixed", "value": "jar1"},
        "azure_attributes.availability": {"type": "fixed", "value": "ON_DEMAND_AZURE"},
    }
    ws.cluster_policies.create.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(policy_definition_actual),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    )


def test_cluster_policy_definition_aws_glue(ws, mock_installation):
    ws.config.is_aws = True
    ws.config.is_azure = False
    ws.config.is_gcp = False
    policy_definition = {
        "spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {"type": "fixed", "value": "true"},
        "aws_attributes.instance_profile_arn": {"value": "role_arn_1"},
    }

    ws.cluster_policies.list.return_value = [
        Policy(
            policy_id="id1",
            name="foo",
            definition=json.dumps(policy_definition),
            description="Custom cluster policy for Unity Catalog Migration (UCX)",
        )
    ]
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",  # specify names
            r".*workspace group names.*": "g1, g2, g99",
            r".*We have identified one or more cluster.*": "Yes",
            r".*Choose a cluster policy.*": "0",
            r".*": "",
        }
    )
    install = WorkspaceInstaller(prompts, mock_installation, ws)
    install.configure()
    policy_definition_actual = {
        "spark_version": {"type": "fixed", "value": "14.2.x-scala2.12"},
        "node_type_id": {"type": "fixed", "value": "Standard_F4s"},
        "spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {"type": "fixed", "value": "true"},
        "aws_attributes.availability": {"type": "fixed", "value": "ON_DEMAND"},
        "aws_attributes.instance_profile_arn": {"type": "fixed", "value": "role_arn_1"},
    }
    ws.cluster_policies.create.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(policy_definition_actual),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    )


def test_cluster_policy_definition_gcp(ws, mock_installation):
    ws.config.is_aws = False
    ws.config.is_azure = False
    ws.config.is_gcp = True
    policy_definition = {
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL": {"value": "url"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionUserName": {"value": "user1"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionPassword": {"value": "pwd"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionDriverName": {"value": "SQLServerDriver"},
        "spark_conf.spark.sql.hive.metastore.version": {"value": "0.13"},
        "spark_conf.spark.sql.hive.metastore.jars": {"value": "jar1"},
    }

    ws.cluster_policies.list.return_value = [
        Policy(
            policy_id="id1",
            name="foo",
            definition=json.dumps(policy_definition),
            description="Custom cluster policy for Unity Catalog Migration (UCX)",
        )
    ]
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",  # specify names
            r".*workspace group names.*": "g1, g2, g99",
            r".*We have identified one or more cluster.*": "Yes",
            r".*Choose a cluster policy.*": "0",
            r".*": "",
        }
    )
    install = WorkspaceInstaller(prompts, mock_installation, ws)
    install.configure()
    policy_definition_actual = {
        "spark_version": {"type": "fixed", "value": "14.2.x-scala2.12"},
        "node_type_id": {"type": "fixed", "value": "Standard_F4s"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL": {"type": "fixed", "value": "url"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionUserName": {"type": "fixed", "value": "user1"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionPassword": {"type": "fixed", "value": "pwd"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionDriverName": {"type": "fixed", "value": "SQLServerDriver"},
        "spark_conf.spark.sql.hive.metastore.version": {"type": "fixed", "value": "0.13"},
        "spark_conf.spark.sql.hive.metastore.jars": {"type": "fixed", "value": "jar1"},
        "gcp_attributes.availability": {"type": "fixed", "value": "ON_DEMAND_GCP"},
    }
    ws.cluster_policies.create.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(policy_definition_actual),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    )


def test_install_edit_policy_with_library(ws, mock_installation, any_prompt):
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx', override_clusters={"main": 'one', "tacl": 'two'}, policy_id="foo"),
        mock_installation,
        sql_backend,
        wheels,
        ws,
        any_prompt,
        timedelta(seconds=1),
    )
    wheels.upload_to_wsfs.return_value = "path1"
    ws.cluster_policies.get.return_value = Policy(
        policy_id="foo", name="Unity Catalog Migration (ucx) (me@example.com)"
    )
    workspace_installation.create_jobs()
    ws.cluster_policies.edit.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        policy_id="foo",
        definition=None,
        libraries=[compute.Library(whl="dbfs:path1")],
    )


def test_install_edit_policy_not_found(ws, mock_installation, any_prompt):
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx', override_clusters={"main": 'one', "tacl": 'two'}, policy_id="foo1"),
        mock_installation,
        sql_backend,
        wheels,
        ws,
        any_prompt,
        timedelta(seconds=1),
    )
    ws.cluster_policies.get.side_effect = NotFound()
    with pytest.raises(NotFound):
        workspace_installation.create_jobs()


def test_install_edit_policy_not_present(ws, mock_installation, any_prompt):
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
    with pytest.raises(InvalidParameterValue):
        workspace_installation.create_jobs()


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
            'version': 2,
            'default_catalog': 'ucx_default',
            'inventory_database': 'ucx',
            'log_level': 'INFO',
            'num_days_submit_runs_history': 30,
            'num_threads': 8,
            'policy_id': 'foo',
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
            'version': 2,
            'default_catalog': 'ucx_default',
            'instance_profile': 'arn:aws:iam::111222333:instance-profile/foo-instance-profile',
            'inventory_database': 'ucx',
            'log_level': 'INFO',
            'num_days_submit_runs_history': 30,
            'num_threads': 8,
            'policy_id': 'foo',
            'renamed_group_prefix': 'db-temp-',
            'spark_conf': {'spark.databricks.hive.metastore.glueCatalog.enabled': 'true'},
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
        },
    )


def test_main_with_existing_conf_does_not_recreate_config(ws, mocker, mock_installation):
    webbrowser_open = mocker.patch("webbrowser.open")
    sql_backend = MockBackend()
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "yes",
            r".*": "",
        }
    )
    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database="...", policy_id='123'),
        mock_installation,
        sql_backend,
        create_autospec(WheelsV2),
        ws,
        prompts,
        verify_timeout=timedelta(seconds=1),
    )
    workspace_installation.run()

    webbrowser_open.assert_called_with('https://localhost/#workspace~/mock/README')


def test_query_metadata(ws):
    local_query_files = find_project_root(__file__) / "src/databricks/labs/ucx/queries"
    DashboardFromFiles(ws, InstallState(ws, "any"), local_query_files, "any", "any").validate()


def test_remove_database(ws):
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    ws = create_autospec(WorkspaceClient)
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            r'Do you want to delete the inventory database.*': 'yes',
        }
    )
    installation = create_autospec(Installation)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(config, installation, sql_backend, wheels, ws, prompts, timeout)

    workspace_installation.uninstall()

    assert sql_backend.queries == ['DROP SCHEMA IF EXISTS hive_metastore.ucx CASCADE']


def test_remove_jobs_no_state(ws):
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    ws = create_autospec(WorkspaceClient)
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = create_autospec(Installation)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(config, installation, sql_backend, wheels, ws, prompts, timeout)

    workspace_installation.uninstall()

    ws.jobs.delete.assert_not_called()


def test_remove_jobs_with_state_missing_job(ws, caplog, mock_installation_with_jobs):
    ws.jobs.delete.side_effect = InvalidParameterValue("job id 123 not found")

    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(
        config, mock_installation_with_jobs, sql_backend, wheels, ws, prompts, timeout
    )

    with caplog.at_level('ERROR'):
        workspace_installation.uninstall()
        assert 'Already deleted: assessment job_id=123.' in caplog.messages

    mock_installation_with_jobs.assert_removed()


def test_remove_warehouse(ws):
    ws.warehouses.get.return_value = sql.GetWarehouseResponse(id="123", name="Unity Catalog Migration 123456")

    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = create_autospec(Installation)
    config = WorkspaceConfig(inventory_database='ucx', warehouse_id="123")
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(config, installation, sql_backend, wheels, ws, prompts, timeout)

    workspace_installation.uninstall()

    ws.warehouses.delete.assert_called_once()


def test_not_remove_warehouse_with_a_different_prefix(ws):
    ws.warehouses.get.return_value = sql.GetWarehouseResponse(id="123", name="Starter Endpoint")

    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = create_autospec(Installation)
    config = WorkspaceConfig(inventory_database='ucx', warehouse_id="123")
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(config, installation, sql_backend, wheels, ws, prompts, timeout)

    workspace_installation.uninstall()

    ws.warehouses.delete.assert_not_called()


def test_remove_secret_scope(ws, caplog):
    wheels = create_autospec(WheelsV2)
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = MockInstallation()
    config = WorkspaceConfig(inventory_database='ucx', uber_spn_id="123")
    timeout = timedelta(seconds=1)
    # ws.secrets.delete_scope.side_effect = NotFound()
    workspace_installation = WorkspaceInstallation(config, installation, MockBackend(), wheels, ws, prompts, timeout)
    workspace_installation.uninstall()
    ws.secrets.delete_scope.assert_called_with('ucx')


def test_remove_secret_scope_no_scope(ws, caplog):
    wheels = create_autospec(WheelsV2)
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = MockInstallation()
    config = WorkspaceConfig(inventory_database='ucx', uber_spn_id="123")
    timeout = timedelta(seconds=1)
    ws.secrets.delete_scope.side_effect = NotFound()
    workspace_installation = WorkspaceInstallation(config, installation, MockBackend(), wheels, ws, prompts, timeout)
    with caplog.at_level('ERROR'):
        workspace_installation.uninstall()
        assert 'Secret scope already deleted' in caplog.messages


def test_remove_cluster_policy_not_exists(ws, caplog):
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = create_autospec(Installation)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    ws.cluster_policies.delete.side_effect = NotFound()
    workspace_installation = WorkspaceInstallation(config, installation, sql_backend, wheels, ws, prompts, timeout)

    with caplog.at_level('ERROR'):
        workspace_installation.uninstall()
        assert 'UCX Policy already deleted' in caplog.messages


def test_remove_warehouse_not_exists(ws, caplog):
    ws.warehouses.delete.side_effect = InvalidParameterValue("warehouse id 123 not found")

    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = create_autospec(Installation)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(config, installation, sql_backend, wheels, ws, prompts, timeout)

    with caplog.at_level('ERROR'):
        workspace_installation.uninstall()
        assert 'Error accessing warehouse details' in caplog.messages


def test_repair_run(ws, mocker, any_prompt, mock_installation_with_jobs):
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
    mocker.patch("webbrowser.open")
    ws.jobs.list_runs.return_value = base
    ws.jobs.list_runs.repair_run = None

    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(
        config, mock_installation_with_jobs, sql_backend, wheels, ws, any_prompt, timeout
    )

    workspace_installation.repair_run("assessment")


def test_repair_run_success(ws, caplog, mock_installation_with_jobs, any_prompt):
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
    ws.jobs.list_runs.return_value = base
    ws.jobs.list_runs.repair_run = None

    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(
        config, mock_installation_with_jobs, sql_backend, wheels, ws, any_prompt, timeout
    )

    workspace_installation.repair_run("assessment")

    assert "job is not in FAILED state" in caplog.text


def test_repair_run_no_job_id(ws, mock_installation, any_prompt, caplog):
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
    ws.jobs.list_runs.return_value = base
    ws.jobs.list_runs.repair_run = None

    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(
        config, mock_installation, sql_backend, wheels, ws, any_prompt, timeout
    )

    with caplog.at_level('WARNING'):
        workspace_installation.repair_run("assessment")
        assert 'skipping assessment: job does not exists hence skipping repair' in caplog.messages


def test_repair_run_no_job_run(ws, mock_installation_with_jobs, any_prompt, caplog):
    ws.jobs.list_runs.return_value = ""
    ws.jobs.list_runs.repair_run = None

    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(
        config, mock_installation_with_jobs, sql_backend, wheels, ws, any_prompt, timeout
    )

    with caplog.at_level('WARNING'):
        workspace_installation.repair_run("assessment")
        assert "skipping assessment: job is not initialized yet. Can't trigger repair run now" in caplog.messages


def test_repair_run_exception(ws, mock_installation_with_jobs, any_prompt, caplog):
    ws.jobs.list_runs.side_effect = InvalidParameterValue("Workflow does not exists")

    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(
        config, mock_installation_with_jobs, sql_backend, wheels, ws, any_prompt, timeout
    )

    with caplog.at_level('WARNING'):
        workspace_installation.repair_run("assessment")
        assert "skipping assessment: Workflow does not exists" in caplog.messages


def test_repair_run_result_state(ws, caplog, mock_installation_with_jobs, any_prompt):
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
    ws.jobs.list_runs.return_value = base
    ws.jobs.list_runs.repair_run = None

    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(
        config, mock_installation_with_jobs, sql_backend, wheels, ws, any_prompt, timeout
    )

    workspace_installation.repair_run("assessment")
    assert "Please try after sometime" in caplog.text


@pytest.mark.parametrize(
    "state,expected",
    [
        (
            RunState(
                result_state=None,
                life_cycle_state=RunLifeCycleState.RUNNING,
            ),
            "RUNNING",
        ),
        (
            RunState(
                result_state=RunResultState.SUCCESS,
                life_cycle_state=RunLifeCycleState.TERMINATED,
            ),
            "SUCCESS",
        ),
        (
            RunState(
                result_state=RunResultState.FAILED,
                life_cycle_state=RunLifeCycleState.TERMINATED,
            ),
            "FAILED",
        ),
        (
            RunState(
                result_state=None,
                life_cycle_state=None,
            ),
            "UNKNOWN",
        ),
    ],
)
def test_latest_job_status_states(ws, mock_installation_with_jobs, any_prompt, state, expected):
    base = [
        BaseRun(
            job_id=123,
            run_name="assessment",
            state=state,
            start_time=1704114000000,
        )
    ]
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(
        config, mock_installation_with_jobs, sql_backend, wheels, ws, any_prompt, timeout
    )
    ws.jobs.list_runs.return_value = base
    status = workspace_installation.latest_job_status()
    assert len(status) == 1
    assert status[0]["state"] == expected


@patch(f"{databricks.labs.ucx.install.__name__}.datetime", wraps=datetime)
@pytest.mark.parametrize(
    "start_time,expected",
    [
        (1704114000000, "1 hour ago"),  # 2024-01-01 13:00:00
        (1704117600000, "less than 1 second ago"),  # 2024-01-01 14:00:00
        (1704116990000, "10 minutes 10 seconds ago"),  # 2024-01-01 13:49:50
        (None, "<never run>"),
    ],
)
def test_latest_job_status_success_with_time(
    mock_datetime, ws, mock_installation_with_jobs, any_prompt, start_time, expected
):
    base = [
        BaseRun(
            job_id=123,
            run_name="assessment",
            state=RunState(
                result_state=RunResultState.SUCCESS,
                life_cycle_state=RunLifeCycleState.TERMINATED,
            ),
            start_time=start_time,
        )
    ]
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(
        config, mock_installation_with_jobs, sql_backend, wheels, ws, any_prompt, timeout
    )
    ws.jobs.list_runs.return_value = base
    faked_now = datetime(2024, 1, 1, 14, 0, 0)
    mock_datetime.now.return_value = faked_now
    status = workspace_installation.latest_job_status()
    assert status[0]["started"] == expected


def test_latest_job_status_list(ws, any_prompt):
    runs = [
        [
            BaseRun(
                job_id=1,
                run_name="job1",
                state=RunState(
                    result_state=None,
                    life_cycle_state=RunLifeCycleState.RUNNING,
                ),
                start_time=1705577671907,
            )
        ],
        [
            BaseRun(
                job_id=2,
                run_name="job2",
                state=RunState(
                    result_state=RunResultState.SUCCESS,
                    life_cycle_state=RunLifeCycleState.TERMINATED,
                ),
                start_time=1705577671907,
            )
        ],
        [],  # the last job has no runs
    ]
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    mock_install = MockInstallation({'state.json': {'resources': {'jobs': {"job1": "1", "job2": "2", "job3": "3"}}}})
    workspace_installation = WorkspaceInstallation(config, mock_install, sql_backend, wheels, ws, any_prompt, timeout)
    ws.jobs.list_runs.side_effect = iter(runs)
    status = workspace_installation.latest_job_status()
    assert len(status) == 3
    assert status[0]["step"] == "job1"
    assert status[0]["state"] == "RUNNING"
    assert status[1]["step"] == "job2"
    assert status[1]["state"] == "SUCCESS"
    assert status[2]["step"] == "job3"
    assert status[2]["state"] == "UNKNOWN"


def test_latest_job_status_no_job_run(ws, mock_installation_with_jobs, any_prompt):
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(
        config, mock_installation_with_jobs, sql_backend, wheels, ws, any_prompt, timeout
    )
    ws.jobs.list_runs.return_value = ""
    status = workspace_installation.latest_job_status()
    assert len(status) == 1
    assert status[0]["step"] == "assessment"


def test_latest_job_status_exception(ws, mock_installation_with_jobs, any_prompt):
    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)
    config = WorkspaceConfig(inventory_database='ucx')
    timeout = timedelta(seconds=1)
    workspace_installation = WorkspaceInstallation(
        config, mock_installation_with_jobs, sql_backend, wheels, ws, any_prompt, timeout
    )
    ws.jobs.list_runs.side_effect = InvalidParameterValue("Workflow does not exists")
    status = workspace_installation.latest_job_status()
    assert len(status) == 0


def test_open_config(ws, mocker, mock_installation):
    webbrowser_open = mocker.patch("webbrowser.open")
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",
            r".*workspace group names.*": "g1, g2, g99",
            r"Open config file in.*": "yes",
            r".*": "",
        }
    )

    install = WorkspaceInstaller(prompts, mock_installation, ws)
    install.configure()

    webbrowser_open.assert_called_with('https://localhost/#workspace~/mock/config.yml')


def test_runs_upgrades_on_too_old_version(ws, any_prompt):
    existing_installation = MockInstallation(
        {
            'state.json': {'resources': {'dashboards': {'assessment_main': 'abc'}}},
            'config.yml': {
                'inventory_database': 'x',
                'warehouse_id': 'abc',
                'connect': {'host': '...', 'token': '...'},
            },
        }
    )
    install = WorkspaceInstaller(any_prompt, existing_installation, ws)

    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)

    # TODO: (HariGS-DB) remove this, once added the policy upgrade
    # TODO: fix along https://github.com/databrickslabs/ucx/issues/1012
    with pytest.raises(InvalidParameterValue):
        install.run(
            verify_timeout=timedelta(seconds=1),
            sql_backend_factory=lambda _: sql_backend,
            wheel_builder_factory=lambda: wheels,
        )


def test_runs_upgrades_on_more_recent_version(ws, any_prompt):
    existing_installation = MockInstallation(
        {
            'version.json': {'version': '0.3.0', 'wheel': '...', 'date': '...'},
            'state.json': {'resources': {'dashboards': {'assessment_main': 'abc'}}},
            'config.yml': {
                'inventory_database': 'x',
                'warehouse_id': 'abc',
                'policy_id': 'abc',  # TODO: (HariGS-DB) remove this, once added the policy upgrade
                'connect': {'host': '...', 'token': '...'},
            },
        }
    )
    install = WorkspaceInstaller(any_prompt, existing_installation, ws)

    sql_backend = MockBackend()
    wheels = create_autospec(WheelsV2)

    install.run(
        verify_timeout=timedelta(seconds=1),
        sql_backend_factory=lambda _: sql_backend,
        wheel_builder_factory=lambda: wheels,
    )

    existing_installation.assert_file_uploaded('logs/README.md')
