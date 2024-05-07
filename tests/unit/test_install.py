import io
import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, create_autospec, patch

import pytest
import yaml
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.parallel import ManyError
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2, find_project_root
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import (  # pylint: disable=redefined-builtin
    AlreadyExists,
    InvalidParameterValue,
    NotFound,
    NotImplemented,
    OperationFailed,
    PermissionDenied,
    Unknown,
)
from databricks.sdk.errors.platform import BadRequest
from databricks.sdk.service import iam, jobs, sql
from databricks.sdk.service.compute import ClusterDetails, CreatePolicyResponse, DataSecurityMode, Policy, State
from databricks.sdk.service.jobs import BaseRun, RunLifeCycleState, RunResultState, RunState
from databricks.sdk.service.provisioning import Workspace
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

import databricks.labs.ucx.installer.mixins
import databricks.labs.ucx.uninstall  # noqa
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.install import AccountInstaller, WorkspaceInstallation, WorkspaceInstaller, extract_major_minor
from databricks.labs.ucx.installer.workflows import DeployedWorkflows, WorkflowsDeployment
from databricks.labs.ucx.runtime import Workflows

PRODUCT_INFO = ProductInfo.from_class(WorkspaceConfig)


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
    state = {
        "/Applications/ucx/config.yml": yaml.dump(
            {
                'version': 1,
                'inventory_database': 'ucx_exists',
                'connect': {
                    'host': '...',
                    'token': '...',
                },
            }
        ),
    }

    def download(path: str) -> io.StringIO | io.BytesIO:
        if path not in state:
            raise NotFound(path)
        if ".csv" in path:
            return io.BytesIO(state[path].encode('utf-8'))
        return io.StringIO(state[path])

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
    workspace_client.clusters.select_spark_version = lambda **_: "14.2.x-scala2.12"
    workspace_client.clusters.select_node_type = lambda **_: "Standard_F4s"
    workspace_client.workspace.download = download

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
    return MockInstallation(
        {
            'state.json': {
                'resources': {
                    'dashboards': {'assessment_main': 'abc', 'assessment_estimates': 'def', 'migration_main': 'ghi'}
                }
            }
        }
    )


@pytest.fixture
def mock_installation_with_jobs():
    return MockInstallation(
        {
            'state.json': {
                'resources': {
                    'jobs': {"assessment": "123"},
                    'dashboards': {'assessment_main': 'abc', 'assessment_estimates': 'def'},
                }
            }
        }
    )


@pytest.fixture
def mock_installation_extra_jobs():
    return MockInstallation(
        {
            'state.json': {
                'resources': {
                    'jobs': {"assessment": "123", "extra_job": "123"},
                    'dashboards': {'assessment_main': 'abc', 'assessment_estimates': 'def'},
                }
            }
        }
    )


@pytest.fixture
def any_prompt():
    return MockPrompts({".*": ""})


def not_found(_):
    msg = "save_config"
    raise NotFound(msg)


def test_create_database(ws, caplog, mock_installation, any_prompt):
    sql_backend = MockBackend(
        fails_on_first={'CREATE TABLE': '[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable is incorrect'}
    )
    install_state = InstallState.from_installation(mock_installation)
    wheels = create_autospec(WheelsV2)
    workflows_installation = WorkflowsDeployment(
        WorkspaceConfig(inventory_database="...", policy_id='123'),
        mock_installation,
        install_state,
        ws,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        [],
    )

    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx'),
        mock_installation,
        install_state,
        sql_backend,
        ws,
        workflows_installation,
        any_prompt,
        PRODUCT_INFO,
    )

    with pytest.raises(BadRequest) as failure:
        try:
            workspace_installation.run()
        except ManyError as e:
            assert len(e.errs) == 1
            raise e.errs[0]

    assert "Kindly uninstall and reinstall UCX" in str(failure.value)
    wheels.upload_to_wsfs.assert_not_called()


def test_install_cluster_override_jobs(ws, mock_installation):
    wheels = create_autospec(WheelsV2)
    workflows_installation = WorkflowsDeployment(
        WorkspaceConfig(inventory_database='ucx', override_clusters={"main": 'one', "tacl": 'two'}, policy_id='123'),
        mock_installation,
        InstallState.from_installation(mock_installation),
        ws,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        Workflows.all().tasks(),
    )

    workflows_installation.create_jobs()

    tasks = created_job_tasks(ws, '[MOCK] assessment')
    assert tasks['assess_jobs'].existing_cluster_id == 'one'
    assert tasks['crawl_grants'].existing_cluster_id == 'two'
    assert tasks['estimates_report'].sql_task.dashboard.dashboard_id == 'def'
    wheels.upload_to_wsfs.assert_called_once()
    wheels.upload_to_dbfs.assert_not_called()


def test_writeable_dbfs(ws, tmp_path, mock_installation):
    """Ensure configure does not add cluster override for happy path of writable DBFS"""
    wheels = create_autospec(WheelsV2)
    workflows_installation = WorkflowsDeployment(
        WorkspaceConfig(inventory_database='ucx', policy_id='123'),
        mock_installation,
        InstallState.from_installation(mock_installation),
        ws,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        Workflows.all().tasks(),
    )

    workflows_installation.create_jobs()

    job = created_job(ws, '[MOCK] assessment')
    job_clusters = {_.job_cluster_key: _ for _ in job['job_clusters']}
    assert 'main' in job_clusters
    assert 'tacl' in job_clusters
    assert job_clusters["main"].new_cluster.policy_id == "123"
    wheels.upload_to_dbfs.assert_not_called()
    wheels.upload_to_wsfs.assert_called_once()


def test_run_workflow_creates_proper_failure(ws, mocker, mock_installation_with_jobs):
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
    ws.jobs.wait_get_run_job_terminated_or_skipped.side_effect = OperationFailed("does not compute")
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    installer = DeployedWorkflows(ws, install_state, timedelta(seconds=1))
    with pytest.raises(Unknown) as failure:
        installer.run_workflow("assessment")

    assert str(failure.value) == "stuff: does not compute"


def test_run_workflow_run_id_not_found(ws, mocker, mock_installation_with_jobs):
    def run_now(job_id):
        assert job_id == 123

        def result():
            raise OperationFailed(...)

        waiter = mocker.Mock()
        waiter.result = result
        waiter.run_id = None
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
    ws.jobs.wait_get_run_job_terminated_or_skipped.side_effect = OperationFailed("does not compute")
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    deployed = DeployedWorkflows(ws, install_state, timedelta(seconds=1))
    with pytest.raises(NotFound):
        deployed.run_workflow("assessment")


def test_run_workflow_creates_failure_from_mapping(ws, mocker, mock_installation, mock_installation_with_jobs):
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
    ws.jobs.wait_get_run_job_terminated_or_skipped.side_effect = OperationFailed("does not compute")
    ws.jobs.get_run_output.return_value = jobs.RunOutput(
        error="something: PermissionDenied: does not compute", error_trace="# goes to stderr"
    )
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    deployed = DeployedWorkflows(ws, install_state, timedelta(seconds=1))
    with pytest.raises(PermissionDenied) as failure:
        deployed.run_workflow("assessment")

    assert str(failure.value) == "does not compute"


def test_run_workflow_creates_failure_many_error(ws, mocker, mock_installation_with_jobs):
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
    ws.jobs.wait_get_run_job_terminated_or_skipped.side_effect = OperationFailed("does not compute")
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    deployed = DeployedWorkflows(ws, install_state, timedelta(seconds=1))
    with pytest.raises(ManyError) as failure:
        deployed.run_workflow("assessment")

    assert str(failure.value) == (
        "Detected 3 failures: "
        "DataLoss: does not compute, "
        "DeadlineExceeded: things: The run was stopped after reaching the timeout"
    )


def test_save_config(ws, mock_installation):
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
    install = WorkspaceInstaller(ws).replace(
        prompts=prompts,
        installation=mock_installation,
        product_info=PRODUCT_INFO,
    )
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
            'min_workers': 1,
            'max_workers': 10,
            'policy_id': 'foo',
            'renamed_group_prefix': 'db-temp-',
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
        },
    )


def test_corrupted_config(ws, mock_installation, caplog):
    installation = MockInstallation({'config.yml': "corrupted"})

    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",
            r".*": "",
            r".*days to analyze submitted runs.*": "1",
        }
    )
    install = WorkspaceInstaller(ws).replace(
        prompts=prompts,
        installation=installation,
        product_info=PRODUCT_INFO,
    )
    with caplog.at_level('WARNING'):
        install.configure()

    assert 'Existing installation at ~/mock is corrupted' in caplog.text


def test_save_config_strip_group_names(ws, mock_installation):
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",  # specify names
            r".*workspace group names.*": "g1, g2, g99",
            r".*": "",
        }
    )
    ws.workspace.get_status = not_found

    install = WorkspaceInstaller(ws).replace(
        prompts=prompts,
        installation=mock_installation,
        product_info=PRODUCT_INFO,
    )
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
            'min_workers': 1,
            'max_workers': 10,
            'policy_id': 'foo',
            'renamed_group_prefix': 'db-temp-',
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
        },
    )


def test_create_cluster_policy(ws, mock_installation):
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
    ws.workspace.get_status = not_found
    install = WorkspaceInstaller(ws).replace(
        prompts=prompts,
        installation=mock_installation,
        product_info=PRODUCT_INFO,
    )
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
            'min_workers': 1,
            'max_workers': 10,
            'policy_id': 'foo1',
            'renamed_group_prefix': 'db-temp-',
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
    install_state = InstallState.from_installation(mock_installation)
    wheels = create_autospec(WheelsV2)
    workflows_installer = WorkflowsDeployment(
        WorkspaceConfig(inventory_database="...", policy_id='123'),
        mock_installation,
        install_state,
        ws,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        [],
    )
    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database="...", policy_id='123'),
        mock_installation,
        install_state,
        sql_backend,
        ws,
        workflows_installer,
        prompts,
        PRODUCT_INFO,
    )
    workspace_installation.run()

    webbrowser_open.assert_called_with('https://localhost/#workspace~/mock/README')
    wheels.upload_to_wsfs.assert_called_once()
    wheels.upload_to_dbfs.assert_not_called()


def test_query_metadata(ws):
    local_query_files = find_project_root(__file__) / "src/databricks/labs/ucx/queries"
    DashboardFromFiles(ws, InstallState(ws, "any"), local_query_files, "any", "any").validate()


def test_remove_database(ws):
    sql_backend = MockBackend()
    ws = create_autospec(WorkspaceClient)
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            r'Do you want to delete the inventory database.*': 'yes',
        }
    )
    installation = MockInstallation()
    config = WorkspaceConfig(inventory_database='ucx')
    workflow_installer = create_autospec(WorkflowsDeployment)
    workspace_installation = WorkspaceInstallation(
        config,
        installation,
        InstallState.from_installation(installation),
        sql_backend,
        ws,
        workflow_installer,
        prompts,
        PRODUCT_INFO,
    )

    workspace_installation.uninstall()

    assert sql_backend.queries == ['DROP SCHEMA IF EXISTS hive_metastore.ucx CASCADE']
    ws.jobs.delete.assert_not_called()
    ws.cluster_policies.delete.assert_called_once()
    installation.assert_removed()
    workflow_installer.create_jobs.assert_not_called()


def test_remove_jobs_no_state(ws):
    sql_backend = MockBackend()
    ws = create_autospec(WorkspaceClient)
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = MockInstallation()
    config = WorkspaceConfig(inventory_database='ucx')
    install_state = InstallState.from_installation(installation)
    wheels = create_autospec(WheelsV2)
    workflows_installer = WorkflowsDeployment(
        config,
        installation,
        install_state,
        ws,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        [],
    )
    workspace_installation = WorkspaceInstallation(
        config, installation, install_state, sql_backend, ws, workflows_installer, prompts, PRODUCT_INFO
    )

    workspace_installation.uninstall()

    ws.jobs.delete.assert_not_called()
    installation.assert_removed()
    wheels.upload_to_wsfs.assert_not_called()


def test_remove_jobs_with_state_missing_job(ws, caplog, mock_installation_with_jobs):
    ws.jobs.delete.side_effect = InvalidParameterValue("job id 123 not found")

    sql_backend = MockBackend()
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    config = WorkspaceConfig(inventory_database='ucx')
    installation = mock_installation_with_jobs
    install_state = InstallState.from_installation(installation)
    wheels = create_autospec(WheelsV2)
    workflows_installer = WorkflowsDeployment(
        config,
        installation,
        install_state,
        ws,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        [],
    )
    workspace_installation = WorkspaceInstallation(
        config,
        mock_installation_with_jobs,
        install_state,
        sql_backend,
        ws,
        workflows_installer,
        prompts,
        PRODUCT_INFO,
    )

    with caplog.at_level('ERROR'):
        workspace_installation.uninstall()
        assert 'Already deleted: assessment job_id=123.' in caplog.messages

    mock_installation_with_jobs.assert_removed()
    wheels.upload_to_wsfs.assert_not_called()


def test_remove_warehouse(ws):
    ws.warehouses.get.return_value = sql.GetWarehouseResponse(id="123", name="Unity Catalog Migration 123456")

    sql_backend = MockBackend()
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = MockInstallation()
    config = WorkspaceConfig(inventory_database='ucx', warehouse_id="123")
    workflows_installer = create_autospec(WorkflowsDeployment)
    workspace_installation = WorkspaceInstallation(
        config,
        installation,
        InstallState.from_installation(installation),
        sql_backend,
        ws,
        workflows_installer,
        prompts,
        PRODUCT_INFO,
    )

    workspace_installation.uninstall()

    ws.warehouses.delete.assert_called_once()
    installation.assert_removed()
    workflows_installer.create_jobs.assert_not_called()


def test_not_remove_warehouse_with_a_different_prefix(ws):
    ws.warehouses.get.return_value = sql.GetWarehouseResponse(id="123", name="Starter Endpoint")

    sql_backend = MockBackend()
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = MockInstallation()
    config = WorkspaceConfig(inventory_database='ucx', warehouse_id="123")
    workflows_installer = create_autospec(WorkflowsDeployment)
    workspace_installation = WorkspaceInstallation(
        config,
        installation,
        InstallState.from_installation(installation),
        sql_backend,
        ws,
        workflows_installer,
        prompts,
        PRODUCT_INFO,
    )

    workspace_installation.uninstall()

    ws.warehouses.delete.assert_not_called()
    workflows_installer.create_jobs.assert_not_called()
    installation.assert_removed()


def test_remove_secret_scope(ws, caplog):
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = MockInstallation()
    config = WorkspaceConfig(inventory_database='ucx', uber_spn_id="123")
    workflows_installer = create_autospec(WorkflowsDeployment)
    workspace_installation = WorkspaceInstallation(
        config,
        installation,
        InstallState.from_installation(installation),
        MockBackend(),
        ws,
        workflows_installer,
        prompts,
        PRODUCT_INFO,
    )
    workspace_installation.uninstall()
    ws.secrets.delete_scope.assert_called_with('ucx')
    workflows_installer.create_jobs.assert_not_called()


def test_remove_secret_scope_no_scope(ws, caplog):
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = MockInstallation()
    config = WorkspaceConfig(inventory_database='ucx', uber_spn_id="123")
    workflows_installer = create_autospec(WorkflowsDeployment)
    ws.secrets.delete_scope.side_effect = NotFound()
    workspace_installation = WorkspaceInstallation(
        config,
        installation,
        InstallState.from_installation(installation),
        MockBackend(),
        ws,
        workflows_installer,
        prompts,
        PRODUCT_INFO,
    )
    with caplog.at_level('ERROR'):
        workspace_installation.uninstall()
        assert 'Secret scope already deleted' in caplog.messages

    ws.secrets.delete_scope.assert_called_with('ucx')
    workflows_installer.create_jobs.assert_not_called()


def test_remove_cluster_policy_not_exists(ws, caplog):
    sql_backend = MockBackend()
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = MockInstallation()
    config = WorkspaceConfig(inventory_database='ucx')
    ws.cluster_policies.delete.side_effect = NotFound()
    workflows_installer = create_autospec(WorkflowsDeployment)
    workspace_installation = WorkspaceInstallation(
        config,
        installation,
        InstallState.from_installation(installation),
        sql_backend,
        ws,
        workflows_installer,
        prompts,
        PRODUCT_INFO,
    )

    with caplog.at_level('ERROR'):
        workspace_installation.uninstall()
        assert 'UCX Policy already deleted' in caplog.messages

    installation.assert_removed()
    workflows_installer.create_jobs.assert_not_called()


def test_remove_warehouse_not_exists(ws, caplog):
    ws.warehouses.delete.side_effect = InvalidParameterValue("warehouse id 123 not found")

    sql_backend = MockBackend()
    prompts = MockPrompts(
        {
            r'Do you want to uninstall ucx.*': 'yes',
            'Do you want to delete the inventory database ucx too?': 'no',
        }
    )
    installation = MockInstallation()
    config = WorkspaceConfig(inventory_database='ucx')
    workflows_installer = create_autospec(WorkflowsDeployment)
    workspace_installation = WorkspaceInstallation(
        config,
        installation,
        InstallState.from_installation(installation),
        sql_backend,
        ws,
        workflows_installer,
        prompts,
        PRODUCT_INFO,
    )

    with caplog.at_level('ERROR'):
        workspace_installation.uninstall()
        assert 'Error accessing warehouse details' in caplog.messages

    installation.assert_removed()
    workflows_installer.create_jobs.assert_not_called()


def test_repair_run(ws, mocker, mock_installation_with_jobs):
    mocker.patch("webbrowser.open")
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
    ws.jobs.list_runs.return_value = base
    ws.jobs.list_runs.repair_run = None

    timeout = timedelta(seconds=1)
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    deployed = DeployedWorkflows(ws, install_state, timeout)

    deployed.repair_run("assessment")


def test_repair_run_success(ws, caplog, mock_installation_with_jobs):
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
    timeout = timedelta(seconds=1)
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    deployed = DeployedWorkflows(ws, install_state, timeout)

    deployed.repair_run("assessment")

    assert "job is not in FAILED state" in caplog.text


def test_repair_run_no_job_id(ws, mock_installation, caplog):
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

    timeout = timedelta(seconds=1)
    install_state = InstallState.from_installation(mock_installation)
    deployed = DeployedWorkflows(ws, install_state, timeout)

    with caplog.at_level('WARNING'):
        deployed.repair_run("assessment")
        assert 'Skipping assessment: job does not exists hence skipping repair' in caplog.messages


def test_repair_run_no_job_run(ws, mock_installation_with_jobs, caplog):
    ws.jobs.list_runs.return_value = ""
    ws.jobs.list_runs.repair_run = None

    timeout = timedelta(seconds=1)
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    deployed = DeployedWorkflows(ws, install_state, timeout)

    with caplog.at_level('WARNING'):
        deployed.repair_run("assessment")
        assert "Skipping assessment: job is not initialized yet. Can't trigger repair run now" in caplog.messages


def test_repair_run_exception(ws, mock_installation_with_jobs, caplog):
    ws.jobs.list_runs.side_effect = InvalidParameterValue("Workflow does not exists")

    timeout = timedelta(seconds=1)
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    deployed = DeployedWorkflows(ws, install_state, timeout)

    with caplog.at_level('WARNING'):
        deployed.repair_run("assessment")
        assert "Skipping assessment: Workflow does not exists" in caplog.messages


def test_repair_run_result_state(ws, caplog, mock_installation_with_jobs):
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

    timeout = timedelta(seconds=1)
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    deployed = DeployedWorkflows(ws, install_state, timeout)

    deployed.repair_run("assessment")
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
def test_latest_job_status_states(ws, mock_installation_with_jobs, state, expected):
    base = [
        BaseRun(
            job_id=123,
            run_name="assessment",
            state=state,
            start_time=1704114000000,
        )
    ]
    timeout = timedelta(seconds=1)
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    deployed = DeployedWorkflows(ws, install_state, timeout)
    ws.jobs.list_runs.return_value = base
    status = deployed.latest_job_status()
    assert len(status) == 1
    assert status[0]["state"] == expected


@patch(f"{databricks.labs.ucx.installer.workflows.__name__}.datetime", wraps=datetime)
@pytest.mark.parametrize(
    "start_time,expected",
    [
        (1704114000000, "1 hour ago"),  # 2024-01-01 13:00:00
        (1704117600000, "less than 1 second ago"),  # 2024-01-01 14:00:00
        (1704116990000, "10 minutes 10 seconds ago"),  # 2024-01-01 13:49:50
        (None, "<never run>"),
    ],
)
def test_latest_job_status_success_with_time(mock_datetime, ws, mock_installation_with_jobs, start_time, expected):
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
    timeout = timedelta(seconds=1)
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    deployed = DeployedWorkflows(ws, install_state, timeout)
    ws.jobs.list_runs.return_value = base
    faked_now = datetime(2024, 1, 1, 14, 0, 0)
    mock_datetime.now.return_value = faked_now
    status = deployed.latest_job_status()
    assert status[0]["started"] == expected


def test_latest_job_status_list(ws):
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
    timeout = timedelta(seconds=1)
    installation = MockInstallation({'state.json': {'resources': {'jobs': {"job1": "1", "job2": "2", "job3": "3"}}}})
    install_state = InstallState.from_installation(installation)
    deployed = DeployedWorkflows(ws, install_state, timeout)
    ws.jobs.list_runs.side_effect = iter(runs)
    status = deployed.latest_job_status()
    assert len(status) == 3
    assert status[0]["step"] == "job1"
    assert status[0]["state"] == "RUNNING"
    assert status[1]["step"] == "job2"
    assert status[1]["state"] == "SUCCESS"
    assert status[2]["step"] == "job3"
    assert status[2]["state"] == "UNKNOWN"


def test_latest_job_status_no_job_run(ws, mock_installation_with_jobs):
    timeout = timedelta(seconds=1)
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    deployed = DeployedWorkflows(ws, install_state, timeout)
    ws.jobs.list_runs.return_value = ""
    status = deployed.latest_job_status()
    assert len(status) == 1
    assert status[0]["step"] == "assessment"


def test_latest_job_status_exception(ws, mock_installation_with_jobs):
    timeout = timedelta(seconds=1)
    install_state = InstallState.from_installation(mock_installation_with_jobs)
    deployed = DeployedWorkflows(ws, install_state, timeout)
    ws.jobs.list_runs.side_effect = InvalidParameterValue("Workflow does not exists")
    status = deployed.latest_job_status()
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
    ws.workspace.get_status = not_found

    install = WorkspaceInstaller(ws).replace(
        prompts=prompts,
        installation=mock_installation,
        product_info=PRODUCT_INFO,
    )
    install.configure()

    webbrowser_open.assert_called_with('https://localhost/#workspace~/mock/config.yml')


def test_save_config_should_include_databases(ws, mock_installation):
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",  # specify names
            r"Comma-separated list of databases to migrate.*": "db1,db2",
            r".*": "",
        }
    )
    ws.workspace.get_status = not_found
    install = WorkspaceInstaller(ws).replace(
        prompts=prompts,
        installation=mock_installation,
        product_info=PRODUCT_INFO,
    )
    install.configure()

    mock_installation.assert_file_written(
        'config.yml',
        {
            'version': 2,
            'default_catalog': 'ucx_default',
            'include_databases': ['db1', 'db2'],
            'inventory_database': 'ucx',
            'log_level': 'INFO',
            'num_threads': 8,
            'min_workers': 1,
            'max_workers': 10,
            'policy_id': 'foo',
            'renamed_group_prefix': 'db-temp-',
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
            'num_days_submit_runs_history': 30,
        },
    )


def test_triggering_assessment_wf(ws, mocker, mock_installation):
    ws.jobs.run_now = mocker.Mock()
    mocker.patch("webbrowser.open")
    sql_backend = MockBackend()
    prompts = MockPrompts(
        {
            r".*": "",
            r"Do you want to trigger assessment job ?.*": "yes",
            r"Open assessment Job url that just triggered ?.*": "yes",
        }
    )
    config = WorkspaceConfig(inventory_database="ucx", policy_id='123')
    wheels = create_autospec(WheelsV2)
    installation = mock_installation
    install_state = InstallState.from_installation(installation)
    workflows_installer = WorkflowsDeployment(
        config,
        installation,
        install_state,
        ws,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        Workflows.all().tasks(),
    )
    workspace_installation = WorkspaceInstallation(
        config, installation, install_state, sql_backend, ws, workflows_installer, prompts, PRODUCT_INFO
    )
    workspace_installation.run()
    wheels.upload_to_wsfs.assert_called_once()
    ws.jobs.run_now.assert_not_called()


def test_triggering_assessment_wf_w_job(ws, mocker, mock_installation):
    ws.jobs.run_now = mocker.Mock()
    mocker.patch("webbrowser.open")
    sql_backend = MockBackend()
    prompts = MockPrompts(
        {
            r".*": "",
            r"Do you want to trigger assessment job ?.*": "yes",
            r"Open assessment Job url that just triggered ?.*": "yes",
        }
    )
    config = WorkspaceConfig(inventory_database="ucx", policy_id='123', trigger_job=True)
    wheels = create_autospec(WheelsV2)
    installation = mock_installation
    install_state = InstallState.from_installation(installation)
    workflows_installer = WorkflowsDeployment(
        config,
        installation,
        install_state,
        ws,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        Workflows.all().tasks(),
    )
    workspace_installation = WorkspaceInstallation(
        config, installation, install_state, sql_backend, ws, workflows_installer, prompts, PRODUCT_INFO
    )
    workspace_installation.run()
    wheels.upload_to_wsfs.assert_called_once()
    ws.jobs.run_now.assert_called_once()


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
    wheels = create_autospec(WheelsV2)
    install = WorkspaceInstaller(ws).replace(
        prompts=any_prompt,
        installation=existing_installation,
        product_info=PRODUCT_INFO,
        sql_backend=MockBackend(),
        wheels=wheels,
    )

    install.run(
        verify_timeout=timedelta(seconds=60),
    )

    wheels.upload_to_wsfs.assert_called_once()


def test_runs_upgrades_on_more_recent_version(ws, any_prompt):
    existing_installation = MockInstallation(
        {
            'version.json': {'version': '0.3.0', 'wheel': '...', 'date': '...'},
            'state.json': {'resources': {'dashboards': {'assessment_main': 'abc'}}},
            'config.yml': {
                'inventory_database': 'x',
                'warehouse_id': 'abc',
                'connect': {'host': '...', 'token': '...'},
            },
        }
    )
    wheels = create_autospec(WheelsV2)
    install = WorkspaceInstaller(ws).replace(
        prompts=any_prompt,
        installation=existing_installation,
        product_info=PRODUCT_INFO,
        sql_backend=MockBackend(),
        wheels=wheels,
    )

    install.run(
        verify_timeout=timedelta(seconds=10),
    )

    existing_installation.assert_file_uploaded('logs/README.md')
    wheels.upload_to_wsfs.assert_called_once()


def test_fresh_install(ws, mock_installation):
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",
            r"Open config file in.*": "no",
            r"Parallelism for migrating.*": "1000",
            r"Min workers for auto-scale.*": "2",
            r"Max workers for auto-scale.*": "20",
            r".*": "",
        }
    )
    ws.workspace.get_status = not_found

    install = WorkspaceInstaller(ws).replace(
        prompts=prompts,
        installation=mock_installation,
        product_info=PRODUCT_INFO,
    )
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
            'spark_conf': {'spark.sql.sources.parallelPartitionDiscovery.parallelism': '1000'},
            'min_workers': 2,
            'max_workers': 20,
            'renamed_group_prefix': 'db-temp-',
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
        },
    )


def test_remove_jobs(ws, caplog, mock_installation_extra_jobs, any_prompt):
    sql_backend = MockBackend()
    install_state = InstallState.from_installation(mock_installation_extra_jobs)
    wheels = create_autospec(WheelsV2)
    workflows_installation = WorkflowsDeployment(
        WorkspaceConfig(inventory_database="...", policy_id='123'),
        mock_installation_extra_jobs,
        install_state,
        ws,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        [],
    )

    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx'),
        mock_installation_extra_jobs,
        install_state,
        sql_backend,
        ws,
        workflows_installation,
        any_prompt,
        PRODUCT_INFO,
    )

    workspace_installation.run()
    ws.jobs.delete.assert_called_with("123")
    wheels.upload_to_wsfs.assert_called_once()


def test_remove_jobs_already_deleted(ws, caplog, mock_installation_extra_jobs, any_prompt):
    sql_backend = MockBackend()
    ws.jobs.delete.side_effect = InvalidParameterValue(...)
    install_state = InstallState.from_installation(mock_installation_extra_jobs)
    wheels = create_autospec(WheelsV2)
    workflows_installation = WorkflowsDeployment(
        WorkspaceConfig(inventory_database="...", policy_id='123'),
        mock_installation_extra_jobs,
        install_state,
        ws,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        [],
    )

    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database='ucx'),
        mock_installation_extra_jobs,
        install_state,
        sql_backend,
        ws,
        workflows_installation,
        any_prompt,
        PRODUCT_INFO,
    )

    workspace_installation.run()
    wheels.upload_to_wsfs.assert_called_once()


def test_get_existing_installation_global(ws, mock_installation):
    base_prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",
            r"Open config file in.*": "no",
            r".*": "",
        }
    )

    first_prompts = base_prompts.extend(
        {
            r"Inventory Database stored in hive_metastore.*": "ucx_global",
            r".*": "",
        }
    )

    installation = MockInstallation(
        {
            'config.yml': {
                'inventory_database': 'ucx_global',
                'connect': {
                    'host': '...',
                    'token': '...',
                },
            },
        }
    )

    first_install = WorkspaceInstaller(ws).replace(
        prompts=first_prompts,
        installation=installation,
        product_info=PRODUCT_INFO,
    )
    workspace_config = first_install.configure()
    assert workspace_config.inventory_database == 'ucx_global'

    force_user_environ = {'UCX_FORCE_INSTALL': 'user'}

    second_prompts = base_prompts.extend(
        {
            r".*UCX is already installed on this workspace.*": "no",
        }
    )
    # test for force user install variable without prompts
    second_install = WorkspaceInstaller(ws, force_user_environ).replace(
        prompts=second_prompts,
        installation=installation,
        product_info=PRODUCT_INFO,
    )
    with pytest.raises(RuntimeWarning, match='UCX is already installed, but no confirmation'):
        second_install.configure()

    # test for force user install variable with prompts
    third_prompts = base_prompts.extend(
        {
            r".*UCX is already installed on this workspace.*": "yes",
            r"Inventory Database stored in hive_metastore.*": "ucx_user",
        }
    )
    third_install = WorkspaceInstaller(ws, force_user_environ).replace(
        prompts=third_prompts,
        installation=installation,
        product_info=PRODUCT_INFO,
    )
    workspace_config = third_install.configure()
    assert workspace_config.inventory_database == 'ucx_user'


def test_existing_installation_user(ws, mock_installation):
    # test configure on existing user install
    base_prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",
            r".*workspace group names.*": "g1, g2, g99",
            r"Open config file in.*": "no",
            r".*": "",
        }
    )

    first_prompts = base_prompts.extend(
        {
            r".*UCX is already installed on this workspace.*": "yes",
            r"Inventory Database stored in hive_metastore.*": "ucx_user",
            r".*": "",
        }
    )

    installation = MockInstallation(
        {
            'config.yml': {
                'inventory_database': 'ucx_user',
                'connect': {
                    'host': '...',
                    'token': '...',
                },
            },
        },
        is_global=False,
    )
    first_install = WorkspaceInstaller(ws).replace(
        prompts=first_prompts,
        installation=installation,
        product_info=PRODUCT_INFO,
    )
    workspace_config = first_install.configure()
    assert workspace_config.inventory_database == 'ucx_user'

    # test for force global install variable without prompts
    # resetting prompts to remove confirmation
    second_prompts = base_prompts.extend(
        {
            r".*UCX is already installed on this workspace.*": "no",
        }
    )

    force_global_env = {'UCX_FORCE_INSTALL': 'global'}
    second_install = WorkspaceInstaller(ws, force_global_env).replace(
        prompts=second_prompts,
        installation=installation,
        product_info=PRODUCT_INFO,
    )
    with pytest.raises(RuntimeWarning, match='UCX is already installed, but no confirmation'):
        second_install.configure()

    # test for force global install variable with prompts
    third_prompts = base_prompts.extend(
        {
            r".*UCX is already installed on this workspace.*": "yes",
            r"Inventory Database stored in hive_metastore.*": "ucx_user_new",
            r".*": "",
        }
    )

    third_install = WorkspaceInstaller(ws, force_global_env).replace(
        prompts=third_prompts,
        installation=installation,
        product_info=PRODUCT_INFO,
    )
    with pytest.raises(NotImplemented, match="Migration needed. Not implemented yet."):
        third_install.configure()


def test_databricks_runtime_version_set(ws, mock_installation):
    prompts = MockPrompts(
        {
            r".*": "",
        }
    )
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    environ = {'DATABRICKS_RUNTIME_VERSION': "13.3"}

    with pytest.raises(SystemExit, match="WorkspaceInstaller is not supposed to be executed in Databricks Runtime"):
        WorkspaceInstaller(ws, environ).replace(
            prompts=prompts,
            installation=mock_installation,
            product_info=product_info,
        )


def test_check_inventory_database_exists(ws, mock_installation):
    ws.current_user.me().user_name = "foo"

    prompts = MockPrompts(
        {
            r".*Inventory Database stored in hive_metastore": "ucx_exists",
            r".*": "",
            r"Choose how to map the workspace groups.*": "2",
            r".*workspace group names.*": "g1, g2, g99",
        }
    )

    installation = Installation(ws, 'ucx')
    install = WorkspaceInstaller(ws).replace(
        prompts=prompts,
        installation=installation,
        product_info=PRODUCT_INFO,
    )

    with pytest.raises(AlreadyExists, match="Inventory database 'ucx_exists' already exists in another installation"):
        install.configure()


def test_user_not_admin(ws, mock_installation):
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="group1")])
    wheels = create_autospec(WheelsV2)
    workspace_installation = WorkflowsDeployment(
        WorkspaceConfig(inventory_database='ucx', policy_id='123'),
        mock_installation,
        InstallState.from_installation(mock_installation),
        ws,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        Workflows.all().tasks(),
    )

    with pytest.raises(PermissionError) as failure:
        workspace_installation.create_jobs()
    assert "Current user is not a workspace admin" in str(failure.value)

    wheels.upload_to_wsfs.assert_called_once()


@pytest.mark.parametrize(
    "result_state,expected",
    [
        (RunState(result_state=RunResultState.SUCCESS, life_cycle_state=RunLifeCycleState.TERMINATED), True),
        (RunState(result_state=RunResultState.FAILED, life_cycle_state=RunLifeCycleState.TERMINATED), False),
    ],
)
def test_validate_step(ws, result_state, expected):
    installation = MockInstallation(
        {
            'state.json': {
                'resources': {
                    'jobs': {"assessment": "123"},
                }
            }
        }
    )
    install_state = InstallState.from_installation(installation)
    deployed = DeployedWorkflows(ws, install_state, timedelta(seconds=1))
    ws.jobs.list_runs.return_value = [
        BaseRun(
            job_id=123,
            run_id=456,
            run_name="assessment",
            state=RunState(result_state=None, life_cycle_state=RunLifeCycleState.RUNNING),
        )
    ]

    ws.jobs.wait_get_run_job_terminated_or_skipped.return_value = BaseRun(
        job_id=123,
        run_id=456,
        run_name="assessment",
        state=RunState(result_state=RunResultState.SUCCESS, life_cycle_state=RunLifeCycleState.TERMINATED),
    )

    ws.jobs.get_run.return_value = BaseRun(
        job_id=123,
        run_id=456,
        run_name="assessment",
        state=result_state,
    )

    assert deployed.validate_step("assessment") == expected


def test_are_remote_local_versions_equal(ws, mock_installation, mocker):
    ws.jobs.run_now = mocker.Mock()

    mocker.patch("webbrowser.open")
    base_prompts = MockPrompts(
        {
            r"Open config file in.*": "yes",
            r"Open job overview in your browser.*": "yes",
            r"Do you want to trigger assessment job ?.*": "yes",
            r"Open assessment Job url that just triggered ?.*": "yes",
            r".*": "",
        }
    )

    product_info = create_autospec(ProductInfo)
    product_info.released_version.return_value = "0.3.0"

    installation = MockInstallation(
        {
            'config.yml': {
                'inventory_database': 'ucx_user',
                'connect': {
                    'host': '...',
                    'token': '...',
                },
            },
            'version.json': {'version': '0.3.0', 'wheel': '...', 'date': '...'},
        },
        is_global=False,
    )

    install = WorkspaceInstaller(ws).replace(
        prompts=base_prompts,
        installation=installation,
        product_info=product_info,
    )

    # raises runtime warning when versions match and no override provided
    with pytest.raises(
        RuntimeWarning,
        match="UCX workspace remote and local install versions are same and no override is requested. Exiting...",
    ):
        install.configure()

    first_prompts = base_prompts.extend(
        {
            r"Do you want to update the existing installation?": "yes",
        }
    )
    install = WorkspaceInstaller(ws).replace(
        prompts=first_prompts,
        installation=installation,
        product_info=product_info,
    )

    # finishes successfully when versions match and override is provided
    config = install.configure()
    assert config.inventory_database == "ucx_user"

    # finishes successfully when versions don't match and no override is provided/needed
    product_info.released_version.return_value = "0.4.1"
    install = WorkspaceInstaller(ws).replace(
        prompts=base_prompts,
        installation=installation,
        product_info=product_info,
    )
    config = install.configure()
    assert config.inventory_database == "ucx_user"


def test_extract_major_minor_versions():
    version_string1 = "0.3.123151"
    version_string2 = "0.17.1232141"

    assert extract_major_minor(version_string1) == "0.3"
    assert extract_major_minor(version_string2) == "0.17"

    version_string3 = "should not match"
    assert extract_major_minor(version_string3) is None


def test_account_installer(ws):
    acc = create_autospec(AccountClient)
    acc.workspaces.list.return_value = [
        Workspace(workspace_id=123, deployment_name="test"),
        Workspace(workspace_id=456, deployment_name="test2"),
    ]
    acc.get_workspace_client.return_value = ws

    account_installer = AccountInstaller(acc)
    account_installer.replace(
        prompts=MockPrompts(
            {
                r"UCX has detected the following workspaces*": "Yes",
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Choose how to map the workspace groups.*": "0",
                r"Do you want to install UCX on the remaining*": "Yes",
                r".*": "",
            }
        ),
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )
    account_installer.install_on_account()
    ws.workspace.upload.assert_called()
    # should have 4 uploaded call, 2 for config.yml, 2 for workspace.json
    assert ws.workspace.upload.call_count == 4


@pytest.fixture
def mock_ws():
    def get_status(path: str):
        raise NotFound(path)

    workspace_client = create_autospec(WorkspaceClient)  # pylint: disable=mock-no-usage
    workspace_client.workspace.get_status = get_status

    return workspace_client


def test_global_workspace_installer(mock_ws):
    workspace_installer = WorkspaceInstaller(
        mock_ws,
        {'UCX_FORCE_INSTALL': 'global'},
    )
    # installation folder should start with /Applications
    assert workspace_installer.install_state.install_folder().startswith("/Applications")


def test_user_workspace_installer(mock_ws):
    workspace_installer = WorkspaceInstaller(mock_ws, {'UCX_FORCE_INSTALL': 'user'})
    # installation folder should start with /Users/
    assert workspace_installer.install_state.install_folder().startswith("/Users/")


def test_save_config_ext_hms(ws, mock_installation):
    ws.get_workspace_id.return_value = 12345678
    cluster_policy = {
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
            definition=json.dumps(cluster_policy),
            description="Custom cluster policy for Unity Catalog Migration (UCX)",
        )
    ]
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Choose how to map the workspace groups.*": "2",  # specify names
            r"Comma-separated list of databases to migrate.*": "db1,db2",
            r".*": "",
        }
    )
    ws.workspace.get_status = not_found
    install = WorkspaceInstaller(ws).replace(
        prompts=prompts,
        installation=mock_installation,
        product_info=PRODUCT_INFO,
    )
    install.configure()

    mock_installation.assert_file_written(
        'config.yml',
        {
            'version': 2,
            'default_catalog': 'ucx_default',
            'include_databases': ['db1', 'db2'],
            'inventory_database': 'ucx_12345678',
            'log_level': 'INFO',
            'num_threads': 8,
            'min_workers': 1,
            'max_workers': 10,
            'policy_id': 'foo',
            'renamed_group_prefix': 'db-temp-',
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
            'num_days_submit_runs_history': 30,
        },
    )
