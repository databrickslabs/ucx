import json
from datetime import timedelta
from unittest.mock import MagicMock, create_autospec

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
from databricks.sdk.service import iam, jobs, sql
from databricks.sdk.service.compute import CreatePolicyResponse, Policy, State
from databricks.sdk.service.jobs import BaseRun, RunResultState, RunState
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
            'version': 2,
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


def test_run_workflow_creates_proper_failure(ws, mocker, any_prompt, mock_installation_with_jobs):
    def run_now(job_id):
        assert 123 == job_id

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

    assert "stuff: does not compute" == str(failure.value)


def test_run_workflow_creates_failure_from_mapping(
    ws, mocker, mock_installation, any_prompt, mock_installation_with_jobs
):
    def run_now(job_id):
        assert 123 == job_id

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
        assert 123 == job_id

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
    ws.cluster_policies.list = lambda: []
    ws.cluster_policies.create.return_value = CreatePolicyResponse(policy_id="foo")
    ws.clusters.select_spark_version = lambda latest: "14.2.x-scala2.12"
    ws.clusters.select_node_type = lambda local_disk: "Standard_F4s"
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
            'version': 2,
            'default_catalog': 'ucx_default',
            'inventory_database': 'ucx',
            'log_level': 'INFO',
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
    ws.cluster_policies.create.return_value = CreatePolicyResponse(policy_id="foo")
    ws.clusters.select_spark_version = lambda latest: "14.2.x-scala2.12"
    ws.clusters.select_node_type = lambda local_disk: "Standard_F4s"
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
            'num_threads': 8,
            'policy_id': 'foo',
            'renamed_group_prefix': 'db-temp-',
            'warehouse_id': 'abc',
            'workspace_start_path': '/',
        },
    )


def test_cluster_policy_definition_azure_hms(ws, mock_installation):
    install = WorkspaceInstaller(MockPrompts({r".*": ""}), mock_installation, ws)
    ws.config.is_aws = False
    ws.config.is_azure = True
    ws.config.is_gcp = False
    ws.clusters.select_spark_version = lambda latest: "14.2.x-scala2.12"
    ws.clusters.select_node_type = lambda local_disk: "Standard_F4s"
    conf = {
        "spark.hadoop.javax.jdo.option.ConnectionURL": "url",
        "spark.hadoop.javax.jdo.option.ConnectionUserName": "user1",
        "spark.hadoop.javax.jdo.option.ConnectionPassword": "pwd",
        "spark.hadoop.javax.jdo.option.ConnectionDriverName": "SQLServerDriver",
        "sql.hive.metastore.version": "0.13",
        "sql.hive.metastore.jars": "jar1",
    }
    instance_profile = "role_arn_1"
    policy_definition = install._cluster_policy_definition(conf, instance_profile)
    policy_definition_dict = json.loads(policy_definition)
    assert policy_definition_dict["spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL"]["value"] == "url"
    assert policy_definition_dict["spark_conf.spark.hadoop.javax.jdo.option.ConnectionUserName"]["value"] == "user1"
    assert policy_definition_dict["spark_conf.spark.hadoop.javax.jdo.option.ConnectionPassword"]["value"] == "pwd"
    assert (
        policy_definition_dict["spark_conf.spark.hadoop.javax.jdo.option.ConnectionDriverName"]["value"]
        == "SQLServerDriver"
    )
    assert policy_definition_dict["spark_conf.sql.hive.metastore.version"]["value"] == "0.13"
    assert policy_definition_dict["spark_conf.sql.hive.metastore.jars"]["value"] == "jar1"
    assert policy_definition_dict["azure_attributes.availability"]["value"] == "ON_DEMAND_AZURE"
    assert policy_definition_dict["spark_version"]["value"] == "14.2.x-scala2.12"
    assert policy_definition_dict["node_type_id"]["value"] == "Standard_F4s"


def test_cluster_policy_definition_aws_glue(ws):
    install = WorkspaceInstaller(MockPrompts({r".*": ""}), mock_installation, ws)
    ws.config.is_aws = True
    ws.config.is_azure = False
    ws.config.is_gcp = False
    ws.clusters.select_spark_version = lambda latest: "14.2.x-scala2.12"
    ws.clusters.select_node_type = lambda local_disk: "Standard_F4s"
    conf = {"spark.databricks.hive.metastore.glueCatalog.enabled": "True"}
    instance_profile = "role_arn_1"
    policy_definition = install._cluster_policy_definition(conf, instance_profile)
    policy_definition_dict = json.loads(policy_definition)
    assert policy_definition_dict["spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled"]["value"] == "True"
    assert policy_definition_dict["aws_attributes.availability"]["value"] == "ON_DEMAND"
    assert policy_definition_dict["spark_version"]["value"] == "14.2.x-scala2.12"
    assert policy_definition_dict["node_type_id"]["value"] == "Standard_F4s"
    assert policy_definition_dict["aws_attributes.instance_profile_arn"]["value"] == "role_arn_1"


def test_cluster_policy_definition_gcp(ws):
    install = WorkspaceInstaller(MockPrompts({r".*": ""}), mock_installation, ws)
    ws.config.is_aws = False
    ws.config.is_azure = False
    ws.config.is_gcp = True
    ws.clusters.select_spark_version = lambda latest: "14.2.x-scala2.12"
    ws.clusters.select_node_type = lambda local_disk: "Standard_F4s"

    policy_definition = install._cluster_policy_definition(None, "")
    policy_definition_dict = json.loads(policy_definition)
    assert policy_definition_dict["gcp_attributes.availability"]["value"] == "ON_DEMAND_GCP"
    assert policy_definition_dict["spark_version"]["value"] == "14.2.x-scala2.12"
    assert policy_definition_dict["node_type_id"]["value"] == "Standard_F4s"


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
    ws.cluster_policies.create.return_value = CreatePolicyResponse(policy_id="foo")
    ws.clusters.select_spark_version = lambda latest: "14.2.x-scala2.12"
    ws.clusters.select_node_type = lambda local_disk: "Standard_F4s"

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
    ws.cluster_policies.create.return_value = CreatePolicyResponse(policy_id="foo")
    ws.clusters.select_spark_version = lambda latest: "14.2.x-scala2.12"
    ws.clusters.select_node_type = lambda local_disk: "Standard_F4s"

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
        WorkspaceConfig(inventory_database="..."),
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
