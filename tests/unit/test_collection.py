import io
from datetime import timedelta
from unittest.mock import create_autospec

import pytest
import yaml
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.sql import (
    Dashboard,
    DataSource,
    EndpointInfo,
    EndpointInfoWarehouseType,
    Query,
    Visualization,
    Widget,
)
from databricks.sdk.service import iam, jobs
from databricks.sdk.service.workspace import ObjectInfo
from databricks.sdk.service.compute import State, CreatePolicyResponse
from databricks.sdk.errors import NotFound
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.install import WorkspaceInstallation
from databricks.labs.ucx.installer.workflows import WorkflowsDeployment

PRODUCT_INFO = ProductInfo.from_class(WorkspaceConfig)


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
    workspace_client.cluster_policies.create.return_value = CreatePolicyResponse(policy_id="foo")
    workspace_client.clusters.select_spark_version = lambda **_: "14.2.x-scala2.12"
    workspace_client.clusters.select_node_type = lambda **_: "Standard_F4s"
    workspace_client.workspace.download = download

    return workspace_client


def workspace_installation_prepare(ws_patcher, account_client, prompts):
    sql_backend = MockBackend()
    mock_installation = MockInstallation()
    install_state = InstallState.from_installation(mock_installation)
    wheels = PRODUCT_INFO.wheels(ws_patcher)
    workflows_installer = WorkflowsDeployment(
        WorkspaceConfig(inventory_database="...", policy_id='123'),
        mock_installation,
        install_state,
        ws_patcher,
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
        ws_patcher,
        workflows_installer,
        prompts,
        PRODUCT_INFO,
        account_client,
    )
    return workspace_installation


def test_join_collection_prompt_no_join(ws):
    account_client = create_autospec(AccountClient)
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "no",
            r".*": "",
        }
    )
    workspace_installation = workspace_installation_prepare(ws, account_client, prompts)
    workspace_installation.run()
    account_client.workspaces.list.assert_not_called()


def test_join_collection_no_sync_called(ws):
    account_client = create_autospec(AccountClient)
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "yes",
            r".*": "",
        }
    )
    workspace_installation = workspace_installation_prepare(ws, account_client, prompts)
    workspace_installation.run()
    account_client.get_workspace_client.assert_not_called()


def test_join_collection_join_collection(ws):
    account_client = create_autospec(AccountClient)
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "yes",
            r".*": "",
        }
    )
    workspace_installation = workspace_installation_prepare(ws, account_client, prompts)
    workspace_installation.run()
    account_client.get_workspace_client.assert_not_called()
