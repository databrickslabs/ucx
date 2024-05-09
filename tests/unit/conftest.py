import os
import sys
import threading
from unittest.mock import patch, create_autospec
import io
import pytest

import yaml
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.sdk.errors import (
    NotFound,
)
from databricks.sdk.service import iam, jobs
from databricks.sdk.service.compute import ClusterDetails, CreatePolicyResponse, DataSecurityMode, State
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
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.config import Config

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext

pytest.register_assert_rewrite('databricks.labs.blueprint.installation')

# Lock to prevent concurrent execution of tests that patch the environment
_lock = threading.Lock()

PRODUCT_INFO = ProductInfo.from_class(WorkspaceConfig)


def mock_installation() -> MockInstallation:
    return MockInstallation(
        {
            'config.yml': {
                'connect': {
                    'host': 'adb-9999999999999999.14.azuredatabricks.net',
                    'token': '...',
                },
                'inventory_database': 'ucx',
                'warehouse_id': 'abc',
            },
            'mapping.csv': [
                {
                    'catalog_name': 'catalog',
                    'dst_schema': 'schema',
                    'dst_table': 'table',
                    'src_schema': 'schema',
                    'src_table': 'table',
                    'workspace_name': 'workspace',
                },
            ],
        }
    )


@pytest.fixture
def run_workflow(mocker):
    def inner(cb, **replace) -> RuntimeContext:
        with _lock, patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
            pyspark_sql_session = mocker.Mock()
            sys.modules["pyspark.sql.session"] = pyspark_sql_session
            installation = mock_installation()
            if 'installation' not in replace:
                replace['installation'] = installation
            if 'workspace_client' not in replace:
                w = create_autospec(WorkspaceClient)
                w.api_client.do.return_value = {}
                w.permissions.get.return_value = {}
                replace['workspace_client'] = w
            if 'sql_backend' not in replace:
                replace['sql_backend'] = MockBackend()
            if 'config' not in replace:
                replace['config'] = installation.load(WorkspaceConfig)

            module = __import__(cb.__module__, fromlist=[cb.__name__])
            klass, method = cb.__qualname__.split('.', 1)
            workflow = getattr(module, klass)()
            current_task = getattr(workflow, method)

            ctx = RuntimeContext().replace(**replace)
            current_task(ctx)

            return ctx

    yield inner


@pytest.fixture
def acc_client():
    acc = create_autospec(AccountClient)  # pylint: disable=mock-no-usage
    acc.config = Config(host="https://accounts.cloud.databricks.com", account_id="123", token="123")
    return acc


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
