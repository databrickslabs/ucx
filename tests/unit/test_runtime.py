import os.path
import sys
from unittest.mock import create_autospec, patch

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.tasks import _TASKS, Task
from databricks.labs.ucx.runtime import assess_azure_service_principals

from .framework.mocks import MockBackend


def azure_mock_config() -> WorkspaceConfig:
    config = WorkspaceConfig(
        connect=Config(
            host="adb-9999999999999999.14.azuredatabricks.net",
            token="dapifaketoken",
        ),
        inventory_database="ucx",
    )
    return config


def test_azure_crawler(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()

        _fetch = mocker.patch(
            "databricks.labs.ucx.framework.crawlers.RuntimeBackend.fetch",
            return_value=[
                ["1", "secret_scope", "secret_key", "tenant_id", "storage_account"],
            ],
        )
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        assess_azure_service_principals(cfg, ws, sql_backend)


def test_tasks():
    tasks = [
        Task(task_id=0, workflow="wl_1", name="n3", doc="d3", fn=lambda: None, cloud="azure"),
        Task(task_id=1, workflow="wl_2", name="n2", doc="d2", fn=lambda: None, cloud="aws"),
        Task(task_id=2, workflow="wl_1", name="n1", doc="d1", fn=lambda: None, cloud="gcp"),
    ]

    assert len([_ for _ in tasks if _.cloud == "azure"]) == 1
    assert len([_ for _ in tasks if _.cloud == "aws"]) == 1
    assert len([_ for _ in tasks if _.cloud == "gcp"]) == 1


def test_assessment_tasks():
    """Test task decorator"""
    assert len(_TASKS) >= 19
    azure = [v for k, v in _TASKS.items() if v.cloud == "azure"]
    assert len(azure) >= 1
