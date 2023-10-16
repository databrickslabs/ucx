import os.path

from databricks.labs.ucx.config import ConnectConfig, GroupsConfig, WorkspaceConfig
from databricks.labs.ucx.framework.tasks import _TASKS, Task
from databricks.labs.ucx.runtime import assess_azure_service_principals


def azure_mock_config(mocker) -> WorkspaceConfig:
    config = WorkspaceConfig(
        connect=ConnectConfig(
            host="adb-9999999999999999.14.azuredatabricks.net",
            token="dapifaketoken",
        ),
        inventory_database="ucx",
        groups=GroupsConfig(auto=True),
    )
    return config


def test_azure_mock(mocker):
    cfg = azure_mock_config(mocker)
    assert cfg is not None
    assert cfg.connect.to_databricks_config() is not None


def test_azure_crawler(mocker):
    import sys
    from unittest import mock

    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config(mocker)

        _fetch = mocker.patch(
            "databricks.labs.ucx.framework.crawlers.RuntimeBackend.fetch",
            return_value=[
                ["1", "secret_scope", "secret_key", "tenant_id", "storage_account"],
            ],
        )
        assess_azure_service_principals(cfg)


def test_tasks(mocker):
    tasks = [
        Task(task_id=0, workflow="wl_1", name="n3", doc="d3", fn=lambda: None, is_aws=True),
        Task(task_id=1, workflow="wl_2", name="n2", doc="d2", fn=lambda: None, is_azure=True),
        Task(task_id=2, workflow="wl_1", name="n1", doc="d1", fn=lambda: None, is_gcp=True),
    ]

    assert len([_ for _ in tasks if _.is_azure is True]) == 1
    assert len([_ for _ in tasks if _.is_aws is True]) == 1
    assert len([_ for _ in tasks if _.is_gcp is True]) == 1


def test_assessment_tasks(mocker):
    """Test task decorator"""
    assert len(_TASKS) == 19
    for task in _TASKS:
        assert task is not None
        t = _TASKS[task]
        assert t is not None

    azure = [_TASKS[_] for _ in _TASKS if _TASKS[_].is_azure]
    aws = [_TASKS[_] for _ in _TASKS if _TASKS[_].is_aws]
    gcp = [_TASKS[_] for _ in _TASKS if _TASKS[_].is_gcp]

    assert len(azure) == 1
    assert len(aws) == 0
    assert len(gcp) == 0
