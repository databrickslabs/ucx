import io
import logging
import os
import pytest
import sys
import yaml

from requests import PreparedRequest, Response
from unittest.mock import create_autospec, patch

from databricks.sdk import WorkspaceClient
from databricks.labs.ucx.framework.tasks import (
    Task,
    TaskLogger,
    remove_extra_indentation,
    task,
    trigger,
)


def test_replace_pydoc():
    doc = remove_extra_indentation(
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


def test_task_cloud():
    ws = create_autospec(WorkspaceClient)
    ws.config.is_aws = True
    ws.config.is_azure = False
    ws.config.is_gcp = False

    tasks = [
        Task(task_id=0, workflow="wl_1", name="n3", doc="d3", fn=lambda: None, cloud="aws"),
        Task(task_id=1, workflow="wl_2", name="n2", doc="d2", fn=lambda: None, cloud="azure"),
        Task(task_id=2, workflow="wl_1", name="n1", doc="d1", fn=lambda: None, cloud="gcp"),
    ]

    filter_tasks = sorted([t.name for t in tasks if t.cloud_compatible(ws.config)])
    assert filter_tasks == ["n3"]


def test_task_logger(tmp_path):
    app_logger = logging.getLogger("databricks.labs.ucx.foo")
    databricks_logger = logging.getLogger("databricks.sdk.core")
    with TaskLogger(tmp_path, "assessment", "123", "crawl-tables", "234") as task_logger:
        app_logger.info(f"log file is {task_logger.log_file}")
        databricks_logger.debug("something from sdk")
    contents = _log_contents(tmp_path)
    assert len(contents) == 2
    assert "log file is" in contents["logs/assessment/run-234/crawl-tables.log"]
    assert "something from sdk" in contents["logs/assessment/run-234/crawl-tables.log"]
    assert "[run #234](/#job/123/run/234)" in contents["logs/assessment/run-234/README.md"]


def test_task_failure(tmp_path):
    with pytest.raises(ValueError):
        with TaskLogger(tmp_path, "assessment", "123", "crawl-tables", "234"):
            raise ValueError("some value not found")
    contents = _log_contents(tmp_path)
    assert len(contents) == 2
    # CLI debug info present
    assert "databricks workspace export" in contents["logs/assessment/run-234/crawl-tables.log"]
    # log file name present
    assert "logs/assessment/run-234/crawl-tables.log" in contents["logs/assessment/run-234/crawl-tables.log"]
    # traceback present
    assert 'raise ValueError("some value not found")' in contents["logs/assessment/run-234/crawl-tables.log"]


def _log_contents(tmp_path):
    contents = {}
    for path in tmp_path.glob("**/*"):
        if path.is_dir():
            continue
        contents[path.relative_to(tmp_path).as_posix()] = path.read_text()
    return contents


def mock_cfg_response():
    mock_cfg = io.StringIO(
        yaml.dump(
            {
                'version': 2,
                'inventory_database': 'ucx',
                'warehouse_id': 'test',
                'connect': {
                    'host': 'foo',
                    'token': 'bar',
                },
            }
        )
    )

    mock_api_request = create_autospec(PreparedRequest)
    mock_api_request.method = "GET"
    mock_api_request.body = None
    mock_api_request.url = "http://example.com"

    mock_api_response = create_autospec(Response)
    mock_api_response.request = mock_api_request
    mock_api_response.status_code = 200
    mock_api_response.reason = None
    mock_api_response.content.return_value = b'{"userName": "user@example.com"}'

    return mock_cfg, mock_api_response


def test_trigger_task_of_migrate_tables(mocker, capsys):
    # define a mock task that is under "migrate-tables" workflow, which needs 4 parameters including installation
    @task("migrate-tables", job_cluster="migration_sync")
    def mock_migrate_external_tables_sync(cfg, workspace_client, sql_backend, installation):
        """This mock task of migrate-tables"""
        return f"Hello, World! {cfg} {workspace_client} {sql_backend} {installation}"

    mock_cfg, mock_api_response = mock_cfg_response()

    with (
        patch('pathlib.Path.open', return_value=mock_cfg),
        patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}),
        patch("requests.Session.send", return_value=mock_api_response),
    ):
        sys.modules["pyspark.sql.session"] = mocker.Mock()
        trigger("--config=config.yml", "--task=mock_migrate_external_tables_sync")
        assert "This mock task of migrate-tables" in capsys.readouterr().out


def test_trigger_task_of_assessment(mocker, capsys):
    # define a mock task that is under "assessment" workflow, which needs 3 parameters
    @task("assessment", job_cluster="main")
    def mock_crawl_tables(cfg, workspace_client, sql_backend):
        """This mock task of assessment"""
        return f"Hello, World! {cfg} {workspace_client} {sql_backend}"

    mock_cfg, mock_api_response = mock_cfg_response()

    with (
        patch('pathlib.Path.open', return_value=mock_cfg),
        patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}),
        patch("requests.Session.send", return_value=mock_api_response),
    ):
        sys.modules["pyspark.sql.session"] = mocker.Mock()
        trigger("--config=config.yml", "--task=mock_crawl_tables")
        assert "This mock task of assessment" in capsys.readouterr().out
