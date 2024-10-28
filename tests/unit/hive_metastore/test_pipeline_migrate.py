import datetime
import logging
import sys
from collections.abc import Generator
from itertools import cycle
from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import MockBackend, SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound


from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler, PipelineInfo
from databricks.labs.ucx.hive_metastore.pipelines_migrate import PipelineRule, PipelineToMigrate, PipelineMapping
from unit import mock_pipeline_mapping

logger = logging.getLogger(__name__)

def test_pipeline_rule():
    rule = PipelineRule(workspace_name="ws", src_pipeline_id="id")
    assert rule.workspace_name == "ws"
    assert rule.src_pipeline_id == "id"
    assert rule.target_catalog_name is None
    assert rule.target_schema_name is None
    assert rule.target_pipeline_name is None

    rule = PipelineRule.from_src_dst(workspace_name="ws", src_pipeline_id="id", target_catalog_name="cat", target_schema_name="sch", target_pipeline_name="pipe")
    assert rule.workspace_name == "ws"
    assert rule.src_pipeline_id == "id"
    assert rule.target_catalog_name == "cat"
    assert rule.target_schema_name == "sch"
    assert rule.target_pipeline_name == "pipe"

    rule = PipelineRule.initial(workspace_name="ws", catalog_name="cat", pipeline=PipelineInfo(pipeline_id="id", pipeline_name="pipe",success=1, failures="failed for something"))
    assert rule.workspace_name == "ws"
    assert rule.src_pipeline_id == "id"
    assert rule.target_catalog_name == "cat"
    assert rule.target_schema_name is None
    assert rule.target_pipeline_name == "pipe"

def test_current_pipelines(mock_installation):
    errors = {}
    rows = {
        "hive_metastore.inventory_database.pipelines": [],
    }
    sql_backend = MockBackend(fails_on_first=errors, rows=rows)
    workspace_client = create_autospec(WorkspaceClient)

    pipeline_mapping = PipelineMapping(mock_installation, workspace_client, sql_backend)
    pipelines_crawler = PipelinesCrawler(workspace_client, sql_backend, "schema")

    pipelines = pipeline_mapping.current_pipelines(pipelines_crawler, "workspace_name", "catalog_name")
    assert isinstance(pipelines, Generator)
