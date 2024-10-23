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
from databricks.labs.ucx.hive_metastore.pipelines_migrate import PipelineRule, PipelineToMigrate

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
