import logging
from collections.abc import Generator

from databricks.labs.lsql.backends import MockBackend


from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler, PipelineInfo
from databricks.labs.ucx.hive_metastore.pipelines_migrate import PipelineRule, PipelineMapping, PipelinesMigrator

logger = logging.getLogger(__name__)


def test_pipeline_rule():
    rule = PipelineRule(workspace_name="ws", src_pipeline_id="id")
    assert rule.workspace_name == "ws"
    assert rule.src_pipeline_id == "id"
    assert rule.target_catalog_name is None
    assert rule.target_schema_name is None
    assert rule.target_pipeline_name is None

    rule = PipelineRule.from_src_dst(
        workspace_name="ws",
        src_pipeline_id="id",
        target_catalog_name="cat",
        target_schema_name="sch",
        target_pipeline_name="pipe",
    )
    assert rule.workspace_name == "ws"
    assert rule.src_pipeline_id == "id"
    assert rule.target_catalog_name == "cat"
    assert rule.target_schema_name == "sch"
    assert rule.target_pipeline_name == "pipe"

    rule = PipelineRule.initial(
        workspace_name="ws",
        catalog_name="cat",
        pipeline=PipelineInfo(pipeline_id="id", pipeline_name="pipe", success=1, failures="failed for something"),
    )
    assert rule.workspace_name == "ws"
    assert rule.src_pipeline_id == "id"
    assert rule.target_catalog_name == "cat"
    assert rule.target_schema_name is None
    assert rule.target_pipeline_name == "pipe"


def test_current_pipelines(ws, mock_installation):
    errors = {}
    rows = {
        "`hive_metastore`.`inventory_database`.`pipelines`": [
            ("id1", "pipe1", 1, "[]", "creator1"),
            ("id2", "pipe2", 1, "[]", "creator2"),
            ("id3", "pipe3", 1, "[]", "creator3"),
        ],
    }
    sql_backend = MockBackend(fails_on_first=errors, rows=rows)

    pipeline_mapping = PipelineMapping(mock_installation, ws, sql_backend)
    pipelines_crawler = PipelinesCrawler(ws, sql_backend, "inventory_database")
    pipelines = pipeline_mapping.current_pipelines(pipelines_crawler, "workspace_name", "catalog_name")
    assert isinstance(pipelines, Generator)
    assert len(list(pipelines)) == 3


def test_load(ws, mock_installation):
    sql_backend = MockBackend()
    # TODO:
    # try to add the pipeline mapping from this file but its unable to fetch
    # add test for NotFound when fetching the pipeline mapping
    # pipeline_mapping_file = """{
    #         'workspace_name': 'test_workspace',
    #         'src_pipeline_id': 'pipeline_123',
    #         'target_catalog_name': 'test_catalog',
    #         'target_schema_name': None,
    #         'target_pipeline_name': None,
    #     }"""
    # mock_installation.upload("pipeline_mapping.csv", pipeline_mapping_file.encode("ASCII"))

    pipeline_mapping = PipelineMapping(mock_installation, ws, sql_backend)
    pipelines_rules_fetch = pipeline_mapping.load()
    assert len(pipelines_rules_fetch) == 1


def test_pipeline_to_migrate(ws, mock_installation):
    errors = {}
    rows = {
        "`hive_metastore`.`inventory_database`.`pipelines`": [
            ("123", "pipe1", 1, "[]", "creator1"),
            ("456", "pipe2", 1, "[]", "creator2"),
            ("789", "pipe3", 1, "[]", "creator3"),
        ],
    }
    sql_backend = MockBackend(fails_on_first=errors, rows=rows)

    pipeline_mapping = PipelineMapping(mock_installation, ws, sql_backend)
    pipelines_crawler = PipelinesCrawler(ws, sql_backend, "inventory_database")

    pipelines_to_migrate = pipeline_mapping.get_pipelines_to_migrate(pipelines_crawler)
    assert len(pipelines_to_migrate) == 1


def test_migrate_pipelines(ws, mock_installation):
    errors = {}
    rows = {
        "`hive_metastore`.`inventory_database`.`pipelines`": [
            ("123", "pipe1", 1, "[]", "creator1"),
            ("456", "pipe2", 1, "[]", "creator2"),
            ("789", "pipe3", 1, "[]", "creator3"),
        ],
    }
    sql_backend = MockBackend(fails_on_first=errors, rows=rows)

    pipeline_mapping = PipelineMapping(mock_installation, ws, sql_backend)
    pipelines_crawler = PipelinesCrawler(ws, sql_backend, "inventory_database")
    pipelines_migrator = PipelinesMigrator(ws, pipelines_crawler, pipeline_mapping)
    pipelines_migrator.migrate_pipelines()
    ws.api_client.do.assert_called_once()
