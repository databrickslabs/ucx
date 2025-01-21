import logging
from unittest.mock import call
import pytest

from databricks.labs.lsql.backends import MockBackend
from databricks.sdk.service.jobs import BaseJob, JobSettings, Task, PipelineTask
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.assessment.jobs import JobsCrawler
from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler
from databricks.labs.ucx.hive_metastore.pipelines_migrate import PipelinesMigrator

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "pipeline_spec, include_flag, expected, api_calls",
    [
        # empty spec
        (
            ("empty-spec", "pipe1", 1, "[]", "creator1"),
            "empty-spec",
            1,
            call(
                'POST',
                '/api/2.0/pipelines/empty-spec/clone',
                body={
                    'catalog': 'catalog_name',
                    'clone_mode': 'MIGRATE_TO_UC',
                    'configuration': {'pipelines.migration.ignoreExplicitPath': 'true'},
                    'name': 'New DLT Pipeline',
                },
                headers={'Accept': 'application/json', 'Content-Type': 'application/json'},
            ),
        ),
        # migrated dlt spec
        (("migrated-dlt-spec", "pipe2", 1, "[]", "creator2"), "migrated-dlt-spec", 0, None),
        # spec with spn
        (
            ("spec-with-spn", "pipe3", 1, "[]", "creator3"),
            "spec-with-spn",
            1,
            call(
                'POST',
                '/api/2.0/pipelines/spec-with-spn/clone',
                body={
                    'catalog': 'catalog_name',
                    'clone_mode': 'MIGRATE_TO_UC',
                    'configuration': {'pipelines.migration.ignoreExplicitPath': 'true'},
                    'name': 'New DLT Pipeline',
                },
                headers={'Accept': 'application/json', 'Content-Type': 'application/json'},
            ),
        ),
        # skip pipeline
        (("skip-pipeline", "pipe3", 1, "[]", "creator3"), "some-other-spec", 0, None),
    ],
)
def test_migrate_pipelines(ws, mock_installation, pipeline_spec, include_flag, expected, api_calls):
    errors = {}
    rows = {
        "`hive_metastore`.`inventory_database`.`pipelines`": [pipeline_spec],
        "`hive_metastore`.`inventory_database`.`jobs`": [
            ("536591785949415", 1, [], "single-job", "anonymous@databricks.com")
        ],
    }
    sql_backend = MockBackend(fails_on_first=errors, rows=rows)
    pipelines_crawler = PipelinesCrawler(ws, sql_backend, "inventory_database")
    jobs_crawler = JobsCrawler(ws, sql_backend, "inventory_database")
    pipelines_migrator = PipelinesMigrator(
        ws, pipelines_crawler, jobs_crawler, "catalog_name", include_pipeline_ids=[include_flag]
    )

    ws.jobs.get.side_effect = [
        BaseJob(
            job_id=536591785949415,
            settings=JobSettings(
                name="single-job",
                tasks=[Task(pipeline_task=PipelineTask(pipeline_id="empty-spec"), task_key="task_key")],
            ),
        ),
        BaseJob(
            job_id=536591785949417,
        ),
    ]

    ws.api_client.do.side_effect = [{"pipeline_id": "new-pipeline-id"}, {}]
    pipelines_migrator.migrate_pipelines()

    assert ws.api_client.do.call_count == expected
    if api_calls:
        ws.api_client.do.assert_has_calls([api_calls])


def test_migrate_pipelines_no_pipelines(ws) -> None:
    sql_backend = MockBackend()
    pipelines_crawler = PipelinesCrawler(ws, sql_backend, "inventory_database")
    jobs_crawler = JobsCrawler(ws, sql_backend, "inventory_database")
    pipelines_migrator = PipelinesMigrator(ws, pipelines_crawler, jobs_crawler, "catalog_name")
    ws.jobs.list.return_value = [BaseJob(job_id=536591785949415), BaseJob(), BaseJob(job_id=536591785949417)]
    pipelines_migrator.migrate_pipelines()


def test_migrate_pipelines_skips_not_found_job(caplog, ws) -> None:
    job_columns = MockBackend.rows("job_id", "success", "failures", "job_name", "creator")
    sql_backend = MockBackend(
        rows={
            "`hive_metastore`.`inventory_database`.`jobs`": job_columns[
                ("536591785949415", 1, [], "single-job", "anonymous@databricks.com")
            ]
        }
    )
    pipelines_crawler = PipelinesCrawler(ws, sql_backend, "inventory_database")
    jobs_crawler = JobsCrawler(ws, sql_backend, "inventory_database")
    pipelines_migrator = PipelinesMigrator(ws, pipelines_crawler, jobs_crawler, "catalog_name")

    ws.jobs.get.side_effect = NotFound

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.hive_metastore.pipelines_migrate"):
        pipelines_migrator.migrate_pipelines()
    assert "Skipping non-existing job: 536591785949415" in caplog.messages
