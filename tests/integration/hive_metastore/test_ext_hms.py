import dataclasses
import logging
from datetime import timedelta

import pytest
from databricks.labs.blueprint.installer import RawState
from databricks.labs.lsql.backends import CommandExecutionBackend, SqlBackend
from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried

logger = logging.getLogger(__name__)


@pytest.fixture
def sql_backend(ws, env_or_skip) -> SqlBackend:
    cluster_id = env_or_skip("TEST_EXT_HMS_CLUSTER_ID")
    return CommandExecutionBackend(ws, cluster_id)


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
@pytest.mark.parametrize('prepare_tables_for_migration', ['regular'], indirect=True)
def test_migration_job_ext_hms(ws, installation_ctx, prepare_tables_for_migration, env_or_skip):
    ext_hms_cluster_id = env_or_skip("TEST_EXT_HMS_CLUSTER_ID")
    tables, dst_schema = prepare_tables_for_migration
    ext_hms_ctx = installation_ctx.replace(
        config_transform=lambda wc: dataclasses.replace(
            wc,
            override_clusters={
                "main": ext_hms_cluster_id,
                "table_migration": ext_hms_cluster_id,
            },
        ),
        extend_prompts={
            r"Parallelism for migrating.*": "1000",
            r"Min workers for auto-scale.*": "2",
            r"Max workers for auto-scale.*": "20",
            r"Instance pool id to be set.*": env_or_skip("TEST_INSTANCE_POOL_ID"),
            r".*Do you want to update the existing installation?.*": 'yes',
            r".*connect to the external metastore?.*": "yes",
            r"Choose a cluster policy": "0",
        },
    )
    ext_hms_ctx.workspace_installation.run()
    ext_hms_ctx.deployed_workflows.run_workflow("migrate-tables")
    # assert the workflow is successful
    assert ext_hms_ctx.deployed_workflows.validate_step("migrate-tables")

    # assert the tables are migrated
    for table in tables.values():
        try:
            assert ws.tables.get(f"{dst_schema.catalog_name}.{dst_schema.name}.{table.name}").name
        except NotFound:
            assert False, f"{table.name} not found in {dst_schema.catalog_name}.{dst_schema.name}"
    # assert the cluster is configured correctly with ext hms
    install_state = ext_hms_ctx.installation.load(RawState)
    for job_cluster in ws.jobs.get(install_state.resources["jobs"]["migrate-tables"]).settings.job_clusters:
        if ws.config.is_azure:
            assert "spark.sql.hive.metastore.version" in job_cluster.new_cluster.spark_conf
        if ws.config.is_aws:
            assert "spark.databricks.hive.metastore.glueCatalog.enabled" in job_cluster.new_cluster.spark_conf
