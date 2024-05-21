import dataclasses
import logging
import os
import sys
from collections.abc import Iterator, Sequence
from datetime import timedelta
from types import UnionType
from typing import Any, ClassVar, Protocol, TypeVar

import pytest
from databricks.labs.blueprint.commands import CommandExecutor
from databricks.labs.blueprint.installer import RawState
from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.core import Row
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    InvalidParameterValue,
    NotFound,
)
from databricks.sdk.retries import retried
from databricks.sdk.service.compute import Language
from databricks.sdk.service.iam import PermissionLevel

logger = logging.getLogger(__name__)


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict]


Result = TypeVar("Result", bound=DataclassInstance)
Dataclass = type[DataclassInstance]


class CommandContextBackend(SqlBackend):
    def __init__(self, ws: WorkspaceClient, cluster_id, *, max_records_per_batch: int = 1000):
        self._sql = CommandExecutor(ws.clusters, ws.command_execution, lambda: cluster_id, language=Language.SQL)
        self._max_records_per_batch = max_records_per_batch
        debug_truncate_bytes = ws.config.debug_truncate_bytes
        self._debug_truncate_bytes = debug_truncate_bytes if isinstance(debug_truncate_bytes, int) else 96

    def execute(self, sql: str, *, catalog: str | None = None, schema: str | None = None) -> None:
        _ = catalog, schema
        logger.debug(f"[api][execute] {self._only_n_bytes(sql, self._debug_truncate_bytes)}")
        self._sql.run(sql)

    def fetch(self, sql: str, *, catalog: str | None = None, schema: str | None = None) -> Iterator[Row]:
        _ = catalog, schema
        logger.debug(f"[api][fetch] {self._only_n_bytes(sql, self._debug_truncate_bytes)}")
        return self._sql.run(sql, result_as_json=True)

    def save_table(self, full_name: str, rows: Sequence[DataclassInstance], klass: Dataclass, mode="append"):
        rows = self._filter_none_rows(rows, klass)
        self.create_table(full_name, klass)
        if len(rows) == 0:
            return
        fields = dataclasses.fields(klass)
        field_names = [f.name for f in fields]
        if mode == "overwrite":
            self.execute(f"TRUNCATE TABLE {full_name}")
        for i in range(0, len(rows), self._max_records_per_batch):
            batch = rows[i : i + self._max_records_per_batch]
            vals = "), (".join(self._row_to_sql(r, fields) for r in batch)
            sql = f'INSERT INTO {full_name} ({", ".join(field_names)}) VALUES ({vals})'
            self.execute(sql)

    @staticmethod
    def _row_to_sql(row: DataclassInstance, fields: tuple[dataclasses.Field[Any], ...]):
        data = []
        for f in fields:
            value = getattr(row, f.name)
            field_type = f.type
            if isinstance(field_type, UnionType):
                field_type = field_type.__args__[0]
            if value is None:
                data.append("NULL")
            elif field_type == bool:
                data.append("TRUE" if value else "FALSE")
            elif field_type == str:
                value = str(value).replace("'", "''")
                data.append(f"'{value}'")
            elif field_type == int:
                data.append(f"{value}")
            else:
                msg = f"unknown type: {field_type}"
                raise ValueError(msg)
        return ", ".join(data)


@pytest.fixture
def sql_backend(ws, env_or_skip) -> SqlBackend:
    cluster_id = env_or_skip("TEST_EXT_HMS_CLUSTER_ID")
    return CommandContextBackend(ws, cluster_id)


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
@pytest.mark.parametrize('prepare_tables_for_migration', ['regular'], indirect=True)
def test_migration_job_ext_hms(ws, installation_ctx, prepare_tables_for_migration, env_or_skip):
    # this test spins up clusters using ext hms cluster policy, which will have a startup time of ~ 7-10m
    # skip this test if not in nightly test job or debug mode
    if os.path.basename(sys.argv[0]) not in {"_jb_pytest_runner.py", "testlauncher.py"}:
        env_or_skip("TEST_NIGHTLY")

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


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_real_assessment_job_ext_hms(
    ws, installation_ctx, env_or_skip, make_cluster_policy, make_cluster_policy_permissions
):
    # this test spins up clusters using ext hms cluster policy, which will have a startup time of ~ 7-10m
    # skip this test if not in nightly test job or debug mode
    if os.path.basename(sys.argv[0]) not in {"_jb_pytest_runner.py", "testlauncher.py"}:
        env_or_skip("TEST_NIGHTLY")

    ext_hms_ctx = installation_ctx.replace(
        skip_dashboards=True,
        config_transform=lambda wc: dataclasses.replace(
            wc,
            override_clusters=None,
        ),
        extend_prompts={
            r"Instance pool id to be set.*": env_or_skip("TEST_INSTANCE_POOL_ID"),
            r".*Do you want to update the existing installation?.*": 'yes',
            r".*connect to the external metastore?.*": "yes",
            r"Choose a cluster policy": "0",
        },
    )
    ws_group_a, _ = ext_hms_ctx.make_ucx_group(wait_for_provisioning=True)

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )
    ext_hms_ctx.__dict__['include_object_permissions'] = [f"cluster-policies:{cluster_policy.policy_id}"]
    ext_hms_ctx.workspace_installation.run()

    ext_hms_ctx.deployed_workflows.run_workflow("assessment")

    # assert the workflow is successful. the tasks on sql warehouse will fail so skip checking them
    assert ext_hms_ctx.deployed_workflows.validate_step("assessment")

    after = ext_hms_ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert ws_group_a.display_name in after, f"Group {ws_group_a.display_name} not found in cluster policy"
    assert after[ws_group_a.display_name] == PermissionLevel.CAN_USE
