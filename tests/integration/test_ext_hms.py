import logging
from collections.abc import Iterator, Sequence
from dataclasses import replace
from datetime import timedelta
from typing import ClassVar, Protocol, TypeVar

import pytest
from databricks.labs.blueprint.commands import CommandExecutor
from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.core import Row
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    InvalidParameterValue,
    NotFound,
)
from databricks.sdk.retries import retried
from databricks.sdk.service.compute import Language

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

    def execute(self, sql: str) -> None:
        logger.debug(f"[api][execute] {self._only_n_bytes(sql, self._debug_truncate_bytes)}")
        self._sql.run(sql)

    def fetch(self, sql: str) -> Iterator[Row]:
        logger.debug(f"[api][fetch] {self._only_n_bytes(sql, self._debug_truncate_bytes)}")
        return self._sql.run(sql, result_as_json=True)

    def save_table(self, full_name: str, rows: Sequence[DataclassInstance], klass: Dataclass, mode="append"):
        return NotImplementedError()


@pytest.fixture
def sql_backend(ws, env_or_skip) -> SqlBackend:
    cluster_id = env_or_skip("TEST_EXT_HMS_CLUSTER_ID")
    return CommandContextBackend(ws, cluster_id)


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_assessment_and_migration_job_ext_hms(ws, new_installation, env_or_skip):
    _, deployed_workflow = new_installation(
        lambda wc: replace(wc, override_clusters=None),
        skip_dashboards=False,
        extend_prompts={
            r"Parallelism for migrating.*": "1000",
            r"Min workers for auto-scale.*": "2",
            r"Max workers for auto-scale.*": "20",
            r"Instance pool id to be set.*": env_or_skip("TEST_INSTANCE_POOL_ID"),
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    deployed_workflow.run_workflow("assessment")
    # assert the workflow is successful
    assert deployed_workflow.validate_step("assessment")

    deployed_workflow.run_workflow("migrate-tables")
    # assert the workflow is successful
    assert deployed_workflow.validate_step("migrate-tables")
