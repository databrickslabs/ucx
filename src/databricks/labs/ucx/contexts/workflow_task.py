from functools import cached_property
from pathlib import Path

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import RuntimeBackend, SqlBackend
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationOwnership, TableMigrationStatus
from databricks.sdk import WorkspaceClient, core

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.assessment.clusters import (
    ClustersCrawler,
    PoliciesCrawler,
    ClusterOwnership,
    ClusterInfo,
    ClusterPolicyOwnership,
    PolicyInfo,
)
from databricks.labs.ucx.assessment.init_scripts import GlobalInitScriptCrawler
from databricks.labs.ucx.assessment.jobs import JobOwnership, JobInfo, JobsCrawler, SubmitRunsCrawler
from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler, PipelineInfo, PipelineOwnership
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.application import GlobalContext
from databricks.labs.ucx.hive_metastore import TablesInMounts, TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant, GrantOwnership
from databricks.labs.ucx.hive_metastore.table_size import TableSizeCrawler
from databricks.labs.ucx.hive_metastore.tables import FasterTableScanCrawler, Table, TableOwnership
from databricks.labs.ucx.hive_metastore.udfs import Udf, UdfOwnership
from databricks.labs.ucx.installer.logs import TaskRunWarningRecorder
from databricks.labs.ucx.progress.history import HistoryLog
from databricks.labs.ucx.progress.workflow_runs import WorkflowRunRecorder

# As with GlobalContext, service factories unavoidably have a lot of public methods.
# pylint: disable=too-many-public-methods


class RuntimeContext(GlobalContext):
    @cached_property
    def _config_path(self) -> Path:
        config = self.named_parameters.get("config")
        if not config:
            raise ValueError("config flag is required")
        return Path(config)

    @cached_property
    def config(self) -> WorkspaceConfig:
        return Installation.load_local(WorkspaceConfig, self._config_path)

    @cached_property
    def connect_config(self) -> core.Config:
        connect = self.config.connect
        assert connect, "connect is required"
        return connect

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        return WorkspaceClient(config=self.connect_config, product='ucx', product_version=__version__)

    @cached_property
    def sql_backend(self) -> SqlBackend:
        return RuntimeBackend(debug_truncate_bytes=self.connect_config.debug_truncate_bytes)

    @cached_property
    def installation(self) -> Installation:
        install_folder = self._config_path.parent.as_posix().removeprefix("/Workspace")
        return Installation(self.workspace_client, "ucx", install_folder=install_folder)

    @cached_property
    def jobs_crawler(self) -> JobsCrawler:
        return JobsCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def submit_runs_crawler(self) -> SubmitRunsCrawler:
        return SubmitRunsCrawler(
            self.workspace_client,
            self.sql_backend,
            self.inventory_database,
            self.config.num_days_submit_runs_history,
        )

    @cached_property
    def clusters_crawler(self) -> ClustersCrawler:
        return ClustersCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def pipelines_crawler(self) -> PipelinesCrawler:
        return PipelinesCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def table_size_crawler(self) -> TableSizeCrawler:
        return TableSizeCrawler(self.tables_crawler)

    @cached_property
    def policies_crawler(self) -> PoliciesCrawler:
        return PoliciesCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def global_init_scripts_crawler(self) -> GlobalInitScriptCrawler:
        return GlobalInitScriptCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def tables_crawler(self) -> TablesCrawler:
        return FasterTableScanCrawler(self.sql_backend, self.inventory_database, self.config.include_databases)

    @cached_property
    def tables_ownership(self) -> TableOwnership:
        return TableOwnership(self.administrator_locator)

    @cached_property
    def tables_in_mounts(self) -> TablesInMounts:
        return TablesInMounts(
            self.sql_backend,
            self.workspace_client,
            self.inventory_database,
            self.mounts_crawler,
            self.config.include_mounts,
            self.config.exclude_paths_in_mount,
            self.config.include_paths_in_mount,
        )

    @cached_property
    def task_run_warning_recorder(self) -> TaskRunWarningRecorder:
        return TaskRunWarningRecorder(
            self._config_path.parent,
            self.named_parameters["workflow"],
            int(self.named_parameters["job_id"]),
            int(self.named_parameters["parent_run_id"]),
            self.sql_backend,
            self.inventory_database,
            int(self.named_parameters.get("attempt", "0")),
        )

    @cached_property
    def workflow_run_recorder(self) -> WorkflowRunRecorder:
        return WorkflowRunRecorder(
            self.sql_backend,
            self.config.ucx_catalog,
            workspace_id=self.workspace_id,
            workflow_name=self.named_parameters["workflow"],
            workflow_id=int(self.named_parameters["job_id"]),
            workflow_run_id=int(self.named_parameters["parent_run_id"]),
            workflow_run_attempt=int(self.named_parameters.get("attempt", 0)),
            workflow_start_time=self.named_parameters["start_time"],
        )

    @cached_property
    def workspace_id(self) -> int:
        return self.workspace_client.get_workspace_id()

    @cached_property
    def historical_clusters_log(self) -> HistoryLog[ClusterInfo]:
        cluster_owner = ClusterOwnership(self.administrator_locator)
        return HistoryLog(
            self.sql_backend,
            cluster_owner,
            ClusterInfo,
            int(self.named_parameters["parent_run_id"]),
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def historical_cluster_policies_log(self) -> HistoryLog[PolicyInfo]:
        cluster_policy_owner = ClusterPolicyOwnership(self.administrator_locator)
        return HistoryLog(
            self.sql_backend,
            cluster_policy_owner,
            PolicyInfo,
            int(self.named_parameters["parent_run_id"]),
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def historical_grants_log(self) -> HistoryLog[Grant]:
        grant_owner = GrantOwnership(self.administrator_locator)
        return HistoryLog(
            self.sql_backend,
            grant_owner,
            Grant,
            int(self.named_parameters["parent_run_id"]),
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def historical_jobs_log(self) -> HistoryLog[JobInfo]:
        job_owner = JobOwnership(self.administrator_locator)
        return HistoryLog(
            self.sql_backend,
            job_owner,
            JobInfo,
            int(self.named_parameters["parent_run_id"]),
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def historical_pipelines_log(self) -> HistoryLog[PipelineInfo]:
        pipeline_owner = PipelineOwnership(self.administrator_locator)
        return HistoryLog(
            self.sql_backend,
            pipeline_owner,
            PipelineInfo,
            int(self.named_parameters["parent_run_id"]),
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def historical_tables_log(self) -> HistoryLog[Table]:
        return HistoryLog(
            self.sql_backend,
            self.tables_ownership,
            Table,
            int(self.named_parameters["parent_run_id"]),
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def historical_table_migration_log(self) -> HistoryLog[TableMigrationStatus]:
        table_migration_owner = TableMigrationOwnership(self.tables_crawler, self.tables_ownership)
        return HistoryLog(
            self.sql_backend,
            table_migration_owner,
            TableMigrationStatus,
            int(self.named_parameters["parent_run_id"]),
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def historical_udfs_log(self) -> HistoryLog[Udf]:
        udf_owner = UdfOwnership(self.administrator_locator)
        return HistoryLog(
            self.sql_backend,
            udf_owner,
            Udf,
            int(self.named_parameters["parent_run_id"]),
            self.workspace_id,
            self.config.ucx_catalog,
        )
