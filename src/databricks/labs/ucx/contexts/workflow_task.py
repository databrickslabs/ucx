from functools import cached_property
from pathlib import Path

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import RuntimeBackend, SqlBackend
from databricks.sdk import WorkspaceClient, core

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.assessment.dashboards import Dashboard
from databricks.labs.ucx.assessment.clusters import (
    ClustersCrawler,
    PoliciesCrawler,
    ClusterOwnership,
    ClusterInfo,
    ClusterPolicyOwnership,
    PolicyInfo,
)
from databricks.labs.ucx.assessment.init_scripts import GlobalInitScriptCrawler
from databricks.labs.ucx.assessment.jobs import JobOwnership, JobInfo, SubmitRunsCrawler
from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler, PipelineInfo, PipelineOwnership
from databricks.labs.ucx.assessment.sequencing import MigrationSequencer
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.application import GlobalContext
from databricks.labs.ucx.hive_metastore import TablesInMounts, TablesCrawler
from databricks.labs.ucx.hive_metastore.table_size import TableSizeCrawler
from databricks.labs.ucx.hive_metastore.tables import FasterTableScanCrawler, Table
from databricks.labs.ucx.hive_metastore.udfs import Udf
from databricks.labs.ucx.installer.logs import TaskRunWarningRecorder
from databricks.labs.ucx.progress.dashboards import DashboardProgressEncoder
from databricks.labs.ucx.progress.grants import Grant, GrantProgressEncoder
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.progress.jobs import JobsProgressEncoder
from databricks.labs.ucx.progress.tables import TableProgressEncoder
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
    def job_ownership(self) -> JobOwnership:
        return JobOwnership(self.administrator_locator)

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
    def cluster_ownership(self) -> ClusterOwnership:
        return ClusterOwnership(self.administrator_locator)

    @cached_property
    def pipelines_crawler(self) -> PipelinesCrawler:
        return PipelinesCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def pipeline_ownership(self) -> PipelineOwnership:
        return PipelineOwnership(self.administrator_locator)

    @cached_property
    def table_size_crawler(self) -> TableSizeCrawler:
        return TableSizeCrawler(self.tables_crawler)

    @cached_property
    def policies_crawler(self) -> PoliciesCrawler:
        return PoliciesCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def cluster_policy_ownership(self) -> ClusterPolicyOwnership:
        return ClusterPolicyOwnership(self.administrator_locator)

    @cached_property
    def global_init_scripts_crawler(self) -> GlobalInitScriptCrawler:
        return GlobalInitScriptCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def tables_crawler(self) -> TablesCrawler:
        # Warning: Not all runtime contexts support the fast-scan implementation; it requires the JVM bridge to Spark
        # and that's not always available.
        return FasterTableScanCrawler(self.sql_backend, self.inventory_database, self.config.include_databases)

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
            self.parent_run_id,
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
            workflow_run_id=self.parent_run_id,
            workflow_run_attempt=int(self.named_parameters.get("attempt", 0)),
            workflow_start_time=self.named_parameters["start_time"],
        )

    @cached_property
    def workspace_id(self) -> int:
        return self.workspace_client.get_workspace_id()

    @cached_property
    def parent_run_id(self) -> int:
        return int(self.named_parameters["parent_run_id"])

    @cached_property
    def clusters_progress(self) -> ProgressEncoder[ClusterInfo]:
        return ProgressEncoder(
            self.sql_backend,
            self.cluster_ownership,
            ClusterInfo,
            self.parent_run_id,
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def policies_progress(self) -> ProgressEncoder[PolicyInfo]:
        return ProgressEncoder(
            self.sql_backend,
            self.cluster_policy_ownership,
            PolicyInfo,
            self.parent_run_id,
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def grants_progress(self) -> ProgressEncoder[Grant]:
        return GrantProgressEncoder(
            self.sql_backend,
            self.grant_ownership,
            self.parent_run_id,
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def jobs_progress(self) -> ProgressEncoder[JobInfo]:
        return JobsProgressEncoder(
            self.sql_backend,
            self.job_ownership,
            [self.directfs_access_crawler_for_paths, self.directfs_access_crawler_for_queries],
            self.inventory_database,
            self.parent_run_id,
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def pipelines_progress(self) -> ProgressEncoder[PipelineInfo]:
        return ProgressEncoder(
            self.sql_backend,
            self.pipeline_ownership,
            PipelineInfo,
            self.parent_run_id,
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def tables_progress(self) -> ProgressEncoder[Table]:
        return TableProgressEncoder(
            self.sql_backend,
            self.table_ownership,
            self.migration_status_refresher,
            self.parent_run_id,
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def udfs_progress(self) -> ProgressEncoder[Udf]:
        return ProgressEncoder(
            self.sql_backend,
            self.udf_ownership,
            Udf,
            self.parent_run_id,
            self.workspace_id,
            self.config.ucx_catalog,
        )

    @cached_property
    def dashboards_progress(self) -> ProgressEncoder[Dashboard]:
        return DashboardProgressEncoder(
            self.sql_backend,
            self.dashboard_ownership,
            used_tables_crawlers=[self.used_tables_crawler_for_queries],
            inventory_database=self.config.inventory_database,
            job_run_id=self.parent_run_id,
            workspace_id=self.workspace_id,
            catalog=self.config.ucx_catalog,
        )

    @cached_property
    def migration_sequencer(self) -> MigrationSequencer:
        return MigrationSequencer(self.workspace_client, self.administrator_locator)
