from functools import cached_property
from pathlib import Path

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import RuntimeBackend, SqlBackend
from databricks.sdk import WorkspaceClient, core

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.assessment.clusters import ClustersCrawler, PoliciesCrawler
from databricks.labs.ucx.assessment.init_scripts import GlobalInitScriptCrawler
from databricks.labs.ucx.assessment.jobs import JobsCrawler, SubmitRunsCrawler
from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.application import GlobalContext
from databricks.labs.ucx.hive_metastore import TablesInMounts
from databricks.labs.ucx.hive_metastore.table_size import TableSizeCrawler
from databricks.labs.ucx.installer.logs import TaskRunWarningRecorder


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
    def installation(self):
        install_folder = self._config_path.parent.as_posix().removeprefix("/Workspace")
        return Installation(self.workspace_client, "ucx", install_folder=install_folder)

    @cached_property
    def jobs_crawler(self):
        return JobsCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def submit_runs_crawler(self):
        return SubmitRunsCrawler(
            self.workspace_client,
            self.sql_backend,
            self.inventory_database,
            self.config.num_days_submit_runs_history,
        )

    @cached_property
    def clusters_crawler(self):
        return ClustersCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def pipelines_crawler(self):
        return PipelinesCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def table_size_crawler(self):
        return TableSizeCrawler(self.sql_backend, self.inventory_database)

    @cached_property
    def policies_crawler(self):
        return PoliciesCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def global_init_scripts_crawler(self):
        return GlobalInitScriptCrawler(self.workspace_client, self.sql_backend, self.inventory_database)

    @cached_property
    def tables_in_mounts(self):
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
    def task_run_warning_recorder(self):
        return TaskRunWarningRecorder(
            self._config_path.parent,
            self.named_parameters["workflow"],
            int(self.named_parameters["job_id"]),
            int(self.named_parameters["parent_run_id"]),
            self.sql_backend,
            self.inventory_database,
            int(self.named_parameters.get("attempt", "0")),
        )
