import logging
from typing import Callable, Protocol

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.assessment.clusters import ClustersCrawler, PoliciesCrawler
from databricks.labs.ucx.assessment.init_scripts import GlobalInitScriptCrawler
from databricks.labs.ucx.assessment.jobs import JobsCrawler, SubmitRunsCrawler
from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.factories import GlobalContext
from databricks.labs.ucx.hive_metastore import (
    ExternalLocations,
    Mounts,
)
from databricks.labs.ucx.hive_metastore.table_size import TableSizeCrawler
from databricks.labs.ucx.hive_metastore.tables import AclMigrationWhat, What
from databricks.labs.ucx.hive_metastore.verification import VerifyHasMetastore
from databricks.labs.ucx.workspace_access.generic import WorkspaceListing

logger = logging.getLogger(__name__)


class Snapshot(Protocol):
    def __init__(self, backend: SqlBackend, ws: WorkspaceClient, schema: str): ...

    def snapshot(self): ...


class Workflow:
    def __init__(self, name: str, ws: WorkspaceClient, cfg: WorkspaceConfig, sql_backend: SqlBackend):
        self._name = name
        self.workspace_client = ws
        self.config = cfg
        self.sql_backend = sql_backend

    def simple_snapshot(self, crawler_class: type[Snapshot]):
        crawler = crawler_class(self.sql_backend, self.workspace_client, self.config.inventory_database)
        crawler.snapshot()


def task(
    *,
    depends_on=None,
    job_cluster="main",
    notebook: str | None = None,
    dashboard: str | None = None,
    cloud: str | None = None,
) -> Callable[[Callable], Callable]: ...


class Assessment(Workflow, GlobalContext):
    def __init__(self, ws: WorkspaceClient, cfg: WorkspaceConfig, sql_backend: SqlBackend):
        super().__init__('assessment', ws, cfg, sql_backend)

    @task(notebook="hive_metastore/tables.scala")
    def crawl_tables(*_):
        """Iterates over all tables in the Hive Metastore of the current workspace and persists their metadata, such
        as _database name_, _table name_, _table type_, _table location_, etc., in the Delta table named
        `$inventory_database.tables`. Note that the `inventory_database` is set in the configuration file. The metadata
        stored is then used in the subsequent tasks and workflows to, for example,  find all Hive Metastore tables that
        cannot easily be migrated to Unity Catalog."""

    @task(job_cluster="tacl")
    def setup_tacl(*_):
        """(Optimization) Starts `tacl` job cluster in parallel to crawling tables."""

    @task(depends_on=[crawl_tables, setup_tacl], job_cluster="tacl")
    def crawl_grants(self):
        """Scans the previously created Delta table named `$inventory_database.tables` and issues a `SHOW GRANTS`
        statement for every object to retrieve the permissions it has assigned to it. The permissions include information
        such as the _principal_, _action type_, and the _table_ it applies to. This is persisted in the Delta table
        `$inventory_database.grants`. Other, migration related jobs use this inventory table to convert the legacy Table
        ACLs to Unity Catalog  permissions.

        Note: This job runs on a separate cluster (named `tacl`) as it requires the proper configuration to have the Table
        ACLs enabled and available for retrieval."""
        self.grants_crawler.snapshot()

    @task(depends_on=[crawl_tables])
    def estimate_table_size_for_migration(self):
        """Scans the previously created Delta table named `$inventory_database.tables` and locate tables that cannot be
        "synced". These tables will have to be cloned in the migration process.
        Assesses the size of these tables and create `$inventory_database.table_size` table to list these sizes.
        The table size is a factor in deciding whether to clone these tables."""
        table_size = TableSizeCrawler(self.sql_backend, self.config.inventory_database)
        table_size.snapshot()

    @task
    def crawl_mounts(self):
        """Defines the scope of the _mount points_ intended for migration into Unity Catalog. As these objects are not
        compatible with the Unity Catalog paradigm, a key component of the migration process involves transferring them
        to Unity Catalog External Locations.

        The assessment involves scanning the workspace to compile a list of all existing mount points and subsequently
        storing this information in the `$inventory.mounts` table. This is crucial for planning the migration."""
        self.simple_snapshot(Mounts)

    @task(depends_on=[crawl_mounts, crawl_tables])
    def guess_external_locations(self):
        """Determines the shared path prefixes of all the tables. Specifically, the focus is on identifying locations that
        utilize mount points. The goal is to identify the _external locations_ necessary for a successful migration and
        store this information in the `$inventory.external_locations` table.

        The approach taken in this assessment involves the following steps:
          - Extracting all the locations associated with tables that do not use DBFS directly, but a mount point instead
          - Scanning all these locations to identify folders that can act as shared path prefixes
          - These identified external locations will be created subsequently prior to the actual table migration"""
        self.simple_snapshot(ExternalLocations)

    @task
    def assess_jobs(self):
        """Scans through all the jobs and identifies those that are not compatible with UC. The list of all the jobs is
        stored in the `$inventory.jobs` table.

        It looks for:
          - Clusters with Databricks Runtime (DBR) version earlier than 11.3
          - Clusters using Passthrough Authentication
          - Clusters with incompatible Spark config tags
          - Clusters referencing DBFS locations in one or more config options
        """
        self.simple_snapshot(JobsCrawler)

    @task
    def assess_clusters(self):
        """Scan through all the clusters and identifies those that are not compatible with UC. The list of all the clusters
        is stored in the`$inventory.clusters` table.

        It looks for:
          - Clusters with Databricks Runtime (DBR) version earlier than 11.3
          - Clusters using Passthrough Authentication
          - Clusters with incompatible spark config tags
          - Clusters referencing DBFS locations in one or more config options
        """
        self.simple_snapshot(ClustersCrawler)

    @task
    def assess_pipelines(self):
        """This module scans through all the Pipelines and identifies those pipelines which has Azure Service Principals
        embedded (who has been given access to the Azure storage accounts via spark configurations) in the pipeline
        configurations.

        It looks for:
          - all the pipelines which has Azure Service Principal embedded in the pipeline configuration

        Subsequently, a list of all the pipelines with matching configurations are stored in the
        `$inventory.pipelines` table."""
        self.simple_snapshot(PipelinesCrawler)

    @task
    def assess_incompatible_submit_runs(self):
        """This module scans through all the Submit Runs and identifies those runs which may become incompatible after
        the workspace attachment.

        It looks for:
          - All submit runs with DBR >=11.3 and data_security_mode:None

        It also combines several submit runs under a single pseudo_id based on hash of the submit run configuration.
        Subsequently, a list of all the incompatible runs with failures are stored in the
        `$inventory.submit_runs` table."""
        crawler = SubmitRunsCrawler(
            self.workspace_client,
            self.sql_backend,
            self.config.inventory_database,
            self.config.num_days_submit_runs_history,
        )
        crawler.snapshot()

    @task
    def crawl_cluster_policies(self):
        """This module scans through all the Cluster Policies and get the necessary information

        It looks for:
          - Clusters Policies with Databricks Runtime (DBR) version earlier than 11.3

          Subsequently, a list of all the policies with matching configurations are stored in the
        `$inventory.policies` table."""
        self.simple_snapshot(PoliciesCrawler)

    @task(cloud="azure")
    def assess_azure_service_principals(self):
        """This module scans through all the clusters configurations, cluster policies, job cluster configurations,
        Pipeline configurations, Warehouse configuration and identifies all the Azure Service Principals who has been
        given access to the Azure storage accounts via spark configurations referred in those entities.

        It looks in:
          - all those entities and prepares a list of Azure Service Principal embedded in their configurations

        Subsequently, the list of all the Azure Service Principals referred in those configurations are saved
        in the `$inventory.azure_service_principals` table."""
        if self.workspace_client.config.is_azure:
            self.simple_snapshot(AzureServicePrincipalCrawler)

    @task
    def assess_global_init_scripts(self):
        """This module scans through all the global init scripts and identifies if there is an Azure Service Principal
        who has been given access to the Azure storage accounts via spark configurations referred in those scripts.

        It looks in:
          - the list of all the global init scripts are saved in the `$inventory.azure_service_principals` table."""
        self.simple_snapshot(GlobalInitScriptCrawler)

    @task
    def workspace_listing(self):
        """Scans the workspace for workspace objects. It recursively list all sub directories
        and compiles a list of directories, notebooks, files, repos and libraries in the workspace.

        It uses multi-threading to parallelize the listing process to speed up execution on big workspaces.
        It accepts starting path as the parameter defaulted to the root path '/'."""
        crawler = WorkspaceListing(
            self.workspace_client,
            self.sql_backend,
            self.config.inventory_database,
            self.config.num_threads,
            self.config.workspace_start_path,
        )
        crawler.snapshot()

    @task(depends_on=[crawl_grants, workspace_listing])
    def crawl_permissions(self):
        """Scans the workspace-local groups and all their permissions. The list is stored in the `$inventory.permissions`
        Delta table.

        This is the first step for the _group migration_ process, which is continued in the `migrate-groups` workflow.
        This step includes preparing Legacy Table ACLs for local group migration."""
        permission_manager = self.permission_manager
        permission_manager.cleanup()
        permission_manager.inventorize_permissions()

    @task
    def crawl_groups(self):
        """Scans all groups for the local group migration scope"""
        self.group_manager.snapshot()

    @task(
        depends_on=[
            crawl_grants,
            crawl_groups,
            crawl_permissions,
            guess_external_locations,
            assess_jobs,
            assess_incompatible_submit_runs,
            assess_clusters,
            crawl_cluster_policies,
            assess_azure_service_principals,
            assess_pipelines,
            assess_global_init_scripts,
            crawl_tables,
        ],
        dashboard="assessment_main",
    )
    def assessment_report(self):
        """Refreshes the assessment dashboard after all previous tasks have been completed. Note that you can access the
        dashboard _before_ all tasks have been completed, but then only already completed information is shown."""

    @task(
        depends_on=[
            assess_jobs,
            assess_incompatible_submit_runs,
            assess_clusters,
            assess_pipelines,
            crawl_tables,
        ],
        dashboard="assessment_estimates",
    )
    def estimates_report(self):
        """Refreshes the assessment dashboard after all previous tasks have been completed. Note that you can access the
        dashboard _before_ all tasks have been completed, but then only already completed information is shown."""


class GroupMigration(Workflow, GlobalContext):
    def __init__(self, ws: WorkspaceClient, cfg: WorkspaceConfig, sql_backend: SqlBackend):
        super().__init__('migrate-groups', ws, cfg, sql_backend)

    @task(depends_on=[Assessment.crawl_groups])
    def rename_workspace_local_groups(self):
        """Renames workspace local groups by adding `ucx-renamed-` prefix."""
        verify_has_metastore = VerifyHasMetastore(self.workspace_client)
        if verify_has_metastore.verify_metastore():
            logger.info("Metastore exists in the workspace")
        self.group_manager.rename_groups()

    @task(depends_on=[rename_workspace_local_groups])
    def reflect_account_groups_on_workspace(self):
        """Adds matching account groups to this workspace. The matching account level group(s) must preexist(s) for this
        step to be successful. This process does not create the account level group(s)."""
        self.group_manager.reflect_account_groups_on_workspace()

    @task(depends_on=[reflect_account_groups_on_workspace], job_cluster="tacl")
    def apply_permissions_to_account_groups(self):
        """Fourth phase of the workspace-local group migration process. It does the following:
          - Assigns the full set of permissions of the original group to the account-level one

        It covers local workspace-local permissions for all entities: Legacy Table ACLs, Entitlements,
        AWS instance profiles, Clusters, Cluster policies, Instance Pools, Databricks SQL warehouses, Delta Live
        Tables, Jobs, MLflow experiments, MLflow registry, SQL Dashboards & Queries, SQL Alerts, Token and Password usage
        permissions, Secret Scopes, Notebooks, Directories, Repos, Files.

        See [interactive tutorial here](https://app.getreprise.com/launch/myM3VNn/)."""
        migration_state = self.group_manager.get_migration_state()
        if len(migration_state.groups) == 0:
            logger.info("Skipping group migration as no groups were found.")
            return
        self.permission_manager.apply_group_permissions(migration_state)

    @task(job_cluster="tacl")
    def validate_groups_permissions(self):
        """Validate that all the crawled permissions are applied correctly to the destination groups."""
        self.permission_manager.verify_group_permissions()


class TableMigration(Workflow, GlobalContext):
    def __init__(self, ws: WorkspaceClient, cfg: WorkspaceConfig, sql_backend: SqlBackend, install: Installation):
        super().__init__('migrate-tables', ws, cfg, sql_backend)
        self._install = install

    @task(job_cluster="table_migration")
    def migrate_external_tables_sync(self):
        """This workflow task migrates the *external tables that are supported by SYNC command* from the Hive Metastore to the Unity Catalog.
        Following cli commands are required to be run before running this task:
        - For Azure: `principal-prefix-access`, `create-table-mapping`, `create-uber-principal`, `migrate-credentials`, `migrate-locations`, `create-catalogs-schemas`
        - For AWS: TBD
        """
        self.tables_migrator.migrate_tables(what=What.EXTERNAL_SYNC, acl_strategy=[AclMigrationWhat.LEGACY_TACL])

    @task(job_cluster="table_migration")
    def migrate_dbfs_root_delta_tables(self):
        """This workflow task migrates `delta tables stored in DBFS root` from the Hive Metastore to the Unity Catalog using deep clone.
        Following cli commands are required to be run before running this task:
        - For Azure: `principal-prefix-access`, `create-table-mapping`, `create-uber-principal`, `migrate-credentials`, `migrate-locations`, `create-catalogs-schemas`
        - For AWS: TBD
        """
        self.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA, acl_strategy=[AclMigrationWhat.LEGACY_TACL])
