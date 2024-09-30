import logging

from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, job_task


logger = logging.getLogger(__name__)


class Assessment(Workflow):
    def __init__(self):
        super().__init__('assessment')

    @job_task
    def crawl_tables(self, ctx: RuntimeContext):
        """Iterates over all tables in the Hive Metastore of the current workspace and persists their metadata, such
        as _database name_, _table name_, _table type_, _table location_, etc., in the Delta table named
        `$inventory_database.tables`. Note that the `inventory_database` is set in the configuration file. The metadata
        stored is then used in the subsequent tasks and workflows to, for example,  find all Hive Metastore tables that
        cannot easily be migrated to Unity Catalog."""
        ctx.tables_crawler.snapshot()

    @job_task
    def crawl_udfs(self, ctx: RuntimeContext):
        """Iterates over all UDFs in the Hive Metastore of the current workspace and persists their metadata in the
        table named `$inventory_database.udfs`. This inventory is currently used when scanning securable objects for
        issues with grants that cannot be migrated to Unit Catalog."""
        ctx.udfs_crawler.snapshot()

    @job_task(job_cluster="tacl")
    def setup_tacl(self, ctx: RuntimeContext):
        """(Optimization) Starts `tacl` job cluster in parallel to crawling tables."""

    @job_task(depends_on=[crawl_tables, crawl_udfs, setup_tacl], job_cluster="tacl")
    def crawl_grants(self, ctx: RuntimeContext):
        """Scans all securable objects for permissions that have been assigned: this include database-level permissions,
        as well permissions directly configured on objects in the (already gathered) table and UDF inventories. The
        captured information is stored in the `$inventory_database.grants` inventory table for further use during the
        migration of legacy ACLs to Unity Catalog permissions.

        Note: This job runs on a separate cluster (named `tacl`) as it requires the proper configuration to have the Table
        ACLs enabled and available for retrieval."""
        ctx.grants_crawler.snapshot()

    @job_task(depends_on=[crawl_tables])
    def estimate_table_size_for_migration(self, ctx: RuntimeContext):
        """Scans the previously created Delta table named `$inventory_database.tables` and locate tables that cannot be
        "synced". These tables will have to be cloned in the migration process.
        Assesses the size of these tables and create `$inventory_database.table_size` table to list these sizes.
        The table size is a factor in deciding whether to clone these tables."""
        ctx.table_size_crawler.snapshot()

    @job_task
    def crawl_mounts(self, ctx: RuntimeContext):
        """Defines the scope of the _mount points_ intended for migration into Unity Catalog. As these objects are not
        compatible with the Unity Catalog paradigm, a key component of the migration process involves transferring them
        to Unity Catalog External Locations.

        The assessment involves scanning the workspace to compile a list of all existing mount points and subsequently
        storing this information in the `$inventory.mounts` table. This is crucial for planning the migration."""
        ctx.mounts_crawler.snapshot()

    @job_task(depends_on=[crawl_mounts, crawl_tables])
    def guess_external_locations(self, ctx: RuntimeContext):
        """Determines the shared path prefixes of all the tables. Specifically, the focus is on identifying locations that
        utilize mount points. The goal is to identify the _external locations_ necessary for a successful migration and
        store this information in the `$inventory.external_locations` table.

        The approach taken in this assessment involves the following steps:
          - Extracting all the locations associated with tables that do not use DBFS directly, but a mount point instead
          - Scanning all these locations to identify folders that can act as shared path prefixes
          - These identified external locations will be created subsequently prior to the actual table migration"""
        ctx.external_locations.snapshot()

    @job_task
    def assess_jobs(self, ctx: RuntimeContext):
        """Scans through all the jobs and identifies those that are not compatible with UC. The list of all the jobs is
        stored in the `$inventory.jobs` table.

        It looks for:
          - Clusters with Databricks Runtime (DBR) version earlier than 11.3
          - Clusters using Passthrough Authentication
          - Clusters with incompatible Spark config tags
          - Clusters referencing DBFS locations in one or more config options
        """
        ctx.jobs_crawler.snapshot()

    @job_task
    def assess_clusters(self, ctx: RuntimeContext):
        """Scan through all the clusters and identifies those that are not compatible with UC. The list of all the clusters
        is stored in the`$inventory.clusters` table.

        It looks for:
          - Clusters with Databricks Runtime (DBR) version earlier than 11.3
          - Clusters using Passthrough Authentication
          - Clusters with incompatible spark config tags
          - Clusters referencing DBFS locations in one or more config options
        """
        ctx.clusters_crawler.snapshot()

    @job_task
    def assess_pipelines(self, ctx: RuntimeContext):
        """This module scans through all the Pipelines and identifies those pipelines which has Azure Service Principals
        embedded (who has been given access to the Azure storage accounts via spark configurations) in the pipeline
        configurations.

        It looks for:
          - all the pipelines which has Azure Service Principal embedded in the pipeline configuration

        Subsequently, a list of all the pipelines with matching configurations are stored in the
        `$inventory.pipelines` table."""
        ctx.pipelines_crawler.snapshot()

    @job_task
    def assess_incompatible_submit_runs(self, ctx: RuntimeContext):
        """This module scans through all the Submit Runs and identifies those runs which may become incompatible after
        the workspace attachment.

        It looks for:
          - All submit runs with DBR >=11.3 and data_security_mode:None

        It also combines several submit runs under a single pseudo_id based on hash of the submit run configuration.
        Subsequently, a list of all the incompatible runs with failures are stored in the
        `$inventory.submit_runs` table."""
        ctx.submit_runs_crawler.snapshot()

    @job_task
    def crawl_cluster_policies(self, ctx: RuntimeContext):
        """This module scans through all the Cluster Policies and get the necessary information

        It looks for:
          - Clusters Policies with Databricks Runtime (DBR) version earlier than 11.3

          Subsequently, a list of all the policies with matching configurations are stored in the
        `$inventory.policies` table."""
        ctx.policies_crawler.snapshot()

    @job_task(cloud="azure")
    def assess_azure_service_principals(self, ctx: RuntimeContext):
        """This module scans through all the clusters configurations, cluster policies, job cluster configurations,
        Pipeline configurations, Warehouse configuration and identifies all the Azure Service Principals who has been
        given access to the Azure storage accounts via spark configurations referred in those entities.

        It looks in:
          - all those entities and prepares a list of Azure Service Principal embedded in their configurations

        Subsequently, the list of all the Azure Service Principals referred in those configurations are saved
        in the `$inventory.azure_service_principals` table."""
        if ctx.is_azure:
            ctx.azure_service_principal_crawler.snapshot()

    @job_task
    def assess_global_init_scripts(self, ctx: RuntimeContext):
        """This module scans through all the global init scripts and identifies if there is an Azure Service Principal
        who has been given access to the Azure storage accounts via spark configurations referred in those scripts.

        It looks in:
          - the list of all the global init scripts are saved in the `$inventory.global_init_scripts` table."""
        ctx.global_init_scripts_crawler.snapshot()

    @job_task
    def workspace_listing(self, ctx: RuntimeContext):
        """Scans the workspace for workspace objects. It recursively list all sub directories
        and compiles a list of directories, notebooks, files, repos and libraries in the workspace.

        It uses multi-threading to parallelize the listing process to speed up execution on big workspaces.
        It accepts starting path as the parameter defaulted to the root path '/'."""
        ctx.workspace_listing.snapshot()

    @job_task(depends_on=[crawl_grants, workspace_listing])
    def crawl_permissions(self, ctx: RuntimeContext):
        """Scans the workspace-local groups and all their permissions. The list is stored in the `$inventory.permissions`
        Delta table.

        This is the first step for the _group migration_ process, which is continued in the `migrate-groups` workflow.
        This step includes preparing Legacy Table ACLs for local group migration."""
        permission_manager = ctx.permission_manager
        permission_manager.reset()
        permission_manager.snapshot()

    @job_task
    def crawl_groups(self, ctx: RuntimeContext):
        """Scans all groups for the local group migration scope"""
        ctx.group_manager.snapshot()

    @job_task
    def assess_dashboards(self, ctx: RuntimeContext):
        """Scans all dashboards for migration issues in SQL code of embedded widgets.
        Also stores direct filesystem accesses for display in the migration dashboard."""
        ctx.query_linter.refresh_report(ctx.sql_backend, ctx.inventory_database)

    @job_task
    def assess_workflows(self, ctx: RuntimeContext):
        """Scans all jobs for migration issues in notebooks.
        Also stores direct filesystem accesses for display in the migration dashboard."""
        ctx.workflow_linter.refresh_report(ctx.sql_backend, ctx.inventory_database)


class Failing(Workflow):
    def __init__(self):
        super().__init__('failing')

    @job_task
    def failing_task(self, ctx: RuntimeContext):
        """This task always fails. It is used to test the failure handling of the framework."""
        attempt = ctx.named_parameters.get("attempt", "0")
        if int(attempt) > 0:
            logger.info("This task is no longer failing.")
            return
        logger.warning("This is a test warning message.")
        logger.error("This is a test error message.")
        raise ValueError("This task is supposed to fail.")
