from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, job_task


class MigrationProgress(Workflow):
    """Experimental workflow that rescans the environment to reflect and track progress that has been made.

    This is a subset of the assessment workflow and covers:

     - Clusters
     - Grants
     - Jobs
     - Pipelines
     - Policies
     - Tables
     - TableMigrationStatus
     - UDFs
    """

    def __init__(self) -> None:
        super().__init__('migration-progress-experimental')

    @job_task
    def crawl_tables(self, ctx: RuntimeContext) -> None:
        """Iterates over all tables in the Hive Metastore of the current workspace and persists their metadata, such
        as _database name_, _table name_, _table type_, _table location_, etc., in the table named
        `$inventory_database.tables`. The metadata stored is then used in the subsequent tasks and workflows to, for
        example, find all Hive Metastore tables that cannot easily be migrated to Unity Catalog."""
        ctx.tables_crawler.snapshot(force_refresh=True)

    @job_task
    def crawl_udfs(self, ctx: RuntimeContext) -> None:
        """Iterates over all UDFs in the Hive Metastore of the current workspace and persists their metadata in the
        table named `$inventory_database.udfs`. This inventory is currently used when scanning securable objects for
        issues with grants that cannot be migrated to Unit Catalog."""
        ctx.udfs_crawler.snapshot(force_refresh=True)

    @job_task(job_cluster="tacl")
    def setup_tacl(self, ctx: RuntimeContext) -> None:
        """(Optimization) Starts `tacl` job cluster in parallel to crawling tables."""

    @job_task(depends_on=[crawl_tables, crawl_udfs, setup_tacl], job_cluster="tacl")
    def crawl_grants(self, ctx: RuntimeContext) -> None:
        """Scans all securable objects for permissions that have been assigned: this include database-level permissions,
        as well permissions directly configured on objects in the (already gathered) table and UDF inventories. The
        captured information is stored in the `$inventory_database.grants` inventory table for further use during the
        migration of legacy ACLs to Unity Catalog permissions.

        Note: This job runs on a separate cluster (named `tacl`) as it requires the proper configuration to have the Table
        ACLs enabled and available for retrieval."""
        ctx.grants_crawler.snapshot(force_refresh=True)

    @job_task
    def assess_jobs(self, ctx: RuntimeContext) -> None:
        """Scans through all the jobs and identifies those that are not compatible with UC. The list of all the jobs is
        stored in the `$inventory.jobs` table.

        It looks for:
          - Clusters with Databricks Runtime (DBR) version earlier than 11.3
          - Clusters using Passthrough Authentication
          - Clusters with incompatible Spark config tags
          - Clusters referencing DBFS locations in one or more config options
        """
        ctx.jobs_crawler.snapshot(force_refresh=True)

    @job_task
    def assess_clusters(self, ctx: RuntimeContext) -> None:
        """Scan through all the clusters and identifies those that are not compatible with UC. The list of all the clusters
        is stored in the`$inventory.clusters` table.

        It looks for:
          - Clusters with Databricks Runtime (DBR) version earlier than 11.3
          - Clusters using Passthrough Authentication
          - Clusters with incompatible spark config tags
          - Clusters referencing DBFS locations in one or more config options
        """
        ctx.clusters_crawler.snapshot(force_refresh=True)

    @job_task
    def assess_pipelines(self, ctx: RuntimeContext) -> None:
        """This module scans through all the Pipelines and identifies those pipelines which has Azure Service Principals
        embedded (who has been given access to the Azure storage accounts via spark configurations) in the pipeline
        configurations.

        It looks for:
          - all the pipelines which has Azure Service Principal embedded in the pipeline configuration

        Subsequently, a list of all the pipelines with matching configurations are stored in the
        `$inventory.pipelines` table."""
        ctx.pipelines_crawler.snapshot(force_refresh=True)

    @job_task
    def crawl_cluster_policies(self, ctx: RuntimeContext) -> None:
        """This module scans through all the Cluster Policies and get the necessary information

        It looks for:
          - Clusters Policies with Databricks Runtime (DBR) version earlier than 11.3

          Subsequently, a list of all the policies with matching configurations are stored in the
        `$inventory.policies` table."""
        ctx.policies_crawler.snapshot(force_refresh=True)

    @job_task(job_cluster="table_migration")
    def setup_table_migration(self, ctx: RuntimeContext) -> None:
        """(Optimization) Starts `table_migration` job cluster in parallel to crawling tables."""

    @job_task(depends_on=[crawl_tables, setup_table_migration], job_cluster="table_migration")
    def refresh_table_migration_status(self, ctx: RuntimeContext) -> None:
        """Scan the tables (and views) in the inventory and record whether each has been migrated or not.

        The results of the scan are stored in the `$inventory.migration_status` inventory table.
        """
        ctx.migration_status_refresher.snapshot(force_refresh=True)

    @job_task(
        depends_on=[
            crawl_grants,
            assess_jobs,
            assess_clusters,
            assess_pipelines,
            crawl_cluster_policies,
            refresh_table_migration_status,
        ]
    )
    def record_workflow_run(self, ctx: RuntimeContext) -> None:
        """Record the workflow run of this workflow."""
        ctx.workflow_run_recorder.record()
