import datetime as dt

from databricks.sdk.service.jobs import CronSchedule, PauseStatus

from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, job_task


class MigrationProgress(Workflow):
    """Experimental workflow that rescans the environment to reflect and track progress that has been made.

    It overlaps substantially with the assessment workflow, covering:

     - Clusters
     - Dashboards
     - Grants
     - Jobs (inventory & linting)
     - Pipelines
     - Policies
     - Tables
     - TableMigrationStatus
     - UDFs
    """

    def __init__(self) -> None:
        super().__init__('migration-progress-experimental')

    @property
    def schedule(self) -> CronSchedule:
        """Schedule the migration progress workflow to run by default daily at 5:00 a.m. (UTC)."""
        return CronSchedule(
            quartz_cron_expression="0 0 5 * * ?", timezone_id="Etc/UTC", pause_status=PauseStatus.PAUSED
        )

    @job_task(job_cluster="user_isolation")
    def verify_prerequisites(self, ctx: RuntimeContext) -> None:
        """Verify the prerequisites for running this job on the table migration cluster are fulfilled.

        We will wait up to 1 hour for the assessment run to finish if it is running or pending.
        """
        ctx.verify_progress_tracking.verify(timeout=dt.timedelta(hours=1))

    @job_task(depends_on=[verify_prerequisites])
    def crawl_tables(self, ctx: RuntimeContext) -> None:
        """Iterates over all tables in the Hive Metastore of the current workspace and persists their metadata, such
        as _database name_, _table name_, _table type_, _table location_, etc., in the table named
        `$inventory_database.tables`. The metadata stored is then used in the subsequent tasks and workflows to, for
        example, find all Hive Metastore tables that cannot easily be migrated to Unity Catalog."""
        # The table inventory cannot be (quickly) crawled from the table_migration cluster, and the main cluster is not
        # UC-enabled, so we cannot both snapshot and update the history log from the same location.
        # Step 1 of 3: Just refresh the inventory.
        ctx.tables_crawler.snapshot(force_refresh=True)

    @job_task(depends_on=[verify_prerequisites, crawl_tables], job_cluster="user_isolation")
    def refresh_table_migration_status(self, ctx: RuntimeContext) -> None:
        """Scan the tables (and views) in the inventory and record whether each has been migrated or not."""
        # Step 2 of 3: Refresh the migration status of all the tables (updated in the previous step on the main cluster.)
        ctx.migration_status_refresher.snapshot(force_refresh=True)

    @job_task(
        depends_on=[verify_prerequisites, crawl_tables, refresh_table_migration_status], job_cluster="user_isolation"
    )
    def update_tables_history_log(self, ctx: RuntimeContext) -> None:
        """Update the history log with the latest tables inventory snapshot."""
        # Step 3 of 3: Assuming (due to depends-on) the inventory and migration status were refreshed, capture into the
        # history log.
        # TODO: Avoid triggering implicit refresh here if either the table or migration-status inventory is empty.
        history_log = ctx.tables_progress
        tables_snapshot = ctx.tables_crawler.snapshot()
        history_log.append_inventory_snapshot(tables_snapshot)

    @job_task(depends_on=[verify_prerequisites], job_cluster="user_isolation")
    def crawl_udfs(self, ctx: RuntimeContext) -> None:
        """Iterates over all UDFs in the Hive Metastore of the current workspace and persists their metadata in the
        table named `$inventory_database.udfs`. This inventory is currently used when scanning securable objects for
        issues with grants that cannot be migrated to Unit Catalog."""
        history_log = ctx.udfs_progress
        udfs_snapshot = ctx.udfs_crawler.snapshot(force_refresh=True)
        history_log.append_inventory_snapshot(udfs_snapshot)

    @job_task(depends_on=[verify_prerequisites, crawl_tables, crawl_udfs], job_cluster="user_isolation")
    def crawl_grants(self, ctx: RuntimeContext) -> None:
        """Scans all securable objects for permissions that have been assigned: this include database-level permissions,
        as well permissions directly configured on objects in the (already gathered) table and UDF inventories. The
        captured information is stored in the `$inventory_database.grants` inventory table for further use during the
        migration of legacy ACLs to Unity Catalog permissions.

        Note: This job runs on a separate cluster (named `tacl`) as it requires the proper configuration to have the Table
        ACLs enabled and available for retrieval."""
        history_log = ctx.grants_progress
        grants_snapshot = ctx.grants_crawler.snapshot(force_refresh=True)
        history_log.append_inventory_snapshot(grants_snapshot)

    @job_task(depends_on=[verify_prerequisites], job_cluster="user_isolation")
    def assess_jobs(self, ctx: RuntimeContext) -> None:
        """Scans through all the jobs and identifies those that are not compatible with UC. The list of all the jobs is
        stored in the `$inventory.jobs` table.

        It looks for:
          - Clusters with Databricks Runtime (DBR) version earlier than 11.3
          - Clusters using Passthrough Authentication
          - Clusters with incompatible Spark config tags
          - Clusters referencing DBFS locations in one or more config options
        """
        history_log = ctx.jobs_progress
        jobs_snapshot = ctx.jobs_crawler.snapshot(force_refresh=True)
        history_log.append_inventory_snapshot(jobs_snapshot)

    @job_task(depends_on=[verify_prerequisites], job_cluster="user_isolation")
    def assess_clusters(self, ctx: RuntimeContext) -> None:
        """Scan through all the clusters and identifies those that are not compatible with UC. The list of all the clusters
        is stored in the`$inventory.clusters` table.

        It looks for:
          - Clusters with Databricks Runtime (DBR) version earlier than 11.3
          - Clusters using Passthrough Authentication
          - Clusters with incompatible spark config tags
          - Clusters referencing DBFS locations in one or more config options
        """
        history_log = ctx.clusters_progress
        clusters_snapshot = ctx.clusters_crawler.snapshot(force_refresh=True)
        history_log.append_inventory_snapshot(clusters_snapshot)

    @job_task(depends_on=[verify_prerequisites], job_cluster="user_isolation")
    def assess_pipelines(self, ctx: RuntimeContext) -> None:
        """This module scans through all the Pipelines and identifies those pipelines which has Azure Service Principals
        embedded (who has been given access to the Azure storage accounts via spark configurations) in the pipeline
        configurations.

        It looks for:
          - all the pipelines which has Azure Service Principal embedded in the pipeline configuration

        Subsequently, a list of all the pipelines with matching configurations are stored in the
        `$inventory.pipelines` table."""
        history_log = ctx.pipelines_progress
        pipelines_snapshot = ctx.pipelines_crawler.snapshot(force_refresh=True)
        history_log.append_inventory_snapshot(pipelines_snapshot)

    @job_task(depends_on=[verify_prerequisites], job_cluster="user_isolation")
    def crawl_cluster_policies(self, ctx: RuntimeContext) -> None:
        """This module scans through all the Cluster Policies and get the necessary information

        It looks for:
          - Clusters Policies with Databricks Runtime (DBR) version earlier than 11.3

          Subsequently, a list of all the policies with matching configurations are stored in the
        `$inventory.policies` table."""
        history_log = ctx.policies_progress
        cluster_policies_snapshot = ctx.policies_crawler.snapshot(force_refresh=True)
        history_log.append_inventory_snapshot(cluster_policies_snapshot)

    @job_task(depends_on=[verify_prerequisites])
    def crawl_redash_dashboards(self, ctx: RuntimeContext):
        """Scans all Redash dashboards."""
        ctx.redash_crawler.snapshot(force_refresh=True)

    @job_task(depends_on=[verify_prerequisites])
    def crawl_lakeview_dashboards(self, ctx: RuntimeContext):
        """Scans all Lakeview dashboards."""
        ctx.lakeview_crawler.snapshot(force_refresh=True)

    @job_task(depends_on=[crawl_redash_dashboards, crawl_lakeview_dashboards])
    def assess_dashboards(self, ctx: RuntimeContext):
        """Scans all dashboards for migration issues in SQL code of embedded widgets."""
        ctx.query_linter.refresh_report()

    @job_task(depends_on=[assess_dashboards], job_cluster="user_isolation")
    def update_redash_dashboards_history_log(self, ctx: RuntimeContext) -> None:
        """Update the history log with the latest Redash dashboards inventory snapshot."""
        redash_dashboards_snapshot = ctx.redash_crawler.snapshot(force_refresh=False)
        ctx.dashboards_progress.append_inventory_snapshot(redash_dashboards_snapshot)

    @job_task(depends_on=[assess_dashboards], job_cluster="user_isolation")
    def update_lakeview_dashboards_history_log(self, ctx: RuntimeContext) -> None:
        """Update the history log with the latest Lakeview dashboards inventory snapshot."""
        lakeview_dashboards_snapshot = ctx.lakeview_crawler.snapshot(force_refresh=False)
        ctx.dashboards_progress.append_inventory_snapshot(lakeview_dashboards_snapshot)

    @job_task(depends_on=[verify_prerequisites])
    def assess_workflows(self, ctx: RuntimeContext):
        """Scans all jobs for migration issues in notebooks.
        Also stores direct filesystem accesses for display in the migration dashboard."""
        # TODO: Ensure these are captured in the history log.
        ctx.workflow_linter.refresh_report(ctx.sql_backend, ctx.inventory_database, last_run_days=30)

    @job_task(
        depends_on=[
            verify_prerequisites,
            crawl_grants,
            crawl_udfs,
            assess_jobs,
            assess_clusters,
            assess_pipelines,
            crawl_cluster_policies,
            refresh_table_migration_status,
            update_tables_history_log,
        ],
        job_cluster="user_isolation",
    )
    def record_workflow_run(self, ctx: RuntimeContext) -> None:
        """Record the workflow run of this workflow."""
        ctx.workflow_run_recorder.record()
