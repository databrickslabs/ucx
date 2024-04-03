import functools
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.assessment.clusters import ClustersCrawler, PoliciesCrawler
from databricks.labs.ucx.assessment.init_scripts import GlobalInitScriptCrawler
from databricks.labs.ucx.assessment.jobs import JobsCrawler, SubmitRunsCrawler
from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler
from databricks.labs.ucx.factories import RuntimeContext
from databricks.labs.ucx.framework.tasks import Task, TaskLogger
from databricks.labs.ucx.hive_metastore import ExternalLocations, Mounts
from databricks.labs.ucx.hive_metastore.table_size import TableSizeCrawler
from databricks.labs.ucx.hive_metastore.tables import AclMigrationWhat, What
from databricks.labs.ucx.hive_metastore.verification import VerifyHasMetastore
from databricks.labs.ucx.workspace_access.generic import WorkspaceListing

logger = logging.getLogger(__name__)


class Snapshot(Protocol):
    def __init__(self, backend: SqlBackend, ws: WorkspaceClient, schema: str): ...

    def snapshot(self): ...


class Workflow:
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self):
        return self._name

    def simple_snapshot(self, ctx: RuntimeContext, crawler_class: type[Snapshot]):
        crawler = crawler_class(ctx.sql_backend, ctx.workspace_client, ctx.config.inventory_database)
        crawler.snapshot()

    def tasks(self) -> list[Task]:
        return []


def task(
    fn=None,
    *,
    depends_on=None,
    job_cluster="main",
    notebook: str | None = None,
    dashboard: str | None = None,
    cloud: str | None = None,
) -> Callable[[Callable], Callable]:
    def register(func):
        if not func.__doc__:
            raise SyntaxError(f"{func.__name__} must have some doc comment")
        deps = []
        if depends_on is not None:
            if not isinstance(depends_on, list):
                msg = "depends_on has to be a list"
                raise SyntaxError(msg)
            for fn in depends_on:
                deps.append(fn.__name__)
        func.__task__ = Task(
            task_id=-1,
            workflow='...',
            name=func.__name__,
            # doc=remove_extra_indentation(func.__doc__),
            doc=func.__doc__,
            fn=func,
            depends_on=deps,
            job_cluster=job_cluster,
            notebook=notebook,
            dashboard=dashboard,
            cloud=cloud,
        )
        return func

    if fn is None:
        return functools.partial(register)
    register(fn)
    return fn


class Assessment(Workflow):
    def __init__(self):
        super().__init__('assessment')

    @task(notebook="hive_metastore/tables.scala")
    def crawl_tables(self, ctx: RuntimeContext):
        """Iterates over all tables in the Hive Metastore of the current workspace and persists their metadata, such
        as _database name_, _table name_, _table type_, _table location_, etc., in the Delta table named
        `$inventory_database.tables`. Note that the `inventory_database` is set in the configuration file. The metadata
        stored is then used in the subsequent tasks and workflows to, for example,  find all Hive Metastore tables that
        cannot easily be migrated to Unity Catalog."""

    @task(job_cluster="tacl")
    def setup_tacl(self, ctx: RuntimeContext):
        """(Optimization) Starts `tacl` job cluster in parallel to crawling tables."""

    @task(depends_on=[crawl_tables, setup_tacl], job_cluster="tacl")
    def crawl_grants(self, ctx: RuntimeContext):
        """Scans the previously created Delta table named `$inventory_database.tables` and issues a `SHOW GRANTS`
        statement for every object to retrieve the permissions it has assigned to it. The permissions include information
        such as the _principal_, _action type_, and the _table_ it applies to. This is persisted in the Delta table
        `$inventory_database.grants`. Other, migration related jobs use this inventory table to convert the legacy Table
        ACLs to Unity Catalog  permissions.

        Note: This job runs on a separate cluster (named `tacl`) as it requires the proper configuration to have the Table
        ACLs enabled and available for retrieval."""
        ctx.grants_crawler.snapshot()

    @task(depends_on=[crawl_tables])
    def estimate_table_size_for_migration(self, ctx: RuntimeContext):
        """Scans the previously created Delta table named `$inventory_database.tables` and locate tables that cannot be
        "synced". These tables will have to be cloned in the migration process.
        Assesses the size of these tables and create `$inventory_database.table_size` table to list these sizes.
        The table size is a factor in deciding whether to clone these tables."""
        table_size = TableSizeCrawler(ctx.sql_backend, ctx.config.inventory_database)
        table_size.snapshot()

    @task
    def crawl_mounts(self, ctx: RuntimeContext):
        """Defines the scope of the _mount points_ intended for migration into Unity Catalog. As these objects are not
        compatible with the Unity Catalog paradigm, a key component of the migration process involves transferring them
        to Unity Catalog External Locations.

        The assessment involves scanning the workspace to compile a list of all existing mount points and subsequently
        storing this information in the `$inventory.mounts` table. This is crucial for planning the migration."""
        self.simple_snapshot(ctx, Mounts)

    @task(depends_on=[crawl_mounts, crawl_tables])
    def guess_external_locations(self, ctx: RuntimeContext):
        """Determines the shared path prefixes of all the tables. Specifically, the focus is on identifying locations that
        utilize mount points. The goal is to identify the _external locations_ necessary for a successful migration and
        store this information in the `$inventory.external_locations` table.

        The approach taken in this assessment involves the following steps:
          - Extracting all the locations associated with tables that do not use DBFS directly, but a mount point instead
          - Scanning all these locations to identify folders that can act as shared path prefixes
          - These identified external locations will be created subsequently prior to the actual table migration"""
        self.simple_snapshot(ctx, ExternalLocations)

    @task
    def assess_jobs(self, ctx: RuntimeContext):
        """Scans through all the jobs and identifies those that are not compatible with UC. The list of all the jobs is
        stored in the `$inventory.jobs` table.

        It looks for:
          - Clusters with Databricks Runtime (DBR) version earlier than 11.3
          - Clusters using Passthrough Authentication
          - Clusters with incompatible Spark config tags
          - Clusters referencing DBFS locations in one or more config options
        """
        self.simple_snapshot(ctx, JobsCrawler)

    @task
    def assess_clusters(self, ctx: RuntimeContext):
        """Scan through all the clusters and identifies those that are not compatible with UC. The list of all the clusters
        is stored in the`$inventory.clusters` table.

        It looks for:
          - Clusters with Databricks Runtime (DBR) version earlier than 11.3
          - Clusters using Passthrough Authentication
          - Clusters with incompatible spark config tags
          - Clusters referencing DBFS locations in one or more config options
        """
        self.simple_snapshot(ctx, ClustersCrawler)

    @task
    def assess_pipelines(self, ctx: RuntimeContext):
        """This module scans through all the Pipelines and identifies those pipelines which has Azure Service Principals
        embedded (who has been given access to the Azure storage accounts via spark configurations) in the pipeline
        configurations.

        It looks for:
          - all the pipelines which has Azure Service Principal embedded in the pipeline configuration

        Subsequently, a list of all the pipelines with matching configurations are stored in the
        `$inventory.pipelines` table."""
        self.simple_snapshot(ctx, PipelinesCrawler)

    @task
    def assess_incompatible_submit_runs(self, ctx: RuntimeContext):
        """This module scans through all the Submit Runs and identifies those runs which may become incompatible after
        the workspace attachment.

        It looks for:
          - All submit runs with DBR >=11.3 and data_security_mode:None

        It also combines several submit runs under a single pseudo_id based on hash of the submit run configuration.
        Subsequently, a list of all the incompatible runs with failures are stored in the
        `$inventory.submit_runs` table."""
        crawler = SubmitRunsCrawler(
            ctx.workspace_client,
            ctx.sql_backend,
            ctx.config.inventory_database,
            ctx.config.num_days_submit_runs_history,
        )
        crawler.snapshot()

    @task
    def crawl_cluster_policies(self, ctx: RuntimeContext):
        """This module scans through all the Cluster Policies and get the necessary information

        It looks for:
          - Clusters Policies with Databricks Runtime (DBR) version earlier than 11.3

          Subsequently, a list of all the policies with matching configurations are stored in the
        `$inventory.policies` table."""
        self.simple_snapshot(ctx, PoliciesCrawler)

    @task(cloud="azure")
    def assess_azure_service_principals(self, ctx: RuntimeContext):
        """This module scans through all the clusters configurations, cluster policies, job cluster configurations,
        Pipeline configurations, Warehouse configuration and identifies all the Azure Service Principals who has been
        given access to the Azure storage accounts via spark configurations referred in those entities.

        It looks in:
          - all those entities and prepares a list of Azure Service Principal embedded in their configurations

        Subsequently, the list of all the Azure Service Principals referred in those configurations are saved
        in the `$inventory.azure_service_principals` table."""
        if ctx.workspace_client.config.is_azure:
            self.simple_snapshot(ctx, AzureServicePrincipalCrawler)

    @task
    def assess_global_init_scripts(self, ctx: RuntimeContext):
        """This module scans through all the global init scripts and identifies if there is an Azure Service Principal
        who has been given access to the Azure storage accounts via spark configurations referred in those scripts.

        It looks in:
          - the list of all the global init scripts are saved in the `$inventory.azure_service_principals` table."""
        self.simple_snapshot(ctx, GlobalInitScriptCrawler)

    @task
    def workspace_listing(self, ctx: RuntimeContext):
        """Scans the workspace for workspace objects. It recursively list all sub directories
        and compiles a list of directories, notebooks, files, repos and libraries in the workspace.

        It uses multi-threading to parallelize the listing process to speed up execution on big workspaces.
        It accepts starting path as the parameter defaulted to the root path '/'."""
        crawler = WorkspaceListing(
            ctx.workspace_client,
            ctx.sql_backend,
            ctx.config.inventory_database,
            ctx.config.num_threads,
            ctx.config.workspace_start_path,
        )
        crawler.snapshot()

    @task(depends_on=[crawl_grants, workspace_listing])
    def crawl_permissions(self, ctx: RuntimeContext):
        """Scans the workspace-local groups and all their permissions. The list is stored in the `$inventory.permissions`
        Delta table.

        This is the first step for the _group migration_ process, which is continued in the `migrate-groups` workflow.
        This step includes preparing Legacy Table ACLs for local group migration."""
        permission_manager = ctx.permission_manager
        permission_manager.cleanup()
        permission_manager.inventorize_permissions()

    @task
    def crawl_groups(self, ctx: RuntimeContext):
        """Scans all groups for the local group migration scope"""
        ctx.group_manager.snapshot()

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
    def assessment_report(self, ctx: RuntimeContext):
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
    def estimates_report(self, ctx: RuntimeContext):
        """Refreshes the assessment dashboard after all previous tasks have been completed. Note that you can access the
        dashboard _before_ all tasks have been completed, but then only already completed information is shown."""


class GroupMigration(Workflow):
    def __init__(self):
        super().__init__('migrate-groups')

    @task(depends_on=[Assessment.crawl_groups])
    def rename_workspace_local_groups(self, ctx: RuntimeContext):
        """Renames workspace local groups by adding `ucx-renamed-` prefix."""
        verify_has_metastore = VerifyHasMetastore(ctx.workspace_client)
        if verify_has_metastore.verify_metastore():
            logger.info("Metastore exists in the workspace")
        ctx.group_manager.rename_groups()

    @task(depends_on=[rename_workspace_local_groups])
    def reflect_account_groups_on_workspace(self, ctx: RuntimeContext):
        """Adds matching account groups to this workspace. The matching account level group(s) must preexist(s) for this
        step to be successful. This process does not create the account level group(s)."""
        ctx.group_manager.reflect_account_groups_on_workspace()

    @task(depends_on=[reflect_account_groups_on_workspace], job_cluster="tacl")
    def apply_permissions_to_account_groups(self, ctx: RuntimeContext):
        """Fourth phase of the workspace-local group migration process. It does the following:
          - Assigns the full set of permissions of the original group to the account-level one

        It covers local workspace-local permissions for all entities: Legacy Table ACLs, Entitlements,
        AWS instance profiles, Clusters, Cluster policies, Instance Pools, Databricks SQL warehouses, Delta Live
        Tables, Jobs, MLflow experiments, MLflow registry, SQL Dashboards & Queries, SQL Alerts, Token and Password usage
        permissions, Secret Scopes, Notebooks, Directories, Repos, Files.

        See [interactive tutorial here](https://app.getreprise.com/launch/myM3VNn/)."""
        migration_state = ctx.group_manager.get_migration_state()
        if len(migration_state.groups) == 0:
            logger.info("Skipping group migration as no groups were found.")
            return
        ctx.permission_manager.apply_group_permissions(migration_state)

    @task(job_cluster="tacl")
    def validate_groups_permissions(self, ctx: RuntimeContext):
        """Validate that all the crawled permissions are applied correctly to the destination groups."""
        ctx.permission_manager.verify_group_permissions()


class TableMigration(Workflow):
    def __init__(self):
        super().__init__('migrate-tables')

    @task(job_cluster="table_migration")
    def migrate_external_tables_sync(self, ctx: RuntimeContext):
        """This workflow task migrates the *external tables that are supported by SYNC command* from the Hive Metastore to the Unity Catalog.
        Following cli commands are required to be run before running this task:
        - For Azure: `principal-prefix-access`, `create-table-mapping`, `create-uber-principal`, `migrate-credentials`, `migrate-locations`, `create-catalogs-schemas`
        - For AWS: TBD
        """
        ctx.tables_migrator.migrate_tables(what=What.EXTERNAL_SYNC, acl_strategy=[AclMigrationWhat.LEGACY_TACL])

    @task(job_cluster="table_migration")
    def migrate_dbfs_root_delta_tables(self, ctx: RuntimeContext):
        """This workflow task migrates `delta tables stored in DBFS root` from the Hive Metastore to the Unity Catalog using deep clone.
        Following cli commands are required to be run before running this task:
        - For Azure: `principal-prefix-access`, `create-table-mapping`, `create-uber-principal`, `migrate-credentials`, `migrate-locations`, `create-catalogs-schemas`
        - For AWS: TBD
        """
        ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA, acl_strategy=[AclMigrationWhat.LEGACY_TACL])


class Workflows:
    _tasks: dict[str, Workflow]

    def __init__(self, workflows: list[Workflow]):
        for workflow in workflows:
            for task_definition in workflow.tasks():
                if task_definition.name in self._tasks:
                    raise ValueError(f"Task {task_definition.name} is already defined in another workflow")
                self._tasks[task_definition.name] = workflow

    @classmethod
    def all(cls):
        return cls([Assessment(), GroupMigration(), TableMigration()])

    @staticmethod
    def _parse_args(*argv) -> dict[str, str]:
        args = dict(a[2:].split("=") for a in argv if a[0:2] == "--")
        if "config" not in args:
            msg = "no --config specified"
            raise KeyError(msg)
        return args

    def trigger(self, *argv):
        args = self._parse_args(*argv)
        config_path = Path(args["config"])

        ctx = RuntimeContext(config_path)

        install_dir = config_path.parent

        task_name = args.get("task", "not specified")
        if task_name not in self._tasks:
            msg = f'task "{task_name}" not found. Valid tasks are: {", ".join(self._tasks.keys())}'
            raise KeyError(msg)
        print(f"UCX v{__version__}")
        workflow = self._tasks[task_name]
        # `{{parent_run_id}}` is the run of entire workflow, whereas `{{run_id}}` is the run of a task
        workflow_run_id = args.get("parent_run_id", "unknown_run_id")
        job_id = args.get("job_id", "unknown_job_id")
        with TaskLogger(
            install_dir,
            workflow=workflow.name,
            workflow_id=job_id,
            task_name=task_name,
            workflow_run_id=workflow_run_id,
            log_level=ctx.config.log_level,
        ) as task_logger:
            ucx_logger = logging.getLogger("databricks.labs.ucx")
            ucx_logger.info(f"UCX v{__version__} After job finishes, see debug logs at {task_logger}")
            current_task = getattr(workflow, task_name)
            current_task(ctx)
