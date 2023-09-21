import logging
import os
import sys

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.assessment.crawlers import ClustersCrawler, JobsCrawler
from databricks.labs.ucx.config import MigrationConfig
from databricks.labs.ucx.framework.crawlers import RuntimeBackend
from databricks.labs.ucx.framework.tasks import task, trigger
from databricks.labs.ucx.hive_metastore import GrantsCrawler, TablesCrawler
from databricks.labs.ucx.hive_metastore.data_objects import ExternalLocationCrawler
from databricks.labs.ucx.hive_metastore.list_mounts import Mounts
from databricks.labs.ucx.workspace_access import GroupMigrationToolkit

logger = logging.getLogger(__name__)


@task("assessment")
def setup_schema(cfg: MigrationConfig):
    """Creates a database for UCX migration intermediate state"""
    backend = RuntimeBackend()
    backend.execute(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{cfg.inventory_database}")


@task("assessment", depends_on=[setup_schema], notebook="hive_metastore/tables.scala")
def crawl_tables(_: MigrationConfig):
    """In this procedure, we systematically scan every table stored within the Hive Metastore. This scanning process
    retrieves vital information for each table, which includes its distinct identifier or name, table format, and
    storage location details.

    The gathered metadata is then subsequently organized and documented within a designated storage entity referred to
    as the `$inventory.tables` table. This table serves as an extensive inventory, offering a well-structured and
    readily accessible point of reference for users, data engineers, and administrators."""


@task("assessment", depends_on=[crawl_tables], job_cluster="tacl")
def crawl_grants(cfg: MigrationConfig):
    """During this process, our methodology is purposefully designed to systematically scan and retrieve ACLs
    (Access Control Lists) associated with Legacy Tables from the Hive Metastore. These ACLs encompass comprehensive
    information, including permissions for users and groups, role-based access settings, and any custom access
    configurations. These ACLs are then thoughtfully structured and securely stored within the `$inventory.grants`
    table. This dedicated table serves as a central repository, ensuring the uninterrupted preservation of access
    control data as we transition to the Databricks Unity Catalog.

    By meticulously migrating these Legacy Table ACLs, we guarantee the seamless transfer of the data governance and
    security framework established in our legacy Hive Metastore environment to our new Databricks Unity Catalog
    setup. This approach not only safeguards data integrity and access control but also ensures a smooth and
    secure transition for our data assets. It reinforces our commitment to data security and compliance throughout the
    migration process and beyond"""
    backend = RuntimeBackend()
    tables = TablesCrawler(backend, cfg.inventory_database)
    grants = GrantsCrawler(tables)
    grants.snapshot()


@task("assessment", depends_on=[setup_schema])
def crawl_mounts(cfg: MigrationConfig):
    """In this segment of the assessment, we will define the scope of the mount points intended for migration into the
    Unity Catalog. As these objects are not compatible with the Unity Catalog paradigm, a key component of the
    migration process involves transferring them to Unity Catalog External Locations.

    The assessment involves scanning the workspace to compile a list of all existing mount points and subsequently
    storing this information in the `$inventory.mounts` table. This step enables you to create a snapshot of your
    current Mount Point infrastructure, which is crucial for planning the migration."""
    ws = WorkspaceClient(config=cfg.to_databricks_config())
    mounts = Mounts(backend=RuntimeBackend(), ws=ws, inventory_database=cfg.inventory_database)
    mounts.inventorize_mounts()


@task("assessment", depends_on=[crawl_mounts, crawl_tables])
def guess_external_locations(cfg: MigrationConfig):
    """In this section of the assessment, our objective is to determine the whereabouts of all the tables.
    Specifically, we will focus on identifying locations that utilize Mount Points. Our goal is to identify the
    External Locations necessary for a successful migration and store this information in the
    $inventory.external_locations Table.

    The approach taken in this assessment involves the following steps:
    -   Extracting all the locations associated with tables that do not use DBFS (with a focus on those
    using mount points).
    -   Scanning all these locations to identify common folders that can accommodate them.
    -   These identified external locations will be created subsequently prior to the actual table migration"""
    ws = WorkspaceClient(config=cfg.to_databricks_config())
    crawler = ExternalLocationCrawler(ws, RuntimeBackend(), cfg.inventory_database)
    crawler.snapshot()


@task("assessment", depends_on=[setup_schema])
def assess_jobs(cfg: MigrationConfig):
    """This module scans through all the jobs and identifies those that are not compatible with UC.
    It looks for:
      - Clusters with DBR version earlier than 11.3
      - Clusters using Passthrough Authentication
      - Clusters with incompatible spark config tags
      - Clusters referencing DBFS locations in one or more config options
    Subsequently, the list of all the jobs is stored in the `$inventory.jobs` table."""
    ws = WorkspaceClient(config=cfg.to_databricks_config())
    crawler = JobsCrawler(ws, RuntimeBackend(), cfg.inventory_database)
    crawler.snapshot()


@task("assessment", depends_on=[setup_schema])
def assess_clusters(cfg: MigrationConfig):
    """This module scan through all the clusters and identifies those that are not compatible with UC.
    It looks for:
      - Clusters with DBR version earlier than 11.3
      - Clusters using Passthrough Authentication
      - Clusters with incompatible spark config tags
      - Clusters referencing DBFS locations in one or more config options
    Subsequently, the list of all the clusters is stored in the`$inventory.clusters` table."""
    ws = WorkspaceClient(config=cfg.to_databricks_config())
    crawler = ClustersCrawler(ws, RuntimeBackend(), cfg.inventory_database)
    crawler.snapshot()


@task("assessment", depends_on=[setup_schema])
def crawl_permissions(cfg: MigrationConfig):
    """As we commence the intricate migration process from Hive Metastore to the Databricks Unity Catalog, a critical
    element of this transition is the thorough examination and preservation of permissions linked to a wide array of
    Databricks Workspace components. These components encompass a broad spectrum of resources, including clusters,
    cluster policies, jobs, models, experiments, SQL warehouses, SQL alerts, dashboards, queries, AWS IAM instance
    profiles, and secret scopes. Ensuring the uninterrupted continuity of permissions is of paramount importance,
    as it not only upholds data security but also facilitates a smooth and secure migration journey.

    Our carefully designed procedure systematically scans and extracts permissions associated with these diverse
    Databricks Workspace objects. This process encompasses rights granted to users and groups, role-based permissions,
    custom access configurations, and any specialized policies governing resource access. The results of this
    meticulous scan are methodically stored within the `$inventory.permissions` table, which serves as a central
    repository for preserving and managing these crucial access control details."""
    toolkit = GroupMigrationToolkit(cfg)
    toolkit.cleanup_inventory_table()
    toolkit.inventorize_permissions()


@task(
    "assessment",
    depends_on=[crawl_grants, crawl_permissions, guess_external_locations, assess_jobs, assess_clusters],
    dashboard="assessment",
)
def assessment_report(_: MigrationConfig):
    """This meticulously prepared report serves the purpose of evaluating and gauging the preparedness of a specific
    workspace for a smooth transition to the Unity Catalog.

    Our assessment procedure involves a comprehensive examination of various critical elements, including data schemas,
    metadata structures, permissions, access controls, data assets, and dependencies within the workspace. We dive deep
    into the intricacies of the current environment, taking into account factors like the complexity of data models,
    the intricacy of access control lists (ACLs), the existence of custom scripts, and the overall data ecosystem.

    The outcome of this thorough assessment is a comprehensive report that offers a holistic perspective on the
    workspace's readiness for migration to the Databricks Unity Catalog. This report serves as a valuable resource,
    provides insights, recommendations, and practical steps to ensure a seamless and successful transition.
    It assists data engineers, administrators, and decision-makers in making informed decisions, addressing potential
    challenges, and optimizing the migration strategy.

    Through the creation of this readiness assessment report, we demonstrate our commitment to a well-planned,
    risk-mitigated migration process. It guarantees that our migration to the Databricks Unity Catalog is not only
    efficient but also seamlessly aligns with our data governance, security, and operational requirements, paving the
    way for a new era of excellence in data management."""


@task("migrate-groups", depends_on=[crawl_permissions])
def migrate_permissions(cfg: MigrationConfig):
    """As we embark on the complex journey of migrating from Hive Metastore to the Databricks Unity Catalog,
    a crucial phase in this transition involves the careful management of permissions.
    This intricate process entails several key steps: first, applying permissions to designated backup groups;
    second, smoothly substituting workspace groups with account groups;
    and finally, applying permissions to these newly established account groups.

    Throughout this meticulous process, we ensure that existing permissions are thoughtfully mapped to backup groups
    to maintain robust and consistent data security and access control during the migration.

    Concurrently, we gracefully replace workspace groups with account groups to align with the structure and policies
    of the Databricks Unity Catalog.

    Once this transition is complete, we diligently apply permissions to the newly formed account groups,
    preserving the existing access control framework while facilitating the seamless integration of data assets into
    the Unity Catalog environment.

    This careful orchestration of permissions guarantees the continuity of data security, minimizes disruptions to data
    workflows, and ensures a smooth migration experience for both users and administrators. By executing this precise
    operation, we not only meet data security and governance standards but also enhance the overall efficiency and
    manageability of our data ecosystem, laying the foundation for a new era of data management excellence within our
    organization."""
    toolkit = GroupMigrationToolkit(cfg)
    toolkit.prepare_environment()
    toolkit.apply_permissions_to_backup_groups()
    toolkit.replace_workspace_groups_with_account_groups()
    toolkit.apply_permissions_to_account_groups()


@task("migrate-groups-cleanup", depends_on=[migrate_permissions])
def delete_backup_groups(cfg: MigrationConfig):
    """Removes workspace-level backup groups"""
    toolkit = GroupMigrationToolkit(cfg)
    toolkit.prepare_environment()
    toolkit.delete_backup_groups()


def main():
    trigger(*sys.argv)


if __name__ == "__main__":
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        msg = "Only intended to run in Databricks Runtime"
        raise SystemExit(msg)
    main()
