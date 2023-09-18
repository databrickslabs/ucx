import logging
import os
import sys

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import MigrationConfig
from databricks.labs.ucx.framework.crawlers import RuntimeBackend
from databricks.labs.ucx.framework.tasks import task, trigger
from databricks.labs.ucx.hive_metastore import TaclToolkit
from databricks.labs.ucx.workspace_access import GroupMigrationToolkit

logger = logging.getLogger(__name__)


@task("assessment")
def setup_schema(cfg: MigrationConfig):
    """Creates a database for UCX migration intermediate state"""
    backend = RuntimeBackend()
    backend.execute(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{cfg.inventory_database}")


@task("assessment", depends_on=[setup_schema])
def crawl_tables(cfg: MigrationConfig):
    """During this operation, a systematic scan is conducted, encompassing every table within the Hive Metastore.
    This scan extracts essential details associated with each table, including its unique identifier or name, table
    format, storage location details.

    The extracted metadata is subsequently organized and cataloged within a dedicated storage entity known as
    the `$inventory.tables` table. This table functions as a comprehensive inventory, providing a structured and
    easily accessible reference point for users, data engineers, and administrators."""
    ws = WorkspaceClient(config=cfg.to_databricks_config())
    tacls = TaclToolkit(
        ws, inventory_catalog="hive_metastore", inventory_schema=cfg.inventory_database, databases=cfg.tacl.databases
    )
    tacls.database_snapshot()


@task("assessment", depends_on=[crawl_tables], job_cluster="tacl")
def crawl_grants(cfg: MigrationConfig):
    """During this operation, our process is designed to systematically scan and retrieve Legacy Table ACLs from
    the Hive Metastore. This includes comprehensive details such as user and group permissions, role-based access
    settings, and any custom access configurations. These ACLs are then thoughtfully organized and securely stored
    within the `$inventory.grants` table. This dedicated table serves as a central repository, safeguarding the
    continuity of access control data as we transition to the Databricks Unity Catalog.

    By undertaking this meticulous migration of Legacy Table ACLs, we ensure that the data governance and security
    framework established within our legacy Hive Metastore environment seamlessly carries over to our new Databricks
    Unity Catalog setup. This approach not only safeguards data integrity and access control but also guarantees
    a smooth and secure transition for our data assets, bolstering our commitment to data security and compliance
    throughout the migration process and beyond."""
    ws = WorkspaceClient(config=cfg.to_databricks_config())
    tacls = TaclToolkit(
        ws, inventory_catalog="hive_metastore", inventory_schema=cfg.inventory_database, databases=cfg.tacl.databases
    )
    tacls.grants_snapshot()


@task("assessment", depends_on=[setup_schema])
def inventorize_permissions(cfg: MigrationConfig):
    """As we embark on the complex migration journey from Hive Metastore to the Databricks Unity Catalog, a pivotal
    aspect of this transition is the comprehensive examination and preservation of permissions associated with a myriad
     of Databricks Workspace objects. These objects encompass a wide spectrum of resources such as clusters, cluster
     policies, jobs, models, experiments, SQL warehouses, SQL alerts, dashboards, queries, AWS IAM instance profiles,
     and secret scopes. Ensuring the continuity of permissions is essential not only for maintaining data security but
     also for enabling a seamless and secure migration process.

    Our meticulously designed operation systematically scans and extracts permissions across these diverse Databricks
    Workspace objects. This encompasses user and group access rights, role-based permissions, custom access
    configurations, and any specialized policies governing access to these resources. The outcome of this thorough scan
    is methodically stored within the `$inventory.permissions` table, which serves as a central repository for
    preserving and managing these critical access control details."""
    toolkit = GroupMigrationToolkit(cfg)
    toolkit.prepare_environment()
    toolkit.cleanup_inventory_table()
    toolkit.inventorize_permissions()


@task("assessment", depends_on=[crawl_tables, crawl_grants, inventorize_permissions])
def assessment_report(_: MigrationConfig):
    """This report is meticulously crafted to evaluate and assess the readiness of a specific workspace for
    the seamless adoption of the Unity Catalog.

    Our assessment process encompasses a thorough analysis of various critical aspects, including data schemas, metadata
    structures, permissions, access controls, data assets, and dependencies within the workspace. We delve deep into
    the intricacies of the existing environment, taking into consideration factors such as the complexity of data
    models, the intricacy of ACLs, the presence of custom scripts, and the overall data ecosystem.

    The result of this meticulous assessment is a detailed report that provides a holistic view of the workspace's
    readiness for migration to the Databricks Unity Catalog. This report serves as a valuable guide, offering insights,
    recommendations, and actionable steps to ensure a smooth and successful transition. It aids data engineers,
    administrators, and decision-makers in making informed choices, mitigating potential challenges, and optimizing
    the migration strategy.

    By creating this readiness assessment report, we demonstrate our commitment to a well-planned, risk-mitigated
    migration process. It ensures that our migration to the Databricks Unity Catalog is not only efficient but also
    aligns seamlessly with our data governance, security, and operational requirements, setting the stage for a new era
    of data management excellence."""
    logger.info(f"{__name__} called")


@task("migrate-groups", depends_on=[inventorize_permissions])
def migrate_permissions(cfg: MigrationConfig):
    """As we embark on the intricate migration journey from Hive Metastore to the Databricks Unity Catalog, a pivotal
    phase in this transition involves the careful orchestration of permissions. This multifaceted operation includes
    the application of permissions to designated backup groups, the seamless substitution of workspace groups with
    account groups, and the subsequent application of permissions to these newly formed account groups.

    During this meticulous process, existing permissions are thoughtfully mapped to backup groups, ensuring that data
    security and access control remain robust and consistent throughout the migration. Simultaneously, workspace groups
    are gracefully replaced with account groups to align with the structure and policies of the Databricks Unity
    Catalog.

    Once this transition is complete, permissions are diligently applied to the newly established account groups,
    preserving the existing access control framework while facilitating the seamless integration of data assets into
    the Unity Catalog environment. This meticulous orchestration of permissions guarantees the continuity of data
    security, minimizes disruption to data workflows, and enables a smooth migration experience for users and
    administrators alike.

    By undertaking this precise operation, we ensure that the transition to the Databricks Unity Catalog not only meets
    data security and governance standards but also enhances the overall efficiency and manageability of our data
    ecosystem, setting the stage for a new era of data management excellence within our organization."""
    toolkit = GroupMigrationToolkit(cfg)
    toolkit.prepare_environment()
    toolkit.apply_permissions_to_backup_groups()
    toolkit.replace_workspace_groups_with_account_groups()
    toolkit.apply_permissions_to_account_groups()


@task("migrate-groups-cleanup", depends_on=[migrate_permissions])
def delete_backup_groups(cfg: MigrationConfig):
    """Removes backup groups"""
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
