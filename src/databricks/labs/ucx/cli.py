import json
import os
import shutil
import webbrowser
from collections.abc import Callable
from pathlib import Path

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.account import AccountWorkspaces, WorkspaceInfo
from databricks.labs.ucx.assessment.aws import AWSResources
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.aws.credentials import IamRoleMigration
from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.credentials import ServicePrincipalMigration
from databricks.labs.ucx.azure.locations import ExternalLocationsMigration
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore import ExternalLocations, TablesCrawler
from databricks.labs.ucx.hive_metastore.catalog_schema import CatalogSchema
from databricks.labs.ucx.hive_metastore.mapping import TableMapping
from databricks.labs.ucx.hive_metastore.table_migrate import TablesMigrate
from databricks.labs.ucx.hive_metastore.table_move import TableMove
from databricks.labs.ucx.install import WorkspaceInstallation
from databricks.labs.ucx.installer.workflows import WorkflowsInstallation
from databricks.labs.ucx.source_code.files import Files
from databricks.labs.ucx.workspace_access.clusters import ClusterAccess
from databricks.labs.ucx.workspace_access.groups import GroupManager

ucx = App(__file__)
logger = get_logger(__file__)

CANT_FIND_UCX_MSG = (
    "Couldn't find UCX configuration in the user's home folder. "
    "Make sure the current user has configured and installed UCX."
)


@ucx.command
def workflows(w: WorkspaceClient):
    """Show deployed workflows and their state"""
    installation = WorkflowsInstallation.current(w)
    logger.info("Fetching deployed jobs...")
    print(json.dumps(installation.latest_job_status()))


@ucx.command
def open_remote_config(w: WorkspaceClient):
    """Opens remote configuration in the browser"""
    installation = WorkspaceInstallation.current(w)
    webbrowser.open(installation.config_file_link())


@ucx.command
def installations(w: WorkspaceClient):
    """Show installations by different users on the same workspace"""
    logger.info("Fetching installations...")
    all_users = []
    for installation in Installation.existing(w, 'ucx'):
        try:
            config = installation.load(WorkspaceConfig)
            all_users.append(
                {
                    'database': config.inventory_database,
                    'path': installation.install_folder(),
                    'warehouse_id': config.warehouse_id,
                }
            )
        except NotFound:
            continue
        except SerdeError:
            continue
    print(json.dumps(all_users))


@ucx.command
def skip(w: WorkspaceClient, schema: str | None = None, table: str | None = None):
    """Create a skip comment on a schema or a table"""
    logger.info("Running skip command")
    if not schema:
        logger.error("--schema is a required parameter.")
        return
    mapping = TableMapping.current(w)
    if table:
        mapping.skip_table(schema, table)
    else:
        mapping.skip_schema(schema)


@ucx.command(is_account=True)
def sync_workspace_info(a: AccountClient):
    """upload workspace config to all workspaces in the account where ucx is installed"""
    logger.info(f"Account ID: {a.config.account_id}")
    workspaces = AccountWorkspaces(a)
    workspaces.sync_workspace_info()


@ucx.command(is_account=True)
def create_account_groups(
    a: AccountClient, prompts: Prompts, workspace_ids: str | None = None, new_workspace_client=WorkspaceClient
):
    """
    Crawl all workspaces configured in workspace_ids, then creates account level groups if a WS local group is not present
    in the account.
    If workspace_ids is not specified, it will create account groups for all workspaces configured in the account.

    The following scenarios are supported, if a group X:
    - Exist in workspaces A,B,C and it has same members in there, it will be created in the account
    - Exist in workspaces A,B but not in C, it will be created in the account
    - Exist in workspaces A,B,C. It has same members in A,B, but not in C. Then, X and C_X will be created in the
    account
    """

    logger.info(f"Account ID: {a.config.account_id}")
    if workspace_ids is not None:
        workspace_id_list = [int(x.strip()) for x in workspace_ids.split(",")]
    else:
        workspace_id_list = None
    workspaces = AccountWorkspaces(a, new_workspace_client)
    workspaces.create_account_level_groups(prompts, workspace_id_list)


@ucx.command
def manual_workspace_info(w: WorkspaceClient, prompts: Prompts):
    """only supposed to be run if cannot get admins to run `databricks labs ucx sync-workspace-info`"""
    installation = Installation.current(w, 'ucx')
    workspace_info = WorkspaceInfo(installation, w)
    workspace_info.manual_workspace_info(prompts)


@ucx.command
def create_table_mapping(w: WorkspaceClient):
    """create initial table mapping for review"""
    table_mapping = TableMapping.current(w)
    installation = Installation.current(w, 'ucx')
    workspace_info = WorkspaceInfo(installation, w)
    config = installation.load(WorkspaceConfig)
    sql_backend = StatementExecutionBackend(w, config.warehouse_id)
    tables_crawler = TablesCrawler(sql_backend, config.inventory_database)
    path = table_mapping.save(tables_crawler, workspace_info)
    webbrowser.open(f"{w.config.host}/#workspace{path}")


@ucx.command
def validate_external_locations(w: WorkspaceClient, prompts: Prompts):
    """validates and provides mapping to external table to external location and shared generation tf scripts"""
    installation = Installation.current(w, 'ucx')
    config = installation.load(WorkspaceConfig)
    sql_backend = StatementExecutionBackend(w, config.warehouse_id)
    location_crawler = ExternalLocations(w, sql_backend, config.inventory_database)
    path = location_crawler.save_as_terraform_definitions_on_workspace(installation)
    if path and prompts.confirm(f"external_locations.tf file written to {path}. Do you want to open it?"):
        webbrowser.open(f"{w.config.host}/#workspace{path}")


@ucx.command
def ensure_assessment_run(w: WorkspaceClient):
    """ensure the assessment job was run on a workspace"""
    installation = WorkspaceInstallation.current(w)
    installation.validate_and_run("assessment")


@ucx.command
def repair_run(w: WorkspaceClient, step):
    """Repair Run the Failed Job"""
    if not step:
        raise KeyError("You did not specify --step")
    installation = WorkflowsInstallation.current(w)
    logger.info(f"Repair Running {step} Job")
    installation.repair_run(step)


@ucx.command
def validate_groups_membership(w: WorkspaceClient):
    """Validate the groups to see if the groups at account level and workspace level has different membership"""
    installation = Installation.current(w, 'ucx')
    config = installation.load(WorkspaceConfig)
    sql_backend = StatementExecutionBackend(w, config.warehouse_id)
    logger.info("Validating Groups which are having different memberships between account and workspace")
    group_manager = GroupManager(
        sql_backend=sql_backend,
        ws=w,
        inventory_database=config.inventory_database,
        include_group_names=config.include_group_names,
        renamed_group_prefix=config.renamed_group_prefix,
        workspace_group_regex=config.workspace_group_regex,
        workspace_group_replace=config.workspace_group_replace,
        account_group_regex=config.account_group_regex,
    )
    mismatch_groups = group_manager.validate_group_membership()
    print(json.dumps(mismatch_groups))


@ucx.command
def revert_migrated_tables(
    w: WorkspaceClient, prompts: Prompts, schema: str, table: str, *, delete_managed: bool = False
):
    """remove notation on a migrated table for re-migration"""
    if not schema and not table:
        question = "You haven't specified a schema or a table. All migrated tables will be reverted. Continue?"
        if not prompts.confirm(question, max_attempts=2):
            return
    tables_migrate = TablesMigrate.for_cli(w)
    revert = tables_migrate.print_revert_report(delete_managed=delete_managed)
    if revert and prompts.confirm("Would you like to continue?", max_attempts=2):
        tables_migrate.revert_migrated_tables(schema, table, delete_managed=delete_managed)


@ucx.command
def move(
    w: WorkspaceClient,
    prompts: Prompts,
    from_catalog: str,
    from_schema: str,
    from_table: str,
    to_catalog: str,
    to_schema: str,
):
    """move a uc table/tables from one schema to another schema in same or different catalog"""
    logger.info("Running move command")
    if from_catalog == "" or to_catalog == "":
        logger.error("Please enter from_catalog and to_catalog details")
        return
    if from_schema == "" or to_schema == "" or from_table == "":
        logger.error("Please enter from_schema, to_schema and from_table (enter * for migrating all tables) details.")
        return
    if from_catalog == to_catalog and from_schema == to_schema:
        logger.error("please select a different schema or catalog to migrate to")
        return
    tables = TableMove.for_cli(w)
    if not prompts.confirm(f"[WARNING] External tables will be dropped and recreated in the target schema {to_schema}"):
        return
    del_table = prompts.confirm(
        f"should we delete managed tables & views after moving to the new schema" f" {to_catalog}.{to_schema}"
    )
    logger.info(f"migrating tables {from_table} from {from_catalog}.{from_schema} to {to_catalog}.{to_schema}")
    tables.move(from_catalog, from_schema, from_table, to_catalog, to_schema, del_table=del_table)


@ucx.command
def alias(
    w: WorkspaceClient,
    from_catalog: str,
    from_schema: str,
    from_table: str,
    to_catalog: str,
    to_schema: str,
):
    """move a uc table/tables from one schema to another schema in same or different catalog"""
    if from_catalog == "" or to_catalog == "":
        logger.error("Please enter from_catalog and to_catalog details")
        return
    if from_schema == "" or to_schema == "" or from_table == "":
        logger.error("Please enter from_schema, to_schema and from_table (enter * for migrating all tables) details.")
        return
    if from_catalog == to_catalog and from_schema == to_schema:
        logger.error("please select a different schema or catalog to migrate to")
        return
    tables = TableMove.for_cli(w)
    logger.info(f"aliasing table {from_table} from {from_catalog}.{from_schema} to {to_catalog}.{to_schema}")
    tables.alias_tables(from_catalog, from_schema, from_table, to_catalog, to_schema)


def _execute_for_cloud(
    w: WorkspaceClient,
    prompts: Prompts,
    func_azure: Callable,
    func_aws: Callable,
    azure_resource_permissions: AzureResourcePermissions | None = None,
    subscription_id: str | None = None,
    aws_permissions: AWSResourcePermissions | None = None,
    aws_profile: str | None = None,
):
    if w.config.is_azure:
        if w.config.auth_type != "azure-cli":
            logger.error("In order to obtain AAD token, Please run azure cli to authenticate.")
            return None
        if not subscription_id:
            logger.error("Please enter subscription id to scan storage accounts in.")
            return None
        return func_azure(
            w, prompts, subscription_id=subscription_id, azure_resource_permissions=azure_resource_permissions
        )
    if w.config.is_aws:
        if not shutil.which("aws"):
            logger.error("Couldn't find AWS CLI in path. Please install the CLI from https://aws.amazon.com/cli/")
            return None
        if not aws_profile:
            aws_profile = os.getenv("AWS_DEFAULT_PROFILE")
        if not aws_profile:
            logger.error(
                "AWS Profile is not specified. Use the environment variable [AWS_DEFAULT_PROFILE] "
                "or use the '--aws-profile=[profile-name]' parameter."
            )
            return None
        return func_aws(w, prompts, aws_profile=aws_profile, aws_permissions=aws_permissions)
    logger.error("This cmd is only supported for azure and aws workspaces")
    return None


@ucx.command
def create_uber_principal(
    w: WorkspaceClient,
    prompts: Prompts,
    subscription_id: str | None = None,
    azure_resource_permissions: AzureResourcePermissions | None = None,
    aws_profile: str | None = None,
    aws_resource_permissions: AWSResourcePermissions | None = None,
):
    """For azure cloud, creates a service principal and gives STORAGE BLOB READER access on all the storage account
    used by tables in the workspace and stores the spn info in the UCX cluster policy. For aws,
    it identifies all s3 buckets used by the Instance Profiles configured in the workspace.
    Pass subscription_id for azure and aws_profile for aws."""
    return _execute_for_cloud(
        w,
        prompts,
        _azure_setup_uber_principal,
        _aws_setup_uber_principal,
        azure_resource_permissions,
        subscription_id,
        aws_resource_permissions,
        aws_profile,
    )


def _azure_setup_uber_principal(
    w: WorkspaceClient,
    prompts: Prompts,
    subscription_id: str,
    azure_resource_permissions: AzureResourcePermissions | None = None,
):
    include_subscriptions = [subscription_id] if subscription_id else None
    if azure_resource_permissions is None:
        azure_resource_permissions = AzureResourcePermissions.for_cli(w, include_subscriptions=include_subscriptions)
    azure_resource_permissions.create_uber_principal(prompts)


def _aws_setup_uber_principal(
    w: WorkspaceClient,
    prompts: Prompts,
    aws_profile: str,
    aws_resource_permissions: AWSResourcePermissions | None = None,
):
    installation = Installation.current(w, 'ucx')
    config = installation.load(WorkspaceConfig)
    sql_backend = StatementExecutionBackend(w, config.warehouse_id)
    aws = AWSResources(aws_profile)
    if aws_resource_permissions is None:
        aws_resource_permissions = AWSResourcePermissions.for_cli(
            w, installation, sql_backend, aws, config.inventory_database
        )
    aws_resource_permissions.create_uber_principal(prompts)


@ucx.command
def principal_prefix_access(
    w: WorkspaceClient,
    prompts: Prompts,
    subscription_id: str | None = None,
    azure_resource_permissions: AzureResourcePermissions | None = None,
    aws_profile: str | None = None,
    aws_resource_permissions: AWSResourcePermissions | None = None,
):
    """For azure cloud, identifies all storage accounts used by tables in the workspace, identify spn and its
    permission on each storage accounts. For aws, identifies all the Instance Profiles configured in the workspace and
    its access to all the S3 buckets, along with AWS roles that are set with UC access and its access to S3 buckets.
    The output is stored in the workspace install folder.
    Pass subscription_id for azure and aws_profile for aws."""
    return _execute_for_cloud(
        w,
        prompts,
        _azure_principal_prefix_access,
        _aws_principal_prefix_access,
        azure_resource_permissions,
        subscription_id,
        aws_resource_permissions,
        aws_profile,
    )


def _azure_principal_prefix_access(
    w: WorkspaceClient,
    _: Prompts,
    *,
    subscription_id: str,
    azure_resource_permissions: AzureResourcePermissions | None = None,
):
    if w.config.auth_type != "azure-cli":
        logger.error("In order to obtain AAD token, Please run azure cli to authenticate.")
        return
    include_subscriptions = [subscription_id] if subscription_id else None
    if azure_resource_permissions is None:
        azure_resource_permissions = AzureResourcePermissions.for_cli(w, include_subscriptions=include_subscriptions)
    logger.info("Generating azure storage accounts and service principal permission info")
    path = azure_resource_permissions.save_spn_permissions()
    if path:
        logger.info(f"storage and spn info saved under {path}")
    return


def _aws_principal_prefix_access(
    w: WorkspaceClient,
    _: Prompts,
    *,
    aws_profile: str,
    aws_permissions: AWSResourcePermissions | None = None,
):
    if not shutil.which("aws"):
        logger.error("Couldn't find AWS CLI in path. Please install the CLI from https://aws.amazon.com/cli/")
        return
    logger.info("Generating instance profile and bucket permission info")
    installation = Installation.current(w, 'ucx')
    config = installation.load(WorkspaceConfig)
    sql_backend = StatementExecutionBackend(w, config.warehouse_id)
    aws = AWSResources(aws_profile)
    if aws_permissions is None:
        aws_permissions = AWSResourcePermissions.for_cli(w, installation, sql_backend, aws, config.inventory_database)
    instance_role_path = aws_permissions.save_instance_profile_permissions()
    logger.info(f"Instance profile and bucket info saved {instance_role_path}")
    logger.info("Generating UC roles and bucket permission info")
    uc_role_path = aws_permissions.save_uc_compatible_roles()
    logger.info(f"UC roles and bucket info saved {uc_role_path}")


@ucx.command
def migrate_credentials(
    w: WorkspaceClient, prompts: Prompts, aws_profile: str | None = None, aws_resources: AWSResources | None = None
):
    """For Azure, this command migrates Azure Service Principals, which have Storage Blob Data Contributor,
    Storage Blob Data Reader, Storage Blob Data Owner roles on ADLS Gen2 locations that are being used in
    Databricks, to UC storage credentials.
    The Azure Service Principals to location mapping are listed in
    {install_folder}/.ucx/azure_storage_account_info.csv which is generated by principal_prefix_access command.
    Please review the file and delete the Service Principals you do not want to be migrated.
    The command will only migrate the Service Principals that have client secret stored in Databricks Secret.
    For AWS, this command migrates AWS Instance Profiles that are being used in Databricks, to UC storage credentials.
    The AWS Instance Profiles to location mapping are listed in
    {install_folder}/.ucx/aws_instance_profile_info.csv which is generated by principal_prefix_access command.
    Please review the file and delete the Instance Profiles you do not want to be migrated.
    Pass aws_profile for aws.
    """
    installation = Installation.current(w, 'ucx')
    if w.config.is_azure:
        logger.info("Running migrate_credentials for Azure")
        service_principal_migration = ServicePrincipalMigration.for_cli(w, installation, prompts)
        service_principal_migration.run(prompts)
        return
    if w.config.is_aws:
        if not aws_profile:
            aws_profile = os.getenv("AWS_DEFAULT_PROFILE")
        if not aws_profile:
            logger.error(
                "AWS Profile is not specified. Use the environment variable [AWS_DEFAULT_PROFILE] "
                "or use the '--aws-profile=[profile-name]' parameter."
            )
            return
        logger.info("Running migrate_credentials for AWS")
        if not aws_resources:
            aws_resources = AWSResources(aws_profile)
        instance_profile_migration = IamRoleMigration.for_cli(w, installation, aws_resources, prompts)
        instance_profile_migration.run(prompts)
        return
    if w.config.is_gcp:
        logger.error("migrate_credentials is not yet supported in GCP")


@ucx.command
def migrate_locations(w: WorkspaceClient, aws_profile: str | None = None):
    """This command creates UC external locations. The candidate locations to be created are extracted from
    guess_external_locations task in the assessment job. You can run validate_external_locations command to check
    the candidate locations. Please make sure the credentials haven migrated before running this command. The command
    will only create the locations that have corresponded UC Storage Credentials.
    """
    if w.config.is_azure:
        logger.info("Running migrate_locations for Azure")
        installation = Installation.current(w, 'ucx')
        service_principal_migration = ExternalLocationsMigration.for_cli(w, installation)
        service_principal_migration.run()
    if w.config.is_aws:
        logger.error("Migrate_locations for AWS")
        if not shutil.which("aws"):
            logger.error("Couldn't find AWS CLI in path. Please install the CLI from https://aws.amazon.com/cli/")
            return
        if not aws_profile:
            aws_profile = os.getenv("AWS_DEFAULT_PROFILE")
        if not aws_profile:
            logger.error(
                "AWS Profile is not specified. Use the environment variable [AWS_DEFAULT_PROFILE] "
                "or use the '--aws-profile=[profile-name]' parameter."
            )
            return
        installation = Installation.current(w, 'ucx')
        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(w, config.warehouse_id)
        aws = AWSResources(aws_profile)
        location = ExternalLocations(w, sql_backend, config.inventory_database)
        aws_permissions = AWSResourcePermissions(installation, w, sql_backend, aws, location, config.inventory_database)
        aws_permissions.create_external_locations()
    if w.config.is_gcp:
        logger.error("migrate_locations is not yet supported in GCP")


@ucx.command
def create_catalogs_schemas(w: WorkspaceClient, prompts: Prompts):
    """Create UC catalogs and schemas based on the destinations created from create_table_mapping command."""
    installation = Installation.current(w, 'ucx')
    catalog_schema = CatalogSchema.for_cli(w, installation)
    catalog_schema.create_all_catalogs_schemas(prompts)


@ucx.command
def cluster_remap(w: WorkspaceClient, prompts: Prompts):
    """Re-mapping the cluster to UC"""
    logger.info("Remapping the Clusters to UC")
    installation = Installation.current(w, 'ucx')
    cluster = ClusterAccess(installation, w, prompts)
    cluster_list = cluster.list_cluster()
    if not cluster_list:
        logger.info("No cluster information present in the workspace")
        return
    print(f"{'Cluster Name':<50}\t{'Cluster Id':<50}")
    for cluster_details in cluster_list:
        print(f"{cluster_details.cluster_name:<50}\t{cluster_details.cluster_id:<50}")
    cluster_ids = prompts.question(
        "Please provide the cluster id's as comma separated value from the above list", default="<ALL>"
    )
    cluster.map_cluster_to_uc(cluster_ids, cluster_list)


@ucx.command
def revert_cluster_remap(w: WorkspaceClient, prompts: Prompts):
    """Reverting Re-mapping of  clusters from UC"""
    logger.info("Reverting the Remapping of the Clusters from UC")
    installation = Installation.current(w, 'ucx')
    cluster_ids = [
        cluster_files.path.split("/")[-1].split(".")[0]
        for cluster_files in installation.files()
        if cluster_files.path is not None and cluster_files.path.find("backup/clusters") > 0
    ]
    if not cluster_ids:
        logger.info("There is no cluster files in the backup folder. Skipping the reverting process")
        return
    for cluster in cluster_ids:
        logger.info(cluster)
    cluster_list = prompts.question(
        "Please provide the cluster id's as comma separated value from the above list", default="<ALL>"
    )
    cluster_details = ClusterAccess(installation, w, prompts)
    cluster_details.revert_cluster_remap(cluster_list, cluster_ids)


@ucx.command
def migrate_local_code(w: WorkspaceClient, prompts: Prompts):
    """Fix the code files based on their language."""
    files = Files.for_cli(w)
    working_directory = Path.cwd()
    if not prompts.confirm("Do you want to apply UC migration to all files in the current directory?"):
        return
    files.apply(working_directory)


if __name__ == "__main__":
    ucx()
