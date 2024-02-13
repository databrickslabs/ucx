import json
import os
import shutil
import webbrowser

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.account import AccountWorkspaces, WorkspaceInfo
from databricks.labs.ucx.assessment.aws import AWSResourcePermissions
from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore import ExternalLocations, TablesCrawler
from databricks.labs.ucx.hive_metastore.mapping import TableMapping
from databricks.labs.ucx.hive_metastore.table_migrate import TableMove, TablesMigrate
from databricks.labs.ucx.install import WorkspaceInstallation
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
    installation = WorkspaceInstallation.current(w)
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


@ucx.command
def manual_workspace_info(w: WorkspaceClient):
    """only supposed to be run if cannot get admins to run `databricks labs ucx sync-workspace-info`"""
    prompts = Prompts()
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
def validate_external_locations(w: WorkspaceClient):
    """validates and provides mapping to external table to external location and shared generation tf scripts"""
    prompts = Prompts()
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
    installation = WorkspaceInstallation.current(w)
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
def revert_migrated_tables(w: WorkspaceClient, schema: str, table: str, *, delete_managed: bool = False):
    """remove notation on a migrated table for re-migration"""
    prompts = Prompts()
    if not schema and not table:
        question = "You haven't specified a schema or a table. All migrated tables will be reverted. Continue?"
        if not prompts.confirm(question, max_attempts=2):
            return
    tm = TablesMigrate.for_cli(w)
    revert = tm.print_revert_report(delete_managed=delete_managed)
    if revert and prompts.confirm("Would you like to continue?", max_attempts=2):
        tm.revert_migrated_tables(schema, table, delete_managed=delete_managed)


@ucx.command
def move(
    w: WorkspaceClient,
    from_catalog: str,
    from_schema: str,
    from_table: str,
    to_catalog: str,
    to_schema: str,
):
    """move a uc table/tables from one schema to another schema in same or different catalog"""
    logger.info("Running move command")
    prompts = Prompts()
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
    del_table = prompts.confirm(f"should we delete tables/view after moving to new schema {to_catalog}.{to_schema}")
    logger.info(f"migrating tables {from_table} from {from_catalog}.{from_schema} to {to_catalog}.{to_schema}")
    tables.move_tables(from_catalog, from_schema, from_table, to_catalog, to_schema, del_table)


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


@ucx.command
def save_azure_storage_accounts(w: WorkspaceClient, subscription_id: str):
    """identifies all azure storage account used by external tables
    identifies all spn which has storage blob reader, blob contributor, blob owner access
    saves the data in ucx database."""
    if not w.config.is_azure:
        logger.error("Workspace is not on azure, please run this command on azure databricks workspaces.")
        return
    if w.config.auth_type != "azure_cli":
        logger.error("In order to obtain AAD token, Please run azure cli to authenticate.")
        return
    if subscription_id == "":
        logger.error("Please enter subscription id to scan storage account in.")
        return
    azure_resource_permissions = AzureResourcePermissions.for_cli(w)
    logger.info("Generating azure storage accounts and service principal permission info")
    azure_resource_permissions.save_spn_permissions()


@ucx.command
def save_aws_iam_profiles(w: WorkspaceClient, aws_profile: str | None = None):
    """identifies all Instance Profiles and map their access to S3 buckets.
    Requires a working setup of AWS CLI.
    https://aws.amazon.com/cli/
    The command saves a CSV to the UCX installation folder with the mapping.

    The user has to be authenticated with AWS and the have the permissions to browse the resources and iam services.
    More information can be found here:
    https://docs.aws.amazon.com/IAM/latest/UserGuide/access_permissions-required.html
    """
    if not shutil.which("aws"):
        logger.error("Couldn't find AWS CLI in path.Please obtain and install the CLI from https://aws.amazon.com/cli/")
        return None
    if not aws_profile:
        aws_profile = os.getenv("AWS_DEFAULT_PROFILE")
    if not aws_profile:
        logger.error(
            "AWS Profile is not specified. Use the environment variable [AWS_DEFAULT_PROFILE] "
            "or use the '--aws-profile=[profile-name]' parameter."
        )
        return None
    aws_permissions = AWSResourcePermissions.for_cli(w, aws_profile)
    aws_permissions.save_instance_profile_permissions()
    return None


@ucx.command
def save_uc_compatible_roles(w: WorkspaceClient, *, aws_profile: str | None = None):
    """extracts all the iam roles with trust relationships to the UC master role.
     Map these roles to the S3 buckets they have access to.
    Requires a working setup of AWS CLI.
    https://aws.amazon.com/cli/
    The command saves a CSV to the UCX installation folder with the mapping.

    The user has to be authenticated with AWS and the have the permissions to browse the resources and iam services.
    More information can be found here:
    https://docs.aws.amazon.com/IAM/latest/UserGuide/access_permissions-required.html
    """
    if not shutil.which("aws"):
        logger.error("Couldn't find AWS CLI in path.Please obtain and install the CLI from https://aws.amazon.com/cli/")
        return None
    if not aws_profile:
        aws_profile = os.getenv("AWS_DEFAULT_PROFILE")
    if not aws_profile:
        logger.error(
            "AWS Profile is not specified. Use the environment variable [AWS_DEFAULT_PROFILE] "
            "or use the '--aws-profile=[profile-name]' parameter."
        )
        return None
    aws_permissions = AWSResourcePermissions.for_cli(w, aws_profile)
    aws_permissions.save_uc_compatible_roles()
    return None


if __name__ == "__main__":
    ucx()
