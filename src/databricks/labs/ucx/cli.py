import json
import webbrowser
from pathlib import Path

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.account import AccountWorkspaces
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.cli_command import AccountContext, WorkspaceContext
from databricks.labs.ucx.hive_metastore.tables import What

ucx = App(__file__)
logger = get_logger(__file__)

CANT_FIND_UCX_MSG = (
    "Couldn't find UCX configuration in the user's home folder. "
    "Make sure the current user has configured and installed UCX."
)


@ucx.command
def workflows(w: WorkspaceClient):
    """Show deployed workflows and their state"""
    ctx = WorkspaceContext(w)
    logger.info("Fetching deployed jobs...")
    latest_job_status = ctx.deployed_workflows.latest_job_status()
    print(json.dumps(latest_job_status))


@ucx.command
def logs(w: WorkspaceClient, workflow: str | None = None):
    """Show logs of the latest job run"""
    ctx = WorkspaceContext(w)
    ctx.deployed_workflows.relay_logs(workflow)


@ucx.command
def open_remote_config(w: WorkspaceClient):
    """Opens remote configuration in the browser"""
    ctx = WorkspaceContext(w)
    workspace_link = ctx.installation.workspace_link('config.yml')
    webbrowser.open(workspace_link)


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
        return None
    ctx = WorkspaceContext(w)
    if table:
        return ctx.table_mapping.skip_table(schema, table)
    return ctx.table_mapping.skip_schema(schema)


@ucx.command(is_account=True)
def sync_workspace_info(a: AccountClient):
    """upload workspace config to all workspaces in the account where ucx is installed"""
    logger.info(f"Account ID: {a.config.account_id}")
    ctx = AccountContext(a)
    ctx.account_workspaces.sync_workspace_info()


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
    ctx = WorkspaceContext(w)
    ctx.workspace_info.manual_workspace_info(prompts)


@ucx.command
def create_table_mapping(w: WorkspaceClient):
    """create initial table mapping for review"""
    ctx = WorkspaceContext(w)
    path = ctx.table_mapping.save(ctx.tables_crawler, ctx.workspace_info)
    webbrowser.open(f"{w.config.host}/#workspace{path}")


@ucx.command
def validate_external_locations(w: WorkspaceClient, prompts: Prompts):
    """validates and provides mapping to external table to external location and shared generation tf scripts"""
    ctx = WorkspaceContext(w)
    path = ctx.external_locations.save_as_terraform_definitions_on_workspace(ctx.installation)
    if path and prompts.confirm(f"external_locations.tf file written to {path}. Do you want to open it?"):
        webbrowser.open(f"{w.config.host}/#workspace{path}")


@ucx.command
def ensure_assessment_run(w: WorkspaceClient):
    """ensure the assessment job was run on a workspace"""
    ctx = WorkspaceContext(w)
    deployed_workflows = ctx.deployed_workflows
    if not deployed_workflows.validate_step("assessment"):
        deployed_workflows.run_workflow("assessment")


@ucx.command
def repair_run(w: WorkspaceClient, step):
    """Repair Run the Failed Job"""
    if not step:
        raise KeyError("You did not specify --step")
    ctx = WorkspaceContext(w)
    logger.info(f"Repair Running {step} Job")
    ctx.deployed_workflows.repair_run(step)


@ucx.command
def validate_groups_membership(w: WorkspaceClient):
    """Validate the groups to see if the groups at account level and workspace level has different membership"""
    ctx = WorkspaceContext(w)
    mismatch_groups = ctx.group_manager.validate_group_membership()
    print(json.dumps(mismatch_groups))


@ucx.command
def revert_migrated_tables(
    w: WorkspaceClient,
    prompts: Prompts,
    schema: str,
    table: str,
    *,
    delete_managed: bool = False,
    ctx: WorkspaceContext | None = None,
):
    """remove notation on a migrated table for re-migration"""
    if not schema and not table:
        question = "You haven't specified a schema or a table. All migrated tables will be reverted. Continue?"
        if not prompts.confirm(question, max_attempts=2):
            return
    if not ctx:
        ctx = WorkspaceContext(w)
    revert = ctx.tables_migrator.print_revert_report(delete_managed=delete_managed)
    if revert and prompts.confirm("Would you like to continue?", max_attempts=2):
        ctx.tables_migrator.revert_migrated_tables(schema, table, delete_managed=delete_managed)


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
    if not prompts.confirm(f"[WARNING] External tables will be dropped and recreated in the target schema {to_schema}"):
        return
    del_table = prompts.confirm(
        f"should we delete managed tables & views after moving to the new schema" f" {to_catalog}.{to_schema}"
    )
    logger.info(f"migrating tables {from_table} from {from_catalog}.{from_schema} to {to_catalog}.{to_schema}")
    ctx = WorkspaceContext(w)
    ctx.table_move.move(from_catalog, from_schema, from_table, to_catalog, to_schema, del_table=del_table)


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
    logger.info(f"aliasing table {from_table} from {from_catalog}.{from_schema} to {to_catalog}.{to_schema}")
    ctx = WorkspaceContext(w)
    ctx.table_move.alias_tables(from_catalog, from_schema, from_table, to_catalog, to_schema)


@ucx.command
def create_uber_principal(
    w: WorkspaceClient,
    prompts: Prompts,
    ctx: WorkspaceContext | None = None,
    **named_parameters,
):
    """For azure cloud, creates a service principal and gives STORAGE BLOB READER access on all the storage account
    used by tables in the workspace and stores the spn info in the UCX cluster policy. For aws,
    it identifies all s3 buckets used by the Instance Profiles configured in the workspace.
    Pass subscription_id for azure and aws_profile for aws."""
    if not ctx:
        ctx = WorkspaceContext(w, named_parameters)
    if ctx.is_azure:
        return ctx.azure_resource_permissions.create_uber_principal(prompts)
    if ctx.is_aws:
        return ctx.aws_resource_permissions.create_uber_principal(prompts)
    raise ValueError("Unsupported cloud provider")


@ucx.command
def principal_prefix_access(w: WorkspaceClient, ctx: WorkspaceContext | None = None, **named_parameters):
    """For azure cloud, identifies all storage accounts used by tables in the workspace, identify spn and its
    permission on each storage accounts. For aws, identifies all the Instance Profiles configured in the workspace and
    its access to all the S3 buckets, along with AWS roles that are set with UC access and its access to S3 buckets.
    The output is stored in the workspace install folder.
    Pass subscription_id for azure and aws_profile for aws."""
    if not ctx:
        ctx = WorkspaceContext(w, named_parameters)
    if ctx.is_azure:
        return ctx.azure_resource_permissions.save_spn_permissions()
    if ctx.is_aws:
        instance_role_path = ctx.aws_resource_permissions.save_instance_profile_permissions()
        logger.info(f"Instance profile and bucket info saved {instance_role_path}")
        logger.info("Generating UC roles and bucket permission info")
        return ctx.aws_resource_permissions.save_uc_compatible_roles()
    raise ValueError("Unsupported cloud provider")


@ucx.command
def migrate_credentials(w: WorkspaceClient, prompts: Prompts, ctx: WorkspaceContext | None = None, **named_parameters):
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
    if not ctx:
        ctx = WorkspaceContext(w, named_parameters)
    if ctx.is_azure:
        return ctx.service_principal_migration.run(prompts)
    if ctx.is_aws:
        return ctx.iam_role_migration.run(prompts)
    raise ValueError("Unsupported cloud provider")


@ucx.command
def migrate_locations(w: WorkspaceClient, ctx: WorkspaceContext | None = None, **named_parameters):
    """This command creates UC external locations. The candidate locations to be created are extracted from
    guess_external_locations task in the assessment job. You can run validate_external_locations command to check
    the candidate locations. Please make sure the credentials haven migrated before running this command. The command
    will only create the locations that have corresponded UC Storage Credentials.
    """
    if not ctx:
        ctx = WorkspaceContext(w, named_parameters)
    if ctx.is_azure:
        return ctx.azure_external_locations_migration.run()
    if ctx.is_aws:
        return ctx.aws_resource_permissions.create_external_locations()
    raise ValueError("Unsupported cloud provider")


@ucx.command
def create_catalogs_schemas(w: WorkspaceClient, prompts: Prompts):
    """Create UC catalogs and schemas based on the destinations created from create_table_mapping command."""
    ctx = WorkspaceContext(w)
    ctx.catalog_schema.create_all_catalogs_schemas(prompts)


@ucx.command
def cluster_remap(w: WorkspaceClient, prompts: Prompts):
    """Re-mapping the cluster to UC"""
    logger.info("Remapping the Clusters to UC")
    ctx = WorkspaceContext(w)
    cluster_list = ctx.cluster_access.list_cluster()
    if not cluster_list:
        logger.info("No cluster information present in the workspace")
        return
    print(f"{'Cluster Name':<50}\t{'Cluster Id':<50}")
    for cluster_details in cluster_list:
        print(f"{cluster_details.cluster_name:<50}\t{cluster_details.cluster_id:<50}")
    cluster_ids = prompts.question(
        "Please provide the cluster id's as comma separated value from the above list", default="<ALL>"
    )
    ctx.cluster_access.map_cluster_to_uc(cluster_ids, cluster_list)


@ucx.command
def revert_cluster_remap(w: WorkspaceClient, prompts: Prompts):
    """Reverting Re-mapping of  clusters from UC"""
    logger.info("Reverting the Remapping of the Clusters from UC")
    ctx = WorkspaceContext(w)
    cluster_ids = [
        cluster_files.path.split("/")[-1].split(".")[0]
        for cluster_files in ctx.installation.files()
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
    ctx.cluster_access.revert_cluster_remap(cluster_list, cluster_ids)


@ucx.command
def migrate_local_code(w: WorkspaceClient, prompts: Prompts):
    """Fix the code files based on their language."""
    ctx = WorkspaceContext(w)
    working_directory = Path.cwd()
    if not prompts.confirm("Do you want to apply UC migration to all files in the current directory?"):
        return
    ctx.local_file_migrator.apply(working_directory)


@ucx.command(is_account=True)
def show_all_metastores(a: AccountClient, workspace_id: str | None = None):
    """Show all metastores in the account"""
    logger.info(f"Account ID: {a.config.account_id}")
    ctx = AccountContext(a)
    ctx.account_metastores.show_all_metastores(workspace_id)


@ucx.command(is_account=True)
def assign_metastore(
    a: AccountClient,
    workspace_id: str | None = None,
    metastore_id: str | None = None,
    default_catalog: str | None = None,
):
    """Assign metastore to a workspace"""
    logger.info(f"Account ID: {a.config.account_id}")
    ctx = AccountContext(a)
    ctx.account_metastores.assign_metastore(ctx.prompts, workspace_id, metastore_id, default_catalog)


@ucx.command
def migrate_tables(w: WorkspaceClient, prompts: Prompts, *, ctx: WorkspaceContext | None = None):
    """
    Trigger the migrate-tables workflow and, optionally, the migrate-external-hiveserde-tables-in-place-experimental
    workflow.
    """
    if ctx is None:
        ctx = WorkspaceContext(w)
    deployed_workflows = ctx.deployed_workflows
    deployed_workflows.run_workflow("migrate-tables")

    tables = ctx.tables_crawler.snapshot()
    hiveserde_tables = [table for table in tables if table.what == What.EXTERNAL_HIVESERDE]
    if len(hiveserde_tables) > 0:
        percentage_hiveserde_tables = len(hiveserde_tables) / len(tables) * 100
        if prompts.confirm(
            f"Found {len(hiveserde_tables)} ({percentage_hiveserde_tables:.2f}%) hiveserde tables, do you want to run "
            f"the migrate-external-hiveserde-tables-in-place-experimental workflow?"
        ):
            deployed_workflows.run_workflow("migrate-external-hiveserde-tables-in-place-experimental")


if __name__ == "__main__":
    ucx()
