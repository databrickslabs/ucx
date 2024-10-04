from io import BytesIO
import json
import webbrowser
from pathlib import Path

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ExportFormat
from databricks.labs.ucx.__about__ import __version__

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.account_cli import AccountContext
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext, LocalCheckoutContext
from databricks.labs.ucx.hive_metastore.tables import What
from databricks.labs.ucx.install import AccountInstaller
from databricks.labs.ucx.source_code.linters.files import LocalCodeLinter

ucx = App(__file__)
logger = get_logger(__file__)

CANT_FIND_UCX_MSG = (
    "Couldn't find UCX configuration in the user's home folder. "
    "Make sure the current user has configured and installed UCX."
)


def _get_workspace_contexts(
    w: WorkspaceClient, a: AccountClient | None = None, run_as_collection: bool = False, **named_parameters
) -> list[WorkspaceContext]:
    """Get workspace contexts to the workspaces for which the user has access"""
    if not a:
        a = AccountClient(product='ucx', product_version=__version__)
    account_installer = AccountInstaller(a)
    workspace_contexts = account_installer.get_workspace_contexts(w, run_as_collection, **named_parameters)
    return workspace_contexts


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
        return ctx.table_mapping.skip_table_or_view(schema, table, ctx.tables_crawler.load_one)
    return ctx.table_mapping.skip_schema(schema)


@ucx.command
def unskip(w: WorkspaceClient, schema: str | None = None, table: str | None = None):
    """Unset the skip mark from a schema or a table"""
    logger.info("Running unskip command")
    if not schema:
        logger.error("--schema is a required parameter.")
        return None
    ctx = WorkspaceContext(w)
    if table:
        return ctx.table_mapping.unskip_table_or_view(schema, table, ctx.tables_crawler.load_one)
    return ctx.table_mapping.unskip_schema(schema)


@ucx.command(is_account=True)
def sync_workspace_info(a: AccountClient):
    """upload workspace config to all workspaces in the account where ucx is installed"""
    logger.info(f"Account ID: {a.config.account_id}")
    ctx = AccountContext(a)
    ctx.account_workspaces.sync_workspace_info()


@ucx.command(is_account=True)
def report_account_compatibility(a: AccountClient, ctx: AccountContext | None = None, **named_parameters):
    """Report compatibility of all workspaces available in the account"""
    if not ctx:
        ctx = AccountContext(a, named_parameters)
    ctx.account_aggregate.readiness_report()


@ucx.command(is_account=True)
def validate_table_locations(a: AccountClient, ctx: AccountContext | None = None, **named_parameters):
    """Validate if the table locations are overlapping in a workspace and across workspaces"""
    if not ctx:
        ctx = AccountContext(a, named_parameters)
    ctx.account_aggregate.validate_table_locations()


@ucx.command(is_account=True)
def create_account_groups(
    a: AccountClient,
    prompts: Prompts,
    ctx: AccountContext | None = None,
    **named_parameters,
):
    """
    Crawl all workspaces configured in workspace_ids, then creates account level groups if a WS local group is not present
    in the account.
    If workspace_ids is not specified, it will create account groups for all workspaces configured in the account.

    The following scenarios are supported, if a group X:
    - Exist in workspaces A,B,C, and it has same members in there, it will be created in the account
    - Exist in workspaces A,B but not in C, it will be created in the account
    - Exist in workspaces A,B,C. It has same members in A,B, but not in C. Then, X and C_X will be created in the
    account
    """
    if not ctx:
        ctx = AccountContext(a, named_parameters)
    ctx.account_workspaces.create_account_level_groups(prompts, ctx.workspace_ids)


@ucx.command
def manual_workspace_info(w: WorkspaceClient, prompts: Prompts):
    """only supposed to be run if cannot get admins to run `databricks labs ucx sync-workspace-info`"""
    ctx = WorkspaceContext(w)
    ctx.workspace_info.manual_workspace_info(prompts)


@ucx.command
def create_table_mapping(
    w: WorkspaceClient,
    ctx: WorkspaceContext | None = None,
    run_as_collection: bool = False,
    a: AccountClient | None = None,
):
    """create initial table mapping for review"""
    workspace_contexts = _get_workspace_contexts(w, a, run_as_collection)
    if ctx:
        workspace_contexts = [ctx]
    for workspace_ctx in workspace_contexts:
        logger.info(f"Running cmd for workspace {workspace_ctx.workspace_client.get_workspace_id()}")
        path = workspace_ctx.table_mapping.save(workspace_ctx.tables_crawler, workspace_ctx.workspace_info)
        if len(workspace_contexts) == 1:
            webbrowser.open(f"{w.config.host}/#workspace{path}")


@ucx.command
def validate_external_locations(
    w: WorkspaceClient,
    prompts: Prompts,
    ctx: WorkspaceContext | None = None,
    run_as_collection: bool = False,
    a: AccountClient | None = None,
):
    """validates and provides mapping to external table to external location and shared generation tf scripts"""
    if ctx:
        workspace_contexts = [ctx]
    else:
        workspace_contexts = _get_workspace_contexts(w, a, run_as_collection)
    for workspace_context in workspace_contexts:
        path = workspace_context.external_locations.save_as_terraform_definitions_on_workspace(
            workspace_context.installation
        )
        if path and prompts.confirm(f"external_locations.tf file written to {path}. Do you want to open it?"):
            webbrowser.open(f"{w.config.host}/#workspace{path}")


@ucx.command
def ensure_assessment_run(w: WorkspaceClient, run_as_collection: bool = False, a: AccountClient | None = None):
    """ensure the assessment job was run on a workspace"""
    workspace_contexts = _get_workspace_contexts(w, a, run_as_collection)
    for ctx in workspace_contexts:
        workspace_id = ctx.workspace_client.get_workspace_id()
        logger.info(f"Checking assessment workflow in workspace: {workspace_id}")
        deployed_workflows = ctx.deployed_workflows
        # Note: will block if the workflow is already underway but not completed.
        if deployed_workflows.validate_step("assessment"):
            logger.info(f"The assessment workflow has successfully completed in workspace: {workspace_id}")
        else:
            logger.info(f"Starting assessment workflow in workspace: {workspace_id}")
            # If running for a collection, don't wait for each assessment job to finish as that will take a long time.
            deployed_workflows.run_workflow("assessment", skip_job_wait=run_as_collection)


@ucx.command
def update_migration_progress(
    w: WorkspaceClient,
    run_as_collection: bool = False,
    a: AccountClient | None = None,
) -> None:
    """Manually trigger the migration-progress-experimental job."""
    workspace_contexts = _get_workspace_contexts(w, a, run_as_collection)
    for ctx in workspace_contexts:
        workspace_id = ctx.workspace_client.get_workspace_id()
        logger.info(f"Starting 'migration-progress-experimental' workflow in workspace: {workspace_id}")
        deployed_workflows = ctx.deployed_workflows
        # If running for a collection, don't wait for each migration-progress job to finish as that will take long time.
        deployed_workflows.run_workflow("migration-progress-experimental", skip_job_wait=run_as_collection)


@ucx.command
def repair_run(w: WorkspaceClient, step):
    """Repair Run the Failed Job"""
    if not step:
        raise KeyError("You did not specify --step")
    ctx = WorkspaceContext(w)
    logger.info(f"Repair Running {step} Job")
    ctx.deployed_workflows.repair_run(step)


@ucx.command
def validate_groups_membership(
    w: WorkspaceClient,
    ctx: WorkspaceContext | None = None,
    run_as_collection: bool = False,
    a: AccountClient | None = None,
) -> None:
    """Validate the groups to see if the groups at account level and workspace level has different membership"""
    if ctx:
        workspace_contexts = [ctx]
    else:
        workspace_contexts = _get_workspace_contexts(w, a, run_as_collection)
    for workspace_context in workspace_contexts:
        mismatch_groups = workspace_context.group_manager.validate_group_membership()
        print(json.dumps(mismatch_groups))


@ucx.command
def revert_migrated_tables(
    w: WorkspaceClient,
    prompts: Prompts,
    schema: str | None = None,
    table: str | None = None,
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
    revert = ctx.tables_migrator.print_revert_report(schema=schema, table=table, delete_managed=delete_managed)
    if revert and prompts.confirm("Would you like to continue?", max_attempts=2):
        ctx.tables_migrator.revert_migrated_tables(schema=schema, table=table, delete_managed=delete_managed)


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
    run_as_collection: bool = False,
    a: AccountClient | None = None,
    **named_parameters,
):
    """For azure cloud, creates a service principal and gives STORAGE BLOB READER access on all the storage account
    used by tables in the workspace and stores the spn info in the UCX cluster policy. For aws,
    it identifies all s3 buckets used by the Instance Profiles configured in the workspace.
    Pass subscription ids for Azure and aws_profile for AWS."""
    if ctx:
        workspace_contexts = [ctx]
    else:
        workspace_contexts = _get_workspace_contexts(w, a, run_as_collection, **named_parameters)
    for workspace_context in workspace_contexts:
        if workspace_context.is_azure:
            workspace_context.azure_resource_permissions.create_uber_principal(prompts)
        elif workspace_context.is_aws:
            workspace_context.aws_resource_permissions.create_uber_principal(prompts)
        else:
            raise ValueError("Unsupported cloud provider")


@ucx.command
def principal_prefix_access(
    w: WorkspaceClient,
    ctx: WorkspaceContext | None = None,
    run_as_collection: bool = False,
    a: AccountClient | None = None,
    **named_parameters,
):
    """For azure cloud, identifies all storage accounts used by tables in the workspace, identify spn and its
    permission on each storage accounts. For aws, identifies all the Instance Profiles configured in the workspace and
    its access to all the S3 buckets, along with AWS roles that are set with UC access and its access to S3 buckets.
    The output is stored in the workspace install folder.
    Pass subscription ids for Azure and aws_profile for AWS."""
    workspace_contexts = _get_workspace_contexts(w, a, run_as_collection, **named_parameters)
    if ctx:
        workspace_contexts = [ctx]
    if w.config.is_azure:
        for workspace_ctx in workspace_contexts:
            logger.info(f"Running cmd for workspace {workspace_ctx.workspace_client.get_workspace_id()}")
            workspace_ctx.azure_resource_permissions.save_spn_permissions()
        return
    if w.config.is_aws:
        for workspace_ctx in workspace_contexts:
            logger.info(f"Running cmd for workspace {workspace_ctx.workspace_client.get_workspace_id()}")
            instance_role_path = workspace_ctx.aws_resource_permissions.save_instance_profile_permissions()
            logger.info(f"Instance profile and bucket info saved {instance_role_path}")
            logger.info("Generating UC roles and bucket permission info")
            workspace_ctx.aws_resource_permissions.save_uc_compatible_roles()
        return
    raise ValueError("Unsupported cloud provider")


@ucx.command
def create_missing_principals(
    w: WorkspaceClient,
    prompts: Prompts,
    ctx: WorkspaceContext | None = None,
    run_as_collection: bool = False,
    a: AccountClient | None = None,
    single_role: bool = False,
    role_name="UC_ROLE",
    policy_name="UC_POLICY",
    **named_parameters,
):
    """Not supported for Azure.
    For AWS, this command identifies all the S3 locations that are missing a UC compatible role and creates them.
    By default, it will create a  role per S3 location. Set the optional single_role parameter to True to create a single role for all S3 locations.
    """
    workspace_contexts = _get_workspace_contexts(w, a, run_as_collection, **named_parameters)
    if ctx:
        workspace_contexts = [ctx]
    if w.config.is_aws:
        for workspace_ctx in workspace_contexts:
            logger.info(f"Running cmd for workspace {workspace_ctx.workspace_client.get_workspace_id()}")
            workspace_ctx.iam_role_creation.run(
                prompts, single_role=single_role, role_name=role_name, policy_name=policy_name
            )
    else:
        raise ValueError("Unsupported cloud provider")


@ucx.command
def delete_missing_principals(
    w: WorkspaceClient,
    prompts: Prompts,
    ctx: WorkspaceContext | None = None,
    **named_parameters,
):
    """Not supported for Azure.
    For AWS, this command identifies all the UC roles that are created through the create-missing-principals cmd.
    It lists all the UC roles in aws and lets users select the roles to delete. It also validates if the selected roles
    are used by any storage credential and prompts to confirm if roles should still be deleted.
    """
    if not ctx:
        ctx = WorkspaceContext(w, named_parameters)
    if ctx.is_aws:
        return ctx.iam_role_creation.delete_uc_roles(prompts)
    raise ValueError("Unsupported cloud provider")


@ucx.command
def migrate_credentials(
    w: WorkspaceClient,
    prompts: Prompts,
    ctx: WorkspaceContext | None = None,
    run_as_collection: bool = False,
    a: AccountClient | None = None,
    **named_parameters,
):
    """For Azure, this command prompts to i) create UC storage credentials for the access connectors with a
    managed identity created for each storage account present in the ADLS Gen2 locations, the access connectors are
    granted Storage Blob Data Contributor permissions on their corresponding storage account, to prepare for adopting to
    use Databricks' best practice for using access connectors to authenticate with external storage, and ii) to migrate
    Azure Service Principals, which have Storage Blob Data Contributor, Storage Blob Data Reader, Storage Blob Data
    Owner roles on ADLS Gen2 locations that are being used in Databricks, to UC storage credentials. The Azure Service
    Principals to location mapping are listed in {install_folder}/.ucx/azure_storage_account_info.csv which is generated
    by principal_prefix_access command. Please review the file and delete the Service Principals you do not want to be
    migrated. The command will only migrate the Service Principals that have client secret stored in Databricks Secret.
    For AWS, this command migrates AWS UC compatible roles that are required by Databricks, to UC storage credentials.
    The AWS Roles to location mapping are listed in
    {install_folder}/.ucx/uc_roles_access.csv which is generated by principal_prefix_access command.
    Please review the file and delete the Roles you do not want to be migrated.
    Pass aws_profile for aws.
    """
    workspace_contexts = _get_workspace_contexts(w, a, run_as_collection, **named_parameters)
    if ctx:
        workspace_contexts = [ctx]
    if w.config.is_azure:
        for workspace_ctx in workspace_contexts:
            logger.info(f"Running cmd for workspace {workspace_ctx.workspace_client.get_workspace_id()}")
            workspace_ctx.service_principal_migration.run(prompts)
    elif w.config.is_aws:
        for workspace_ctx in workspace_contexts:
            logger.info(f"Running cmd for workspace {workspace_ctx.workspace_client.get_workspace_id()}")
            workspace_ctx.iam_role_migration.run(prompts)
    else:
        raise ValueError("Unsupported cloud provider")


@ucx.command
def migrate_locations(
    w: WorkspaceClient,
    ctx: WorkspaceContext | None = None,
    run_as_collection: bool = False,
    a: AccountClient | None = None,
    **named_parameters,
):
    """This command creates UC external locations. The candidate locations to be created are extracted from
    guess_external_locations task in the assessment job. You can run validate_external_locations command to check
    the candidate locations. Please make sure the credentials haven migrated before running this command. The command
    will only create the locations that have corresponded UC Storage Credentials.
    """
    if ctx:
        workspace_contexts = [ctx]
    else:
        workspace_contexts = _get_workspace_contexts(w, a, run_as_collection, **named_parameters)
    for workspace_context in workspace_contexts:
        if workspace_context.is_azure or workspace_context.is_aws:
            workspace_context.external_locations_migration.run()
        else:
            raise ValueError("Unsupported cloud provider")


@ucx.command
def create_catalogs_schemas(
    w: WorkspaceClient,
    prompts: Prompts,
    ctx: WorkspaceContext | None = None,
    run_as_collection: bool = False,
    a: AccountClient | None = None,
) -> None:
    """Create UC catalogs and schemas based on the destinations created from create_table_mapping command."""
    if ctx:
        workspace_contexts = [ctx]
    else:
        workspace_contexts = _get_workspace_contexts(w, a, run_as_collection)
    for workspace_context in workspace_contexts:
        workspace_context.catalog_schema.create_all_catalogs_schemas(prompts)


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
    cluster_ids = []
    for cluster_files in ctx.installation.files():
        if cluster_files.path is None:
            continue
        if cluster_files.path.find("backup/clusters") == 0:
            continue
        cluster_id = cluster_files.path.split("/")[-1].split(".")[0]
        cluster_ids.append(cluster_id)
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
    ctx = LocalCheckoutContext(w)
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
    ctx: AccountContext | None = None,
):
    """Assign metastore to a workspace"""
    if workspace_id is None:
        logger.error("--workspace-id is a required parameter.")
        return
    try:
        workspace_id_casted = int(workspace_id)
    except ValueError:
        logger.error("--workspace-id should be an integer.")
        return
    logger.info(f"Account ID: {a.config.account_id}")
    ctx = ctx or AccountContext(a)
    ctx.account_metastores.assign_metastore(
        ctx.prompts,
        workspace_id_casted,
        metastore_id=metastore_id,
        default_catalog=default_catalog,
    )


@ucx.command
def create_ucx_catalog(w: WorkspaceClient, prompts: Prompts, ctx: WorkspaceContext | None = None) -> None:
    """Create and setup UCX artifact catalog

    Amongst other things, the artifacts are used for tracking the migration progress across workspaces.
    """
    workspace_context = ctx or WorkspaceContext(w)
    workspace_context.catalog_schema.create_ucx_catalog(prompts)
    workspace_context.progress_tracking_installation.run()


@ucx.command
def migrate_tables(
    w: WorkspaceClient,
    prompts: Prompts,
    *,
    ctx: WorkspaceContext | None = None,
    run_as_collection: bool = False,
    a: AccountClient | None = None,
) -> None:
    """
    Trigger the migrate-tables workflow and, optionally, the migrate-external-hiveserde-tables-in-place-experimental
    workflow and migrate-external-tables-ctas.
    """
    if ctx:
        workspace_contexts = [ctx]
    else:
        workspace_contexts = _get_workspace_contexts(w, a, run_as_collection)
    for workspace_context in workspace_contexts:
        deployed_workflows = workspace_context.deployed_workflows
        deployed_workflows.run_workflow("migrate-tables")

        tables = list(workspace_context.tables_crawler.snapshot())
        hiveserde_tables = [table for table in tables if table.what == What.EXTERNAL_HIVESERDE]
        if len(hiveserde_tables) > 0:
            percentage_hiveserde_tables = len(hiveserde_tables) / len(tables) * 100
            if prompts.confirm(
                f"Found {len(hiveserde_tables)} ({percentage_hiveserde_tables:.2f}%) hiveserde tables in "
                f"{workspace_context.workspace_client.config.host}, do you want to run "
                f"the `migrate-external-hiveserde-tables-in-place-experimental` workflow?"
            ):
                deployed_workflows.run_workflow("migrate-external-hiveserde-tables-in-place-experimental")

        external_ctas_tables = [table for table in tables if table.what == What.EXTERNAL_NO_SYNC]
        if len(external_ctas_tables) > 0:
            percentage_external_ctas_tables = len(external_ctas_tables) / len(tables) * 100
            if prompts.confirm(
                f"Found {len(external_ctas_tables)} ({percentage_external_ctas_tables:.2f}%) external tables which "
                f"cannot be migrated using sync in {workspace_context.workspace_client.config.host}, do you want to "
                "run the `migrate-external-tables-ctas` workflow?"
            ):
                deployed_workflows.run_workflow("migrate-external-tables-ctas")


@ucx.command
def migrate_acls(
    w: WorkspaceClient,
    *,
    ctx: WorkspaceContext | None = None,
    run_as_collection: bool = False,
    a: AccountClient | None = None,
    **named_parameters,
):
    """
    Migrate the ACLs for migrated tables and view. Can work with hms federation or other table migration scenarios.
    """
    if ctx:
        workspace_contexts = [ctx]
    else:
        workspace_contexts = _get_workspace_contexts(w, a, run_as_collection, **named_parameters)
    target_catalog, hms_fed = named_parameters.get("target_catalog"), named_parameters.get("hms_fed", False)
    for workspace_context in workspace_contexts:
        workspace_context.acl_migrator.migrate_acls(target_catalog=target_catalog, hms_fed=hms_fed)


@ucx.command
def migrate_dbsql_dashboards(
    w: WorkspaceClient,
    dashboard_id: str | None = None,
    ctx: WorkspaceContext | None = None,
    run_as_collection: bool = False,
    a: AccountClient | None = None,
) -> None:
    """Migrate table references in DBSQL Dashboard queries"""
    if ctx:
        workspace_contexts = [ctx]
    else:
        workspace_contexts = _get_workspace_contexts(w, a, run_as_collection)
    for workspace_context in workspace_contexts:
        workspace_context.redash.migrate_dashboards(dashboard_id)


@ucx.command
def revert_dbsql_dashboards(w: WorkspaceClient, dashboard_id: str | None = None):
    """Revert migrated DBSQL Dashboard queries back to their original state"""
    ctx = WorkspaceContext(w)
    ctx.redash.revert_dashboards(dashboard_id)


@ucx.command(is_account=True)
def join_collection(a: AccountClient, workspace_ids: str):
    """joins the workspace to an existing collection"""
    account_installer = AccountInstaller(a)
    w_ids = [int(_.strip()) for _ in workspace_ids.split(",") if _]
    account_installer.join_collection(w_ids)


@ucx.command
def upload(
    file: Path | str,
    w: WorkspaceClient,
    run_as_collection: bool = False,
    a: AccountClient | None = None,  # Only used while testing
):
    """Upload a file to the (collection of) workspace(s)"""
    file = Path(file)
    contexts = _get_workspace_contexts(w, run_as_collection=run_as_collection, a=a)
    logger.warning("The schema of CSV files is NOT validated, ensure it is correct")
    for ctx in contexts:
        ctx.installation.upload(file.name, file.read_bytes())
    if len(contexts) > 0:
        logger.info(f"Finished uploading {file}")


@ucx.command
def download(
    file: Path | str,
    w: WorkspaceClient,
    run_as_collection: bool = False,
    a: AccountClient | None = None,  # Only used while testing
):
    """Download and merge a CSV file from the ucx installation in a (collection of) workspace(s)"""
    file = Path(file)
    if file.suffix != ".csv":
        raise ValueError("Command only supported for CSV files")
    contexts = _get_workspace_contexts(w, run_as_collection=run_as_collection, a=a)
    csv_header = None
    with file.open("wb") as output:
        for ctx in contexts:
            remote_file_name = f"{ctx.installation.install_folder()}/{file.name}"
            try:
                # Installation does not have a download method
                data = ctx.workspace_client.workspace.download(remote_file_name, format=ExportFormat.AUTO).read()
            except NotFound:
                logger.warning(f"File not found for {ctx.workspace_client.config.host}: {remote_file_name}")
                continue
            input_ = BytesIO()  # BytesIO supports .readline() to read the header, where StreamingResponse does not
            input_.write(data.rstrip(b"\n"))
            input_.seek(0)  # Go back to the beginning of the file
            csv_header_next = input_.readline()
            if csv_header is None:
                csv_header = csv_header_next
                output.write(csv_header)
            elif csv_header == csv_header_next:
                output.write(b"\n")
            else:
                raise ValueError("CSV files have different headers")
            output.write(input_.read())
    if csv_header is None:
        logger.warning("No file(s) to download found")
    if file.is_file() and file.stat().st_size == 0:
        file.unlink()
    else:
        logger.info(f"Finished downloading {file}")


@ucx.command
def lint_local_code(
    w: WorkspaceClient, prompts: Prompts, path: str | None = None, ctx: LocalCheckoutContext | None = None
):
    """Lint local code files looking for problems."""
    if ctx is None:
        ctx = LocalCheckoutContext(w)
    linter: LocalCodeLinter = ctx.local_code_linter
    linter.lint(prompts, None if path is None else Path(path))


if __name__ == "__main__":
    ucx()
