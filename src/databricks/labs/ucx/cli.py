import json
import os
import shutil
import webbrowser
from collections.abc import Callable

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import Policy

from databricks.labs.ucx.account import AccountWorkspaces, WorkspaceInfo
from databricks.labs.ucx.assessment.aws import (
    AWSInstanceProfile,
    AWSResourcePermissions,
)
from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.credentials import ServicePrincipalMigration
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


@ucx.command(is_account=True)
def create_account_groups(a: AccountClient, workspace_ids: list[int] | None = None):
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
    prompts = Prompts()
    workspaces = AccountWorkspaces(a)
    workspaces.create_account_level_groups(prompts, workspace_ids)


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
    tables_migrate = TablesMigrate.for_cli(w)
    revert = tables_migrate.print_revert_report(delete_managed=delete_managed)
    if revert and prompts.confirm("Would you like to continue?", max_attempts=2):
        tables_migrate.revert_migrated_tables(schema, table, delete_managed=delete_managed)


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
def principal_prefix_access(w: WorkspaceClient, subscription_id: str | None = None, aws_profile: str | None = None):
    """For azure cloud, identifies all storage accounts used by tables in the workspace, identify spn and its
    permission on each storage accounts. For aws, identifies all the Instance Profiles configured in the workspace and
    its access to all the S3 buckets, along with AWS roles that are set with UC access and its access to S3 buckets.
    The output is stored in the workspace install folder.
    Pass subscription_id for azure and aws_profile for aws."""
    return _execute_for_cloud(
        w, _azure_principal_prefix_access, _aws_principal_prefix_access, subscription_id, aws_profile
    )


@ucx.command
def setup_migration_principal(w: WorkspaceClient, subscription_id: str | None = None, aws_profile: str | None = None):
    """Update UCX cluster policy with the instance profile that gives access to perform tables migration.
    For azure cloud, it identifies all storage accounts used by tables in the workspace. For aws,
    it identifies all s3 buckets used by the Instance Profiles configured in the workspace.
    Pass subscription_id for azure and aws_profile for aws."""
    return _execute_for_cloud(
        w, _azure_setup_migration_principal, _aws_setup_migration_principal, subscription_id, aws_profile
    )


def _execute_for_cloud(
    w: WorkspaceClient,
    func_azure: Callable,
    func_aws: Callable,
    subscription_id: str | None = None,
    aws_profile: str | None = None,
):
    if w.config.is_azure:
        if not subscription_id:
            logger.error("Please enter subscription id to scan storage accounts in.")
            return None
        return func_azure(w, subscription_id)
    if w.config.is_aws:
        if not aws_profile:
            aws_profile = os.getenv("AWS_DEFAULT_PROFILE")
        if not aws_profile:
            logger.error(
                "AWS Profile is not specified. Use the environment variable [AWS_DEFAULT_PROFILE] "
                "or use the '--aws-profile=[profile-name]' parameter."
            )
            return None
        return func_aws(w, aws_profile)
    logger.error("This cmd is only supported for azure and aws workspaces")
    return None


def _azure_setup_migration_principal(w: WorkspaceClient, subscription_id: str):
    # issue: https://github.com/databrickslabs/ucx/issues/881
    raise NotImplementedError


def _aws_setup_migration_principal(w: WorkspaceClient, aws_profile: str):
    if not shutil.which("aws"):
        logger.error("Couldn't find AWS CLI in path. Please install the CLI from https://aws.amazon.com/cli/")
        return

    installation = Installation.current(w, 'ucx')
    config = installation.load(WorkspaceConfig)

    if not config.policy_id:
        msg = "Cluster policy not found in UCX config"
        logger.error(msg)
        return

    sql_backend = StatementExecutionBackend(w, config.warehouse_id)
    location_crawler = ExternalLocations(w, sql_backend, config.inventory_database)
    external_locations = location_crawler.snapshot()
    s3_paths = {loc.location for loc in external_locations}

    if len(s3_paths) == 0:
        logger.info("No S3 paths to migrate found")
        return

    aws_permissions = AWSResourcePermissions.for_cli(w, sql_backend, aws_profile, config.inventory_database)
    cluster_policy = _get_cluster_policy(w, config.policy_id)
    iam_role_name_in_cluster_policy = _get_iam_role_from_cluster_policy(str(cluster_policy.definition))

    iam_policy_name = f"UCX_MIGRATION_POLICY_{config.inventory_database}"
    prompts = Prompts()
    if iam_role_name_in_cluster_policy and aws_permissions.is_role_exists(iam_role_name_in_cluster_policy):
        if prompts.confirm(
            f"We have identified existing UCX migration role \"{iam_role_name_in_cluster_policy}\" "
            f"in cluster policy \"{cluster_policy.name}\". "
            f"Do you want to update the role's migration policy?"
        ):
            aws_permissions.add_or_update_migration_policy(iam_role_name_in_cluster_policy, iam_policy_name, s3_paths)
        return

    iam_role_name = f"UCX_MIGRATION_ROLE_{config.inventory_database}"
    if aws_permissions.is_role_exists(iam_role_name):
        if not prompts.confirm(
            f"We have identified existing UCX migration role \"{iam_role_name}\". "
            f"Do you want to update the role's migration iam policy "
            f"and add the role to UCX migration cluster policy \"{cluster_policy.name}\"?"
        ):
            return
        aws_permissions.add_or_update_migration_policy(iam_role_name, iam_policy_name, s3_paths)
        iam_instance_profile = aws_permissions.get_instance_profile(iam_role_name)
    else:
        if not prompts.confirm(
            f"Do you want to create new migration role \"{iam_role_name}\" and "
            f"add the role to UCX migration cluster policy \"{cluster_policy.name}\"?"
        ):
            return
        iam_instance_profile = aws_permissions.create_migration_instance_profile(
            iam_role_name, iam_policy_name, s3_paths
        )

    if iam_instance_profile:
        _update_cluster_policy_with_aws_instance_profile(w, cluster_policy, iam_instance_profile)
        logger.info(f"Cluster policy \"{cluster_policy.name}\" updated successfully")


def _get_cluster_policy(w: WorkspaceClient, policy_id: str) -> Policy:
    try:
        return w.cluster_policies.get(policy_id=policy_id)
    except NotFound as err:
        msg = f"UCX Policy {policy_id} not found, please reinstall UCX"
        logger.error(msg)
        raise NotFound(msg) from err


def _get_iam_role_from_cluster_policy(cluster_policy_definition: str) -> AWSInstanceProfile | None:
    definition_dict = json.loads(cluster_policy_definition)
    if definition_dict.get("aws_attributes.instance_profile_arn") is not None:
        instance_profile_arn = definition_dict.get("aws_attributes.instance_profile_arn").get("value")
        logger.info(f"Migration instance profile is set to {instance_profile_arn}")

        return AWSInstanceProfile(instance_profile_arn).role_name

    return None


def _update_cluster_policy_with_aws_instance_profile(
    w: WorkspaceClient, cluster_policy: Policy, iam_instance_profile: AWSInstanceProfile
):
    definition_dict = json.loads(str(cluster_policy.definition))
    definition_dict["aws_attributes.instance_profile_arn"] = {
        "type": "fixed",
        "value": iam_instance_profile.instance_profile_arn,
    }

    w.cluster_policies.edit(
        str(cluster_policy.policy_id), str(cluster_policy.name), definition=json.dumps(definition_dict)
    )


def _azure_principal_prefix_access(w: WorkspaceClient, subscription_id: str):
    if w.config.auth_type != "azure-cli":
        logger.error("In order to obtain AAD token, Please run azure cli to authenticate.")
        return
    include_subscriptions = [subscription_id] if subscription_id else None
    azure_resource_permissions = AzureResourcePermissions.for_cli(w, include_subscriptions=include_subscriptions)
    logger.info("Generating azure storage accounts and service principal permission info")
    path = azure_resource_permissions.save_spn_permissions()
    if path:
        logger.info(f"storage and spn info saved under {path}")
    return


def _aws_principal_prefix_access(w: WorkspaceClient, aws_profile: str):
    if not shutil.which("aws"):
        logger.error("Couldn't find AWS CLI in path. Please install the CLI from https://aws.amazon.com/cli/")
        return
    logger.info("Generating instance profile and bucket permission info")
    installation = Installation.current(w, 'ucx')
    config = installation.load(WorkspaceConfig)
    sql_backend = StatementExecutionBackend(w, config.warehouse_id)
    aws_permissions = AWSResourcePermissions.for_cli(w, sql_backend, aws_profile, config.inventory_database)
    instance_role_path = aws_permissions.save_instance_profile_permissions()
    logger.info(f"Instance profile and bucket info saved {instance_role_path}")
    logger.info("Generating UC roles and bucket permission info")
    uc_role_path = aws_permissions.save_uc_compatible_roles()
    logger.info(f"UC roles and bucket info saved {uc_role_path}")


@ucx.command
def migrate_credentials(w: WorkspaceClient):
    """For Azure, this command migrate Azure Service Principals, which have Storage Blob Data Contributor,
    Storage Blob Data Reader, Storage Blob Data Owner roles on ADLS Gen2 locations that are being used in
    Databricks, to UC storage credentials.
    The Azure Service Principals to location mapping are listed in /Users/{user_name}/.ucx/azure_storage_account_info.csv
    which is generated by principal_prefix_access command. Please review the file and delete the Service Principals
    you do not want to be migrated.
    The command will only migrate the Service Principals that have client secret stored in Databricks Secret.
    """
    prompts = Prompts()
    if w.config.is_azure:
        logger.info("Running migrate_credentials for Azure")
        installation = Installation.current(w, 'ucx')
        service_principal_migration = ServicePrincipalMigration.for_cli(w, installation, prompts)
        service_principal_migration.run(prompts)
    if w.config.is_aws:
        logger.error("migrate_credentials is not yet supported in AWS")
    if w.config.is_gcp:
        logger.error("migrate_credentials is not yet supported in GCP")


if __name__ == "__main__":
    ucx()
