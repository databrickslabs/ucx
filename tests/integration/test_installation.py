import functools
import json
import logging
import os.path
import sys
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta

import pytest  # pylint: disable=wrong-import-order
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState, RawState
from databricks.labs.blueprint.parallel import Threads, ManyError
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.sdk.errors import (
    AlreadyExists,
    InvalidParameterValue,
    NotFound,
)
from databricks.sdk.retries import retried
from databricks.sdk.service import compute, sql
from databricks.sdk.service.iam import PermissionLevel

import databricks
from databricks.labs.ucx.azure.access import StoragePermissionMapping
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.locations import Mount
from databricks.labs.ucx.hive_metastore.mapping import Rule
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.install import WorkspaceInstallation, WorkspaceInstaller
from databricks.labs.ucx.installer.workflows import (
    DeployedWorkflows,
    WorkflowsDeployment,
)
from databricks.labs.ucx.runtime import Workflows
from databricks.labs.ucx.workspace_access import redash
from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    Listing,
)
from databricks.labs.ucx.workspace_access.groups import GroupManager, MigratedGroup
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.redash import RedashPermissionsSupport
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport
from tests.integration.conftest import (
    StaticGrantsCrawler,
    StaticTablesCrawler,
    StaticUdfsCrawler,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def new_installation(ws, sql_backend, env_or_skip, make_random):
    cleanup = []

    def factory(
        config_transform: Callable[[WorkspaceConfig], WorkspaceConfig] | None = None,
        installation: Installation | None = None,
        product_info: ProductInfo | None = None,
        environ: dict[str, str] | None = None,
        extend_prompts: dict[str, str] | None = None,
        inventory_schema_name: str | None = None,
        skip_dashboards: bool = True,
    ):
        logger.debug("Creating new installation...")
        if not product_info:
            product_info = ProductInfo.for_testing(WorkspaceConfig)
        if not environ:
            environ = {}
        if not inventory_schema_name:
            inventory_schema_name = f"ucx_S{make_random(4).lower()}"
        renamed_group_prefix = f"rename-{product_info.product_name()}-"
        prompts = MockPrompts(
            {
                r'Open job overview in your browser.*': 'no',
                r'Do you want to uninstall ucx.*': 'yes',
                r'Do you want to delete the inventory database.*': 'yes',
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Choose how to map the workspace groups.*": "1",
                r".*connect to the external metastore?.*": "yes",
                r".*Inventory Database.*": inventory_schema_name,
                r".*Backup prefix*": renamed_group_prefix,
                r".*": "",
            }
            | (extend_prompts or {})
        )

        logger.debug("Waiting for clusters to start...")
        default_cluster_id = env_or_skip("TEST_DEFAULT_CLUSTER_ID")
        tacl_cluster_id = env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID")
        table_migration_cluster_id = env_or_skip("TEST_USER_ISOLATION_CLUSTER_ID")
        Threads.strict(
            "ensure clusters running",
            [
                functools.partial(ws.clusters.ensure_cluster_is_running, default_cluster_id),
                functools.partial(ws.clusters.ensure_cluster_is_running, tacl_cluster_id),
                functools.partial(ws.clusters.ensure_cluster_is_running, table_migration_cluster_id),
            ],
        )
        logger.debug("Waiting for clusters to start...")

        if not installation:
            installation = Installation(ws, product_info.product_name())
        install_state = InstallState.from_installation(installation)
        installer = WorkspaceInstaller(prompts, installation, ws, product_info, environ)
        workspace_config = installer.configure()
        installation = product_info.current_installation(ws)
        overrides = {"main": default_cluster_id, "tacl": tacl_cluster_id, "table_migration": table_migration_cluster_id}
        workspace_config.override_clusters = overrides

        if workspace_config.workspace_start_path == '/':
            workspace_config.workspace_start_path = installation.install_folder()
        if config_transform:
            workspace_config = config_transform(workspace_config)

        installation.save(workspace_config)

        tasks = Workflows.all().tasks()

        # TODO: see if we want to move building wheel as a context manager for yield factory,
        # so that we can shave off couple of seconds and build wheel only once per session
        # instead of every test
        workflows_installation = WorkflowsDeployment(
            workspace_config,
            installation,
            install_state,
            ws,
            product_info.wheels(ws),
            product_info,
            timedelta(minutes=3),
            tasks,
            skip_dashboards=skip_dashboards,
        )
        workspace_installation = WorkspaceInstallation(
            workspace_config,
            installation,
            install_state,
            sql_backend,
            ws,
            workflows_installation,
            prompts,
            product_info,
            skip_dashboards=skip_dashboards,
        )
        workspace_installation.run()
        cleanup.append(workspace_installation)
        return workspace_installation, DeployedWorkflows(ws, install_state, timedelta(minutes=3))

    yield factory

    for pending in cleanup:
        pending.uninstall()


def test_experimental_permissions_migration_for_group_with_same_name(  # pylint: disable=too-many-locals
    ws,
    sql_backend,
    new_installation,
    make_schema,
    make_ucx_group,
    make_table,
    make_cluster_policy,
    make_cluster_policy_permissions,
):
    ws_group, acc_group = make_ucx_group()
    migrated_group = MigratedGroup.partial_info(ws_group, acc_group)
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=migrated_group.name_in_workspace,
    )
    schema_a = make_schema()
    table_a = make_table(schema_name=schema_a.name)
    sql_backend.execute(f"GRANT USAGE ON SCHEMA {schema_a.name} TO `{migrated_group.name_in_workspace}`")
    sql_backend.execute(f"ALTER SCHEMA {schema_a.name} OWNER TO `{migrated_group.name_in_workspace}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{migrated_group.name_in_workspace}`")

    workspace_installation, deployed_workflow = new_installation(
        config_transform=lambda wc: replace(wc, include_group_names=[migrated_group.name_in_workspace])
    )
    workspace_installation.run()

    inventory_database = workspace_installation.config.inventory_database
    sql_backend.save_table(f'{inventory_database}.groups', [migrated_group], MigratedGroup)

    udf_crawler = StaticUdfsCrawler(sql_backend, inventory_database, [])
    tables_crawler = StaticTablesCrawler(sql_backend, inventory_database, [table_a])
    grants_crawler = StaticGrantsCrawler(
        tables_crawler,
        udf_crawler,
        [
            Grant(
                catalog=table_a.catalog_name,
                database=table_a.schema_name,
                table=table_a.name,
                principal=migrated_group.name_in_workspace,
                action_type="SELECT",
            ),
        ],
    )
    tacl_support = TableAclSupport(grants_crawler, sql_backend)

    generic_permissions = GenericPermissionsSupport(
        ws, [Listing(lambda: [cluster_policy], "policy_id", "cluster-policies")]
    )
    permission_manager = PermissionManager(sql_backend, inventory_database, [generic_permissions, tacl_support])
    permission_manager.inventorize_permissions()

    deployed_workflow.run_workflow("migrate-groups-experimental")

    object_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    new_schema_grants = grants_crawler.for_schema_info(schema_a)

    assert {"USAGE", "OWN"} == new_schema_grants[migrated_group.name_in_account]
    assert object_permissions[migrated_group.name_in_account] == PermissionLevel.CAN_USE


@retried(on=[NotFound, TimeoutError], timeout=timedelta(minutes=3))
def test_job_failure_propagates_correct_error_message_and_logs(ws, sql_backend, new_installation):
    workspace_installation, deployed_workflow = new_installation()

    with pytest.raises(ManyError) as failure:
        deployed_workflow.run_workflow("failing")

    assert "This is a test error message." in str(failure.value)
    assert "This task is supposed to fail." in str(failure.value)

    workflow_run_logs = list(ws.workspace.list(f"{workspace_installation.folder}/logs"))
    assert len(workflow_run_logs) == 1

    inventory_database = workspace_installation.config.inventory_database
    (records,) = next(sql_backend.fetch(f"SELECT COUNT(*) AS cnt FROM {inventory_database}.logs"))
    assert records == 3


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=3))
def test_job_cluster_policy(ws, new_installation):
    install, _ = new_installation(lambda wc: replace(wc, override_clusters=None))
    user_name = ws.current_user.me().user_name
    cluster_policy = ws.cluster_policies.get(policy_id=install.config.policy_id)
    policy_definition = json.loads(cluster_policy.definition)

    assert cluster_policy.name == f"Unity Catalog Migration ({install.config.inventory_database}) ({user_name})"

    spark_version = ws.clusters.select_spark_version(latest=True, long_term_support=True)
    assert policy_definition["spark_version"]["value"] == spark_version
    assert policy_definition["node_type_id"]["value"] == ws.clusters.select_node_type(local_disk=True)
    if ws.config.is_azure:
        assert (
            policy_definition["azure_attributes.availability"]["value"]
            == compute.AzureAvailability.ON_DEMAND_AZURE.value
        )
    if ws.config.is_aws:
        assert policy_definition["aws_attributes.availability"]["value"] == compute.AwsAvailability.ON_DEMAND.value


@pytest.mark.skip
@retried(on=[NotFound, TimeoutError], timeout=timedelta(minutes=5))
def test_new_job_cluster_with_policy_assessment(
    ws, new_installation, make_ucx_group, make_cluster_policy, make_cluster_policy_permissions
):
    ws_group_a, _ = make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )
    _, deployed_workflow = new_installation(
        lambda wc: replace(wc, override_clusters=None, include_group_names=[ws_group_a.display_name])
    )
    deployed_workflow.run_workflow("assessment")
    generic_permissions = GenericPermissionsSupport(ws, [])
    before = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert before[ws_group_a.display_name] == PermissionLevel.CAN_USE


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_real_assessment_job(
    ws, new_installation, make_ucx_group, make_cluster_policy, make_cluster_policy_permissions
):
    ws_group_a, _ = make_ucx_group()

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )

    _, deployed_workflow = new_installation(
        lambda wc: replace(wc, include_group_names=[ws_group_a.display_name]),
        skip_dashboards=False,
    )
    deployed_workflow.run_workflow("assessment")

    generic_permissions = GenericPermissionsSupport(ws, [])
    before = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert before[ws_group_a.display_name] == PermissionLevel.CAN_USE


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_real_migrate_groups_job(
    ws, sql_backend, new_installation, make_ucx_group, make_cluster_policy, make_cluster_policy_permissions
):
    ws_group_a, acc_group_a = make_ucx_group()

    # perhaps we also want to do table grants here (to test acl cluster)
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.cluster_policies.list, "policy_id", "cluster-policies"),
        ],
    )

    install, deployed_workflow = new_installation(lambda wc: replace(wc, include_group_names=[ws_group_a.display_name]))
    inventory_database = install.config.inventory_database
    permission_manager = PermissionManager(sql_backend, inventory_database, [generic_permissions])
    permission_manager.inventorize_permissions()

    deployed_workflow.run_workflow("migrate-groups")

    found = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert found[acc_group_a.display_name] == PermissionLevel.CAN_USE
    assert found[f"{install.config.renamed_group_prefix}{ws_group_a.display_name}"] == PermissionLevel.CAN_USE


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_real_validate_groups_permissions_job(
    ws, sql_backend, new_installation, make_group, make_query, make_query_permissions
):
    ws_group_a = make_group()

    query = make_query()
    make_query_permissions(
        object_id=query.id,
        permission_level=sql.PermissionLevel.CAN_EDIT,
        group_name=ws_group_a.display_name,
    )

    redash_permissions = RedashPermissionsSupport(
        ws,
        [redash.Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES)],
    )

    install, deployed_workflow = new_installation(lambda wc: replace(wc, include_group_names=[ws_group_a.display_name]))
    permission_manager = PermissionManager(sql_backend, install.config.inventory_database, [redash_permissions])
    permission_manager.inventorize_permissions()

    # assert the job does not throw any exception
    deployed_workflow.run_workflow("validate-groups-permissions")


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_running_real_validate_groups_permissions_job_fails(
    ws, sql_backend, new_installation, make_group, make_cluster_policy, make_cluster_policy_permissions
):
    ws_group_a = make_group()

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.cluster_policies.list, "policy_id", "cluster-policies"),
        ],
    )

    install, deployed_workflow = new_installation(lambda wc: replace(wc, include_group_names=[ws_group_a.display_name]))
    inventory_database = install.config.inventory_database
    permission_manager = PermissionManager(sql_backend, inventory_database, [generic_permissions])
    permission_manager.inventorize_permissions()

    # remove permission so the validation fails
    ws.permissions.set(
        request_object_type="cluster-policies", request_object_id=cluster_policy.policy_id, access_control_list=[]
    )

    with pytest.raises(ManyError):
        deployed_workflow.run_workflow("validate-groups-permissions")


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_real_remove_backup_groups_job(ws, sql_backend, new_installation, make_ucx_group):
    ws_group_a, _ = make_ucx_group()

    install, deployed_workflow = new_installation(lambda wc: replace(wc, include_group_names=[ws_group_a.display_name]))
    cfg = install.config
    group_manager = GroupManager(
        sql_backend, ws, cfg.inventory_database, cfg.include_group_names, cfg.renamed_group_prefix
    )
    group_manager.snapshot()
    group_manager.rename_groups()
    group_manager.reflect_account_groups_on_workspace()

    deployed_workflow.run_workflow("remove-workspace-local-backup-groups")

    with pytest.raises(NotFound):
        ws.groups.get(ws_group_a.id)


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=3))
def test_repair_run_workflow_job(new_installation, mocker):
    mocker.patch("webbrowser.open")
    _, deployed_workflows = new_installation()
    with pytest.raises(ManyError):
        deployed_workflows.run_workflow("failing")

    deployed_workflows.repair_run("failing")

    assert deployed_workflows.validate_step("failing")


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_uninstallation(ws, sql_backend, new_installation):
    install, _ = new_installation()
    installation = Installation(ws, product=os.path.basename(install.folder), install_folder=install.folder)
    state = InstallState.from_installation(installation)
    assessment_job_id = state.jobs["assessment"]
    install.uninstall()
    with pytest.raises(NotFound):
        ws.workspace.get_status(install.folder)
    with pytest.raises(InvalidParameterValue):
        ws.jobs.get(job_id=assessment_job_id)
    with pytest.raises(NotFound):
        sql_backend.execute(f"show tables from hive_metastore.{install.config.inventory_database}")


def test_fresh_global_installation(ws, new_installation):
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    global_installation, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
    )
    assert global_installation.folder == f"/Applications/{product_info.product_name()}"
    global_installation.uninstall()


def test_fresh_user_installation(ws, new_installation):
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    user_installation, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_user_home(ws, product_info.product_name()),
    )
    assert user_installation.folder == f"/Users/{ws.current_user.me().user_name}/.{product_info.product_name()}"
    user_installation.uninstall()


def test_global_installation_on_existing_global_install(ws, new_installation):
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    existing_global_installation, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
    )
    assert existing_global_installation.folder == f"/Applications/{product_info.product_name()}"
    reinstall_global, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    assert reinstall_global.folder == f"/Applications/{product_info.product_name()}"
    reinstall_global.uninstall()


def test_user_installation_on_existing_global_install(ws, new_installation, make_random):
    # existing install at global level
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    existing_global_installation, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
    )

    # warning to be thrown by installer if override environment variable present but no confirmation
    with pytest.raises(RuntimeWarning, match="UCX is already installed, but no confirmation"):
        new_installation(
            product_info=product_info,
            installation=Installation.assume_global(ws, product_info.product_name()),
            environ={'UCX_FORCE_INSTALL': 'user'},
            extend_prompts={
                r".*UCX is already installed on this workspace.*": 'no',
                r".*Do you want to update the existing installation?.*": 'yes',
            },
        )

    # successful override with confirmation
    reinstall_user_force, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
        environ={'UCX_FORCE_INSTALL': 'user'},
        extend_prompts={
            r".*UCX is already installed on this workspace.*": 'yes',
            r".*Do you want to update the existing installation?.*": 'yes',
        },
        inventory_schema_name=f"ucx_S{make_random(4)}_reinstall",
    )
    assert reinstall_user_force.folder == f"/Users/{ws.current_user.me().user_name}/.{product_info.product_name()}"
    reinstall_user_force.uninstall()
    existing_global_installation.uninstall()


def test_global_installation_on_existing_user_install(ws, new_installation):
    # existing installation at user level
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    existing_user_installation, _ = new_installation(
        product_info=product_info, installation=Installation.assume_user_home(ws, product_info.product_name())
    )
    assert (
        existing_user_installation.folder == f"/Users/{ws.current_user.me().user_name}/.{product_info.product_name()}"
    )

    # warning to be thrown by installer if override environment variable present but no confirmation
    with pytest.raises(RuntimeWarning, match="UCX is already installed, but no confirmation"):
        new_installation(
            product_info=product_info,
            installation=Installation.assume_user_home(ws, product_info.product_name()),
            environ={'UCX_FORCE_INSTALL': 'global'},
            extend_prompts={
                r".*UCX is already installed on this workspace.*": 'no',
                r".*Do you want to update the existing installation?.*": 'yes',
            },
        )

    # not implemented error with confirmation
    with pytest.raises(databricks.sdk.errors.NotImplemented, match="Migration needed. Not implemented yet."):
        new_installation(
            product_info=product_info,
            installation=Installation.assume_user_home(ws, product_info.product_name()),
            environ={'UCX_FORCE_INSTALL': 'global'},
            extend_prompts={
                r".*UCX is already installed on this workspace.*": 'yes',
                r".*Do you want to update the existing installation?.*": 'yes',
            },
        )
    existing_user_installation.uninstall()


def test_check_inventory_database_exists(ws, new_installation, make_random):
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    install, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
        inventory_schema_name=f"ucx_S{make_random(4)}_exists",
    )
    inventory_database = install.config.inventory_database

    with pytest.raises(
        AlreadyExists, match=f"Inventory database '{inventory_database}' already exists in another installation"
    ):
        new_installation(
            product_info=product_info,
            installation=Installation.assume_global(ws, product_info.product_name()),
            environ={'UCX_FORCE_INSTALL': 'user'},
            extend_prompts={
                r".*UCX is already installed on this workspace.*": 'yes',
                r".*Do you want to update the existing installation?.*": 'yes',
            },
            inventory_schema_name=inventory_database,
        )


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_table_migration_job(
    ws,
    new_installation,
    make_catalog,
    make_schema,
    make_table,
    env_or_skip,
    make_random,
    make_dbfs_data_copy,
    sql_backend,
):

    # skip this test if not in nightly test job or debug mode
    if os.path.basename(sys.argv[0]) not in {"_jb_pytest_runner.py", "testlauncher.py"}:
        env_or_skip("TEST_NIGHTLY")
    # create external and managed tables to be migrated
    schema = make_schema(catalog_name="hive_metastore", name=f"migrate_{make_random(5).lower()}")
    src_managed_table = make_table(schema_name=schema.name)
    existing_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/c'
    new_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/{make_random(4)}'
    make_dbfs_data_copy(src_path=existing_mounted_location, dst_path=new_mounted_location)
    src_external_table = make_table(schema_name=schema.name, external_csv=new_mounted_location)
    # create destination catalog and schema
    dst_catalog = make_catalog()
    make_schema(catalog_name=dst_catalog.name, name=schema.name)

    _, deployed_workflow = new_installation(
        lambda wc: replace(wc, override_clusters=None),
        product_info=ProductInfo.from_class(WorkspaceConfig),
        extend_prompts={
            r"Parallelism for migrating.*": "1000",
            r"Min workers for auto-scale.*": "2",
            r"Max workers for auto-scale.*": "20",
            r"Instance pool id to be set.*": env_or_skip("TEST_INSTANCE_POOL_ID"),
            r".*Do you want to update the existing installation?.*": 'yes',
        },
        inventory_schema_name=f"ucx_S{make_random(4)}_migrate_inventory",
    )

    installation = ProductInfo.from_class(WorkspaceConfig).current_installation(ws)
    installation.save(
        [
            Rule(
                "ws_name",
                dst_catalog.name,
                schema.name,
                schema.name,
                src_managed_table.name,
                src_managed_table.name,
            ),
            Rule(
                "ws_name",
                dst_catalog.name,
                schema.name,
                schema.name,
                src_external_table.name,
                src_external_table.name,
            ),
        ],
        filename='mapping.csv',
    )
    sql_backend.save_table(
        f"{installation.load(WorkspaceConfig).inventory_database}.mounts",
        [Mount(f'/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a', 'abfss://things@labsazurethings.dfs.core.windows.net/a')],
        Mount,
    )
    sql_backend.save_table(
        f"{installation.load(WorkspaceConfig).inventory_database}.tables",
        [
            Table("hive_metastore", schema.name, src_managed_table.name, "MANAGED", "DELTA", location="dbfs:/test"),
            Table("hive_metastore", schema.name, src_external_table.name, "EXTERNAL", "CSV"),
        ],
        Table,
    )
    # inject dummy group and table acl to avoid crawling which will slow down the test
    sql_backend.save_table(
        f"{installation.load(WorkspaceConfig).inventory_database}.groups",
        [
            MigratedGroup(
                "group_id",
                "test_group_ws",
                "test_group_ac",
                "tmp",
            )
        ],
        MigratedGroup,
    )
    sql_backend.save_table(
        f"{installation.load(WorkspaceConfig).inventory_database}.grants",
        [
            Grant(
                "test_user",
                "SELECT",
                database="test_database",
                table="test_table",
            )
        ],
        Grant,
    )
    installation.save(
        [
            StoragePermissionMapping(
                'abfss://things@labsazurethings.dfs.core.windows.net',
                'labsazurethings',
                'dummy_application_id',
                'principal_1',
                'WRITE_FILES',
                'Storage Blob Contributor',
                'Application',
                'directory_id_ss1',
            )
        ],
        filename='azure_storage_account_info.csv',
    )

    deployed_workflow.run_workflow("migrate-tables")
    # assert the workflow is successful
    assert deployed_workflow.validate_step("migrate-tables")
    # assert the tables are migrated
    try:
        assert ws.tables.get(f"{dst_catalog.name}.{schema.name}.{src_managed_table.name}").name
        assert ws.tables.get(f"{dst_catalog.name}.{schema.name}.{src_external_table.name}").name
    except NotFound:
        assert (
            False
        ), f"{src_managed_table.name} and {src_external_table.name} not found in {dst_catalog.name}.{schema.name}"
    # assert the cluster is configured correctly
    job_id = installation.load(RawState).resources["jobs"]["migrate-tables"]
    for job_cluster in ws.jobs.get(job_id).settings.job_clusters:
        assert job_cluster.new_cluster.autoscale.min_workers == 2
        assert job_cluster.new_cluster.autoscale.max_workers == 20
        assert job_cluster.new_cluster.spark_conf["spark.sql.sources.parallelPartitionDiscovery.parallelism"] == "1000"


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_table_migration_job_cluster_override(  # pylint: disable=too-many-locals
    ws,
    new_installation,
    make_catalog,
    make_schema,
    make_table,
    env_or_skip,
    make_random,
    make_dbfs_data_copy,
    sql_backend,
):
    # create external and managed tables to be migrated
    schema = make_schema(catalog_name="hive_metastore", name=f"migrate_{make_random(5).lower()}")
    src_managed_table = make_table(schema_name=schema.name)
    existing_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/c'
    new_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/{make_random(4)}'
    make_dbfs_data_copy(src_path=existing_mounted_location, dst_path=new_mounted_location)
    src_external_table = make_table(schema_name=schema.name, external_csv=new_mounted_location)
    # create destination catalog and schema
    dst_catalog = make_catalog()
    make_schema(catalog_name=dst_catalog.name, name=schema.name)

    product_info = ProductInfo.from_class(WorkspaceConfig)
    _, deployed_workflow = new_installation(
        product_info=product_info,
        inventory_schema_name=f"ucx_S{make_random(4)}_migrate_inventory",
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    installation = product_info.current_installation(ws)
    migrate_rules = [
        Rule(
            "ws_name",
            dst_catalog.name,
            schema.name,
            schema.name,
            src_managed_table.name,
            src_managed_table.name,
        ),
        Rule(
            "ws_name",
            dst_catalog.name,
            schema.name,
            schema.name,
            src_external_table.name,
            src_external_table.name,
        ),
    ]
    installation.save(migrate_rules, filename='mapping.csv')
    sql_backend.save_table(
        f"{installation.load(WorkspaceConfig).inventory_database}.mounts",
        [Mount(f'/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a', 'abfss://things@labsazurethings.dfs.core.windows.net/a')],
        Mount,
    )
    sql_backend.save_table(
        f"{installation.load(WorkspaceConfig).inventory_database}.tables",
        [
            Table("hive_metastore", schema.name, src_managed_table.name, "MANAGED", "DELTA", location="dbfs:/test"),
            Table("hive_metastore", schema.name, src_external_table.name, "EXTERNAL", "CSV"),
        ],
        Table,
    )
    # inject dummy group and table acl to avoid crawling which will slow down the test
    sql_backend.save_table(
        f"{installation.load(WorkspaceConfig).inventory_database}.groups",
        [
            MigratedGroup(
                "group_id",
                "test_group_ws",
                "test_group_ac",
                "tmp",
            )
        ],
        MigratedGroup,
    )
    sql_backend.save_table(
        f"{installation.load(WorkspaceConfig).inventory_database}.grants",
        [
            Grant(
                "test_user",
                "SELECT",
                database="test_database",
                table="test_table",
            )
        ],
        Grant,
    )
    installation.save(
        [
            StoragePermissionMapping(
                'abfss://things@labsazurethings.dfs.core.windows.net',
                'labsazurethings',
                'dummy_application_id',
                'principal_1',
                'WRITE_FILES',
                'Storage Blob Contributor',
                'Application',
                'directory_id_ss1',
            )
        ],
        filename='azure_storage_account_info.csv',
    )
    deployed_workflow.run_workflow("migrate-tables")
    # assert the workflow is successful
    assert deployed_workflow.validate_step("migrate-tables")
    # assert the tables are migrated
    try:
        assert ws.tables.get(f"{dst_catalog.name}.{schema.name}.{src_managed_table.name}").name
        assert ws.tables.get(f"{dst_catalog.name}.{schema.name}.{src_external_table.name}").name
    except NotFound:
        assert (
            False
        ), f"{src_managed_table.name} and {src_external_table.name} not found in {dst_catalog.name}.{schema.name}"
    # assert the cluster is configured correctly on the migrate tables tasks
    install_state = installation.load(RawState)
    job_id = install_state.resources["jobs"]["migrate-tables"]
    assert all(
        task.existing_cluster_id == env_or_skip("TEST_USER_ISOLATION_CLUSTER_ID")
        for task in ws.jobs.get(job_id).settings.tasks
        if task.task_key != "parse_logs"
    )


def test_compare_remote_local_install_versions(ws, new_installation):
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    new_installation(product_info=product_info)
    with pytest.raises(
        RuntimeWarning,
        match="UCX workspace remote and local install versions are same and no override is requested. Exiting...",
    ):
        new_installation(product_info=product_info)

    new_installation(
        product_info=product_info,
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
