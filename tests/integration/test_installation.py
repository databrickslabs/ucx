import functools
import json
import logging
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta

import pytest
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk.errors import InvalidParameterValue, NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.install import WorkspaceInstaller
from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    Listing,
)
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager

logger = logging.getLogger(__name__)
WAREHOUSE_ID = "1ea03a36113ad8bb"


@pytest.fixture
def new_installation(ws, sql_backend, env_or_skip, inventory_schema, make_random):
    cleanup = []

    def factory(config_transform: Callable[[WorkspaceConfig], WorkspaceConfig] | None = None):
        prefix = make_random(4)
        renamed_group_prefix = f"rename-{prefix}-"
        workspace_start_path = f"/Users/{ws.current_user.me().user_name}/.{prefix}"
        prompts = MockPrompts(
            {
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Choose how to map the workspace groups.*": "1",
                r".*connect to the external metastore?.*": "yes",
                r".*Inventory Database.*": inventory_schema,
                r".*Backup prefix*": renamed_group_prefix,
                r".*Do you want to uninstall*": "yes",
                r".*Do you want to delete the inventory database *": "yes",
                r".*": "",
            }
        )
        default_cluster_id = env_or_skip("TEST_DEFAULT_CLUSTER_ID")
        tacl_cluster_id = env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID")
        Threads.strict(
            "ensure clusters running",
            [
                functools.partial(ws.clusters.ensure_cluster_is_running, default_cluster_id),
                functools.partial(ws.clusters.ensure_cluster_is_running, tacl_cluster_id),
            ],
        )
        overrides = {"main": default_cluster_id, "tacl": tacl_cluster_id}
        install = WorkspaceInstaller(ws, promtps=prompts, sql_backend=sql_backend, prefix=prefix)
        install._configure()
        install.current_config.override_clusters = overrides
        install.current_config.workspace_start_path = workspace_start_path
        if config_transform:
            install._config = config_transform(install.current_config)
        install._run_configured()
        cleanup.append(install)
        return install

    yield factory

    for installation in cleanup:
        installation.uninstall()


@retried(on=[NotFound, TimeoutError], timeout=timedelta(minutes=5))
def test_job_cluster_policy(ws, new_installation):
    install = new_installation(lambda wc: replace(wc, override_clusters=None))
    cluster_policy = ws.cluster_policies.get(policy_id=install.current_config.custom_cluster_policy_id)
    policy_definition = json.loads(cluster_policy.definition)

    assert cluster_policy.name == f"ucx-policy-{install.current_config.inventory_database}"

    assert policy_definition["spark_version"]["value"] == "14.2.x-scala2.12"
    assert policy_definition["node_type_id"]["value"] == "Standard_F4s"
    assert policy_definition["azure_attributes.availability"]["value"] == "ON_DEMAND_AZURE"


@pytest.mark.skip
@retried(on=[NotFound, TimeoutError], timeout=timedelta(minutes=15))
def test_new_job_cluster_with_policy_assessment(
    ws, new_installation, make_ucx_group, make_cluster_policy, make_cluster_policy_permissions
):
    ws_group_a, acc_group_a = make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )
    install = new_installation(
        lambda wc: replace(wc, override_clusters=None, include_group_names=[ws_group_a.display_name])
    )
    install.run_workflow("assessment")
    generic_permissions = GenericPermissionsSupport(ws, [])
    before = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert before[ws_group_a.display_name] == PermissionLevel.CAN_USE


@retried(on=[NotFound, TimeoutError], timeout=timedelta(minutes=5))
def test_job_failure_propagates_correct_error_message_and_logs(ws, sql_backend, new_installation, make_cluster_policy):
    make_cluster_policy()
    install = new_installation()

    sql_backend.execute(f"DROP SCHEMA {install.current_config.inventory_database} CASCADE")

    with pytest.raises(NotFound) as failure:
        install.run_workflow("099-destroy-schema")

    assert "cannot be found" in str(failure.value)

    workflow_run_logs = list(ws.workspace.list(f"{install._install_folder}/logs"))
    assert len(workflow_run_logs) == 1


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=12))
def test_running_real_assessment_job(
    ws, new_installation, make_ucx_group, make_cluster_policy, make_cluster_policy_permissions
):
    ws_group_a, acc_group_a = make_ucx_group()

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )

    install = new_installation(lambda wc: replace(wc, include_group_names=[ws_group_a.display_name]))
    install.run_workflow("assessment")

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

    install = new_installation(lambda wc: replace(wc, include_group_names=[ws_group_a.display_name]))
    inventory_database = install.current_config.inventory_database
    permission_manager = PermissionManager(sql_backend, inventory_database, [generic_permissions])
    permission_manager.inventorize_permissions()

    install.run_workflow("migrate-groups")

    found = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert found[acc_group_a.display_name] == PermissionLevel.CAN_USE
    assert found[f"{install.current_config.renamed_group_prefix}{ws_group_a.display_name}"] == PermissionLevel.CAN_USE


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_real_remove_backup_groups_job(ws, sql_backend, new_installation, make_ucx_group):
    ws_group_a, acc_group_a = make_ucx_group()

    install = new_installation(lambda wc: replace(wc, include_group_names=[ws_group_a.display_name]))
    cfg = install.current_config
    group_manager = GroupManager(
        sql_backend, ws, cfg.inventory_database, cfg.include_group_names, cfg.renamed_group_prefix
    )
    group_manager.snapshot()
    group_manager.rename_groups()
    group_manager.reflect_account_groups_on_workspace()

    install.run_workflow("remove-workspace-local-backup-groups")

    with pytest.raises(NotFound):
        ws.groups.get(ws_group_a.id)


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=10))
def test_repair_run_workflow_job(ws, mocker, new_installation, sql_backend):
    install = new_installation()
    mocker.patch("webbrowser.open")
    sql_backend.execute(f"DROP SCHEMA {install.current_config.inventory_database} CASCADE")
    with pytest.raises(NotFound):
        install.run_workflow("099-destroy-schema")

    sql_backend.execute(f"CREATE SCHEMA IF NOT EXISTS {install.current_config.inventory_database}")

    install.repair_run("099-destroy-schema")
    workflow_job_id = install._state.jobs["099-destroy-schema"]
    run_status = None
    while run_status is None:
        job_runs = list(install._ws.jobs.list_runs(job_id=workflow_job_id, limit=1))
        run_status = job_runs[0].state.result_state
    assert run_status.value == "SUCCESS"


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_uninstallation(ws, sql_backend, new_installation):
    install = new_installation()
    install_folder = install._install_folder
    assessment_job_id = install._state.jobs["assessment"]
    install.uninstall()
    with pytest.raises(NotFound):
        ws.workspace.get_status(install_folder)
    with pytest.raises(InvalidParameterValue):
        ws.jobs.get(job_id=assessment_job_id)
    with pytest.raises(NotFound):
        sql_backend.execute(f"show tables from hive_metastore.{install.current_config.inventory_database}")
