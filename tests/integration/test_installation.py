import functools
import logging
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta

import pytest
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import WheelsV2
from databricks.sdk.errors import InvalidParameterValue, NotFound, Unknown
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.install import PRODUCT_INFO, WorkspaceInstallation
from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    Listing,
)
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager

logger = logging.getLogger(__name__)


@pytest.fixture
def new_installation(ws, sql_backend, env_or_skip, inventory_schema, make_random):
    cleanup = []

    prompts = MockPrompts(
        {
            r'Open job overview in your browser.*': 'no',
            r'Do you want to uninstall ucx.*': 'yes',
            r'Do you want to delete the inventory database.*': 'yes',
        }
    )

    def factory(config_transform: Callable[[WorkspaceConfig], WorkspaceConfig] | None = None):
        prefix = make_random(4)
        renamed_group_prefix = f"rename-{prefix}-"
        workspace_start_path = f"/Users/{ws.current_user.me().user_name}/.{prefix}"
        default_cluster_id = env_or_skip("TEST_DEFAULT_CLUSTER_ID")
        tacl_cluster_id = env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID")
        Threads.strict(
            "ensure clusters running",
            [
                functools.partial(ws.clusters.ensure_cluster_is_running, default_cluster_id),
                functools.partial(ws.clusters.ensure_cluster_is_running, tacl_cluster_id),
            ],
        )
        workspace_config = WorkspaceConfig(
            inventory_database=inventory_schema,
            log_level="DEBUG",
            renamed_group_prefix=renamed_group_prefix,
            workspace_start_path=workspace_start_path,
            override_clusters={"main": default_cluster_id, "tacl": tacl_cluster_id},
        )
        if config_transform:
            workspace_config = config_transform(workspace_config)
        installation = Installation(ws, prefix)
        installation.save(workspace_config)

        # TODO: see if we want to move building wheel as a context manager for yield factory,
        # so that we can shave off couple of seconds and build wheel only once per session
        # instead of every test
        wheels = WheelsV2(installation, PRODUCT_INFO)
        workspace_installation = WorkspaceInstallation(
            workspace_config,
            installation,
            sql_backend,
            wheels,
            ws,
            prompts,
            verify_timeout=timedelta(minutes=2),
        )
        workspace_installation.run()
        cleanup.append(workspace_installation)
        return workspace_installation

    yield factory

    for pending in cleanup:
        pending.uninstall()


@retried(on=[NotFound, Unknown, TimeoutError], timeout=timedelta(minutes=5))
def test_job_failure_propagates_correct_error_message_and_logs(ws, sql_backend, new_installation):
    install = new_installation()

    sql_backend.execute(f"DROP SCHEMA {install.config.inventory_database} CASCADE")

    with pytest.raises(NotFound) as failure:
        install.run_workflow("099-destroy-schema")

    assert "cannot be found" in str(failure.value)

    workflow_run_logs = list(ws.workspace.list(f"{install.folder}/logs"))
    assert len(workflow_run_logs) == 1


@retried(on=[NotFound, Unknown, InvalidParameterValue], timeout=timedelta(minutes=8))
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


@retried(on=[NotFound, Unknown, InvalidParameterValue], timeout=timedelta(minutes=5))
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
    inventory_database = install.config.inventory_database
    permission_manager = PermissionManager(sql_backend, inventory_database, [generic_permissions])
    permission_manager.inventorize_permissions()

    install.run_workflow("migrate-groups")

    found = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert found[acc_group_a.display_name] == PermissionLevel.CAN_USE
    assert found[f"{install.config.renamed_group_prefix}{ws_group_a.display_name}"] == PermissionLevel.CAN_USE


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_real_remove_backup_groups_job(ws, sql_backend, new_installation, make_ucx_group):
    ws_group_a, acc_group_a = make_ucx_group()

    install = new_installation(lambda wc: replace(wc, include_group_names=[ws_group_a.display_name]))
    cfg = install.config
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
    sql_backend.execute(f"DROP SCHEMA {install.config.inventory_database} CASCADE")
    with pytest.raises(NotFound):
        install.run_workflow("099-destroy-schema")

    sql_backend.execute(f"CREATE SCHEMA IF NOT EXISTS {install.config.inventory_database}")

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
    assessment_job_id = install._state.jobs["assessment"]
    install.uninstall()
    with pytest.raises(NotFound):
        ws.workspace.get_status(install.folder)
    with pytest.raises(InvalidParameterValue):
        ws.jobs.get(job_id=assessment_job_id)
    with pytest.raises(NotFound):
        sql_backend.execute(f"show tables from hive_metastore.{install.config.inventory_database}")
