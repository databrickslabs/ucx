import io
import json
import logging
import time
from pathlib import Path
from unittest.mock import create_autospec, patch, Mock

import pytest
import yaml
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.ucx.aws.credentials import IamRoleCreation
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.errors.platform import BadRequest
from databricks.sdk.service import jobs, sql
from databricks.sdk.service.catalog import ExternalLocationInfo, MetastoreInfo
from databricks.sdk.service.compute import ClusterDetails, ClusterSource
from databricks.sdk.service.iam import ComplexValue, User
from databricks.sdk.service.jobs import Run, RunResultState, RunState
from databricks.sdk.service.provisioning import Workspace
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, ObjectInfo, ObjectType

from databricks.labs.ucx.assessment.aws import AWSResources, AWSRoleAction
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.resources import AzureResource, AzureResources, StorageAccount
from databricks.labs.ucx.cli import (
    alias,
    assign_metastore,
    cluster_remap,
    create_account_groups,
    create_catalogs_schemas,
    create_missing_principals,
    create_table_mapping,
    create_uber_principal,
    create_ucx_catalog,
    download,
    ensure_assessment_run,
    installations,
    join_collection,
    logs,
    manual_workspace_info,
    migrate_acls,
    migrate_credentials,
    migrate_dbsql_dashboards,
    migrate_local_code,
    migrate_locations,
    migrate_tables,
    move,
    open_remote_config,
    principal_prefix_access,
    repair_run,
    revert_cluster_remap,
    revert_dbsql_dashboards,
    revert_migrated_tables,
    show_all_metastores,
    skip,
    sync_workspace_info,
    upload,
    validate_external_locations,
    validate_groups_membership,
    workflows,
    delete_missing_principals,
    export_assessment,
)
from databricks.labs.ucx.contexts.account_cli import AccountContext
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext
from databricks.labs.ucx.hive_metastore import TablesCrawler, ExternalLocations
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.source_code.linters.files import LocalFileMigrator


def create_workspace_client_mock(workspace_id: int) -> WorkspaceClient:
    # This function is meant to cover the setup for the tests below, it is not intended to be more flexibile than that.
    # If you want to create mocks with other workspace ids, please update the list below with **all** workspace ids
    # used by the tests
    installed_workspace_ids = [123, 456]
    assert workspace_id in installed_workspace_ids

    state = {
        "/Users/foo/.ucx/config.yml": yaml.dump(
            {
                'version': 2,
                'inventory_database': 'ucx',
                'warehouse_id': 'test',
                'connect': {
                    'host': 'foo',
                    'token': 'bar',
                },
                'installed_workspace_ids': installed_workspace_ids,
                'policy_id': '01234567A8BCDEF9',
                # Exit Azure's `create_uber_principal` early by setting the uber service principal id
                # to isolate cli testing as much as possible to the cli commands and not the invoked ucx functionality.
                'uber_spn_id': '0123456789',
            }
        ),
        '/Users/foo/.ucx/state.json': json.dumps(
            {
                'resources': {
                    'jobs': {
                        'assessment': '123',
                        'migrate-tables': '456',
                        'migrate-external-hiveserde-tables-in-place-experimental': '789',
                        'migrate-external-tables-ctas': '987',
                    }
                }
            }
        ),
        '/Users/foo/.ucx/workspaces.json': json.dumps(
            [
                {'workspace_id': 123, 'workspace_name': '123'},
                {'workspace_id': 456, 'workspace_name': '456'},
            ]
        ),
        "/Users/foo/.ucx/uc_roles_access.csv": "role_arn,resource_type,privilege,resource_path\n"
        "arn:aws:iam::123456789012:role/role_name,s3,READ_FILES,s3://labsawsbucket/",
        "/Users/foo/.ucx/azure_storage_account_info.csv": "prefix,client_id,principal,privilege,type,directory_id\ntest,test,test,test,Application,test",
        "/Users/foo/.ucx/mapping.csv": "workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table\ntest,test,test,test,test,test",
        "/Users/foo/.ucx/logs/run-123-1/foo.log-123": """18:59:17 INFO [databricks.labs.ucx] {MainThread} Something is logged
18:59:17 DEBUG [databricks.labs.ucx.framework.crawlers] {MainThread} [hive_metastore.ucx_serge.clusters] fetching clusters inventory
18:59:17 DEBUG [databricks.labs.lsql.backends] {MainThread} [spark][fetch] SELECT * FROM ucx_serge.clusters
""",
    }

    def mock_download(path: str, **_) -> io.StringIO | io.BytesIO:
        if path not in state:
            raise NotFound(path)
        if ".csv" in path or ".log" in path:
            return io.BytesIO(state[path].encode('utf-8'))
        return io.StringIO(state[path])

    workspace_client = create_autospec(WorkspaceClient)
    workspace_client.get_workspace_id.return_value = workspace_id
    workspace_client.config.host = 'https://localhost'
    workspace_client.current_user.me.return_value = User(user_name="foo", groups=[ComplexValue(display="admins")])
    workspace_client.workspace.download.side_effect = mock_download
    workspace_client.statement_execution.execute_statement.return_value = sql.StatementResponse(
        status=sql.StatementStatus(state=sql.StatementState.SUCCEEDED),
        manifest=sql.ResultManifest(schema=sql.ResultSchema()),
        statement_id='123',
    )
    return workspace_client


@pytest.fixture
def ws() -> WorkspaceClient:
    return create_workspace_client_mock(123)


@pytest.fixture
def ws2() -> WorkspaceClient:
    return create_workspace_client_mock(456)


@pytest.fixture
def workspace_clients(ws, ws2) -> list[WorkspaceClient]:
    return [ws, ws2]


@pytest.fixture
def acc_client(acc_client: Mock, workspace_clients: list[WorkspaceClient]) -> Mock:
    acc_client.workspaces.get.side_effect = lambda workspace_id: Workspace(workspace_id=workspace_id)

    def get_workspace_client(workspace: Workspace) -> WorkspaceClient:
        workspace_client = [ws for ws in workspace_clients if ws.get_workspace_id() == workspace.workspace_id]
        if len(workspace_client) == 0:
            raise NotFound(f"Workspace not found: {workspace.workspace_id}")
        return workspace_client[0]

    acc_client.get_workspace_client.side_effect = get_workspace_client
    return acc_client


def test_workflow(ws, caplog):
    workflows(ws)
    assert "Fetching deployed jobs..." in caplog.messages
    ws.jobs.list_runs.assert_called()


def test_open_remote_config(ws):
    with patch("webbrowser.open") as mock_webbrowser_open:
        open_remote_config(ws)
        mock_webbrowser_open.assert_called_with('https://localhost/#workspace/Users/foo/.ucx/config.yml')


def test_installations(ws, capsys):
    ws.users.list.return_value = [User(user_name='foo')]
    installations(ws)
    assert '{"database": "ucx", "path": "/Users/foo/.ucx", "warehouse_id": "test"}' in capsys.readouterr().out


def test_skip_with_table(ws):
    skip(ws, "schema", "table")

    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id='test',
        statement="SELECT * FROM `hive_metastore`.`ucx`.`tables` WHERE database='schema' AND name='table' LIMIT 1",
        byte_limit=None,
        catalog=None,
        schema=None,
        disposition=None,
        format=sql.Format.JSON_ARRAY,
        wait_timeout=None,
    )


def test_skip_with_schema(ws):
    skip(ws, "schema", None)

    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id='test',
        statement="ALTER SCHEMA `schema` SET DBPROPERTIES('databricks.labs.ucx.skip' = true)",
        byte_limit=None,
        catalog=None,
        schema=None,
        disposition=None,
        format=sql.Format.JSON_ARRAY,
        wait_timeout=None,
    )


def test_skip_no_schema(ws, caplog):
    skip(ws, schema=None, table="table")

    assert '--schema is a required parameter.' in caplog.messages


def test_sync_workspace_info():
    a = create_autospec(AccountClient)
    sync_workspace_info(a)
    a.workspaces.list.assert_called()


@pytest.mark.parametrize("run_as_collection", [False, True])
def test_upload(tmp_path, workspace_clients, acc_client, run_as_collection):
    if not run_as_collection:
        workspace_clients = [workspace_clients[0]]
    test_file = tmp_path / "test.txt"
    content = b"test"
    test_file.write_bytes(content)

    upload(test_file, workspace_clients[0], run_as_collection=run_as_collection, a=acc_client)

    for workspace_client in workspace_clients:
        workspace_client.workspace.upload.assert_called_with(
            f"/Users/foo/.ucx/{test_file.name}",
            content,
            format=ImportFormat.AUTO,
            overwrite=True,
        )


def test_create_account_groups():
    a = create_autospec(AccountClient)
    w = create_autospec(WorkspaceClient)
    a.get_workspace_client.return_value = w
    w.get_workspace_id.return_value = None
    prompts = MockPrompts({})
    ctx = AccountContext(a).replace()
    create_account_groups(a, prompts, ctx=ctx)
    a.groups.list.assert_called_with(attributes="id")


def test_create_account_groups_with_id():
    a = create_autospec(AccountClient)
    w = create_autospec(WorkspaceClient)
    a.get_workspace_client.return_value = w
    w.get_workspace_id.return_value = None
    prompts = MockPrompts({})
    ctx = AccountContext(a, {"workspace_ids": "123,456"})
    with pytest.raises(ValueError, match="No workspace ids provided in the configuration found in the account"):
        create_account_groups(a, prompts, ctx=ctx)


def test_manual_workspace_info(ws):
    prompts = MockPrompts({'Workspace name for 123': 'abc', 'Next workspace id': ''})
    manual_workspace_info(ws, prompts)


def test_create_table_mapping_raises_value_error_because_no_tables_found(ws, acc_client) -> None:
    ctx = WorkspaceContext(ws)
    with pytest.raises(ValueError, match="No tables found. .*"):
        create_table_mapping(ws, ctx, False, acc_client)


def test_validate_external_locations(ws) -> None:
    validate_external_locations(ws, MockPrompts({}), ctx=WorkspaceContext(ws))
    ws.statement_execution.execute_statement.assert_called()


def test_validate_external_locations_runs_as_collection(workspace_clients, acc_client) -> None:
    validate_external_locations(
        workspace_clients[0],
        MockPrompts({}),
        run_as_collection=True,
        a=acc_client,
    )

    for workspace_client in workspace_clients:
        workspace_client.statement_execution.execute_statement.assert_called()


def test_ensure_assessment_run(ws, acc_client):
    ws.jobs.wait_get_run_job_terminated_or_skipped.return_value = Run(
        state=RunState(result_state=RunResultState.SUCCESS), start_time=0, end_time=1000, run_duration=1000
    )
    ensure_assessment_run(ws, a=acc_client)
    ws.jobs.list_runs.assert_called_once()
    ws.jobs.wait_get_run_job_terminated_or_skipped.assert_called_once()


def test_ensure_assessment_run_collection(workspace_clients, acc_client):
    ensure_assessment_run(workspace_clients[0], run_as_collection=True, a=acc_client)

    for workspace_client in workspace_clients:
        workspace_client.jobs.run_now.assert_called_with(123)


def test_repair_run(ws):
    repair_run(ws, "assessment")

    ws.jobs.list_runs.assert_called_once()


def test_no_step_in_repair_run(ws):
    with pytest.raises(KeyError):
        repair_run(ws, "")


def test_revert_migrated_tables(ws, caplog):
    # test with no schema and no table, user confirm to not retry
    prompts = MockPrompts({'.*': 'no'})
    ctx = WorkspaceContext(ws).replace(
        is_azure=True,
        azure_cli_authenticated=True,
        azure_subscription_ids=["test"],
        is_gcp=False,
    )
    assert revert_migrated_tables(ws, prompts, schema=None, table=None, ctx=ctx) is None

    # test with no schema and no table, user confirm to retry, but no ucx installation found
    prompts = MockPrompts({'.*': 'yes'})
    assert revert_migrated_tables(ws, prompts, schema=None, table=None, ctx=ctx) is None
    assert 'No migrated tables were found.' in caplog.messages


def test_move_no_catalog(ws, caplog):
    prompts = MockPrompts({})
    move(ws, prompts, "", "", "", "", "")

    assert 'Please enter from_catalog and to_catalog details' in caplog.messages


def test_move_same_schema(ws, caplog):
    prompts = MockPrompts({})
    move(ws, prompts, "SrcCat", "SrcS", "*", "SrcCat", "SrcS")

    assert 'please select a different schema or catalog to migrate to' in caplog.messages


def test_move_no_schema(ws, caplog):
    prompts = MockPrompts({})
    move(ws, prompts, "SrcCat", "", "*", "TgtCat", "")

    assert (
        'Please enter from_schema, to_schema and from_table (enter * for migrating all tables) details.'
        in caplog.messages
    )


def test_move(ws):
    prompts = MockPrompts({'.*': 'yes'})
    move(ws, prompts, "SrcC", "SrcS", "*", "TgtC", "ToS")

    ws.tables.list.assert_called_once()


def test_move_aborted_via_prompt(ws):
    prompts = MockPrompts({'.*tables will be dropped and recreated.*': 'no'})
    move(ws, prompts, "SrcC", "SrcS", "*", "TgtC", "ToS")

    ws.tables.list.assert_not_called()


def test_alias_no_catalog(ws, caplog):
    alias(ws, "", "", "", "", "")

    assert "Please enter from_catalog and to_catalog details" in caplog.messages


def test_alias_same_schema(ws, caplog):
    alias(ws, "SrcCat", "SrcS", "*", "SrcCat", "SrcS")

    assert 'please select a different schema or catalog to migrate to' in caplog.messages


def test_alias_no_schema(ws, caplog):
    alias(ws, "SrcCat", "", "*", "TgtCat", "")

    assert (
        'Please enter from_schema, to_schema and from_table (enter * for migrating all tables) details.'
        in caplog.messages
    )


def test_alias(ws):
    alias(ws, "SrcC", "SrcS", "*", "TgtC", "ToS")

    ws.tables.list.assert_called_once()


def test_save_storage_and_principal_azure_no_azure_cli(ws):
    ws.config.is_azure = True
    ws.config.is_aws = False
    ctx = WorkspaceContext(ws)
    with pytest.raises(ValueError):
        principal_prefix_access(ws, ctx, False)


def test_save_storage_and_principal_azure(ws, caplog, acc_client):
    azure_resource_permissions = create_autospec(AzureResourcePermissions)
    ws.config.is_azure = True
    ws.config.is_aws = False
    ctx = WorkspaceContext(ws).replace(azure_resource_permissions=azure_resource_permissions)
    principal_prefix_access(ws, ctx, False, a=acc_client)
    azure_resource_permissions.save_spn_permissions.assert_called_once()


@pytest.mark.parametrize("run_as_collection", [True, False])
def test_validate_groups_membership_lists_groups(
    run_as_collection,
    workspace_clients,
    acc_client,
) -> None:
    if not run_as_collection:
        workspace_clients = [workspace_clients[0]]
    validate_groups_membership(
        workspace_clients[0],
        run_as_collection=run_as_collection,
        a=acc_client,
    )
    for workspace_client in workspace_clients:
        workspace_client.groups.list.assert_called()


def test_save_storage_and_principal_aws(ws, acc_client):
    aws_resource_permissions = create_autospec(AWSResourcePermissions)
    ws.config.is_azure = False
    ws.config.is_aws = True
    ctx = WorkspaceContext(ws).replace(aws_resource_permissions=aws_resource_permissions)
    principal_prefix_access(ws, ctx=ctx, a=acc_client)
    aws_resource_permissions.save_instance_profile_permissions.assert_called_once()


def test_save_storage_and_principal_gcp(ws):
    ws.config.is_azure = False
    ws.config.is_aws = False
    ctx = WorkspaceContext(ws)
    with pytest.raises(ValueError):
        principal_prefix_access(ws, ctx=ctx)


@pytest.mark.parametrize("run_as_collection", [True, False])
def test_migrate_acls_calls_workspace_id(
    run_as_collection,
    workspace_clients,
    acc_client,
) -> None:
    if not run_as_collection:
        workspace_clients = [workspace_clients[0]]
    migrate_acls(
        workspace_clients[0],
        run_as_collection=run_as_collection,
        a=acc_client,
    )
    for workspace_client in workspace_clients:
        workspace_client.get_workspace_id.assert_called()


def test_migrate_credentials_azure(ws, acc_client):
    ws.config.is_azure = True
    ws.workspace.upload.return_value = "test"
    prompts = MockPrompts({'.*': 'yes'})
    azure_resources = create_autospec(AzureResources)
    ctx = WorkspaceContext(ws).replace(
        is_azure=True,
        azure_cli_authenticated=True,
        azure_subscription_ids=["test"],
        azure_resources=azure_resources,
    )
    migrate_credentials(ws, prompts, ctx=ctx, a=acc_client)
    ws.storage_credentials.list.assert_called()
    azure_resources.storage_accounts.assert_called()


def test_migrate_credentials_aws(ws, acc_client):
    ws.config.is_azure = False
    ws.config.is_aws = True
    aws_resources = create_autospec(AWSResources)
    aws_resources.validate_connection.return_value = {"Account": "123456789012"}
    prompts = MockPrompts({'.*': 'yes'})
    ctx = WorkspaceContext(ws).replace(is_aws=True, aws_resources=aws_resources)
    migrate_credentials(ws, prompts, ctx=ctx, a=acc_client)
    ws.storage_credentials.list.assert_called()


def test_migrate_credentials_raises_runtime_warning_when_hitting_storage_credential_limit(ws, acc_client):
    """The storage credential limit is 200, so we should raise a warning when we hit that limit."""
    ws.config.is_azure = True
    azure_resources = create_autospec(AzureResources)
    external_locations = create_autospec(ExternalLocations)
    storage_accounts_mock, external_locations_mock = [], []
    for i in range(200):
        storage_account_id = AzureResource(
            f"/subscriptions/test/resourceGroups/test/providers/Microsoft.Storage/storageAccounts/storage{i}"
        )
        storage_account = StorageAccount(
            id=storage_account_id,
            name=f"storage{i}",
            location="eastus",
            default_network_action="Allow",
        )
        external_location = ExternalLocation(
            location=f"abfss://container{i}@storage{i}.dfs.core.windows.net/folder{i}",
            table_count=i % 20,
        )
        storage_accounts_mock.append(storage_account)
        external_locations_mock.append(external_location)
    azure_resources.storage_accounts.return_value = storage_accounts_mock
    external_locations.snapshot.return_value = external_locations_mock
    prompts = MockPrompts({'.*': 'yes'})
    ctx = WorkspaceContext(ws).replace(
        is_azure=True,
        azure_cli_authenticated=True,
        azure_subscription_ids=["test"],
        azure_resources=azure_resources,
        external_locations=external_locations,
    )
    migrate_credentials(ws, prompts, ctx=ctx, a=acc_client)
    ws.storage_credentials.list.assert_called()
    azure_resources.storage_accounts.assert_called()

    storage_account_id = AzureResource(
        "/subscriptions/test/resourceGroups/test/providers/Microsoft.Storage/storageAccounts/storage201"
    )
    storage_account = StorageAccount(
        id=storage_account_id,
        name="storage201",
        location="eastus",
        default_network_action="Allow",
    )
    external_location = ExternalLocation(
        location=f"abfss://container{201}@storage{201}.dfs.core.windows.net/folder{201}", table_count=25
    )
    storage_accounts_mock.append(storage_account)
    external_locations_mock.append(external_location)
    with pytest.raises(RuntimeWarning):
        migrate_credentials(ws, prompts, ctx=ctx, a=acc_client)


def test_migrate_credentials_limit_aws(ws, acc_client):
    ws.config.is_azure = False
    ws.config.is_aws = True
    aws_resources = create_autospec(AWSResources)
    external_locations = create_autospec(ExternalLocations)

    external_locations_mock = []
    aws_role_actions_mock = []
    for i in range(200):
        location = f"s3://labsawsbucket/{i}"
        role = f"arn:aws:iam::123456789012:role/role_name{i}"
        external_locations_mock.append(ExternalLocation(location=location, table_count=i % 20))
        aws_role_actions_mock.append(
            AWSRoleAction(
                role_arn=role,
                privilege="READ_FILES",
                resource_path=location,
                resource_type="s3",
            )
        )
    external_locations.snapshot.return_value = external_locations_mock
    aws_resources.validate_connection.return_value = {"Account": "123456789012"}

    prompts = MockPrompts({'.*': 'yes'})
    AWSResourcePermissions.load_uc_compatible_roles = Mock()
    AWSResourcePermissions.load_uc_compatible_roles.return_value = aws_role_actions_mock
    ctx = WorkspaceContext(ws).replace(is_aws=True, aws_resources=aws_resources, external_locations=external_locations)
    migrate_credentials(ws, prompts, ctx=ctx, a=acc_client)
    ws.storage_credentials.list.assert_called()

    external_locations_mock.append(ExternalLocation(location="s3://labsawsbucket/201", table_count=25))
    aws_role_actions_mock.append(
        AWSRoleAction(
            role_arn="arn:aws:iam::123456789012:role/role_name201",
            privilege="READ_FILES",
            resource_path="s3://labsawsbucket/201",
            resource_type="s3",
        )
    )
    with pytest.raises(RuntimeWarning):
        migrate_credentials(ws, prompts, ctx=ctx, a=acc_client)


def test_create_uber_principal_raises_value_error_for_unsupported_cloud(ws) -> None:
    ctx = WorkspaceContext(ws).replace(
        is_azure=False,
        is_aws=False,
    )
    prompts = MockPrompts({})
    with pytest.raises(ValueError, match="Unsupported cloud provider"):
        create_uber_principal(ws, prompts, ctx=ctx)


def test_create_azure_uber_principal_raises_value_error_if_subscription_id_is_missing(ws) -> None:
    ctx = WorkspaceContext(ws).replace(
        is_azure=True,
        is_aws=False,
        azure_cli_authenticated=True,
    )
    prompts = MockPrompts({"Enter a name for the uber service principal to be created": "test"})
    with pytest.raises(ValueError, match="Please enter subscription ids to scan storage accounts in."):
        create_uber_principal(ws, prompts, ctx=ctx)


def test_create_azure_uber_principal_calls_workspace_id(ws) -> None:
    ctx = WorkspaceContext(ws).replace(
        is_azure=True,
        is_aws=False,
        azure_cli_authenticated=True,
        azure_subscription_ids=["id"],
    )
    prompts = MockPrompts({"Enter a name for the uber service principal to be created": "test"})

    create_uber_principal(ws, prompts, ctx=ctx)

    ws.get_workspace_id.assert_called_once()


def test_create_azure_uber_principal_runs_as_collection_requests_workspace_ids(workspace_clients, acc_client) -> None:
    for workspace_client in workspace_clients:
        # Setting the auth as follows as we (currently) do not support injecting multiple workspace contexts
        workspace_client.config.auth_type = "azure-cli"
    prompts = MockPrompts({"Enter a name for the uber service principal to be created": "test"})

    create_uber_principal(
        workspace_clients[0],
        prompts,
        run_as_collection=True,
        a=acc_client,
        subscription_ids="test",
    )

    for workspace_client in workspace_clients:
        workspace_client.get_workspace_id.assert_called()


def test_create_aws_uber_principal_raises_value_error_if_aws_profile_is_missing(ws) -> None:
    ctx = WorkspaceContext(ws).replace(
        is_azure=False,
        is_aws=True,
    )
    prompts = MockPrompts({})
    with pytest.raises(ValueError, match="AWS Profile is not specified. .*"):
        create_uber_principal(ws, prompts, ctx=ctx)


def successful_aws_cli_call(_):
    successful_return = """
    {
        "UserId": "uu@mail.com",
        "Account": "1234",
        "Arn": "arn:aws:sts::1234:assumed-role/AWSVIEW/uu@mail.com"
    }
    """
    return 0, successful_return, ""


def test_create_aws_uber_principal_calls_dbutils_fs_mounts(ws) -> None:
    ctx = WorkspaceContext(ws).replace(
        is_azure=False,
        is_aws=True,
        aws_profile="test",
        aws_cli_run_command=successful_aws_cli_call,
    )
    prompts = MockPrompts({})
    create_uber_principal(ws, prompts, ctx=ctx)
    ws.dbutils.fs.mounts.assert_called_once()


def test_migrate_locations_raises_value_error_for_unsupported_cloud_provider(ws) -> None:
    ctx = WorkspaceContext(ws).replace(is_azure=False, is_aws=False)
    with pytest.raises(ValueError, match="Unsupported cloud provider"):
        migrate_locations(ws, ctx=ctx)


def test_migrate_locations_azure(ws) -> None:
    azurerm = create_autospec(AzureResources)
    ctx = WorkspaceContext(ws).replace(
        is_azure=True,
        is_aws=False,
        azure_cli_authenticated=True,
        azure_subscription_ids=["test"],
        azure_resources=azurerm,
    )

    migrate_locations(ws, ctx=ctx)

    ws.external_locations.list.assert_called()
    azurerm.storage_accounts.assert_called()


@pytest.mark.xfail(reason="Currently not supported in unit tests see TODO")
def test_migrate_locations_azure_run_as_collection(workspace_clients, acc_client) -> None:
    """Test migrate locations for a collection of workspaces

    The "run as collection" test should run the same as the test `test_migrate_locations_azure`, but the assert should
    be called on **all** workspace clients.
    """
    for workspace_client in workspace_clients:
        # Setting the auth as follows as we (currently) do not support injecting multiple workspace contexts
        workspace_client.config.is_aws = False
        workspace_client.config.auth_type = "azure-cli"

    # TODO: Migrate locations fails in unit testing as we currently not support injecting multiple workspace contexts
    # thus we cannot mock azure-cli login for multiple workspaces.
    # If we support this in the future, the `pytest.raises` can be removed and the test should pass
    with pytest.raises(OSError, match=".*The provided request must include a 'scope' input parameter.*"):
        migrate_locations(
            workspace_clients[0],
            run_as_collection=True,
            a=acc_client,
            subscription_ids=["test"],
        )

    for workspace_client in workspace_clients:
        workspace_client.external_locations.list.assert_called()


def test_migrate_locations_aws(ws, caplog) -> None:
    ctx = WorkspaceContext(ws).replace(
        is_aws=True,
        is_azure=False,
        aws_profile="profile",
        aws_cli_run_command=successful_aws_cli_call,
    )

    migrate_locations(ws, ctx=ctx)

    ws.external_locations.list.assert_called()


@pytest.mark.xfail(reason="Currently not supported in unit tests see TODO")
def test_migrate_locations_run_as_collection(workspace_clients, acc_client) -> None:
    """Test migrate locations for a collection of workspaces

    The "run as collection" test should run the same as the test `test_migrate_locations_aws`, but the assert should
    be called on **all** workspace clients.
    """
    for workspace_client in workspace_clients:
        # Setting the auth as follows as we (currently) do not support injecting multiple workspace contexts
        workspace_client.config.is_azure = False

    # TODO: Migrate locations fails in unit testing as we currently not support injecting multiple workspace contexts
    # thus we cannot mock AWS login for multiple workspaces.
    # If we support this in the future, the `pytest.raises` can be removed and the test should pass
    with pytest.raises(AttributeError, match="'NoneType' object has no attribute 'get'"):
        migrate_locations(
            workspace_clients[0],
            run_as_collection=True,
            a=acc_client,
            aws_profile="profile",
        )

    for workspace_client in workspace_clients:
        workspace_client.external_locations.list.assert_called()


def test_migrate_locations_gcp(ws):
    ctx = WorkspaceContext(ws).replace(is_aws=False, is_azure=False)
    with pytest.raises(ValueError):
        migrate_locations(ws, ctx=ctx)


@pytest.mark.parametrize("run_as_collection", [False, True])
def test_create_catalogs_schemas_lists_catalogs(run_as_collection, workspace_clients, acc_client) -> None:
    if not run_as_collection:
        workspace_clients = [workspace_clients[0]]
    for workspace_client in workspace_clients:
        workspace_client.external_locations.list.return_value = [ExternalLocationInfo(url="s3://test")]
    prompts = MockPrompts({'.*': 's3://test'})

    create_catalogs_schemas(workspace_clients[0], prompts, run_as_collection=run_as_collection, a=acc_client)

    for workspace_client in workspace_clients:
        workspace_client.catalogs.list.assert_called_once()


def test_create_catalogs_schemas_handles_existing(ws, caplog) -> None:
    prompts = MockPrompts({'.*': 's3://test'})
    ws.external_locations.list.return_value = [ExternalLocationInfo(url="s3://test")]
    ws.catalogs.create.side_effect = [BadRequest("Catalog 'test' already exists")]
    ws.schemas.create.side_effect = [BadRequest("Schema 'test' already exists")]
    create_catalogs_schemas(ws, prompts, ctx=WorkspaceContext(ws))
    ws.catalogs.list.assert_called_once()

    assert "Catalog 'test' already exists. Skipping." in caplog.messages
    assert "Schema 'test' in catalog 'test' already exists. Skipping." in caplog.messages


def test_cluster_remap(ws, caplog):
    prompts = MockPrompts({"Please provide the cluster id's as comma separated value from the above list.*": "1"})
    ws.clusters.get.return_value = ClusterDetails(cluster_id="123", cluster_name="test_cluster")
    ws.clusters.list.return_value = [
        ClusterDetails(cluster_id="123", cluster_name="test_cluster", cluster_source=ClusterSource.UI),
        ClusterDetails(cluster_id="1234", cluster_name="test_cluster1", cluster_source=ClusterSource.JOB),
    ]
    cluster_remap(ws, prompts)
    assert "Remapping the Clusters to UC" in caplog.messages


def test_cluster_remap_error(ws, caplog):
    prompts = MockPrompts({"Please provide the cluster id's as comma separated value from the above list.*": "1"})
    ws.clusters.list.return_value = []
    cluster_remap(ws, prompts)
    assert "No cluster information present in the workspace" in caplog.messages


def test_revert_cluster_remap(caplog):
    # TODO: What is this test supposed to test? Why do we expect a TypeError below?
    prompts = MockPrompts({"Please provide the cluster id's as comma separated value from the above list.*": "1"})
    workspace_client = create_autospec(WorkspaceClient)
    workspace_client.workspace.list.return_value = [ObjectInfo(path='/ucx/backup/clusters/123.json')]
    with pytest.raises(TypeError):
        revert_cluster_remap(workspace_client, prompts)


def test_revert_cluster_remap_empty(ws, caplog):
    prompts = MockPrompts({"Please provide the cluster id's as comma separated value from the above list.*": "1"})
    revert_cluster_remap(ws, prompts)
    assert "There is no cluster files in the backup folder. Skipping the reverting process" in caplog.messages
    ws.workspace.list.assert_called_once()


def test_relay_logs(ws, caplog):
    ws.jobs.list_runs.return_value = [jobs.BaseRun(run_id=123, start_time=int(time.time()))]
    ws.workspace.list.side_effect = [
        [
            ObjectInfo(path='/Users/foo/.ucx/logs/run-123-0', object_type=ObjectType.DIRECTORY),
            ObjectInfo(path='/Users/foo/.ucx/logs/run-123-1', object_type=ObjectType.DIRECTORY),
        ],
        [ObjectInfo(path='/Users/foo/.ucx/logs/run-123-1/foo.log-123')],
    ]
    logs(ws)
    assert 'Something is logged' in caplog.messages


def test_migrate_local_code(ws):
    prompts = MockPrompts({'.*': 'yes'})
    with patch.object(LocalFileMigrator, 'apply') as mock_apply:
        migrate_local_code(ws, prompts)

        mock_apply.assert_called_once_with(Path.cwd())


def test_migrate_local_code_aborted_via_prompt(ws):
    prompts = MockPrompts({'.*apply UC migration to all files.*': 'no'})
    with patch.object(LocalFileMigrator, 'apply') as mock_apply:
        migrate_local_code(ws, prompts)

        mock_apply.assert_not_called()


def test_show_all_metastores(acc_client, caplog):
    show_all_metastores(acc_client)
    assert 'Matching metastores are:' in caplog.messages


def test_assign_metastore_logs_account_id_and_assigns_metastore(caplog, acc_client) -> None:
    ctx = AccountContext(acc_client)
    acc_client.metastores.list.return_value = [MetastoreInfo(name="test", metastore_id="123")]

    with caplog.at_level(logging.INFO, logger="databricks.labs.ucx.cli"):
        assign_metastore(acc_client, "456", ctx=ctx)

    assert "Account ID: 123" in caplog.messages
    acc_client.metastore_assignments.create.assert_called_once()


def test_create_ucx_catalog_calls_create_catalog(ws) -> None:
    prompts = MockPrompts({"Please provide storage location url for catalog: .*": "metastore"})

    create_ucx_catalog(ws, prompts, ctx=WorkspaceContext(ws))

    ws.catalogs.create.assert_called_once()


def test_create_ucx_catalog_creates_history_schema_and_table(ws, mock_backend) -> None:
    prompts = MockPrompts({"Please provide storage location url for catalog: .*": "metastore"})

    create_ucx_catalog(ws, prompts, ctx=WorkspaceContext(ws).replace(sql_backend=mock_backend))

    assert len(mock_backend.queries) > 0, "No queries executed on backend"
    assert "CREATE SCHEMA" in mock_backend.queries[0]


@pytest.mark.parametrize("run_as_collection", [False, True])
def test_migrate_tables_calls_migrate_table_job_run_now(
    run_as_collection,
    workspace_clients,
    acc_client,
) -> None:
    if not run_as_collection:
        workspace_clients = [workspace_clients[0]]
    run = Run(
        state=RunState(result_state=RunResultState.SUCCESS),
        start_time=0,
        end_time=1000,
        run_duration=1000,
    )
    for workspace_client in workspace_clients:
        workspace_client.jobs.wait_get_run_job_terminated_or_skipped.return_value = run

    migrate_tables(workspace_clients[0], MockPrompts({}), run_as_collection=run_as_collection, a=acc_client)

    for workspace_client in workspace_clients:
        workspace_client.jobs.run_now.assert_called_with(456)
        workspace_client.jobs.wait_get_run_job_terminated_or_skipped.assert_called_once()


def test_migrate_tables_calls_external_hiveserde_tables_job_run_now(ws) -> None:
    # TODO: Test for running on a collection when context injection for multiple workspaces is supported.
    tables_crawler = create_autospec(TablesCrawler)
    table = Table(
        catalog="hive_metastore",
        database="test",
        name="hiveserde",
        object_type="UNKNOWN",
        table_format="HIVE",
    )
    tables_crawler.snapshot.return_value = [table]
    ctx = WorkspaceContext(ws).replace(tables_crawler=tables_crawler)
    ws.jobs.wait_get_run_job_terminated_or_skipped.return_value = Run(
        state=RunState(result_state=RunResultState.SUCCESS),
        start_time=0,
        end_time=1000,
        run_duration=1000,
    )

    prompt = (
        "Found 1 (.*) hiveserde tables in https://localhost, do you want to run the "
        "`migrate-external-hiveserde-tables-in-place-experimental` workflow?"
    )
    prompts = MockPrompts({prompt: "Yes"})

    migrate_tables(ws, prompts, ctx=ctx)

    ws.jobs.run_now.assert_called_with(789)
    ws.jobs.wait_get_run_job_terminated_or_skipped.call_count = 2


def test_migrate_tables_calls_external_tables_ctas_job_run_now(ws) -> None:
    # TODO: Test for running on a collection when context injection for multiple workspaces is supported.
    tables_crawler = create_autospec(TablesCrawler)
    table = Table(
        catalog="hive_metastore",
        database="test",
        name="externalctas",
        object_type="UNKNOWN",
        table_format="EXTERNAL",
    )
    tables_crawler.snapshot.return_value = [table]
    ctx = WorkspaceContext(ws).replace(tables_crawler=tables_crawler)
    ws.jobs.wait_get_run_job_terminated_or_skipped.return_value = Run(
        state=RunState(result_state=RunResultState.SUCCESS),
        start_time=0,
        end_time=1000,
        run_duration=1000,
    )

    prompt = (
        "Found 1 (.*) external tables which cannot be migrated using sync in https://localhost, do you want to run the "
        "`migrate-external-tables-ctas` workflow?"
    )

    prompts = MockPrompts({prompt: "Yes"})

    migrate_tables(ws, prompts, ctx=ctx)

    ws.jobs.run_now.assert_called_with(987)
    ws.jobs.wait_get_run_job_terminated_or_skipped.call_count = 2


def test_create_missing_principal_aws(ws, acc_client):
    ws.config.is_aws = True
    aws_resource_permissions = create_autospec(AWSResourcePermissions)
    ctx = WorkspaceContext(ws).replace(is_aws=True, is_azure=False, aws_resource_permissions=aws_resource_permissions)
    prompts = MockPrompts({'.*': 'yes'})
    create_missing_principals(ws, prompts=prompts, ctx=ctx, a=acc_client)
    aws_resource_permissions.create_uc_roles.assert_called_once()


def test_create_missing_principal_aws_not_approved(ws, acc_client):
    ws.config.is_aws = True
    aws_resource_permissions = create_autospec(AWSResourcePermissions)
    ctx = WorkspaceContext(ws).replace(is_aws=True, is_azure=False, aws_resource_permissions=aws_resource_permissions)
    prompts = MockPrompts({'.*': 'No'})
    create_missing_principals(ws, prompts=prompts, ctx=ctx, a=acc_client)
    aws_resource_permissions.create_uc_roles.assert_not_called()


def test_create_missing_principal_azure(ws, caplog, acc_client):
    ws.config.is_aws = False
    ctx = WorkspaceContext(ws).replace(is_aws=False, is_azure=True)
    prompts = MockPrompts({'.*': 'yes'})
    with pytest.raises(ValueError) as failure:
        create_missing_principals(ws, prompts=prompts, ctx=ctx, a=acc_client)
    assert str(failure.value) == "Unsupported cloud provider"


@pytest.mark.parametrize("run_as_collection", [False, True])
def test_migrate_dbsql_dashboards_list_dashboards(
    run_as_collection,
    workspace_clients,
    acc_client,
) -> None:
    if not run_as_collection:
        workspace_clients = [workspace_clients[0]]
    migrate_dbsql_dashboards(
        workspace_clients[0],
        run_as_collection=run_as_collection,
        a=acc_client,
    )
    for workspace_client in workspace_clients:
        workspace_client.dashboards.list.assert_called_once()


def test_revert_dbsql_dashboards(ws, caplog):
    revert_dbsql_dashboards(ws)
    ws.dashboards.list.assert_called_once()


def test_cli_missing_awscli(ws, mocker, caplog):
    mocker.patch("shutil.which", side_effect=ValueError("Couldn't find AWS CLI in path"))
    with pytest.raises(ValueError):
        ctx = WorkspaceContext(ws).replace(is_aws=True, is_azure=False, aws_profile="profile")
        migrate_locations(ws, ctx)


def test_join_collection():
    a = create_autospec(AccountClient)
    w = create_autospec(WorkspaceClient)
    a.get_workspace_client.return_value = w
    a.workspaces.list.return_value = [Workspace(workspace_id=123, deployment_name="test")]
    w.workspace.download.return_value = io.StringIO(json.dumps([{"workspace_id": 123, "workspace_name": "some"}]))
    join_collection(a, "123")
    w.workspace.download.assert_not_called()


def test_download_raises_value_error_if_not_downloading_a_csv(ws):
    with pytest.raises(ValueError) as e:
        download(Path("test.txt"), ws)
    assert "Command only supported for CSV files" in str(e)


@pytest.mark.parametrize("run_as_collection", [False, True])
def test_download_calls_workspace_download(tmp_path, workspace_clients, acc_client, run_as_collection):
    if not run_as_collection:
        workspace_clients = [workspace_clients[0]]

    download(
        tmp_path / "test.csv",
        workspace_clients[0],
        run_as_collection=run_as_collection,
        a=acc_client,
    )

    for workspace_client in workspace_clients:
        workspace_client.workspace.download.assert_called_with(
            "/Users/foo/.ucx/test.csv",
            format=ExportFormat.AUTO,
        )


def test_download_warns_if_file_not_found(caplog, ws, acc_client):
    ws.workspace.download.side_effect = NotFound("test.csv")
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.cli"):
        download(
            Path("test.csv"),
            ws,
            run_as_collection=False,
            a=acc_client,
        )
    assert "File not found for https://localhost: /Users/foo/.ucx/test.csv" in caplog.messages
    assert "No file(s) to download found" in caplog.messages


def test_download_deletes_empty_file(tmp_path, ws, acc_client):
    ws.workspace.download.side_effect = NotFound("test.csv")
    mapping_path = tmp_path / "mapping.csv"
    download(
        mapping_path,
        ws,
        run_as_collection=False,
        a=acc_client,
    )
    assert not mapping_path.is_file()


def test_download_has_expected_content(tmp_path, workspace_clients, acc_client):
    expected = (
        "workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table"
        "\ntest,test,test,test,test,test"
        "\ntest,test,test,test,test,test"
    )
    mapping_path = tmp_path / "mapping.csv"

    download(
        mapping_path,
        workspace_clients[0],
        run_as_collection=True,
        a=acc_client,
    )

    content = mapping_path.read_text()
    assert content == expected


def test_delete_principals(ws):
    ws.config.is_azure = False
    ws.config.is_aws = True
    role_creation = create_autospec(IamRoleCreation)
    ctx = WorkspaceContext(ws).replace(iam_role_creation=role_creation, workspace_client=ws)
    prompts = MockPrompts({"Select the list of roles *": "0"})
    delete_missing_principals(ws, prompts, ctx)
    role_creation.delete_uc_roles.assert_called_once()


def test_export_assessment(ws, tmp_path):
    query_choice = {"assessment_name": "main", "option": 3}
    mock_prompts = MockPrompts(
        {
            "Choose a path to save the UCX Assessment results": tmp_path.as_posix(),
            "Choose which assessment results to export": query_choice["option"],
        }
    )

    export_assessment(ws, mock_prompts)
    # Construct the expected filename based on the query_choice
    expected_filename = f"export_{query_choice['assessment_name']}_results.zip"
    # Assert that the file exists in the temporary path
    assert len(list(tmp_path.glob(expected_filename))) == 1
