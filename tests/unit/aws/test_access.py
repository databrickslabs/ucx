import json
import logging
from unittest import mock
from unittest.mock import MagicMock, call, create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service import iam
from databricks.sdk.service.catalog import AwsIamRoleResponse, ExternalLocationInfo, StorageCredentialInfo
from databricks.sdk.service.compute import InstanceProfile, Policy
from databricks.sdk.service.sql import EndpointConfPair, GetWorkspaceWarehouseConfigResponse

from databricks.labs.ucx.assessment.aws import AWSPolicyAction, AWSResources, AWSRole, AWSRoleAction
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.aws.credentials import IamRoleCreation
from databricks.labs.ucx.aws.locations import AWSExternalLocationsMigration
from databricks.labs.ucx.hive_metastore import ExternalLocations
from databricks.labs.ucx.hive_metastore.grants import PrincipalACL
from tests.unit import DEFAULT_CONFIG


@pytest.fixture
def mock_ws():
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.instance_profiles.list.return_value = [
        InstanceProfile("arn:aws:iam::12345:instance-profile/role1", "arn:aws:iam::12345:role/role1")
    ]
    return ws


@pytest.fixture
def mock_installation():
    installation = MockInstallation(DEFAULT_CONFIG | {"uc_roles_access.csv": []})
    return installation


@pytest.fixture
def installation_single_role():
    return MockInstallation(
        DEFAULT_CONFIG
        | {
            "uc_roles_access.csv": [
                {
                    "role_arn": "arn:aws:iam::12345:role/uc-role1",
                    "resource_type": "s3",
                    "privilege": "WRITE_FILES",
                    "resource_path": "s3://BUCKETX/*",
                }
            ]
        }
    )


@pytest.fixture
def installation_multiple_roles():
    return MockInstallation(
        DEFAULT_CONFIG
        | {
            "uc_roles_access.csv": [
                {
                    "role_arn": "arn:aws:iam::12345:role/uc-role1",
                    "resource_type": "s3",
                    "privilege": "WRITE_FILES",
                    "resource_path": "s3://BUCKET1",
                },
                {
                    "role_arn": "arn:aws:iam::12345:role/uc-role1",
                    "resource_type": "s3",
                    "privilege": "WRITE_FILES",
                    "resource_path": "s3://BUCKET2",
                },
                {
                    "role_arn": "arn:aws:iam::12345:role/uc-rolex",
                    "resource_type": "s3",
                    "privilege": "WRITE_FILES",
                    "resource_path": "s3://BUCKETX",
                },
            ]
        }
    )


@pytest.fixture
def installation_no_roles():
    return MockInstallation(DEFAULT_CONFIG | {"uc_roles_access.csv": []})


@pytest.fixture
def backend():
    rows = {
        "external_locations": [["s3://BUCKET1/FOLDER1", 1], ["s3://BUCKET2/FOLDER2", 1], ["s3://BUCKETX/FOLDERX", 1]]
    }
    return MockBackend(rows=rows, fails_on_first={})


@pytest.fixture
def locations(mock_ws, backend):
    return ExternalLocations(mock_ws, backend, "ucx")


def test_create_external_locations(mock_ws, installation_multiple_roles, backend, locations):
    mock_ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(
            id="1",
            name="cred1",
            aws_iam_role=AwsIamRoleResponse("arn:aws:iam::12345:role/uc-role1"),
        ),
        StorageCredentialInfo(
            id="2",
            name="credx",
            aws_iam_role=AwsIamRoleResponse("arn:aws:iam::12345:role/uc-rolex"),
        ),
    ]
    aws = create_autospec(AWSResources)
    aws_resource_permissions = AWSResourcePermissions(installation_multiple_roles, mock_ws, aws, locations)
    principal_acl = create_autospec(PrincipalACL)
    external_locations_migration = AWSExternalLocationsMigration(
        mock_ws,
        locations,
        aws_resource_permissions,
        principal_acl,
    )
    external_locations_migration.run()
    calls = [
        call(mock.ANY, 's3://BUCKET1/FOLDER1', 'cred1', skip_validation=True),
        call(mock.ANY, 's3://BUCKET2/FOLDER2', 'cred1', skip_validation=True),
        call(mock.ANY, 's3://BUCKETX/FOLDERX', 'credx', skip_validation=True),
    ]
    mock_ws.external_locations.create.assert_has_calls(calls, any_order=True)
    aws.get_role_policy.assert_not_called()
    principal_acl.apply_location_acl.assert_called()


def test_create_external_locations_skip_existing(mock_ws, backend, locations):
    install = MockInstallation(
        {
            "uc_roles_access.csv": [
                {
                    'privilege': 'WRITE_FILES',
                    'resource_path': 's3://BUCKET1',
                    'resource_type': 's3',
                    'role_arn': 'arn:aws:iam::12345:role/uc-role1',
                },
                {
                    'privilege': 'WRITE_FILES',
                    'resource_path': 's3://BUCKETX',
                    'resource_type': 's3',
                    'role_arn': 'arn:aws:iam::12345:role/uc-role1',
                },
            ]
        }
    )
    mock_ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(
            id="1",
            name="cred1",
            aws_iam_role=AwsIamRoleResponse("arn:aws:iam::12345:role/uc-role1"),
        ),
        StorageCredentialInfo(
            id="2",
            name="credx",
            aws_iam_role=AwsIamRoleResponse("arn:aws:iam::12345:role/uc-rolex"),
        ),
    ]
    mock_ws.external_locations.list.return_value = [
        ExternalLocationInfo(name="UCX_FOO_1", url="s3://BUCKETX/FOLDERX", credential_name="credx"),
    ]
    aws = create_autospec(AWSResources)
    principal_acl = create_autospec(PrincipalACL)
    aws_resource_permissions = AWSResourcePermissions(install, mock_ws, aws, locations)
    external_locations_migration = AWSExternalLocationsMigration(
        mock_ws,
        locations,
        aws_resource_permissions,
        principal_acl,
    )
    external_locations_migration.run(location_prefix="UCX_FOO")
    calls = [
        call("UCX_FOO_2", 's3://BUCKET1/FOLDER1', 'cred1', skip_validation=True),
    ]
    mock_ws.external_locations.create.assert_has_calls(calls, any_order=True)
    aws.get_role_policy.assert_not_called()
    principal_acl.apply_location_acl.assert_called()


def test_create_uber_principal_existing_role_in_policy(mock_ws, mock_installation, backend, locations):
    instance_profile_arn = "arn:aws:iam::12345:instance-profile/role1"
    cluster_policy = Policy(
        policy_id="foo",
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(
            {"foo": "bar", "aws_attributes.instance_profile_arn": {"type": "fixed", "value": instance_profile_arn}}
        ),
    )
    mock_ws.cluster_policies.get.return_value = cluster_policy
    aws = create_autospec(AWSResources)
    aws.get_instance_profile_arn.return_value = instance_profile_arn
    locations = ExternalLocations(mock_ws, backend, "ucx")
    prompts = MockPrompts({"We have identified existing UCX migration role *": "yes"})
    aws_resource_permissions = AWSResourcePermissions(
        mock_installation,
        mock_ws,
        aws,
        locations,
    )
    aws_resource_permissions.create_uber_principal(prompts)
    aws.put_role_policy.assert_called_with(
        'role1',
        'UCX_MIGRATION_POLICY_ucx',
        {'s3://BUCKET1/FOLDER1', 's3://BUCKET2/FOLDER2', 's3://BUCKETX/FOLDERX'},
        None,
        None,
    )


def test_create_uber_principal_existing_role(mock_ws, mock_installation, backend, locations):
    cluster_policy = Policy(
        policy_id="foo", name="Unity Catalog Migration (ucx) (me@example.com)", definition=json.dumps({"foo": "bar"})
    )
    mock_ws.cluster_policies.get.return_value = cluster_policy
    mock_ws.warehouses.get_workspace_warehouse_config.return_value = GetWorkspaceWarehouseConfigResponse(
        instance_profile_arn="arn:aws:iam::12345:instance-profile/existing-role"
    )
    instance_profile_arn = "arn:aws:iam::12345:instance-profile/role1"
    aws = create_autospec(AWSResources)
    aws.get_instance_profile_arn.return_value = instance_profile_arn
    locations = ExternalLocations(mock_ws, backend, "ucx")
    prompts = MockPrompts(
        {
            "There is an existing instance profile *": "yes",
            "We have identified existing UCX migration role *": "yes",
        }
    )
    aws_resource_permissions = AWSResourcePermissions(
        mock_installation,
        mock_ws,
        aws,
        locations,
    )
    aws_resource_permissions.create_uber_principal(prompts)
    definition = {"foo": "bar", "aws_attributes.instance_profile_arn": {"type": "fixed", "value": instance_profile_arn}}
    mock_ws.cluster_policies.edit.assert_called_with(
        'foo', 'Unity Catalog Migration (ucx) (me@example.com)', definition=json.dumps(definition)
    )
    mock_ws.warehouses.set_workspace_warehouse_config.assert_called_with(
        data_access_config=None,
        instance_profile_arn='arn:aws:iam::12345:instance-profile/role1',
        sql_configuration_parameters=None,
    )


def test_create_uber_principal_no_existing_role(mock_ws, mock_installation, backend, locations):
    cluster_policy = Policy(
        policy_id="foo", name="Unity Catalog Migration (ucx) (me@example.com)", definition=json.dumps({"foo": "bar"})
    )
    mock_ws.cluster_policies.get.return_value = cluster_policy
    mock_ws.warehouses.get_workspace_warehouse_config.return_value = GetWorkspaceWarehouseConfigResponse(
        data_access_config=[EndpointConfPair("jdbc", "jdbc:sqlserver://localhost:1433;databaseName=master")]
    )
    aws = create_autospec(AWSResources)
    aws.role_exists.return_value = False
    instance_profile_arn = "arn:aws:iam::12345:instance-profile/role1"
    aws.create_migration_role.return_value = instance_profile_arn
    aws.create_instance_profile.return_value = instance_profile_arn
    aws.get_instance_profile_arn.return_value = instance_profile_arn
    locations = ExternalLocations(mock_ws, backend, "ucx")
    prompts = MockPrompts({"Do you want to create new migration role *": "yes"})
    aws_resource_permissions = AWSResourcePermissions(
        mock_installation,
        mock_ws,
        aws,
        locations,
    )

    aws_resource_permissions.create_uber_principal(prompts)
    definition = {"foo": "bar", "aws_attributes.instance_profile_arn": {"type": "fixed", "value": instance_profile_arn}}
    mock_ws.cluster_policies.edit.assert_called_with(
        'foo', 'Unity Catalog Migration (ucx) (me@example.com)', definition=json.dumps(definition)
    )
    mock_ws.warehouses.set_workspace_warehouse_config.assert_called_with(
        data_access_config=[EndpointConfPair("jdbc", "jdbc:sqlserver://localhost:1433;databaseName=master")],
        instance_profile_arn='arn:aws:iam::12345:instance-profile/role1',
        sql_configuration_parameters=None,
    )


def test_create_uber_principal_no_storage(mock_ws, mock_installation, locations):
    cluster_policy = Policy(
        policy_id="foo", name="Unity Catalog Migration (ucx) (me@example.com)", definition=json.dumps({"foo": "bar"})
    )
    mock_ws.cluster_policies.get.return_value = cluster_policy
    locations = ExternalLocations(mock_ws, MockBackend(), "ucx")
    prompts = MockPrompts({})
    aws = create_autospec(AWSResources)
    aws_resource_permissions = AWSResourcePermissions(
        mock_installation,
        mock_ws,
        aws,
        locations,
    )
    assert not aws_resource_permissions.create_uber_principal(prompts)
    aws.list_attached_policies_in_role.assert_not_called()
    aws.get_role_policy.assert_not_called()


def test_create_uc_role_single(mock_ws, installation_single_role, backend, locations):
    aws = create_autospec(AWSResources)
    aws_resource_permissions = AWSResourcePermissions(installation_single_role, mock_ws, aws, locations)
    role_creation = IamRoleCreation(installation_single_role, mock_ws, aws_resource_permissions)
    aws.list_all_uc_roles.return_value = []
    role_creation.run(MockPrompts({"Above *": "yes"}))
    assert aws.create_uc_role.assert_called
    assert (
        call('UC_ROLE', 'UC_POLICY', {'s3://BUCKET1/FOLDER1', 's3://BUCKET2/FOLDER2'}, None, None)
        in aws.put_role_policy.call_args_list
    )


def test_create_uc_role_multiple(mock_ws, installation_single_role, backend, locations):
    aws = create_autospec(AWSResources)
    aws_resource_permissions = AWSResourcePermissions(installation_single_role, mock_ws, aws, locations)
    role_creation = IamRoleCreation(installation_single_role, mock_ws, aws_resource_permissions)
    aws.list_all_uc_roles.return_value = []
    role_creation.run(MockPrompts({"Above *": "yes"}), single_role=False)
    assert call('UC_ROLE_1') in aws.create_uc_role.call_args_list
    assert call('UC_ROLE_2') in aws.create_uc_role.call_args_list
    assert call('UC_ROLE_1', 'UC_POLICY', {'s3://BUCKET1/FOLDER1'}, None, None) in aws.put_role_policy.call_args_list
    assert call('UC_ROLE_2', 'UC_POLICY', {'s3://BUCKET2/FOLDER2'}, None, None) in aws.put_role_policy.call_args_list


def test_create_uc_no_roles(installation_no_roles, mock_ws, caplog):
    sql_backend = MockBackend(rows={}, fails_on_first={})
    external_locations = ExternalLocations(mock_ws, sql_backend, "ucx")
    aws = create_autospec(AWSResources)
    aws_resource_permissions = AWSResourcePermissions(
        installation_no_roles,
        mock_ws,
        aws,
        external_locations,
    )
    role_creation = IamRoleCreation(installation_no_roles, mock_ws, aws_resource_permissions)
    aws.list_all_uc_roles.return_value = []
    with caplog.at_level(logging.INFO):
        role_creation.run(MockPrompts({"Above *": "yes"}), single_role=False)
        assert ['No IAM Role created'] == caplog.messages
        aws.create_uc_role.assert_not_called()


def test_get_uc_compatible_roles(mock_ws, mock_installation, locations):
    aws = create_autospec(AWSResources)
    aws.get_role_policy.side_effect = [
        [
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket1",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket2",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket3",
            ),
        ],
        [],
        [],
        [
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketA",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketB",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketC",
            ),
        ],
        [],
        [],
    ]
    aws.list_role_policies.return_value = ["Policy1", "Policy2", "Policy3"]
    aws.list_attached_policies_in_role.return_value = [
        "arn:aws:iam::aws:policy/Policy1",
        "arn:aws:iam::aws:policy/Policy2",
    ]
    aws.list_all_uc_roles.return_value = [
        AWSRole(path='/', role_name='uc-role1', role_id='12345', arn='arn:aws:iam::12345:role/uc-role1')
    ]
    aws_resource_permissions = AWSResourcePermissions(
        mock_installation,
        mock_ws,
        aws,
        locations,
    )
    # TODO: this is bad practice, we should not be mocking load() methon on a MockInstallation class
    mock_installation.load = MagicMock(
        side_effect=[
            ResourceDoesNotExist(),
            [AWSRoleAction("arn:aws:iam::12345:role/uc-role1", "s3", "WRITE_FILES", "s3://BUCKETX/*")],
        ]
    )
    aws_resource_permissions.load_uc_compatible_roles()
    mock_installation.assert_file_written(
        'uc_roles_access.csv',
        [
            {
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket1',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket2',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket3',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketA',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketB',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketC',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
        ],
    )


def test_instance_profiles_empty_mapping(mock_ws, mock_installation, locations, caplog):
    aws = create_autospec(AWSResources)
    aws.get_instance_profile_role_arn.return_value = "arn:aws:iam::12345:role/role1"
    aws_resource_permissions = AWSResourcePermissions(
        mock_installation,
        mock_ws,
        aws,
        locations,
    )
    aws_resource_permissions.save_instance_profile_permissions()
    assert 'No mapping was generated.' in caplog.messages
    aws.list_role_policies.assert_called_once()
    aws.list_role_policies.assert_called_once()
    aws.list_attached_policies_in_role.assert_called_once_with('role1')


def test_uc_roles_empty_mapping(mock_ws, mock_installation, locations, caplog):
    aws = create_autospec(AWSResources)
    aws_resource_permissions = AWSResourcePermissions(
        mock_installation,
        mock_ws,
        aws,
        locations,
    )
    aws_resource_permissions.save_uc_compatible_roles()
    assert 'No mapping was generated.' in caplog.messages
    aws.list_all_uc_roles.assert_called_once()


def test_save_instance_profile_permissions(mock_ws, mock_installation, locations):
    aws = create_autospec(AWSResources)
    aws.get_instance_profile_role_arn.return_value = "arn:aws:iam::12345:role/role1"
    aws.get_role_policy.side_effect = [
        [
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket1",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket2",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket3",
            ),
        ],
        [],
        [],
        [
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketA",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketB",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketC",
            ),
        ],
        [],
        [],
    ]
    aws.list_role_policies.return_value = ["Policy1", "Policy2", "Policy3"]
    aws.list_attached_policies_in_role.return_value = [
        "arn:aws:iam::aws:policy/Policy1",
        "arn:aws:iam::aws:policy/Policy2",
    ]
    aws_resource_permissions = AWSResourcePermissions(
        mock_installation,
        mock_ws,
        aws,
        locations,
    )
    aws_resource_permissions.save_instance_profile_permissions()

    mock_installation.assert_file_written(
        'aws_instance_profile_info.csv',
        [
            {
                'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket1',
                'resource_type': 's3',
            },
            {
                'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket2',
                'resource_type': 's3',
            },
            {
                'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket3',
                'resource_type': 's3',
            },
            {
                'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketA',
                'resource_type': 's3',
            },
            {
                'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketB',
                'resource_type': 's3',
            },
            {
                'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketC',
                'resource_type': 's3',
            },
        ],
    )


def test_save_uc_compatible_roles(mock_ws, mock_installation, locations):
    aws = create_autospec(AWSResources)
    aws.get_role_policy.side_effect = [
        [
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket1",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket2",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="READ_FILES",
                resource_path="s3://bucket3",
            ),
        ],
        [],
        [],
        [
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketA",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketB",
            ),
            AWSPolicyAction(
                resource_type="s3",
                privilege="WRITE_FILES",
                resource_path="s3://bucketC",
            ),
        ],
        [],
        [],
    ]
    aws.list_role_policies.return_value = ["Policy1", "Policy2", "Policy3"]
    aws.list_attached_policies_in_role.return_value = [
        "arn:aws:iam::aws:policy/Policy1",
        "arn:aws:iam::aws:policy/Policy2",
    ]
    aws.list_all_uc_roles.return_value = [
        AWSRole(path='/', role_name='uc-role1', role_id='12345', arn='arn:aws:iam::12345:role/uc-role1')
    ]
    aws_resource_permissions = AWSResourcePermissions(
        mock_installation,
        mock_ws,
        aws,
        locations,
    )
    aws_resource_permissions.save_uc_compatible_roles()
    mock_installation.assert_file_written(
        'uc_roles_access.csv',
        [
            {
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket1',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket2',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'READ_FILES',
                'resource_path': 's3://bucket3',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketA',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketB',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
            {
                'privilege': 'WRITE_FILES',
                'resource_path': 's3://bucketC',
                'resource_type': 's3',
                'role_arn': 'arn:aws:iam::12345:role/uc-role1',
            },
        ],
    )


def test_instance_profile_lookup():
    def instance_lookup(_):
        ip_doc = """
{
    "InstanceProfile": {
        "Path": "/",
        "InstanceProfileName": "instance_profile_1",
        "InstanceProfileId": "778899",
        "Arn": "arn:aws:iam::12345678:instance-profile/instance_profile_1",
        "CreateDate": "2024-01-01T12:00:00+00:00",
        "Roles": [
            {
                "Path": "/",
                "RoleName": "arn:aws:iam::12345678:role/role_1",
                "RoleId": "445566",
                "Arn": "arn:aws:iam::12345678:role/role_1",
                "CreateDate": "2024-01-01T12:00:00+00:00"
            }
        ]
    }
}

        """
        return 0, ip_doc, ""

    aws = AWSResources("profile", instance_lookup)
    assert aws.get_instance_profile_role_arn("instance_profile_1") == "arn:aws:iam::12345678:role/role_1"


def test_instance_profile_failed_lookup():
    def instance_lookup(_):
        ip_doc = """
{
    "InstanceProfile": {
        "Path": "/",
        "InstanceProfileName": "instance_profile_1",
        "InstanceProfileId": "778899",
        "Arn": "arn:aws:iam::12345678:instance-profile/instance_profile_1"
    }
}

        """
        return 0, ip_doc, ""

    aws = AWSResources("profile", instance_lookup)
    assert aws.get_instance_profile_role_arn("instance_profile_1") is None
