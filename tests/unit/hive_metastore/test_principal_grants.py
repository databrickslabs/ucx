from unittest.mock import create_autospec, call

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service import iam
from databricks.sdk.service.catalog import (
    ExternalLocationInfo,
    Privilege,
    SecurableType,
    PermissionsChange,
)
from databricks.sdk.service.compute import (
    AwsAttributes,
    ClusterDetails,
    ClusterSource,
    DataSecurityMode,
)

from databricks.labs.ucx.assessment.azure import (
    AzureServicePrincipalCrawler,
    AzureServicePrincipalInfo,
    ServicePrincipalClusterMapping,
)
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore import Mounts, TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import AzureACL, Grant, PrincipalACL
from databricks.labs.ucx.hive_metastore.locations import Mount
from databricks.labs.ucx.hive_metastore.grants import (
    AwsACL,
)
from databricks.labs.ucx.hive_metastore.tables import Table


@pytest.fixture
def ws():
    w = create_autospec(WorkspaceClient)
    w.external_locations.list.return_value = [
        ExternalLocationInfo(url="abfss://container1@storage1.dfs.core.windows.net/folder1", name='loc1'),
        ExternalLocationInfo(url="abfss://container1@storage2.dfs.core.windows.net/folder2", name='loc2'),
        ExternalLocationInfo(url="abfss://container1@storage3.dfs.core.windows.net/folder3", name='loc3'),
        ExternalLocationInfo(url="s3://storage5/folder5", name='loc1'),
        ExternalLocationInfo(url="s3://storage2/folder2", name='loc2'),
        ExternalLocationInfo(url="s3://storage3/folder3"),
    ]

    permissions = {
        'cluster1': iam.ObjectPermissions(
            object_id='cluster1',
            object_type="clusters",
            access_control_list=[
                iam.AccessControlResponse(group_name='group1', all_permissions=[iam.Permission(inherited=False)]),
                iam.AccessControlResponse(
                    user_name='foo.bar@imagine.com',
                    all_permissions=[iam.Permission(permission_level=iam.PermissionLevel.CAN_USE)],
                ),
            ],
        ),
        'cluster2': iam.ObjectPermissions(
            object_id='cluster2',
            object_type="clusters",
            access_control_list=[
                iam.AccessControlResponse(
                    service_principal_name='spn1',
                    all_permissions=[iam.Permission(permission_level=iam.PermissionLevel.CAN_USE)],
                ),
            ],
        ),
        'cluster3': iam.ObjectPermissions(object_id='cluster2', object_type="clusters"),
        'cluster4': iam.ObjectPermissions(
            object_id='cluster4',
            object_type="clusters",
            access_control_list=[
                iam.AccessControlResponse(
                    service_principal_name='spn1',
                    all_permissions=[iam.Permission(permission_level=iam.PermissionLevel.CAN_USE)],
                ),
            ],
        ),
    }
    w.permissions.get.side_effect = lambda _, object_id: permissions[object_id]

    return w


def azure_acl(w, install, cluster_spn: list):
    config = install.load(WorkspaceConfig)
    sql_backend = StatementExecutionBackend(w, config.warehouse_id)
    spn_crawler = create_autospec(AzureServicePrincipalCrawler)
    spn_crawler.get_cluster_to_storage_mapping.return_value = cluster_spn
    return AzureACL(w, sql_backend, spn_crawler, install)


def aws_acl(w, install):
    config = install.load(WorkspaceConfig)
    sql_backend = StatementExecutionBackend(w, config.warehouse_id)
    return AwsACL(w, sql_backend, install)


def principal_acl(w, install, cluster_spn: list):
    config = install.load(WorkspaceConfig)
    sql_backend = StatementExecutionBackend(w, config.warehouse_id)
    table_crawler = create_autospec(TablesCrawler)
    tables = [
        Table(
            'hive_metastore',
            'schema1',
            'table1',
            'TABLE',
            'delta',
            location='abfss://container1@storage1.dfs.core.windows.net/folder1/table1',
        ),
        Table('hive_metastore', 'schema1', 'view1', 'VIEW', 'delta', view_text="select * from table1"),
        Table(
            'hive_metastore',
            'schema1',
            'table2',
            'TABLE',
            'delta',
            location='abfss://container1@storage2.dfs.core.windows.net/folder2/table2',
        ),
        Table('hive_metastore', 'schema1', 'table3', 'TABLE', 'delta', location='dbfs:/mnt/folder1/table3'),
        Table('hive_metastore', 'schema1', 'table5', 'TABLE', 'delta', location='dbfs:/hms/folder1/table1'),
        Table(
            'hive_metastore',
            'schema2',
            'table4',
            'TABLE',
            'delta',
            location='abfss://container1@storage3.dfs.core.windows.net/folder3/table3',
        ),
        Table(
            'hive_metastore',
            'schema4',
            'table6',
            'TABLE',
            'delta',
            location='dbfs:/mnt/folder5/table6',
        ),
    ]
    table_crawler.snapshot.return_value = tables
    mount_crawler = create_autospec(Mounts)
    mount_crawler.snapshot.return_value = [
        Mount('/mnt/folder1', 'abfss://container1@storage1.dfs.core.windows.net/folder1'),
        Mount('/mnt/folder5', 's3://storage5/folder5'),
    ]

    spn_crawler = create_autospec(AzureServicePrincipalCrawler)
    spn_crawler.get_cluster_to_storage_mapping.return_value = cluster_spn
    locations = {}
    if w.config.is_azure:
        locations = azure_acl(w, install, cluster_spn).get_eligible_locations_principals()
    if w.config.is_aws:
        locations = aws_acl(w, install).get_eligible_locations_principals()
    return PrincipalACL(w, sql_backend, install, table_crawler, mount_crawler, locations)


@pytest.fixture
def installation():
    return MockInstallation(
        {
            "config.yml": {'warehouse_id': 'abc', 'connect': {'host': 'a', 'token': 'b'}, 'inventory_database': 'ucx'},
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'abfss://container1@storage1.dfs.core.windows.net',
                    'client_id': 'client1',
                    'principal': 'principal_1',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_ss1',
                },
                {
                    'prefix': 'abfss://container1@storage2.dfs.core.windows.net',
                    'client_id': 'client2',
                    'principal': 'principal_1',
                    'privilege': 'READ_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_ss1',
                },
                {
                    'prefix': 'abfss://container1@storage3.dfs.core.windows.net',
                    'client_id': 'client2',
                    'principal': 'principal_1',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_ss1',
                },
            ],
            "aws_instance_profile_info.csv": [
                {
                    'resource_path': 's3://storage5/*',
                    'role_arn': 'arn:aws:iam::12345:instance-profile/role1',
                    'privilege': 'WRITE_FILES',
                    'resource_type': 's3',
                },
                {
                    'resource_path': 's3://storage3/*',
                    'role_arn': 'arn:aws:iam::12345:instance-profile/role2',
                    'privilege': 'WRITE_FILES',
                    'resource_type': 's3',
                },
            ],
        }
    )


def test_get_eligible_locations_principals_no_cluster_mapping(ws, installation):
    ws.config.is_azure = True
    locations = azure_acl(ws, installation, [])
    locations.get_eligible_locations_principals()
    ws.external_locations.list.assert_not_called()


def test_get_eligible_locations_principals_no_external_location(ws, installation):
    ws.config.is_azure = True
    cluster_spn = ServicePrincipalClusterMapping(
        'abc', {AzureServicePrincipalInfo(application_id='Hello, World!', storage_account='abcde')}
    )
    locations = azure_acl(ws, installation, [cluster_spn])
    ws.external_locations.list.return_value = []
    with pytest.raises(ResourceDoesNotExist):
        locations.get_eligible_locations_principals()


def test_get_eligible_locations_principals_no_permission_mapping(ws):
    ws.config.is_azure = True
    cluster_spn = ServicePrincipalClusterMapping(
        'abc', {AzureServicePrincipalInfo(application_id='Hello, World!', storage_account='abcde')}
    )
    install = MockInstallation(
        {
            "config.yml": {'warehouse_id': 'abc', 'connect': {'host': 'a', 'token': 'b'}, 'inventory_database': 'ucx'},
            "azure_storage_account_info.csv": [],
        }
    )
    locations = azure_acl(ws, install, [cluster_spn])

    with pytest.raises(ResourceDoesNotExist):
        locations.get_eligible_locations_principals()


def test_get_eligible_locations_principals(ws, installation):
    ws.config.is_azure = True
    cluster_spn = ServicePrincipalClusterMapping(
        'abc', {AzureServicePrincipalInfo(application_id='client1', storage_account='storage1')}
    )
    locations = azure_acl(ws, installation, [cluster_spn])
    eligible_locations = locations.get_eligible_locations_principals()
    assert len(eligible_locations) == 1
    assert eligible_locations['abc'] == {'abfss://container1@storage1.dfs.core.windows.net/folder1': 'WRITE_FILES'}


def test_interactive_cluster_no_acl(ws, installation):
    ws.config.is_azure = True
    cluster_spn = ServicePrincipalClusterMapping(
        'cluster3', {AzureServicePrincipalInfo(application_id='client1', storage_account='storage1')}
    )
    grants = principal_acl(ws, installation, [cluster_spn])
    actual_grants = grants.get_interactive_cluster_grants()
    assert len(actual_grants) == 0


def test_interactive_cluster_single_spn(ws, installation):
    ws.config.is_azure = True
    ws.config.is_aws = False
    cluster_spn = ServicePrincipalClusterMapping(
        'cluster1',
        {AzureServicePrincipalInfo(application_id='client1', storage_account='storage1')},
    )
    grants = principal_acl(ws, installation, [cluster_spn])
    expected_grants = [
        Grant('group1', "ALL PRIVILEGES", "hive_metastore", 'schema1', 'table1'),
        Grant('foo.bar@imagine.com', "ALL PRIVILEGES", "hive_metastore", 'schema1', 'table1'),
        Grant('group1', "ALL PRIVILEGES", "hive_metastore", 'schema1', view='view1'),
        Grant('foo.bar@imagine.com', "ALL PRIVILEGES", "hive_metastore", 'schema1', view='view1'),
        Grant('group1', "ALL PRIVILEGES", "hive_metastore", 'schema1', 'table3'),
        Grant('foo.bar@imagine.com', "ALL PRIVILEGES", "hive_metastore", 'schema1', 'table3'),
        Grant('group1', "ALL PRIVILEGES", "hive_metastore", 'schema1', 'table5'),
        Grant('foo.bar@imagine.com', "ALL PRIVILEGES", "hive_metastore", 'schema1', 'table5'),
        Grant('group1', "USAGE", "hive_metastore", 'schema1'),
        Grant('foo.bar@imagine.com', "USAGE", "hive_metastore", 'schema1'),
    ]
    actual_grants = grants.get_interactive_cluster_grants()
    for grant in expected_grants:
        assert grant in actual_grants


def test_interactive_cluster_multiple_spn(ws, installation):
    ws.config.is_azure = True
    ws.config.is_aws = False
    cluster_spn = ServicePrincipalClusterMapping(
        'cluster2',
        {
            AzureServicePrincipalInfo(application_id='client2', storage_account='storage2'),
            AzureServicePrincipalInfo(application_id='client2', storage_account='storage3'),
        },
    )
    grants = principal_acl(ws, installation, [cluster_spn])
    expected_grants = [
        Grant('spn1', "SELECT", "hive_metastore", 'schema1', 'table2'),
        Grant('spn1', "ALL PRIVILEGES", "hive_metastore", 'schema2', 'table4'),
        Grant('spn1', "ALL PRIVILEGES", "hive_metastore", 'schema1', 'table5'),
        Grant('spn1', "ALL PRIVILEGES", "hive_metastore", 'schema1', view='view1'),
        Grant('spn1', "USAGE", "hive_metastore", 'schema1'),
        Grant('spn1', "USAGE", "hive_metastore", 'schema2'),
    ]
    actual_grants = grants.get_interactive_cluster_grants()
    for grant in expected_grants:
        assert grant in actual_grants


def test_get_eligible_locations_principals_no_cluster_return_empty(ws, installation):
    ws.clusters.list.return_value = []
    locations = aws_acl(ws, installation)
    locations.get_eligible_locations_principals()
    ws.external_locations.list.assert_not_called()


def test_get_eligible_locations_principals_no_interactive_cluster_return_empty(ws, installation):
    ws.clusters.list.return_value = [
        ClusterDetails(cluster_source=ClusterSource.JOB),
        ClusterDetails(cluster_source=ClusterSource.UI, data_security_mode=DataSecurityMode.SINGLE_USER),
    ]
    locations = aws_acl(ws, installation)
    locations.get_eligible_locations_principals()
    ws.external_locations.list.assert_not_called()


def test_get_eligible_locations_principals_interactive_no_instance_profiles(ws, installation):
    ws.clusters.list.return_value = [
        ClusterDetails(cluster_id='cluster1', cluster_source=ClusterSource.JOB),
        ClusterDetails(
            cluster_id='cluster1', cluster_source=ClusterSource.UI, data_security_mode=DataSecurityMode.NONE
        ),
    ]
    locations = aws_acl(ws, installation)
    locations.get_eligible_locations_principals()
    ws.external_locations.list.assert_not_called()


def test_get_eligible_locations_principals_interactive_with_instance_profile(ws, installation):
    ws.clusters.list.return_value = [
        ClusterDetails(cluster_id='cluster1', cluster_source=ClusterSource.JOB),
        ClusterDetails(
            cluster_id='cluster1',
            cluster_source=ClusterSource.UI,
            data_security_mode=DataSecurityMode.NONE,
            aws_attributes=AwsAttributes(instance_profile_arn="arn:aws:iam::12345:instance-profile/role1"),
        ),
    ]
    locations = aws_acl(ws, installation)
    ws.external_locations.list.return_value = []
    with pytest.raises(ResourceDoesNotExist):
        locations.get_eligible_locations_principals()


def test_get_eligible_locations_principals_aws_no_permission_mapping(ws):
    ws.config.is_azure = False
    ws.config.is_aws = True
    ws.clusters.list.return_value = [
        ClusterDetails(cluster_id='cluster1', cluster_source=ClusterSource.JOB),
        ClusterDetails(
            cluster_id='cluster1',
            cluster_source=ClusterSource.UI,
            data_security_mode=DataSecurityMode.NONE,
            aws_attributes=AwsAttributes(instance_profile_arn="arn:aws:iam::12345:instance-profile/role1"),
        ),
    ]

    install = MockInstallation(
        {
            "config.yml": {'warehouse_id': 'abc', 'connect': {'host': 'a', 'token': 'b'}, 'inventory_database': 'ucx'},
            "aws_instance_profile_info.csv": [],
        }
    )
    locations = aws_acl(ws, install)

    with pytest.raises(ResourceDoesNotExist):
        locations.get_eligible_locations_principals()


def test_get_eligible_locations_principals_aws_no_matching_locations(ws, installation):
    ws.config.is_azure = False
    ws.config.is_aws = True
    ws.clusters.list.return_value = [
        ClusterDetails(cluster_id='cluster1', cluster_source=ClusterSource.JOB),
        ClusterDetails(
            cluster_id='cluster1',
            cluster_source=ClusterSource.UI,
            data_security_mode=DataSecurityMode.NONE,
            aws_attributes=AwsAttributes(instance_profile_arn="arn:aws:iam::12345:instance-profile/role3"),
        ),
    ]

    locations = aws_acl(ws, installation)
    eligible_locations = locations.get_eligible_locations_principals()
    assert len(eligible_locations) == 0


def test_get_eligible_locations_principals_aws(ws, installation):
    ws.config.is_azure = False
    ws.config.is_aws = True
    ws.clusters.list.return_value = [
        ClusterDetails(cluster_id='cluster1', cluster_source=ClusterSource.JOB),
        ClusterDetails(
            cluster_id='cluster1',
            cluster_source=ClusterSource.UI,
            data_security_mode=DataSecurityMode.NONE,
            aws_attributes=AwsAttributes(instance_profile_arn="arn:aws:iam::12345:instance-profile/role1"),
        ),
    ]

    locations = aws_acl(ws, installation)
    eligible_locations = locations.get_eligible_locations_principals()
    assert len(eligible_locations) == 1
    assert eligible_locations['cluster1'] == {'s3://storage5/folder5': 'WRITE_FILES'}


def test_interactive_cluster_aws_no_acl(ws, installation):
    ws.config.is_azure = False
    ws.config.is_aws = True
    ws.clusters.list.return_value = [
        ClusterDetails(
            cluster_id='cluster3',
            cluster_source=ClusterSource.UI,
            data_security_mode=DataSecurityMode.NONE,
            aws_attributes=AwsAttributes(instance_profile_arn="arn:aws:iam::12345:instance-profile/role1"),
        ),
    ]
    grants = principal_acl(ws, installation, [])
    actual_grants = grants.get_interactive_cluster_grants()
    assert len(actual_grants) == 0


def test_interactive_cluster_aws(ws, installation):
    ws.config.is_azure = False
    ws.config.is_aws = True
    ws.clusters.list.return_value = [
        ClusterDetails(
            cluster_id='cluster2',
            cluster_source=ClusterSource.UI,
            data_security_mode=DataSecurityMode.NONE,
            aws_attributes=AwsAttributes(instance_profile_arn="arn:aws:iam::12345:instance-profile/role1"),
        ),
    ]
    grants = principal_acl(ws, installation, [])
    expected_grants = [
        Grant('spn1', "ALL PRIVILEGES", "hive_metastore", 'schema1', 'table5'),
        Grant('spn1', "ALL PRIVILEGES", "hive_metastore", 'schema1', view='view1'),
        Grant('spn1', "ALL PRIVILEGES", "hive_metastore", 'schema4', 'table6'),
        Grant('spn1', "USAGE", "hive_metastore", 'schema1'),
        Grant('spn1', "USAGE", "hive_metastore", 'schema4'),
    ]
    actual_grants = grants.get_interactive_cluster_grants()
    for grant in expected_grants:
        assert grant in actual_grants


def test_apply_location_acl_no_principal_azure(ws, installation):
    ws.config.is_azure = True
    ws.config.is_aws = False
    cluster_spn = ServicePrincipalClusterMapping(
        'cluster3', {AzureServicePrincipalInfo(application_id='client1', storage_account='storage1')}
    )
    location_acl = principal_acl(ws, installation, [cluster_spn])
    location_acl.apply_location_acl()
    ws.grants.update.assert_not_called()


def test_apply_location_acl_no_principal_aws(ws, installation):
    ws.config.is_azure = False
    ws.config.is_aws = True
    ws.clusters.list.return_value = [
        ClusterDetails(
            cluster_id='cluster3',
            cluster_source=ClusterSource.UI,
            data_security_mode=DataSecurityMode.NONE,
            aws_attributes=AwsAttributes(instance_profile_arn="arn:aws:iam::12345:instance-profile/role1"),
        ),
    ]
    location_acl = principal_acl(ws, installation, [])
    location_acl.apply_location_acl()
    ws.grants.update.assert_not_called()


def test_apply_location_acl_no_location_name(ws, installation):
    ws.config.is_azure = False
    ws.config.is_aws = True
    ws.clusters.list.return_value = [
        ClusterDetails(
            cluster_id='cluster4',
            cluster_source=ClusterSource.UI,
            data_security_mode=DataSecurityMode.NONE,
            aws_attributes=AwsAttributes(instance_profile_arn="arn:aws:iam::12345:instance-profile/role2"),
        ),
    ]
    location_acl = principal_acl(ws, installation, [])
    location_acl.apply_location_acl()
    ws.grants.update.assert_not_called()


def test_apply_location_acl_single_spn_azure(ws, installation):
    ws.config.is_azure = True
    ws.config.is_aws = False
    cluster_spn = ServicePrincipalClusterMapping(
        'cluster1', {AzureServicePrincipalInfo(application_id='client1', storage_account='storage1')}
    )
    location_acl = principal_acl(ws, installation, [cluster_spn])
    location_acl.apply_location_acl()
    permissions = [Privilege.CREATE_EXTERNAL_TABLE, Privilege.CREATE_EXTERNAL_VOLUME, Privilege.READ_FILES]
    calls = [
        call(
            SecurableType.EXTERNAL_LOCATION,
            'loc1',
            changes=[
                PermissionsChange(add=permissions, principal='group1'),
                PermissionsChange(add=permissions, principal='foo.bar@imagine.com'),
            ],
        )
    ]
    ws.grants.update.assert_has_calls(calls, any_order=True)


def test_apply_location_acl_single_spn_aws(ws, installation):
    ws.config.is_azure = False
    ws.config.is_aws = True
    ws.clusters.list.return_value = [
        ClusterDetails(
            cluster_id='cluster2',
            cluster_source=ClusterSource.UI,
            data_security_mode=DataSecurityMode.NONE,
            aws_attributes=AwsAttributes(instance_profile_arn="arn:aws:iam::12345:instance-profile/role1"),
        ),
    ]
    location_acl = principal_acl(ws, installation, [])
    location_acl.apply_location_acl()
    permissions = [Privilege.CREATE_EXTERNAL_TABLE, Privilege.CREATE_EXTERNAL_VOLUME, Privilege.READ_FILES]
    calls = [
        call(SecurableType.EXTERNAL_LOCATION, 'loc1', changes=[PermissionsChange(add=permissions, principal='spn1')])
    ]
    ws.grants.update.assert_has_calls(calls, any_order=True)
