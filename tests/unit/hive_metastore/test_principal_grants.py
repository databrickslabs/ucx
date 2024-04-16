from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service import iam
from databricks.sdk.service.catalog import ExternalLocationInfo

from databricks.labs.ucx.assessment.azure import (
    AzureServicePrincipalCrawler,
    AzureServicePrincipalInfo,
    ServicePrincipalClusterMapping,
)
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore import Mounts, TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import AzureACL, Grant, PrincipalACL
from databricks.labs.ucx.hive_metastore.locations import Mount
from databricks.labs.ucx.hive_metastore.tables import Table


@pytest.fixture
def ws():
    w = create_autospec(WorkspaceClient)
    w.config.is_azure = True
    w.external_locations.list.return_value = [
        ExternalLocationInfo(url="abfss://container1@storage1.dfs.core.windows.net/folder1"),
        ExternalLocationInfo(url="abfss://container1@storage2.dfs.core.windows.net/folder2"),
        ExternalLocationInfo(url="abfss://container1@storage3.dfs.core.windows.net/folder3"),
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
    }
    w.permissions.get.side_effect = lambda _, object_id: permissions[object_id]
    return w


def azure_acl(w, install, cluster_spn: list):
    config = install.load(WorkspaceConfig)
    sql_backend = StatementExecutionBackend(w, config.warehouse_id)
    spn_crawler = create_autospec(AzureServicePrincipalCrawler)
    spn_crawler.get_cluster_to_storage_mapping.return_value = cluster_spn
    return AzureACL(w, sql_backend, spn_crawler, install)


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
    ]
    table_crawler.snapshot.return_value = tables
    mount_crawler = create_autospec(Mounts)
    mount_crawler.snapshot.return_value = [
        Mount('/mnt/folder1', 'abfss://container1@storage1.dfs.core.windows.net/folder1')
    ]

    spn_crawler = create_autospec(AzureServicePrincipalCrawler)
    spn_crawler.get_cluster_to_storage_mapping.return_value = cluster_spn
    azure_locations = azure_acl(w, install, cluster_spn)
    return PrincipalACL(
        w, sql_backend, install, table_crawler, mount_crawler, azure_locations.get_eligible_locations_principals()
    )


@pytest.fixture
def installation():
    return MockInstallation(
        {
            "config.yml": {'warehouse_id': 'abc', 'connect': {'host': 'a', 'token': 'b'}, 'inventory_database': 'ucx'},
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'abfss://container1@storage1.dfs.core.windows.net',
                    'storage_account': 'storage1',
                    'client_id': 'client1',
                    'principal': 'principal_1',
                    'privilege': 'WRITE_FILES',
                    'role_name': 'Storage Blob Data Contributor',
                    'type': 'Application',
                    'directory_id': 'directory_id_ss1',
                },
                {
                    'prefix': 'abfss://container1@storage2.dfs.core.windows.net',
                    'storage_account': 'storage1',
                    'client_id': 'client2',
                    'principal': 'principal_1',
                    'privilege': 'READ_FILES',
                    'role_name': 'Storage Blob Data Reader',
                    'type': 'Application',
                    'directory_id': 'directory_id_ss1',
                },
                {
                    'prefix': 'abfss://container1@storage3.dfs.core.windows.net',
                    'storage_account': 'storage3',
                    'client_id': 'client2',
                    'principal': 'principal_1',
                    'privilege': 'WRITE_FILES',
                    'role_name': 'Storage Blob Data Contributor',
                    'type': 'Application',
                    'directory_id': 'directory_id_ss1',
                },
            ],
        }
    )


def test_get_eligible_locations_principals_no_cluster_mapping(ws, installation):
    locations = azure_acl(ws, installation, [])
    locations.get_eligible_locations_principals()
    ws.external_locations.list.assert_not_called()


def test_get_eligible_locations_principals_no_external_location(ws, installation):
    cluster_spn = ServicePrincipalClusterMapping(
        'abc', {AzureServicePrincipalInfo(application_id='Hello, World!', storage_account='abcde')}
    )
    locations = azure_acl(ws, installation, [cluster_spn])
    ws.external_locations.list.return_value = []
    with pytest.raises(ResourceDoesNotExist):
        locations.get_eligible_locations_principals()


def test_get_eligible_locations_principals_no_permission_mapping(ws):
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
    cluster_spn = ServicePrincipalClusterMapping(
        'abc', {AzureServicePrincipalInfo(application_id='client1', storage_account='storage1')}
    )
    locations = azure_acl(ws, installation, [cluster_spn])
    eligible_locations = locations.get_eligible_locations_principals()
    assert len(eligible_locations) == 1
    assert eligible_locations['abc'] == {'abfss://container1@storage1.dfs.core.windows.net/folder1': 'WRITE_FILES'}


def test_interactive_cluster_no_acl(ws, installation):
    cluster_spn = ServicePrincipalClusterMapping(
        'cluster3', {AzureServicePrincipalInfo(application_id='client1', storage_account='storage1')}
    )
    grants = principal_acl(ws, installation, [cluster_spn])
    actual_grants = grants.get_interactive_cluster_grants()
    assert len(actual_grants) == 0


def test_interactive_cluster_single_spn(ws, installation):
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
        Grant('group1', "USE", "hive_metastore", 'schema1'),
        Grant('foo.bar@imagine.com', "USE", "hive_metastore", 'schema1'),
        Grant('group1', "USE", "hive_metastore"),
        Grant('foo.bar@imagine.com', "USE", "hive_metastore"),
    ]
    actual_grants = grants.get_interactive_cluster_grants()
    for grant in expected_grants:
        assert grant in actual_grants


def test_interactive_cluster_multiple_spn(ws, installation):
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
        Grant('spn1', "USE", "hive_metastore", 'schema1'),
        Grant('spn1', "USE", "hive_metastore", 'schema2'),
        Grant('spn1', "USE", "hive_metastore"),
    ]
    actual_grants = grants.get_interactive_cluster_grants()
    for grant in expected_grants:
        assert grant in actual_grants
