import base64
import re
from dataclasses import dataclass

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalInfo
from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.credentials import (
    ServicePrincipalMigration,
    StorageCredentialManager,
    StorageCredentialValidationResult,
)
from databricks.labs.ucx.azure.resources import AzureResources
from databricks.labs.ucx.hive_metastore import ExternalLocations
from tests.integration.conftest import StaticServicePrincipalCrawler


@dataclass
class MigrationTestInfo:
    credential_name: str
    application_id: str
    directory_id: str
    secret_scope: str
    secret_key: str
    client_secret: str


@pytest.fixture
def extract_test_info(ws, env_or_skip, make_random):
    random = make_random(6).lower()
    credential_name = f"testinfra_storageaccess_{random}"

    spark_conf = ws.clusters.get(env_or_skip("TEST_LEGACY_SPN_CLUSTER_ID")).spark_conf

    application_id = spark_conf.get("fs.azure.account.oauth2.client.id")

    end_point = spark_conf.get("fs.azure.account.oauth2.client.endpoint")
    directory_id = end_point.split("/")[3]

    secret_matched = re.findall(r"{{secrets\/(.*)\/(.*)}}", spark_conf.get("fs.azure.account.oauth2.client.secret"))
    secret_scope = secret_matched[0][0]
    secret_key = secret_matched[0][1]
    assert secret_scope is not None
    assert secret_key is not None

    secret_response = ws.secrets.get_secret(secret_scope, secret_key)
    client_secret = base64.b64decode(secret_response.value).decode("utf-8")

    return MigrationTestInfo(credential_name, application_id, directory_id, secret_scope, secret_key, client_secret)


@pytest.fixture
def run_migration(ws, sql_backend):
    def inner(
        test_info: MigrationTestInfo, credentials: set[str], read_only=False
    ) -> list[StorageCredentialValidationResult]:
        azurerm = AzureResources(ws)
        locations = ExternalLocations(ws, sql_backend, "dont_need_a_schema")

        installation = MockInstallation(
            {
                "azure_storage_account_info.csv": [
                    {
                        'prefix': 'abfss://things@labsazurethings.dfs.core.windows.net/avoid_ext_loc_overlap',
                        'client_id': test_info.application_id,
                        'principal': test_info.credential_name,
                        'privilege': "READ_FILES" if read_only else "WRITE_FILES",
                        'directory_id': test_info.directory_id,
                    },
                ]
            }
        )
        resource_permissions = AzureResourcePermissions(installation, ws, azurerm, locations)

        sp_infos = [
            AzureServicePrincipalInfo(
                test_info.application_id,
                test_info.secret_scope,
                test_info.secret_key,
                "test",
                "test",
            )
        ]
        sp_crawler = StaticServicePrincipalCrawler(sp_infos, ws, sql_backend, "dont_need_a_schema")

        spn_migration = ServicePrincipalMigration(
            installation, ws, resource_permissions, sp_crawler, StorageCredentialManager(ws)
        )
        return spn_migration.run(
            MockPrompts({"Above Azure Service Principals will be migrated to UC storage credentials *": "Yes"}),
            credentials,
        )

    return inner


def test_spn_migration_existed_storage_credential(extract_test_info, make_storage_credential_spn, run_migration):
    # create a storage credential for this test
    make_storage_credential_spn(
        credential_name=extract_test_info.credential_name,
        application_id=extract_test_info.application_id,
        client_secret=extract_test_info.client_secret,
        directory_id=extract_test_info.directory_id,
    )

    # test that the spn migration will be skipped due to above storage credential is existed
    migration_result = run_migration(extract_test_info, {extract_test_info.credential_name})

    # assert no spn migrated since migration_result will be empty
    assert not migration_result


@pytest.mark.parametrize("read_only", [False, True])
def test_spn_migration(ws, extract_test_info, run_migration, read_only):
    try:
        migration_results = run_migration(extract_test_info, {"lets_migrate_the_spn"}, read_only)
        storage_credential = ws.storage_credentials.get(extract_test_info.credential_name)
    finally:
        ws.storage_credentials.delete(extract_test_info.credential_name, force=True)

    assert storage_credential is not None
    assert storage_credential.read_only is read_only

    if read_only:
        failures = migration_results[0].failures
        # in this test LIST should fail as validation path does not exist
        assert failures
        match = re.match(r"LIST validation failed with message: .*The specified path does not exist", failures[0])
        assert match is not None, "LIST validation should fail"
    else:
        # all validation should pass
        assert not migration_results[0].failures
