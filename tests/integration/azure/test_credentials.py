import base64
import re
from dataclasses import dataclass

import pytest
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalInfo
from databricks.labs.ucx.assessment.crawlers import _SECRET_PATTERN
from databricks.labs.ucx.azure.access import StoragePermissionMapping
from databricks.labs.ucx.azure.credentials import StorageCredentialValidationResult
from databricks.labs.ucx.azure.resources import AzureResources
from databricks.labs.ucx.hive_metastore import ExternalLocations
from tests.integration.conftest import (
    StaticResourcePermissions,
    StaticServicePrincipalCrawler,
    StaticServicePrincipalMigration,
    StaticStorageCredentialManager,
)


@dataclass
class MigrationTestInfo:
    credential_name: str
    application_id: str
    directory_id: str
    secret_scope: str
    secret_key: str
    client_secret: str


@pytest.fixture
def extract_test_info(ws, debug_env, make_random):
    random = make_random(6).lower()
    credential_name = f"testinfra_storageaccess_{random}"

    spark_conf = ws.clusters.get(debug_env["TEST_LEGACY_SPN_CLUSTER_ID"]).spark_conf

    application_id = spark_conf.get("fs.azure.account.oauth2.client.id")

    end_point = spark_conf.get("fs.azure.account.oauth2.client.endpoint")
    directory_id = end_point.split("/")[3]

    secret_matched = re.search(_SECRET_PATTERN, spark_conf.get("fs.azure.account.oauth2.client.secret"))
    if secret_matched:
        secret_scope, secret_key = (
            secret_matched.group(1).split("/")[1],
            secret_matched.group(1).split("/")[2],
        )
    assert secret_scope is not None
    assert secret_key is not None

    secret_response = ws.secrets.get_secret(secret_scope, secret_key)
    client_secret = base64.b64decode(secret_response.value).decode("utf-8")

    return MigrationTestInfo(credential_name, application_id, directory_id, secret_scope, secret_key, client_secret)


@pytest.fixture
def run_migration(ws, sql_backend):
    def inner(
        test_info: MigrationTestInfo, credentials: list[str], read_only=False
    ) -> list[StorageCredentialValidationResult]:
        installation = Installation(ws, 'ucx')
        azurerm = AzureResources(ws)
        locations = ExternalLocations(ws, sql_backend, "dont_need_a_schema")

        permission_mappings = [
            StoragePermissionMapping(
                prefix="abfss://things@labsazurethings.dfs.core.windows.net/avoid_ext_loc_overlap",
                client_id=test_info.application_id,
                principal=test_info.credential_name,
                privilege="READ_FILES" if read_only else "WRITE_FILES",
                directory_id=test_info.directory_id,
            )
        ]
        resource_permissions = StaticResourcePermissions(permission_mappings, installation, ws, azurerm, locations)

        sp_infos = [
            AzureServicePrincipalInfo(
                application_id=test_info.application_id,
                secret_scope=test_info.secret_scope,
                secret_key=test_info.secret_key,
                tenant_id="test",
                storage_account="test",
            )
        ]
        sp_crawler = StaticServicePrincipalCrawler(sp_infos, ws, sql_backend, "dont_need_a_schema")

        spn_migration = StaticServicePrincipalMigration(
            installation, ws, resource_permissions, sp_crawler, StaticStorageCredentialManager(ws, credentials)
        )
        return spn_migration.run(
            MockPrompts({"Above Azure Service Principals will be migrated to UC storage credentials *": "Yes"})
        )

    return inner


def test_spn_migration_existed_storage_credential(extract_test_info, make_storage_credential_from_spn, run_migration):
    # create a storage credential for this test
    make_storage_credential_from_spn(
        name=extract_test_info.credential_name,
        application_id=extract_test_info.application_id,
        client_secret=extract_test_info.client_secret,
        directory_id=extract_test_info.directory_id,
    )

    # test that the spn migration will be skipped due to above storage credential is existed
    migration_result = run_migration(extract_test_info, [extract_test_info.credential_name])

    # assert no spn migrated since migration_result will be empty
    assert not migration_result


@pytest.mark.parametrize("read_only", [False, True])
def test_spn_migration(ws, extract_test_info, run_migration, read_only):
    try:
        migration_results = run_migration(extract_test_info, ["lets_migrate_the_spn"], read_only)

        storage_credential = ws.storage_credentials.get(extract_test_info.credential_name)
        assert storage_credential is not None
        assert storage_credential.read_only is read_only

        # assert the storage credential validation results
        for res in migration_results[0].results:
            if res.operation is None:
                # TODO: file a ticket to SDK team, PATH_EXISTS and HIERARCHICAL_NAMESPACE_ENABLED
                # should be added to the validation operations. They are None right now.
                # Once it's fixed, the None check here can be removed
                continue
            if read_only:
                if res.operation.value in ("WRITE", "DELETE"):
                    # We only assert that write validation are not performed for read only storage credential here.
                    # In real life, the READ validation for read only storage credential may fail if there is no file,
                    # but that is fine, as the storage credential is created, and we just cannot validate it until it's really used.
                    assert False, "WRITE operation should not be checked for read-only storage credential"
            if not read_only:
                if res.result.value == "FAIL":
                    assert False, f"{res.operation.value} operation is failed while validating storage credential"
    finally:
        ws.storage_credentials.delete(extract_test_info.credential_name, force=True)
