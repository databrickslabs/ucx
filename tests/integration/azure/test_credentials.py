import base64
import logging
import re

import pytest
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalInfo
from databricks.labs.ucx.assessment.crawlers import _SECRET_PATTERN
from databricks.labs.ucx.azure.access import StoragePermissionMapping
from databricks.labs.ucx.azure.credentials import ServicePrincipalMigration
from databricks.labs.ucx.azure.resources import AzureResources
from databricks.labs.ucx.hive_metastore import ExternalLocations
from tests.integration.conftest import StaticStorageCredentialManager, \
    StaticAzureResourcePermissions, StaticAzureServicePrincipalCrawler, \
    StaticServicePrincipalMigration


@pytest.fixture
def prepare_spn_migration_test(ws, debug_env, make_random, sql_backend):
    def inner(read_only=False) -> dict:
        spark_conf = ws.clusters.get(debug_env["TEST_LEGACY_SPN_CLUSTER_ID"]).spark_conf

        application_id = spark_conf.get("fs.azure.account.oauth2.client.id")

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

        end_point = spark_conf.get("fs.azure.account.oauth2.client.endpoint")
        directory_id = end_point.split("/")[3]

        random = make_random(6).lower()
        name = f"testinfra_storageaccess_{random}"


        installation = Installation(ws, 'ucx')
        azurerm = AzureResources(ws)
        locations = ExternalLocations(ws, sql_backend, "dont_need_a_schema")

        azure_resource_permissions = StaticAzureResourcePermissions(installation, ws, azurerm, locations, [
            StoragePermissionMapping(
                prefix="abfss://things@labsazurethings.dfs.core.windows.net/avoid_ext_loc_overlap",
                client_id=application_id,
                principal=name,
                privilege="READ_FILES" if read_only else "WRITE_FILES",
                directory_id=directory_id,
            )
        ])

        azure_sp_crawler = StaticAzureServicePrincipalCrawler(ws, sql_backend, "dont_need_a_schema", [
            AzureServicePrincipalInfo(
                application_id=application_id,
                secret_scope=secret_scope,
                secret_key=secret_key,
                tenant_id="test",
                storage_account="test",
            )
        ])

        return {
            "storage_credential_name": name,
            "application_id": application_id,
            "directory_id": directory_id,
            "client_secret": client_secret,
            "azure_resource_permissions": azure_resource_permissions,
            "azure_sp_crawler": azure_sp_crawler,
            "installation": installation,
        }

    return inner


@pytest.fixture
def execute_migration(ws):
    def inner(variables: dict, credentials: list[str]) -> ServicePrincipalMigration:
        spn_migration = StaticServicePrincipalMigration(
            variables["installation"],
            ws,
            variables["azure_resource_permissions"],
            variables["azure_sp_crawler"],
            StaticStorageCredentialManager(ws, credentials)
        )
        return spn_migration.run(
            MockPrompts({"Above Azure Service Principals will be migrated to UC storage credentials *": "Yes"})
        )

    return inner


def test_spn_migration_existed_storage_credential(
        caplog, execute_migration, make_storage_credential_from_spn, prepare_spn_migration_test
):
    caplog.set_level(logging.INFO)
    variables = prepare_spn_migration_test(read_only=False)

    # create a storage credential for this test
    make_storage_credential_from_spn(
        name=variables["storage_credential_name"],
        application_id=variables["application_id"],
        client_secret=variables["client_secret"],
        directory_id=variables["directory_id"],
    )

    # test that the spn migration will be skipped due to above storage credential is existed
    migration_result = execute_migration(variables, [variables["storage_credential_name"]])

    # assert no spn migrated since migration_result will be empty
    assert not migration_result


@pytest.mark.parametrize("read_only", [False, True])
def test_spn_migration(ws, execute_migration, prepare_spn_migration_test, read_only):
    variables = prepare_spn_migration_test(read_only)

    try:
        migration_results = execute_migration(variables, ["lets_migrate_the_spn"])

        storage_credential = ws.storage_credentials.get(variables["storage_credential_name"])
        assert storage_credential is not None
        assert storage_credential.read_only is read_only

        for res in migration_results[0].results:
            if res.operation is None:
                #TODO: file a ticket to SDK team, PATH_EXISTS and HIERARCHICAL_NAMESPACE_ENABLED
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
        ws.storage_credentials.delete(name=variables["storage_credential_name"], force=True)
