from databricks.labs.blueprint.installation import MockInstallation

from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.locations import ExternalLocationsMigration
from databricks.labs.ucx.azure.resources import AzureResources
from databricks.labs.ucx.hive_metastore import ExternalLocations
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation


def test_run(caplog, ws, sql_backend, inventory_schema):
    azurerm = AzureResources(ws)

    locations = [
        ExternalLocation("abfss://uctest@ziyuanqintest.dfs.core.windows.net/one", 1),
        ExternalLocation("abfss://uctest@ziyuanqintest.dfs.core.windows.net/two", 2),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", locations, ExternalLocation)
    location_crawler = ExternalLocations(ws, sql_backend, inventory_schema)

    installation = MockInstallation(
        {
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'abfss://uctest@ziyuanqintest.dfs.core.windows.net/',
                    'client_id': "0cb5b9c2-b5e3-4bb5-89a8-ebcef6708997",
                    'principal': "labsazurethings-access",
                    'privilege': "WRITE_FILES",
                    'type': "ManagedIdentity",
                },
            ]
        }
    )
    resource_permissions = AzureResourcePermissions(installation, ws, azurerm, location_crawler)

    location_migration = ExternalLocationsMigration(ws, location_crawler, resource_permissions, azurerm)

    location_migration.run()

    assert "All UC external location are created." in caplog.text


# def test_run(ws, env_or_skip, sql_backend, inventory_schema, make_random):
#     azurerm = AzureResources(ws)
#
#     locations = [
#         ExternalLocation("abfss://things@labsazurethings.dfs.core.windows.net/one", 1),
#         ExternalLocation("abfss://things@labsazurethings.dfs.core.windows.net/two", 2),
#     ]
#     sql_backend.save_table(f"{inventory_schema}.external_locations", locations, ExternalLocation)
#     location_crawler = ExternalLocations(ws, sql_backend, inventory_schema)
#
#     installation = MockInstallation({
#         "azure_storage_account_info.csv": [
#             {
#                 'prefix': 'abfss://things@labsazurethings.dfs.core.windows.net/',
#                 'client_id': "9cbd8a1a-8169-4ff8-a929-f1e9572c090c",
#                 'principal': "labsazurethings-access",
#                 'privilege': "WRITE_FILES",
#                 'directory_id': "9f37a392-f0ae-4280-9796-f1864a10effc",
#             },
#         ]
#     })
#     resource_permissions = AzureResourcePermissions(installation, ws, azurerm, location_crawler)
#
#     location_migration = ExternalLocationsMigration(ws, location_crawler, resource_permissions, azurerm)
#
#     location_migration.run()
