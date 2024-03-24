from unittest.mock import Mock, create_autospec

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.dbutils import FileInfo, MountInfo
from databricks.sdk.service.catalog import ExternalLocationInfo

from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocation,
    ExternalLocations,
    Mounts,
    TablesInMounts,
)
from databricks.labs.ucx.hive_metastore.tables import Table


def test_list_mounts_should_return_a_list_of_mount_without_encryption_type():
    client = create_autospec(WorkspaceClient)
    client.dbutils.fs.mounts.return_value = [
        MountInfo("mp_1", "path_1", "info_1"),
        MountInfo("mp_2", "path_2", "info_2"),
        MountInfo("mp_3", "path_3", "info_3"),
    ]

    backend = MockBackend()
    instance = Mounts(backend, client, "test")

    instance.inventorize_mounts()

    assert [
        Row(name="mp_1", source="path_1"),
        Row(name="mp_2", source="path_2"),
        Row(name="mp_3", source="path_3"),
    ] == backend.rows_written_for("hive_metastore.test.mounts", "append")


def test_list_mounts_should_return_a_deduped_list_of_mount_without_encryption_type():
    client = create_autospec(WorkspaceClient)
    client.dbutils.fs.mounts.return_value = [
        MountInfo("mp_1", "path_1", "info_1"),
        MountInfo("mp_2", "path_2", "info_2"),
        MountInfo("mp_2", "path_2", "info_2"),
    ]

    backend = MockBackend()
    instance = Mounts(backend, client, "test")

    instance.inventorize_mounts()

    assert [
        Row(name="mp_1", source="path_1"),
        Row(name="mp_2", source="path_2"),
    ] == backend.rows_written_for("hive_metastore.test.mounts", "append")


def test_list_mounts_should_return_a_deduped_list_of_mount_without_variable_volume_names():
    client = create_autospec(WorkspaceClient)
    client.dbutils.fs.mounts.return_value = [
        MountInfo("/Volume", "DbfsReserved", "info_1"),
        MountInfo("/Volumes", "DbfsReserved", "info_2"),
        MountInfo("/volume", "DbfsReserved", "info_3"),
        MountInfo("/volumes", "DbfsReserved", "info_4"),
        MountInfo("mp_1", "path_1", "info_1"),
        MountInfo("mp_2", "path_2", "info_2"),
        MountInfo("mp_2", "path_2", "info_2"),
    ]

    backend = MockBackend()
    instance = Mounts(backend, client, "test")

    instance.inventorize_mounts()

    expected = [
        Row(name="/Volume", source="DbfsReserved"),
        Row(name="mp_1", source="path_1"),
        Row(name="mp_2", source="path_2"),
    ]
    assert expected == backend.rows_written_for("hive_metastore.test.mounts", "append")


def test_external_locations():
    row_factory = type("Row", (Row,), {"__columns__": ["location", "storage_properties"]})
    sql_backend = MockBackend(
        rows={
            'SELECT location, storage_properties FROM test.tables WHERE location IS NOT NULL': [
                row_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/Table", ""]),
                row_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/Table2", ""]),
                row_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/testloc/Table3", ""]),
                row_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/anotherloc/Table4", ""]),
                row_factory(["dbfs:/mnt/ucx/database1/table1", ""]),
                row_factory(["dbfs:/mnt/ucx/database2/table2", ""]),
                row_factory(["DatabricksRootmntDatabricksRoot", ""]),
                row_factory(
                    [
                        "jdbc:databricks://",
                        "[personalAccessToken=*********(redacted), \
            httpPath=/sql/1.0/warehouses/65b52fb5bd86a7be, host=dbc-test1-aa11.cloud.databricks.com, \
            dbtable=samples.nyctaxi.trips]",
                    ]
                ),
                row_factory(
                    [
                        "jdbc:/MYSQL",
                        "[database=test_db, host=somemysql.us-east-1.rds.amazonaws.com, \
                port=3306, dbtable=movies, user=*********(redacted), password=*********(redacted)]",
                    ]
                ),
                row_factory(
                    [
                        "jdbc:providerknown:/",
                        "[database=test_db, host=somedb.us-east-1.rds.amazonaws.com, \
                port=1234, dbtable=sometable, user=*********(redacted), password=*********(redacted), \
                provider=providerknown]",
                    ]
                ),
                row_factory(
                    [
                        "jdbc:providerknown:/",
                        "[database=test_db, host=somedb.us-east-1.rds.amazonaws.com, \
                port=1234, dbtable=sometable2, user=*********(redacted), password=*********(redacted), \
                provider=providerknown]",
                    ]
                ),
                row_factory(
                    [
                        "jdbc:providerunknown:/",
                        "[database=test_db, host=somedb.us-east-1.rds.amazonaws.com, \
                port=1234, dbtable=sometable, user=*********(redacted), password=*********(redacted)]",
                    ]
                ),
            ],
            r"SELECT \* FROM test.mounts": [
                ("/mnt/ucx", "s3://us-east-1-ucx-container"),
            ],
        }
    )
    crawler = ExternalLocations(Mock(), sql_backend, "test")
    result_set = crawler.snapshot()
    assert len(result_set) == 7
    assert result_set[0].location == "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/"
    assert result_set[0].table_count == 2
    assert result_set[1].location == "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/"
    assert (
        result_set[3].location
        == "jdbc:databricks://dbc-test1-aa11.cloud.databricks.com;httpPath=/sql/1.0/warehouses/65b52fb5bd86a7be"
    )
    assert result_set[4].location == "jdbc:mysql://somemysql.us-east-1.rds.amazonaws.com:3306/test_db"
    assert result_set[5].location == "jdbc:providerknown://somedb.us-east-1.rds.amazonaws.com:1234/test_db"
    assert result_set[6].location == "jdbc:providerunknown://somedb.us-east-1.rds.amazonaws.com:1234/test_db"


LOCATION_STORAGE = MockBackend.rows("location", "storage_properties")
MOUNT_STORAGE = MockBackend.rows("name", "source")


def test_save_external_location_mapping_missing_location():
    ws = create_autospec(WorkspaceClient)
    sbe = MockBackend(
        rows={
            "SELECT location, storage_properties FROM test.tables WHERE location IS NOT NULL": LOCATION_STORAGE[
                ("s3://test_location/test1/table1", ""),
                ("gcs://test_location2/test2/table2", ""),
                ("abfss://cont1@storagetest1.dfs.core.windows.net/test2/table3", ""),
            ],
        }
    )
    location_crawler = ExternalLocations(ws, sbe, "test")
    ws.external_locations.list.return_value = [ExternalLocationInfo(name="loc1", url="s3://test_location/test11")]

    installation = create_autospec(Installation)
    location_crawler.save_as_terraform_definitions_on_workspace(installation)

    installation.upload.assert_called_with(
        "external_locations.tf",
        (
            'resource "databricks_external_location" "test_location_test1" { \n'
            '    name = "test_location_test1"\n'
            '    url  = "s3://test_location/test1"\n'
            "    credential_name = databricks_storage_credential.<storage_credential_reference>.id\n"
            "}\n\n"
            'resource "databricks_external_location" "test_location2_test2" { \n'
            '    name = "test_location2_test2"\n'
            '    url  = "gcs://test_location2/test2"\n'
            "    credential_name = databricks_storage_credential.<storage_credential_reference>.id\n"
            "}\n\n"
            'resource "databricks_external_location" "cont1_storagetest1_test2" { \n'
            '    name = "cont1_storagetest1_test2"\n'
            '    url  = "abfss://cont1@storagetest1.dfs.core.windows.net/test2"\n'
            "    credential_name = databricks_storage_credential.<storage_credential_reference>.id\n"
            "}\n"
        ).encode("utf8"),
    )


def test_save_external_location_mapping_no_missing_location():
    ws = create_autospec(WorkspaceClient)
    sbe = MockBackend(
        rows={
            "SELECT location, storage_properties FROM test.tables WHERE location IS NOT NULL": LOCATION_STORAGE[
                ("s3://test_location/test1/table1", ""),
            ],
        }
    )
    location_crawler = ExternalLocations(ws, sbe, "test")
    ws.external_locations.list.return_value = [ExternalLocationInfo(name="loc1", url="s3://test_location/test1")]
    location_crawler.save_as_terraform_definitions_on_workspace("~/.ucx")
    ws.workspace.upload.assert_not_called()


def test_match_table_external_locations():
    ws = create_autospec(WorkspaceClient)
    sbe = MockBackend(
        rows={
            "SELECT location, storage_properties FROM test.tables WHERE location IS NOT NULL": LOCATION_STORAGE[
                ("s3://test_location/a/b/c/table1", ""),
                ("s3://test_location/a/b/table1", ""),
                ("gcs://test_location2/a/b/table2", ""),
                ("abfss://cont1@storagetest1/a/table3", ""),
                ("abfss://cont1@storagetest1/a/table4", ""),
            ],
        }
    )
    location_crawler = ExternalLocations(ws, sbe, "test")
    ws.external_locations.list.return_value = [ExternalLocationInfo(name="loc1", url="s3://test_location/a")]

    matching_locations, missing_locations = location_crawler.match_table_external_locations()
    assert len(matching_locations) == 1
    assert ExternalLocation("gcs://test_location2/a/b/", 1) in missing_locations
    assert ExternalLocation("abfss://cont1@storagetest1/a/", 2) in missing_locations


def test_mount_listing_one_table():
    client = create_autospec(WorkspaceClient)
    client.dbutils.fs.ls.return_value = [
        FileInfo("/mnt/lmao/_delta_log", "_delta_log", "", ''),
        FileInfo("/mnt/lmao/xxx.parquet", "xxx.parquet", "", ''),
        FileInfo("/mnt/lmao/yyy.parquet", "yyy.parquet", "", ''),
    ]
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': [],
            'test.mounts': MOUNT_STORAGE[("/mnt/lmao", "")],
        }
    )
    mounts = Mounts(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot()
    assert results == [Table("hive_metastore", "", "lmao", "EXTERNAL", "DELTA", "/mnt/lmao")]
