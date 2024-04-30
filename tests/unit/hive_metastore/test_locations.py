from unittest.mock import Mock, call, create_autospec

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
TABLE_STORAGE = MockBackend.rows("catalog", "database", "name", "object_type", "table_format", "location")


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


def test_mount_listing_multiple_folders():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/table1/", "table1/", "", "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/table2/", "table2/", "", "")
    folder_table1 = FileInfo("dbfs:/mnt/test_mount/table1/_delta_log/", "_delta_log/", "", "")
    folder_table2 = FileInfo("dbfs:/mnt/test_mount/table2/_SUCCESS", "_SUCCESS", "", "")
    folder_table3 = FileInfo("dbfs:/mnt/test_mount/table2/1.snappy.parquet", "1.snappy.parquet", "", "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder, second_folder]
        if path == "dbfs:/mnt/test_mount/table1/":
            return [folder_table1]
        if path == "dbfs:/mnt/test_mount/table2/":
            return [folder_table2, folder_table3]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': [],
            'test.mounts': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = Mounts(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot()
    assert results == [
        Table("hive_metastore", "mounted_test_mount", "table1", "EXTERNAL", "DELTA", "adls://bucket/table1"),
        Table("hive_metastore", "mounted_test_mount", "table2", "EXTERNAL", "PARQUET", "adls://bucket/table2"),
    ]


def test_mount_listing_sub_folders():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/entity/", "entity/", "", "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/", "domain/", "", "")
    third_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/", "table1/", "", "")
    fourth_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/_delta_log/", "_delta_log/", "", "")
    fourth_folder_parquet = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/1.parquet", "1.parquet", "", "")
    delta_log = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/_delta_log/000.json", "000.json", "", "")
    delta_log_2 = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/_delta_log/001.json", "001.json", "", "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder]
        if path == "dbfs:/mnt/test_mount/entity/":
            return [second_folder]
        if path == "dbfs:/mnt/test_mount/entity/domain/":
            return [third_folder]
        if path == "dbfs:/mnt/test_mount/entity/domain/table1/":
            return [fourth_folder, fourth_folder_parquet]
        if path == "dbfs:/mnt/test_mount/entity/domain/table1/_delta_log/":
            return [delta_log, delta_log_2]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': [],
            'test.mounts': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = Mounts(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot()
    assert results == [
        Table(
            "hive_metastore",
            "mounted_test_mount",
            "table1",
            "EXTERNAL",
            "DELTA",
            "adls://bucket/entity/domain/table1",
        )
    ]


def test_partitioned_parquet_layout():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/entity/", "entity/", "", "")
    first_partition = FileInfo("dbfs:/mnt/test_mount/entity/xxx=yyy/", "xxx=yyy/", "", "")
    first_partition_files = FileInfo("dbfs:/mnt/test_mount/entity/xxx=yyy/1.parquet", "1.parquet", "", "")
    second_partition = FileInfo("dbfs:/mnt/test_mount/entity/xxx=zzz/", "xxx=zzz/", "", "")
    second_partition_files = FileInfo("dbfs:/mnt/test_mount/entity/xxx=zzz/1.parquet", "1.parquet", "", "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder]
        if path == "dbfs:/mnt/test_mount/entity/":
            return [first_partition, second_partition]
        if path == "dbfs:/mnt/test_mount/entity/xxx=yyy/":
            return [first_partition_files]
        if path == "dbfs:/mnt/test_mount/entity/xxx=zzz/":
            return [second_partition_files]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': [],
            'test.mounts': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = Mounts(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot()
    assert results == [
        Table(
            "hive_metastore",
            "mounted_test_mount",
            "entity",
            "EXTERNAL",
            "PARQUET",
            "adls://bucket/entity",
            is_partitioned=True,
        )
    ]


def test_partitioned_delta():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/entity/", "entity/", "", "")
    first_first_partition = FileInfo("dbfs:/mnt/test_mount/entity/xxx=yyy/", "xxx=yyy/", "", "")
    first_first_partition_files = FileInfo("dbfs:/mnt/test_mount/entity/xxx=yyy/1.parquet", "1.parquet", "", "")
    first_second_partition = FileInfo("dbfs:/mnt/test_mount/entity/xxx=zzz/", "xxx=zzz/", "", "")
    first_second_partition_files = FileInfo("dbfs:/mnt/test_mount/entity/xxx=zzz/1.parquet", "1.parquet", "", "")
    first_delta_log = FileInfo("dbfs:/mnt/test_mount/entity/_delta_log/", "_delta_log/", "", "")

    second_folder = FileInfo("dbfs:/mnt/test_mount/entity_2/", "entity_2/", "", "")
    second_first_partition = FileInfo("dbfs:/mnt/test_mount/entity_2/xxx=yyy/", "xxx=yyy/", "", "")
    second_second_partition = FileInfo("dbfs:/mnt/test_mount/entity_2/xxx=yyy/aaa=bbb/", "aaa=bbb/", "", "")
    second_second_partition_files = FileInfo(
        "dbfs:/mnt/test_mount/entity_2/xxx=yyy/aaa=bbb/1.parquet", "1.parquet", "", ""
    )
    second_delta_log = FileInfo("dbfs:/mnt/test_mount/entity_2/_delta_log/", "_delta_log/", "", "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder, second_folder]
        if path == "dbfs:/mnt/test_mount/entity/":
            return [first_delta_log, first_first_partition, first_second_partition]
        if path == "dbfs:/mnt/test_mount/entity/xxx=yyy/":
            return [first_first_partition_files]
        if path == "dbfs:/mnt/test_mount/entity/xxx=zzz/":
            return [first_second_partition_files]
        if path == "dbfs:/mnt/test_mount/entity_2/":
            return [second_first_partition, second_delta_log]
        if path == "dbfs:/mnt/test_mount/entity_2/xxx=yyy/":
            return [second_second_partition]
        if path == "dbfs:/mnt/test_mount/entity_2/xxx=yyy/aaa=bbb/":
            return [second_second_partition_files]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': [],
            'test.mounts': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = Mounts(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot()
    assert len(results) == 2
    assert results[0].table_format == "DELTA"
    assert results[0].is_partitioned
    assert results[1].table_format == "DELTA"
    assert results[1].is_partitioned


def test_filtering_irrelevant_paths():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/table1/", "table1/", "", "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/$_azuretempfolder/", "$_azuretempfolder/", "", "")
    first_folder_delta_log = FileInfo("dbfs:/mnt/test_mount/table1/_delta_log/", "_delta_log/", "", "")
    second_folder_delta_log = FileInfo("dbfs:/mnt/test_mount/$_azuretempfolder/_delta_log/", "_delta_log/", "", "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder, second_folder]
        if path == "dbfs:/mnt/test_mount/table1/":
            return [first_folder_delta_log]
        if path == "dbfs:/mnt/test_mount/$_azuretempfolder/":
            return [second_folder_delta_log]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': [],
            'test.mounts': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = Mounts(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts, exclude_paths_in_mount=["$_azuretempfolder"]).snapshot()
    assert results == [
        Table("hive_metastore", "mounted_test_mount", "table1", "EXTERNAL", "DELTA", "adls://bucket/table1"),
    ]


def test_filter_irrelevant_mounts():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("/mnt/test_mount/table1/", "table1/", "", "")
    second_folder = FileInfo("/mnt/test_mount2/table2/", "table2/", "", "")
    first_folder_delta_log = FileInfo("/mnt/test_mount/table1/_delta_log/", "_delta_log/", "", "")
    second_folder_delta_log = FileInfo("/mnt/test_mount2/table2/_delta_log/", "_delta_log/", "", "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder]
        if path == "/mnt/test_mount2/":
            return [second_folder]
        if path == "/mnt/test_mount/table1/":
            return [first_folder_delta_log]
        if path == "/mnt/test_mount/table2/":
            return [second_folder_delta_log]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': [],
            'test.mounts': MOUNT_STORAGE[("/mnt/test_mount", ""), ("/mnt/test_mount2", "")],
        }
    )
    mounts = Mounts(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts, include_mounts=["/mnt/test_mount"]).snapshot()

    assert results == [
        Table("hive_metastore", "mounted_test_mount", "table1", "EXTERNAL", "DELTA", "/mnt/test_mount/table1"),
    ]
    client.dbutils.fs.ls.assert_has_calls([call('/mnt/test_mount'), call('/mnt/test_mount/table1/')])


def test_historical_data_should_be_overwritten():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/table1/", "table1/", "", "")
    second_folder = FileInfo("dbfs:/mnt/test_mount2/table2/", "table2/", "", "")
    first_folder_delta_log = FileInfo("dbfs:/mnt/test_mount/table1/_delta_log/", "_delta_log/", "", "")
    second_folder_delta_log = FileInfo("dbfs:/mnt/test_mount2/table2/_delta_log/", "_delta_log/", "", "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder]
        if path == "dbfs:/mnt/test_mount2/":
            return [second_folder]
        if path == "dbfs:/mnt/test_mount/table1/":
            return [first_folder_delta_log]
        if path == "dbfs:/mnt/test_mount/table2/":
            return [second_folder_delta_log]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': TABLE_STORAGE[
                ("catalog", "database", "name", "object_type", "table_format", "location")
            ],
            'test.mounts': MOUNT_STORAGE[("/mnt/test_mount", "abfss://bucket@windows/")],
        }
    )
    mounts = Mounts(backend, client, "test")
    TablesInMounts(backend, client, "test", mounts).snapshot()
    assert backend.rows_written_for("hive_metastore.test.tables", "overwrite") == [
        Row(
            catalog='hive_metastore',
            database='mounted_test_mount',
            name='table1',
            object_type='EXTERNAL',
            table_format='DELTA',
            location='abfss://bucket@windows/table1',
            view_text=None,
            upgraded_to=None,
            storage_properties=None,
            is_partitioned=False,
        ),
        Row(
            catalog='catalog',
            database='database',
            name='name',
            object_type='object_type',
            table_format='table_format',
            location='location',
            view_text=None,
            upgraded_to=None,
            storage_properties=None,
            is_partitioned=False,
        ),
    ]


def test_mount_include_paths():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/table1/", "table1/", "", "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/table2/", "table2/", "", "")
    folder_table1 = FileInfo("dbfs:/mnt/test_mount/table1/_delta_log/", "_delta_log/", "", "")
    folder_table2 = FileInfo("dbfs:/mnt/test_mount/table2/_SUCCESS", "_SUCCESS", "", "")
    folder_table3 = FileInfo("dbfs:/mnt/test_mount/table2/1.snappy.parquet", "1.snappy.parquet", "", "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder, second_folder]
        if path == "dbfs:/mnt/test_mount/table1/":
            return [folder_table1]
        if path == "dbfs:/mnt/test_mount/table2/":
            return [folder_table2, folder_table3]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': [],
            'test.mounts': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = Mounts(backend, client, "test")
    results = TablesInMounts(
        backend, client, "test", mounts, include_paths_in_mount=["dbfs:/mnt/test_mount/table2/"]
    ).snapshot()
    assert results == [
        Table("hive_metastore", "mounted_test_mount", "table2", "EXTERNAL", "PARQUET", "adls://bucket/table2"),
    ]


def test_mount_listing_csv_json():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/entity/", "entity/", "", "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/", "domain/", "", "")
    second_folder_random_csv = FileInfo("dbfs:/mnt/test_mount/entity/domain/test.csv", "test.csv", "", "")
    third_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/", "table1/", "", "")
    first_json = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/some_jsons.json", "some_jsons.json", "", "")
    second_json = FileInfo(
        "dbfs:/mnt/test_mount/entity/domain/table1/some_other_jsons.json", "some_other_jsons.json", "", ""
    )

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder]
        if path == "dbfs:/mnt/test_mount/entity/":
            return [second_folder, second_folder_random_csv]
        if path == "dbfs:/mnt/test_mount/entity/domain/":
            return [third_folder]
        if path == "dbfs:/mnt/test_mount/entity/domain/table1/":
            return [first_json, second_json]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': [],
            'test.mounts': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = Mounts(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot()
    assert results == [
        Table(
            "hive_metastore",
            "mounted_test_mount",
            "table1",
            "EXTERNAL",
            "JSON",
            "adls://bucket/entity/domain/table1",
        ),
        Table(
            "hive_metastore",
            "mounted_test_mount",
            "entity",
            "EXTERNAL",
            "CSV",
            "adls://bucket/entity",
        ),
    ]


def test_mount_listing_seen_tables():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/table1/", "table1/", "", "")
    folder_table1 = FileInfo("dbfs:/mnt/test_mount/table1/_delta_log/", "_delta_log/", "", "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/table2/", "table2/", "", "")
    second_folder1 = FileInfo("dbfs:/mnt/test_mount/table2/_delta_log/", "_delta_log/", "", "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder, second_folder]
        if path == "dbfs:/mnt/test_mount/table1/":
            return [folder_table1]
        if path == "dbfs:/mnt/test_mount/table2/":
            return [second_folder1]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': TABLE_STORAGE[
                ("hive_metastore", "database", "name", "EXTERNAL", "DELTA", "adls://bucket/table1"),
                ("hive_metastore", "database", "name_2", "EXTERNAL", "DELTA", "dbfs:/mnt/test_mount/table2"),
                ("hive_metastore", "database", "name_3", "MANAGED", "DELTA", None),
            ],
            'test.mounts': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = Mounts(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot()
    assert len(results) == 3
    assert results[0].location == "adls://bucket/table1"
    assert results[1].location == "dbfs:/mnt/test_mount/table2"
    assert results[2].location is None
