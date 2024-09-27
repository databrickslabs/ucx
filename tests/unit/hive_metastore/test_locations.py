from unittest.mock import Mock, call, create_autospec

import pytest
import sys
from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.dbutils import FileInfo, MountInfo
from databricks.sdk.service.catalog import ExternalLocationInfo

from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocation,
    ExternalLocations,
    LocationTrie,
    MountsCrawler,
    TablesInMounts,
    Mount,
)
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler


@pytest.mark.parametrize(
    "location",
    [
        "s3://databricks-e2demofieldengwest/b169/b50",
        "s3a://databricks-datasets-oregon/delta-sharing/share/open-datasets.share",
        "s3n://bucket-name/path-to-file-in-bucket",
        "gcs://test_location2/test2/table2",
        "abfss://cont1@storagetest1.dfs.core.windows.net/test2/table3",
    ],
)
def test_location_trie_valid_and_full_location(location):
    table = Table("catalog", "database", "table", "TABLE", "DELTA", location)
    trie = LocationTrie()
    trie.insert(table)
    node = trie.find(table)
    assert node is not None
    assert node.is_valid()
    assert node.location == location


@pytest.mark.parametrize(
    "location",
    ["s3:/missing-slash", "//missing-scheme", "gcs:/missing-netloc/path", "unsupported-file-scheme://bucket"],
)
def test_location_trie_invalid_location(location):
    table = Table("catalog", "database", "table", "TABLE", "DELTA", location)
    trie = LocationTrie()
    trie.insert(table)
    node = trie.find(table)
    assert not node.is_valid()


def test_location_trie_has_children():
    locations = ["s3://bucket/a/b/c", "s3://bucket/a/b/d", "s3://bucket/a/b/d/g"]
    tables = [Table("catalog", "database", "table", "TABLE", "DELTA", location) for location in locations]
    trie = LocationTrie()
    for table in tables:
        trie.insert(table)

    c_node = trie.find(tables[0])
    assert not c_node.has_children()

    d_node = trie.find(tables[1])
    assert d_node.has_children()


def test_location_trie_tables():
    locations = ["s3://bucket/a/b/c", "s3://bucket/a/b/c"]
    tables = [Table("catalog", "database", "table", "TABLE", "DELTA", location) for location in locations]
    trie = LocationTrie()
    for table in tables:
        trie.insert(table)

    c_node = trie.find(tables[0])
    assert c_node.tables == tables


def test_mounts_inventory_should_contain_mounts_without_encryption_type():
    client = create_autospec(WorkspaceClient)
    client.dbutils.fs.mounts.return_value = [
        MountInfo("mp_1", "path_1", "info_1"),
        MountInfo("mp_2", "path_2", "info_2"),
        MountInfo("mp_3", "path_3", "info_3"),
    ]

    backend = MockBackend()
    instance = MountsCrawler(backend, client, "test")

    instance.snapshot()

    assert [
        Row(name="mp_1", source="path_1"),
        Row(name="mp_2", source="path_2"),
        Row(name="mp_3", source="path_3"),
    ] == backend.rows_written_for("hive_metastore.test.mounts", "overwrite")


def test_mounts_inventory_should_contain_deduped_mounts_without_encryption_type():
    client = create_autospec(WorkspaceClient)
    client.dbutils.fs.mounts.return_value = [
        MountInfo("mp_1", "path_1", "info_1"),
        MountInfo("mp_2", "path_2", "info_2"),
        MountInfo("mp_2", "path_2", "info_2"),
    ]

    backend = MockBackend()
    instance = MountsCrawler(backend, client, "test")

    instance.snapshot()

    assert [
        Row(name="mp_1", source="path_1"),
        Row(name="mp_2", source="path_2"),
    ] == backend.rows_written_for("hive_metastore.test.mounts", "overwrite")


def test_mounts_inventory_should_contain_deduped_mounts_without_variable_volume_names():
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
    instance = MountsCrawler(backend, client, "test")

    instance.snapshot()

    expected = [
        Row(name="/Volume", source="DbfsReserved"),
        Row(name="mp_1", source="path_1"),
        Row(name="mp_2", source="path_2"),
    ]
    assert expected == backend.rows_written_for("hive_metastore.test.mounts", "overwrite")


def test_mount_inventory_warning_on_incompatible_compute(caplog):
    client = create_autospec(WorkspaceClient)
    client.dbutils.fs.mounts.side_effect = Exception(
        "Blah Blah com.databricks.backend.daemon.dbutils.DBUtilsCore.mounts() is not whitelisted"
    )
    backend = MockBackend()
    instance = MountsCrawler(backend, client, "test")

    instance.snapshot()

    expected_warning = "dbutils.fs.mounts() is not whitelisted"
    assert expected_warning in caplog.text


def table_factory(args):
    location, storage_properties = args
    return Table('', '', '', '', '', location=location, storage_properties=storage_properties)


def test_external_locations():
    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        table_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/Table", ""]),
        table_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/Table2", ""]),
        table_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/testloc/Table3", ""]),
        table_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/anotherloc/Table4", ""]),
        table_factory(["gcs://test_location2/a/b/table2", ""]),
        table_factory(["dbfs:/mnt/ucx/database1/table1", ""]),
        table_factory(["/dbfs/mnt/ucx/database2/table2", ""]),
        table_factory(["DatabricksRootmntDatabricksRoot", ""]),
        table_factory(
            [
                "jdbc:databricks://",
                "[personalAccessToken=*********(redacted), \
    httpPath=/sql/1.0/warehouses/65b52fb5bd86a7be, host=dbc-test1-aa11.cloud.databricks.com, \
    dbtable=samples.nyctaxi.trips]",
            ]
        ),
        table_factory(
            [
                "jdbc:/MYSQL",
                "[database=test_db, host=somemysql.us-east-1.rds.amazonaws.com, \
        port=3306, dbtable=movies, user=*********(redacted), password=*********(redacted)]",
            ]
        ),
        table_factory(
            [
                "jdbc:providerknown:/",
                "[database=test_db, host=somedb.us-east-1.rds.amazonaws.com, \
        port=1234, dbtable=sometable, user=*********(redacted), password=*********(redacted), \
        provider=providerknown]",
            ]
        ),
        table_factory(
            [
                "jdbc:providerknown:/",
                "[database=test_db, host=somedb.us-east-1.rds.amazonaws.com, \
        port=1234, dbtable=sometable2, user=*********(redacted), password=*********(redacted), \
        provider=providerknown]",
            ]
        ),
        table_factory(
            [
                "jdbc:providerunknown:/",
                "[database=test_db, host=somedb.us-east-1.rds.amazonaws.com, \
        port=1234, dbtable=sometable, user=*********(redacted), password=*********(redacted)]",
            ]
        ),
    ]
    mounts_crawler = create_autospec(MountsCrawler)
    mounts_crawler.snapshot.return_value = [Mount("/mnt/ucx", "s3://us-east-1-ucx-container")]
    sql_backend = MockBackend()
    crawler = ExternalLocations(Mock(), sql_backend, "test", tables_crawler, mounts_crawler)
    assert crawler.snapshot() == [
        ExternalLocation('gcs://test_location2/a/b', 1),
        ExternalLocation(
            'jdbc:databricks://dbc-test1-aa11.cloud.databricks.com;httpPath=/sql/1.0/warehouses/65b52fb5bd86a7be', 1
        ),
        ExternalLocation('jdbc:mysql://somemysql.us-east-1.rds.amazonaws.com:3306/test_db', 1),
        ExternalLocation('jdbc:providerknown://somedb.us-east-1.rds.amazonaws.com:1234/test_db', 2),
        ExternalLocation('jdbc:providerunknown://somedb.us-east-1.rds.amazonaws.com:1234/test_db', 1),
        ExternalLocation('s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location', 2),
        ExternalLocation('s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23', 2),
        ExternalLocation('s3://us-east-1-ucx-container', 2),
    ]


LOCATION_STORAGE = MockBackend.rows("location", "storage_properties")
MOUNT_STORAGE = MockBackend.rows("name", "source")
TABLE_STORAGE = MockBackend.rows("catalog", "database", "name", "object_type", "table_format", "location")


def test_save_external_location_mapping_missing_location():
    ws = create_autospec(WorkspaceClient)
    sql_backend = MockBackend()
    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        table_factory(["s3://test_location/test1/table1", ""]),
        table_factory(["s3://test_location/test1/table2", ""]),
    ]
    mounts_crawler = create_autospec(MountsCrawler)
    mounts_crawler.snapshot.return_value = []
    location_crawler = ExternalLocations(ws, sql_backend, "test", tables_crawler, mounts_crawler)
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
            "}\n"
        ).encode("utf8"),
    )


def test_save_external_location_mapping_no_missing_location():
    ws = create_autospec(WorkspaceClient)
    sql_backend = MockBackend()

    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        table_factory(["s3://test_location/test1/table1", ""]),
    ]
    mounts_crawler = create_autospec(MountsCrawler)
    mounts_crawler.snapshot.return_value = []
    location_crawler = ExternalLocations(ws, sql_backend, "test", tables_crawler, mounts_crawler)
    ws.external_locations.list.return_value = [ExternalLocationInfo(name="loc1", url="s3://test_location/test1")]
    location_crawler.save_as_terraform_definitions_on_workspace("~/.ucx")
    ws.workspace.upload.assert_not_called()


def test_match_table_external_locations():
    ws = create_autospec(WorkspaceClient)
    sql_backend = MockBackend()
    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        table_factory(["s3://test_location/a/b/c/table1", ""]),
        table_factory(["s3://test_location/a/b/table1", ""]),
        table_factory(["gcs://test_location2/a/b/table2", ""]),
        table_factory(["abfss://cont1@storagetest1/a/table3", ""]),
        table_factory(["abfss://cont1@storagetest1/a/table4", ""]),
    ]
    mounts_crawler = create_autospec(MountsCrawler)
    mounts_crawler.snapshot.return_value = []
    location_crawler = ExternalLocations(ws, sql_backend, "test", tables_crawler, mounts_crawler)
    ws.external_locations.list.return_value = [ExternalLocationInfo(name="loc1", url="s3://test_location/a")]

    matching_locations, missing_locations = location_crawler.match_table_external_locations()

    assert len(matching_locations) == 1
    assert [
        ExternalLocation("abfss://cont1@storagetest1/a", 2),
        ExternalLocation("gcs://test_location2/a/b", 1),
    ] == missing_locations


def test_mount_listing_multiple_folders():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/table1/", "table1/", 0, "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/table2/", "table2/", 0, "")
    folder_table1 = FileInfo("dbfs:/mnt/test_mount/table1/_delta_log/", "_delta_log/", 0, "")
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
            '`hive_metastore`.`test`.`tables`': [],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = MountsCrawler(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot(force_refresh=True)
    assert results == [
        Table("hive_metastore", "mounted_test_mount", "table1", "EXTERNAL", "DELTA", "adls://bucket/table1"),
        Table("hive_metastore", "mounted_test_mount", "table2", "EXTERNAL", "PARQUET", "adls://bucket/table2"),
    ]


def test_mount_listing_sub_folders():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/entity/", "entity/", 0, "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/", "domain/", 0, "")
    third_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/", "table1/", 0, "")
    fourth_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/_delta_log/", "_delta_log/", 0, "")
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
            '`hive_metastore`.`test`.`tables`': [],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = MountsCrawler(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot(force_refresh=True)
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

    first_folder = FileInfo("dbfs:/mnt/test_mount/entity/", "entity/", 0, "")
    first_partition = FileInfo("dbfs:/mnt/test_mount/entity/xxx=yyy/", "xxx=yyy/", 0, "")
    first_partition_files = FileInfo("dbfs:/mnt/test_mount/entity/xxx=yyy/1.parquet", "1.parquet", "", "")
    second_partition = FileInfo("dbfs:/mnt/test_mount/entity/xxx=zzz/", "xxx=zzz/", 0, "")
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
            '`hive_metastore`.`test`.`tables`': [],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = MountsCrawler(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot(force_refresh=True)
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

    first_folder = FileInfo("dbfs:/mnt/test_mount/entity/", "entity/", 0, "")
    first_first_partition = FileInfo("dbfs:/mnt/test_mount/entity/xxx=yyy/", "xxx=yyy/", 0, "")
    first_first_partition_files = FileInfo("dbfs:/mnt/test_mount/entity/xxx=yyy/1.parquet", "1.parquet", "", "")
    first_second_partition = FileInfo("dbfs:/mnt/test_mount/entity/xxx=zzz/", "xxx=zzz/", 0, "")
    first_second_partition_files = FileInfo("dbfs:/mnt/test_mount/entity/xxx=zzz/1.parquet", "1.parquet", "", "")
    first_delta_log = FileInfo("dbfs:/mnt/test_mount/entity/_delta_log/", "_delta_log/", 0, "")

    second_folder = FileInfo("dbfs:/mnt/test_mount/entity_2/", "entity_2/", 0, "")
    second_first_partition = FileInfo("dbfs:/mnt/test_mount/entity_2/xxx=yyy/", "xxx=yyy/", 0, "")
    second_second_partition = FileInfo("dbfs:/mnt/test_mount/entity_2/xxx=yyy/aaa=bbb/", "aaa=bbb/", 0, "")
    second_second_partition_files = FileInfo(
        "dbfs:/mnt/test_mount/entity_2/xxx=yyy/aaa=bbb/1.parquet", "1.parquet", "", ""
    )
    second_delta_log = FileInfo("dbfs:/mnt/test_mount/entity_2/_delta_log/", "_delta_log/", 0, "")

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
            '`hive_metastore`.`test`.`tables`': [],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = MountsCrawler(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot(force_refresh=True)
    assert len(results) == 2
    assert results[0].table_format == "DELTA"
    assert results[0].is_partitioned
    assert results[1].table_format == "DELTA"
    assert results[1].is_partitioned


def test_filtering_irrelevant_paths():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/table1/", "table1/", 0, "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/$_azuretempfolder/", "$_azuretempfolder/", 0, "")
    first_folder_delta_log = FileInfo("dbfs:/mnt/test_mount/table1/_delta_log/", "_delta_log/", 0, "")
    second_folder_delta_log = FileInfo("dbfs:/mnt/test_mount/$_azuretempfolder/_delta_log/", "_delta_log/", 0, "")

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
            '`hive_metastore`.`test`.`tables`': [],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = MountsCrawler(backend, client, "test")
    crawler = TablesInMounts(backend, client, "test", mounts, exclude_paths_in_mount=["$_azuretempfolder"])
    results = crawler.snapshot(force_refresh=True)
    assert results == [
        Table("hive_metastore", "mounted_test_mount", "table1", "EXTERNAL", "DELTA", "adls://bucket/table1"),
    ]


def test_filter_irrelevant_mounts():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("/mnt/test_mount/table1/", "table1/", 0, "")
    second_folder = FileInfo("/mnt/test_mount2/table2/", "table2/", 0, "")
    first_folder_delta_log = FileInfo("/mnt/test_mount/table1/_delta_log/", "_delta_log/", 0, "")
    second_folder_delta_log = FileInfo("/mnt/test_mount2/table2/_delta_log/", "_delta_log/", 0, "")

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
            '`hive_metastore`.`test`.`tables`': [],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", ""), ("/mnt/test_mount2", "")],
        }
    )
    mounts = MountsCrawler(backend, client, "test")
    crawler = TablesInMounts(backend, client, "test", mounts, include_mounts=["/mnt/test_mount"])
    results = crawler.snapshot(force_refresh=True)

    assert results == [
        Table("hive_metastore", "mounted_test_mount", "table1", "EXTERNAL", "DELTA", "/mnt/test_mount/table1"),
    ]
    client.dbutils.fs.ls.assert_has_calls([call('/mnt/test_mount'), call('/mnt/test_mount/table1/')])


def test_historical_data_should_be_overwritten() -> None:
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/table1/", "table1/", 0, "")
    second_folder = FileInfo("dbfs:/mnt/test_mount2/table2/", "table2/", 0, "")
    first_folder_delta_log = FileInfo("dbfs:/mnt/test_mount/table1/_delta_log/", "_delta_log/", 0, "")
    second_folder_delta_log = FileInfo("dbfs:/mnt/test_mount2/table2/_delta_log/", "_delta_log/", 0, "")

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
            '`hive_metastore`.`test`.`tables`': TABLE_STORAGE[
                ("catalog", "database", "name", "object_type", "table_format", "location")
            ],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "abfss://bucket@windows/")],
        }
    )
    mounts = MountsCrawler(backend, client, "test")
    TablesInMounts(backend, client, "test", mounts).snapshot(force_refresh=True)
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
            table_format='TABLE_FORMAT',
            location='location',
            view_text=None,
            upgraded_to=None,
            storage_properties=None,
            is_partitioned=False,
        ),
    ]


def test_mount_include_paths():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/table1/", "table1/", 0, "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/table2/", "table2/", 0, "")
    folder_table1 = FileInfo("dbfs:/mnt/test_mount/table1/_delta_log/", "_delta_log/", 0, "")
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
            '`hive_metastore`.`test`.`tables`': [],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = MountsCrawler(backend, client, "test")
    crawler = TablesInMounts(backend, client, "test", mounts, include_paths_in_mount=["dbfs:/mnt/test_mount/table2/"])
    results = crawler.snapshot(force_refresh=True)
    assert results == [
        Table("hive_metastore", "mounted_test_mount", "table2", "EXTERNAL", "PARQUET", "adls://bucket/table2"),
    ]


def test_mount_listing_csv_json():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/entity/", "entity/", 0, "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/", "domain/", 0, "")
    second_folder_random_csv = FileInfo("dbfs:/mnt/test_mount/entity/domain/test.csv", "test.csv", "", "")
    third_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/", "table1/", 0, "")
    first_json = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/some_jsons.json", "some_jsons.json", "", "")
    second_json = FileInfo(
        "dbfs:/mnt/test_mount/entity/domain/table1/some_other_jsons.json", "some_other_jsons.json", "", ""
    )

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder]
        if path == "dbfs:/mnt/test_mount/entity/":
            return [second_folder]
        if path == "dbfs:/mnt/test_mount/entity/domain/":
            return [third_folder, second_folder_random_csv]
        if path == "dbfs:/mnt/test_mount/entity/domain/table1/":
            return [first_json, second_json]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            '`hive_metastore`.`test`.`tables`': [],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = MountsCrawler(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot(force_refresh=True)
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
            "domain",
            "EXTERNAL",
            "CSV",
            "adls://bucket/entity/domain",
        ),
    ]


def test_mount_listing_seen_tables():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/table1/", "table1/", 0, "")
    folder_table1 = FileInfo("dbfs:/mnt/test_mount/table1/_delta_log/", "_delta_log/", 0, "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/table2/", "table2/", 0, "")
    second_folder1 = FileInfo("dbfs:/mnt/test_mount/table2/_delta_log/", "_delta_log/", 0, "")

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
            '`hive_metastore`.`test`.`tables`': TABLE_STORAGE[
                ("hive_metastore", "database", "name", "EXTERNAL", "DELTA", "adls://bucket/table1"),
                ("hive_metastore", "database", "name_2", "EXTERNAL", "DELTA", "dbfs:/mnt/test_mount/table2"),
                ("hive_metastore", "database", "name_3", "MANAGED", "DELTA", None),
            ],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = MountsCrawler(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot(force_refresh=True)
    assert len(results) == 3
    assert results[0].location == "adls://bucket/table1"
    assert results[1].location == "dbfs:/mnt/test_mount/table2"
    assert results[2].location is None


def test_resolve_dbfs_root_in_hms_federation():
    jvm = Mock()
    sql_backend = MockBackend()
    client = create_autospec(WorkspaceClient)
    client.dbutils.fs.mounts.return_value = [MountInfo('/', 'DatabricksRoot', '')]

    mounts_crawler = MountsCrawler(sql_backend, client, "test", enable_hms_federation=True)
    mounts_crawler.__dict__['_jvm'] = jvm

    hms_fed_dbfs_utils = jvm.com.databricks.sql.managedcatalog.connections.HmsFedDbfsUtils
    hms_fed_dbfs_utils.resolveDbfsPath().get().toString.return_value = 's3://original/bucket/user/hive/warehouse'

    mounts = mounts_crawler.snapshot()

    assert [Mount("/", 's3://original/bucket/')] == mounts


def test_mount_listing_misplaced_flat_file():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/entity/", "entity/", 0, "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/", "domain/", 0, "")
    misplaced_csv = FileInfo("dbfs:/mnt/test_mount/entity/domain/test.csv", "test.csv", "", "")
    third_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/", "table1/", 0, "")
    first_json = FileInfo("dbfs:/mnt/test_mount/entity/domain/table1/some_jsons.json", "some_jsons.json", "", "")
    second_json = FileInfo(
        "dbfs:/mnt/test_mount/entity/domain/table1/some_other_jsons.json", "some_other_jsons.json", "", ""
    )
    z_dir = FileInfo("dbfs:/mnt/test_mount/entity/domain/z_dir/", "z_dir", 0, "")
    z_dir_json = FileInfo("dbfs:/mnt/test_mount/entity/domain/z_dir/some.json", "some.json", "", "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder]
        if path == "dbfs:/mnt/test_mount/entity/":
            return [second_folder]
        if path == "dbfs:/mnt/test_mount/entity/domain/":
            return [third_folder, misplaced_csv, z_dir]
        if path == "dbfs:/mnt/test_mount/entity/domain/table1/":
            return [first_json, second_json]
        if path == "dbfs:/mnt/test_mount/entity/domain/z_dir/":
            return [z_dir_json]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            '`hive_metastore`.`test`.`tables`': [],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
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
            "domain",
            "EXTERNAL",
            "CSV",
            "adls://bucket/entity/domain",
        ),
        Table(
            "hive_metastore",
            "mounted_test_mount",
            "z_dir",
            "EXTERNAL",
            "JSON",
            "adls://bucket/entity/domain/z_dir",
        ),
    ]


def test_mount_dont_list_partitions():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/entity/", "entity/", 0, "")
    first_first_partition = FileInfo("dbfs:/mnt/test_mount/entity/xxx=yyy/", "xxx=yyy/", 0, "")
    first_first_partition_files = FileInfo("dbfs:/mnt/test_mount/entity/xxx=yyy/1.parquet", "1.parquet", "", "")
    first_second_partition = FileInfo("dbfs:/mnt/test_mount/entity/xxx=zzz/", "xxx=zzz/", 0, "")
    first_second_partition_files = FileInfo("dbfs:/mnt/test_mount/entity/xxx=zzz/1.parquet", "1.parquet", "", "")
    misplaced_json = FileInfo("dbfs:/mnt/test_mount/entity/xxx=zzz/misplaced.json", "misplaced.json", "", "")
    first_delta_log = FileInfo("dbfs:/mnt/test_mount/entity/_delta_log/", "_delta_log/", 0, "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder]
        if path == "dbfs:/mnt/test_mount/entity/":
            return [first_delta_log, first_first_partition, first_second_partition]
        if path == "dbfs:/mnt/test_mount/entity/xxx=yyy/":
            return [first_first_partition_files, misplaced_json]
        if path == "dbfs:/mnt/test_mount/entity/xxx=zzz/":
            return [first_second_partition_files]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            '`hive_metastore`.`test`.`tables`': [],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = Mounts(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot()
    assert len(results) == 1
    assert results[0].table_format == "DELTA"
    assert results[0].is_partitioned


def test_mount_infinite_loop():

    client = create_autospec(WorkspaceClient)

    folder = FileInfo("dbfs:/mnt/test_mount/entity/", "entity/", 0, "")
    partition = FileInfo("dbfs:/mnt/test_mount/entity/xxx=yyy/", "xxx=yyy/", 0, "")
    file = FileInfo("dbfs:/mnt/test_mount/entity/xxx=yyy/document=2023-12-20", "document=2023-12-20", "", "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [folder]
        if path == "dbfs:/mnt/test_mount/entity/":
            return [partition]
        if path == "dbfs:/mnt/test_mount/entity/xxx=yyy/":
            return [file]
        if path == "dbfs:/mnt/test_mount/entity/xxx=yyy/document=2023-12-20":
            return [file]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            '`hive_metastore`.`test`.`tables`': [],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )

    original_limit = sys.getrecursionlimit()
    sys.setrecursionlimit(150)  # Temporarily lower recursion limit to catch errors early
    try:
        mounts = Mounts(backend, client, "test")
        results = TablesInMounts(backend, client, "test", mounts).snapshot()
    except RecursionError:
        pytest.fail("Recursion depth exceeded, possible infinite loop.")
    finally:
        sys.setrecursionlimit(original_limit)  # Restore the original limit after test

    assert len(results) == 0


def test_mount_exclude_checkpoint_dir():
    client = create_autospec(WorkspaceClient)

    first_folder = FileInfo("dbfs:/mnt/test_mount/entity/", "entity/", 0, "")
    second_folder = FileInfo("dbfs:/mnt/test_mount/entity/domain/", "domain/", 0, "")
    csv = FileInfo("dbfs:/mnt/test_mount/entity/domain/test.csv", "test.csv", "", "")
    checkpoint_dir = FileInfo(
        "dbfs:/mnt/test_mount/entity/domain/streaming_checkpoint/", "streaming_checkpoint/", 0, ""
    )
    offsets = FileInfo("dbfs:/mnt/test_mount/entity/domain/streaming_checkpoint/offsets/", "offsets/", 0, "")
    commit = FileInfo("dbfs:/mnt/test_mount/entity/domain/streaming_checkpoint/commits/", "commits/", 0, "")
    state = FileInfo("dbfs:/mnt/test_mount/entity/domain/streaming_checkpoint/state/", "state/", 0, "")
    metadata = FileInfo("dbfs:/mnt/test_mount/entity/domain/streaming_checkpoint/metadata", "metadata", "", "")
    some_json = FileInfo("dbfs:/mnt/test_mount/entity/domain/streaming_checkpoint/offsets/a.json", "a.json", "", "")

    def my_side_effect(path, **_):
        if path == "/mnt/test_mount":
            return [first_folder]
        if path == "dbfs:/mnt/test_mount/entity/":
            return [second_folder]
        if path == "dbfs:/mnt/test_mount/entity/domain/":
            return [checkpoint_dir, csv]
        if path == "dbfs:/mnt/test_mount/entity/domain/streaming_checkpoint/":
            return [offsets, commit, state, metadata]
        if path == "dbfs:/mnt/test_mount/entity/domain/streaming_checkpoint/offsets/":
            return [some_json]
        return None

    client.dbutils.fs.ls.side_effect = my_side_effect
    backend = MockBackend(
        rows={
            '`hive_metastore`.`test`.`tables`': [],
            '`test`.`mounts`': MOUNT_STORAGE[("/mnt/test_mount", "adls://bucket/")],
        }
    )
    mounts = Mounts(backend, client, "test")
    results = TablesInMounts(backend, client, "test", mounts).snapshot()
    assert len(results) == 1
    assert results[0].table_format == "CSV"
