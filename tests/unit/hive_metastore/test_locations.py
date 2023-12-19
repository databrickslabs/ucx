from unittest.mock import MagicMock, Mock

from databricks.sdk.dbutils import MountInfo

from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocations,
    Mount,
    Mounts,
)
from databricks.labs.ucx.mixins.sql import Row
from tests.unit.framework.mocks import MockBackend


def test_list_mounts_should_return_a_list_of_mount_without_encryption_type():
    client = MagicMock()
    client.dbutils.fs.mounts.return_value = [
        MountInfo("mp_1", "path_1", "info_1"),
        MountInfo("mp_2", "path_2", "info_2"),
        MountInfo("mp_3", "path_3", "info_3"),
    ]

    backend = MockBackend()
    instance = Mounts(backend, client, "test")

    instance.inventorize_mounts()

    expected = [Mount("mp_1", "path_1"), Mount("mp_2", "path_2"), Mount("mp_3", "path_3")]
    assert expected == backend.rows_written_for("hive_metastore.test.mounts", "append")


def test_list_mounts_should_return_a_deduped_list_of_mount_without_encryption_type():
    client = MagicMock()
    client.dbutils.fs.mounts.return_value = [
        MountInfo("mp_1", "path_1", "info_1"),
        MountInfo("mp_2", "path_2", "info_2"),
        MountInfo("mp_2", "path_2", "info_2"),
    ]

    backend = MockBackend()
    instance = Mounts(backend, client, "test")

    instance.inventorize_mounts()

    expected = [Mount("mp_1", "path_1"), Mount("mp_2", "path_2")]
    assert expected == backend.rows_written_for("hive_metastore.test.mounts", "append")


def test_external_locations():
    crawler = ExternalLocations(Mock(), MockBackend(), "test")
    row_factory = type("Row", (Row,), {"__columns__": ["location", "storage_properties"]})
    sample_locations = [
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
    ]
    sample_mounts = [Mount("/mnt/ucx", "s3://us-east-1-ucx-container")]
    result_set = crawler._external_locations(sample_locations, sample_mounts)
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
