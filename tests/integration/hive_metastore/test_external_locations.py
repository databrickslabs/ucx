import logging

from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocation,
    ExternalLocations,
    Mount,
)
from databricks.labs.ucx.hive_metastore.tables import Table

logger = logging.getLogger(__name__)


def test_external_locations(ws, sql_backend, inventory_schema, env_or_skip):
    logger.info("setting up fixtures")
    tables = [
        Table("hive_metastore", "foo", "bar", "MANAGED", "delta", location="s3://test_location/test1/table1"),
        Table("hive_metastore", "foo", "bar", "EXTERNAL", "delta", location="s3://test_location/test2/table2"),
        Table("hive_metastore", "foo", "bar", "EXTERNAL", "delta", location="dbfs:/mnt/foo/test3/table3"),
        Table(
            "hive_metastore",
            "foo",
            "bar",
            "EXTERNAL",
            "delta",
            location="jdbc://databricks/",
            storage_properties="[personalAccessToken=*********(redacted), \
            httpPath=/sql/1.0/warehouses/65b52fb5bd86a7be, host=dbc-test1-aa11.cloud.databricks.com, \
            dbtable=samples.nyctaxi.trips]",
        ),
        Table(
            "hive_metastore",
            "foo",
            "bar",
            "EXTERNAL",
            "delta",
            location="jdbc://MYSQL/",
            storage_properties="[database=test_db, host=somemysql.us-east-1.rds.amazonaws.com, \
            port=3306, dbtable=movies, user=*********(redacted), password=*********(redacted)]",
        ),
        Table(
            "hive_metastore",
            "foo",
            "bar",
            "EXTERNAL",
            "delta",
            location="jdbc://providerknown/",
            storage_properties="[database=test_db, host=somedb.us-east-1.rds.amazonaws.com, \
            port=1234, dbtable=sometable, user=*********(redacted), password=*********(redacted), \
            provider=providerknown]",
        ),
        Table(
            "hive_metastore",
            "foo",
            "bar2",
            "EXTERNAL",
            "delta",
            location="jdbc://providerknown/",
            storage_properties="[database=test_db, host=somedb.us-east-1.rds.amazonaws.com, \
            port=1234, dbtable=sometable2, user=*********(redacted), password=*********(redacted), \
            provider=providerknown]",
        ),
        Table(
            "hive_metastore",
            "foo",
            "bar",
            "EXTERNAL",
            "delta",
            location="jdbc://providerunknown/",
            storage_properties="[database=test_db, host=somedb.us-east-1.rds.amazonaws.com, \
            port=1234, dbtable=sometable, user=*********(redacted), password=*********(redacted)]",
        ),
    ]
    sql_backend.save_table(f"{inventory_schema}.tables", tables, Table)
    sql_backend.save_table(f"{inventory_schema}.mounts", [Mount("/mnt/foo", "s3://bar")], Mount)

    crawler = ExternalLocations(ws, sql_backend, inventory_schema)
    results = crawler.snapshot()
    assert len(results) == 6
    assert results[1].location == "s3://bar/test3/"
    assert (
        results[2].location
        == "jdbc:databricks://dbc-test1-aa11.cloud.databricks.com;httpPath=/sql/1.0/warehouses/65b52fb5bd86a7be"
    )
    assert results[3].location == "jdbc:mysql://somemysql.us-east-1.rds.amazonaws.com:3306/test_db"
    assert results[4].location == "jdbc:providerknown://somedb.us-east-1.rds.amazonaws.com:1234/test_db"
    assert results[4].table_count == 2
    assert results[5].location == "jdbc://providerunknown//somedb.us-east-1.rds.amazonaws.com:1234/test_db"


def test_save_external_location_mapping_missing_location(ws, sql_backend, inventory_schema, make_directory):
    folder = make_directory()
    locations = [
        ExternalLocation("abfss://cont1@storage123/test_location", 2),
        ExternalLocation("abfss://cont1@storage456/test_location2", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", locations, ExternalLocation)
    location_crawler = ExternalLocations(ws, sql_backend, inventory_schema)
    path = location_crawler.save_as_terraform_definitions_on_workspace(folder)
    assert ws.workspace.get_status(path)
