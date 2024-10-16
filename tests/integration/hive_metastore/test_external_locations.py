import logging

from databricks.labs.blueprint.installation import Installation

from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocation,
    ExternalLocations,
    Mount,
    MountsCrawler,
)
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler

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

    tables_crawler = TablesCrawler(sql_backend, inventory_schema)
    mounts_crawler = MountsCrawler(sql_backend, ws, inventory_schema)
    crawler = ExternalLocations(ws, sql_backend, inventory_schema, tables_crawler, mounts_crawler)
    results = crawler.snapshot()
    assert results == [
        ExternalLocation('s3://test_location/', 2),
        ExternalLocation('s3://bar/test3/', 1),
        ExternalLocation(
            'jdbc:databricks://dbc-test1-aa11.cloud.databricks.com;httpPath=/sql/1.0/warehouses/65b52fb5bd86a7be', 1
        ),
        ExternalLocation('jdbc:mysql://somemysql.us-east-1.rds.amazonaws.com:3306/test_db', 1),
        ExternalLocation('jdbc:providerknown://somedb.us-east-1.rds.amazonaws.com:1234/test_db', table_count=2),
        ExternalLocation('jdbc://providerunknown//somedb.us-east-1.rds.amazonaws.com:1234/test_db', 1),
    ]


def test_save_external_location_mapping_missing_location(ws, sql_backend, inventory_schema, make_random):
    locations = [
        ExternalLocation("abfss://cont1@storage123/test_location", 2),
        ExternalLocation("abfss://cont1@storage456/test_location2", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", locations, ExternalLocation)
    tables_crawler = TablesCrawler(sql_backend, inventory_schema)
    mounts_crawler = MountsCrawler(sql_backend, ws, inventory_schema)
    location_crawler = ExternalLocations(ws, sql_backend, inventory_schema, tables_crawler, mounts_crawler)
    installation = Installation(ws, make_random)
    path = location_crawler.save_as_terraform_definitions_on_workspace(installation)
    assert ws.workspace.get_status(path)
