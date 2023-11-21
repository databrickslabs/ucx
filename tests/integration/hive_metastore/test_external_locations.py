import logging

from databricks.labs.ucx.hive_metastore.data_objects import ExternalLocationCrawler
from databricks.labs.ucx.hive_metastore.mounts import Mount
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
    ]
    sql_backend.save_table(f"{inventory_schema}.tables", tables, Table)
    sql_backend.save_table(f"{inventory_schema}.mounts", [Mount("/mnt/foo", "s3://bar")], Mount)

    crawler = ExternalLocationCrawler(ws, sql_backend, inventory_schema)
    results = crawler.snapshot()
    assert len(results) == 4
    assert results[1].location == "s3://bar/test3/"
    assert (
        results[2].location
        == "jdbc:databricks://dbc-test1-aa11.cloud.databricks.com;httpPath=/sql/1.0/warehouses/65b52fb5bd86a7be"
    )
    assert results[3].location == "jdbc:mysql://somemysql.us-east-1.rds.amazonaws.com:3306/test_db"
