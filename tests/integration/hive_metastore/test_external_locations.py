import logging
import os

from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore.data_objects import ExternalLocationCrawler
from databricks.labs.ucx.hive_metastore.list_mounts import Mount
from databricks.labs.ucx.hive_metastore.tables import Table

logger = logging.getLogger(__name__)


def test_table_inventory(ws, make_warehouse, make_schema):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    logger.info("setting up fixtures")
    sbe = StatementExecutionBackend(ws, warehouse_id)
    tables = [
        Table("hive_metastore", "foo", "bar", "MANAGED", "delta", location="s3://test_location/test1/table1"),
        Table("hive_metastore", "foo", "bar", "EXTERNAL", "delta", location="s3://test_location/test2/table2"),
        Table("hive_metastore", "foo", "bar", "EXTERNAL", "delta", location="dbfs:/mnt/foo/test3/table3"),
    ]
    schema = make_schema()
    sbe.save_table(f"{schema}.tables", tables)
    sbe.save_table(f"{schema}.mounts", [Mount("/mnt/foo", "s3://bar")])

    crawler = ExternalLocationCrawler(ws, sbe, schema.split(".")[1])
    results = crawler.snapshot()
    assert len(results) == 2
    assert results[1].location == "s3://bar/test3/"
