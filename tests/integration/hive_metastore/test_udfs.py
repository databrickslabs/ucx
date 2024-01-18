import logging
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler

logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_describe_all_udfs_in_databases(ws, sql_backend, inventory_schema, make_schema, make_udf):
    schema_a = make_schema(catalog_name="hive_metastore")
    make_schema(catalog_name="hive_metastore")
    make_udf(schema_name=schema_a.name)
    make_udf(schema_name=schema_a.name)

    udfs_crawler = UdfsCrawler(sql_backend, inventory_schema)
    crawled_udfs = udfs_crawler.snapshot()

    assert len(list(crawled_udfs)) == 2
