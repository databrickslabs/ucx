import logging
from datetime import timedelta

from databricks.sdk.errors import NotFound

from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler

from ..retries import retried

logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_describe_all_udfs_in_databases(ws, sql_backend, inventory_schema, make_schema, make_udf):
    schema_a = make_schema(catalog_name="hive_metastore")
    schema_b = make_schema(catalog_name="hive_metastore")
    make_schema(catalog_name="hive_metastore")
    make_udf(schema_name=schema_a.name)
    hive_udf = make_udf(schema_name=schema_a.name, hive_udf=True)
    make_udf(schema_name=schema_b.name)

    udfs_crawler = UdfsCrawler(sql_backend, inventory_schema, [schema_a.name, schema_b.name])
    udfs = udfs_crawler.snapshot()

    assert len(udfs) == 3
    assert sum(udf.success for udf in udfs) == 2  # hive_udf should fail
    assert [udf.failures for udf in udfs if udf.key == hive_udf.full_name] == ["Only SCALAR functions are supported"]
