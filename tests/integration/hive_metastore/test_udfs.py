import logging
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler

logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_describe_all_udfs_in_databases(ws, sql_backend, inventory_schema, make_schema, make_udf):
    schema_a = make_schema(catalog_name="hive_metastore")
    schema_b = make_schema(catalog_name="hive_metastore")
    make_schema(catalog_name="hive_metastore")
    udf_a = make_udf(schema_name=schema_a.name)
    udf_b = make_udf(schema_name=schema_a.name)
    make_udf(schema_name=schema_b.name)

    udfs_crawler = UdfsCrawler(sql_backend, inventory_schema, [schema_a.name, schema_b.name, "ucxx"])
    actual_grants = udfs_crawler.snapshot()

    unique_udf_grants = {
        grant.name
        for grant in actual_grants
        if f"{grant.catalog}.{grant.database}.{grant.name}" in [udf_a.full_name, udf_b.full_name]
    }

    assert len(unique_udf_grants) == 2
