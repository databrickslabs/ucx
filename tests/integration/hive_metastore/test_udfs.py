import logging
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler, UdfOwnership

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


def test_udf_ownership(runtime_ctx, inventory_schema, sql_backend) -> None:
    """Verify the ownership can be determined for crawled UDFs."""
    # This currently isn't very useful: we don't currently locate specific owners for UDFs.

    # A UDF for which we'll determine the owner.
    udf = runtime_ctx.make_udf()

    # Produce the crawled records
    crawler = UdfsCrawler(sql_backend, schema=inventory_schema, include_databases=[udf.schema_name])
    records = crawler.snapshot(force_refresh=True)

    # Find the crawled record for the table we made.
    udf_record = next(r for r in records if f"{r.catalog}.{r.database}.{r.name}" == udf.full_name)

    # Verify ownership can be made.
    ownership = UdfOwnership(runtime_ctx.administrator_locator)
    assert ownership.owner_of(udf_record) == runtime_ctx.administrator_locator.get_workspace_administrator()
