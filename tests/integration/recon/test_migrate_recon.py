from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.hive_metastore.mapping import Rule
from databricks.labs.ucx.hive_metastore.tables import What


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_migrate_managed_tables_and_recon(ws, sql_backend, runtime_ctx, make_catalog):
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_managed_table = runtime_ctx.make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)

    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    rules = [Rule.from_src_dst(src_managed_table, dst_schema)]

    runtime_ctx.with_table_mapping_rules(rules)

    runtime_ctx.with_dummy_resource_permission()
    runtime_ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA)

    recon_result = runtime_ctx.migration_recon.snapshot()
    assert len(list(recon_result)) == 1, "Expected 1 recon result"
