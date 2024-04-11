import logging
from collections import defaultdict
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.compute import DataSecurityMode

from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler
from databricks.labs.ucx.hive_metastore.mapping import Rule
from databricks.sdk.service.iam import PermissionLevel


logger = logging.getLogger(__name__)


@retried(on=[NotFound, TimeoutError], timeout=timedelta(minutes=3))
def test_all_grants_in_databases(runtime_ctx, sql_backend, make_group):
    group_a = make_group()
    group_b = make_group()
    schema_a = runtime_ctx.make_schema()
    schema_b = runtime_ctx.make_schema()
    schema_c = runtime_ctx.make_schema()
    empty_schema = runtime_ctx.make_schema()
    table_a = runtime_ctx.make_table(schema_name=schema_a.name)
    table_b = runtime_ctx.make_table(schema_name=schema_b.name)
    view_c = runtime_ctx.make_table(schema_name=schema_a.name, view=True, ctas="SELECT id FROM range(10)")
    view_d = runtime_ctx.make_table(schema_name=schema_a.name, view=True, ctas="SELECT id FROM range(10)")
    table_e = runtime_ctx.make_table(schema_name=schema_c.name)

    sql_backend.execute(f"GRANT USAGE ON SCHEMA {schema_c.name} TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT USAGE ON SCHEMA {schema_c.name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON TABLE {table_e.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_b.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON SCHEMA {schema_b.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON SCHEMA {empty_schema.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON VIEW {view_c.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON TABLE {view_d.full_name} TO `{group_b.display_name}`")

    # 20 seconds less than TablesCrawler(sql_backend, inventory_schema)
    grants = GrantsCrawler(runtime_ctx.tables_crawler, runtime_ctx.udfs_crawler)

    all_grants = {}
    for grant in grants.snapshot():
        logging.info(f"grant:\n{grant}\n  hive: {grant.hive_grant_sql()}\n  uc: {grant.uc_grant_sql()}")
        all_grants[f"{grant.principal}.{grant.object_key}"] = grant.action_type

    assert len(all_grants) >= 8, "must have at least three grants"
    assert all_grants[f"{group_a.display_name}.hive_metastore.{schema_c.name}"] == "USAGE"
    assert all_grants[f"{group_b.display_name}.hive_metastore.{schema_c.name}"] == "USAGE"
    assert all_grants[f"{group_a.display_name}.{table_a.full_name}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{table_b.full_name}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{schema_b.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.{empty_schema.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.{view_c.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.{view_d.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.{table_e.full_name}"] == "MODIFY"


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_all_grants_for_udfs_in_databases(runtime_ctx, sql_backend, make_group):
    group = make_group()
    schema = runtime_ctx.make_schema()
    udf_a = runtime_ctx.make_udf(schema_name=schema.name)
    udf_b = runtime_ctx.make_udf(schema_name=schema.name)

    sql_backend.execute(f"GRANT SELECT ON FUNCTION {udf_a.full_name} TO `{group.display_name}`")
    sql_backend.execute(f"GRANT READ_METADATA ON FUNCTION {udf_a.full_name} TO `{group.display_name}`")
    sql_backend.execute(f"ALTER FUNCTION {udf_a.full_name} OWNER TO `{group.display_name}`")
    sql_backend.execute(f"GRANT ALL PRIVILEGES ON FUNCTION {udf_b.full_name} TO `{group.display_name}`")

    grants = GrantsCrawler(runtime_ctx.tables_crawler, runtime_ctx.udfs_crawler)

    actual_grants = defaultdict(set)
    for grant in grants.snapshot():
        actual_grants[f"{grant.principal}.{grant.object_key}"].add(grant.action_type)

    assert {"SELECT", "READ_METADATA", "OWN"} == actual_grants[f"{group.display_name}.{udf_a.full_name}"]
    assert {"SELECT", "READ_METADATA"} == actual_grants[f"{group.display_name}.{udf_b.full_name}"]


def test_wide_mount_grants_to_uc_acls(sql_backend, inventory_schema, make_schema, env_or_skip, runtime_ctx,
                                      make_table, make_user, make_cluster, make_cluster_permissions, make_dbfs_data_copy,
                                      make_catalog):
    _SPARK_CONF = {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*]",
        "fs.azure.account.auth.type.labsazurethings.dfs.core.windows.net": "OAuth",
        "fs.azure.account.oauth.provider.type.labsazurethings.dfs.core.windows.net": "org.apache.hadoop.fs"
                                                                                     ".azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id.labsazurethings.dfs.core.windows.net": "dummy_application_id",
        "fs.azure.account.oauth2.client.secret.labsazurethings.dfs.core.windows.net": "dummy",
        "fs.azure.account.oauth2.client.endpoint.labsazurethings.dfs.core.windows.net": "https://login"
                                                                                        ".microsoftonline.com/directory_12345/oauth2/token",
    }
    cluster = make_cluster(single_node=True, spark_conf=_SPARK_CONF, data_security_mode=DataSecurityMode.NONE)
    new_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/{runtime_ctx.inventory_database}'

    user = make_user()
    make_cluster_permissions(object_id=cluster.cluster_id,
                             permission_level=PermissionLevel.CAN_ATTACH_TO,
                             user_name=user.user_name,)

    make_dbfs_data_copy(src_path=f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/c', dst_path=new_mounted_location)
    src_schema = make_schema(catalog_name="hive_metastore")
    src_external_table = make_table(
        catalog_name=src_schema.catalog_name, schema_name=src_schema.name, external_csv=new_mounted_location, name="ppai_debug_test"
    )
    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    rules = [
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema.name,
            dst_schema.name,
            src_external_table.name,
            src_external_table.name,
        ),
    ]
    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_azure_resource_permission()

