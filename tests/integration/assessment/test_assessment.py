# def test_table_inventory(ws, make_catalog, make_schema):
#     pytest.skip("test is broken")

import pytest
from databricks.labs.ucx.mixins.compute import CommandExecutor


@pytest.mark.skip(reason="Needs to have DLT Pipelines already created ")
def test_pipeline_crawler(ws, wsfs_wheel, make_schema, sql_fetch_all):
    _, inventory_database = make_schema(catalog="hive_metastore", schema="ucx_dk").split(".")
    commands = CommandExecutor(ws)
    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")

    commands.run(
        f"""
        from databricks.sdk import WorkspaceClient
        from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.labs.ucx.config import WorkspaceConfig, GroupsConfig
        from databricks.labs.ucx.assessment.crawlers import PipelinesCrawler
        cfg = WorkspaceConfig(
        inventory_database="{inventory_database}",
        groups=GroupsConfig(auto=True))
        ws = WorkspaceClient(config=cfg.to_databricks_config())
        pipeline_crawler = PipelinesCrawler( ws=ws, sbe=RuntimeBackend(), schema=cfg.inventory_database)
        pipeline_crawler.snapshot()
        """
    )
    pipelines = sql_fetch_all(f"SELECT * FROM hive_metastore.{inventory_database}.pipelines")
    results = []
    for pipeline in pipelines:
        if pipeline["success"] != 0:
            continue
        results.append(pipeline)

    assert len(results) > 0
    assert results[0].pipeline_id == "7faaf6c7-f3bb-4b7c-af9c-8be38f32f658"
    assert results[0].pipeline_name == "ucx-test-pipeline"
    assert results[0].creator_name == "dipankar.kushari@databricks.com"

# Cluster config setup -
# fs.azure.account.auth.type.storage_acct_1.dfs.core.windows.net OAuth
# fs.azure.account.oauth.provider.type.storage_acct_1.dfs.core.windows.net org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
# fs.azure.account.oauth2.client.id.storage_acct_1.dfs.core.windows.net dummy_application_id
# fs.azure.account.oauth2.client.secret.storage_acct_1.dfs.core.windows.net ddddddddddddddddddd
# fs.azure.account.oauth2.client.endpoint.storage_acct_1.dfs.core.windows.net https://login.microsoftonline.com/dummy_tenant_id/oauth2/token


def test_cluster_crawler(ws, wsfs_wheel, make_schema, sql_fetch_all):
    _, inventory_database = make_schema(catalog="hive_metastore", schema="ucx_dk").split(".")
    commands = CommandExecutor(ws)
    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")

    commands.run(
        f"""
        from databricks.sdk import WorkspaceClient
        from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.labs.ucx.config import WorkspaceConfig, GroupsConfig
        from databricks.labs.ucx.assessment.crawlers import ClustersCrawler
        cfg = WorkspaceConfig(
        inventory_database="{inventory_database}",
        groups=GroupsConfig(auto=True))
        ws = WorkspaceClient(config=cfg.to_databricks_config())
        cluster_crawler = ClustersCrawler( ws=ws, sbe=RuntimeBackend(), schema=cfg.inventory_database)
        cluster_crawler.snapshot()
        """
    )
    clusters = sql_fetch_all(f"SELECT * FROM hive_metastore.{inventory_database}.clusters")
    results = []
    for cluster in clusters:
        if cluster["success"] != 0:
            continue
        results.append(cluster)

    assert len(results) > 0
    assert results[0].cluster_id == "1005-133624-c9lbpv58"
    assert results[0].cluster_name == "Dipankar Kushari's Cluster"
    assert results[0].creator == "dipankar.kushari@databricks.com"


# def test_spn_crawler(ws, wsfs_wheel, make_schema, sql_fetch_all):
#     _, inventory_database = make_schema(catalog="hive_metastore", schema="ucx_dk").split(".")
#     commands = CommandExecutor(ws)
#     commands.install_notebook_library(f"/Workspace{wsfs_wheel}")
#
#     commands.run(
#         f"""
#         from databricks.sdk import WorkspaceClient
#         from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
#         from databricks.labs.ucx.framework.crawlers import RuntimeBackend
#         from databricks.labs.ucx.config import WorkspaceConfig, GroupsConfig
#         from databricks.labs.ucx.assessment.crawlers import AzureServicePrincipalCrawler
#         cfg = WorkspaceConfig(
#         inventory_database="{inventory_database}",
#         groups=GroupsConfig(auto=True))
#         ws = WorkspaceClient(config=cfg.to_databricks_config())
#         spn_crawler = AzureServicePrincipalCrawler( ws=ws, sbe=RuntimeBackend(), schema=cfg.inventory_database)
#         spn_crawler.snapshot()
#         """
#     )
#     spns = sql_fetch_all(f"SELECT * FROM hive_metastore.{inventory_database}.azure_service_principals")
#     results = []
#     for spn in spns:
#         if spn['success'] != 0:
#             continue
#         results.append(spn)
#
#     assert len(results) > 0
    # assert results[0].cluster_id == "1005-055227-lnfw00g5"
    # assert results[0].cluster_name == "Dipankar Kushari's Cluster"
    # assert results[0].creator == "dipankar.kushari@databricks.com"
