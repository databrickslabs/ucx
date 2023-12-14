from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.assessment.clusters import ClustersCrawler

from .test_assessment import _SPARK_CONF


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_cluster_crawler(ws, make_cluster, inventory_schema, sql_backend):
    created_cluster = make_cluster(single_node=True, spark_conf=_SPARK_CONF)
    cluster_crawler = ClustersCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    clusters = cluster_crawler.snapshot()
    results = []
    for cluster in clusters:
        if cluster.success != 0:
            continue
        if cluster.cluster_id == created_cluster.cluster_id:
            results.append(cluster)

    assert len(results) >= 1
    assert results[0].cluster_id == created_cluster.cluster_id
