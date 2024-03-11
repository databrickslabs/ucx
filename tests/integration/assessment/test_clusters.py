from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.compute import DataSecurityMode

from databricks.labs.ucx.assessment.clusters import ClustersCrawler, PoliciesCrawler

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


def test_cluster_crawler_no_isolation(ws, make_cluster, inventory_schema, sql_backend):
    created_cluster = make_cluster(data_security_mode=DataSecurityMode.NONE, num_workers=1)
    cluster_crawler = ClustersCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    clusters = cluster_crawler.snapshot()
    results = []
    for cluster in clusters:
        if cluster.cluster_id == created_cluster.cluster_id:
            results.append(cluster)

    assert len(results) == 1
    assert results[0].failures == '["No isolation shared clusters not supported in UC"]'


@retried(on=[NotFound], timeout=timedelta(minutes=6))
def test_policy_crawler(ws, make_cluster_policy, inventory_schema, sql_backend):
    created_policy = make_cluster_policy(name="test_policy_check")
    policy_crawler = PoliciesCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    policies = policy_crawler.snapshot()
    results = []
    for policy in policies:
        if policy.policy_id == created_policy.policy_id:
            results.append(policy)

    assert len(results) >= 1
