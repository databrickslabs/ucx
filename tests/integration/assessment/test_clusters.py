import json
from datetime import timedelta

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.compute import DataSecurityMode

from databricks.labs.ucx.assessment.clusters import (
    ClustersCrawler,
    PoliciesCrawler,
    ClusterOwnership,
    ClusterPolicyOwnership,
)

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


def _change_cluster_owner(ws, cluster_id: str, owner_user_name: str) -> None:
    """Replacement for ClustersAPI.change_owner()."""
    # As of SDK 0.33.0 there is a call to wait for cluster termination that fails because it doesn't pass the cluster id
    body = {'cluster_id': cluster_id, 'owner_username': owner_user_name}
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    ws.api_client.do('POST', '/api/2.1/clusters/change-owner', body=body, headers=headers)


def test_cluster_ownership(ws, runtime_ctx, make_cluster, make_user, inventory_schema, sql_backend) -> None:
    """Verify the ownership can be determined for crawled clusters."""

    # Set up two clusters: one with us as owner and one for a different user.
    # TODO: Figure out how to clear the creator for a cluster.
    # (Contrary to the documentation for the creator field, deleting the user doesn't clear it immediately and waiting
    # for 10 min doesn't help: the UI reports no creator, but the REST API continues to report the deleted user.)
    another_user = make_user()
    my_cluster = make_cluster(single_node=True, spark_conf=_SPARK_CONF)
    their_cluster = make_cluster(single_node=True, spark_conf=_SPARK_CONF)
    ws.clusters.delete_and_wait(cluster_id=their_cluster.cluster_id)
    _change_cluster_owner(ws, their_cluster.cluster_id, owner_user_name=another_user.user_name)

    # Produce the crawled records.
    crawler = ClustersCrawler(ws, sql_backend, inventory_schema)
    records = crawler.snapshot(force_refresh=True)

    # Find the crawled records for our clusters.
    my_cluster_record = next(record for record in records if record.cluster_id == my_cluster.cluster_id)
    their_cluster_record = next(record for record in records if record.cluster_id == their_cluster.cluster_id)

    # Verify ownership is as expected.
    administrator_locator = runtime_ctx.administrator_locator
    ownership = ClusterOwnership(administrator_locator)
    assert ownership.owner_of(my_cluster_record) == ws.current_user.me().user_name
    assert ownership.owner_of(their_cluster_record) == another_user.user_name


def test_cluster_crawler_mlr_no_isolation(ws, make_cluster, inventory_schema, sql_backend):
    created_cluster = make_cluster(
        data_security_mode=DataSecurityMode.NONE, spark_version='15.4.x-cpu-ml-scala2.12', num_workers=1
    )
    cluster_crawler = ClustersCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    clusters = cluster_crawler.snapshot()
    results = []
    for cluster in clusters:
        if cluster.cluster_id == created_cluster.cluster_id:
            results.append(cluster)

    assert len(results) == 1
    expected_failures = (
        '["No isolation shared clusters not supported in UC",'
        + ' "Shared Machine Learning Runtime clusters are not supported in UC"]'
    )
    assert results[0].failures == expected_failures


@retried(on=[NotFound], timeout=timedelta(minutes=6))
def test_policy_crawler(ws, make_cluster_policy, inventory_schema, sql_backend, make_random):
    policy_1 = f"test_policy_check_{make_random(4)}"
    policy_2 = f"test_policy_check2_{make_random(4)}"
    created_policy = make_cluster_policy(
        name=f"{policy_1}",
        definition=json.dumps({"spark_version": {'type': 'fixed', 'value': '14.3.x-scala2.12'}}),
    )
    policy_definition = {
        "spark_version": {'type': 'fixed', 'value': '14.3.x-scala2.12'},
        "spark_conf.fs.azure.account.auth.type": {"type": "fixed", "value": "OAuth", "hidden": True},
    }
    created_policy_2 = make_cluster_policy(name=f"{policy_2}", definition=json.dumps(policy_definition))
    policy_crawler = PoliciesCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    policies = policy_crawler.snapshot()
    results = []
    for policy in policies:
        if policy.policy_id in (created_policy.policy_id, created_policy_2.policy_id):
            results.append(policy)

    assert results[0].policy_name == policy_1
    assert results[0].success == 1
    assert results[0].failures == "[]"
    assert results[0].spark_version == json.dumps({'type': 'fixed', 'value': '14.3.x-scala2.12'})

    assert results[1].policy_name == policy_2
    assert results[1].success == 0
    assert results[1].failures == '["Uses azure service principal credentials config in policy."]'


# TODO: Investigate whether this is a bug or something wrong with this fixture.
@pytest.mark.xfail(reason="Cluster policy creators always seem to be null.")
def test_cluster_policy_ownership(ws, runtime_ctx, make_cluster_policy, inventory_schema, sql_backend) -> None:
    """Verify the ownership can be determined for crawled cluster policies."""

    # Set up a cluster policy.
    # Note: there doesn't seem to be a way to change the owner of a cluster policy, so we can't test policies without
    # an owner.
    policy = make_cluster_policy()

    # Produce the crawled records.
    crawler = PoliciesCrawler(ws, sql_backend, inventory_schema)
    records = crawler.snapshot(force_refresh=True)

    # Find the crawled record for our cluster policy.
    policy_record = next(record for record in records if record.policy_id == policy.policy_id)

    # Verify ownership is as expected.
    ownership = ClusterPolicyOwnership(runtime_ctx.administrator_locator)
    assert ownership.owner_of(policy_record) == ws.current_user.me().user_name
