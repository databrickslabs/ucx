import logging
from datetime import timedelta

from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.workspace_access.clusters import ClusterAccess

logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_prepare_environment(ws, make_cluster):
    cluster_response = make_cluster(
        cluster_name="remap_test_cluster",
        single_node=True,
        spark_version="9.1.x-scala2.12",
        driver_node_type_id="Standard_DS3_v2",
    )
    prompts = MockPrompts({"Select cluster access mode": "1"})
    latest_spark_version = ws.clusters.select_spark_version(latest=True)
    cluster_id = cluster_response.cluster_id
    initial_cluster_state = ws.clusters.get(cluster_id).state.value
    while initial_cluster_state == 'PENDING':
        initial_cluster_state = ws.clusters.get(cluster_id).state.value
    cluster = ClusterAccess(prompts, ws)
    cluster.map_cluster_to_uc(cluster_id)
    cluster_details = ws.clusters.get(cluster_id)
    spark_version = cluster_details.spark_version
    assert spark_version == latest_spark_version
    assert cluster_details.data_security_mode.value == "SINGLE_USER"
