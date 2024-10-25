import json
from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.assessment.clusters import ClusterInfo, PolicyInfo
from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.progress.clusters import ClusterProgressEncoder, ClusterPolicyProgressEncoder


@pytest.mark.parametrize(
    "cluster_info",
    [
        ClusterInfo("id", 1, failures=""),
    ],
)
def test_cluster_progress_encoder_no_failures(mock_backend, cluster_info: ClusterInfo) -> None:
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    encoder = ClusterProgressEncoder(
        mock_backend,
        ownership,
        ClusterInfo,
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([cluster_info])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert len(rows[0].failures) == 0
    ownership.owner_of.assert_called_once()


@pytest.mark.parametrize(
    "cluster_info",
    [
        ClusterInfo("id", 1, failures='["not supported DBT: old"]', spark_version="old"),
    ],
)
def test_cluster_progress_encoder_failures(mock_backend, cluster_info: ClusterInfo) -> None:
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    encoder = ClusterProgressEncoder(
        mock_backend,
        ownership,
        ClusterInfo,
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([cluster_info])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert rows[0].failures == json.loads(cluster_info.failures)
    ownership.owner_of.assert_called_once()


@pytest.mark.parametrize(
    "policy_info",
    [
        PolicyInfo("id", "name", 1, failures=""),
    ],
)
def test_cluster_policy_progress_encoder_no_failures(mock_backend, policy_info: PolicyInfo) -> None:
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    encoder = ClusterPolicyProgressEncoder(
        mock_backend,
        ownership,
        PolicyInfo,
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([policy_info])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert len(rows[0].failures) == 0
    ownership.owner_of.assert_called_once()


@pytest.mark.parametrize(
    "policy_info",
    [
        PolicyInfo("id", "name", 1, failures='["Uses azure service principal credentials config in policy."]'),
    ],
)
def test_cluster_policy_progress_encoder_failures(mock_backend, policy_info: PolicyInfo) -> None:
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    encoder = ClusterPolicyProgressEncoder(
        mock_backend,
        ownership,
        PolicyInfo,
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([policy_info])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert rows[0].failures == json.loads(policy_info.failures)
    ownership.owner_of.assert_called_once()
