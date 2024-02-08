import json
import pathlib
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails, Policy
from databricks.sdk.service.pipelines import GetPipelineResponse

__dir = pathlib.Path(__file__).parent


def _load_fixture(filename: str):
    with (__dir / filename).open("r") as f:
        return json.load(f)


def _load_list(cls: type, filename: str):
    return [cls.from_dict(_) for _ in _load_fixture(filename)]  # type: ignore[attr-defined]


def _cluster_policy(policy_id: str):
    fixture = _load_fixture(f"policies/{policy_id}.json")
    definition = json.dumps(fixture["definition"])
    overrides = json.dumps(fixture["policy_family_definition_overrides"])
    return Policy(description=definition, policy_family_definition_overrides=overrides)


def _pipeline_cluster(pipeline_id: str):
    pipeline_response = _load_list(GetPipelineResponse, f"clusters/{pipeline_id}.json")[0]
    return pipeline_response


def workspace_client_mock(clusters="no-spark-conf.json"):
    ws = create_autospec(WorkspaceClient)
    ws.clusters.list.return_value = _load_list(ClusterDetails, f"../assessment/clusters/{clusters}")
    ws.cluster_policies.get = _cluster_policy
    ws.pipelines.get = _pipeline_cluster
    return ws
