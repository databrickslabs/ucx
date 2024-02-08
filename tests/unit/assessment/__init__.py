import json
import pathlib
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import ClusterDetails, Policy
from databricks.sdk.service.jobs import BaseJob
from databricks.sdk.service.pipelines import GetPipelineResponse, PipelineStateInfo
from databricks.sdk.service.sql import EndpointConfPair
from databricks.sdk.service.workspace import GetSecretResponse

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


def _pipeline(pipeline_id: str):
    fixture = _load_fixture(f"pipelines/{pipeline_id}.json")
    return GetPipelineResponse.from_dict(fixture)


def _secret_not_found(secret_scope, secret_key):
    msg = f"Secret Scope {secret_scope} does not exist!"
    raise NotFound(msg)


def get_az_api_mapping(*args, **kwargs):
    mapping = _load_fixture("../assessment/azure/mappings.json")[0]
    if args[1] in mapping:
        return mapping[args[1]]
    else:
        return {}


def workspace_client_mock(
    clusters="no-spark-conf.json",
    pipelines="single-pipeline.json",
    pipeline_spec="empty-pipeline-spec.json",
    jobs="single-job.json",
    warehouse_config="single-config.json",
    secret_exists=True,
):
    ws = create_autospec(WorkspaceClient)
    ws.clusters.list.return_value = _load_list(ClusterDetails, f"../assessment/clusters/{clusters}")
    ws.cluster_policies.get = _cluster_policy
    ws.pipelines.list_pipelines.return_value = _load_list(PipelineStateInfo, f"../assessment/pipelines/{pipelines}")
    ws.pipelines.get = _pipeline
    ws.jobs.list.return_value = _load_list(BaseJob, f"../assessment/jobs/{jobs}")
    ws.warehouses.get_workspace_warehouse_config().data_access_config = _load_list(
        EndpointConfPair, f"../assessment/warehouses/{warehouse_config}"
    )
    if secret_exists:
        ws.secrets.get_secret.return_value = GetSecretResponse(key="username", value="SGVsbG8sIFdvcmxkIQ==")
    else:
        ws.secrets.get_secret = _secret_not_found
    return ws
