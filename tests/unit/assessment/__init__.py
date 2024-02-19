import base64
import json
import pathlib
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import ClusterDetails, Policy
from databricks.sdk.service.jobs import BaseJob
from databricks.sdk.service.pipelines import GetPipelineResponse, PipelineStateInfo
from databricks.sdk.service.sql import EndpointConfPair
from databricks.sdk.service.workspace import ExportResponse, GetSecretResponse

__dir = pathlib.Path(__file__).parent


def _base64(filename: str):
    with (__dir / filename).open("rb") as f:
        return base64.b64encode(f.read())


def _workspace_export(filename: str):
    res = _base64(f'workspace/{filename}')
    return ExportResponse(content=res.decode('utf8'))


def _load_fixture(filename: str):
    with (__dir / filename).open("r") as f:
        return json.load(f)


_FOLDERS = {
    ClusterDetails: '../assessment/clusters',
    PipelineStateInfo: '../assessment/pipelines',
}


def _load_list(cls: type, filename: str, ids=None):
    if not ids:  # TODO: remove
        return [cls.from_dict(_) for _ in _load_fixture(filename)]  # type: ignore[attr-defined]
    return _id_list(cls, ids)


def _id_list(cls: type, ids=None):
    if not ids:
        return []
    return [cls.from_dict(_load_fixture(f'{_FOLDERS[cls]}/{_}.json')) for _ in ids] # type: ignore[attr-defined]


def _cluster_policy(policy_id: str):
    fixture = _load_fixture(f"policies/{policy_id}.json")
    definition = json.dumps(fixture["definition"])
    overrides = json.dumps(fixture["policy_family_definition_overrides"])
    return Policy(description=definition, policy_family_definition_overrides=overrides)


def _pipeline(pipeline_id: str):
    fixture = _load_fixture(f"pipelines/{pipeline_id}.json")
    return GetPipelineResponse.from_dict(fixture)


def _secret_not_found(secret_scope, _):
    msg = f"Secret Scope {secret_scope} does not exist!"
    raise NotFound(msg)


def workspace_client_mock(
    cluster_ids: list[str] | None = None,
    pipelines="single-pipeline.json",
    jobs="single-job.json",
    warehouse_config="single-config.json",
    secret_exists=True,
):
    ws = create_autospec(WorkspaceClient)
    ws.clusters.list.return_value = _id_list(ClusterDetails, cluster_ids)
    ws.cluster_policies.get = _cluster_policy
    ws.pipelines.list_pipelines.return_value = _load_list(PipelineStateInfo, f"../assessment/pipelines/{pipelines}")
    ws.pipelines.get = _pipeline
    ws.jobs.list.return_value = _load_list(BaseJob, f"../assessment/jobs/{jobs}")
    ws.warehouses.get_workspace_warehouse_config().data_access_config = _load_list(
        EndpointConfPair, f"../assessment/warehouses/{warehouse_config}"
    )
    ws.workspace.export = _workspace_export
    if secret_exists:
        ws.secrets.get_secret.return_value = GetSecretResponse(key="username", value="SGVsbG8sIFdvcmxkIQ==")
    else:
        ws.secrets.get_secret = _secret_not_found
    return ws
