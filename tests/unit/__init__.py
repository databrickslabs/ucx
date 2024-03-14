import base64
import json
import logging
import pathlib
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import ClusterDetails, Policy
from databricks.sdk.service.jobs import BaseJob, BaseRun
from databricks.sdk.service.pipelines import GetPipelineResponse, PipelineStateInfo
from databricks.sdk.service.sql import EndpointConfPair
from databricks.sdk.service.workspace import ExportResponse, GetSecretResponse

from databricks.labs.ucx.hive_metastore.mapping import TableMapping, TableToMigrate

logging.getLogger("tests").setLevel("DEBUG")

DEFAULT_CONFIG = {
    "config.yml": {
        'version': 2,
        'inventory_database': 'ucx',
        'policy_id': 'policy_id',
        'connect': {
            'host': 'foo',
            'token': 'bar',
        },
    },
}

__dir = pathlib.Path(__file__).parent


def _base64(filename: str):
    try:
        with (__dir / filename).open("rb") as f:
            return base64.b64encode(f.read())
    except FileNotFoundError as err:
        raise NotFound(filename) from err


def _workspace_export(filename: str):
    res = _base64(f'workspace/{filename}')
    return ExportResponse(content=res.decode('utf8'))


def _load_fixture(filename: str):
    try:
        with (__dir / filename).open("r") as f:
            return json.load(f)
    except FileNotFoundError as err:
        raise NotFound(filename) from err


_FOLDERS = {
    BaseJob: 'assessment/jobs',
    BaseRun: 'assessment/jobruns',
    ClusterDetails: 'assessment/clusters',
    PipelineStateInfo: 'assessment/pipelines',
    Policy: 'assessment/policies',
    TableToMigrate: 'hive_metastore/tables',
}


def _load_list(cls: type, filename: str, ids=None):
    if not ids:  # TODO: remove
        return [cls.from_dict(_) for _ in _load_fixture(filename)]  # type: ignore[attr-defined]
    return _id_list(cls, ids)


def _id_list(cls: type, ids=None):
    if not ids:
        return []
    return [cls.from_dict(_load_fixture(f'{_FOLDERS[cls]}/{_}.json')) for _ in ids]  # type: ignore[attr-defined]


def _cluster_policy(policy_id: str):
    fixture = _load_fixture(f"assessment/policies/{policy_id}.json")
    definition = json.dumps(fixture["definition"])
    overrides = json.dumps(fixture["policy_family_definition_overrides"])
    return Policy(description=definition, policy_family_definition_overrides=overrides)


def _pipeline(pipeline_id: str):
    fixture = _load_fixture(f"assessment/pipelines/{pipeline_id}.json")
    return GetPipelineResponse.from_dict(fixture)


def _secret_not_found(secret_scope, _):
    msg = f"Secret Scope {secret_scope} does not exist!"
    raise NotFound(msg)


def workspace_client_mock(
    cluster_ids: list[str] | None = None,
    pipeline_ids: list[str] | None = None,
    job_ids: list[str] | None = None,
    jobruns_ids: list[str] | None = None,
    policy_ids: list[str] | None = None,
    warehouse_config="single-config.json",
    secret_exists=True,
):
    ws = create_autospec(WorkspaceClient)
    ws.clusters.list.return_value = _id_list(ClusterDetails, cluster_ids)
    ws.cluster_policies.list.return_value = _id_list(Policy, policy_ids)
    ws.cluster_policies.get = _cluster_policy
    ws.pipelines.list_pipelines.return_value = _id_list(PipelineStateInfo, pipeline_ids)
    ws.pipelines.get = _pipeline
    ws.jobs.list.return_value = _id_list(BaseJob, job_ids)
    ws.jobs.list_runs.return_value = _id_list(BaseRun, jobruns_ids)
    ws.warehouses.get_workspace_warehouse_config().data_access_config = _load_list(
        EndpointConfPair, f"assessment/warehouses/{warehouse_config}"
    )
    ws.workspace.export = _workspace_export
    if secret_exists:
        ws.secrets.get_secret.return_value = GetSecretResponse(key="username", value="SGVsbG8sIFdvcmxkIQ==")
    else:
        ws.secrets.get_secret = _secret_not_found
    return ws


def table_mapping_mock(tables: list[str] | None = None):
    table_mapping = create_autospec(TableMapping)
    table_mapping.get_tables_to_migrate.return_value = _id_list(TableToMigrate, tables)
    return table_mapping
