import base64
import dataclasses
import json
import logging
import os
import pathlib
from unittest.mock import create_autospec
from typing import BinaryIO

from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import ClusterDetails, Policy
from databricks.sdk.service.jobs import BaseJob, BaseRun
from databricks.sdk.service.pipelines import GetPipelineResponse, PipelineStateInfo
from databricks.sdk.service.sql import EndpointConfPair
from databricks.sdk.service.workspace import ExportResponse, GetSecretResponse, Language

from databricks.labs.ucx.hive_metastore.mapping import TableMapping, TableToMigrate
from databricks.labs.ucx.source_code.dependency_loaders import SourceContainer
from databricks.labs.ucx.source_code.files import LocalFile
from databricks.labs.ucx.source_code.notebook import Notebook, NOTEBOOK_HEADER
from databricks.labs.ucx.source_code.whitelist import Whitelist

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

GROUPS = MockBackend.rows(
    "id_in_workspace",
    "name_in_workspace",
    "name_in_account",
    "temporary_name",
    "members",
    "entitlements",
    "external_id",
    "roles",
)

PERMISSIONS = MockBackend.rows(
    "object_id",
    "object_type",
    "raw",
)

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


def _load_source(filename: str):
    try:
        with (__dir / filename).open("r") as f:
            return f.read()
    except FileNotFoundError as err:
        raise NotFound(filename) from err


_FOLDERS = {
    BaseJob: 'assessment/jobs',
    BaseRun: 'assessment/jobruns',
    ClusterDetails: 'assessment/clusters',
    PipelineStateInfo: 'assessment/pipelines',
    Policy: 'assessment/policies',
    TableToMigrate: 'hive_metastore/tables',
    EndpointConfPair: 'assessment/warehouses',
    SourceContainer: 'source_code/samples',
}


def _load_list(cls: type, filename: str):
    fixtures = _load_fixture(f'{_FOLDERS[cls]}/{filename}.json')
    installation = MockInstallation(DEFAULT_CONFIG | {str(num): fixture for num, fixture in enumerate(fixtures)})
    return [installation.load(cls, filename=str(num)) for num in range(len(fixtures))]


def _id_list(cls: type, ids=None):
    if not ids:
        return []
    installation = MockInstallation(DEFAULT_CONFIG | {_: _load_fixture(f'{_FOLDERS[cls]}/{_}.json') for _ in ids})
    if cls is Policy:
        output = []
        for _ in ids:
            raw_json = _load_fixture(f'{_FOLDERS[cls]}/{_}.json')
            # need special handling for reading definition & overrides
            policy: Policy = dataclasses.replace(
                installation.load(cls, filename=_),
                definition=json.dumps(raw_json["definition"]),
                policy_family_definition_overrides=json.dumps(raw_json["policy_family_definition_overrides"]),
            )
            output.append(policy)
        return output
    return [installation.load(cls, filename=_) for _ in ids]


def _load_sources(cls: type, *filenames: str):
    if not filenames:
        return []
    installation = MockInstallation(DEFAULT_CONFIG | {_: _load_source(f'{_FOLDERS[cls]}/{_}') for _ in filenames})
    # cleanly avoid mypy error
    setattr(installation, "_unmarshal_type", lambda as_dict, filename, type_ref: as_dict)
    return [installation.load(cls, filename=_) for _ in filenames]


def _cluster_policy(policy_id: str):
    fixture = _load_fixture(f"{_FOLDERS[Policy]}/{policy_id}.json")
    definition = json.dumps(fixture["definition"])
    overrides = json.dumps(fixture["policy_family_definition_overrides"])
    return Policy(description=definition, policy_family_definition_overrides=overrides)


def _pipeline(pipeline_id: str):
    fixture = _load_fixture(f"{_FOLDERS[PipelineStateInfo]}/{pipeline_id}.json")
    return GetPipelineResponse.from_dict(fixture)


def _secret_not_found(secret_scope, _):
    msg = f"Secret Scope {secret_scope} does not exist!"
    raise NotFound(msg)


# can't remove **kwargs because it receives format=xxx
# pylint: disable=unused-argument
def _download_side_effect(sources: dict[str, str], visited: dict[str, bool], *args, **kwargs):
    filename = args[0]
    if filename.startswith('./'):
        filename = filename[2:]
    visited[filename] = True
    source = sources.get(filename, None)
    if filename.find(".py") < 0:
        filename = filename + ".py"
    if filename.find(".txt") < 0:
        filename = filename + ".txt"
    result = create_autospec(BinaryIO)
    if source is None:
        source = sources.get(filename)
    assert source is not None
    result.__enter__.return_value.read.return_value = source.encode("utf-8")
    return result


def _load_dependency_side_effect(sources: dict[str, str], visited: dict[str, bool], *args):
    dependency = args[0]
    filename = str(dependency.path)
    is_package_file = os.path.isfile(dependency.path)
    if is_package_file:
        with dependency.path.open("r") as f:
            source = f.read()
    else:
        if filename.startswith('./'):
            filename = filename[2:]
        visited[filename] = True
        source = sources.get(filename, None)
        if filename.find(".py") < 0:
            filename = filename + ".py"
        if filename.find(".txt") < 0:
            filename = filename + ".txt"
        if source is None:
            source = sources.get(filename)
    assert source is not None
    if NOTEBOOK_HEADER in source:
        return Notebook.parse(filename, source, Language.PYTHON)
    return LocalFile(pathlib.Path(filename), source, Language.PYTHON)


def _is_notebook_side_effect(sources: dict[str, str], *args):
    dependency = args[0]
    filename = str(dependency.path)
    if filename.startswith('./'):
        filename = filename[2:]
    source = sources.get(filename, None)
    if filename.find(".py") < 0:
        filename = filename + ".py"
    if filename.find(".txt") < 0:
        filename = filename + ".txt"
    if source is None:
        source = sources.get(filename)
    assert source is not None
    return NOTEBOOK_HEADER in source


def workspace_client_mock(
    cluster_ids: list[str] | None = None,
    pipeline_ids: list[str] | None = None,
    job_ids: list[str] | None = None,
    jobruns_ids: list[str] | None = None,
    policy_ids: list[str] | None = None,
    warehouse_config="single-config",
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
    ws.warehouses.get_workspace_warehouse_config().data_access_config = _load_list(EndpointConfPair, warehouse_config)
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


def whitelist_mock():
    wls = create_autospec(Whitelist)
    wls.compatibility.return_value = None
    return wls


def locate_site_packages() -> pathlib.Path:
    project_path = pathlib.Path(os.path.dirname(__file__)).parent.parent
    python_lib_path = pathlib.Path(project_path, ".venv", "lib")
    actual_python = next(file for file in os.listdir(str(python_lib_path)) if file.startswith("python3."))
    return pathlib.Path(python_lib_path, actual_python, "site-packages")
