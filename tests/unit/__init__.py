import base64
import dataclasses
import io
import json
import logging
import os
import pathlib
from pathlib import Path
from unittest.mock import create_autospec

import yaml
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import ClusterDetails, Policy
from databricks.sdk.service.jobs import BaseJob, BaseRun
from databricks.sdk.service.pipelines import GetPipelineResponse, PipelineStateInfo
from databricks.sdk.service.sql import EndpointConfPair
from databricks.sdk.service.workspace import ExportResponse, GetSecretResponse, ObjectInfo
from databricks.sdk.service import iam
from databricks.labs.ucx.hive_metastore.mapping import TableMapping, TableToMigrate
from databricks.labs.ucx.source_code.graph import BaseNotebookResolver, SourceContainer
from databricks.labs.ucx.source_code.path_lookup import PathLookup

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
    # TODO: remove the usage of it in favor of MockPathLookup
    if not filenames:
        return []
    installation = MockInstallation(DEFAULT_CONFIG | {_: _load_source(f'{_FOLDERS[cls]}/{_}') for _ in filenames})
    # cleanly avoid mypy error
    setattr(installation, "_unmarshal_type", lambda as_dict, filename, type_ref: as_dict)
    return [installation.load(cls, filename=_) for _ in filenames]


def _samples_path(cls: type) -> str:
    return (__dir / _FOLDERS[cls]).as_posix()


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


class MockPathLookup(PathLookup):
    def __init__(self, cwd='source_code/samples', sys_paths: list[Path] | None = None):
        super().__init__(pathlib.Path(__file__).parent / cwd, sys_paths or [])

    def change_directory(self, new_working_directory: Path) -> 'MockPathLookup':
        return MockPathLookup(new_working_directory, self._sys_paths)

    def resolve(self, path: pathlib.Path) -> pathlib.Path | None:
        candidates = [path]
        if not path.name.endswith('.txt'):
            candidates.append(Path(f"{path}.txt"))
        for candidate in candidates:
            absolute_path = super().resolve(candidate)
            if not absolute_path:
                continue
            return absolute_path
        return None

    def __repr__(self):
        return f"<MockPathLookup {self._cwd}>"


def mock_workspace_client(
    cluster_ids: list[str] | None = None,
    pipeline_ids: list[str] | None = None,
    job_ids: list[str] | None = None,
    jobruns_ids: list[str] | None = None,
    policy_ids: list[str] | None = None,
    warehouse_config="single-config",
    secret_exists=True,
):
    ws = create_autospec(WorkspaceClient)
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.clusters.list.return_value = _id_list(ClusterDetails, cluster_ids)
    ws.cluster_policies.list.return_value = _id_list(Policy, policy_ids)
    ws.cluster_policies.get = _cluster_policy
    ws.pipelines.list_pipelines.return_value = _id_list(PipelineStateInfo, pipeline_ids)
    ws.pipelines.get = _pipeline
    ws.workspace.get_status = lambda _: ObjectInfo(object_id=123)
    ws.jobs.list.return_value = _id_list(BaseJob, job_ids)
    ws.jobs.list_runs.return_value = _id_list(BaseRun, jobruns_ids)
    ws.warehouses.get_workspace_warehouse_config().data_access_config = _load_list(EndpointConfPair, warehouse_config)
    ws.workspace.export = _workspace_export
    if secret_exists:
        ws.secrets.get_secret.return_value = GetSecretResponse(key="username", value="SGVsbG8sIFdvcmxkIQ==")
    else:
        ws.secrets.get_secret = _secret_not_found
    download_yaml = yaml.dump(
        {
            'version': 1,
            'inventory_database': 'ucx_exists',
            'connect': {
                'host': '...',
                'token': '...',
            },
            'installed_workspace_ids': [123, 456],
        }
    )
    ws.workspace.download.return_value = io.StringIO(download_yaml)
    return ws


def mock_table_mapping(tables: list[str] | None = None):
    table_mapping = create_autospec(TableMapping)
    table_mapping.get_tables_to_migrate.return_value = _id_list(TableToMigrate, tables)
    return table_mapping


def locate_site_packages() -> pathlib.Path:
    project_path = pathlib.Path(os.path.dirname(__file__)).parent.parent
    python_lib_path = pathlib.Path(project_path, ".venv", "lib")
    actual_python = next(file for file in os.listdir(str(python_lib_path)) if file.startswith("python3."))
    return pathlib.Path(python_lib_path, actual_python, "site-packages")


def mock_notebook_resolver():
    resolver = create_autospec(BaseNotebookResolver)
    resolver.resolve_notebook.return_value = None
    return resolver
