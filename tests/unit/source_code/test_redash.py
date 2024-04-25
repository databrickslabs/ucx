from unittest import mock
from unittest.mock import create_autospec, call

import pytest
from databricks.sdk.service.sql import Query, Dashboard

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.queries import FromTable
from databricks.labs.ucx.source_code.redash import Redash

from databricks.sdk import WorkspaceClient


@pytest.fixture
def redash_ws():
    def api_client_do(method: str, path: str, body: dict | None = None):
        _ = body
        tags_lookup = {"1": [Redash.BACKUP_TAG], "2": [Redash.MIGRATED_TAG, "backup: 123", "original"], "3": []}
        if method == "POST":
            return {}
        if path.startswith("/api/2.0/preview/sql/dashboards"):
            dashboard_id = path.removeprefix("/api/2.0/preview/sql/dashboards/")
            tags = tags_lookup[dashboard_id]
            return {
                "widgets": [
                    {},
                    {"visualization": {}},
                    {
                        "visualization": {
                            "query": {
                                "id": "1",
                                "name": "test_query",
                                "query": "SELECT * FROM old.things",
                                "tags": tags,
                            }
                        }
                    },
                    {
                        "visualization": {
                            "query": {
                                "id": "1",
                                "name": "test_query",
                                "query": "SELECT * FROM old.things",
                                "tags": None,
                            }
                        }
                    },
                ]
            }
        if path.startswith("/api/2.0/preview/sql/queries"):
            return {}
        return {}

    workspace_client = create_autospec(WorkspaceClient)
    workspace_client.api_client.do.side_effect = api_client_do
    workspace_client.queries.create.return_value = Query(id="123")
    workspace_client.dashboards.list.return_value = [
        Dashboard(id="1", tags=[Redash.BACKUP_TAG]),
        Dashboard(id="2", tags=[Redash.MIGRATED_TAG]),
        Dashboard(id="3", tags=[]),
    ]

    return workspace_client


@pytest.fixture
def empty_fixer(empty_index):
    return FromTable(empty_index, CurrentSessionState())


def test_fix_all_dashboards(redash_ws, empty_fixer):
    redash = Redash(empty_fixer, redash_ws)
    redash.fix_all_dashboards()
    redash_ws.queries.create.assert_called_with(
        name='test_query_original',
        query='SELECT * FROM old.things',
        data_source_id=None,
        description=None,
        options=None,
        parent=None,
        run_as_role=None,
    )
    redash_ws.api_client.do.assert_has_calls(
        [
            call('GET', '/api/2.0/preview/sql/dashboards/3'),
            # tag the backup query
            call('POST', '/api/2.0/preview/sql/queries/123', body={'tags': [Redash.BACKUP_TAG]}),
            # tag the migrated query
            call(
                'POST',
                '/api/2.0/preview/sql/queries/1',
                body={'query': 'SELECT * FROM old.things', 'tags': [Redash.MIGRATED_TAG, "backup: 123"]},
            ),
        ]
    )


def test_revert_dashboard(redash_ws, empty_fixer):
    redash = Redash(empty_fixer, redash_ws)
    redash.revert_dashboard(Dashboard(id="2", tags=[Redash.MIGRATED_TAG]))
    redash_ws.api_client.do.assert_has_calls(
        [
            call('GET', '/api/2.0/preview/sql/dashboards/2'),
            # update the migrated query
            call(
                'POST',
                '/api/2.0/preview/sql/queries/1',
                body={'query': mock.ANY, 'tags': []},
            ),
        ]
    )
    redash_ws.queries.delete.assert_called_once_with("123")


def test_delete_backup_dashboards(redash_ws, empty_fixer):
    redash = Redash(empty_fixer, redash_ws)
    redash.delete_backup_dashboards()
    redash_ws.dashboards.delete.assert_called_once_with("1")
