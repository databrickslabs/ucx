from unittest.mock import create_autospec, call

import pytest
from databricks.labs.blueprint.installation import MockInstallation

from databricks.sdk.service.sql import (
    Query,
    Dashboard,
    Widget,
    QueryOptions,
    LegacyQuery,
    LegacyVisualization,
)

from databricks.labs.ucx.source_code.redash import Redash

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied, NotFound


@pytest.fixture
def redash_ws():
    workspace_client = create_autospec(WorkspaceClient)
    workspace_client.workspace.get_status.side_effect = NotFound("error")
    workspace_client.queries.create.return_value = Query(id="123")
    workspace_client.dashboards.list.return_value = [
        Dashboard(
            id="1",
            widgets=[
                Widget(
                    visualization=LegacyVisualization(
                        query=LegacyQuery(
                            id="1",
                            name="test_query",
                            query="SELECT * FROM old.things",
                            options=QueryOptions(catalog="hive_metastore", schema="default"),
                            tags=["test_tag"],
                        )
                    )
                ),
                Widget(
                    visualization=LegacyVisualization(
                        query=LegacyQuery(
                            id="1",
                            name="test_query",
                            query="SELECT * FROM old.things",
                            tags=[Redash.MIGRATED_TAG],
                        )
                    )
                ),
                None,
            ],
        ),
        Dashboard(
            id="2",
            tags=[Redash.MIGRATED_TAG],
            widgets=[
                Widget(
                    visualization=LegacyVisualization(
                        query=LegacyQuery(
                            id="1",
                            name="test_query",
                            query="SELECT * FROM old.things",
                            tags=[Redash.MIGRATED_TAG],
                        )
                    )
                ),
                Widget(visualization=LegacyVisualization(query=LegacyQuery(id="2", query="SELECT"))),
                Widget(
                    visualization=LegacyVisualization(
                        query=LegacyQuery(id="3", query="SELECT", tags=[Redash.MIGRATED_TAG])
                    )
                ),
            ],
        ),
        Dashboard(id="3", tags=[]),
    ]
    workspace_client.dashboards.get.return_value = Dashboard(
        id="2",
        tags=[Redash.MIGRATED_TAG],
        widgets=[
            Widget(
                visualization=LegacyVisualization(
                    query=LegacyQuery(
                        id="1",
                        name="test_query",
                        query="SELECT * FROM old.things",
                        tags=[Redash.MIGRATED_TAG],
                    )
                )
            )
        ],
    )

    return workspace_client


@pytest.fixture
def redash_installation():
    installation = MockInstallation(
        {
            "backup/queries/1.json": {"id": "1", "query": "original_query"},
            "backup/queries/3.json": {"id": "3", "query": "original_query", "tags": ["test_tag"]},
        }
    )
    return installation


def test_migrate_all_dashboards(redash_ws, empty_index, redash_installation):
    redash = Redash(empty_index, redash_ws, redash_installation)
    redash.migrate_dashboards()
    redash_installation.assert_file_written(
        "backup/queries/1.json",
        {
            'id': '1',
            'name': 'test_query',
            'options': {'catalog': 'hive_metastore', 'schema': 'default'},
            'query': 'SELECT * FROM old.things',
            'tags': ['test_tag'],
        },
    )
    redash_ws.queries.update.assert_called_with(
        "1",
        query='SELECT * FROM old.things',
        tags=[Redash.MIGRATED_TAG, 'test_tag'],
    )


def test_migrate_all_dashboards_error(redash_ws, empty_index, redash_installation, caplog):
    redash_ws.dashboards.list.side_effect = PermissionDenied("error")
    redash = Redash(empty_index, redash_ws, redash_installation)
    redash.migrate_dashboards()
    assert "Cannot list dashboards" in caplog.text


def test_revert_single_dashboard(redash_ws, empty_index, redash_installation, caplog):
    redash_ws.queries.get.return_value = LegacyQuery(id="1", query="original_query")
    redash = Redash(empty_index, redash_ws, redash_installation)
    redash.revert_dashboards("2")
    redash_ws.queries.update.assert_called_with("1", query="original_query", tags=None)
    redash_ws.queries.update.side_effect = PermissionDenied("error")
    redash.revert_dashboards("2")
    assert "Cannot restore" in caplog.text


def test_revert_dashboards(redash_ws, empty_index, redash_installation):
    redash_ws.queries.get.return_value = LegacyQuery(id="1", query="original_query")
    redash = Redash(empty_index, redash_ws, redash_installation)
    redash.revert_dashboards()
    redash_ws.queries.update.assert_has_calls(
        [
            call("1", query="original_query", tags=None),
            call("3", query="original_query", tags=["test_tag"]),
        ]
    )


def test_get_queries_from_dashboard(redash_ws):
    empty_dashboard = Dashboard(
        id="1",
    )
    assert len(list(Redash.get_queries_from_dashboard(empty_dashboard))) == 0
    dashboard = Dashboard(
        id="1",
        widgets=[
            Widget(),
            Widget(visualization=LegacyVisualization()),
            Widget(
                visualization=LegacyVisualization(
                    query=LegacyQuery(
                        id="1",
                        name="test_query",
                        query="SELECT * FROM old.things",
                    )
                )
            ),
        ],
    )
    queries = list(Redash.get_queries_from_dashboard(dashboard))
    assert len(queries) == 1
    assert queries[0].id == "1"
