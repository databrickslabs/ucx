from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.tui import MockPrompts

from databricks.sdk.service.sql import Query, Dashboard, Widget, Visualization, QueryOptions

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
            tags=[Redash.BACKUP_TAG],
            widgets=[
                Widget(
                    visualization=Visualization(
                        query=Query(
                            id="1",
                            name="test_query",
                            query="SELECT * FROM old.things",
                            options=QueryOptions(catalog="hive_metastore", schema="default"),
                            tags=["test_tag"],
                        )
                    )
                ),
                Widget(
                    visualization=Visualization(
                        query=Query(
                            id="1",
                            name="test_query",
                            query="SELECT * FROM old.things",
                            tags=[Redash.MIGRATED_TAG, 'backup:123'],
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
                    visualization=Visualization(
                        query=Query(
                            id="1",
                            name="test_query",
                            query="SELECT * FROM old.things",
                            tags=[Redash.MIGRATED_TAG, 'backup:123'],
                        )
                    )
                ),
                Widget(visualization=Visualization(query=Query(id="2", query="SELECT"))),
                Widget(visualization=Visualization(query=Query(id="3", query="SELECT", tags=['backup:123']))),
                Widget(visualization=Visualization(query=Query(id="3", query="SELECT", tags=[Redash.MIGRATED_TAG]))),
            ],
        ),
        Dashboard(id="3", tags=[]),
    ]
    workspace_client.dashboards.get.return_value = Dashboard(
        id="2",
        tags=[Redash.MIGRATED_TAG],
        widgets=[
            Widget(
                visualization=Visualization(
                    query=Query(
                        id="1",
                        name="test_query",
                        query="SELECT * FROM old.things",
                        tags=[Redash.MIGRATED_TAG, 'backup:123'],
                    )
                )
            )
        ],
    )

    return workspace_client


def test_migrate_all_dashboards(redash_ws, empty_index):
    redash = Redash(empty_index, redash_ws, "backup")
    redash.migrate_dashboards()
    redash_ws.queries.create.assert_called_with(
        name='test_query_original',
        query='SELECT * FROM old.things',
        data_source_id=None,
        description=None,
        options=QueryOptions(catalog="hive_metastore", schema="default"),
        parent="backup/backup_queries",
        run_as_role=None,
        tags=[Redash.BACKUP_TAG],
    )
    redash_ws.queries.update.assert_called_with(
        "1",
        query='SELECT * FROM old.things',
        tags=[Redash.MIGRATED_TAG, 'backup:123', 'test_tag'],
    )
    redash_ws.workspace.mkdirs.assert_called_once_with("backup/backup_queries")


def test_migrate_all_dashboards_error(redash_ws, empty_index, caplog):
    redash_ws.dashboards.list.side_effect = PermissionDenied("error")
    redash = Redash(empty_index, redash_ws, "backup")
    redash.migrate_dashboards()
    assert "Cannot list dashboards" in caplog.text


def test_revert_single_dashboard(redash_ws, empty_index, caplog):
    redash_ws.queries.get.return_value = Query(id="1", query="original_query")
    redash = Redash(empty_index, redash_ws, "")
    redash.revert_dashboards("2")
    redash_ws.queries.update.assert_called_with("1", query="original_query", tags=[])
    redash_ws.queries.delete.assert_called_once_with("123")
    redash_ws.queries.update.side_effect = PermissionDenied("error")
    redash.revert_dashboards("2")
    assert "Cannot restore" in caplog.text


def test_revert_dashboards(redash_ws, empty_index):
    redash_ws.queries.get.return_value = Query(id="1", query="original_query")
    redash = Redash(empty_index, redash_ws, "")
    redash.revert_dashboards()
    redash_ws.queries.update.assert_called_with("1", query="original_query", tags=[])
    redash_ws.queries.delete.assert_called_once_with("123")


def test_delete_backup_dashboards(redash_ws, empty_index):
    redash_ws.queries.list.return_value = [Query(id="1", tags=[Redash.BACKUP_TAG]), Query(id="2", tags=[]), Query()]
    redash = Redash(empty_index, redash_ws, "")
    mock_prompts = MockPrompts({"Are you sure you want to delete all backup queries*": "Yes"})
    redash.delete_backup_queries(mock_prompts)
    redash_ws.queries.delete.assert_called_once_with("1")


def test_delete_backup_dashboards_not_confirmed(redash_ws, empty_index):
    redash = Redash(empty_index, redash_ws, "")
    mock_prompts = MockPrompts({"Are you sure you want to delete all backup queries*": "No"})
    redash.delete_backup_queries(mock_prompts)
    redash_ws.queries.delete.assert_not_called()


def test_get_queries_from_dashboard(redash_ws):
    empty_dashboard = Dashboard(
        id="1",
    )
    assert len(list(Redash.get_queries_from_dashboard(empty_dashboard))) == 0
    dashboard = Dashboard(
        id="1",
        widgets=[
            Widget(),
            Widget(visualization=Visualization()),
            Widget(
                visualization=Visualization(
                    query=Query(
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
