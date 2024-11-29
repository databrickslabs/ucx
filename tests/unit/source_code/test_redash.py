import logging
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied, NotFound
from databricks.sdk.service.sql import LegacyQuery, QueryOptions, UpdateQueryRequestQuery

from databricks.labs.ucx.assessment.dashboards import RedashDashboard, RedashDashboardCrawler
from databricks.labs.ucx.source_code.redash import Redash


def get_query(query_id: str) -> LegacyQuery:
    queries = [
        LegacyQuery(
            id="1",
            name="test_query",
            query="SELECT * FROM old.things",
            options=QueryOptions(catalog="hive_metastore", schema="default"),
            tags=["test_tag"],
        ),
        LegacyQuery(
            id="2",
            name="test_query",
            query="SELECT * FROM old.things",
            options=QueryOptions(catalog="hive_metastore", schema="default"),
            tags=["test_tag"],
        ),
        LegacyQuery(
            id="3",
            name="test_query",
            query="SELECT * FROM old.things",
            options=QueryOptions(catalog="hive_metastore", schema="default"),
            tags=["test_tag", Redash.MIGRATED_TAG],
        ),
    ]
    for query in queries:
        if query.id == query_id:
            return query
    raise NotFound(f"Query not found: {query_id}")


@pytest.fixture
def redash_ws():
    workspace_client = create_autospec(WorkspaceClient)
    workspace_client.queries_legacy.get.side_effect = get_query
    return workspace_client


@pytest.fixture
def redash_installation():
    installation = MockInstallation(
        {
            "backup/queries/1.json": {"id": "1", "query": "SELECT * FROM old.things"},
            "backup/queries/3.json": {"id": "3", "query": "SELECT * FROM old.things", "tags": ["test_tag"]},
        }
    )
    return installation


@pytest.fixture
def redash_dashboard_crawler():
    crawler = create_autospec(RedashDashboardCrawler)
    crawler.snapshot.return_value = [
        RedashDashboard(id="1", query_ids=["1"]),
        RedashDashboard(id="2", query_ids=["1", "2", "3"], tags=[Redash.MIGRATED_TAG]),
        RedashDashboard(id="3", tags=[]),
    ]
    return crawler


def test_migrate_all_dashboards(redash_ws, empty_index, redash_installation, redash_dashboard_crawler) -> None:
    redash = Redash(empty_index, redash_ws, redash_installation, redash_dashboard_crawler)

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
    query = UpdateQueryRequestQuery(
        query_text="SELECT * FROM old.things",
        tags=[Redash.MIGRATED_TAG, 'test_tag'],
    )
    redash_ws.queries.update.assert_called_with(
        "1",
        update_mask="query_text,tags",
        query=query,
    )
    redash_dashboard_crawler.snapshot.assert_called_once()


def test_revert_single_dashboard(caplog, redash_ws, empty_index, redash_installation, redash_dashboard_crawler) -> None:
    redash_ws.queries.get.return_value = LegacyQuery(id="1", query="original_query")
    redash = Redash(empty_index, redash_ws, redash_installation, redash_dashboard_crawler)

    redash.revert_dashboards("2")

    query = UpdateQueryRequestQuery(query_text="SELECT * FROM old.things", tags=["test_tag"])
    redash_ws.queries.update.assert_called_with("3", update_mask="query_text,tags", query=query)
    redash_ws.queries.update.side_effect = PermissionDenied("error")
    redash_dashboard_crawler.snapshot.assert_called_once()


def test_revert_dashboards(redash_ws, empty_index, redash_installation, redash_dashboard_crawler) -> None:
    redash_ws.queries.get.return_value = LegacyQuery(id="1", query="original_query")
    redash = Redash(empty_index, redash_ws, redash_installation, redash_dashboard_crawler)

    redash.revert_dashboards()

    query = UpdateQueryRequestQuery(query_text="SELECT * FROM old.things", tags=["test_tag"])
    redash_ws.queries.update.assert_called_with("3", update_mask="query_text,tags", query=query)
    redash_dashboard_crawler.snapshot.assert_called_once()


def test_get_queries_from_empty_dashboard(
    redash_ws, empty_index, redash_installation, redash_dashboard_crawler
) -> None:
    redash = Redash(empty_index, redash_ws, redash_installation, redash_dashboard_crawler)
    empty_dashboard = RedashDashboard(id="1")

    queries = list(redash._get_queries_from_dashboard(empty_dashboard))

    assert len(queries) == 0
    redash_dashboard_crawler.snapshot.assert_not_called()


def test_get_queries_from_dashboard_with_query(
    redash_ws, empty_index, redash_installation, redash_dashboard_crawler
) -> None:
    redash = Redash(empty_index, redash_ws, redash_installation, redash_dashboard_crawler)
    dashboard = RedashDashboard(id="1", query_ids=["1"])

    queries = list(redash._get_queries_from_dashboard(dashboard))

    assert len(queries) == 1
    assert queries[0].id == "1"
    redash_dashboard_crawler.snapshot.assert_not_called()


def test_get_queries_from_dashboard_with_non_existing_query(
    caplog, redash_ws, empty_index, redash_installation, redash_dashboard_crawler
) -> None:
    redash = Redash(empty_index, redash_ws, redash_installation, redash_dashboard_crawler)
    dashboard = RedashDashboard(id="1", query_ids=["-1"])

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.aggregate"):
        queries = list(redash._get_queries_from_dashboard(dashboard))

    assert len(queries) == 0
    assert "Cannot get query: -1" in caplog.messages
    redash_dashboard_crawler.snapshot.assert_not_called()
