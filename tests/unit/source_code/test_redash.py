from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.sql import LegacyQuery, QueryOptions, UpdateQueryRequestQuery

from databricks.labs.ucx.assessment.dashboards import Dashboard, RedashDashboardCrawler
from databricks.labs.ucx.source_code.redash import Redash


@pytest.fixture
def redash_installation():
    installation = MockInstallation(
        {
            "backup/queries/1.json": {"id": "1", "query": "SELECT * FROM old.things"},
            "backup/queries/3.json": {"id": "3", "query": "SELECT * FROM old.things", "tags": ["test_tag"]},
        }
    )
    return installation


def list_legacy_queries(dashboard: Dashboard) -> list[LegacyQuery]:
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
    query_mapping = {query.id: query for query in queries}
    queries_matched = []
    for query_id in dashboard.query_ids:
        query = query_mapping.get(query_id)
        if query:
            queries_matched.append(query)
    return queries_matched


@pytest.fixture
def redash_dashboard_crawler():
    crawler = create_autospec(RedashDashboardCrawler)
    crawler.snapshot.return_value = [
        Dashboard(id="1", query_ids=["1"]),
        Dashboard(id="2", query_ids=["1", "2", "3"], tags=[Redash.MIGRATED_TAG]),
        Dashboard(id="3", tags=[]),
    ]
    crawler.list_legacy_queries.side_effect = list_legacy_queries
    return crawler


def test_migrate_all_dashboards(ws, empty_index, redash_installation, redash_dashboard_crawler) -> None:
    redash = Redash(empty_index, ws, redash_installation, redash_dashboard_crawler)

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
    ws.queries.update.assert_called_with(
        "1",
        update_mask="query_text,tags",
        query=query,
    )
    redash_dashboard_crawler.snapshot.assert_called_once()


def test_revert_single_dashboard(caplog, ws, empty_index, redash_installation, redash_dashboard_crawler) -> None:
    ws.queries.get.return_value = LegacyQuery(id="1", query="original_query")
    redash = Redash(empty_index, ws, redash_installation, redash_dashboard_crawler)

    redash.revert_dashboards("2")

    query = UpdateQueryRequestQuery(query_text="SELECT * FROM old.things", tags=["test_tag"])
    ws.queries.update.assert_called_with("3", update_mask="query_text,tags", query=query)
    ws.queries.update.side_effect = PermissionDenied("error")
    redash_dashboard_crawler.snapshot.assert_called_once()


def test_revert_dashboards(ws, empty_index, redash_installation, redash_dashboard_crawler) -> None:
    ws.queries.get.return_value = LegacyQuery(id="1", query="original_query")
    redash = Redash(empty_index, ws, redash_installation, redash_dashboard_crawler)

    redash.revert_dashboards()

    query = UpdateQueryRequestQuery(query_text="SELECT * FROM old.things", tags=["test_tag"])
    ws.queries.update.assert_called_with("3", update_mask="query_text,tags", query=query)
    redash_dashboard_crawler.snapshot.assert_called_once()


def test_migrate_dashboard_gets_no_queries_when_dashboard_is_empty(
    ws, empty_index, redash_installation, redash_dashboard_crawler
) -> None:
    empty_dashboard = Dashboard(id="1")
    redash_dashboard_crawler.snapshot.return_value = [empty_dashboard]
    redash = Redash(empty_index, ws, redash_installation, redash_dashboard_crawler)

    redash.migrate_dashboards()

    ws.queries_legacy.get.assert_not_called()
    redash_dashboard_crawler.snapshot.assert_called_once()


def test_migrate_dashboard_lists_legacy_queries_from_dashboard(
    ws, empty_index, redash_installation, redash_dashboard_crawler
) -> None:
    dashboard = Dashboard(id="1", query_ids=["1"])
    redash_dashboard_crawler.snapshot.return_value = [dashboard]
    redash = Redash(empty_index, ws, redash_installation, redash_dashboard_crawler)

    redash.migrate_dashboards()

    redash_dashboard_crawler.list_legacy_queries.assert_called_with(dashboard)
    redash_dashboard_crawler.snapshot.assert_called_once()
