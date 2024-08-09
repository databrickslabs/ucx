import logging

import pytest


from databricks.labs.ucx.mixins.fixtures import get_test_purge_time, factory
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.dfsa import DfsaCrawler, DfsaCollector

logger = logging.getLogger("__name__")


@pytest.fixture
def make_query(ws, make_random):
    def create(name: str, sql: str, **kwargs):
        # add RemoveAfter tag for test job cleanup
        date_to_remove = get_test_purge_time()
        tags: list[str] = kwargs["tags"] if 'tags' in kwargs else []
        tags.append(str({"key": "RemoveAfter", "value": date_to_remove}))
        query = ws.queries.create(name=name, query=sql, tags=tags)
        logger.info(f"Query: {ws.config.host}#query/{query.id}")
        return query

    yield from factory("query", create, lambda query: ws.queries.delete(query.id))


@pytest.mark.parametrize(
    "name, sql, dfsa_paths, is_read, is_write",
    [
        (
            "create_location",
            "CREATE TABLE hive_metastore.indices_historical_data.sp_500 LOCATION 's3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/'",
            ['s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/'],
            False,
            True,
        )
    ],
)
def test_dfsa_collector_collects_dfsas_from_query(  # pylint: disable=too-many-arguments
    name, sql, dfsa_paths, is_read, is_write, ws, sql_backend, simple_ctx, make_query, make_schema
):
    query = make_query(name=name, sql=sql)
    schema = make_schema(catalog_name="hive_metastore")
    crawler = DfsaCrawler(sql_backend, schema.name)
    collector = DfsaCollector(crawler, simple_ctx.path_lookup, CurrentSessionState())
    _ = list(collector.collect_from_workspace_queries(ws))
    for dfsa in crawler.snapshot():
        assert dfsa.path in set(dfsa_paths)
        assert dfsa.source_type == "QUERY"
        assert dfsa.source_id.endswith(query.name)
        assert dfsa.is_read == is_read
        assert dfsa.is_write == is_write
