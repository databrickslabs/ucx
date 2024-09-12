from unittest.mock import create_autospec

import pytest

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Query, ListQueryObjectsResponseQuery

from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawlers
from databricks.labs.ucx.source_code.queries import QueryLinter


@pytest.mark.parametrize(
    "name, query, dfsa_paths, is_read, is_write",
    [
        ("simple", "SELECT * from dual", [], False, False),
        (
            "location",
            "CREATE TABLE hive_metastore.indices_historical_data.sp_500 LOCATION 's3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/'",
            ["s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/"],
            False,
            True,
        ),
    ],
)
def test_workflow_linter_collects_dfsas_from_queries(
    name,
    query,
    dfsa_paths,
    is_read,
    is_write,
):
    ws = create_autospec(WorkspaceClient)
    crawlers = create_autospec(DirectFsAccessCrawlers)
    query = Query.from_dict({"parent_path": "workspace", "display_name": name, "query_text": query})
    response = ListQueryObjectsResponseQuery.from_dict(query.as_dict())
    ws.queries.list.return_value = iter([response])
    linter = QueryLinter(ws, crawlers)
    dfsas = linter.collect_dfsas_from_queries()
    crawlers.assert_not_called()
    assert set(dfsa.path for dfsa in dfsas) == set(dfsa_paths)
    assert not any(dfsa for dfsa in dfsas if dfsa.is_read != is_read)
    assert not any(dfsa for dfsa in dfsas if dfsa.is_write != is_write)
