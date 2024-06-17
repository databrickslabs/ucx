import json
import logging
import datetime as dt

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler
from databricks.labs.ucx.install import deploy_schema

from ..conftest import TestRuntimeContext

logger = logging.getLogger(__name__)


@pytest.fixture()
def _deployed_schema(runtime_ctx) -> None:
    """Ensure that the schemas (and views) are initialized."""
    # Takes ~11 seconds.
    deploy_schema(runtime_ctx.sql_backend, runtime_ctx.inventory_database)


@retried(on=[NotFound, TimeoutError], timeout=dt.timedelta(minutes=3))
def test_all_grant_types(
    runtime_ctx: TestRuntimeContext, sql_backend: StatementExecutionBackend, _deployed_schema: None
):
    """Test that all types of grants are properly handled by the view when reporting the object type and identifier."""

    # Fixture: a group and schema to hold all the objects, the objects themselves and a grant on each to the group.
    group = runtime_ctx.make_group()
    schema = runtime_ctx.make_schema()
    table = runtime_ctx.make_table(schema_name=schema.name)
    view = runtime_ctx.make_table(schema_name=schema.name, view=True, ctas="select 1")
    udf = runtime_ctx.make_udf(schema_name=schema.name)
    sql_backend.execute(f"GRANT SELECT ON CATALOG {schema.catalog_name} TO `{group.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON SCHEMA {schema.full_name} TO `{group.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table.full_name} TO `{group.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON VIEW {view.full_name} TO `{group.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON FUNCTION {udf.full_name} TO `{group.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON ANY FILE TO `{group.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON ANONYMOUS FUNCTION TO `{group.display_name}`")

    # Ensure the view is populated (it's based on the crawled grants) and fetch the content.
    GrantsCrawler(runtime_ctx.tables_crawler, runtime_ctx.udfs_crawler).snapshot()

    rows = list(
        sql_backend.fetch(
            f"""
        SELECT object_type, object_id
        FROM {runtime_ctx.inventory_database}.grant_detail
        WHERE principal_type='group' AND principal='{group.display_name}' and action_type='SELECT'
    """
        )
    )
    grants = {(row.object_type, row.object_id) for row in rows}

    # TODO: The types of objects targeted by grants is missclassified; this needs to be fixed.

    # Test the results.
    expected_grants = {
        ("TABLE", table.full_name),
        ("VIEW", view.full_name),
        ("DATABASE", schema.full_name),
        ("CATALOG", schema.catalog_name),
        ("UDF", udf.full_name),
        ("ANY FILE", None),
        ("ANONYMOUS FUNCTION", None),
    }
    assert grants == expected_grants


@retried(on=[NotFound, TimeoutError], timeout=dt.timedelta(minutes=3))
def test_grant_findings(
    runtime_ctx: TestRuntimeContext, sql_backend: StatementExecutionBackend, _deployed_schema: None
) -> None:
    """Test that findings are reported for a grant."""

    # Fixture: two objects, one with a grant that is okay and the other with a grant that is not okay.
    group = runtime_ctx.make_group()
    schema = runtime_ctx.make_schema()
    table_a = runtime_ctx.make_table(schema_name=schema.name)
    table_b = runtime_ctx.make_table(schema_name=schema.name)
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{group.display_name}`")
    sql_backend.execute(f"DENY SELECT ON TABLE {table_b.full_name} TO `{group.display_name}`")

    # Ensure the view is populated (it's based on the crawled grants) and fetch the content.
    GrantsCrawler(runtime_ctx.tables_crawler, runtime_ctx.udfs_crawler).snapshot()

    rows = sql_backend.fetch(
        f"""
        SELECT object_type, object_id, success, failures
        FROM {runtime_ctx.inventory_database}.grant_detail
        WHERE catalog='{schema.catalog_name}' AND database='{schema.name}'
          AND principal_type='group' AND principal='{group.display_name}'
        """
    )
    grants = {
        (row.object_type, row.object_id): (row.success, json.loads(row.failures) if row.failures is not None else None)
        for row in rows
    }

    # Check the findings on our objects.
    expected_grants = {
        ("TABLE", table_a.full_name): (1, []),
        ("TABLE", table_b.full_name): (0, ["Explicitly DENYing privileges is not supported in UC."]),
    }
    assert grants == expected_grants
