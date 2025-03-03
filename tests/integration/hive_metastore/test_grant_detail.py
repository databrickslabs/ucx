import json
import logging
import datetime as dt
from collections.abc import Callable, Iterable

import pytest
from databricks.labs.lsql.backends import CommandExecutionBackend, SqlBackend
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.hive_metastore.grants import Grant, GrantsCrawler
from databricks.labs.ucx.install import deploy_schema

from ..conftest import MockRuntimeContext


logger = logging.getLogger(__name__)


@pytest.fixture
def sql_backend_tacl(ws, env_or_skip) -> SqlBackend:
    """Ensure the SQL backend used during fixture setup is using the TACL cluster.

    The TACL cluster is used for grants.
    """
    cluster_id = env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID")
    return CommandExecutionBackend(ws, cluster_id)


@pytest.fixture()
def _deployed_schema(runtime_ctx) -> None:
    """Ensure that the schemas (and views) are initialized."""
    # Takes ~11 seconds.
    deploy_schema(runtime_ctx.sql_backend, runtime_ctx.inventory_database)


@retried(on=[NotFound, TimeoutError], timeout=dt.timedelta(minutes=3))
def test_all_grant_types(runtime_ctx: MockRuntimeContext, sql_backend_tacl: SqlBackend, _deployed_schema: None):
    """All types of grants should be reported by the grant_detail view."""

    # Fixture: a group and schema to hold all the objects, the objects themselves and a grant on each to the group.
    group = runtime_ctx.make_group()
    schema = runtime_ctx.make_schema()
    table = runtime_ctx.make_table(schema_name=schema.name)
    view = runtime_ctx.make_table(schema_name=schema.name, view=True, ctas="select 1")
    udf = runtime_ctx.make_udf(schema_name=schema.name)
    runtime_ctx.sql_backend.execute(f"GRANT SELECT ON CATALOG {schema.catalog_name} TO `{group.display_name}`")
    runtime_ctx.sql_backend.execute(f"GRANT SELECT ON SCHEMA {schema.full_name} TO `{group.display_name}`")
    runtime_ctx.sql_backend.execute(f"GRANT SELECT ON TABLE {table.full_name} TO `{group.display_name}`")
    runtime_ctx.sql_backend.execute(f"GRANT SELECT ON VIEW {view.full_name} TO `{group.display_name}`")
    runtime_ctx.sql_backend.execute(f"GRANT SELECT ON FUNCTION {udf.full_name} TO `{group.display_name}`")
    runtime_ctx.sql_backend.execute(f"GRANT SELECT ON ANY FILE TO `{group.display_name}`")
    runtime_ctx.sql_backend.execute(f"GRANT SELECT ON ANONYMOUS FUNCTION TO `{group.display_name}`")

    @retried(on=[ValueError], timeout=dt.timedelta(minutes=2))
    def wait_for_grants(condition: Callable[[Iterable[Grant]], bool], **kwargs) -> None:
        """Wait for grants to meet the condition.

        The method retries the condition check to account for eventual consistency of the permission API.
        """
        grants = runtime_ctx.grants_crawler.grants(**kwargs)
        if not condition(grants):
            raise ValueError("Grants do not meet condition")

    def grants_contain_select_action(grants: Iterable[Grant]) -> bool:
        """Check if the SELECT permission on ANY FILE is present in the grants."""
        return any(g.principal == group.display_name and g.action_type == "SELECT" for g in grants)

    # Wait for the grants to be available so that we can snapshot them.
    # Only verifying the SELECT permission on ANY FILE and ANONYMOUS FUNCTION as those take a while to propagate.
    wait_for_grants(grants_contain_select_action, any_file=True)
    wait_for_grants(grants_contain_select_action, anonymous_function=True)

    # Snapshotting tables and udfs to avoid snapshot on TACL cluster during grants crawler
    runtime_ctx.tables_crawler.snapshot()
    runtime_ctx.udfs_crawler.snapshot()

    # Fetching the grants requires a table access control list enabled cluster
    runtime_ctx.replace(sql_backend=sql_backend_tacl).grants_crawler.snapshot()

    grants_detail_query = f"""
        SELECT object_type, object_id
        FROM {runtime_ctx.inventory_database}.grant_detail
        WHERE principal_type='group' AND principal='{group.display_name}' and action_type='SELECT'
    """
    grants = {(row.object_type, row.object_id) for row in runtime_ctx.sql_backend.fetch(grants_detail_query)}

    # TODO: The types of objects targeted by grants is missclassified; this needs to be fixed.

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
def test_grant_findings(runtime_ctx: MockRuntimeContext, _deployed_schema: None) -> None:
    """Test that findings are reported for a grant."""

    # Fixture: two objects, one with a grant that is okay and the other with a grant that is not okay.
    group = runtime_ctx.make_group()
    schema = runtime_ctx.make_schema()
    table_a = runtime_ctx.make_table(schema_name=schema.name)
    table_b = runtime_ctx.make_table(schema_name=schema.name)
    runtime_ctx.sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{group.display_name}`")
    runtime_ctx.sql_backend.execute(f"DENY SELECT ON TABLE {table_b.full_name} TO `{group.display_name}`")

    # Ensure the view is populated (it's based on the crawled grants) and fetch the content.
    GrantsCrawler(runtime_ctx.tables_crawler, runtime_ctx.udfs_crawler).snapshot()

    rows = runtime_ctx.sql_backend.fetch(
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
