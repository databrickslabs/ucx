import logging
from unittest.mock import create_autospec
import pytest
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.hive_metastore.grants import Grant, PrincipalACL, MigrateGrants
from databricks.labs.ucx.hive_metastore.table_migrate import (
    ACLMigrator,
)
from databricks.labs.ucx.hive_metastore.migration_status import (
    MigrationStatusRefresher,
    MigrationIndex,
)
from databricks.labs.ucx.hive_metastore.tables import (
    TablesCrawler,
)

from .. import GROUPS
from ..workspace_access.test_tacl import UCX_TABLES

logger = logging.getLogger(__name__)


@pytest.fixture
def ws():
    client = create_autospec(WorkspaceClient)
    client.get_workspace_id.return_value = "12345"
    return client


@pytest.fixture
def ws_info():
    info = create_autospec(WorkspaceInfo)
    info.current.return_value = "hms_fed"
    return info


GRANTS = MockBackend.rows("principal", "action_type", "catalog", "database", "table", "view")

expected_statements = [
    "GRANT SELECT ON TABLE ucx_default.db1_dst.managed_dbfs TO `account group`",
    "GRANT MODIFY ON TABLE ucx_default.db1_dst.managed_mnt TO `account group`",
    "ALTER TABLE ucx_default.db1_dst.managed_other OWNER TO `account group`",
    "GRANT SELECT ON VIEW ucx_default.db1_dst.view_dst TO `account group`",
]

unexpected_statements = [
    "GRANT MODIFY ON TABLE ucx_default.db1_dst.managed_dbfs TO `account group`",
    "ALTER TABLE ucx_default.db1_dst.managed_dbfs OWNER TO `account group`",
    "GRANT SELECT ON TABLE ucx_default.db1_dst.managed_mnt TO `account group`",
    "GRANT SELECT ON TABLE ucx_default.db1_dst.managed_other TO `account group`",
    "GRANT MODIFY ON TABLE ucx_default.db1_dst.managed_other TO `account group`",
    "GRANT MODIFY ON VIEW ucx_default.db1_dst.view_dst TO `account group`",
]


def assert_grant_statements(backend_queries, check_in, check_not_in):
    for statement in check_in:
        assert statement in backend_queries
    for statement in check_not_in:
        assert statement not in backend_queries


test_produce_proper_queries_rows = {
    'SELECT \\* FROM hive_metastore.inventory_database.grants': GRANTS[
        ("workspace_group", "SELECT", "", "db1_src", "managed_dbfs", ""),
        ("workspace_group", "MODIFY", "", "db1_src", "managed_mnt", ""),
        ("workspace_group", "OWN", "", "db1_src", "managed_other", ""),
        ("workspace_group", "INVALID", "", "db1_src", "managed_other", ""),
        ("workspace_group", "SELECT", "", "db1_src", "view_src", ""),
        ("workspace_group", "SELECT", "", "db1_random", "view_src", ""),
    ],
    r"SYNC .*": MockBackend.rows("status_code", "description")[("SUCCESS", "test")],
    'SELECT \\* FROM hive_metastore.inventory_database.groups': GROUPS[
        ("11", "workspace_group", "account group", "temp", "", "", "", ""),
    ],
    "SHOW CREATE TABLE": [
        {
            "createtab_stmt": "CREATE OR REPLACE VIEW "
            "hive_metastore.db1_src.view_src AS SELECT * FROM db1_src.managed_dbfs"
        }
    ],
    'SELECT \\* FROM hive_metastore.inventory_database.tables': UCX_TABLES[
        ("hive_metastore", "db1_src", "managed_dbfs", "table", "DELTA", "/foo/bar/test", None),
        ("hive_metastore", "db1_src", "managed_mnt", "table", "DELTA", "/foo/bar/test", None),
        ("hive_metastore", "db1_src", "managed_other", "table", "DELTA", "/foo/bar/test", None),
        ("hive_metastore", "db1_src", "view_src", "table", "DELTA", "/foo/bar/test", "select * from foo.bar"),
    ],
}


def test_migrate_acls_should_produce_proper_queries(ws, ws_info, caplog):
    # all grants succeed except for one
    errors = {"GRANT SELECT ON VIEW ucx_default.db1_dst.view_dst TO `account group`": "TABLE_OR_VIEW_NOT_FOUND: error"}
    rows = test_produce_proper_queries_rows
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    workspace_info = ws_info
    migration_status_refresher = create_autospec(MigrationStatusRefresher)

    migrate_grants = create_autospec(MigrateGrants)
    acl_migrate = ACLMigrator(
        table_crawler,
        workspace_info,
        backend,
        migration_status_refresher,
        migrate_grants,
    )
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs",
        "ucx_default.db1_dst.managed_mnt": "hive_metastore.db1_src.managed_mnt",
        "ucx_default.db1_dst.managed_other": "hive_metastore.db1_src.managed_other",
        "ucx_default.db1_dst.view_dst": "hive_metastore.db1_src.view_src",
    }
    acl_migrate.migrate_acls()
    migrate_grants.apply.assert_called()

    assert_grant_statements(backend.queries, expected_statements, unexpected_statements)

    assert "Cannot identify UC grant" in caplog.text


def test_migrate_principal_acls_should_produce_proper_queries(ws, ws_info):
    errors = {}
    rows = {
        'SELECT \\* FROM hive_metastore.inventory_database.tables': UCX_TABLES[
            ("hive_metastore", "db1_src", "managed_dbfs", "table", "DELTA", "/foo/bar/test", None),
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    migration_index = create_autospec(MigrationIndex)
    migration_status_refresher = create_autospec(MigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs",
    }
    migration_index.is_migrated.return_value = True
    migration_status_refresher.index.return_value = migration_index
    principal_grants = create_autospec(PrincipalACL)
    expected_grants = [
        Grant('spn1', "ALL PRIVILEGES", "hive_metastore", 'db1_src', 'managed_dbfs'),
        Grant('spn1', "USE", "hive_metastore", 'db1_src'),
        Grant('spn1', "USE", "hive_metastore"),
    ]
    principal_grants.get_interactive_cluster_grants.return_value = expected_grants
    migrate_grants = create_autospec(MigrateGrants)
    acl_migrate = ACLMigrator(
        table_crawler,
        ws_info,
        backend,
        migration_status_refresher,
        migrate_grants,
    )
    acl_migrate.migrate_acls()

    migrate_grants.apply.assert_called()

    assert "GRANT ALL PRIVILEGES ON TABLE ucx_default.db1_dst.managed_dbfs TO `spn1`" in backend.queries


def test_migrate_acls_hms_fed_proper_queries(ws, ws_info, caplog):
    # all grants succeed except for one
    errors = {}
    rows = {
        'SELECT \\* FROM hive_metastore.inventory_database.grants': GRANTS[
            ("workspace_group", "SELECT", "", "db1_src", "managed_dbfs", ""),
        ],
        'SELECT \\* FROM hive_metastore.inventory_database.groups': GROUPS[
            ("11", "workspace_group", "account group", "temp", "", "", "", ""),
        ],
        'SELECT \\* FROM hive_metastore.inventory_database.tables': UCX_TABLES[
            ("hive_metastore", "db1_src", "managed_dbfs", "table", "DELTA", "/foo/bar/test", None),
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    workspace_info = ws_info
    migration_status_refresher = create_autospec(MigrationStatusRefresher)
    migrate_grants = create_autospec(MigrateGrants)

    acl_migrate = ACLMigrator(
        table_crawler,
        workspace_info,
        backend,
        migration_status_refresher,
        migrate_grants,
    )
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs",
    }
    acl_migrate.migrate_acls(hms_fed=True)
    migration_index = create_autospec(MigrationIndex)
    migration_index.is_migrated.return_value = True
    migration_status_refresher.index.return_value = migration_index

    migrate_grants.apply.assert_called()

    assert "GRANT SELECT ON TABLE hms_fed.db1_src.managed_dbfs TO `account group`" in backend.queries
    assert "GRANT MODIFY ON TABLE hms_fed.db1_src.managed_dbfs TO `account group`" not in backend.queries
