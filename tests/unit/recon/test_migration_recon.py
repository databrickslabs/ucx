import logging
from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.migration_status import MigrationStatusRefresher
from databricks.labs.ucx.recon.base import TableIdentifier
from databricks.labs.ucx.recon.data_comparator import StandardDataComparator
from databricks.labs.ucx.recon.data_profiler import StandardDataProfiler
from databricks.labs.ucx.recon.metadata_retriever import DatabricksTableMetadataRetriever
from databricks.labs.ucx.recon.migration_recon import MigrationRecon
from databricks.labs.ucx.recon.schema_comparator import StandardSchemaComparator

logger = logging.getLogger(__name__)


@pytest.fixture
def ws():
    client = create_autospec(WorkspaceClient)
    client.get_workspace_id.return_value = "12345"
    return client


MIGRATION_STATUS = MockBackend.rows(
    "src_schema",
    "src_table",
    "dst_catalog",
    "dst_schema",
    "dst_table",
    "update_ts",
)
UPGRADED_TO = MockBackend.rows("key", "value")


def test_migrate_recon_should_produce_proper_queries(
    ws,
    metadata_row_factory,
    row_count_row_factory,
    data_comp_row_factory,
):
    source = TableIdentifier("hive_metastore", "db1", "table1")
    target = TableIdentifier("catalog1", "schema1", "table1")
    errors = {}
    rows = {
        'SELECT \\* FROM inventory_database.migration_status': MIGRATION_STATUS[
            (source.schema, source.table, target.catalog, target.schema, target.table, "2021-01-01T00:00:00Z"),
            ("schema_none", "table_none", None, "schema_a", "table_a", "2021-01-01T00:00:00Z"),
        ],
        f"SHOW TBLPROPERTIES {source.schema}.{source.table} \\('upgraded_to'\\)": UPGRADED_TO[("value", "fake_dest"),],
        "DESCRIBE TABLE": metadata_row_factory[
            ("col1", "int"),
            ("col2", "string"),
        ],
        f"{target.catalog_escaped}\\.information_schema\\.columns": metadata_row_factory[
            ("col1", "int"),
            ("col2", "string"),
        ],
        f"SELECT COUNT\\(\\*\\) as row_count FROM {source.fqn_escaped}": row_count_row_factory[100,],
        f"SELECT COUNT\\(\\*\\) as row_count FROM {target.fqn_escaped}": row_count_row_factory[2,],
        "WITH compare_results": data_comp_row_factory[(102, 100, 2),],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    metadata_retriever = DatabricksTableMetadataRetriever(backend)
    schema_comparator = StandardSchemaComparator(metadata_retriever)
    data_profiler = StandardDataProfiler(backend, metadata_retriever)
    data_comparator = StandardDataComparator(backend, data_profiler)
    migration_recon = MigrationRecon(
        backend,
        "inventory_database",
        migration_status_refresher,
        schema_comparator,
        data_comparator,
    )
    results = list(migration_recon.snapshot())
    assert len(results) == 1
