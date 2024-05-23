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
from tests.unit import mock_table_mapping


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
    src = TableIdentifier("hive_metastore", "db1", "table1")
    src_2 = TableIdentifier("hive_metastore", "db2", "table2")
    src_3 = TableIdentifier("hive_metastore", "db3", "table3")
    tgt = TableIdentifier("catalog1", "schema1", "table1")
    tgt_2 = TableIdentifier("catalog2", "schema2", "table2")
    tgt_3 = TableIdentifier("catalog3", "schema3", "table3")
    errors = {}
    rows = {
        'SELECT \\* FROM inventory_database.migration_status': MIGRATION_STATUS[
            (src.schema, src.table, tgt.catalog, tgt.schema, tgt.table, "2021-01-01T00:00:00Z"),
            (src_2.schema, src_2.table, tgt_2.catalog, tgt_2.schema, tgt_2.table, "2021-01-01T00:00:00Z"),
            (src_3.schema, src_3.table, tgt_3.catalog, tgt_3.schema, tgt_3.table, "2021-01-01T00:00:00Z"),
            ("schema_none", "table_none", None, "schema_a", "table_a", "2021-01-01T00:00:00Z"),
        ],
        f"SHOW TBLPROPERTIES {src.schema}.{src.table} \\('upgraded_to'\\)": UPGRADED_TO[("value", "fake_dest"),],
        f"SHOW TBLPROPERTIES {src_2.schema}.{src_2.table} \\('upgraded_to'\\)": UPGRADED_TO[("value", "fake_dest"),],
        "DESCRIBE TABLE": metadata_row_factory[
            ("col1", "int"),
            ("col2", "string"),
        ],
        f"{tgt.catalog_escaped}\\.information_schema\\.columns": metadata_row_factory[
            ("col1", "int"),
            ("col2", "string"),
        ],
        f"{tgt_2.catalog_escaped}\\.information_schema\\.columns": metadata_row_factory[
            ("col1", "int"),
            ("col2", "string"),
        ],
        f"SELECT COUNT\\(\\*\\) as row_count FROM {src.fqn_escaped}": row_count_row_factory[100,],
        f"SELECT COUNT\\(\\*\\) as row_count FROM {tgt.fqn_escaped}": row_count_row_factory[2,],
        f"SELECT COUNT\\(\\*\\) as row_count FROM {src_2.fqn_escaped}": row_count_row_factory[0,],
        f"SELECT COUNT\\(\\*\\) as row_count FROM {tgt_2.fqn_escaped}": row_count_row_factory[0,],
        "WITH compare_results": data_comp_row_factory[(102, 100, 2),],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    metadata_retriever = DatabricksTableMetadataRetriever(backend)
    data_profiler = StandardDataProfiler(backend, metadata_retriever)
    migration_recon = MigrationRecon(
        backend,
        "inventory_database",
        migration_status_refresher,
        mock_table_mapping(["managed_dbfs", "managed_mnt", "managed_other"]),
        StandardSchemaComparator(metadata_retriever),
        StandardDataComparator(backend, data_profiler),
        0,
    )
    results = list(migration_recon.snapshot())
    assert len(results) == 2
