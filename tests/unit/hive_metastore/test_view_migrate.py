import itertools
import json
import logging
from pathlib import Path
from typing import TypeVar

import pytest

from databricks.labs.ucx.hive_metastore.mapping import Rule, TableToMigrate
from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex, MigrationStatus
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.hive_metastore.view_migrate import ViewsMigrationSequencer, ViewToMigrate


def test_view_to_migrate_sql_migrate_view_sql():
    expected_query = "CREATE OR REPLACE VIEW IF NOT EXISTS `cat1`.`schema1`.`dest_view1` AS SELECT * FROM `cat1`.`schema1`.`dest_table1`"
    view = Table(
        object_type="VIEW",
        table_format="VIEW",
        catalog="hive_metastore",
        database="test_schema1",
        name="test_view1",
        # The view text is overwritten with the create view statement before running the sql migrate view
        view_text="CREATE OR REPLACE VIEW hive_metastore.test_schema1.test_view1 AS SELECT * FROM test_schema1.test_table1",
    )
    rule = Rule("workspace", "cat1", "test_schema1", "schema1", "test_view1", "dest_view1")
    view_to_migrate = ViewToMigrate(view, rule)
    migration_index = MigrationIndex(
        [
            MigrationStatus("test_schema1", "test_table1", "cat1", "schema1", "dest_table1"),
            MigrationStatus("test_schema1", "test_view1"),
        ]
    )

    sql = view_to_migrate.sql_migrate_view(migration_index)

    assert sql == expected_query


@pytest.fixture(scope="session")
def samples() -> dict[str, dict[str, str]]:
    path = Path(Path(__file__).parent, "tables", "tables_and_views.json")
    samples_with_key = {}
    with path.open(encoding="utf-8") as f:
        for sample in json.load(f):
            key = sample["db"] + "." + sample["table"]
            samples_with_key[key] = sample
    return samples_with_key


@pytest.fixture
def tables(request, samples) -> list[TableToMigrate]:
    tables_to_migrate = []
    rule = Rule("ws1", "cat1", "schema", "db1", "table1", "table2")
    for key in request.param:
        sample = samples[key]
        table = Table(
            "hive_metastore",
            sample["db"],
            sample["table"],
            "type",
            "DELTA" if sample.get("view_text") is None else "VIEW",
            view_text=sample.get("view_text"),
        )
        table_to_migrate = TableToMigrate(table, rule)
        tables_to_migrate.append(table_to_migrate)
    return tables_to_migrate


@pytest.mark.parametrize("tables", [("db1.t1", "db2.t1")], indirect=True)
def test_empty_sequence_without_views(tables):
    migration_index = MigrationIndex(
        [
            MigrationStatus("db1", "t1", "cat1", "db2", "t1"),
            MigrationStatus("db2", "t2", "cat1", "db2", "t1"),
        ]
    )
    sequencer = ViewsMigrationSequencer(tables, migration_index=migration_index)
    batches = sequencer.sequence_batches()

    assert len(batches) == 0


T = TypeVar("T")


def flatten(lists: list[list[T]]) -> list[T]:
    return list(itertools.chain.from_iterable(lists))


@pytest.mark.parametrize("tables", [("db1.t1", "db1.v1")], indirect=True)
def test_sequence_direct_view(tables) -> None:
    expected = ["hive_metastore.db1.v1"]
    migration_index = MigrationIndex([MigrationStatus("db1", "t1", "cat1", "db1", "t1")])
    sequencer = ViewsMigrationSequencer(tables, migration_index=migration_index)

    batches = sequencer.sequence_batches()

    assert [t.src.key for t in flatten(batches)] == expected


@pytest.mark.parametrize("tables", [("db1.t1", "db1.v1", "db1.t2", "db1.v2")], indirect=True)
def test_sequence_direct_views(tables) -> None:
    expected = ["hive_metastore.db1.v1", "hive_metastore.db1.v2"]
    migration_index = MigrationIndex(
        [MigrationStatus("db1", "t1", "cat1", "db1", "t1"), MigrationStatus("db1", "t2", "cat1", "db1", "t2")]
    )
    sequencer = ViewsMigrationSequencer(tables, migration_index=migration_index)

    batches = sequencer.sequence_batches()

    # Sort because the order of the views is not guaranteed as they both depend on just tables
    assert sorted([t.src.key for t in flatten(batches)]) == expected


@pytest.mark.parametrize("tables", [("db1.t1", "db1.v1", "db1.v4")], indirect=True)
def test_sequence_indirect_views(tables) -> None:
    expected = ["hive_metastore.db1.v1", "hive_metastore.db1.v4"]
    migration_index = MigrationIndex([MigrationStatus("db1", "t1", "cat1", "db1", "t1")])
    sequencer = ViewsMigrationSequencer(tables, migration_index=migration_index)

    batches = sequencer.sequence_batches()

    assert [t.src.key for t in flatten(batches)] == expected


@pytest.mark.parametrize("tables", [("db1.t1", "db1.v1", "db1.v4", "db1.v5", "db1.v6", "db1.v7")], indirect=True)
def test_sequence_deep_indirect_views(tables) -> None:
    expected = [
        "hive_metastore.db1.v1",
        "hive_metastore.db1.v4",
        "hive_metastore.db1.v7",
        "hive_metastore.db1.v6",
        "hive_metastore.db1.v5",
    ]
    migration_index = MigrationIndex([MigrationStatus("db1", "t1", "cat1", "db1", "t1")])
    sequencer = ViewsMigrationSequencer(tables, migration_index=migration_index)

    batches = sequencer.sequence_batches()

    assert [t.src.key for t in flatten(batches)] == expected


@pytest.mark.parametrize("tables", [("db1.v1", "db1.v15")], indirect=True)
def test_sequence_view_with_view_and_table_dependency(tables) -> None:
    expected = ["hive_metastore.db1.v1", "hive_metastore.db1.v15"]
    migration_index = MigrationIndex([MigrationStatus("db1", "t1", "cat1", "db1", "t1")])
    sequencer = ViewsMigrationSequencer(tables, migration_index=migration_index)

    batches = sequencer.sequence_batches()

    assert [t.src.key for t in flatten(batches)] == expected


@pytest.mark.parametrize("tables", [("db1.v8",)], indirect=True)
def test_sequence_view_with_invalid_query_raises_value_error(tables) -> None:
    sequencer = ViewsMigrationSequencer(tables)

    with pytest.raises(ValueError) as error:
        sequencer.sequence_batches()
    assert "Could not analyze view SQL:" in str(error)


@pytest.mark.parametrize("tables", [("db1.v9",)], indirect=True)
def test_sequencing_logs_unresolved_dependencies(caplog, tables) -> None:
    sequencer = ViewsMigrationSequencer(tables)

    with caplog.at_level(logging.ERROR, logger="databricks.labs.ucx.hive_metastore.view_migrate"):
        sequencer.sequence_batches()
    assert "Unresolved dependencies prevent batch sequencing:" in caplog.text


@pytest.mark.parametrize(
    "tables",
    [
        ("db1.v10", "db1.v11"),
        ("db1.v12", "db1.v13", "db1.v14"),
    ],
    indirect=True,
)
def test_sequencing_logs_circular_dependency(caplog, tables) -> None:
    sequencer = ViewsMigrationSequencer(tables)

    with caplog.at_level(logging.ERROR, logger="databricks.labs.ucx.hive_metastore.view_migrate"):
        sequencer.sequence_batches()
    assert "Circular dependency detected starting from:" in caplog.text
