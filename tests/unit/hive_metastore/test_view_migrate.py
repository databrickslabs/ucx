import logging
import json
from itertools import chain
from pathlib import Path
from typing import TypeVar

import pytest
from databricks.labs.lsql.backends import MockBackend, SqlBackend

from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.mapping import Rule, TableToMigrate
from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex, MigrationStatus
from databricks.labs.ucx.hive_metastore.view_migrate import ViewsMigrationSequencer

SCHEMA_NAME = "schema"

T = TypeVar('T')


def flatten(lists: list[list[T]]) -> list[T]:
    return list(chain.from_iterable(lists))


_RULE = Rule("ws1", "cat1", "schema", "db1", "table1", "table2")


class Samples:
    samples: dict = {}

    @classmethod
    def load(cls, *names: str):
        cls._preload_all()
        valid_keys = set(names)
        return [cls.samples[key] for key in filter(lambda key: key in valid_keys, cls.samples.keys())]

    @classmethod
    def _preload_all(cls):
        if len(cls.samples) == 0:
            path = Path(Path(__file__).parent, "tables", "tables_and_views.json")
            with open(path, encoding="utf-8") as file:
                samples = json.load(file)
                cls.samples = {}
                for sample in samples:
                    key = sample["db"] + "." + sample["table"]
                    cls.samples[key] = sample


def mock_backend(samples: list[dict], *dbnames: str) -> SqlBackend:
    db_rows: dict[str, list[tuple]] = {}
    select_query = 'SELECT \\* FROM hive_metastore.schema.tables'
    for dbname in dbnames:
        # pylint warning W0640 is a pylint bug (verified manually), see https://github.com/pylint-dev/pylint/issues/5263
        # pylint: disable=cell-var-from-loop
        valid_samples = list(filter(lambda s: s["db"] == dbname, samples))
        show_tuples = [(s["db"], s["table"], "true") for s in valid_samples]
        db_rows[f'SHOW TABLES FROM hive_metastore.{dbname}'] = show_tuples
        # catalog, database, table, object_type, table_format, location, view_text
        select_tuples = [
            (
                "hive_metastore",
                s["db"],
                s["table"],
                "type",
                "DELTA" if s.get("view_text", None) is None else "VIEW",
                None,
                s.get("view_text", None),
            )
            for s in valid_samples
        ]
        db_rows[select_query] = select_tuples
    return MockBackend(rows=db_rows)


@pytest.fixture
def tables(request) -> list[TableToMigrate]:
    sample_names = request.param
    samples = Samples.load(*sample_names)
    database_names = set(sample["db"] for sample in samples)
    sql_backend = mock_backend(samples, *database_names)
    crawler = TablesCrawler(sql_backend, SCHEMA_NAME, list(database_names))
    return [TableToMigrate(table, _RULE) for table in crawler.snapshot()]


@pytest.mark.parametrize("tables", [("db1.t1", "db2.t1")], indirect=True)
def test_migrate_no_view_returns_empty_sequence(tables):
    migration_index = MigrationIndex(
        [
            MigrationStatus("db1", "t1", "cat1", "db2", "t1"),
            MigrationStatus("db2", "t2", "cat1", "db2", "t1"),
        ]
    )
    sequencer = ViewsMigrationSequencer(tables, migration_index=migration_index)
    batches = sequencer.sequence_batches()

    assert len(batches) == 0


@pytest.mark.parametrize("tables", [("db1.t1", "db1.v1")], indirect=True)
def test_migrate_direct_view_returns_singleton_sequence(tables) -> None:
    migration_index = MigrationIndex([MigrationStatus("db1", "t1", "cat1", "db1", "t1")])
    sequencer = ViewsMigrationSequencer(tables, migration_index=migration_index)

    batches = sequencer.sequence_batches()

    sequence = list(flatten(batches))
    assert len(sequence) == 1
    assert sequence[0].src.key == "hive_metastore.db1.v1"


@pytest.mark.parametrize("tables", [("db1.t1", "db1.v1", "db1.t2", "db1.v2")], indirect=True)
def test_migrate_direct_views_returns_sequence(tables) -> None:
    expected = {"hive_metastore.db1.v1", "hive_metastore.db1.v2"}
    migration_index = MigrationIndex(
        [MigrationStatus("db1", "t1", "cat1", "db1", "t1"), MigrationStatus("db1", "t2", "cat1", "db1", "t2")]
    )
    sequencer = ViewsMigrationSequencer(tables, migration_index=migration_index)

    batches = sequencer.sequence_batches()

    assert {t.src.key for t in flatten(batches)} == expected


@pytest.mark.parametrize("tables", [("db1.t1", "db1.v1", "db1.v4")], indirect=True)
def test_migrate_indirect_views_returns_correct_sequence(tables) -> None:
    expected = ["hive_metastore.db1.v1", "hive_metastore.db1.v4"]
    migration_index = MigrationIndex([MigrationStatus("db1", "t1", "cat1", "db1", "t1")])
    sequencer = ViewsMigrationSequencer(tables, migration_index=migration_index)

    batches = sequencer.sequence_batches()

    assert [t.src.key for t in flatten(batches)] == expected


@pytest.mark.parametrize("tables", [("db1.t1", "db1.v1", "db1.v4", "db1.v5", "db1.v6", "db1.v7")], indirect=True)
def test_migrate_deep_indirect_views_returns_correct_sequence(tables) -> None:
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
