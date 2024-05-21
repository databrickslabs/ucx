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


def test_migrate_no_view_returns_empty_sequence():
    samples = Samples.load("db1.t1", "db2.t1")
    sql_backend = mock_backend(samples, "db1", "db2")
    crawler = TablesCrawler(sql_backend, SCHEMA_NAME, ["db1", "db2"])
    tables = [TableToMigrate(table, _RULE) for table in crawler.snapshot()]
    migration_index = MigrationIndex(
        [MigrationStatus("db1", "t1", "cat1", "db2", "t1"), MigrationStatus("db2", "t2", "cat1", "db2", "t1")],
    )
    sequencer = ViewsMigrationSequencer(tables, migration_index)
    batches = sequencer.sequence_batches()
    sequence = list(flatten(batches))
    assert len(sequence) == 0


def test_migrate_direct_view_returns_singleton_sequence() -> None:
    samples = Samples.load("db1.t1", "db1.v1")
    sql_backend = mock_backend(samples, "db1")
    crawler = TablesCrawler(sql_backend, SCHEMA_NAME, ["db1"])
    tables = [TableToMigrate(table, _RULE) for table in crawler.snapshot()]
    migration_index = MigrationIndex([MigrationStatus("db1", "t1", "cat1", "db1", "t1")])
    sequencer = ViewsMigrationSequencer(tables, migration_index)
    batches = sequencer.sequence_batches()
    sequence = list(flatten(batches))
    assert len(sequence) == 1
    table = sequence[0]
    assert table.src.key == "hive_metastore.db1.v1"


def test_migrate_direct_views_returns_sequence() -> None:
    samples = Samples.load("db1.t1", "db1.v1", "db1.t2", "db1.v2")
    sql_backend = mock_backend(samples, "db1")
    crawler = TablesCrawler(sql_backend, SCHEMA_NAME, ["db1"])
    tables = [TableToMigrate(table, _RULE) for table in crawler.snapshot()]
    migration_index = MigrationIndex(
        [MigrationStatus("db1", "t1", "cat1", "db1", "t1"), MigrationStatus("db1", "t2", "cat1", "db1", "t2")],
    )
    sequencer = ViewsMigrationSequencer(tables, migration_index)
    batches = sequencer.sequence_batches()
    sequence = list(flatten(batches))
    assert len(sequence) == 2
    expected = {"hive_metastore.db1.v1", "hive_metastore.db1.v2"}
    actual = {t.src.key for t in sequence}
    assert expected == actual


def test_migrate_indirect_views_returns_correct_sequence() -> None:
    samples = Samples.load("db1.t1", "db1.v1", "db1.v4")
    sql_backend = mock_backend(samples, "db1")
    crawler = TablesCrawler(sql_backend, SCHEMA_NAME, ["db1"])
    tables = [TableToMigrate(table, _RULE) for table in crawler.snapshot()]
    migration_index = MigrationIndex([MigrationStatus("db1", "t1", "cat1", "db1", "t1")])
    sequencer = ViewsMigrationSequencer(tables, migration_index)
    batches = sequencer.sequence_batches()
    sequence = list(flatten(batches))
    assert len(sequence) == 2
    expected = ["hive_metastore.db1.v1", "hive_metastore.db1.v4"]
    actual = [t.src.key for t in sequence]
    assert expected == actual


def test_migrate_deep_indirect_views_returns_correct_sequence() -> None:
    samples = Samples.load("db1.t1", "db1.v1", "db1.v4", "db1.v5", "db1.v6", "db1.v7")
    sql_backend = mock_backend(samples, "db1")
    crawler = TablesCrawler(sql_backend, SCHEMA_NAME, ["db1"])
    tables = [TableToMigrate(table, _RULE) for table in crawler.snapshot()]
    migration_index = MigrationIndex([MigrationStatus("db1", "t1", "cat1", "db1", "t1")])
    sequencer = ViewsMigrationSequencer(tables, migration_index)
    batches = sequencer.sequence_batches()
    sequence = list(flatten(batches))
    assert len(sequence) == 5
    expected = [
        "hive_metastore.db1.v1",
        "hive_metastore.db1.v4",
        "hive_metastore.db1.v7",
        "hive_metastore.db1.v6",
        "hive_metastore.db1.v5",
    ]
    actual = [t.src.key for t in sequence]
    assert expected == actual


def test_migrate_invalid_sql_raises_value_error() -> None:
    with pytest.raises(ValueError) as error:
        samples = Samples.load("db1.v8")
        sql_backend = mock_backend(samples, "db1")
        crawler = TablesCrawler(sql_backend, SCHEMA_NAME, ["db1"])
        tables = [TableToMigrate(table, _RULE) for table in crawler.snapshot()]
        migration_index = MigrationIndex([])
        sequencer = ViewsMigrationSequencer(tables, migration_index)
        batches = sequencer.sequence_batches()
        sequence = list(flatten(batches))
        assert sequence is None  # should never get there
    assert "Could not analyze view SQL:" in str(error)


def test_migrate_invalid_sql_tables_raises_value_error() -> None:
    with pytest.raises(ValueError) as error:
        samples = Samples.load("db1.v9")
        sql_backend = mock_backend(samples, "db1")
        crawler = TablesCrawler(sql_backend, SCHEMA_NAME, ["db1"])
        tables = [TableToMigrate(table, _RULE) for table in crawler.snapshot()]
        migration_index = MigrationIndex([])
        sequencer = ViewsMigrationSequencer(tables, migration_index)
        batches = sequencer.sequence_batches()
        sequence = list(flatten(batches))
        assert sequence is None  # should never get there
    assert "Invalid table references are preventing migration:" in str(error)


def test_migrate_circular_views_raises_value_error() -> None:
    with pytest.raises(ValueError) as error:
        samples = Samples.load("db1.v10", "db1.v11")
        sql_backend = mock_backend(samples, "db1")
        crawler = TablesCrawler(sql_backend, SCHEMA_NAME, ["db1"])
        tables = [TableToMigrate(table, _RULE) for table in crawler.snapshot()]
        migration_index = MigrationIndex([])
        sequencer = ViewsMigrationSequencer(tables, migration_index)
        batches = sequencer.sequence_batches()
        sequence = list(flatten(batches))
        assert sequence is None  # should never get there
    assert "Circular dependency detected between" in str(error)


def test_migrate_circular_view_chain_raises_value_error() -> None:
    with pytest.raises(ValueError) as error:
        samples = Samples.load("db1.v12", "db1.v13", "db1.v14")
        sql_backend = mock_backend(samples, "db1")
        crawler = TablesCrawler(sql_backend, SCHEMA_NAME, ["db1"])
        tables = [TableToMigrate(table, _RULE) for table in crawler.snapshot()]
        migration_index = MigrationIndex([])
        sequencer = ViewsMigrationSequencer(tables, migration_index)
        batches = sequencer.sequence_batches()
        sequence = list(flatten(batches))
        assert sequence is None  # should never get there
    assert "Circular dependency detected between" in str(error)


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
