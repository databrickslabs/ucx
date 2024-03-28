import json
from datetime import datetime
from pathlib import Path
from string import Template

from databricks.labs.lsql.backends import MockBackend, SqlBackend

from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.table_migrate import Index, MigrationStatus
from databricks.labs.ucx.hive_metastore.views_migrator import ViewsMigrator

SCHEMA_NAME = "schema"


def test_migrates_no_view_returns_empty_sequence():
    samples = Samples.load("db1_t1", "db2_t1")
    index = create_index(samples)
    sql_backend = mock_backend(samples, "db1", "db2")
    crawler = TablesCrawler(sql_backend, SCHEMA_NAME, ["db1", "db2"])
    migrator = ViewsMigrator(index, crawler)
    sequence = migrator.sequence()
    assert len(sequence) == 0


def test_migrates_direct_view_returns_singleton_sequence() -> None:
    samples = Samples.load("db1_t1", "db1_v1")
    index = create_index(samples)
    sql_backend = mock_backend(samples, "db1")
    crawler = TablesCrawler(sql_backend, SCHEMA_NAME, ["db1", "db2"])
    migrator = ViewsMigrator(index, crawler)
    sequence = migrator.sequence()
    assert len(sequence) == 1
    migration: MigrationStatus = sequence[0]
    assert migration.src_schema == "db1" and migration.src_table == "v1"


def create_index(samples: list[dict]) -> Index:
    def make_status(sample: dict) -> MigrationStatus:
        legacy = sample["legacy"]
        unity = sample["unity"]
        return MigrationStatus(
            legacy["database"], legacy["table"], unity["catalog"], unity["schema"], unity["table"], str(datetime.now())
        )

    statuses = [make_status(s) for s in samples]
    return Index(statuses)


def mock_backend(samples: list[dict], *dbnames: str) -> SqlBackend:
    db_rows: dict[str, list[tuple]] = {}
    show_template = Template('SHOW TABLES FROM hive_metastore.$db')
    select_query = 'SELECT \\* FROM hive_metastore.schema.tables'
    for dbname in dbnames:
        # pylint warning W0640 is a pylint bug (verified manually), see https://github.com/pylint-dev/pylint/issues/5263
        legacy_samples = [s["legacy"] for s in filter(lambda s: s["legacy"]["database"] == dbname, samples)]
        show_tuples = [(s["database"], s["table"], "true") for s in legacy_samples]
        db_rows[show_template.substitute({'db': dbname})] = show_tuples
        # catalog, database, table, object_type, table_format, location, view_text
        select_tuples = [
            (
                "hive_metastore",
                s["database"],
                s["table"],
                "type",
                "DELTA" if s.get("view_text", None) is None else "VIEW",
                None,
                s.get("view_text", None),
            )
            for s in legacy_samples
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
                    cls.samples[sample["id"]] = sample
