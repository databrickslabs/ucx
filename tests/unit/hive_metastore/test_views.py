import json
from datetime import datetime
from pathlib import Path
from string import Template

from databricks.labs.lsql.backends import MockBackend, SqlBackend

from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.table_migrate import Index, MigrationStatus


def test_migrates_no_view():
    samples = Samples.load("db1_t1", "db2_t1")
    index = create_index(samples)
    sql_backend = mock_backend(samples, "db1", "db2")
    crawler = TablesCrawler(sql_backend, "hive_metastore", ["db1", "db2"])
    assert index is not None and crawler is not None


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
    db_rows = {}
    template = Template('SHOW TABLES FROM hive_metastore.$db')
    for dbname in dbnames:
        # pylint W0640 here is a pylint bug (verified manually)
        legacy_samples = [s["legacy"] for s in filter(lambda s: s["legacy"]["database"] == dbname, samples)]
        tuples = [("hive_metastore", s["database"], s["table"], s.get("view_text", None)) for s in legacy_samples]
        db_rows[template.substitute({'db': dbname})] = tuples
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
