from pathlib import Path

import pytest


from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.source_code.base import CurrentSessionState, DFSA
from databricks.labs.ucx.source_code.dfsa import DfsaCollector, DfsaCrawler
from databricks.labs.ucx.source_code.linters.context import LinterContext


def test_crawler_appends_dfsas():
    backend = MockBackend()
    crawler = DfsaCrawler(backend, "schema")
    for path in ("a", "b", "c"):
        dfsa = DFSA(source_type="SOURCE", source_id="ID", path=path, is_read=False, is_write=False)
        crawler.append(dfsa)
    rows = backend.rows_written_for(crawler.full_name, "append")
    assert len(rows) == 3


def test_dfsa_does_not_collect_erroneously(simple_dependency_resolver, migration_index, mock_path_lookup):
    maybe = simple_dependency_resolver.build_local_file_dependency_graph(Path("leaf4.py"), CurrentSessionState())
    crawler = DfsaCrawler(MockBackend(), "schema")
    context = LinterContext(migration_index, CurrentSessionState())
    collector = DfsaCollector(crawler, mock_path_lookup, CurrentSessionState(), lambda: context)
    dfsas = list(collector.collect(maybe.graph))
    assert not dfsas


@pytest.mark.parametrize(
    "source_path, dfsa_paths, is_read, is_write",
    [
        ("dfsa/create_location.sql", ["s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/"], False, True),
        ("dfsa/create_location.py", ["s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/", "s3a://db-gtm-industry-solutions/data/fsi/capm/sp_550/"], False, True),
        ("dfsa/select_read_files.sql", ["s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/file.csv"], True, False),
        ("dfsa/select_format.sql", ["hdfs://examples/src/main/resources/users.parquet"], True, False),
        ("dfsa/spark_read_format_load.py", ["s3a://prefix/some_file.csv"], True, False),
        ("dfsa/inferred_path.py", ["s3a://prefix/some_inferred_file.csv"], True, False),
        ("dfsa/spark_implicit_dbfs.py", ["/prefix/some_file.csv"], True, False),
        ("dfsa/spark_dbfs_mount.py", ["/mnt/some_file.csv"], True, False),
    ],
)
def test_dfsa_collects_sql_dfsas(
    source_path, dfsa_paths, is_read, is_write, simple_dependency_resolver, migration_index, mock_path_lookup
):
    """SQL expression not supported by sqlglot for Databricks, restore the test data below for once we drop sqlglot
    ("dfsa/create_cloud_files.sql", ["s3a://db-gtm-industry-solutions/data/CME/telco/PCMD"]),"""
    maybe = simple_dependency_resolver.build_notebook_dependency_graph(Path(source_path), CurrentSessionState())
    assert not maybe.problems
    crawler = DfsaCrawler(MockBackend(), "schema")
    context = LinterContext(migration_index, CurrentSessionState())
    collector = DfsaCollector(crawler, mock_path_lookup, CurrentSessionState(), lambda: context)
    dfsas = list(collector.collect(maybe.graph))
    assert set(dfsa.path for dfsa in dfsas) == set(dfsa_paths)
    assert not any(dfsa for dfsa in dfsas if dfsa.source_type == DFSA.UNKNOWN)
    assert not any(dfsa for dfsa in dfsas if dfsa.is_read != is_read)
    assert not any(dfsa for dfsa in dfsas if dfsa.is_write != is_write)
