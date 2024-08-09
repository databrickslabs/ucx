from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.dfsa import DfsaCollector


def test_dfsa_does_not_collect_erroneously(simple_dependency_resolver, migration_index, mock_path_lookup):
    maybe = simple_dependency_resolver.build_local_file_dependency_graph(Path("leaf4.py"), CurrentSessionState())
    collector = DfsaCollector(mock_path_lookup, CurrentSessionState())
    dfsas = list(collector.collect(maybe.graph))
    assert not dfsas


@pytest.mark.parametrize(
    "source_path, dfsa_paths",
    [
        ("dfsa/create_location.sql", ["s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/"]),
        (
            "dfsa/create_location.py",
            [
                "s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/",
                "s3a://db-gtm-industry-solutions/data/fsi/capm/sp_550/",
            ],
        ),
        ("dfsa/select_read_files.sql", ["s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/file.csv"]),
        ("dfsa/select_format.sql", ["hdfs://examples/src/main/resources/users.parquet"]),
        ("dfsa/spark_read_format_load.py", ["s3a://prefix/some_file.csv"]),
        ("dfsa/inferred_path.py", ["s3a://prefix/some_inferred_file.csv"]),
        ("dfsa/spark_implicit_dbfs.py", ["/prefix/some_file.csv"]),
        ("dfsa/spark_dbfs_mount.py", ["/mnt/some_file.csv"]),
    ],
)
def test_dfsa_collects_sql_dfsas(
    source_path, dfsa_paths, simple_dependency_resolver, migration_index, mock_path_lookup
):
    """SQL expression not supported by sqlglot for Databricks, restore the test data below for once we drop sqlglot
    ("dfsa/create_cloud_files.sql", ["s3a://db-gtm-industry-solutions/data/CME/telco/PCMD"]),"""
    maybe = simple_dependency_resolver.build_notebook_dependency_graph(Path(source_path), CurrentSessionState())
    assert not maybe.problems
    collector = DfsaCollector(mock_path_lookup, CurrentSessionState())
    dfsas = list(collector.collect(maybe.graph))
    assert set(dfsa.path for dfsa in dfsas) == set(dfsa_paths)
