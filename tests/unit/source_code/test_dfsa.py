from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.dfsa import DfsaCollector
from databricks.labs.ucx.source_code.linters.context import LinterContext


def test_dfsa_does_not_collect_erroneously(simple_dependency_resolver, migration_index, mock_path_lookup):
    maybe = simple_dependency_resolver.build_local_file_dependency_graph(Path("leaf4.py"), CurrentSessionState())
    context = LinterContext(migration_index, CurrentSessionState())
    collector = DfsaCollector(mock_path_lookup, CurrentSessionState(), lambda: context)
    dfsas = list(collector.collect(maybe.graph))
    assert not dfsas


@pytest.mark.parametrize(
    "source_path, dfsa_paths",
    [
        ("dfsa/create_location.sql", ["s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/"]),
        ("dfsa/select_read_files.sql", ["s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/file.csv"]),
        ("dfsa/select_format.sql", ["hdfs://examples/src/main/resources/users.parquet"]),
        ("dfsa/spark_read_format_load.py", ["s3a://prefix/some_file.csv"]),
    ],
)
def test_dfsa_collects_sql_dfsas(
    source_path, dfsa_paths, simple_dependency_resolver, migration_index, mock_path_lookup
):
    """SQL expression not supported by sqlglot for Databricks, restore the test data below for once we drop sqlglot
    ("dfsa/create_cloud_files.sql", ["s3a://db-gtm-industry-solutions/data/CME/telco/PCMD"]),"""
    maybe = simple_dependency_resolver.build_notebook_dependency_graph(Path(source_path), CurrentSessionState())
    assert not maybe.problems
    context = LinterContext(migration_index, CurrentSessionState())
    collector = DfsaCollector(mock_path_lookup, CurrentSessionState(), lambda: context)
    dfsas = list(collector.collect(maybe.graph))
    assert set(dfsa.path for dfsa in dfsas) == set(dfsa_paths)
