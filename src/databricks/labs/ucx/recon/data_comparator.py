from collections.abc import Iterator

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.core import Row

from .base import (
    DataComparator,
    DataComparisonResult,
    TableIdentifier,
    DataProfiler,
)
from .query_builder import build_data_comparison_query


class StandardDataComparator(DataComparator):
    def __init__(self, sql_backend: SqlBackend, data_profiler: DataProfiler):
        self._sql_backend = sql_backend
        self._data_profiler = data_profiler

    def compare_data(self, source: TableIdentifier, target: TableIdentifier) -> DataComparisonResult:
        source_data_profile = self._data_profiler.profile_data(source)
        target_data_profile = self._data_profiler.profile_data(target)
        comparison_query = build_data_comparison_query(source_data_profile, target_data_profile)
        query_result: Iterator[Row] = self._sql_backend.fetch(comparison_query)
        count_row = next(query_result)
        num_missing_records_in_target = int(count_row["num_missing_records_in_target"])
        num_missing_records_in_source = int(count_row["num_missing_records_in_source"])
        return DataComparisonResult(
            source_row_count=source_data_profile.row_count,
            target_row_count=target_data_profile.row_count,
            num_missing_records_in_target=num_missing_records_in_target,
            num_missing_records_in_source=num_missing_records_in_source,
        )
