from collections.abc import Iterator

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.core import Row

from ._base import (
    DataComparator,
    DataComparisonResult,
    TableIdentifier,
    DataProfiler,
    DataProfilingResult,
)


class StandardDataComparator(DataComparator):
    def __init__(self, sql_backend: SqlBackend, data_profiler: DataProfiler):
        self._sql_backend = sql_backend
        self._data_profiler = data_profiler

    def compare_data(self, source: TableIdentifier, target: TableIdentifier) -> DataComparisonResult:
        source_data_profile = self._data_profiler.profile_data(source)
        target_data_profile = self._data_profiler.profile_data(target)
        comparison_query = _prepare_comparison_query(source, source_data_profile, target, target_data_profile)
        query_result: Iterator[Row] = self._sql_backend.fetch(comparison_query)
        count_row = next(query_result)
        source_to_target_mismatch_count = int(count_row["source_to_target_mismatch_count"])
        target_to_source_mismatch_count = int(count_row["target_to_source_mismatch_count"])

        return DataComparisonResult(
            source_row_count=source_data_profile.row_count,
            target_row_count=target_data_profile.row_count,
            source_to_target_mismatch_count=source_to_target_mismatch_count,
            target_to_source_mismatch_count=target_to_source_mismatch_count,
        )


def _prepare_comparison_query(
    source: TableIdentifier,
    source_data_profile: DataProfilingResult,
    target: TableIdentifier,
    target_data_profile: DataProfilingResult,
) -> str:
    source_hash_inputs = _build_hash_inputs(source_data_profile)
    target_hash_inputs = _build_hash_inputs(target_data_profile)
    comparison_query = f"""
        WITH compare_results AS (
            SELECT 
                CASE 
                    WHEN source.hash_value IS NULL AND target.hash_value IS NULL THEN TRUE
                    WHEN source.hash_value IS NULL OR target.hash_value IS NULL THEN FALSE
                    WHEN source.hash_value = target.hash_value THEN TRUE
                    ELSE FALSE
                END AS is_match,
                CASE 
                    WHEN target.hash_value IS NULL THEN 1
                    ELSE 0
                END AS source_to_target_mismatch_count,
                CASE 
                    WHEN source.hash_value IS NULL THEN 1
                    ELSE 0
                END AS target_to_source_mismatch_count
            FROM (
                SELECT SHA2(CONCAT_WS('|', {", ".join(source_hash_inputs)}), 256) AS hash_value
                FROM {source.fqn}
            ) AS source
            FULL OUTER JOIN (
                SELECT SHA2(CONCAT_WS('|', {", ".join(target_hash_inputs)}), 256) AS hash_value
                FROM {target.fqn}
            ) AS target
            ON source.hash_value = target.hash_value
        )
        SELECT 
            COUNT(*) AS total_mismatches,
            SUM(source_to_target_mismatch_count) AS source_to_target_mismatch_count,
            SUM(target_to_source_mismatch_count) AS target_to_source_mismatch_count
        FROM compare_results
        WHERE is_match IS FALSE;
        """

    return comparison_query


def _build_hash_inputs(data_profile: DataProfilingResult) -> list[str]:
    source_metadata = data_profile.table_metadata
    inputs = []
    for column in source_metadata.columns:
        data_type = column.data_type.lower()
        if data_type.startswith("array") or data_type.startswith("map") or data_type.startswith("struct"):
            inputs.append(f"TO_JSON({column.name})")
        else:
            inputs.append(column.name)
    return inputs
