from collections.abc import Iterator

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.core import Row

from .base import (
    DataComparator,
    DataComparisonResult,
    TableIdentifier,
    DataProfiler,
    DataProfilingResult,
)


class StandardDataComparator(DataComparator):
    DATA_COMPARISON_QUERY_TEMPLATE = """
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
            END AS num_missing_records_in_target,
            CASE 
                WHEN source.hash_value IS NULL THEN 1
                ELSE 0
            END AS num_missing_records_in_source
        FROM (
            SELECT {source_hash_expr} AS hash_value
            FROM {source_table_fqn}
        ) AS source
        FULL OUTER JOIN (
            SELECT {target_hash_expr} AS hash_value
            FROM {target_table_fqn}
        ) AS target
        ON source.hash_value = target.hash_value
    )
    SELECT 
        COUNT(*) AS total_mismatches,
        COALESCE(SUM(num_missing_records_in_target), 0) AS num_missing_records_in_target,
        COALESCE(SUM(num_missing_records_in_source), 0) AS num_missing_records_in_source
    FROM compare_results
    WHERE is_match IS FALSE;
    """

    def __init__(self, sql_backend: SqlBackend, data_profiler: DataProfiler):
        self._sql_backend = sql_backend
        self._data_profiler = data_profiler

    def compare_data(
        self,
        source: TableIdentifier,
        target: TableIdentifier,
        row_comparison: bool = False,
    ) -> DataComparisonResult:
        """
        This method compares the data of two tables. It takes two TableIdentifier objects as input, which represent
        the source and target tables for which the data are to be compared.

        Note: This method does not handle exceptions raised during the execution of the SQL query or
        the retrieval of the table metadata. These exceptions are expected to be handled by the caller in a manner
        appropriate for their context.
        """
        source_data_profile = self._data_profiler.profile_data(source)
        target_data_profile = self._data_profiler.profile_data(target)
        if row_comparison:
            return DataComparisonResult(
                source_row_count=source_data_profile.row_count,
                target_row_count=target_data_profile.row_count,
            )
        comparison_query = StandardDataComparator.build_data_comparison_query(
            source_data_profile,
            target_data_profile,
        )
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

    @classmethod
    def build_data_comparison_query(
        cls,
        source_data_profile: DataProfilingResult,
        target_data_profile: DataProfilingResult,
    ) -> str:
        source_table = source_data_profile.table_metadata.identifier
        target_table = target_data_profile.table_metadata.identifier
        source_hash_inputs = _build_data_comparison_hash_inputs(source_data_profile)
        target_hash_inputs = _build_data_comparison_hash_inputs(target_data_profile)
        comparison_query = StandardDataComparator.DATA_COMPARISON_QUERY_TEMPLATE.format(
            source_hash_expr=f"SHA2(CONCAT_WS('|', {', '.join(source_hash_inputs)}), 256)",
            target_hash_expr=f"SHA2(CONCAT_WS('|', {', '.join(target_hash_inputs)}), 256)",
            source_table_fqn=source_table.fqn_escaped,
            target_table_fqn=target_table.fqn_escaped,
        )

        return comparison_query


def _build_data_comparison_hash_inputs(data_profile: DataProfilingResult) -> list[str]:
    source_metadata = data_profile.table_metadata
    inputs = []
    for column in source_metadata.columns:
        data_type = column.data_type.lower()
        transformed_column = column.name

        if data_type.startswith("array"):
            transformed_column = f"TO_JSON(SORT_ARRAY({column.name}))"
        elif data_type.startswith("map") or data_type.startswith("struct"):
            transformed_column = f"TO_JSON({column.name})"

        inputs.append(f"COALESCE(TRIM({transformed_column}), '')")
    return inputs
