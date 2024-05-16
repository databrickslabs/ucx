from databricks.labs.ucx.recon.base import TableIdentifier, DataProfilingResult


def build_metadata_query(entity: TableIdentifier) -> str:
    if entity.catalog == "hive_metastore":
        return f"DESCRIBE TABLE {entity.fqn}"

    query = f"""
        SELECT 
            LOWER(column_name) AS col_name, 
            full_data_type AS data_type
        FROM 
            {entity.catalog}.information_schema.columns
        WHERE
            LOWER(table_catalog)='{entity.catalog}' AND 
            LOWER(table_schema)='{entity.schema}' AND 
            LOWER(table_name) ='{entity.table}'
        ORDER BY col_name"""

    return query


def build_data_comparison_query(
    source_data_profile: DataProfilingResult,
    target_data_profile: DataProfilingResult,
) -> str:
    source_table = source_data_profile.table_metadata.identifier
    target_table = target_data_profile.table_metadata.identifier
    source_hash_inputs = _build_data_comparison_hash_inputs(source_data_profile)
    target_hash_inputs = _build_data_comparison_hash_inputs(target_data_profile)
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
                END AS num_missing_records_in_target,
                CASE 
                    WHEN source.hash_value IS NULL THEN 1
                    ELSE 0
                END AS num_missing_records_in_source
            FROM (
                SELECT SHA2(CONCAT_WS('|', {", ".join(source_hash_inputs)}), 256) AS hash_value
                FROM {source_table.fqn}
            ) AS source
            FULL OUTER JOIN (
                SELECT SHA2(CONCAT_WS('|', {", ".join(target_hash_inputs)}), 256) AS hash_value
                FROM {target_table.fqn}
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
