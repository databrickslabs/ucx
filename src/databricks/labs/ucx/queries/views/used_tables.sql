SELECT
    catalog_name,
    schema_name,
    table_name,
    is_read,
    is_write,
    source_id,
    source_timestamp,
    source_lineage,
    assessment_start_timestamp,
    assessment_end_timestamp
FROM $inventory.used_tables_in_paths
UNION ALL
SELECT
    catalog_name,
    schema_name,
    table_name,
    is_read,
    is_write,
    source_id,
    source_timestamp,
    source_lineage,
    assessment_start_timestamp,
    assessment_end_timestamp
FROM $inventory.used_tables_in_queries
