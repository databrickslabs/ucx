SELECT
    catalog_name,
    schema_name,
    table_name,
    source_id,
    source_timestamp,
    source_lineage,
    assessment_start_timestamp,
    assessment_end_timestamp
FROM $inventory.table_infos_in_paths
UNION ALL
SELECT
    catalog_name,
    schema_name,
    table_name,
    source_id,
    source_timestamp,
    source_lineage,
    assessment_start_timestamp,
    assessment_end_timestamp
FROM $inventory.table_infos_in_queries
