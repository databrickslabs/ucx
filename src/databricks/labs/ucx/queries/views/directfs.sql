SELECT
    path,
    is_read,
    is_write,
    source_id,
    source_timestamp,
    source_lineage,
    assessment_start_timestamp,
    assessment_end_timestamp
FROM $inventory.directfs_in_paths
UNION ALL
SELECT
    path,
    is_read,
    is_write,
    source_id,
    source_timestamp,
    source_lineage,
    assessment_start_timestamp,
    assessment_end_timestamp
FROM $inventory.directfs_in_queries
