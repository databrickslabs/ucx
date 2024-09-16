SELECT
    path,
    is_read,
    is_write,
    source_id,
    source_timestamp,
    source_lineage,
    assessment_start_timestamp,
    assessment_end_timestamp,
    job_id,
    job_name,
    task_key
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
    assessment_end_timestamp,
    NULL as job_id,
    NULL as job_name,
    null as task_key
FROM $inventory.directfs_in_queries
