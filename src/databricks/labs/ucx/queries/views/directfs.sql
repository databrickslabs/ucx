SELECT
    *
FROM $inventory.direct_file_system_access_in_paths
UNION ALL
SELECT
    *, NULL as job_id, NULL as job_name, null as task_key
FROM $inventory.direct_file_system_access_in_queries
