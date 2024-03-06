WITH estimated_hours AS (
  SELECT * FROM (VALUES
    ('jobs', 6, 'not supported DBR:'), -- Upgrade cluster to use a DBR 11.3+ LTS version
    ('jobs', 6, 'Uses azure service principal'), -- Create a cluster that doesn't uses an SP
    ('jobs', 6, 'unsupported config'), -- Create a cluster that doesn't use below spark config
    ('jobs', 6, 'No isolation shared'), -- Recreate cluster with a UC compliant one
    ('jobs', 6, 'cluster type not supported'), -- Recreate cluster with a UC compliant one

    ('submit_runs', 6, 'not supported DBR:'), -- Upgrade cluster to use a DBR 11.3+ LTS version
    ('submit_runs', 6, 'Uses azure service principal'), -- Create a cluster that doesn't uses an SP
    ('submit_runs', 6, 'unsupported config'), -- Create a cluster that doesn't use below spark config
    ('submit_runs', 6, 'No isolation shared'), -- Recreate cluster with a UC compliant one
    ('submit_runs', 6, 'cluster type not supported'), -- Recreate cluster with a UC compliant one
    ('submit_runs', 6, 'no data security mode specified'), -- Recreate cluster with a UC compliant one

    ('clusters', 3, 'not supported DBR:'), -- Upgrade cluster to use a DBR 11.3+ LTS version
    ('clusters', 3, 'Uses azure service principal'), -- Create a cluster that doesn't uses an SP
    ('clusters', 3, 'unsupported config'), -- Create a cluster that doesn't highlighted spark config
    ('clusters', 3, 'No isolation shared'), -- Recreate cluster with a UC compliant one
    ('clusters', 3, 'cluster type not supported'), -- Recreate cluster with a UC compliant one

    ('pipelines', 6, 'Uses azure service principal credentials config in pipeline cluster.'), -- Recreate pipeline on UC without SP in cluster
    ('pipelines', 6, 'Uses azure service principal credentials config in pipeline.'), -- Recreate pipeline on UC without SP in conf
    ('pipelines', 6, 'using DBFS mount in configuration') -- Move input/output data to a volume
  ) AS t(object_type, estimated_hours, failure_prefix)
),
objects AS (
  SELECT object_type, object_id, EXPLODE(from_json(failures, 'array<string>')) AS failure
  FROM $inventory.objects
  WHERE failures <> "[]" AND object_type != "tables"
),
failure_times AS (
  SELECT o.object_type, o.object_id, o.failure, e.estimated_hours FROM objects o LEFT JOIN estimated_hours e ON o.object_type = e.object_type AND startswith(o.failure, e.failure_prefix)
)
SELECT * FROM failure_times