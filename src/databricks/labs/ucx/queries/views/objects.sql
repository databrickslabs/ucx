SELECT
  'jobs' AS object_type,
  job_id AS object_id,
  failures
FROM $inventory.jobs
UNION ALL
SELECT
  'clusters' AS object_type,
  cluster_id AS object_id,
  failures
FROM $inventory.clusters
UNION ALL
SELECT
  'global init scripts' AS object_type,
  script_id AS object_id,
  failures
FROM $inventory.global_init_scripts
UNION ALL
SELECT
  'submit_runs' AS object_type,
  hashed_id AS object_id,
  failures
FROM $inventory.submit_runs
UNION ALL
SELECT
  'pipelines' AS object_type,
  pipeline_id AS object_id,
  failures
FROM $inventory.pipelines
UNION ALL
SELECT
  object_type,
  object_id,
  failures
FROM (
  SELECT
    'tables' AS object_type,
    CONCAT(t.catalog, '.', t.database, '.', t.name) AS object_id,
    TO_JSON(
      FILTER(
        ARRAY(
          IF(NOT STARTSWITH(t.table_format, 'DELTA') AND t.object_type <> 'VIEW', CONCAT('Non-DELTA format: ', t.table_format), NULL),
          IF(STARTSWITH(t.location, 'wasb'), 'Unsupported Storage Type: wasb://', NULL),
          IF(STARTSWITH(t.location, 'adl'), 'Unsupported Storage Type: adl://', NULL),
          CASE
            WHEN STARTSWITH(t.location, 'dbfs:/mnt')
            THEN 'Data is in DBFS Mount'
            WHEN STARTSWITH(t.location, '/dbfs/mnt')
            THEN 'Data is in DBFS Mount'
            WHEN STARTSWITH(t.location, 'dbfs:/')
            THEN 'Data is in DBFS Root'
            WHEN STARTSWITH(t.location, '/dbfs/')
            THEN 'Data is in DBFS Root'
            ELSE NULL
          END,
          tf.error
        ),
        f -> NOT f IS NULL
      )
    ) AS failures
  FROM $inventory.tables AS t
  FULL JOIN $inventory.table_failures AS tf
    USING (catalog, database, name)
)
UNION ALL
SELECT
  'databases' AS object_type,
  CONCAT(catalog, '.', database) AS object_id,
  TO_JSON(ARRAY(error)) AS failures
FROM $inventory.table_failures
WHERE
  name IS NULL
UNION ALL
SELECT
  'permissions' AS object_type,
  object_id,
  failures
FROM $inventory.grant_detail