WITH last_workflow_run AS (
    SELECT
      workspace_id,
      MAX(STRUCT(finished_at, attempt, started_at, workflow_run_id)) AS max_struct
    FROM $inventory.workflow_runs  -- $inventory is a hardcoded name for replacing target schema in a view definition
    WHERE workflow_name = 'migration-progress-experimental'
    GROUP BY workspace_id
)

SELECT historical.*
FROM
    $inventory.historical AS historical  -- $inventory is a hardcoded name for replacing target schema in a view definition
  JOIN
    last_workflow_run
  ON
    historical.workspace_id = last_workflow_run.workspace_id
    AND historical.job_run_id = last_workflow_run.max_struct.workflow_run_id
