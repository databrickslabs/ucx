-- --title 'Workflow migration problems'
SELECT
    path,
    code,
    message,
    job_id AS workflow_id,
    job_name AS workflow_name,
    task_key,
    start_line,
    start_col,
    end_line,
    end_col
FROM inventory.workflow_problems
