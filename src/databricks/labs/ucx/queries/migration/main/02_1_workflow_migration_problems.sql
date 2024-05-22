-- viz type=table, name=Workflow migration problems, search_by=path,code,job_name, columns=path,code,message,workflow_id,workflow_name,task_key,start_line,start_col,end_line,end_col
-- widget title=Workflow migration problems, row=2, col=2, size_x=4, size_y=8
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
FROM $inventory.workflow_problems