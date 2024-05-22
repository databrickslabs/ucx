-- viz type=table, name=Workflow migration problems, search_by=path,code,job_name, columns=job_id,job_name,task_key,code,message,start_line,start_col,end_line,end_cole
-- widget title=Workflow migration problems, row=2, col=2, size_x=4, size_y=8
SELECT
    path,
    code,
    message,
    job_id,
    job_name,
    task_key,
    start_line,
    start_col,
    end_line,
    end_col
FROM {ctx.inventory_database}.workflow_problems