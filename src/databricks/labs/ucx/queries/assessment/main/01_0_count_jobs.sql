-- viz type=counter, name=Total Job Count, counter_label=Total Jobs, value_column=count_total_jobs
-- widget row=1, col=0, size_x=2, size_y=3
SELECT count(*) AS count_total_jobs FROM $inventory.jobs WHERE job_name not like '[UCX]%'