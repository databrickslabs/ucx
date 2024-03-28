-- viz type=table, name=Submit Runs Failures, columns=finding,submit_runs,run_ids
-- widget title=Incompatible Submit Runs Failures, row=36, col=5, size_x=3, size_y=8
SELECT
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
    COUNT(DISTINCT hashed_id) AS submit_runs,
    COLLECT_LIST(DISTINCT run_ids) AS run_ids
FROM $inventory.submit_runs
group by 1
