-- --title 'Submit Runs Failures'
SELECT
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
    COUNT(DISTINCT hashed_id) AS submit_runs,
    COLLECT_LIST(DISTINCT run_ids) AS run_ids
FROM inventory.submit_runs
group by 1
