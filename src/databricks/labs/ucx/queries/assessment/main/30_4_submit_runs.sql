-- viz type=table, name=Submit Runs, columns=hashed_id,finding,run_ids
-- widget title=Incompatible Submit Runs, row=36, col=0, size_x=3, size_y=8
SELECT
    hashed_id,
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
    FROM_JSON(run_ids, 'array<string>') AS run_ids
FROM $inventory.submit_runs
ORDER BY hashed_id DESC