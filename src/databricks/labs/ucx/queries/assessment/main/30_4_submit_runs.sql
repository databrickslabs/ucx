/* --title 'Incompatible Submit Runs' --width 3 --height 6 */
SELECT
  hashed_id,
  EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
  FROM_JSON(run_ids, 'array<string>') AS run_ids
FROM inventory.submit_runs
ORDER BY
  hashed_id DESC