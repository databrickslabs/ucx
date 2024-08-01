/* --title 'Group Migration Failures' --height 4 --width 4 */ /*
 *
 * Messages that we're looking for are of the form:
 *   failed-group-migration: {name_in_workspace} -> {name_in_account}: {reason}
 */
SELECT
  FROM_UNIXTIME(timestamp) AS timestamp,
  job_run_id,
  level,
  message
FROM inventory.logs
WHERE
  workflow_name IN ('migrate-groups', 'migrate-groups-experimental') AND STARTSWITH(message, 'failed-group-migration:')
ORDER BY
  1